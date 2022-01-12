from datetime import datetime, timedelta
import os
import shutil

import pandas as pd
from retry import retry

import lib.hooks.bq_hook as bq
from lib.utils import datetime_util
from lib.utils.google_drive import GoogleDriveClient


def execute(sql,
            file_name_prefix,
            google_drive_folder_id,
            project_id,
            pivot_index=[],
            pivot_columns=[],
            pivot_values=[],
            pivot_sort=[],
            query_parameters=None,
            bq_max_return_records=500000,
            file_encoding='utf_8_sig',
            opt_google_drive_file_date_suffix=False,
            *args,
            **kwargs):
    """BigQueryのデータをピボットテーブルとしてGoogleドライブにファイル出力する。

    Args:
        sql (str): SQLファイルパス
        file_name_prefix (str): Googleドライブに出力するファイル名サフィックス（このファイル名に.csvが付与される）
        google_drive_folder_id (str): Googleドライブの出力先フォルダID
        project_id (str): プロジェクトID
        pivot_index (list): [description]. ピボットテーブルのインデックスのリスト.
        pivot_columns (list): [description]. ピボットテーブルのカラムのリスト.
        pivot_values (list): [description]. ピボットテーブルのインデックスのリスト.
        pivot_sort (list): ピボットテーブルのソートするカラムのリスト.
        query_parameters (dict, optional): BigQueryのクエリパラメータ. Defaults to None.
        bq_max_return_records (int, optional): BigQueryのクエリで参照する最大レコード数. Defaults to 500000.
        file_encoding (str, optional): 出力ファイルの文字コード(SHIFT_JIS,UTF8,UTF8SIGのみ). Defaults to 'utf_8_sig'.
        opt_google_drive_file_date_suffix(boolean, optional):Googleドライブに同名のファイルが既にあった場合、
            作成日が当日であれば上書き、前日以前であれば作成日のサフィックスをつけて退避する。. Defaults to False.
    """

    # 作業フォルダ設定
    work_dir = '/home/airflow/gcs/data/tmp/script/bq_pivot_to_google_drive_hook/{}_{}'.format(
        google_drive_folder_id,
        file_name_prefix)
    work_sub_dir = '{}/tmp'.format(work_dir)

    # 空の作業フォルダ作成
    __make_empty_local_dir(work_dir)
    __make_empty_local_dir(work_sub_dir)

    # BigQueryのデータを取得し、DataFrameを作成
    df = __bq_query(project_id, sql, query_parameters, bq_max_return_records)

    # ピボットテーブル作成
    piv_df = __create_pivot_table(df, pivot_index, pivot_columns, pivot_values, pivot_sort)

    # # CSVファイル出力
    output_file_name = '{}.csv'.format(file_name_prefix)
    output_file_path = '{}/{}'.format(work_dir, output_file_name)
    piv_df.to_csv(
        output_file_path,
        index=False,
        encoding=file_encoding)

    # Googleドライブにアップロード
    __upload_google_drive(
        google_drive_folder_id,
        output_file_name,
        output_file_path,
        work_sub_dir,
        opt_google_drive_file_date_suffix)


def __make_empty_local_dir(local_dir):
    """ローカルに空の作業ディレクトリを作成する。
    既にディレクトリが存在していた場合、配下のファイルは削除される。

    Args:
        local_dir (str): ディレクトリのパス
    """
    try:
        shutil.rmtree(local_dir)
    except FileNotFoundError:
        pass
    os.makedirs(local_dir, exist_ok=True)


def __bq_query(project_id, sql, query_parameters, bq_max_return_records):
    """除外データのうちSFIDが紐付いたものを元に、SFの事業所オブジェクトを更新する
    更新処理はSFのbulk APIで、UPDATEで行う。

    Args:
        project_id (str): GCPのプロジェクトID
        sql (str): SQLファイルパス
        query_parameters (dict): クエリパラメータ
        bq_max_return_records(int): 最大取得レコード数

    Returns:
        DataFrame: クエリ結果
    """

    # BigQueryから事業所データとそれに紐づく除外データを取得
    query_result = bq.bq_query(sql,
                               project_id=project_id,
                               query_parameters=query_parameters,
                               is_return_result=True,
                               max_return_records=bq_max_return_records)
    df = pd.DataFrame.from_records(query_result)
    return df


def __create_pivot_table(df, pivot_index, pivot_columns, pivot_values, pivot_sort):
    """DataFrameをもとにピボットテーブルを作成する。

    Args:
        df (DataFrame): 集計元データ
        pivot_index (list): インデックス
        pivot_columns (list): カラム
        pivot_values (list): 集計値
        pivot_sort (list):ソートするカラム

    Returns:
        DataFrame: ピボットテーブルとして集計したDataFrame
    """
    piv_list = []
    # インデックスリストを論理名、物理名で分割
    index_list = []
    index_name_list = []
    for index in pivot_index:
        index_list.append(index['column'])
        index_name_list.append(index['name'])

    # ソート用カラムをインデックスリストに追加
    sort_name_list = []
    for i, sort in enumerate(pivot_sort):
        # valuesの場合、指標でソート
        if sort == 'values':
            sort_name_list.append('__values_sort__')
            continue
        # 既にインデックスリストに同名のカラムがある場合は追加しない
        skip = False
        for index in pivot_index:
            if sort == index['column']:
                # ソートリストを日本語カラムに書き換え
                sort_name_list.append(index['name'])
                skip = True
                break
        if skip:
            continue
        index_list.append(sort)
        index_name_list.append('{}__sort__'.format(i))
        sort_name_list.append('{}__sort__'.format(i))

    # 集計項目ごとにピボットテーブルを作成
    for i, value in enumerate(pivot_values):
        piv = pd.pivot_table(
            df,
            index=index_list,
            columns=pivot_columns,
            values=[value['column']],
            fill_value=0)
        # 集計項目名を列に追加
        piv.insert(0, '__values_sort__', i)
        piv.insert(0, '指標', value['name'])
        # 出力用に列名整形。indexとカラムを１行にする。
        index_column_names = index_name_list.copy()
        index_column_names.extend(['指標', '__values_sort__'])
        piv.reset_index(inplace=True)
        value_column_names = [col[1] for col in piv.columns.values]
        n = len(index_column_names)
        index_column_names.extend(value_column_names[n:])
        piv.columns = index_column_names
        piv_list.append(piv)

    # １つのdfにまとめる
    ret_df = pd.concat(piv_list)

    # ソート
    ret_df = ret_df.sort_values(sort_name_list)

    # ソート用カラムをテーブルから削除
    ret_df = ret_df.drop('__values_sort__', axis=1)
    for i, s in enumerate(pivot_sort):
        try:
            ret_df = ret_df.drop('{}__sort__'.format(i), axis=1)
        except KeyError:
            continue
    return ret_df


@retry(tries=3)
def __upload_google_drive(
        google_drive_folder_id,
        upload_file_name,
        upload_file_path,
        work_sub_dir,
        opt_google_drive_file_date_suffix):
    """Googleドライブにファイルをアップロードする。
    Googleドライブ上に同名のファイルがあった場合、作成日を参照し、
    処理日以前の作成日であれば作成日のサフィックスを付与してリネーム、
    作成日が処理日の場合は退避せずに上書きでアップロードする。

    Args:
        google_drive_folder_id (str): GoogleドライブフォルダID
        upload_file_name (str): アップロードするファイル名
        upload_file_path (str): アップロードするファイルのフルパス
        work_sub_dir (str): ファイルのリネーム処理を行う作業ディレクトリ
        opt_google_drive_file_date_suffix(boolean, optional):Googleドライブに同名のファイルが既にあった場合、
            作成日が当日であれば上書き、前日以前であれば作成日のサフィックスをつけて退避する。. Defaults to False.
    """
    # Googleドライブ認証
    client = GoogleDriveClient()

    if opt_google_drive_file_date_suffix:
        try:
            # アップロードファイルと同名のファイルをGoogleドライブからダウンロード
            gdrive_files = client.download_files(google_drive_folder_id, upload_file_name, work_sub_dir, query_operator='equals')
            # 同名ファイルの作成日チェック
            created_datetime = datetime.strptime(gdrive_files[0]['createdDate'][:19].replace('T', ' '), '%Y-%m-%d %H:%M:%S') + timedelta(hours=9)
            created_date = created_datetime.date()
            if(created_date == datetime_util.today_jst.date()):
                # 作成日が処理日の場合は削除される
                print('Googleドライブ上の本日作成の「{}」を削除します。'.format(upload_file_name))
            else:
                # 作成日が処理日でない場合は、別名で再アップロード
                client.upload_file(
                    '{}/{}'.format(work_sub_dir, upload_file_name),
                    google_drive_folder_id,
                    upload_file_name.replace('.csv', '_{}.csv'.format(created_date.strftime('%Y%m%d'))))
                print('Googleドライブ上の「{}」に作成日のサフィックスを付与します。'.format(upload_file_name))

            # Googleドライブ上の同名ファイルを削除
            client.delete_file(gdrive_files[0]['id'])
        except FileNotFoundError:
            # ファイルがない=新規作成なのでエラーとしない
            pass

    # 作成したファイルをGoogleドライブ上にアップロード
    client.upload_file(
        upload_file_path,  # アップロードするファイルのファイルパス
        google_drive_folder_id,  # アップロード先のGドライブのファイルID
        upload_file_name)  # アップロードするファイルのファイル名
