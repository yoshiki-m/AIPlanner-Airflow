"""BigQueryからGoogleドライブにファイル連携する
"""
import os
import time
import xlsxwriter

from airflow.models import Variable

from lib.hooks import bq_hook
from lib.utils import file_util, google_drive
from pprint import pprint


# リトライ回数
RETRY_COUNT = 3
# リトライ間隔（秒）
RETRY_INTERVAL = 10


def execute(source_project_dataset_table,
            file_name,
            folder_id,
            project_id,
            compression='NONE',
            export_format='CSV',
            field_delimiter=',',
            print_header=True,
            file_encoding=None,
            file_newline=None,
            location=None,
            *args,
            **kwargs):
    """BigQueryのテーブルをそのままGoogleドライブに出力する

    Args:
        source_project_dataset_table (str): 出力元BigQueryのプロジェクト、データセット、テーブルの文字列
        file_name (str): Googleドライブに出力するファイル名
        folder_id (str): Googleドライブの出力先フォルダID
        project_id (str): プロジェクトID
        compression (google.cloud.bigquery.job.Compression): 圧縮種別。 Defaults to 'NONE'.
        export_format (google.cloud.bigquery.job.DestinationFormat): 出力フォーマット。 Defaults to 'CSV'.
        field_delimiter (str): ファイル区切り文字。 Defaults to ','.
        print_header (bool): ヘッダを出力するかどうか。 Defaults to True.
        file_encoding (str): 出力ファイルの文字コード(SHIFT_JIS,UTF8,UTF8SIGのみ)
        file_newline (str): 出力ファイルの改行コード(CRLF,LFのみ)
        location (str): 出力元BigQueryのデータセットのロケーション。Noneの場合、東京リージョン。 Defaults to None.
    """
    # GCSのURIとローカルファイル名設定
    gcs_uri = '{}/tmp/script/bq_to_google_drive/{}'.format(Variable.get('gcs_data_folder'),
                                                           file_name)
    local_file = '/home/airflow/gcs/data/tmp/script/bq_to_google_drive/{}'.format(file_name)
    upload_file = '/home/airflow/gcs/data/tmp/script/bq_to_google_drive/tmp_{}'.format(file_name)

    # BigQueryのテーブルをGCSに出力
    print('== [START] BigQueryのテーブルをGCSに出力 ==')
    bq_hook.bq_extract(
        source_project_dataset_table=source_project_dataset_table,
        destination_cloud_storage_uris=gcs_uri,
        project_id=project_id,
        compression=compression,
        export_format=export_format,
        field_delimiter=field_delimiter,
        print_header=print_header,
        location=location)
    print('== [END] BigQueryのテーブルをGCSに出力 ==')
    # ファイルの文字コード、改行コードを変換
    if file_encoding == 'UTF8' and file_newline == 'LF':
        # 変換せず、変数名のみ変更
        upload_file = local_file
    else:
        to_file_encoding = file_util.ENCODING_SHIFT_JIS
        to_file_newline = file_util.NEWLINE_WINDOWS
        if file_encoding == 'UTF8':
            to_file_encoding = file_util.ENCODING_UTF_8
        elif file_encoding == 'UTF8SIG':
            to_file_encoding = file_util.ENCODING_UTF_8_SIG

        if file_newline == 'LF':
            to_file_newline = file_util.NEWLINE_UNIX
        print('== [START] 文字コード、改行コードを変換 ==')
        file_util.encode_file(local_file,
                              upload_file,
                              to_file_encoding=to_file_encoding,
                              to_file_newline=to_file_newline,
                              encoding_error_option=file_util.ENCODING_ERROR_REPLACE)
        # GCSの一時ファイルを削除
        os.remove(local_file)
        print('== [END] 文字コード、改行コードを変換 ==')

    # Googleドライブのフォルダにアップロード
    print('== [START] Googleドライブのフォルダにアップロード ==')
    gdrive_client = google_drive.GoogleDriveClient()
    gdrive_client.upload_file(upload_file, folder_id, file_name)
    print('== [END] Googleドライブのフォルダにアップロード ==')

    # GCSの一時ファイルを削除
    os.remove(upload_file)


def execute_sql(sql,
                file_name,
                folder_id,
                parent_folder_id,
                upload_path,
                project_id,
                field_delimiter=',',
                print_header=True,
                file_encoding='SHIFT_JIS',
                file_newline='CRLF',
                location=None,
                upload_format='CSV',
                query_parameters=None,
                xcom_parameters=None,
                local=False,
                *args,
                **kwargs):
    """BigQueryに対してクエリを実行した結果をGoogleドライブに出力する

    Args:
        sql (str): SQLファイルのパス
        file_name (str): Googleドライブに出力するファイル名
        folder_id (str): Googleドライブの出力先フォルダID。
                         Noneの場合、parent_folder_id は必須。
        parent_folder_id (str): Googleドライブの出力先親フォルダID。
                                folder_id が指定されている場合は無視する。
        upload_path (str): parent_folder_id 以下のフォルダパス（'/'区切りで指定）。
                           folder_id が指定されている場合は無視する。
        project_id (str): プロジェクトID
        field_delimiter (str): ファイル区切り文字。 Defaults to ','.
        print_header (bool): ヘッダを出力するかどうか。 Defaults to True.
        file_encoding (str): 出力ファイルの文字コード(SHIFT_JIS,UTF8,UTF8SIGのみ). Defaults to 'SHIFT_JIS'.
        file_newline (str): 出力ファイルの改行コード(CRLF,LFのみ)。Defaults to 'CRLF'.
        location (str): 出力元BigQueryのデータセットのロケーション。
                        None の場合、東京リージョン。 Defaults to None.
        upload_format (str): アップロードフォーマット（CSV, XLSX）。
                             XLSXの場合file_encoding はSHIFT_JIS、file_newline はCRLF固定となる、file_name の拡張子には.xlsxを付けること。
                             Defaults to 'CSV'.
        query_parameters (dict): クエリパラメータ。. Defaults to None.
                                 SQLファイルに、「@hoge」「@hoge2」のように変数を設定することで、パラメータを設定できる。
                                 {'hoge': 'value', 'hoge2', 'value2'}のようにdictでパラメータを設定する。
                                 valueの部分は、str、int、floatのみ設定可能。これ以外の型の場合、
                                 クエリパラメータには設定されず、無視される。
        xcom_parameters (dict): クエリパラメータ。. Defaults to None.
                                「'task_id' : <タスクID>」と「'key' : <XCOMのキー>」のdist形式で指定で指定する。
                                XCOMから値を取得してクエリパラメータに追加する。
                                クエリパラメータに追加するキーは<XCOMのキー>とする。
        local (bool): ローカル環境で実行する場合はTrue. Defaults to False.
    """
    # GCSのローカルファイル名設定
    local_file = '/home/airflow/gcs/data/tmp/script/bq_to_google_drive/{}'.format(file_name)

    print('== [START] BigQueryに対してクエリを発行 ==')
    try:
        df = bq_hook.bq_query(
            sql=sql,
            project_id=project_id,
            query_parameters=query_parameters,
            xcom_parameters=xcom_parameters,
            is_return_result=True,
            return_type='df',
            location=location,
            local=local)
    except Exception as e:
        raise ValueError('クエリの実行に失敗しました。詳細({})'.format(e))
    print('== [END] BigQueryに対してクエリを発行 ==')

    try:
        # GCSに出力
        if upload_format == 'CSV':
            print('== [START] CSV出力 ==')
            # エンコードパラメータの設定
            if file_encoding == 'UTF8':
                prm_file_encoding = 'utf_8'
            elif file_encoding == 'UTF8SIG':
                prm_file_encoding = 'utf_8_sig'
            else:
                prm_file_encoding = 'shift_jis'
            # 改行コードパラメータの設定
            if file_newline == 'LF':
                prm_file_newline = '\n'
            else:
                prm_file_newline = '\r\n'
            df.to_csv(path_or_buf=local_file, sep=field_delimiter, index=False, header=print_header, encoding=prm_file_encoding, line_terminator=prm_file_newline)
            print('== [END] CSV出力 ==')
        elif upload_format == 'XLSX':
            print('== [START] Excel出力 ==')
            # xlsxwriterを使用してExcelファイルを作成（df.to_excel()だとサイズが大きい場合にメモリエラーとなりデーモン化（⇒突然死）してしまう）
            workbook = xlsxwriter.Workbook(local_file, {'constant_memory': True})
            worksheet = workbook.add_worksheet()
            # 行数だけ繰り返す
            for index_row, row_data in enumerate(df.itertuples(name=None)):
                # ヘッダ行を飛ばす
                if index_row == 0 and print_header:
                    continue
                for index_column, data in enumerate(row_data):
                    # index列を飛ばす
                    if index_column == 0:
                        continue
                    worksheet.write(index_row, index_column - 1, data)
            workbook.close()
            print('== [END] Excel出力 ==')
        else:
            raise ValueError('upload_formatパラメータにはCSVまたは、XLSXを指定してください。')

        print('== [START] Googleドライブのクライアントオブジェクトを生成・取得 ==')
        gdrive_client = __create_gdrive_client()
        print('== [END] Googleドライブのクライアントオブジェクトを生成・取得 ==')

        print('== [START] 出力先GoogleドライブのフォルダIDを取得 ==')
        if folder_id is None:
            # アップロードパスをフォルダ名のリストに整形（ツリー）
            folder_name = upload_path.split('/')
            pprint(folder_name)
            hierarchy_max = len(folder_name)
            hierarchy = 0
            target_id = parent_folder_id
            while hierarchy_max != 0:
                print('folder_name[' + str(hierarchy) + ']: ' + folder_name[hierarchy])
                if folder_name[hierarchy] != '':
                    # Googleドライブの指定フォルダID内に存在するフォルダの一覧を取得
                    folder_list = __list_folder(gdrive_client, target_id)
                    # 対象フォルダを探す
                    find = False
                    for folder in folder_list:
                        print("folder['title']: " + folder['title'])
                        if folder['title'] == folder_name[hierarchy]:
                            find = True
                            target_id = folder['id']
                            print('target_id: ' + target_id)
                            break
                    if not find:
                        # 存在しない場合は作成する
                        target_id = __create_folder(gdrive_client, target_id, folder_name[hierarchy])
                        print('target_id(create): ' + target_id)
                hierarchy = hierarchy + 1
                if hierarchy < hierarchy_max:
                    continue
                else:
                    folder_id = target_id
                    break
        if folder_id is None:
            raise ValueError('パラメータfolder_idを指定しない場合、parent_folder_id、upload_pathの指定は必須です。')
        print('出力先GoogleドライブのフォルダID： ' + folder_id)
        print('== [END] 出力先GoogleドライブのフォルダIDを取得 ==')

        print('== [START] Googleドライブのフォルダにアップロード ==')
        __upload_file(gdrive_client, local_file, folder_id, file_name)
        print('== [END] Googleドライブのフォルダにアップロード ==')
    finally:
        # GCSの一時ファイルを削除
        if os.path.exists(local_file):
            os.remove(local_file)


def __create_gdrive_client():
    print('★ __create_gdrive_client() START ★')

    gdrive_client = None
    count = 0
    while True:
        try:
            # Googleドライブのクライアントオブジェクトを生成
            gdrive_client = google_drive.GoogleDriveClient()
            break
        except Exception as e:
            count = count + 1
            if count > RETRY_COUNT:
                raise e
            time.sleep(RETRY_INTERVAL)

    print('★ __create_gdrive_client() END ★')
    return gdrive_client


def __list_folder(gdrive_client, target_id):
    print('★ __list_folder() START ★')

    count = 0
    while True:
        try:
            # Googleドライブの指定フォルダID内に存在するフォルダの一覧を取得
            folder_list = gdrive_client.list_folder(target_id)
            break
        except Exception as e:
            count = count + 1
            if count > RETRY_COUNT:
                raise e
            time.sleep(RETRY_INTERVAL)

    print('★ __list_folder() END ★')
    return folder_list


def __create_folder(gdrive_client, parent_folder_id, folder_name):
    print('★ __create_folder() START ★')

    count = 0
    while True:
        try:
            # Googleドライブの指定フォルダID下にフォルダを作成
            folder_id = gdrive_client.create_folder(parent_folder_id, folder_name)
            break
        except Exception as e:
            count = count + 1
            if count > RETRY_COUNT:
                raise e
            time.sleep(RETRY_INTERVAL)

    print('★ __create_folder() END ★')
    return folder_id


def __upload_file(gdrive_client, upload_file, folder_id, file_name):
    print('★ __upload_file() START ★')

    count = 0
    while True:
        try:
            # Googleドライブの指定フォルダID下にファイルをアップロード
            file_id = gdrive_client.upload_file(upload_file, folder_id, file_name)
            break
        except Exception as e:
            count = count + 1
            if count > RETRY_COUNT:
                raise e
            time.sleep(RETRY_INTERVAL)

    print('★ __upload_file() END ★')
    return file_id
