"""BigQueryのデータをピボットテーブルとしてGoogleドライブにファイル出力する。
"""
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from lib.hooks import bq_pivot_to_google_drive_hook


class BqPivotToGoogleDriveOperator(PythonOperator):

    ui_color = '#6FB07C'
    @apply_defaults
    def __init__(
            self,
            sql,
            file_name_prefix,
            google_drive_folder_id,
            project_id=None,
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
        データソースはSQLで実行して得た結果とし、
        そのデータをpivot_index、pivot_columns、pivot_valuesの値を元にピボットテーブル化する。
        整形後のデータをCSVファイルとしてGoogleドライブに出力する。

        Args:
            sql (str): SQLファイルパス
            file_name_prefix (str): Googleドライブに出力するファイル名サフィックス（このファイル名に.csvが付与される）
            google_drive_folder_id (str): Googleドライブの出力先フォルダID
            project_id (str): プロジェクトID。Noneの場合はAirflowのプロジェクトIDを設定. Defaults to None.
            pivot_index (list): ピボットテーブルのインデックスのリスト.
            pivot_columns (list): ピボットテーブルのカラムのリスト.
            pivot_values (list): ピボットテーブルのインデックスのリスト.
            pivot_sort (list): ピボットテーブルのソートするカラムのリスト.全て昇順ソートとする。集計項目名でソートしたい場合はvalueを指定。
            query_parameters (dict, optional): BigQueryのクエリパラメータ. Defaults to None.
            bq_max_return_records (int, optional): BigQueryのクエリで参照する最大レコード数. Defaults to 500000.
            file_encoding (str, optional): 出力ファイルの文字コード(SHIFT_JIS,UTF8,UTF8SIGのみ). Defaults to 'utf_8_sig'.
            opt_google_drive_file_date_suffix(boolean, optional):Googleドライブに同名のファイルが既にあった場合、
                作成日が当日であれば上書き、前日以前であれば作成日のサフィックスをつけて退避する。. Defaults to False.

        Example:
            インデックスとなる項目（GROUP BYされる項目）
            pivot_index=[
                {'name': '都道府県', 'column': 'prefecture'},
                {'name': '市区町村', 'column': 'city'},
            ]
            横軸となる項目
            pivot_columns'=['date']
            集計項目（
            pivot_values=[
                {'name': 'オーダー数', 'column': 'order_count'},
                {'name': '求人数', 'column': 'job_offer_count'},
                {'name': '募集人数', 'column': 'recruiting_count'},
            ]
            ソート項目
            pivot_sort=['prefecture', 'values']
        """
        python_callable = bq_pivot_to_google_drive_hook.execute

        # プロジェクトIDがNoneの場合はAirflowのプロジェクトIDを設定
        if project_id is None:
            project_id = Variable.get('project_id')

        op_kwargs = {
            'sql': sql,
            'file_name_prefix': file_name_prefix,
            'google_drive_folder_id': google_drive_folder_id,
            'project_id': project_id,
            'pivot_index': pivot_index,
            'pivot_columns': pivot_columns,
            'pivot_values': pivot_values,
            'pivot_sort': pivot_sort,
            'query_parameters': query_parameters,
            'bq_max_return_records': bq_max_return_records,
            'file_encoding': file_encoding,
            'opt_google_drive_file_date_suffix': opt_google_drive_file_date_suffix,
        }

        super(BqPivotToGoogleDriveOperator, self).__init__(python_callable=python_callable,
                                                           op_kwargs=op_kwargs,
                                                           *args,
                                                           **kwargs)
