"""BigQueryに対してクエリを実行した結果をGoogleドライブにファイル連携する
"""
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from lib.hooks import bq_to_google_drive_hook


class BqQueryToGoogleDriveOperator(PythonOperator):

    ui_color = '#FFB36B'
    @apply_defaults
    def __init__(
            self,
            sql,
            file_name,
            folder_id=None,
            parent_folder_id=None,
            upload_path=None,
            project_id=None,
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
        """BigQueryのテーブルをGoogleドライブにファイル連携するOperator

        Args:
            sql (str): SQLファイルのパス
            file_name (str): Googleドライブに出力するファイル名
            folder_id (str): Googleドライブの出力先フォルダID。
                             Noneの場合、parent_folder_id は必須。 Defaults to None.
            parent_folder_id (str): Googleドライブの出力先親フォルダID。
                                    folder_id が指定されている場合は無視する。 Defaults to None.
            upload_path (str): parent_folder_id 以下のフォルダパス（'/'区切りで指定）。
                               folder_id が指定されている場合は無視する。 Defaults to 'NONE'.
            project_id (str): プロジェクトID。Defaults to None.
            field_delimiter (str): ファイル区切り文字。 Defaults to ','.
            print_header (bool): ヘッダを出力するかどうか。 Defaults to True.
            file_encoding (str): 出力ファイルの文字コード(SHIFT_JIS,UTF8のみ). Defaults to 'SHIFT_JIS'.
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

        # プロジェクトIDがNoneの場合はAirflowのプロジェクトIDを設定
        if project_id is None:
            project_id = Variable.get('project_id')

        # BigQueryに対してクエリを実行した結果をGoogleドライブに出力する
        python_callable = bq_to_google_drive_hook.execute_sql
        op_kwargs = {
            'sql': sql,
            'file_name': file_name,
            'folder_id': folder_id,
            'parent_folder_id': parent_folder_id,
            'upload_path': upload_path,
            'project_id': project_id,
            'field_delimiter': field_delimiter,
            'print_header': print_header,
            'file_encoding': file_encoding,
            'file_newline': file_newline,
            'location': location,
            'upload_format': upload_format,
            'query_parameters': query_parameters,
            'xcom_parameters': xcom_parameters,
            'local': local
        }

        super(BqQueryToGoogleDriveOperator, self).__init__(python_callable=python_callable,
                                                           op_kwargs=op_kwargs,
                                                           *args,
                                                           **kwargs)
