"""BigQueryからGoogleドライブにファイル連携する
"""
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from lib.hooks import bq_to_google_drive_hook


class BqToGoogleDriveOperator(PythonOperator):

    ui_color = '#FFB36B'
    @apply_defaults
    def __init__(
            self,
            source_project_dataset_table,
            file_name,
            folder_id,
            project_id=None,
            compression='NONE',
            export_format='CSV',
            field_delimiter=',',
            print_header=True,
            file_encoding='SHIFT_JIS',
            file_newline='CRLF',
            location=None,
            *args,
            **kwargs):
        """BigQueryのテーブルをGoogleドライブにファイル連携するOperator

        Args:
            source_project_dataset_table (str): 出力元BigQueryのプロジェクト、データセット、テーブルの文字列
            file_name (str): Googleドライブに出力するファイル名
            folder_id (str): Googleドライブの出力先フォルダID
            project_id (str): プロジェクトID。Defaults to None.
            compression (google.cloud.bigquery.job.Compression): 圧縮種別。 Defaults to 'NONE'.
            export_format (google.cloud.bigquery.job.DestinationFormat): 出力フォーマット。 Defaults to 'CSV'.
            field_delimiter (str): ファイル区切り文字。 Defaults to ','.
            print_header (bool): ヘッダを出力するかどうか。 Defaults to True.
            file_encoding (str): 出力ファイルの文字コード(SHIFT_JIS,UTF8のみ). Defaults to 'SHIFT_JIS'.
            file_newline (str): 出力ファイルの改行コード(CRLF,LFのみ). Defaults to 'CRLF'.
            location (str): 出力元BigQueryのデータセットのロケーション。Noneの場合、東京リージョン。 Defaults to None.
        """

        python_callable = bq_to_google_drive_hook.execute

        # プロジェクトIDがNoneの場合はAirflowのプロジェクトIDを設定
        if project_id is None:
            project_id = Variable.get('project_id')

        op_kwargs = {
            'source_project_dataset_table': source_project_dataset_table,
            'file_name': file_name,
            'folder_id': folder_id,
            'project_id': project_id,
            'compression': compression,
            'export_format': export_format,
            'field_delimiter': field_delimiter,
            'print_header': print_header,
            'file_encoding': file_encoding,
            'location': location,
        }

        super(BqToGoogleDriveOperator, self).__init__(python_callable=python_callable,
                                                      op_kwargs=op_kwargs,
                                                      *args,
                                                      **kwargs)
