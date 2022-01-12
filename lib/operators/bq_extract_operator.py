"""BigQueryToCloudStorageOperatorが東京リージョンだと動かないので、
GCSの出力の実行はBqExtractOperatorを実行する。
"""
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from lib.hooks import bq_hook


class BqExtractOperator(PythonOperator):

    ui_color = '#D6F9FF'
    @apply_defaults
    def __init__(
            self,
            source_project_dataset_table,
            destination_cloud_storage_uris,
            project_id=None,
            compression='NONE',
            export_format='CSV',
            field_delimiter=',',
            print_header=True,
            location=None,
            *args,
            **kwargs):
        """BigQueryのテーブルをGCSに出力するOperator

        Args:
            source_project_dataset_table (str): 出力元BigQueryのプロジェクト、データセット、テーブルの文字列
            destination_cloud_storage_uris (str): 出力先GCSのURI
            project_id (str): プロジェクトID。Defaults to None.
            compression (google.cloud.bigquery.job.Compression): 圧縮種別。 Defaults to 'NONE'.
            export_format (google.cloud.bigquery.job.DestinationFormat): 出力フォーマット。 Defaults to 'CSV'.
            field_delimiter (str): ファイル区切り文字。 Defaults to ','.
            print_header (bool): ヘッダを出力するかどうか。 Defaults to True.
            location (str): 出力元BigQueryのデータセットのロケーション。Noneの場合、東京リージョン。 Defaults to None.
        """

        python_callable = bq_hook.bq_extract

        # プロジェクトIDがNoneの場合はAirflowのプロジェクトIDを設定
        if project_id is None:
            project_id = Variable.get('project_id')

        op_kwargs = {
            'source_project_dataset_table': source_project_dataset_table,
            'destination_cloud_storage_uris': destination_cloud_storage_uris,
            'project_id': project_id,
            'compression': compression,
            'export_format': export_format,
            'field_delimiter': field_delimiter,
            'print_header': print_header,
            'location': location,
        }

        super(BqExtractOperator, self).__init__(python_callable=python_callable,
                                                op_kwargs=op_kwargs,
                                                *args,
                                                **kwargs)
