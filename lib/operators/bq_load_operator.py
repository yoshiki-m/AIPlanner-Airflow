"""GoogleCloudStorageToBigQueryOperatorが東京リージョンだと動かないので、
SQLの実行はBqQueryOperatorを実行する。
"""
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from lib.hooks import bq_hook


class BqLoadOperator(PythonOperator):

    ui_color = '#6DC1FF'
    @apply_defaults
    def __init__(
            self,
            source_cloud_storage_uris,
            destination_project_dataset_table,
            project_id=None,
            location=None,
            skip_leading_rows=1,
            write_disposition=None,
            retry=0,
            field_delimiter=',',
            quote_character=None,
            *args,
            **kwargs):
        """GCSのファイルをBigQueryにロードするOperator

        Args:
            source_cloud_storage_uris (str): 出力元GCSのURI
            destination_project_dataset_table (str): 出力先Bigqueryのジロジェクト、データセット、テーブルの文字列
            project_id (str): プロジェクトID。Defaults to None.
            location (str): 出力元BigQueryのデータセットのロケーション。Noneの場合、東京リージョン。 Defaults to None.
            skip_leading_rows (int): スキップ行数。 Defaults to 1.
            write_disposition (google.cloud.bigquery.job.WriteDisposition): テーブルの書き込みオプション。
                              Noneの場合、WRITE_EMPTY。 Defaults to None.
            retry (int): リトライ回数。 Defaults to 0.
            quote_character (str): 区切り文字。Noneの場合、ダブルクォーテーション。 Defaults to None.
        """
        python_callable = bq_hook.bq_load

        # プロジェクトIDがNoneの場合はAirflowのプロジェクトIDを設定
        if project_id is None:
            project_id = Variable.get('project_id')

        op_kwargs = {
            'source_cloud_storage_uris': source_cloud_storage_uris,
            'destination_project_dataset_table': destination_project_dataset_table,
            'project_id': project_id,
            'location': location,
            'skip_leading_rows': skip_leading_rows,
            'write_disposition': write_disposition,
            'retry': retry,
            'field_delimiter': field_delimiter,
            'quote_character': quote_character
        }

        super(BqLoadOperator, self).__init__(python_callable=python_callable,
                                             op_kwargs=op_kwargs,
                                             *args,
                                             **kwargs)
                                             
