"""SFのレポートを取得し、BigQueryにロードする。
"""
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from lib.hooks import salesforce_report_to_bigquery_hook as sfr


class SalesforceReportToBigqueryOperator(PythonOperator):

    ui_color = '#F5CFFF'
    @apply_defaults
    def __init__(
            self,
            sf_report_id,
            destination_dataset_table,
            sf_environment='sandbox',
            project_id=None,
            target_partition=None,
            write_disposition='WRITE_TRUNCATE',
            location='asia-northeast1',
            allow_no_record=False,
            *args,
            **kwargs):
        """SFのレポートを取得し、BigQueryにロードするOperator

        Args:
            sf_report_id (str): SalesforceのレポートID
            destination_dataset_table (str): BigQueryのロード先テーブル。'データセット.テーブル'とすること。
            sf_environment (str): Salesforceの環境名。 Defaults to 'sandbox'
            project_id (str): プロジェクトID。Defaults to dev-jinzaisystem-tool.
            target_partition (str): BigQueryがパーティションテーブルの場合、YYYYMMDDで日付を設定。
                                    パーティションテーブルでない場合はNoneにする。 Defaults to None.
            write_disposition (google.cloud.bigquery.job.WriteDisposition): 保存先テーブルの書き込み方法. Defaults to 'WRITE_EMPTY'.
                                                                            'WRITE_EMPTY','WRITE_APPEND','WRITE_TRUNCATE'のいずれか。
            location (str): 出力元BigQueryのデータセットのロケーション。 Defaults to asis-northeast1.
            allow_no_record (bool, optional): SFレポートの結果が0件の場合、Trueなら何もせずに正常終了、Falseならエラー. Defaults to False.
        """

        python_callable = sfr.execute

        # プロジェクトIDがNoneの場合はAirflowのプロジェクトIDを設定
        if project_id is None:
            project_id = Variable.get('project_id')

        op_kwargs = {
            'sf_report_id': sf_report_id,
            'destination_dataset_table': destination_dataset_table,
            'sf_environment': sf_environment,
            'project_id': project_id,
            'target_partition': target_partition,
            'write_disposition': write_disposition,
            'location': location,
            'allow_no_record': allow_no_record,
        }

        super(SalesforceReportToBigqueryOperator, self).__init__(python_callable=python_callable,
                                                                 op_kwargs=op_kwargs,
                                                                 *args,
                                                                 **kwargs)
