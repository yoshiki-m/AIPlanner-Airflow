"""SFのレポートを取得し、BigQueryにロードする。
"""
import re

from lib.utils.salesforce import SalesforceClient
from lib.utils.cloud_storage import CloudStorageClient
from lib.hooks import bq_hook as bq


def execute(sf_report_id,
            destination_dataset_table,
            sf_environment='sandbox',
            project_id='dev-jinzaisystem-tool',
            target_partition=None,
            write_disposition='WRITE_TRUNCATE',
            location='asia-northeast1',
            allow_no_record=False,
            **kwargs):
    """SFのレポートを取得し、BigQueryにロードする。

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
    # 引数チェック destination_dataset_table
    if re.match('.*\\..*', destination_dataset_table):
        lst = destination_dataset_table.split('.')
        dataset_id = lst[0]
        table_id = lst[1]
    else:
        ValueError('destination_dataset_tableの値が不正です。')

    # 引数チェック target_partition
    if target_partition is None:
        gcs_file_path = '{}/{}.csv'.format(dataset_id, table_id)
        bq_table = '{}.{}'.format(dataset_id, table_id)
    else:
        gcs_file_path = '{}/{}_{}.csv'.format(dataset_id, table_id, target_partition)
        bq_table = '{}.{}${}'.format(dataset_id, table_id, target_partition)

    # SFクライアント作成
    sfc = SalesforceClient(environ=sf_environment)
    # レポートIDを指定し、csvファイル出力
    local_csv_file = '/home/airflow/gcs/data/tmp/script/salesforce_report_to_bigquery/{}_{}.csv'.format(dataset_id, table_id)
    sfc.export_report_csv(sf_report_id, local_csv_file)

    # 出力したCSVファイルが0件の場合
    if len(open(local_csv_file).readlines()) == 1:
        if allow_no_record:
            # 正常終了
            return
        else:
            # 異常終了
            raise Exception('取得したレポートの結果が0件でした。')

    # GCSクライアントのインスタンス生成
    gcs_bucket_name = 'gs://{}-sf-report'.format(project_id)
    cs = CloudStorageClient(project_id=project_id)

    # ローカルファイルをGCSにアップロード
    cs.upload(gcs_bucket_name, local_csv_file, gcs_file_path)

    # BigQueryにロードする
    bq.bq_load('{}/{}'.format(gcs_bucket_name, gcs_file_path),
               bq_table,
               project_id=project_id,
               location=location,
               write_disposition=write_disposition,
               retry=3)
