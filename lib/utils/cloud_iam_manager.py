import csv
from datetime import datetime
import json
import re

from googleapiclient import discovery
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import pandas as pd

from lib.utils import datetime_util, cloud_storage


class CloudIamManager:
    """GCPの権限管理を取得するクラス
    """
    def __init__(self, credential_file):

        # サービスアカウント認証
        self.credentials = Credentials.from_service_account_file(
            credential_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"])

        # リソースマネージャーのサービスインスタンスを生成
        self.resource_manager_service = self.__get_service('cloudresourcemanager')

        # IAMのサービスインスタンスを生成
        self.iam_service = self.__get_service('iam')

        # 権限一覧をBigQueryに出力する時のクライアント、設定項目
        self.common_project_id = 'common-jinzaisystem-tool'
        self.common_dataset_id = 'iam'
        self.partitiontime = datetime_util.today_jst.strftime('%Y%m%d')
        self.common_bq_client = bigquery.Client(project=self.common_project_id,
                                                credentials=self.credentials)
        self.bq_config = bigquery.LoadJobConfig()
        self.bq_config.write_disposition = 'WRITE_TRUNCATE'
        self.bq_config.skip_leading_rows = 1

    def __get_service(self, name):
        """APIのサービスインスタンスを作成する。

        Args:
            name (str): APIサービス名

        Returns:
            serviceインスタンス
        """
        service = discovery.build(name, 'v1', credentials=self.credentials)
        return service

    def list_project(self):
        """プロジェクトIDのリストを取得する

        Returns:
            list
        """
        result = self.resource_manager_service.projects().list().execute()
        return_list = []
        for x in result['projects']:
            return_list.append(x['projectId'])
        return return_list

    def list_iam(self, project_id):
        """プロジェクトIAMを取得する

        Args:
            project_id (str): プロジェクトID

        Returns:
            list(dict): IAMリスト
        """
        policy = self.resource_manager_service.projects().getIamPolicy(resource=project_id, body={}).execute()
        return_list = []
        timestamp = datetime.now(datetime_util.JST).strftime('%Y-%m-%d %H:%M:%S')
        for x in policy['bindings']:
            for y in x['members']:
                dic = {}
                dic['project_id'] = project_id
                dic['account_type'] = re.sub(':.*', '', y)
                dic['account'] = re.sub('.*:', '', y)
                dic['role'] = re.sub(r'roles\/', '', x['role'])
                dic['timestamp'] = timestamp
                return_list.append(dic)
        return return_list

    def list_service_account(self, project_id):
        """サービスアカウントの一覧を取得する

        Args:
            project_id (str): プロジェクトID

        Returns:
            list(dict): IAMリスト
        """
        response = self.iam_service.projects().serviceAccounts().list(
            name='projects/{}'.format(project_id)).execute()
        return_list = []
        timestamp = datetime.now(datetime_util.JST).strftime('%Y-%m-%d %H:%M:%S')
        for x in response['accounts']:
            dic = {}
            dic['project_id'] = project_id
            try:
                dic['display_name'] = x['displayName']
            except KeyError:
                dic['display_name'] = ''
            try:
                dic['description'] = x['description']
            except KeyError:
                dic['description'] = ''
            dic['email'] = x['email']
            dic['timestamp'] = timestamp
            return_list.append(dic)
        return return_list

    def list_bq_authority(self, project_id):
        """BigQueryのデータセット毎の権限を取得

        Args:
            project_id (str): プロジェクトID

        Returns:
            list(dict): IAMリスト
        """
        client = bigquery.Client(project=project_id, credentials=self.credentials)
        return_list = []
        timestamp = datetime.now(datetime_util.JST).strftime('%Y-%m-%d %H:%M:%S')
        for dataset in client.list_datasets():
            dataset = client.get_dataset('{}.{}'.format(project_id,
                                                        dataset.dataset_id))
            for x in dataset.access_entries:
                dic = {}
                dic['project_id'] = project_id
                dic['dataset_id'] = dataset.dataset_id
                dic['entity_type'] = x.entity_type
                dic['entity_id'] = x.entity_id
                dic['role'] = x.role
                dic['timestamp'] = timestamp
                return_list.append(dic)
        return return_list

    def bq_load(self, uri, table_id):
        """BigQueryにdictのlistをロードする

        Args:
            dict_list (list(dict)): ロードするデータ。
            table_id (str): ロード先BigQueryのテーブルID。作成済み、取り込み時間分割テーブルであること。
        """

        # configの作成 関数が存在しない。
        print(uri)
        job = self.common_bq_client.load_table_from_uri(source_uris=uri,
                                                        destination='iam.{}${}'.format(table_id,
                                                                                 self.partitiontime),
                                                        job_config=self.bq_config)
        job.result()
        print('ロードジョブ結果：{}'.format(job.state))


def load_iam(table_id):
    """PythonOperatorで実行する関数。権限一覧をBigQueryにロードする。

    Args:
        table_id (): ロード先テーブル

    Raises:
        ValueError: 引数が不正な場合
    """
    manager = CloudIamManager('/home/airflow/gcs/dags/config/jinzaisystem-tool-iam-manager.json')
    local_file = '/var/tmp/{}.csv'.format(table_id)
    record = []

    # データ取得
    for project_id in manager.list_project():
        if table_id == 'project':
            record.extend(manager.list_iam(project_id))
        elif table_id == 'service_account':
            record.extend(manager.list_service_account(project_id))
        elif table_id == 'bigquery_dataset':
            record.extend(manager.list_bq_authority(project_id))
        else:
            raise ValueError('引数が不正です。')

    # CSVファイル出力
    df = pd.DataFrame(record)
    df.to_csv(local_file, columns=record[0].keys(), index=False)

    # # GCSにアップロード
    backet = 'gs://common-jinzaisystem-tool-bq-load'
    gcs_file_name = '{}_{}.json'.format(table_id, manager.partitiontime)
    gcs = cloud_storage.CloudStorageClient(project_id='common-jinzaisystem-tool')
    gcs.upload(backet, local_file, gcs_file_name)

    # # BigQueryにロード
    manager.bq_load('{}/{}'.format(backet, gcs_file_name), table_id)
