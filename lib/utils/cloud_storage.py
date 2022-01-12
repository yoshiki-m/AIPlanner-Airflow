import time
from google.cloud import storage, exceptions
from google.oauth2 import service_account
from lib.errors import exception

# サービスアカウント認証ファイル
CREDENTIALS_FILE = 'config/jinzaisystem-tool-composer.json'
# AirflowのDAGディレクトリ
AIRFLOW_DAGS_DIR = '/home/airflow/gcs/dags'
# 排他制御用ロックファイルの拡張子
LOCK_FILE_EXTENSION = '.lock'
# 排他ロックの取得を試行する間隔（秒）
LOCK_GET_INTERVAL = 5


class CloudStorageClient:

    def __init__(self,
                 project_id=None,
                 local=False):
        """Google Cloud Storageを操作するクライアント

        Args:
            project_id (str): プロジェクトID。 Defaults to None.
            local (bool): ローカル開発環境かどうか。 Defaults to False.
        """
        if project_id is None:
            project_id = 'jinzaisystem-tool'
        self.project_id = project_id
        self.local = local
        self.client = self.__get_client(self.project_id, local=self.local)

    def __get_client(self,
                     project_id,
                     local=False):
        """BigQueryクライアントインスタンスを取得する。

        Args:
            project_id (str): BigQueryのジョブを実行するプロジェクト。
            local (bool): ローカル環境で実行する場合はTrue. Defaults to False.

        Returns:
            bigquery.Client
        """
        if local:
            credentials_file_path = CREDENTIALS_FILE
        else:
            credentials_file_path = '{}/{}'.format(AIRFLOW_DAGS_DIR, CREDENTIALS_FILE)

        # サービスアカウント認証
        credentials = service_account.Credentials.from_service_account_file(
            credentials_file_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        # ストレージクライアント作成
        client = storage.Client(project=project_id, credentials=credentials)
        return client

    def download(self,
                 bucket_name,
                 file_name,
                 local_download_filepath):
        """GCSのファイルをローカルにダウンロードする。

        Args:
            bucket_name (str): バケット名。gs://hogehoge
            file_name (str): GCS上のファイル名。
            local_download_filepath (str): ローカルのファイルパス
        Raises:
            google.cloud.exceptions.NotFound: 指定ファイルが見つからなかった
        """
        bucket = self.client.get_bucket(bucket_name.replace('gs://', ''))
        blob = storage.Blob(file_name, bucket)
        blob.download_to_filename(local_download_filepath)

    def upload(self,
               bucket_name,
               local_upload_filepath,
               file_name):
        """ローカルのファイルをGCSへアップロードする。

        Args:
            bucket_name (str): バケット名。gs://hogehoge
            local_upload_filepath (str): ローカルのファイルパス
            file_name (str): GCS上のファイル名。
        """
        bucket = self.client.get_bucket(bucket_name.replace('gs://', ''))
        blob = bucket.blob(file_name)
        blob.upload_from_filename(filename=local_upload_filepath)

    def delete(self,
               bucket_name,
               file_name):
        """指定ファイルを削除する。

        Args:
            bucket_name (str): バケット名。gs://hogehoge
            file_name (str): GCS上のファイル名
        Raises:
            google.cloud.exceptions.NotFound: 指定ファイルが見つからなかった
        """
        bucket = self.client.get_bucket(bucket_name.replace('gs://', ''))
        blob = bucket.blob(file_name)
        blob.delete()

    def copy(self,
             bucket_name,
             file_name,
             destination_bucket_name,
             destination_file_name):
        """GCS上のファイルをコピーする

        Args:
            bucket_name (str): バケット名。gs://hogehoge or hogehoge
            file_name (str): GCS上のファイル名
            destination_bucket_name (str): バケット名。gs://hogehoge or hogehoge
            destination_file_name (str): GCS上のファイル名
        """
        bucket = self.client.get_bucket(bucket_name.replace('gs://', ''))
        destination_bucket = self.client.get_bucket(destination_bucket_name.replace('gs://', ''))
        blob = bucket.blob(file_name)
        bucket.copy_blob(
            blob, destination_bucket, destination_file_name
        )

    def get_file_lock(self,
                      bucket_name,
                      file_full_name,
                      local_work_path,
                      time_up_sec=0):
        """指定ファイルに対する排他ロックを取得する（取得できるまで待ち続ける）。
           ロックする期間はできるだけ短い実装を心掛けること。
           ※取得した排他ロックはrelease_file_lock()をコールしてプロセス終了前に必ず解除してください。

        Args:
            bucket_name (str): バケット名。gs://hogehoge
            file_full_name (str): 排他ロックを取得したいファイル名（バケット下のフォルダ名も含む）AUTO/fuga.txt
            local_work_path (str): 作業用のローカルフォルダ（末尾に'/'まで含むこと）
            time_up_sec (int): 取得を諦める目安秒数（0以下の場合は無限に待ち続ける）
        Raises:
            lib.errors.exception.GcsFileLockException: 指定時間内に排他ロックの取得に失敗
        """
        # リトライする回数を計算（0の場合は無限）
        retry_max = 0
        if time_up_sec > 0:
            # 切り上げ
            retry_max = -(-time_up_sec // LOCK_GET_INTERVAL)
        # 現在のリトライ回数
        retry_count = 0

        # 排他ロック用のファイル名
        file_name = file_full_name
        file_name_list = file_full_name.split('/')
        if len(file_name_list) > 1:
            file_name = file_name_list[len(file_name_list) - 1]

        # ローカルの排他ロック用ファイル
        local_lock_file = local_work_path + file_name + LOCK_FILE_EXTENSION
        # GCSの排他ロック用ファイル
        gcs_lock_file = file_full_name + LOCK_FILE_EXTENSION
        # 排他ロックが取得できるまで繰り返す
        while True:
            try:
                # 排他制御用ロックファイルをダウンロード
                self.download(bucket_name, gcs_lock_file, local_lock_file)
            except Exception:
                print('排他制御用ロックファイルが見つからなかった。')
                try:
                    # ローカルにロックファイルを作成
                    with open(local_lock_file, "w"):
                        pass
                    # ロックファイルをGCSへアップロード
                    self.upload(bucket_name, local_lock_file, gcs_lock_file)
                except Exception:
                    # ロックファイルのアップロードに失敗したので抜けない
                    print('ロックファイルのアップロードに失敗。retry_count={}'.format(retry_count))
                else:
                    # 排他ロックが取得できたので抜ける
                    break
            # リトライ回数を加算
            retry_count += 1
            if retry_max != 0 and retry_count > retry_max:
                # リトライ回数内に取得できなかったので例外を投げる
                raise exception.GcsFileLockException('排他ロックの取得に失敗しました。ファイル名={}'.format(gcs_lock_file))
            # しばらく待った後に再度確認する
            time.sleep(LOCK_GET_INTERVAL)

    def release_file_lock(self,
                          bucket_name,
                          file_full_name):
        """指定ファイルに対する排他ロックを解除する。
           ※自身が取得した排他ロックのみ解除してください。

        Args:
            bucket_name (str): バケット名。gs://hogehoge
            file_full_name (str): 排他ロックを解除したいファイル名（バケット下のフォルダ名も含む）AUTO/fuga.txt
        """
        # GCSの排他ロック用ファイル
        gcs_lock_file = file_full_name + LOCK_FILE_EXTENSION
        try:
            # 排他制御用ロックファイルを削除する
            self.delete(bucket_name, gcs_lock_file)
        except exceptions.NotFound:
            print('排他制御用のロックファイルは見つかりませんでした。ファイル名={}'.format(gcs_lock_file))

    def exists(self,
               bucket_name,
               file_name):
        """GCSのファイルが存在するかどうか。

        Args:
            bucket_name (str): バケット名。gs://hogehoge
            file_name (str): GCS上のファイル名。
        Returns:
            true / false
        """
        bucket = self.client.get_bucket(bucket_name.replace('gs://', ''))
        blob = storage.Blob(file_name, bucket)
        return blob.exists()
