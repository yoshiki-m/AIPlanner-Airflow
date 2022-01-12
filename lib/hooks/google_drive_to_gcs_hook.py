"""GoogleドライブからGCSにファイルコピーする
"""
import os
import time
from airflow.models import Variable
from lib.logger import logger
from lib.utils.google_drive import GoogleDriveClient
from lib.utils.cloud_storage import CloudStorageClient

IS_LOCAL = True
# リトライ回数
RETRY_COUNT = 3
# リトライ間隔（秒）
RETRY_INTERVAL = 10


def get_folder_list(project_id,
                    gdrive_folder_id,
                    is_local=False
                    ):
    """指定したfolder_id直下に存在するフォルダのidをリストで取得

    Args:
        project_id (str): プロジェクトid
        gdrive_folder_id (str): 同期対象のGドライブフォルダid
        is_local (bool, optional): ローカル実行? Defaults to False.

    Returns:
        [list]: フォルダのidリスト
    """
    count = 0
    is_success = False
    while not is_success:
        try:
            gdrive_client = GoogleDriveClient(local=is_local)
            folder_list = gdrive_client.list_folder(gdrive_folder_id)
            is_success = True
            print('get_folder_list(): finished')
        except Exception as e:
            count = count + 1
            if count > RETRY_COUNT:
                logger.error('フォルダリストの取得に失敗しました。')
                raise e
            time.sleep(RETRY_INTERVAL)

    folder_id_list = []
    for folder in folder_list:
        print(folder['title'])
        folder_id_list.append(folder['id'])

    return folder_id_list


def copy_all_files(project_id,
                   gdrive_folder_id,
                   gcs_bucket_name,
                   gcs_folder,
                   local_download_dir,
                   is_local=False
                   ):
    """指定したフォルダ直下のファイルを全部GCSにコピーする

    Args:
        project_id (str): プロジェクトid
        gdrive_folder_id (str): 同期対象のGドライブフォルダid
        gcs_bucket_name (str): アップロード先GCSバケット
        gcs_folder (str): アップロード先GCSフォルダ
        local_download_dir: ダウンロード用tmpフォルダ
        is_local (bool, optional): ローカル実行? Defaults to False.
    Returns:
        result (dict): 処理結果
    """

    # 指定フォルダ内のファイルリスト取得
    error_count = 0
    is_success = False
    while not is_success:
        try:
            gdrive_client = GoogleDriveClient(local=is_local)
            file_list = gdrive_client.list_file(gdrive_folder_id)
            is_success = True

        except Exception as e:
            print(e.args)
            error_count = error_count + 1
            if error_count > RETRY_COUNT:
                logger.error('ファイルリストの取得に失敗しました。')
                raise e
            time.sleep(RETRY_INTERVAL)

    # ファイルコピー
    error_file_list = []
    error_count = 0
    for file in file_list:
        try:
            if file['mimeType'] == 'application/vnd.google-apps.folder':
                # フォルダはコピー対象外
                continue

            file_name = file['title']
            copy(project_id,
                 gdrive_folder_id,
                 file_name,
                 gcs_bucket_name,
                 gcs_folder,
                 local_download_dir,
                 gdrive_client,
                 is_local
                 )
        except Exception as e:
            # エラーは無視してループ続行
            print(e.args)
            error_count = error_count + 1
            error_file_list.append(file_name)
            continue

    if error_count > 0:
        raise Exception(
            "gdirve_id: {} へのアップロードが {}件失敗しました。ファイル: {}".format(
                gdrive_folder_id,
                error_count,
                error_file_list))


def copy(project_id,
         gdrive_folder_id,
         gdrive_file_name,
         gcs_bucket_name,
         gcs_folder,
         local_download_dir,
         gdrive_client=None,
         is_local=False
         ):
    """対象ファイルをGドライブからDLして、指定のGCSにアップロードする (1ファイルごと)

    Args:
        project_id (str): プロジェクトid
        gdrive_folder_id (str): 同期対象のGドライブフォルダid
        gdrive_file_name (str): Gドライブファイル名
        gcs_bucket_name (str): アップロード先GCSバケット
        gcs_folder (str): アップロード先GCSフォルダ
        local_download_dir: ダウンロード用tmpフォルダ
        gdrive_client (object), optional): gdrive_client. Defaults to None.
        is_local (bool, optional): ローカル実行? Defaults to False.

    Raises:
        e: gdrive or GCS での処理エラー
    """

    print('copy() start> ' + gdrive_file_name, local_download_dir)
    if not gdrive_client:
        gdrive_client = GoogleDriveClient(local=is_local)

    count = 0
    is_success = False
    while not is_success:
        try:
            # GドライブからDL
            gdrive_client.download_files(
                gdrive_folder_id,
                gdrive_file_name,
                local_download_dir,
                query_operator='equals')

            # GCSにアップロード
            local_file_path = local_download_dir + '/' + gdrive_file_name
            gcs = CloudStorageClient(project_id=project_id, local=is_local)
            gcs.upload(
                gcs_bucket_name,
                local_file_path,
                gcs_folder + '/' + gdrive_file_name)
            print('upload success > ' + gdrive_file_name)
            is_success = True

        except Exception as e:
            count = count + 1
            print("例外> ", e.args)
            if count > RETRY_COUNT:
                logger.error('{}のコピーが失敗しました。'.format(gdrive_file_name))
                raise e
            time.sleep(RETRY_INTERVAL)

        finally:
            # tmpファイル削除
            if os.path.isfile:
                os.remove(local_file_path)
