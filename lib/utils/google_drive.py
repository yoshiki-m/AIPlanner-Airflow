"""Googleドライブに接続するモジュール
pydriveがうまく動作していないため、
lib.utils.pydrive_bug_fixにライブラリを書き換えたモジュールを配置。

GoogleAPIのクレデンシャルファイルは、ローカルで実行するときは、以下のファイル名とする。
config/google_drive.json
"""
import re

from airflow.models import Variable
from googleapiclient.errors import HttpError

from lib.utils.pydrive_bug_fix.auth import GoogleAuth
from lib.utils.pydrive_bug_fix.drive import GoogleDrive
from lib.utils.pydrive_bug_fix.files import FileNotDownloadableError


class GoogleDriveClient:
    """Googleドライブに接続するクライアント
    """

    def __init__(self,
                 local=False,
                 supportsTeamDrives=True):
        self.local = local
        self.drive = self.__get_drive_instance()
        self.supportsTeamDrives = True
        self.includeTeamDriveItems = True

        if supportsTeamDrives is False:
            self.supportsTeamDrives = False
            self.includeTeamDriveItems = False

    def __get_drive_instance(self):
        """認証処理

        Returns:
            GoogleDrive: GoogleDriveオブジェクト
        """
        if self.local:
            GOOGLE_API_CREDENTIALS_FILE = 'config/google_drive.json'
        else:
            GOOGLE_API_CREDENTIALS_FILE = Variable.get('google_drive_api_credentials_file')
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile(GOOGLE_API_CREDENTIALS_FILE)
        if gauth.credentials is None or gauth.access_token_expired:
            if gauth.credentials is None:
                # 認証済みファイルがない場合
                gauth.CommandLineAuth()
            elif gauth.access_token_expired:
                # 認証情報が期限切れの場合
                gauth.Refresh()
            # 認証情報をローカルに保存
            gauth.SaveCredentialsFile(GOOGLE_API_CREDENTIALS_FILE)
        else:
            # 認証情報が再利用可能な場合
            gauth.Authorize()
        drive = GoogleDrive(gauth)
        return drive

    def __get_file_list(self,
                        folder_id,
                        file_name,
                        max_number_of_file=30,
                        query_operator='contains'):
        """[Googleドライブの指定フォルダID内にあるファイルを前方一致で検索。ファイル一覧を取得する。

        Args:
            folder_id (str): GoogleドライブのフォルダID
            file_name (str): Googleドライブのファイル名
            max_number_of_file (int, optional): 最大取得ファイル数. Defaults to 30.
            query_operator (str, optional): 部分一致か完全一致か。以下の値のいずれか。. Defaults to 'contains'.
                contains: 部分一致。The content of one string is present in the other.
                equals: 完全一致。The content of a string or boolean is equal to the other.
        Raises:
            ValueError: 引数のフォルダIDが誤っている
            FileNotFoundError: 指定フォルダ内にファイルが存在しない
            googleapiclient.errors import HttpError: その他HTTP Error

        Returns:
            list: Googleドライブファイルリスト
        """
        query_operators = {
            'contains': 'contains',  # The content of one string is present in the other.
            'equals': '=',	         # The content of a string or boolean is equal to the other.
        }
        try:
            qp = query_operators[query_operator]
        except KeyError:
            raise ValueError('引数:query_operatorが不正です。')

        # クエリパラメータ生成
        params = {
            'q': '"{}" in parents and trashed=false and title {} "{}"'.format(
                folder_id,
                qp,
                file_name),
            'corpus': 'DEFAULT',
            'supportsTeamDrives': self.supportsTeamDrives,
            'includeTeamDriveItems': self.includeTeamDriveItems,
            'maxResults': max_number_of_file,
        }
        try:
            file_list = self.drive.ListFile(params).GetList()
        except HttpError as e:
            if e.resp.status == 404:
                raise ValueError('フォルダIDが不正です。フォルダID:{}'.format(folder_id))
            else:
                raise e
        if len(file_list) == 0:
            # 0件であればNoneを返す
            raise FileNotFoundError('Googleドライブのファイル「{}」が見つかりませんでした。フォルダID: {}'.format(
                file_name,
                folder_id))
        return file_list

    def download_files(self, folder_id,
                       file_name,
                       output_file_dir,
                       max_number_of_file=30,
                       query_operator='contains'):
        """[Googleドライブの指定フォルダID内にあるファイルを指定し、ローカルにダウンロードする。

        Args:
            folder_id (str): GoogleドライブのフォルダID
            file_name (str): Googleドライブのファイル名
            output_file_dir (str): ローカルのファイル出力ディレクトリ
            max_number_of_file (int): 最大取得ファイル数. Defaults to 30.
            query_operator (str, optional): 部分一致か完全一致か。以下の値のいずれか。. Defaults to 'contains'.
                contains: 部分一致。The content of one string is present in the other.
                equals: 完全一致。The content of a string or boolean is equal to the other.

        Raises:
            ValueError: 引数のフォルダIDが誤っている
            FileNotFoundError: 指定フォルダ内にファイルが存在しない
            googleapiclient.errors import HttpError: その他HTTP Error

        Returns:
            list: Googleドライブファイルリスト
        """
        try:
            file_list = self.__get_file_list(folder_id, file_name, max_number_of_file, query_operator)
        except Exception as e:
            raise e
        download_filelist = []
        for i, target_file in enumerate(file_list):
            # Googleドライブからローカルにダウンロード
            try:
                # 半角スペース、半角スラッシュ、全角スペースを半角アンダーバーに置換。
                target_file['title'] = re.sub(' |/|　', '_', target_file['title'])
                f = self.drive.CreateFile({'id': target_file['id']})
                f.GetContentFile('{}/{}'.format(output_file_dir, target_file['title']))
                download_filelist.append(target_file)
            except FileNotDownloadableError:
                # ダウンロードできないファイルはダウンロードしない
                print('ダウンロードがスキップされました。ファイル名:{}'.format(target_file['title']))
        return download_filelist

    def move_files(self,
                   from_folder_id,
                   to_folder_id,
                   file_name,
                   max_number_of_file=30,
                   query_operator='contains'):
        """移動元、移動先ファイルIDを指定し、ファイル名が一致するファイルを移動する

        Args:
            from_folder_id (str): GoogleドライブのフォルダID
            to_folder_id (str): GoogleドライブのフォルダID
            file_name (str): Googleドライブのファイル名
            max_number_of_file (int): 最大取得ファイル数. Defaults to 30.
            query_operator (str, optional): 部分一致か完全一致か。以下の値のいずれか。. Defaults to 'contains'.
                contains: 部分一致。The content of one string is present in the other.
                equals: 完全一致。The content of a string or boolean is equal to the other.

        Raises:
            ValueError: 引数のフォルダIDが誤っている
            FileNotFoundError: 指定フォルダ内にファイルが存在しない
            googleapiclient.errors import HttpError: その他HTTP Error
        """
        file_list = self.__get_file_list(from_folder_id, file_name, max_number_of_file, query_operator)

        for target_file in file_list:
            print(target_file['id'])
            print(target_file['title'])
            # Googleドライブのファイルを移動
            f = self.drive.CreateFile({'id': target_file['id'],
                                       'parents': [{'id': from_folder_id}]})

            f.FetchMetadata()
            f['parents'] = [{'id': to_folder_id}]
            try:
                f.Upload(param={'supportsTeamDrives': self.supportsTeamDrives})
            except Exception as e:
                raise ValueError('移動先のフォルダIDが不正です。フォルダID:{} 詳細({})'.format(to_folder_id, e))

    def move_file(self,
                  file_id,
                  from_folder_id,
                  to_folder_id):
        """ファイルIDと移動元、移動先フォルダIDを指定して、ファイルを移動する。

        Args:
            file_id (str): 移動するGoogleドライブのファイルID
            from_folder_id (str): GoogleドライブのフォルダID
            to_folder_id (str): GoogleドライブのフォルダID

        Raises:
            ValueError: 引数のフォルダIDが誤っている
            googleapiclient.errors import HttpError: その他HTTP Error
        """
        # Googleドライブのファイルを移動
        f = self.drive.CreateFile({'id': file_id, 'parents': [{'id': from_folder_id}]})

        f.FetchMetadata()
        f['parents'] = [{'id': to_folder_id}]
        try:
            f.Upload(param={'supportsTeamDrives': self.supportsTeamDrives})
        except Exception as e:
            raise ValueError('移動先のフォルダIDが不正です。フォルダID:{} 詳細({})'.format(to_folder_id, e))

    def upload_file(self,
                    local_file_path,
                    folder_id,
                    upload_file_name):
        """ローカルのファイルをGoogleドライブにアップロードする。

        Args:
            local_file_path (str): ローカルのファイルパス
            folder_id (str): Googleドライブのアップロード先のフォルダID
            upload_file_name (str): アップロードファイル名
        Returns:
            str: 作成したファイルのID
        """
        f = self.drive.CreateFile({'title': upload_file_name, 'parents': [{'id': folder_id}]})
        f.SetContentFile(local_file_path)
        f.Upload(param={'supportsTeamDrives': True})
        return f['id']

    def create_folder(self,
                      parents_id,
                      folder_Name):
        """Googleドライブにフォルダを作成してフォルダIDを返却する。

        Args:
            parents_id (str): 親フォルダのフォルダID
            folder_Name (str): 作成するフォルダ名
        Returns:
            str: 作成したフォルダのフォルダID
        """
        f = self.drive.CreateFile({'title': folder_Name,
                                   'mimeType': 'application/vnd.google-apps.folder',
                                   'parents': [{'id': parents_id}]})
        f.Upload(param={'supportsTeamDrives': True})
        return f['id']

    def list_file(self,
                  folder_id):
        """Googleドライブの指定フォルダID内に存在するファイルの一覧を返却する

        Args:
            folder_id (str): フォルダID
        Returns:
            list: ファイルの一覧
        """
        f = self.drive.ListFile({'q': '"{}" in parents and trashed=false'.format(folder_id),
                                 'corpus': 'DEFAULT',
                                 'supportsTeamDrives': True,
                                 'includeTeamDriveItems': True}).GetList()
        return f

    def list_folder(self,
                    folder_id):
        """Googleドライブの指定フォルダID内に存在するフォルダの一覧を返却する

        Args:
            folder_id (str): フォルダID
        Returns:
            list: フォルダの一覧
        """
        f = self.drive.ListFile({'q': "'{}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed=false".format(folder_id),
                                 'corpus': 'DEFAULT',
                                 'supportsTeamDrives': True,
                                 'includeTeamDriveItems': True}).GetList()
        return f

    def delete_file(self,
                      file_id):
        """ファイルを削除する。

        Args:
            file_id (str): 作成するフォルダ名
        Returns:
            なし
        """
        f = self.drive.CreateFile({'id': file_id})
        f.Trash(param={'supportsTeamDrives': True})
