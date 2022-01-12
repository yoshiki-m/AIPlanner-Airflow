from collections import OrderedDict
import os
import re
import requests

from airflow.models import Variable
import pandas as pd
from simple_salesforce import Salesforce, SFType
from simple_salesforce.bulk import SFBulkHandler, SFBulkType
from simple_salesforce.exceptions import SalesforceMalformedRequest

from lib.errors.exception import AuthenticationException, SoqlException, RequestException


# 一度にファイル出力するサイズ（byte）：1MB
chunk_size = 1024 * 1024 * 1024
# レポートのタイムアウト値（秒）
report_timeout_sec = 540


class SalesforceClient:
    """Salesforceに接続するためのクライアント
    """

    SANDBOX = 'sandbox'
    PRODUCTION = 'production'
    FULLSAND = 'fullsand'

    def __init__(self,
                 local=False,
                 environ=SANDBOX):
        """初期化

        Args:
            local (bool): ローカル開発環境かどうか. Defaults to False.
            env (environ): SalesforceClient.PRODUCTION → Airflowから本番環境に接続
                           SalesforceClient.SANDBOX → Airflowから開発環境に接続
                           SalesforceClient.FULLSAND → Airflowからfullsand環境に接続
                           Defaults to SANDBOX
        """
        self.local = local
        self.environ = environ
        self.is_able_to_change_data = True
        # ローカルPCで実行した場合
        if self.local:
            self.domain = 'test'
            self.base_url = 'https://smsc001--test.cs31.my.salesforce.com'
            self.sorp_api_login_url = 'https://test.salesforce.com/services/Soap/u/39.0'
            self.tmp_dir = 'tests/tmp'
            access_token_url = os.environ['SALESFORCE_ACCESS_TOKEN_URL']
            data = {
                'grant_type': 'password',
                'client_id': os.environ['SALESFORCE_CLIENT_ID'],
                'client_secret': os.environ['SALESFORCE_CLIENT_SECRET'],
                'username': os.environ['SALESFORCE_USER_NAME'],
                'password': os.environ['SALESFORCE_USER_PASSWORD'],
            }
            sorp_api_data = {
                'username': os.environ['SALESFORCE_REPORT_USER_NAME'],
                'password': os.environ['SALESFORCE_REPORT_USER_PASSWORD'],
            }
        # Airflow開発環境で本番環境に接続する場合
        elif self.environ == SalesforceClient.PRODUCTION and Variable.get('project_id') == 'dev-jinzaisystem-tool':
            self.domain = 'login'
            self.base_url = 'https://smsc001.my.salesforce.com/'
            self.sorp_api_login_url = 'https://login.salesforce.com/services/Soap/u/39.0'
            self.tmp_dir = '/var/tmp'
            access_token_url = Variable.get('salesforce_access_token_url')
            data = {
                'grant_type': 'password',
                'client_id': Variable.get('salesforce_client_id'),
                'client_secret': Variable.get('salesforce_client_secret'),
                'username': Variable.get('salesforce_user_name'),
                'password': Variable.get('salesforce_user_password'),
            }
            sorp_api_data = {
                'username': Variable.get('salesforce_report_user_name'),
                'password': Variable.get('salesforce_report_user_password'),
            }
            self.is_able_to_change_data = False
        # Airflow本番環境で本番環境に接続する場合
        elif self.environ == SalesforceClient.PRODUCTION:
            self.domain = 'login'
            self.base_url = 'https://smsc001.my.salesforce.com/'
            self.sorp_api_login_url = 'https://login.salesforce.com/services/Soap/u/39.0'
            self.tmp_dir = '/var/tmp'
            access_token_url = Variable.get('salesforce_access_token_url')
            data = {
                'grant_type': 'password',
                'client_id': Variable.get('salesforce_client_id'),
                'client_secret': Variable.get('salesforce_client_secret'),
                'username': Variable.get('salesforce_user_name'),
                'password': Variable.get('salesforce_user_password'),
            }
            sorp_api_data = {
                'username': Variable.get('salesforce_report_user_name'),
                'password': Variable.get('salesforce_report_user_password'),
            }
        # fullsand環境に接続する場合
        elif self.environ == SalesforceClient.FULLSAND:
            self.domain = 'test'
            self.base_url = 'https://smsc001--fullsand.my.salesforce.com/'
            self.sorp_api_login_url = 'https://test.salesforce.com/services/Soap/u/39.0'
            self.tmp_dir = '/var/tmp'
            access_token_url = Variable.get('fullsand_salesforce_access_token_url')
            data = {
                'grant_type': 'password',
                'client_id': Variable.get('fullsand_salesforce_client_id'),
                'client_secret': Variable.get('fullsand_salesforce_client_secret'),
                'username': Variable.get('fullsand_salesforce_user_name'),
                'password': Variable.get('fullsand_salesforce_user_password'),
            }
            sorp_api_data = {
                'username': Variable.get('fullsand_salesforce_user_name'),
                'password': Variable.get('fullsand_salesforce_user_password'),
            }
        # 開発環境に接続する場合
        else:
            self.domain = 'test'
            self.base_url = 'https://smsc001--test.my.salesforce.com'
            self.sorp_api_login_url = 'https://test.salesforce.com/services/Soap/u/39.0'
            self.tmp_dir = '/var/tmp'
            access_token_url = Variable.get('dev_salesforce_access_token_url')
            data = {
                'grant_type': 'password',
                'client_id': Variable.get('dev_salesforce_client_id'),
                'client_secret': Variable.get('dev_salesforce_client_secret'),
                'username': Variable.get('dev_salesforce_user_name'),
                'password': Variable.get('dev_salesforce_user_password'),
            }
            sorp_api_data = {
                'username': Variable.get('dev_salesforce_report_user_name'),
                'password': Variable.get('dev_salesforce_report_user_password'),
            }

        # SFインスタンス作成
        self.sf = self.__get_salesforce_instance(access_token_url, data)
        self.bulk = SFBulkHandler(self.sf.sesstion_id, self.sf.bulk_url)

        # SOAP APIログイン用のxmlデータ作成
        self.sorp_api_xml_data = self.__set_sorp_api_xml_data(sorp_api_data)

    def __get_salesforce_instance(self, access_token_url, data):
        """Salesforceのセッションインスタンスを取得

        Args:
            access_token_url (str): アクセストークンURL
            data (dict): 認証情報

        Raises:
            AuthenticationException: 認証失敗

        Returns:
            simple_salesforce.api.Salesforce
        """
        headers = {'content-type': 'application/x-www-form-urlencoded'}

        response = requests.post(access_token_url, data=data, headers=headers)
        response = response.json()
        if response.get('error'):
            raise AuthenticationException('Salesforce APIの認証に失敗しました。:{}'.format(
                response.get('error_description')))

        session = requests.Session()
        return Salesforce(instance_url=response['instance_url'],
                          session_id=response['access_token'],
                          domain=self.domain,
                          session=session)

    def __set_sorp_api_xml_data(self, data):
        """SOAP APIログイン用のxmlデータを作成する。

        Args:
            data (dict): username,passwordが含まれるdictデータ

        Returns:
            str: SOAP APIログイン用のxmlデータ
        """

        xml_data = '''<?xml version="1.0" encoding="utf-8" ?>
        <env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
          <env:Body>
            <n1:login xmlns:n1="urn:partner.soap.sforce.com">
              <n1:username>{}</n1:username>
              <n1:password>{}</n1:password>
            </n1:login>
          </env:Body>
        </env:Envelope>
        '''.format(data['username'],
                   data['password'])
        return xml_data

    def __is_able_to_change_data(self):
        """Airflow開発環境から本番環境へデータ更新処理を呼び出す場合、例外発生させる。

        Raises:
            Exception: Airflow開発環境から本番環境に接続している場合。
        """
        if self.is_able_to_change_data is False:
            raise Exception('Airflow開発環境から本番環境のデータの更新は許可されていません。')

    def __get_sf_type(self, sf_object_name):
        """SFTypeオブジェクトを作成する

        Args:
            sf_object_name (str): Salesforceオブジェクト名

        Returns:
            simple_salesforce.api.SFType: SFTypeオブジェクト
        """
        return SFType(sf_object_name, self.sf.session_id, self.sf.sf_instance)

    def __get_sf_bulk_type(self, sf_object_name):
        """SFBulkTypeオブジェクトを作成する

        Args:
            sf_object (str): Salesforceオブジェクト名

        Returns:
            simple_salesforce.api.SFBulkType: bulkオブジェクト
        """
        sf_bulk_type = SFBulkType(sf_object_name,
                                  self.bulk.bulk_url,
                                  self.bulk.headers,
                                  self.bulk.session)

        # headerのsession_idを上書き更新しないとエラーになるため、上書き
        sf_bulk_type.headers.update({'X-SFDC-Session': self.sf.session_id})
        return sf_bulk_type

    def describe(self, max_record=10):
        """オブジェクト一覧を取得し、標準出力する。

        Args:
            max_record (int): 標準出力する件数. Defaults to 10.
        """
        result = self.sf.describe()
        if len(result['sobjects']):
            for i, record in enumerate(result['sobjects']):
                print('{}: {}'.format(record['name'], record['label']))
                if i > max_record:
                    break

    def get_data(self, query):
        """SOQLを実行し、結果を辞書形式で返す。

        Args:
            query(str): SOQL

        Returns:
            クエリの結果（辞書）
        """
        try:
            res = self.sf.query(query)
            return res
        except Exception:
            raise SoqlException()

    def get_df(self, query):
        """SOQLを実行し、結果をDataframeで返す。

        Args:
            query(str): SOQL

        Returns:
            Dataframe: クエリの結果/結果がなければNoneを返す
        """

        # レコードのリスト
        dict_list = []

        # SOQL実行
        try:
            res = self.sf.query(query)
        except Exception:
            raise SoqlException()

        # 結果をdict_listに入れる
        if res['totalSize'] > 0:
            records = res['records']
            for record in records:
                dict = {}
                for key in record.keys():
                    if type(record[key]) is OrderedDict:
                        if key == 'attributes':
                            continue
                        else:
                            for key_child in record[key].keys():
                                if type(record[key][key_child]) is OrderedDict and key_child == 'attributes':
                                    continue
                                else:
                                    dict[key + '.' + key_child] = record[key][key_child]
                    elif isinstance(record[key], type(None)):
                        dict[key] = ''
                    else:
                        dict[key] = record[key]
                dict_list.append(dict)
            # DataFrameに変換
            return pd.io.json.json_normalize(dict_list)
        else:
            return None

    def update(self, sf_object_name, sf_object_id, data):
        """SFオブジェクト1つを対象とし、idを指定してデータを更新する。

        Args:
            sf_object_name (str): Salesforceオブジェクト名
            sf_object_id (str): 更新レコードのID
            data (dict)): SFの更新データ ex. {key1: value1, key2: value2}

        Raises:
            RequestException: データ更新が失敗したとき

        Returns:
            requests.Response: APIレスポンス
        """
        self.__is_able_to_change_data()
        try:
            sf_type = self.__get_sf_type(sf_object_name)
            return sf_type.update(sf_object_id, data, raw_response=True)
        except SalesforceMalformedRequest as e:
            raise RequestException('データのUPDATEが失敗しました。{}'.format(e.message))

    def upsert(self, sf_object_name, sf_object_id, data):
        """SFオブジェクト1つを対象とし、idを指定してデータを挿入・更新する。

        Args:
            sf_object_name (str): Salesforceオブジェクト名
            sf_object_id (str): 更新レコードのID
            data (dict)): SFの更新データ ex. {key1: value1, key2: value2}

        Raises:
            RequestException: データ更新が失敗したとき

        Returns:
            requests.Response: APIレスポンス
        """
        self.__is_able_to_change_data()
        try:
            sf_type = self.__get_sf_type(sf_object_name)
            return sf_type.upsert(sf_object_id, data, raw_response=True)
        except SalesforceMalformedRequest as e:
            raise RequestException('データのUPSERTが失敗しました。{}'.format(e.message))

    def bulk_insert(self, sf_object_name, data):
        """SFオブジェクト1つを対象とし、複数のデータを作成する。
        処理はbulk APIで行う。

        Args:
            sf_object_name (str): Salesforceオブジェクト名
            data (list(dict)): SFの更新データ
                               ex.  [
                                        {key1: value1, key2: value4},
                                        {key1: value2, key2: value5},
                                        {key1: value3, key2: value6},
                                    ]

        Raises:
            RequestException: データの登録が失敗したとき

        Returns:
            requests.Response: APIレスポンス
        """
        self.__is_able_to_change_data()
        try:
            sf_bulk_type = self.__get_sf_bulk_type(sf_object_name)
            return sf_bulk_type.insert(data)
        except SalesforceMalformedRequest as e:
            raise RequestException('データのバルクINSERTが失敗しました。{}'.format(e.message))

    def bulk_update(self, sf_object_name, data):
        """SFオブジェクト1つを対象とし、複数のデータを更新する。
        処理はbulk APIで行う。

        Args:
            sf_object_name (str): Salesforceオブジェクト名
            data (list(dict)): SFの更新データ
                               ex.  [
                                        {key1: value1, key2: value4},
                                        {key1: value2, key2: value5},
                                        {key1: value3, key2: value6},
                                    ]

        Raises:
            RequestException: データ更新が失敗したとき

        Returns:
            requests.Response: APIレスポンス
        """
        self.__is_able_to_change_data()
        try:
            sf_bulk_type = self.__get_sf_bulk_type(sf_object_name)
            return sf_bulk_type.update(data)
        except SalesforceMalformedRequest as e:
            raise RequestException('データのバルクUPDATEが失敗しました。{}'.format(e.message))

    def bulk_upsert(self, sf_object_name, data, external_id_field):
        """SFオブジェクト1つを対象とし、複数のデータを挿入・更新する。
        処理はbulk APIで行う。

        Args:
            sf_object_name (str): Salesforceオブジェクト名
            data (list(dict)): SFの更新データ
                               ex.  [
                                        {key1: value1, key2: value4},
                                        {key1: value2, key2: value5},
                                        {key1: value3, key2: value6},
                                    ]
            external_id_field: unique identifier field for upsert operations

        Raises:
            RequestException: データ更新が失敗したとき

        Returns:
            requests.Response: APIレスポンス
        """
        self.__is_able_to_change_data()
        try:
            sf_bulk_type = self.__get_sf_bulk_type(sf_object_name)
            return sf_bulk_type.upsert(data, external_id_field)
        except SalesforceMalformedRequest as e:
            raise RequestException('データのバルクUPSERTが失敗しました。{}'.format(e.message))

    def bulk_delete(self, sf_object_name, data):
        """複数のデータを削除する。
        処理はbulk APIで行う。

        Args:
            sf_object_name (str): Salesforceオブジェクト名
            data (list(dict)): SFの削除データ
                               ex.  [
                                        {'Id': value1},
                                        {'Id': value2},
                                        {'Id': value3},
                                    ]
        Raises:
            RequestException: データ更新が失敗したとき

        Returns:
            requests.Response: APIレスポンス
        """
        self.__is_able_to_change_data()
        try:
            sf_bulk_type = self.__get_sf_bulk_type(sf_object_name)
            return sf_bulk_type.delete(data)
        except SalesforceMalformedRequest as e:
            raise RequestException('データのバルクDELETEが失敗しました。{}'.format(e.message))

    def export_report_csv(self, report_id, local_file_path):
        """SFレポートをCSV出力する。
           レポートの実行に失敗した場合は出力しない。
           ※削除予定。今後はexport_report() を使用してください。

        Args:
            report_id (str): レポートID
            local_file_path (str): ローカルの出力先ファイルパス
        """
        self.export_report(report_id, local_file_path)

    def export_report(self, report_id, local_file_path, format='csv', charset='UTF-8'):
        """SFレポートをファイル出力する。
           レポートの実行に失敗した場合は出力しない。

        Args:
            report_id (str): レポートID
            local_file_path (str): ローカルの出力先ファイルパス
            format (str): 出力ファイル形式（csv、xsl）
            charset (str): 出力ファイルの文字コード（UTF-8、Shift_JIS）
        """
        headers = {
            'Content-Type': 'text/xml; charset=UTF-8',
            'SOAPAction': 'login',
        }

        with requests.session() as s:
            # SOAP APIでログイン、session_idを取得する
            res = s.post(self.sorp_api_login_url,
                         headers=headers,
                         data=self.sorp_api_xml_data)
            res_xml = res.content.decode('utf-8')
            session_id = re.sub('.*<sessionId>', '', res_xml)
            session_id = re.sub('<\\/sessionId>.*', '', session_id)
            # レポートURL実行
            response = s.get('{}/{}?export=1&enc={}&isdtp=p1&xf={}&skipFooter=true'.format(self.base_url, report_id, charset, format),
                             headers={'cookie': 'sid={}; secure'.format(session_id)},
                             stream=True,
                             timeout=report_timeout_sec)
            if response.status_code == 200:
                with open(local_file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        f.write(chunk)
            else:
                raise Exception('レポートの実行に失敗しました。report_id={} status_code={}'.format(report_id, response.status_code))

    def get_report_dict(self, report_id):
        tmp_file_path = '{}/tmp_{}'.format(self.tmp_dir, report_id)
        # 一時ディレクトリにCSV出力
        try:
            self.export_report_csv(report_id, tmp_file_path)
            df = pd.read_csv(tmp_file_path)
            return df.to_dict(orient='records')
        finally:
            os.remove(tmp_file_path)
