import os

from airflow.models import Variable
import pymysql.cursors
from pymysql.err import ProgrammingError

from lib.logger import logger


class MysqlClient:

    def __init__(self,
                 db=None,
                 local=False,
                 local_infile=False):
        """MySQLに接続するためのクライアント
        Args:
            db (str): MySQLのデータベース名
            local (boolean): ローカル開発環境かどうか. Defaults to False.
            local_infile (boolean): ローカルのファイルをロードするかどうか. Defaults to False.
        """
        self.db = db
        self.local = local
        self.local_infile = local_infile
        self.__connect()

    def __connect(self):
        """MySQLへのコネクション作成

        Raises:
            Exeption: 全エラー
        """
        if self.local:
            host = os.environ['CLOUD_SQL_HOST']
            port = int(os.environ['CLOUD_SQL_PORT'])
            user = os.environ['CLOUD_SQL_USER']
            passwd = os.environ['CLOUD_SQL_PASSWORD']
            ssl = {
                'ca': os.environ['CLOUD_SQL_SSL_CA_FILE'],
                'key': os.environ['CLOUD_SQL_SSL_KEY_FILE'],
                'cert': os.environ['CLOUD_SQL_SSL_CERT_FILE'],
                'check_hostname': False
            }
        else:
            host = Variable.get('cloud_sql_private_ip')
            port = None
            user = Variable.get('cloud_sql_user')
            passwd = Variable.get('cloud_sql_password')
            ssl = {
                'ca': Variable.get('cloud_sql_ssl_ca_file'),
                'key': Variable.get('cloud_sql_ssl_key_file'),
                'cert': Variable.get('cloud_sql_ssl_cert_file'),
                'check_hostname': False
            }
        try:
            self.conn = pymysql.connect(host=host,
                                        port=port,
                                        user=user,
                                        passwd=passwd,
                                        db=self.db,
                                        ssl=ssl,
                                        local_infile=self.local_infile)
        except Exception as e:
            logger.error('データベースの接続に失敗しました。', e)
            raise e

    def execute(self, sql, params=None):
        """SQLを実行する
        Example:
            sql: SELECT host, user FROM user WHERE user = %s
            params: root
        Args:
            sql (str): SQL
            params (): パラメータ
        Raises:
            ProgrammingError: SQL本文が不正
            Exeption: その他全エラー
        Returns:
            list(dict): SQL実行結果
        """
        try:
            with self.conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(sql, params)
                self.conn.commit()
                result = cur.fetchall()
            return result
        except pymysql.ProgrammingError as programming_error:
            logger.error('SQL構文エラー', programming_error)
            raise programming_error
        except Exception as e:
            logger.error('SQL実行が失敗しました。', e)
            raise e

    def insert(self, sql, params=None):
        """INSERT文を実行し、IDを返す
        Example:
            sql: INSERT INTO user (name, age) VALUES(%s, %s)
            params: (root, 22)
        Args:
            sql (str): SQL
            params (): パラメータ
        Raises:
            ProgrammingError: SQL本文が不正
            Exeption: その他全エラー
        Returns:
            str: Id
        """
        try:
            with self.conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(sql, params)
                self.conn.commit()
                result = cur.lastrowid
            return result
        except pymysql.ProgrammingError as programming_error:
            logger.error('SQL構文エラー', programming_error)
            raise programming_error
        except Exception as e:
            logger.error('SQL実行が失敗しました。', e)
            raise e