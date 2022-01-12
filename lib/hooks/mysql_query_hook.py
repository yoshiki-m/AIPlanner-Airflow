
from lib.utils.mysql_client import MysqlClient

# AirflowのDAGディレクトリ
AIRFLOW_DAGS_DIR = '/home/airflow/gcs/dags'


def execute(database_name,
            sql_str=None,
            sql_file_path=None,
            query_params=None):
    """Cloud SQLのクエリを実行する

    Args:
        database_name (str): クエリを実行するDB名
        sql_str (str): 実行するクエリ文字列。 Defaults to None.
        sql_file_path (str): 実行するクエリファイルのパス。 Defaults to None.
        query_params (): クエリ文字列内パラメータの置換後の値。 Defaults to None.
    """
    # Mysqlサーバーにログイン
    mysqlClient = MysqlClient(db=database_name)

    # クエリ文字列
    sql = sql_str
    # クエリ文字が指定されていない場合はファイルから取得
    if sql is None:
        sql_file_airflow_path = '{}/{}'.format(AIRFLOW_DAGS_DIR, sql_file_path)
        # SQLファイルを読み込む
        with open(sql_file_airflow_path, encoding='UTF-8') as f:
            sql = f.read()

    # クエリを実行
    mysqlClient.execute(sql, params=query_params)
