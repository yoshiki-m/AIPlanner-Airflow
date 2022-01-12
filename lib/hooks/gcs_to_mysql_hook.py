import os
from lib.utils.mysql_client import MysqlClient
from lib.utils.cloud_storage import CloudStorageClient

# ロード時に実行するSQLの雛型
SQL_STR = '''LOAD DATA LOCAL INFILE '{local_path}'
             {replace} INTO TABLE {table_name}
             CHARACTER SET {chara_set}
             FIELDS TERMINATED BY '{fields_terminated}'
             OPTIONALLY ENCLOSED BY '{optionally_enclosed}'
             LINES TERMINATED BY '{lines_terminated}'
             IGNORE {ignore_lines} LINES'''


def execute(project_id,
            gcs_path,
            database_name,
            table_name,
            chara_set='utf8',
            fields_terminated=',',
            optionally_enclosed='"',
            lines_terminated='\\n',
            ignore_lines=1,
            is_replace=True):
    """GCSのファイルをCloud SQLにロードする

    Args:
        project_id (str): プロジェクトID
        gcs_path (str): 出力元GCSのファイルパス
        database_name (str): 出力先DB名
        table_name (str): 出力先テーブル名
        chara_set (str): 文字種別。 Defaults to 'utf8'.
        fields_terminated (str): 区切り文字。 Defaults to ','.
        optionally_enclosed (str): 囲い文字。 Defaults to '"'.
        lines_terminated (str): 改行コード。 Defaults to '\n'.
        ignore_lines (int): 無視する行数。 Defaults to 1.
        is_replace (bool): TrueならTRUNCATE INSERT、FalseならUPSERT。 Defaults to True.
    """
    # コピー後のローカルファイルパス
    tmp_file_path = '/var/tmp/{}_{}'.format(database_name, table_name)

    # Mysqlサーバーにログイン
    mysqlClient = MysqlClient(db=database_name,
                              local_infile=True)
    # GCSからローカルにファイルをコピー
    gcsClient = CloudStorageClient(project_id=project_id)
    tmp_str = gcs_path.replace('gs://', '').split('/')
    gcsClient.download(tmp_str[0],
                       gcs_path.replace('gs://' + tmp_str[0] + '/', ''),
                       tmp_file_path)
    # SQL文字列を作成
    replace_str = ''
    if is_replace:
        replace_str = 'REPLACE'

    sql_str = SQL_STR.format(local_path=tmp_file_path,
                             replace=replace_str,
                             table_name=table_name,
                             chara_set=chara_set,
                             fields_terminated=fields_terminated,
                             optionally_enclosed=optionally_enclosed,
                             lines_terminated=lines_terminated,
                             ignore_lines=ignore_lines)

    # コピーしてきたファイルを使ってMysqlにロード
    mysqlClient.execute(sql_str)

    # 一時ファイル削除
    os.remove(tmp_file_path)
