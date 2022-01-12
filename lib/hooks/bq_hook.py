import re

from google.cloud import bigquery
from google.oauth2 import service_account

# サービスアカウント認証ファイル
CREDENTIALS_FILE = 'config/jinzaisystem-tool-composer.json'

# AirflowのDAGディレクトリ
AIRFLOW_DAGS_DIR = '/home/airflow/gcs/dags'


def __get_client(project_id,
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

    # BigQueryクライアント作成
    client = bigquery.Client(project=project_id, credentials=credentials)
    return client


def __set_query_params(query_parameters):
    """query_paramsを設定する

    Args:
        query_parameters (dict)

    Returns:
        query_params
    """
    query_params = []
    for k, v in query_parameters.items():
        if type(v) == str:
            query_params.append(bigquery.ScalarQueryParameter(k, 'STRING', v))
        elif type(v) == int:
            query_params.append(bigquery.ScalarQueryParameter(k, 'INT64', v))
        elif type(v) == float:
            query_params.append(bigquery.ScalarQueryParameter(k, 'FLOAT', v))
        else:
            continue
    return query_params


def __set_query_job_config(client,
                           project_id,
                           destination=None,
                           query_parameters=None,
                           write_disposition=None):
    """bq query実行時に渡すjob_configを設定する

    Raises:
        ValueError: 引数の値に誤りがある場合

    Returns:
        job_config
    """
    job_config = bigquery.QueryJobConfig()

    # 保存先テーブル
    if destination is not None:
        # Project.dataset.table
        if re.match('.*\\..*\\..*', destination):
            job_config.destination = __get_table_ref(client, destination)
        # dataset.table
        elif re.match('.*\\..*', destination):
            job_config.destination = __get_table_ref(client, '{}.{}'.format(project_id, destination))
        else:
            raise ValueError('destinationの値が不正です。')

    # クエリパラメータ
    if query_parameters is not None:
        job_config.query_parameters = __set_query_params(query_parameters)

    # write_disposition
    if write_disposition == 'WRITE_EMPTY':
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_EMPTY
    elif write_disposition == 'WRITE_APPEND':
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
    elif write_disposition == 'WRITE_TRUNCATE':
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
    else:
        raise ValueError('write_dispositionの値が不正です。')

    return job_config


def __get_table_ref(client, dataset_table):
    """テーブルリファレンスを取得する。

    Args:
        client (bigquery.Client)
        dataset_table (str)

    Raises:
        ValueError: 引数の値に誤りがある場合

    Returns:
        dataset_ref.table
    """
    if re.match('.*\\..*\\..*', dataset_table):
        lst = dataset_table.split('.')
        project_id = lst[0]
        dataset_id = lst[1]
        table_id = lst[2]
    elif re.match('.*\\..*', dataset_table):
        project_id = 'default'
        lst = dataset_table.split('.')
        dataset_id = lst[0]
        table_id = lst[1]
    else:
        raise ValueError('destinationの値が不正です。')

    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)
    return table_ref


def __set_extract_job_config(compression=None,
                             export_format=None,
                             field_delimiter=None,
                             print_header=True):
    """bq extractに渡すjob_configを設定する。

    Returns:
        bigquery.ExtractJobConfig
    """
    job_config = bigquery.ExtractJobConfig()
    if compression is not None:
        job_config.compression = compression
    if export_format is not None:
        job_config.destination_format = export_format
    if field_delimiter is not None:
        job_config.field_delimiter = field_delimiter
    job_config.print_header = print_header
    return job_config


def bq_query(sql,
             project_id=None,
             destination=None,
             query_parameters=None,
             xcom_parameters=None,
             write_disposition=None,
             is_return_result=False,
             return_type='list',
             max_return_records=1000000,
             by_query_str=False,
             location=None,
             local=False,
             **kwargs):
    """SQLを実行する。

    Args:
        sql (str): SQLファイルのディレクトリ
        project_id (str): BigQueryのジョブを実行するプロジェクト。指定がなければ jinzaisystem-tool. Defaults to None.
        destination (str): 保存先テーブル。以下の書式のいずれか。. Defaults to None.
                        　 「プロジェクトID.データセットID.テーブルID」
                        　 「プロジェクトID.データセットID.テーブルID$パーティション」
                        　 「データセットID.テーブルID」
                        　 「データセットID.テーブルID$パーティション」
                        　 プロジェクトIDを指定しない場合、ジョブ実行プロジェクトをセットする。
        query_parameters (dict): クエリパラメータ。. Defaults to None.
                                    SQLファイルに、「@hoge」「@hoge2」のように変数を設定することで、パラメータを設定できる。
                                    {'hoge': 'value', 'hoge2', 'value2'}のようにdictでパラメータを設定する。
                                    valueの部分は、str、int、floatのみ設定可能。これ以外の型の場合、
                                    クエリパラメータには設定されず、無視される。
        xcom_parameters (dict): クエリパラメータ。. Defaults to None.
                                    「'task_id' : <タスクID>」と「'key' : <XCOMのキー>」のdist形式で指定で指定する。
                                    XCOMから値を取得してクエリパラメータに追加する。
                                    クエリパラメータに追加するキーは<XCOMのキー>とする。
        write_disposition (str): 保存先テーブルの書き込み方法. Defaults to 'WRITE_EMPTY'.
                                    'WRITE_EMPTY','WRITE_APPEND','WRITE_TRUNCATE'のいずれか。
        is_return_result (bool): Trueの場合、クエリの結果をreturnで返す。. Defaults to False.
        return_type (str): クエリの結果をreturnで（list, df）返す。. Defaults to list.
        max_return_records (int): クエリの結果をlistで返す場合の最大レコード数。指定がなければ1000000を設定する。
        by_query_str (bool): Trueの場合、パラメータsqlをクエリ文字列として扱う。
                            Falseの場合、パラメータsqlをSQLファイルパスとして扱う。 Defaults to False.
        location (str): BigQueryのジョブを実行するロケーション。指定がなければ asia-northeast1. Defaults to None.
        local (bool): ローカル環境で実行する場合はTrue. Defaults to False.
    """
    if project_id is None:
        project_id = 'jinzaisystem-tool'
    if write_disposition is None:
        write_disposition = 'WRITE_EMPTY'
    if location is None:
        location = 'asia-northeast1'
    if max_return_records is None:
        max_return_records = 1000000



    if by_query_str:
        # パラメータのクエリ文字列を利用
        query_str = sql
    else:
        if local is False:
            sql = '{}/{}'.format(AIRFLOW_DAGS_DIR, sql)
        # SQLファイルを読み込みクエリ文字列を作成
        with open(sql, encoding='UTF-8') as f:
            query_str = f.read()

    # XCOMのキーの数だけ値を取得する
    if xcom_parameters is not None:
        # XCOMのタスクインスタンスを取得
        task_instance = kwargs['ti']
        for xcom_parameter in xcom_parameters:
            # XCOMから値を取得
            value = task_instance.xcom_pull(task_ids=xcom_parameter['task_id'],
                                            key=xcom_parameter['key'])
            # クエリパラメータに追加
            if query_parameters is None:
                query_parameters = {}
            query_parameters[xcom_parameter['key']] = value

    # クライアント作成
    client = __get_client(project_id, local=local)

    # BigQueryにクエリを実行、データを取得
    job_config = __set_query_job_config(client=client,
                                        project_id=project_id,
                                        destination=destination,
                                        query_parameters=query_parameters,
                                        write_disposition=write_disposition)

    res = client.query(query_str,
                       job_config=job_config,
                       location=location)
    try:
        res.result()
        if is_return_result:
            df = res.to_dataframe()
            if return_type == 'list':
                ls = df.to_dict(orient='records')
                return ls[:max_return_records]
            elif return_type == 'df':
                return df
        return None
    except Exception as e:
        raise e


def bq_extract(source_project_dataset_table,
               destination_cloud_storage_uris,
               project_id=None,
               compression=None,
               export_format=None,
               field_delimiter=None,
               print_header=True,
               location=None):
    """テーブルをGCSに出力する。

    Args:
        source_project_dataset_table (str): 出力元BigQueryのプロジェクト、データセット、テーブルの文字列
        destination_cloud_storage_uris (str): 出力先GCSのURI
        project_id (str): プロジェクトID。Defaults to None.
        compression (google.cloud.bigquery.job.Compression): 圧縮種別。 Defaults to 'NONE'.
        export_format (google.cloud.bigquery.job.DestinationFormat): 出力フォーマット。 Defaults to 'CSV'.
        field_delimiter (str): ファイル区切り文字。 Defaults to ','.
        print_header (bool): ヘッダを出力するかどうか。 Defaults to True.
        location (str): 出力元BigQueryのデータセットのロケーション。Noneの場合、東京リージョン。 Defaults to None.
    """

    if project_id is None:
        project_id = 'jinzaisystem-tool'
    if compression is None:
        compression = 'NONE'
    if export_format is None:
        export_format = 'CSV'
    if field_delimiter is None:
        field_delimiter = ','
    if location is None:
        location = 'asia-northeast1'

    # クライアント作成
    client = __get_client(project_id)
    # テーブルリファレンス設定
    table_ref = __get_table_ref(client, source_project_dataset_table)
    # ジョブコンフィグ設定
    job_config = __set_extract_job_config(compression=compression,
                                          export_format=export_format,
                                          field_delimiter=field_delimiter,
                                          print_header=print_header)
    res = client.extract_table(table_ref,
                               destination_cloud_storage_uris,
                               job_config=job_config,
                               project=project_id,
                               location=location)
    try:
        res.result()
    except Exception as e:
        raise e


def bq_load(source_cloud_storage_uris,
            destination_project_dataset_table,
            project_id=None,
            location=None,
            skip_leading_rows=1,
            write_disposition=None,
            retry=0,
            field_delimiter=',',
            quote_character=None,
            allow_quoted_newlines=True):
    """GCSのファイルをBigQueryにロードする

    Args:
        source_cloud_storage_uris (str): 出力元GCSのURI
        destination_project_dataset_table (str): 出力先Bigqueryのジロジェクト、データセット、テーブルの文字列
        project_id (str): プロジェクトID。Defaults to None.
        location (str): 出力元BigQueryのデータセットのロケーション。Noneの場合、東京リージョン。 Defaults to None.
        skip_leading_rows (int): スキップ行数。 Defaults to 1.
        write_disposition (google.cloud.bigquery.job.WriteDisposition): テーブルの書き込みオプション。
                            Noneの場合、WRITE_EMPTY。 Defaults to None.
        retry (int): リトライ回数。 Defaults to 0.
        quote_character (str): 区切り文字。Noneの場合、ダブルクォーテーション。 Defaults to None.
    """
    if project_id is None:
        project_id = 'jinzaisystem-tool'
    if location is None:
        location = 'asia-northeast1'
    if write_disposition is None:
        write_disposition = 'WRITE_EMPTY'

    # クライアント作成
    client = __get_client(project_id, local=False)

    # configの作成
    bq_config = bigquery.LoadJobConfig()
    bq_config.skip_leading_rows = skip_leading_rows
    bq_config.write_disposition = write_disposition
    bq_config.field_delimiter = field_delimiter
    bq_config.quote_character = quote_character
    bq_config.allow_quoted_newlines = allow_quoted_newlines
    # BigQueryにロード
    res = client.load_table_from_uri(source_cloud_storage_uris,
                                     destination_project_dataset_table,
                                     location=location,
                                     project=project_id,
                                     job_config=bq_config)
    try:
        res.result()
    except Exception as e:
        raise e
