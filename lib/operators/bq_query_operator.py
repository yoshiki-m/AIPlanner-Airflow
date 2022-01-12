"""BigQueryOperatorが東京リージョンだと動かないので、
SQLの実行はBqQueryOperatorを実行する。
"""
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from lib.hooks import bq_hook


class BqQueryOperator(PythonOperator):

    ui_color = '#B2DDFF'
    @apply_defaults
    def __init__(
            self,
            sql,
            project_id=None,
            destination=None,
            query_parameters=None,
            xcom_parameters=None,
            write_disposition='WRITE_EMPTY',
            is_return_result=False,
            max_return_records=None,
            location=None,
            *args,
            **kwargs):
        """BigQueryのクエリを実行するOperator

        Args:
            sql (str): GCS上のSQLファイルのURI
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
            write_disposition (google.cloud.bigquery.job.WriteDisposition): 保存先テーブルの書き込み方法. Defaults to 'WRITE_EMPTY'.
                                                                            'WRITE_EMPTY','WRITE_APPEND','WRITE_TRUNCATE'のいずれか。
            is_return_result (bool): Trueの場合、クエリの結果をlistで返す。. Defaults to False.
            max_return_records (int): クエリの結果をlistで返す場合の最大レコード数。指定がなければ1000を設定する。
            location (str): BigQueryのジョブを実行するロケーション。指定がなければ asia-northeast1. Defaults to None.
        """

        python_callable = bq_hook.bq_query

        # プロジェクトIDがNoneの場合はAirflowのプロジェクトIDを設定
        if project_id is None:
            project_id = Variable.get('project_id')

        op_kwargs = {
            'sql': sql,
            'project_id': project_id,
            'destination': destination,
            'query_parameters': query_parameters,
            'xcom_parameters': xcom_parameters,
            'write_disposition': write_disposition,
            'is_return_result': is_return_result,
            'max_return_records': max_return_records,
            'location': location,
        }

        super(BqQueryOperator, self).__init__(python_callable=python_callable,
                                              op_kwargs=op_kwargs,
                                              provide_context=True,
                                              *args,
                                              **kwargs)
