"""Cloud SQLでクエリを実行する。
"""
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from lib.hooks import mysql_query_hook


class MysqlQueryOperator(PythonOperator):

    ui_color = '#FFEC4D'
    @apply_defaults
    def __init__(
            self,
            database_name,
            sql_str=None,
            sql_file_path=None,
            query_params=None,
            *args,
            **kwargs):
        """Cloud SQLのクエリを実行するOperator

        Args:
            database_name (str): クエリを実行するDB名
            sql_str (str): 実行するクエリ文字列。 Defaults to None.
            sql_file_path (str): 実行するクエリファイルのパス。 Defaults to None.
            query_params (): クエリ文字列内パラメータの置換後の値。 Defaults to None.
        """

        python_callable = mysql_query_hook.execute

        op_kwargs = {
            'database_name': database_name,
            'sql_str': sql_str,
            'sql_file_path': sql_file_path,
            'query_params': query_params
        }

        super(MysqlQueryOperator, self).__init__(python_callable=python_callable,
                                                 op_kwargs=op_kwargs,
                                                 *args,
                                                 **kwargs)
