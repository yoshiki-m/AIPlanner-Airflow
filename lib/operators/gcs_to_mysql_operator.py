"""GCSのファイルからCloud SQLにロードする。
"""
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from lib.hooks import gcs_to_mysql_hook


class GcsToMysqlOperator(PythonOperator):

    ui_color = '#FFDC73'
    @apply_defaults
    def __init__(
            self,
            project_id,
            gcs_path,
            database_name,
            table_name,
            chara_set='utf8',
            fields_terminated=',',
            optionally_enclosed='"',
            lines_terminated='\\n',
            ignore_lines=1,
            is_replace=True,
            *args,
            **kwargs):
        """GCSのファイルをCloud SQLにロードするOperator

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

        python_callable = gcs_to_mysql_hook.execute

        op_kwargs = {
            'project_id': project_id,
            'gcs_path': gcs_path,
            'database_name': database_name,
            'table_name': table_name,
            'chara_set': chara_set,
            'fields_terminated': fields_terminated,
            'optionally_enclosed': optionally_enclosed,
            'lines_terminated': lines_terminated,
            'ignore_lines': ignore_lines,
            'is_replace': is_replace
        }

        super(GcsToMysqlOperator, self).__init__(python_callable=python_callable,
                                                 op_kwargs=op_kwargs,
                                                 *args,
                                                 **kwargs)
