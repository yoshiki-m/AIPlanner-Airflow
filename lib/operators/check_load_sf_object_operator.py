"""SFのレポートを取得し、BigQueryにロードする。
"""

from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from lib.utils import task_util, datetime_util


class CheckLoadSfObjectOperator(PythonOperator):

    ui_color = '#D9FFF7'
    @apply_defaults
    def __init__(
            self,
            sf_object_tuple,
            end_date=datetime_util.today_jst,
            *args,
            **kwargs):
        """SFオブジェクト名を複数指定し、正常終了したかどうかを確認するOperator。
        正常終了していれば何もせず、そうでない場合は例外を発生させる。
        実行結果はairflow_dbの「task_instance」テーブルを参照する。

        Args:
            sf_object_tuple (tuple):SFオブジェクトのタプル
            end_date (datetime): 確認したいTaskの終了日 Defaults to datetime_util.today_jst（日本時間今日）).

        """

        python_callable = task_util.is_load_sf_object_task_successed
        op_kwargs = {
            'sf_object_tuple': sf_object_tuple,
            'end_date': end_date,
        }

        super(CheckLoadSfObjectOperator, self).__init__(python_callable=python_callable,
                                                        op_kwargs=op_kwargs,
                                                        *args,
                                                        **kwargs)
