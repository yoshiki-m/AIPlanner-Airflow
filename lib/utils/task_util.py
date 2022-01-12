"""
airflow_db にアクセスする共通処理
"""

from airflow.hooks.postgres_hook import PostgresHook

from lib.errors.exception import NotCompletedTaskException
from lib.utils import datetime_util
from lib.logger import logger


def is_task_successed(dag_id,
                      task_id,
                      end_date=datetime_util.today_jst):
    """DAG ID、Task IDを指定し、正常終了したかどうかを確認する。
    実行結果はairflow_dbの「task_instance」テーブルを参照する。

    Args:
        dag_id (str): DAG ID
        task_id (str): TASK ID
        end_date (datetime): 確認したいTaskの終了日 Defaults to datetime_util.today_jst（日本時間今日）.

    Returns:
        bool: タスクが成功していればTrueを、そうでなければFalse

    Raises:
        ValueError: task_id_listの要素が0件
    """

    # airflow_dbに実行するSQLを作成
    sql = '''
        select
            ti.state
        from
            task_instance ti

            inner join (
                select
                    task_id,
                    max(end_date) as end_date
                from
                    task_instance
                where
                    dag_id = %s
                    and task_id = %s
                    and cast((end_date + INTERVAL '9 HOUR') as date) = %s
                group by
                    task_id
            ) ti_max
                on ti.task_id = ti_max.task_id
                and ti.end_date = ti_max.end_date
        '''

    # SQL実行
    hook = PostgresHook(postgres_conn_id='airflow_db')
    query_result = hook.get_records(sql, parameters=(dag_id, task_id, end_date.strftime('%Y%m%d')))

    # レコード件数が0件、またはステータスが'success'でなければFalse
    if len(query_result) == 0:
        return False
    elif query_result[0][0] != 'success':
        return False
    return True


def is_load_sf_object_task_successed(sf_object_tuple,
                                     end_date=datetime_util.today_jst):
    """SFオブジェクト名を複数指定し、正常終了したかどうかを確認する。
    正常終了していれば何もせず、そうでない場合は例外を発生させる。
    実行結果はairflow_dbの「task_instance」テーブルを参照する。

    Args:
        sf_object_tuple (tuple):SFオブジェクトのタプル
        end_date (datetime): 確認したいTaskの終了日 Defaults to datetime_util.today_jst（日本時間今日）.


    Raises:
        NotCompletedTaskException: SFオブジェクトのロードが完了していない場合
    """

    # airflow_dbに実行するSQLを作成
    sql = """
        select
            REPLACE(ti.task_id,'get_sf_object_','') as sf_object,
            ti.state
        from
            task_instance ti

            inner join (
                select
                    task_id,
                    max(end_date) as end_date
                from
                    task_instance
                where
                    dag_id LIKE 'COMMON_load_sf_object%%'
                    and REPLACE(task_id,'get_sf_object_','') IN %s
                    and cast((end_date + INTERVAL '9 HOUR') as date) = %s
                group by
                    task_id
            ) ti_max
                on ti.task_id = ti_max.task_id
                and ti.end_date = ti_max.end_date
    """

    # SQL実行
    hook = PostgresHook(postgres_conn_id='airflow_db')
    query_result = hook.get_records(sql, parameters=[sf_object_tuple, end_date.strftime('%Y%m%d')])

    # クエリ結果が0件の場合はエラー
    if len(query_result) == 0:
        raise NotCompletedTaskException('SFオブジェクト{}のロードが失敗しています。'.format(sf_object_tuple))

    # 結果をdictに変換
    result_dict = {}
    for q in query_result:
        result_dict[q[0]] = q[1]

    # 引数のSFオブジェクトタプルをループし、成功しているかチェック
    error_obj_list = []
    for obj in sf_object_tuple:
        try:
            if result_dict[obj] == 'success':
                logger.info('{}: 正常終了'.format(obj))
            else:
                logger.info('{}: 未完了'.format(obj))
                error_obj_list.append(obj)
        except KeyError:
            logger.info('{}: 未完了'.format(obj))
            error_obj_list.append(obj)

    # 終了判定
    if len(error_obj_list) > 0:
        raise NotCompletedTaskException('SFオブジェクト{}のロードが失敗しています。'.format(error_obj_list))
