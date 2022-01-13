"""メッセージ通知するモジュール
"""
from lib.utils import slack, chatwork


def notify_error_task(context):
    """エラーメッセージを通知する
    DAG定義pyファイルの、default_dag_argsのon_failure_callbackに、
    この関数を設定する。

    Args:
        context (dict): Airflowのcontext
    """
    print(context['params'])

    # # キー'notification'の有無チェック
    # if 'notification' in context['params']:
    #     if 'slack' in context['params']['notification']:
    #         # Slackに通知
    #         slack.notify_error_task(context)
    #     if 'chatwork' in context['params']['notification']:
    #         # chatworkに通知
    #         chatwork.notify_error_task(context)

