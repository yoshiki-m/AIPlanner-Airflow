"""SlackにWebhookでメッセージ通知するモジュール
"""
import requests
import json
import urllib.parse

from airflow.models import Variable

# SLACK_URL = Variable.get('slack_webhook_url_secret')
# CHANNEL = Variable.get('slack_default_notify_channel')
# BASE_URL = Variable.get('slack_airflow_base_url')
# USERNAME = Variable.get('slack_default_user_name')
# ICON_EMOJI = Variable.get('slack_default_icon_emoji')
DEFAULT = 'default'


def notify_error_task(context):
    """slackへエラーメッセージを送信する
    lib/utils/notification.pyから呼び出す。

    Args:
        context (dict): Airflowのcontext
    """
    print('slack_notify_error_task')
#     # Airflow Web UIへのURL作成
#     url = '{}?dag_id={}&execution_date={}'.format(BASE_URL, context['dag'].dag_id, urllib.parse.quote(context['ts']))

#     # エラーメッセージを500文字以内に短縮
#     try:
#         error_message = str(context['exception'])
#         if len(error_message) > 500:
#             error_message = '{}\n(エラーメッセージの一部を表示しています。)'.format(error_message[0:500].strip())
#     except Exception:
#         error_message = context['exception']

#     # メッセージ本文
#     text = ''':aw_yeah::chikachika: AirflowのDAGでエラーが発生しました。:aw_yeah::chikachika:
# Owner: `{Owner}`
# DAG ID: {dag_id}
# ```
# Run ID: {run_id}
# Task: {operator}({task})
# Exception: {exception_type}
# Message: {message}
# ```
# 以下のURLを確認してください。
# {url}
# '''.format(Owner=context['dag'].owner,
#            dag_id=context['dag'].dag_id,
#            run_id=context['run_id'],
#            operator=type(context['task']).__name__,
#            task=context['task'].task_id,
#            exception_type=type(context['exception']).__name__,
#            message=error_message,
#            url=url)

#     # 通知先のSlackのチャンネル
#     channel = CHANNEL
#     try:
#         if context['params']['notification']['slack']['channel'] != DEFAULT:
#             channel = context['params']['notification']['slack']['channel']
#     except KeyError:
#         pass

#     payload = {
#         'channel': channel,
#         'username': USERNAME,
#         'text': text,
#         'icon_emoji': ICON_EMOJI,
#     }

#     data = json.dumps(payload)

#     requests.post(SLACK_URL, data)


def notify_message(message, slack_username='Airflow', slack_icon_emoji=':gcp:', slack_channel='#jinzai-gcp-notification'):
    """
    slackへメッセージを送信する

    Args:
    message (str): Slackの出力メッセージ
    slack_username (str): Slackユーザネーム
    slack_icon_emoji (str): Slackユーザの絵文字
    slack_channel (str): Slackのチャネル
    """
    print('slack_notify_message')
    # Slack
    # slack_url = Variable.get('slack_webhook_url_secret')

    # payload = {
    #     'channel': slack_channel,
    #     'username': slack_username,
    #     'text': message,
    #     'icon_emoji': slack_icon_emoji,
    # }

    # data = json.dumps(payload)

    # requests.post(slack_url, data)
