"""chatworkにメッセージ通知する。
"""
import requests

from airflow.models import Variable

from lib.errors.exception import RequestException

DEFAULT = 'default'
DEFAULT_MESSAGE_FORMAT = '''[info][title](devil)AirflowのDAGでエラーが発生しました。(devil)[/title]DAG ID: {}
{}[/info]'''
DEFAULT_MESSAGE_BODY = '開発者に問い合わせください。'


def notify_chatwork(cw_roomid, notify_str):
    """chatworkへメッセージを送信する

    Args:
        cw_roomid (str): 宛先chatworkの部屋ID
        notify_str (str): 送信する本文
    """
    # chatworkのAPIKEY（開発 システム管理）
    cw_apikey = Variable.get('chatwork_apikey')
    # chatworkのENDPOINT
    cw_endpoint = Variable.get('chatwork_endpoint')

    # POSTするURLを作成
    post_message_url = '{}/rooms/{}/messages'.format(cw_endpoint, cw_roomid)
    headers = {'X-ChatWorkToken': cw_apikey}
    params = {'body': notify_str}
    res = requests.post(post_message_url,
                        headers=headers,
                        params=params)

    # Chatwork APIリクエスト失敗時の処理
    if res.status_code != 200:
        print('★通知メッセージ★')
        print(notify_str)
        print('★エラー内容★')
        print(res.status_code)
        print(res.content.decode('utf-8'))
        raise RequestException('Chatworkへの通知が失敗しました。')


def notify_error_task(context):
    """chatworkへエラーメッセージを送信する
    lib/utils/notification.pyから呼び出す。

    Args:
        context (dict): Airflowのcontext
    """
    # chatworkのAPIKEY（開発 システム管理）
    cw_apikey = Variable.get('chatwork_apikey')
    # chatworkのENDPOINT
    cw_endpoint = Variable.get('chatwork_endpoint')

    # 通知先のchatwork部屋ID
    cw_roomid = Variable.get('chatwork_default_room_id')
    try:
        if context['params']['notification']['chatwork']['room_id'] != DEFAULT:
            cw_roomid = context['params']['notification']['chatwork']['room_id']
    except KeyError:
        pass

    # メッセージ本文
    notify_str = DEFAULT_MESSAGE_FORMAT
    message_body = DEFAULT_MESSAGE_BODY
    try:
        if context['params']['notification']['chatwork']['message'] != DEFAULT:
            message_body = context['params']['notification']['chatwork']['message']
        notify_str = notify_str.format(context['dag'].dag_id,
                                       message_body)
    except KeyError:
        notify_str = notify_str.format(context['dag'].dag_id,
                                       message_body)
        pass

    # POSTするURLを作成
    post_message_url = '{}/rooms/{}/messages'.format(cw_endpoint, cw_roomid)
    headers = {'X-ChatWorkToken': cw_apikey}
    params = {'body': notify_str}
    requests.post(post_message_url,
                  headers=headers,
                  params=params)
