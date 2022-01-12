"""アクリート APIを実行するモジュール

Note:
    アクリート APIの仕様で、秒間30通以上SMSを送信できない。
"""
import json
import time
import urllib.parse
import urllib.request

from datetime import datetime, timedelta, timezone
import random
import requests


def send_sms(url,
             id,
             password,
             telno_list=[],
             text_list=[],
             shorten_url='yes',
             accept='*/*',
             Content_Type='application/x-www-form-urlencoded'):
    """SMSを送信する

    Args:
        url (str): アクリートAPIのURL
        id (str): アクリートAPIのID
        password (str): アクリートAPIのパスワード
        telno_list (list): 宛先電話番号のリスト。 Defaults to [].
        text_list (list): SMS本文のリスト。 Defaults to [].
        shorten_url (str): yesならURLを短縮する。それ以外の文字列の場合、短縮しない。 Defaults to 'yes'.
        accept (str): Acceptリクエストヘッダー。 Defaults to '*/*'.
        Content_Type (str):  Content-Type レスポンスヘッダー。 Defaults to 'application/x-www-form-urlencoded'.

    Returns:
        list: APIのレスポンスに電話番号と本文を追加したリスト
    """
    # 結果返却用のリスト
    send_list = []
    # HTTPリクエストの作成
    for telno, text in zip(telno_list, text_list):
        params = {
            'id': id,
            'pass': password,
            'telno': telno,
            'text': text,
            'shorten_url': shorten_url}
        headers = {
            'Accept': accept,
            'Content-Type': Content_Type,
            'Content-Length': str(len(text))
        }
        data = urllib.parse.urlencode(params)
        data = data.encode('utf-8')

        # SMS配信リクエストの送信（POST）
        print('★ requests.post() ★')
        try:
            response = requests.post(url, data=data, headers=headers)
            body = json.loads(response.content.decode('utf-8'))
        except Exception:
            # APIリクエストで例外発生した場合でも次の処理を続行する
            pass

        body['tel_number'] = telno
        body['text'] = text
        send_list.append(body)

    return send_list


def get_result(url,
               id,
               password,
               send_list,
               accept='*/*',
               Content_Type='application/x-www-form-urlencoded',
               retry_count=1,
               sleep_second=2):
    """SMSの送信結果を取得

    Args:
        url (str): アクリートAPIのURL
        id (str): アクリートAPIのID
        password (str): アクリートAPIのパスワード
        send_list (list): SMS送信APIのレスポンスに電話番号と本文を追加したリスト
        accept (str): Acceptリクエストヘッダー。 Defaults to '*/*'.
        Content_Type (str):  Content-Type レスポンスヘッダー。 Defaults to 'application/x-www-form-urlencoded'.
        retry_count (int): リトライ回数。 Defaults to 1.
        sleep_second (int): リトライ時のスリープ秒数。 Defaults to 2.

    Returns:
        list: APIのレスポンスに配信成功フラグと配信API結果コードを追加したリスト
    """

    # 結果返却用のリスト
    send_result_list = []
    # HTTPリクエストの作成
    params = {
        'id': id,
        'pass': password,
    }
    headers = {
        'Accept': accept,
        'Content-Type': Content_Type,
    }
    for send_dict in send_list:
        # リクエストパラメータ作成
        params['delivery_id'] = send_dict['delivery_id']
        data = urllib.parse.urlencode(params)
        data = data.encode('utf-8')

        #  SMS配信リクエストが失敗している場合、配信結果は取得しない
        try:
            if send_dict['result_code'] != '0000':
                send_dict['is_successed'] = '0'
                send_dict['get_result_code'] = None
                send_result_list.append(send_dict)
                continue
        except KeyError:
            # SMS配信リクエストが失敗していて、かつ「result_code」が存在しない場合
            send_dict['result_code'] = None
            send_dict['delivery_id'] = None
            send_dict['is_successed'] = '0'
            send_dict['get_result_code'] = None
            send_result_list.append(send_dict)
            continue

        # リトライ回数実行（成功したらループを抜ける）
        i = 1
        while True:
            # 配信結果取得リクエストの送信（POST）
            print(datetime.now())
            try:
                response = requests.post(url, data=data, headers=headers)
                body = json.loads(response.content.decode('utf-8'))

                if response.status_code == 200 and body['result_code'] == '0001':
                    # 200 0001 delivered SMS配信成功
                    send_dict['is_successed'] = '1'
                    send_dict['get_result_code'] = body['result_code']
                    # API実行成功のため、ループを抜ける
                    send_result_list.append(send_dict)
                    break
                elif response.status_code == 200 and body['result_code'] == '0003' and i < retry_count:
                    # (リトライ回数未到達で、レスポンスが200 0003 undefined 未確定の場合）
                    # 指定秒数スリーブして、リトライ
                    i = i + 1
                    print('sleep(' + str(sleep_second) + ')')
                    time.sleep(sleep_second)
                else:
                    # API実行失敗のため、ループを抜ける
                    send_dict['is_successed'] = '0'
                    send_dict['get_result_code'] = body['result_code']
                    send_result_list.append(send_dict)
                    break
            except Exception:
                # 例外発生時は、配信処理とみなし、ループを抜ける
                send_dict['is_successed'] = '0'
                send_dict['get_result_code'] = None
                send_result_list.append(send_dict)
                break

    return send_result_list


def run_dummy_sms_send(telno_list=[],
                       text_list=[]):
    """Accrete 配信APIのダミーのJSONレスポンスを作成して返す。
    実際には配信処理は実行されない、テスト用のメソッド。

    Args:
        telno_list (list): 宛先電話番号のリスト。 Defaults to [].
        text_list (list): SMS本文のリスト。 Defaults to [].

    Returns:
        list: APIのレスポンスに電話番号と本文を追加したリスト
    """
    if len(telno_list) > 99999 or len(text_list) > 99999:
        ValueError('配信レコード数は10万件未満としてください。')

    # 結果返却用のリスト
    send_list = []
    # タイムゾーン
    JST = timezone(timedelta(hours=+9), 'JST')
    # 5桁の乱数を生成
    rand = random.randint(10000, 99999)
    # 実行回数
    i = 0
    for telno, text in zip(telno_list, text_list):
        body = {
            'action': 'sms_reg',
            'telno': telno,
            'delivery_id': str(rand) + str(i).zfill(5),  # 10桁のダミーコード（5桁乱数+連番5桁ゼロ埋め）
            'result_code': '0000',
            'message': 'succeeded',
            'register_time': datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S')
        }

        body['tel_number'] = telno
        body['text'] = text
        send_list.append(body)
        i += 1
    return send_list


def run_dummy_get_result(send_list):
    """Accrete 配信結果確認APIのダミーのJSONレスポンスを作成して返す。
    実際にはAPIリクエスト処理は実行されない、テスト用のメソッド。

    Args:
        send_list (list): SMS送信APIのレスポンスに電話番号と本文を追加したリスト
    Returns:
        list: APIのレスポンスに配信成功フラグと配信API結果コードを追加したリスト
    """
    send_result_list = []
    for send_dict in send_list:
        send_dict['is_successed'] = '1'
        send_dict['get_result_code'] = '0000'
        send_result_list.append(send_dict)
    return send_result_list


# ローカル環境実行用
# if __name__ == "__main__":
#     telno_list = []
#     text_list = []
#     for i in range(20):
#         telno_list.append('090XX' + str(i).zfill(5))
#         text_list.append(
#             '''【ダミー本文】
#             {}様へのオススメの求人
#             '''.format(str(i)))
#     send_list = run_dummy_sms_send(telno_list, text_list)
#     result = run_dummy_get_result(send_list)
#     print(result)
