import os
import re

from airflow.models import Variable
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import From, Mail, Cc, Bcc

from lib.errors.exception import SendGridFailedToSendException
from lib.errors.exception import SendGridNotAllowedToSendException


class SendGridClient:
    """SendGridでのメール送信処理をラッピングしたクラス
    """

    def __init__(self,
                 friendly_From=None,
                 from_email=None,
                 api_key=None,
                 is_local=False):
        """初期化

        Args:
            friendly_From (str, optional): 差出人名、未設定の場合はデフォルトの値「Airflowから自動送信」を利用
            from_email (str, optional): 送信元メールアドレス、未設定の場合はVariables（ローカル実行時は環境変数）から取得して利用
            api_key (str, optional): APIキー、未設定の場合はVariables（ローカル実行時は環境変数）から取得して利用
            is_local (bool, optional): ローカルPCで動かす場合はTRUE. Defaults to False.

        Note:
            ローカルPCで動かす場合は以下の環境変数を設定すること。
                SENDGRID_API_KEY: APIキー
                SENDGRID_API_FROM_EMAIL: 上記APIキーで送信元となるメールアドレス
        """
        self.is_local = is_local

        # API Key
        if api_key is None:
            if self.is_local:
                api_key = os.environ['SENDGRID_API_KEY']
            else:
                api_key = Variable.get('sendgrid_api_key')

        # 送信元メールアドレスと差出人名からFROMオブジェクトを生成
        if friendly_From is None:
            friendly_From = 'Airflowから自動送信'

        if from_email is None:
            if self.is_local:
                from_email = os.environ['SENDGRID_API_FROM_EMAIL']
            else:
                from_email = Variable.get('sendgrid_api_from_email')
        self.from_ = From(
            email=from_email,
            name=friendly_From
        )

        self.__api_client = SendGridAPIClient(api_key)

    def __send(self, message):
        """APIコールしてメール送信する

        Args:
            message (sendgrid.helpers.mail.Mail): Mailオブジェクト

        Raises:
            SendGridFailedToSendException: APIコール時にステータス202以外が返ってきたとき
        """
        response = self.__api_client.send(message)
        if response.status_code != 202:
            raise SendGridFailedToSendException(
                'SendGridAPIが失敗しました。ステータスコード: {}, レスポンス: {}'.format(
                    response.status_code,
                    response.body
                )
            )

    def __validate_sendable_emails(self, emails):
        """宛先メールアドレスの送信可否を判定し、
        不正なメールアドレス、または送信が許可されていない宛先の場合、例外を発生させる。

        送信可能なメールアドレス
         - @以前が、「半角英数-_」である
         - ドメインが以下であること
           - @sms-s-s.com
           - @bm-sms.co.jp

        Args:
            emails (list, str): 宛先メールアドレス

        Raises:
            ValueError: パラメータemailsの型が不正なとき
            SendGridNotAllowedToSendException: 送信不可と判定されたとき
        """
        if isinstance(emails, str):
            emails = [emails]
        if not isinstance(emails, list):
            raise ValueError('emailsはstrまたはlistを指定してください。')

        not_matched_emails = []
        for email in emails:
            if not re.match(r'[A-Za-z0-9_\-\.]+\@(sms\-s\-s\.com|bm\-sms\.co\.jp)', email):
                not_matched_emails.append(email)

        if len(not_matched_emails) > 0:
            raise SendGridNotAllowedToSendException(
                '不正なメールアドレス、または送信が許可されていない宛先です。{}'.format(not_matched_emails)
            )

    def send_plain_text(self,
                        subject,
                        text,
                        to_emails,
                        cc_emails=None,
                        bcc_emails=None,
                        ):
        """プレーンテキストemailを送信する。

        Args:
            subject (str): 件名
            text (str): 本文
            to_emails (list, str): To 宛先メールアドレス
            cc_emails (list, str, optional): Cc 宛先メールアドレス. Defaults to None.
            bcc_emails (list, str, optional): Bcc 宛先メールアドレス. Defaults to None.
        """
        self.__validate_sendable_emails(to_emails)
        message = Mail(
            from_email=self.from_,
            subject=subject,
            to_emails=to_emails,
            plain_text_content=text)

        if cc_emails is not None:
            self.__validate_sendable_emails(cc_emails)
            message.add_cc(Cc(cc_emails))
        if bcc_emails is not None:
            self.__validate_sendable_emails(bcc_emails)
            message.add_bcc(Bcc(bcc_emails))

        self.__send(message)
