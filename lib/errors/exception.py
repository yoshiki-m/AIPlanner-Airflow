class AuthenticationException(Exception):
    """認証が失敗したときの例外クラス
    """
    pass


class SoqlException(Exception):
    """SOQL実行に失敗したときの例外クラス
    """
    pass


class RequestException(Exception):
    """リクエストが失敗したときの例外クラス
    """
    pass


class MysqlInvalidDataException(Exception):
    """Mysqlのデータが不正だったときの例外クラス
    """
    pass


class GcsFileLockException(Exception):
    """GCS上にあるファイルに対する排他ロックの取得に失敗したときの例外クラス
    """
    pass


class NotCompletedTaskException(Exception):
    """タスクのステータスをチェックした際、完了していない場合の例外クラス
    """


class SendGridFailedToSendException(Exception):
    """SendGridのメール送信が失敗したときの例外クラス
    """


class SendGridNotAllowedToSendException(Exception):
    """SendGridで許可されていない宛先に送信しようとしたときの例外クラス
    """
