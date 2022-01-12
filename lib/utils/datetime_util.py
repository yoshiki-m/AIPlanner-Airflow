"""datetime系共通モジュール
"""

from datetime import datetime, timedelta, timezone

# タイムゾーン
JST = timezone(timedelta(hours=+9), 'JST')
TODAY = 'today'
YESTERDAY = 'yesterday'
today_jst = datetime.combine(datetime.now(JST), datetime.min.time())
yesterday_jst = datetime.combine(datetime.now(JST) - timedelta(1), datetime.min.time())
tomorrow_jst = datetime.combine(datetime.now(JST) + timedelta(1), datetime.min.time())


def get_target_date(default_date_type=TODAY, **kwargs):
    """DAG内で利用する対象日をdatetimeで作成する。
    デフォルトでは実行日当日を返す。
    DAGの対象日を通常は実行日、リカバリ時は前日以前としたい場合、
    前日以前のリカバリはCLIから行う。
    target_dateに設定した日付を対象日とする。

        # コマンド実行例
        gcloud beta composer environments run jinzaisystem-composer \
            --location="asia-northeast1" trigger_dag -- [YOUR DAG ID] \
            --conf '{"target_date": "2019-12-14"}'

    Args:
        default_date_type (str): datetime_util.TODAY or datetime_util.YESTERDAY Defaults to TODAY.

    Returns:
        datetime: 対象日
    """
    target_date = today_jst
    # DAGを実行時に渡した引数があればその日付を取得
    try:
        target_date = datetime.strptime(kwargs['dag_run'].conf['target_date'], '%Y-%m-%d')
    finally:
        # default_date_typeがyesterdayの場合は実行日前日を設定
        if default_date_type == YESTERDAY:
            target_date = yesterday_jst
        return target_date
