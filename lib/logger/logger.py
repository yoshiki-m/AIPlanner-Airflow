import logging
# ロギングの基本設定(infoレベルを指定)
logging.basicConfig(
    level=logging.DEBUG,
    # format='%(asctime)s %(levelname)-8s %(module)-18s %(funcName)-10s %(lineno)4s: %(message)s'
    format='%(levelname)-8s %(funcName)-10s %(lineno)4s: %(message)s'
)


def debug(msg):
    logging.debug(msg)


def info(msg):
    logging.info(msg)


def warn(msg):
    logging.warning(msg)


def error(msg, ex):
    logging.error(msg, ex)


# 確認
if __name__ == "__main__":
    debug("デバッグ")
    info("インフォ")
    warn("ワーニング")
    try:
        v = 3 / 0
    except Exception as ex:
        error("エラー", ex)