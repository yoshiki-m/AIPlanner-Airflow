"""DAGを記述するときに使うモジュール群
"""


def set_serial_tasks(tasks):
    """タスクの依存関係を設定、すべて直列で実行するように設定する。

    Args:
        tasks (list(Operator)): 直列で実行するOperatorのリスト
    """
    for i, task in enumerate(tasks):
        if i > 0:
            tasks[i - 1] >> tasks[i]
