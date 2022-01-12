"""
Salesforceのオブジェクトを取得し、BigQueryにロードする。


処理概要:
- GKEのpodを作成し、Dockerコンテナを起動、内部で以下の処理を行う。
  - SFのオブジェクト取得
  - GCSにcsv.gzで出力
  - BigQueryにロード(前日のパーティション)

Salesforce,BigQueryの各環境への接続については、Variablesの以下の変数で定義している。
salesforce_to_bigquery_environment
  Airflow 本番環境： SF本番→BQ本番
  Airflow 開発環境： SF本番→BQ開発
SF開発→BQ開発が必要な場合は、開発環境のDAGを書き換えて利用すること。
"""

from datetime import datetime, timedelta

from airflow.models import DAG, Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

import common.config.load_sf_object_config as conf
from lib.utils import datetime_util, slack, notification


default_dag_args = {
    'start_date': datetime(2020, 2, 1, 21, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'owner': 'Sirius',
    'params': {
        'notification': {
            'slack': {
                'channel': slack.DEFAULT
            },
        }
    },
    'on_failure_callback': notification.notify_error_task,
}

with DAG(
        dag_id='COMMON_load_sf_object',
        schedule_interval='0 0 * * *',
        concurrency=4,
        default_args=default_dag_args,
        is_paused_upon_creation=True) as dag:

    bq_load_sf_object_tasks = []
    bq_load_sf_object_id_tasks = []
    cloud_sql_load_tasks = []
    bq_load_sf_object_id_task_count = 0

    # 設定ファイルのSFオブジェクト一覧に従い、タスクを作成
    # タスクの実行は並列数(concurrency)によって制御
    # タスク間の依存関係（優先度）は持たせない
    for i, v in enumerate(conf.sf_object_list['group0']):
        # Dataloaderを実行し、SFオブジェクトをGCSにcsv.gzファイルで出力、BigQueryにロード
        bq_load_sf_object_tasks.append(KubernetesPodOperator(
            task_id='get_sf_object_{}'.format(v['name']),
            name='get_sf_object_{}'.format(v['name']).lower().replace('_', '-'),
            namespace='default',
            image=conf.dataloader_image,
            is_delete_operator_pod=True,
            secrets=conf.secrets,
            cmds=[
                '/opt/dataloader/bin/execute.sh',
                Variable.get('salesforce_to_bigquery_environment'),
                v['name'],
                v['name'],
                '{}/common'.format(Variable.get('gcs_dags_folder')),
                datetime_util.yesterday_jst.strftime('%Y%m%d')
            ]
        ))

        # 差分連携の場合、IDのみを全件取得する。
        if v['link_type'] == conf.DIFFERENCE_LINKAGE:
            bq_load_sf_object_id_tasks.append(KubernetesPodOperator(
                task_id='get_sf_object_{}_id'.format(v['name']),
                name='get_sf_object_{}_id'.format(v['name']).lower().replace('_', '-'),
                namespace='default',
                image=conf.dataloader_image,
                is_delete_operator_pod=True,
                secrets=conf.secrets,
                cmds=[
                    '/opt/dataloader/bin/execute.sh',
                    Variable.get('salesforce_to_bigquery_environment'),
                    v['name'],
                    '{}_id'.format(v['name']),
                    '{}/common'.format(Variable.get('gcs_dags_folder')),
                    datetime_util.yesterday_jst.strftime('%Y%m%d')
                ]
            ))
            # タスクの依存関係を設定
            bq_load_sf_object_id_tasks[bq_load_sf_object_id_task_count] >> bq_load_sf_object_tasks[i]
            bq_load_sf_object_id_task_count += 1
