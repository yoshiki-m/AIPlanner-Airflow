"""
Salesforceオブジェクト連携 設定ファイル（本番用）
"""
from airflow.kubernetes.secret import Secret

# 全件連携 OR 差分連携 OR 履歴連携
ALL_LINKAGE = '全件連携'
DIFFERENCE_LINKAGE = '差分連携'
HISTORY_LINKAGE = '履歴連携'

# Dataloaderを実行するDocker Image
dataloader_image = 'gcr.io/aiplanner-258406/salesforce-dataloader:1.0.0'

# KubernetesPodOperatorに渡す認証情報
secrets = [Secret(
    deploy_type='env',
    deploy_target='SERVICE_ACCOUNT_CREDENTIALS',
    secret='service-account-token',
    key='credentials.json')]

# 取得するオブジェクト一覧
sf_object_list = {
    # 0:00
    'group0': [
        {'name': 'Account', 'link_type': ALL_LINKAGE},
        {'name': 'karte__c', 'link_type': ALL_LINKAGE},
    ],
}
