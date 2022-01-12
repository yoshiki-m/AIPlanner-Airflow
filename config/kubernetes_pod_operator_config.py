"""KubernetesPodOperatorクラスの共通パラメータ
"""

"""
namespace

Kubernetesのデフォルトのnamespaceを定義
"""
namespace = 'default'

"""
affinity

別ノードプール、"gke-nodepool"にPodを作成し処理を実行できる。
高負荷な処理実行時に設定を推奨
"""
affinity = {
    'nodeAffinity': {
        'requiredDuringSchedulingIgnoredDuringExecution': {
            'nodeSelectorTerms': [{
                'matchExpressions': [{
                    'key': 'cloud.google.com/gke-nodepool',
                    'operator': 'In',
                    'values': [
                        'gke-nodepool',
                    ]
                }]
            }]
        }
    }
}
