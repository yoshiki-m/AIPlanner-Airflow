"""BigQueryのテーブルの更新日を見て、更新済みか確認するOperatorクラス群
"""
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from lib.hooks import validate_bq_table_updated_hook


class ValidateBqTableUpdatedOperator(PythonOperator):

    ui_color = '#ffcce0'
    @apply_defaults
    def __init__(
            self,
            dataset_tables,
            project_id=None,
            reference_datetime=None,
            *args,
            **kwargs):
        """対象のテーブルの更新日を取得し、1つでも基準日時以前であれば例外を発生させる。
        kwargs['params']['skip_validate_bq_table_updated'] をFalseにすると、常に正常終了される。
        （デバッグ、テスト用）

        Args:
            dataset_tables (dict):
                更新日をチェックするテーブルの一覧。
                データセットIDをキーに、テーブルをlistに格納したdictとする。
                ex:
                    {
                        'salesforce': ['Account', 'Order__c'],
                        'other_order': ['page_info'],
                    }
            project_id (str, optional):
                対象プロジェクトID. Noneの場合はAirflowのプロジェクトIDを設定
            reference_datetime (datetime, optional):
                基準日時、対象テーブルの更新日が1つでもこの日時以前であれば例外を発生させる。
                Noneをセットした場合、当日の00:00:00を基準日時とする。
        """
        python_callable = validate_bq_table_updated_hook.validate

        # プロジェクトIDがNoneの場合はAirflowのプロジェクトIDを設定
        if project_id is None:
            project_id = Variable.get('project_id')

        op_kwargs = {
            'dataset_tables': dataset_tables,
            'project_id': project_id,
            'reference_datetime': reference_datetime,
        }

        super(ValidateBqTableUpdatedOperator, self).__init__(
            python_callable=python_callable,
            op_kwargs=op_kwargs,
            *args,
            **kwargs)


class ValidateBqSalesforceTableUpdatedOperator(PythonOperator):

    ui_color = '#faccff'
    @apply_defaults
    def __init__(
            self,
            tables,
            project_id=None,
            reference_datetime=None,
            *args,
            **kwargs):
        """salesforceデータセットの対象のテーブルの更新日を取得し、1つでも基準日時以前であれば例外を発生させる。
        salesforceデータセットは、差分連携テーブルの場合、
        Salesforceオブジェクト名、Salesforceオブジェクト名_id、の2テーブルが更新されるため、
        Salesforceオブジェクト名を引数で受けとり、Salesforceオブジェクト名_idの更新日時もチェックする。

        kwargs['params']['skip_validate_bq_table_updated'] をFalseにすると、常に正常終了される。
        （デバッグ、テスト用）


        Args:
            tables (list):
                更新日をチェックするsalesforceデータセット配下のテーブルの一覧。
                _idで終わるテーブルは自動でチェックされるため不要。
                ex:
                    ['Account', 'Order__c', 'Other_Order__c', 'kjb_matching__c']
            project_id (str, optional):
                対象プロジェクトID. Noneの場合はAirflowのプロジェクトIDを設定
            reference_datetime (datetime, optional):
                基準日時、対象テーブルの更新日が1つでもこの日時以前であれば例外を発生させる。
                Noneをセットした場合、当日の00:00:00を基準日時とする。
        """
        python_callable = validate_bq_table_updated_hook.validate_salesforce_dataset

        # プロジェクトIDがNoneの場合はAirflowのプロジェクトIDを設定
        if project_id is None:
            project_id = Variable.get('project_id')

        op_kwargs = {
            'tables': tables,
            'project_id': project_id,
            'reference_datetime': reference_datetime,
        }

        super(ValidateBqSalesforceTableUpdatedOperator, self).__init__(
            python_callable=python_callable,
            op_kwargs=op_kwargs,
            *args,
            **kwargs)
