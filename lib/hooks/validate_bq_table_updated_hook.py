from datetime import datetime
import re

from lib.logger import logger
from lib.hooks import bq_hook as bq
from lib.utils import datetime_util


class BigQueryTableValidator:

    def __init__(self,
                 project_id,
                 reference_datetime,
                 is_local=False):
        """初期化

        Args:
            project_id (str):
                対象プロジェクトID. Defaults to None.
            reference_datetime (datetime, optional):
                基準日時、対象テーブルの更新日が1つでもこの日時以前であれば例外を発生させる。
                Noneをセットした場合、当日の00:00:00を基準日時とする。
            local (bool, optional):
                ローカル環境で実行する場合はTrue. Defaults to False.
        """
        self.project_id = project_id
        self.is_local = is_local

        # 基準日時設定
        if reference_datetime is None:
            reference_datetime = datetime_util.today_jst
        elif not isinstance(reference_datetime, datetime):
            raise ValueError('引数: reference_datetimeにはdatetimeを設定してください')
        self.reference_datetime = reference_datetime
        logger.info('基準日時: {}'.format(self.reference_datetime.strftime(r'%Y-%m-%d %H:%M:%S')))

    def validate_dataset_table_updated(self, dataset_tables):
        """対象のテーブルの更新日を取得し、1つでもこの日時以前であれば例外を発生させる。

        Args:
            dataset_tables (dict):
                更新日をチェックするテーブルの一覧。
                データセットIDをキーに、テーブルをlistに格納したdictとする。
                ex:
                    {
                        'salesforce': ['Account', 'Order__c'],
                        'other_order': ['page_info'],
                    }
        """
        not_updated_tables = []
        for key in dataset_tables:
            not_updated_tables.extend(self.__get_not_updated_tables(key, dataset_tables[key]))
        if len(not_updated_tables) > 0:
            raise Exception('更新されていないテーブルがあります。{}'.format(not_updated_tables))

    def __get_not_updated_tables(self, dataset, tables):
        """対象データセット配下のテーブル、更新日を取得するSQLを実行し、
        対象テーブルの更新日時と基準日時を比較、更新日時が基準日時より前のテーブル一覧を返す。

        Args:
            dataset (str): 対象データセット
            tables (list): 対象テーブル一覧

        Returns:
            list: 更新日時が基準日時より前のテーブル一覧を返す。
        """
        if not re.fullmatch(r'[0-9a-zA-Z_]+', dataset):
            raise ValueError('引数: datasetが不正です。')

        # データセット配下のテーブル、更新日時を取得するSQL
        sql = '''
            # run query by lib.hooks.validate_bq_table_updated_hook.py
            SELECT
                table_id,
                DATETIME(TIMESTAMP_MILLIS(last_modified_time), 'Asia/Tokyo') AS last_modified_datetime
            FROM
                `{dataset}.__TABLES__`
            '''.format(dataset=dataset)
        sql += '''
            WHERE
                # [_2+7桁数字]を含むテーブルは日付サフィックステーブルと判定し、抽出対象外とする
                NOT REGEXP_CONTAINS(table_id, r'_2\d{7}')
                # 実テーブルのみ
                AND type = 1
            '''

        # SQL実行
        result = bq.bq_query(
            sql,
            project_id=self.project_id,
            is_return_result=True,
            by_query_str=True,
            local=self.is_local
        )
        if len(result) == 0:
            raise Exception('テーブルの更新日取得SQLの結果が0件でした。')

        # テーブル毎に更新日のチェック
        not_updated_tables = []
        for row in result:
            if row['table_id'] in tables:
                dataset_table = '{}.{}'.format(dataset, row['table_id'])
                logger.info('テーブル: {}'.format(dataset_table))
                logger.info('更新日時: {}'.format(row['last_modified_datetime'].strftime(r'%Y-%m-%d %H:%M:%S')))

                if self.reference_datetime > row['last_modified_datetime']:
                    logger.info('更新日時が基準日時より前です')
                    not_updated_tables.append(dataset_table)
        return not_updated_tables


def validate(dataset_tables,
             project_id,
             reference_datetime=None,
             is_local=False,
             is_dummy=False,
             **kwargs):
    """対象のテーブルの更新日を取得し、1つでも基準日時以前であれば例外を発生させる。

    Args:
        dataset_tables (dict):
            更新日をチェックするテーブルの一覧。
            データセットIDをキーに、テーブルをlistに格納したdictとする。
            ex:
                {
                    'salesforce': ['Account', 'Order__c'],
                    'other_order': ['page_info'],
                }
        project_id (str):
            対象プロジェクトID. Defaults to None.
        reference_datetime (datetime, optional):
            基準日時、対象テーブルの更新日が1つでもこの日時以前であれば例外を発生させる。
            Noneをセットした場合、当日の00:00:00を基準日時とする。
        is_local (bool, optional):
            ローカル環境で実行する場合はTrue. Defaults to False.
    """

    # スキップオプション判定
    try:
        if kwargs['params']['skip_validate_bq_table_updated']:
            logger.info('params:skip_validate_bq_table_updated がTrue、処理をスキップします。')
            return
    except KeyError:
        pass

    validator = BigQueryTableValidator(project_id, reference_datetime, is_local)
    validator.validate_dataset_table_updated(dataset_tables)


def validate_salesforce_dataset(tables,
                                project_id,
                                reference_datetime=None,
                                is_local=False,
                                is_dummy=False,
                                **kwargs):
    """salesforceデータセットの対象のテーブルの更新日を取得し、1つでも基準日時以前であれば例外を発生させる。
    salesforceデータセットは、差分連携テーブルの場合、
    Salesforceオブジェクト名、Salesforceオブジェクト名_id、の2テーブルが更新されるため、
    Salesforceオブジェクト名を引数で受けとり、Salesforceオブジェクト名_idの更新日時もチェックする。

    Args:
        tables (list):
            更新日をチェックするsalesforceデータセット配下のテーブルの一覧。
            _idで終わるテーブルは自動でチェックされるため不要。
            ex:
                ['Account', 'Order__c', 'Other_Order__c', 'kjb_matching__c']
        project_id (str):
            対象プロジェクトID. Defaults to None.
        reference_datetime (datetime, optional):
            基準日時、対象テーブルの更新日が1つでもこの日時以前であれば例外を発生させる。
            Noneをセットした場合、当日の00:00:00を基準日時とする。
        is_local (bool, optional):
            ローカル環境で実行する場合はTrue. Defaults to False.
    """

    # スキップオプション判定
    try:
        if kwargs['params']['skip_validate_bq_table_updated']:
            logger.info('params:skip_validate_bq_table_updated がTrue、処理をスキップします。')
            return
    except KeyError:
        pass

    target_tables = []
    for table in tables:
        target_tables.append(table)
        target_tables.append(table + '_id')
    validate(
        {'salesforce': target_tables},
        project_id,
        reference_datetime,
        is_local
    )
