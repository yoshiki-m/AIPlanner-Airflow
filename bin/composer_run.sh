####################################################################################
# composer run コマンド関数定義
####################################################################################
##########################################################################
# 指定されたDAGを実行する。
#
# Params:
#     dag: DAG ID
##########################################################################
function run_dag () {
    dag=${1}
    execute_date=`date --date "9 hours ago" +"%Y-%m-%dT%H:%M:%S+00:00"`
    echo '#####################'
    echo '## DAGを実行します ##'
    echo '#####################'
    gcloud composer environments run \
        ${COMPOSER_ENVIRONMENT} --location ${COMPOSER_LOCATION} \
        trigger_dag -- ${dag} -e ${execute_date}
}

##########################################################################
# 指定されたDAGの指定されたTASKをテスト実行する。
# 内部的に airflow test コマンドが実行される。
#
# Params:
#     dag: DAG ID
#     task: TASK ID
##########################################################################
function run_task () {
    dag=${1}
    task=${2}
    execute_date=`date --date "9 hours ago" +"%Y-%m-%dT%H:%M:%S+00:00"`
    echo '############################'
    echo '## タスクを単体実行します ##'
    echo '############################'
    gcloud composer environments run \
        ${COMPOSER_ENVIRONMENT} --location ${COMPOSER_LOCATION} \
        test -- ${dag} ${task} ${execute_date}
}

