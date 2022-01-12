####################################################################################
# composer deploy コマンド関数定義
####################################################################################
# デプロイリスト
DEPLOY_LIST=$(cd $(dirname $0); pwd)/.deploy.list
##########################################################################
# 引数で受け取ったファイルをデプロイリスト追加する。
#
# Params:
#     file_path: デプロイするファイルのパス。GCSのdagsフォルダ配下に、
#                同じディレクトリ構成で上書きでアップロードされる。
##########################################################################
function deploy_add () {

    file_path=${1}
    # デプロイリストがない場合は新規作成
    if [ ! -f ${DEPLOY_LIST} ]; then
        echo "[Info] create new file: ${DEPLOY_LIST}"
        touch ${DEPLOY_LIST}
    fi
    # 存在しないファイルの場合は異常終了
    if [ ! -f ${file_path} ]; then
        echo "[Failed]    not found ${file_path}"
        exit -1
    fi
    # デプロイリストに完全一致する行がなければリストに追加
    if [ ! `grep -cx "${file_path}" ${DEPLOY_LIST}` -gt 0 ]; then
        echo "${file_path}" >> ${DEPLOY_LIST}
    fi
} 
##########################################################################
# 引数で受け取ったファイルをデプロイリストから削除する。
#
# Params:
#     file_path: デプロイするファイルのパス。GCSのdagsフォルダ配下に、
#                同じディレクトリ構成で上書きでアップロードされる。
##########################################################################
function deploy_delete () {

    file_path=${1}
    # デプロイリストがない場合はなにもしない
    if [ ! -f ${DEPLOY_LIST} ]; then
        echo "[Failed]    not found ${DEPLOY_LIST}"
        touch ${DEPLOY_LIST}
    fi
    grep -vx "${file_path}" ${DEPLOY_LIST} > ${DEPLOY_LIST}_tmp
    mv ${DEPLOY_LIST}_tmp ${DEPLOY_LIST}
} 
##########################################################################
# デプロイリストを表示する。
##########################################################################
function deploy_list () {

    # デプロイリストがない場合はなにもしない
    if [ ! -f ${DEPLOY_LIST} ]; then
        echo "[Failed]    not found ${DEPLOY_LIST}"
        touch ${DEPLOY_LIST}
    fi
    cat ${DEPLOY_LIST}
} 
##########################################################################
# 引数で受け取ったファイルをCloud Composerが参照するGCSにアップロードする。
#
# Params:
#     file_path: デプロイするファイルのパス。GCSのdagsフォルダ配下に、
#                同じディレクトリ構成で上書きでアップロードされる。
##########################################################################
function deploy_source () {

    file_path=${1}
    if [ ! -f ${file_path} ]; then
        echo "[Failed]    not found ${file_path}"
    else
        gcloud composer environments storage dags import \
            --environment ${COMPOSER_ENVIRONMENT} \
            --location ${COMPOSER_LOCATION} \
            --source ${file_path}
        if [ ${?} -eq 0 ]; then
            echo "[Successed] deploy ${file_path}"
        else
            echo "[Failed]    deploy ${file_path}"
        fi
    fi
} 
##########################################################################
# デプロイリストにあるファイルをCloud Composerが参照するGCSにアップロードする。
#
##########################################################################
function deploy_from_list () {

    cd ${CLOUD_COMPOSER_SOURCE_DIR}
    echo '[Info]      deploy all files in list'
    # リストに従い処理
    cat ${DEPLOY_LIST} | while read f
    do
        deploy_source ${f}
    done
}
