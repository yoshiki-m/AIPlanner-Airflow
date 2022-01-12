####################################################################################
# composerコマンド定義（呼び出し元ファイル）
####################################################################################

# Cloud Composerの環境名
COMPOSER_ENVIRONMENT=dev-jinzaisystem-composer-v2

# Cloud Composerのロケーション
COMPOSER_LOCATION=asia-northeast1

####################################################################################
# main
####################################################################################
subcommand=${1}
subcommand2=${2}
shift

case ${subcommand} in
    deploy)
        # composer deployで実行する関数を読み込み
        source $(cd $(dirname $0); pwd)/composer_deploy.sh
        if [ -z ${subcommand2} ]; then
            # composer deploy
            deploy_from_list
            exit 0
        fi
        case ${subcommand2} in
            # composer deploy add file_path
            add)
                deploy_add ${2}
                exit 0;;
            # composer deploy delete file_path
            delete)
                deploy_delete ${2}
                exit 0;;
            # composer deploy list
            list)
                deploy_list
                exit 0;;
            # composer deploy source file_path
            source)
                deploy_source ${2}
                exit 0;;
            # composer deploy -adls
            *)
                while getopts ":adls:" OPT
                do
                case ${OPT} in
                    a)
                        deploy_add ${2}
                        exit 0;;
                    d)
                        deploy_delete ${2}
                        exit 0;;
                    l)
                        deploy_list
                        exit 0;;
                    s)
                        deploy_source ${2}
                        exit 0;;
                    :)
                        echo  "[ERROR] Option argument is undefined."
                        exit -1;;
                    \?)
                        echo "[ERROR] Undefined options."
                        exit -1;;
                esac
                done
        esac
        echo "[ERROR] Undefined command."
        exit -1;;
    help)
        # helpテキストを表示
        less $(cd $(dirname $0); pwd)/doc/readme.txt
        ;;
    run)
        # composer runで実行する関数を読み込み
        source $(cd $(dirname $0); pwd)/composer_run.sh
        case ${subcommand2} in
            # composer run dag dag_id
            dag)
                run_dag ${2}
                exit 0;;
            # composer run task dag_id task_id
            task)
                run_task ${2} ${3}
                exit 0;;
        esac
        echo "[ERROR] Undefined command."
        exit -1;;
    *)
        echo "[ERROR] Undefined command."
        exit -1;;
esac