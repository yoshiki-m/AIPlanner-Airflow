# 指定ファイル1ファイルをデプロイする。
composer deploy -s sample/sample_dag.py 
composer deploy source sample/sample_dag.py 
    Cloud Composerが参照するGCSのバケットの、dagsフォルダ配下に上書きでアップロードする。
    ディレクトリ構成も維持されるため、Gitのディレクトリ構成通りにアップロードされる。

# 複数ファイルをまとめてデプロイする。
composer deploy
    `/bin/.deploy.list`にあるファイルをまとめてデプロイする。
    `/bin/.deploy.list`の参照、ファイルの追加、削除は以下のコマンドで行う。

# デプロイリストにファイルを追加
composer deploy -a sample/sample_dag.py 
composer deploy add sample/sample_dag.py 

# デプロイリストを標準出力
composer deploy -l
composer deploy list

# デプロイリストのファイルを削除
composer deploy -d sample/sample_dag.py 
composer deploy delete sample/sample_dag.py 
composer deploy delete sample/sample_da.*

# taskの単体テスト実行
composer run task [DAG ID] [TASK ID]
    DAGとTASKを指定し、TASKを単体でテスト実行する。
    同期的に実行されるので、タスクが終了するとコマンドも終了する。

# DAGトリガー実行
composer run dag [DAG ID]
    DAGを実行する。処理は非同期で、トリガー実行したらコマンドが正常終了するため、実行結果はAirflowのWebUIから確認する。
    Web UIから実行するのと同じなので、あまり意味ないかも。

composer help
    composerコマンドのhelpテキストを表示させる。