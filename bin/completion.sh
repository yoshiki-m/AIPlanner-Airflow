# composer コマンドの補完関数（TABをうつとでてくるやつ）
_composer()
{
    local cur=${COMP_WORDS[COMP_CWORD]}
    case "$COMP_CWORD" in
    1)
        COMPREPLY=( $(compgen -W "deploy run help" -- $cur) );;
    2)
        if [ ${COMP_WORDS[1]} = "deploy" ]; then
            COMPREPLY=( $(compgen -W "add delete list source" -- $cur) )
        elif [ ${COMP_WORDS[1]} = "run" ]; then
            COMPREPLY=( $(compgen -W "dag task" -- $cur) )
        else
            COMPREPLY=()
        fi;;
    *)
        COMPREPLY=()
    esac
}
# complete -F _composer composer
complete -o default -F _composer composer
