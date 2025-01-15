package dynq.jq

fun slurp(filter: String): String {
    return "[$filter]".pipe("if length == 1 then .[0] else . end")
}
