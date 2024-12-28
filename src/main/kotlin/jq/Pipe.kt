package dynq.jq

fun String.pipe(filter: String?): String {
    return _pipe(this, filter) as String
}

fun String?.pipeToNullable(filter: String?): String? {
    return _pipe(this, filter)
}

fun String?.pipeToNonNull(filter: String): String {
    return _pipe(this, filter) as String
}

private fun _pipe(input: String?, filter: String?): String? {
    return when {
        input == null -> filter
        filter == null -> input
        else -> "$input\n|\n$filter"
    }
}
