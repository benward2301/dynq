package dynq.jq

fun String.pipe(arg: String?): String {
    return if (arg == null) this else "$this | $arg"
}