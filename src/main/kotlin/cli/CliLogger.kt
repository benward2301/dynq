package dynq.cli

import dynq.cli.route.CommandBinding

fun whisper(
    coroutineNumber: Int? = null,
    message: () -> String
) {
    if (CommandBinding.global.verbose()) {
        shout(coroutineNumber, message)
    }
}

fun shout(
    coroutineNumber: Int? = null,
    message: () -> String
) {
    var formatted = message()
    if (coroutineNumber != null) {
        formatted = "[$coroutineNumber] $formatted"
    }
    formatted = "* $formatted"
    if (CommandBinding.global.colorize()) {
        formatted = "\u001B[30m$formatted\u001B[0m"
    }
    System.err.println(formatted)
}
