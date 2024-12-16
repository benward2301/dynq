package dynq.cli.logging

fun log(
    message: () -> String
): LogLine {
    return LogLine.new().also { it.log(message) }
}