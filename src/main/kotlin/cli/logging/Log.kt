package dynq.cli.logging

fun log(message: () -> String): LogLine {
    return LogLine.new().also { it.log(message) }
}

fun warn(message: () -> String): LogLine = log { style(YELLOW)("Warning: ${message()}") }

fun err(message: String) = log { style(RED)("Error: $message") }