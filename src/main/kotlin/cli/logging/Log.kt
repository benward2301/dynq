package dynq.cli.logging

fun log(message: () -> String): LogEntry {
    return LogEntry.new().also { it.log(message) }
}

fun warn(message: () -> String): LogEntry = log { style(YELLOW)("Warning: ${message()}") }

fun err(message: String) {
    LogEntry.new(override = true).also {
        it.log { style(RED)("Error: $message") }
    }
}
