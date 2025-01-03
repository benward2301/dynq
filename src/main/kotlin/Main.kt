package dynq

import dynq.cli.command.ReadCommand
import dynq.cli.logging.LogEntry
import dynq.cli.logging.err
import dynq.cli.route.CommandBinding
import dynq.cli.route.dispatchCommandLine
import dynq.cli.route.registerCommand
import dynq.executor.read.executeRead
import kotlin.system.exitProcess

const val PROGRAM_NAME = "dynq"
const val PROGRAM_VERSION = "0.1"
const val JQ_VERSION = "1.6"

fun main(args: Array<String>) {
    disableLibLogging()
    registerCommand(
        CommandBinding(ReadCommand::class, executeRead)
    )
    try {
        dispatchCommandLine(args)
    } catch (e: Exception) {
        err(e.message!!)
        LogEntry.close()
        exitProcess(1)
    }
}

private fun disableLibLogging() {
    System.setProperty("java.util.logging.config.file", "")
}
