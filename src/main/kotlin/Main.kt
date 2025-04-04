package dynq

import dynq.cli.command.ReadCommand
import dynq.logging.LogEntry
import dynq.logging.err
import dynq.cli.route.CommandBinding
import dynq.cli.route.dispatchCommandLine
import dynq.cli.route.registerCommand
import dynq.executor.read.executeRead
import kotlinx.coroutines.runBlocking
import kotlin.system.exitProcess

const val PROGRAM_NAME = "dynq"
const val PROGRAM_VERSION = "0.1.0"
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
        runBlocking { LogEntry.close() }
        exitProcess(1)
    }
}

private fun disableLibLogging() {
    System.setProperty("java.util.logging.config.file", "")
}
