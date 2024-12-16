package dynq

import dynq.cli.command.ReadCommand
import dynq.cli.route.CommandBinding
import dynq.cli.route.dispatchCommandLine
import dynq.cli.route.registerCommand
import dynq.executor.read.executeRead
import kotlin.system.exitProcess

const val PROGRAM_NAME = "dynq"
const val PROGRAM_VERSION = "0.1"
const val JQ_VERSION = "1.6"

fun main(args: Array<String>) {
    registerCommand(
        CommandBinding(ReadCommand::class, executeRead)
    )
    try {
        dispatchCommandLine(args)
    } catch (err: Error) {
        err.printStackTrace()
        exitProcess(1)
    }
}
