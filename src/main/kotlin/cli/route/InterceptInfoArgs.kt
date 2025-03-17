package dynq.cli.route

import dynq.JQ_VERSION
import dynq.PROGRAM_NAME
import dynq.PROGRAM_VERSION
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Options
import kotlin.system.exitProcess

private const val HELP_OPT = "help"
private const val VERSION_OPT = "version"

fun interceptInfoArgs(commandName: String?, options: Options, args: Array<String>) {
    var syntax = "$PROGRAM_NAME [OPTION]..."
    if (commandName != null) {
        syntax += " $commandName"
    }
    when (args[0]) {
        "--$HELP_OPT" -> {
            HelpFormatter().also { it.width = 114 }.printHelp(syntax, options)
            exitProcess(0)
        }

        "--$VERSION_OPT" -> {
            println("$PROGRAM_NAME version $PROGRAM_VERSION")
            println("jq version $JQ_VERSION")
            exitProcess(0)
        }
    }
}
