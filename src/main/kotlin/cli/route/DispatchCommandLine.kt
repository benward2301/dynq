package dynq.cli.route

import org.apache.commons.cli.ParseException
import kotlin.system.exitProcess

fun dispatchCommandLine(args: Array<String>): Nothing {
    try {
        val first = args.getOrNull(0)
        if (first == null || first.startsWith('-')) {
            lookupCommand()?.execute(null, args)
            System.err.println("No command specified")
        } else {
            lookupCommand(first)?.execute(first, args.sliceArray(1..<args.size))
            System.err.println("Unrecognised command: $first")
        }
    } catch (err: ParseException) {
        System.err.println(err.message)
    }
    exitProcess(1)
}
