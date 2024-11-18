package dynq.cli.route

fun dispatchCommandLine(args: Array<String>): Nothing {
    val first = args.getOrNull(0)
    if (first == null || first.startsWith('-')) {
        lookupCommand()?.execute(null, args)
        throw Error("no command specified")
    } else {
        lookupCommand(first)?.execute(first, args.sliceArray(1..<args.size))
        throw Error("unrecognised command: $first")
    }
}
