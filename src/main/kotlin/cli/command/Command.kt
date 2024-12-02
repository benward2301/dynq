package dynq.cli.command

import dynq.cli.anno.CliOption

interface Command {

    @CliOption(
        long = VERBOSE,
        short = 'v'
    )
    fun verbose(): Boolean

    @CliOption(
        long = COLORIZE,
        precludes = [MONOCHROME]
    )
    fun colorize(): Boolean

    @CliOption(
        long = MONOCHROME
    )
    fun monochrome(): Boolean

}

private const val VERBOSE = "verbose"
private const val COLORIZE = "colorize"
private const val MONOCHROME = "monochrome"
