package dynq.cli.command

import dynq.cli.anno.CliOption

interface Command {

    @CliOption(
        long = QUIET,
        short = 'q'
    )
    fun quiet(): Boolean

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

private const val QUIET = "quiet"
private const val COLORIZE = "colorize"
private const val MONOCHROME = "monochrome"
