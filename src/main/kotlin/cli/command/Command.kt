package dynq.cli.command

import dynq.cli.anno.CliOption
import dynq.cli.command.option.postprocess.LogModePostprocessor
import dynq.logging.LogMode

interface Command {

    @CliOption(
        long = QUIET,
        short = 'q'
    )
    fun quiet(): Boolean

    @CliOption(
        long = LOG_MODE,
        postprocessor = LogModePostprocessor::class,
    )
    fun logMode(): LogMode?

    @CliOption(
        long = COLORIZE,
        short = 'C',
        precludes = [MONOCHROME]
    )
    fun colorize(): Boolean

    @CliOption(
        long = MONOCHROME,
        short = 'M'
    )
    fun monochrome(): Boolean

}

private const val QUIET = "quiet"
private const val LOG_MODE = "log-mode"
private const val COLORIZE = "colorize"
private const val MONOCHROME = "monochrome"
