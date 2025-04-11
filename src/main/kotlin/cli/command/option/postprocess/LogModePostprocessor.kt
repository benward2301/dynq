package dynq.cli.command.option.postprocess

import dynq.logging.LogMode

class LogModePostprocessor : OptionPostprocessor<LogMode?> {
    override fun apply(value: LogMode?): LogMode {
        return value ?: LogMode.RENDER
    }
}