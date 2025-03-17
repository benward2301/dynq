package dynq.cli.command.option.postprocess

import dynq.cli.COMPATIBILITY_MODE
import dynq.logging.LogMode

class LogModePostprocessor : OptionPostprocessor<LogMode?> {
    override fun apply(value: LogMode?): LogMode {
        return value ?:
            if (System.getenv(COMPATIBILITY_MODE).isNullOrBlank())
                LogMode.RENDER else
                LogMode.APPEND
    }
}