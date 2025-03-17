package dynq.cli.command.option.postprocess

class NoopOptionPostprocessor : OptionPostprocessor<Any> {
    override fun apply(value: Any?) = value
}