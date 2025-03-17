package dynq.cli.command.option.postprocess

interface OptionPostprocessor<T> {
    fun apply(value: T?): T?
}
