package dynq.cli.anno

import dynq.cli.command.option.postprocess.NoopOptionPostprocessor
import dynq.cli.command.option.postprocess.OptionPostprocessor
import kotlin.reflect.KClass

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class CliOption(
    val long: String,
    val short: Char = ' ',
    val desc: String = "",
    val default: String = "",
    val requires: Array<String> = [],
    val precludes: Array<String> = [],
    val args: Array<String> = [],
    val postprocessor: KClass<out OptionPostprocessor<*>> = NoopOptionPostprocessor::class
)
