package dynq.cli.anno

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class CliOption(
    val long: String,
    val short: Char = ' ',
    val desc: String = ""
)
