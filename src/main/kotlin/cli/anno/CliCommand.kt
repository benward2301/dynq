package dynq.cli.anno

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class CliCommand(
    val name: String = "",
    val root: Boolean = false,
    val description: String = ""
)