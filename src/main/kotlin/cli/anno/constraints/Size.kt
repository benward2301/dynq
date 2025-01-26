package dynq.cli.anno.constraints

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Size(
    val min: Int = 0,
    val max: Int = 0
)
