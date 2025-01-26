package dynq.cli.anno.constraints

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Min(val value: Int = 0)
