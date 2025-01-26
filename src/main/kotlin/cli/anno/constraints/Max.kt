package dynq.cli.anno.constraints

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Max(val value: Int = 0)
