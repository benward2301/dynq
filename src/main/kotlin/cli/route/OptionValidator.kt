package dynq.cli.route

import dynq.cli.anno.CliOption
import jakarta.validation.constraints.*
import kotlin.reflect.KFunction
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.hasAnnotation

fun interface OptionValidator<T, R> {

    fun validate(
        producer: KFunction<*>,
        value: T
    ): R

    companion object {

        private fun <T> wrapValidator(
            validator: OptionValidator<T, List<String?>>
        ): OptionValidator<T, T> {
            return OptionValidator { producer, value ->
                val option = producer.findAnnotation<CliOption>()
                    ?: throw Error("$producer is not annotated with ${CliOption::class}")

                validator.validate(producer, value)
                    .filterNotNull()
                    .takeUnless { it.isEmpty() }
                    ?.joinToString(", ")
                    ?.let { throw Error("--${option.long} $it") }
                    ?: value
            }
        }

        val Int = wrapValidator<Int> { producer, value ->
            listOf(
                producer.findAnnotation<Min>()?.value
                    ?.takeUnless { value >= it.toInt() }
                    ?.let { "must be greater than or equal to $it" },

                producer.findAnnotation<Max>()?.value
                    ?.takeUnless { value <= it.toInt() }
                    ?.let { "must be less than or equal to $it" },

                producer.hasAnnotation<Positive>()
                    .takeUnless { value > 0 }
                    ?.let { "must be positive" }
            )
        }

        val String = wrapValidator<String> { producer, value ->
            listOf(
                producer.findAnnotation<Size>()
                    ?.takeUnless { value.length >= it.min }
                    ?.takeUnless { value.length <= it.max }
                    ?.let { "must be between ${it.min} and ${it.max} characters long" },

                producer.findAnnotation<Pattern>()
                    ?.takeUnless { Regex(it.regexp).matches(value) }
                    ?.let { "must match pattern /$it/" },
            )
        }

        val StringArray = wrapValidator<Array<String>> { producer, value ->
            listOf(
                producer.findAnnotation<Size>()
                    ?.takeUnless { value.size >= it.min }
                    ?.let { "must be passed ${it.min} argument(s)" },

                producer.findAnnotation<Size>()
                    ?.takeUnless { value.size <= it.max }
                    ?.let { "cannot be passed more than ${it.max} argument(s)" }
            )
        }
    }
}
