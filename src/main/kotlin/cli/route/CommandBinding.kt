package dynq.cli.route

import dynq.cli.anno.CliOption
import dynq.cli.command.Command
import jakarta.validation.constraints.Size
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.memberFunctions
import kotlin.reflect.typeOf
import kotlin.system.exitProcess

class CommandBinding<T : Command>(
    val cls: KClass<T>,
    val executor: CommandExecutor<T>
) {

    fun execute(commandName: String?, args: Array<String>): Nothing {
        val options = buildApacheOptions()
        interceptInfoArgs(commandName, options, args)
        val commandLine = DefaultParser().parse(options, args)
        this.executor.accept(createCommandProxy(commandLine))
        exitProcess(0)
    }

    private fun createCommandProxy(commandLine: CommandLine): T {
        val optionValues = mutableMapOf<String, Any?>()

        val proxy = Proxy.newProxyInstance(
            cls.java.classLoader,
            arrayOf(cls.java)
        ) { _, method: Method, _ -> optionValues[method.name] }

        for (method in cls.java.methods) {
            val anno = method.getDeclaredAnnotation(CliOption::class.java) ?: continue
            optionValues[method.name] = getOptionValue(commandLine, proxy, method, anno.long)
        }
        @Suppress("UNCHECKED_CAST")
        return proxy as T
    }

    private fun getOptionValue(
        commandLine: CommandLine,
        proxy: Any,
        method: Method,
        optionName: String
    ): Any? {
        val fn = cls.memberFunctions.find { fn -> fn.name == method.name }
            ?: throw Error("Failed to infer ${KFunction::class.qualifiedName} from ${Method::class.qualifiedName}")
        val type = fn.returnType

        if (satisfies<Boolean>(type)) {
            return commandLine.hasOption(optionName) ||
                    (!fn.isAbstract && InvocationHandler.invokeDefault(proxy, method) as Boolean)
        }

        val values: Array<String>? = commandLine.getOptionValues(optionName)
        return when {
            values == null ->
                if (method.isDefault) InvocationHandler.invokeDefault(proxy, method) else null

            satisfies<Int>(type) ->
                OptionValidator.Int.validate(fn, values[0].toInt())

            satisfies<Array<String>>(type) ->
                OptionValidator.StringArray.validate(fn, values)

            else ->
                OptionValidator.String.validate(fn, values[0])
        }
    }

    private fun buildApacheOptions(): Options {
        val options = Options()
        for (fn in cls.memberFunctions) {
            val anno = fn.findAnnotation<CliOption>() ?: continue
            val type = fn.returnType
            val hasArg = !satisfies<Boolean>(type)
            val argCount = fn.takeIf { satisfies<Array<String>>(type) }?.findAnnotation<Size>()
            val isRequired = hasArg && !type.isMarkedNullable && fn.isAbstract

            options.addOption(
                Option.builder()
                    .option(anno.short.takeIf { it.isLetter() }?.toString())
                    .longOpt(anno.long)
                    .hasArg(hasArg)
                    .numberOfArgs(argCount?.max ?: if (hasArg) 1 else 0)
                    .optionalArg(argCount?.min != argCount?.max)
                    .required(isRequired)
                    .desc(anno.desc)
                    .build()
            )
        }
        return options
    }

}

private inline fun <reified T> satisfies(type: KType): Boolean {
    return typeOf<T>().isSubtypeOf(type)
}
