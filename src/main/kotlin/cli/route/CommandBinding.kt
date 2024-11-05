package dynq.cli.route

import dynq.cli.anno.CliOption
import dynq.cli.command.Command
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
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

        if (typeOf<Boolean>().isSubtypeOf(fn.returnType)) {
            return commandLine.hasOption(optionName) ||
                    (!fn.isAbstract && InvocationHandler.invokeDefault(proxy, method) as Boolean)
        }

        val raw: String? = commandLine.getOptionValue(optionName)
        return when {
            raw == null ->
                if (method.isDefault) InvocationHandler.invokeDefault(proxy, method) else null

            typeOf<Int>().isSubtypeOf(fn.returnType) ->
                Integer.parseInt(raw)

            else -> raw
        }
    }

    private fun buildApacheOptions(): Options {
        val options = Options()
        for (fn in cls.memberFunctions) {
            val anno = fn.findAnnotation<CliOption>() ?: continue
            val type = fn.returnType
            val hasArg = !typeOf<Boolean>().isSubtypeOf(type)
            val isRequired = hasArg && !type.isMarkedNullable && fn.isAbstract

            options.addOption(
                Option.builder()
                    .option(anno.short.takeIf { it.isLetter() }?.toString())
                    .longOpt(anno.long)
                    .hasArg(hasArg)
                    .required(isRequired)
                    .desc(anno.desc)
                    .build()
            )
        }
        return options
    }

}

fun interface CommandExecutor<T : Command> {
    fun accept(command: T)
}