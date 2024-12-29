package dynq.cli.route

import dynq.cli.anno.CliOption
import dynq.cli.command.Command
import dynq.cli.command.option.INTEGER_ARG
import dynq.cli.command.option.OptionValidator
import jakarta.validation.constraints.Size
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
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
    companion object {

        var global: Command = object : Command {
            override fun quiet(): Boolean = true
            override fun colorize(): Boolean = false
            override fun monochrome(): Boolean = false
        }
            private set

    }

    fun execute(commandName: String?, args: Array<String>): Nothing {
        val options = buildApacheOptions()
        interceptInfoArgs(commandName, options, args)
        val commandLine = DefaultParser().parse(options, args)
        val proxy = createCommandProxy(commandLine)
        global = proxy
        this.executor.accept(proxy)
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
            verifyOptionDependencies(anno, commandLine)
            optionValues[method.name] = getOptionValue(commandLine, method, anno)
        }

        // comparison over Boolean cast
        if (optionValues[Command::colorize.name] == false && optionValues[Command::monochrome.name] == false) {
            val colorize = System.console()?.isTerminal ?: false
            optionValues[Command::colorize.name] = colorize
            optionValues[Command::monochrome.name] = !colorize
        }

        @Suppress("UNCHECKED_CAST")
        return proxy as T
    }

    private fun getOptionValue(
        commandLine: CommandLine,
        method: Method,
        anno: CliOption
    ): Any? {
        val fn = cls.memberFunctions.find { fn -> fn.name == method.name }
            ?: throw Error("Failed to infer ${KFunction::class.qualifiedName} from ${Method::class.qualifiedName}")
        val type = fn.returnType

        if (satisfies<Boolean>(type)) {
            return commandLine.hasOption(anno.long)
        }

        val values: Array<String> = commandLine.getOptionValues(anno.long)
            ?: if (anno.default.isNotEmpty()) arrayOf(anno.default) else emptyArray()

        return when {
            values.isEmpty() -> null

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
            val isRequired = hasArg && !type.isMarkedNullable && anno.default.isEmpty()

            options.addOption(
                Option.builder()
                    .option(anno.short.takeIf { it.isLetter() }?.toString())
                    .longOpt(anno.long)
                    .hasArg(hasArg)
                    .numberOfArgs(argCount?.max ?: if (hasArg) 1 else 0)
                    .optionalArg(argCount?.min != argCount?.max)
                    .required(isRequired)
                    .desc(anno.desc)
                    .also { builder ->
                        if (anno.args.isNotEmpty()) {
                            anno.args.joinToString("> <")
                        } else {
                            mapTypeToArgName(type)
                        }?.also { builder.argName(it) }
                    }.build()
            )
        }

        return options
    }

}

private fun verifyOptionDependencies(
    anno: CliOption,
    commandLine: CommandLine
) {
    if (!commandLine.hasOption(anno.long)) return

    fun Array<String>.rejectIf(
        reason: String,
        predicate: (String) -> Boolean
    ) {
        this.filter(predicate)
            .takeUnless { it.isEmpty() }
            ?.joinToString { "--$it" }
            ?.let { throw Error("--${anno.long} $reason $it") }
    }
    anno.precludes.rejectIf("is incompatible with") { commandLine.hasOption(it) }
    anno.requires.rejectIf("requires") { !commandLine.hasOption(it) }
}

private inline fun <reified T> satisfies(type: KType): Boolean {
    return typeOf<T>().isSubtypeOf(type)
}

private fun mapTypeToArgName(type: KType): String? {
    return when {
        satisfies<Int>(type) -> INTEGER_ARG
        else -> null
    }
}
