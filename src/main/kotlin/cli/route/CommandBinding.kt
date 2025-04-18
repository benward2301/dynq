package dynq.cli.route

import com.fasterxml.jackson.databind.ObjectMapper
import dynq.cli.anno.CliOption
import dynq.cli.anno.constraints.Size
import dynq.cli.command.Command
import dynq.cli.command.option.INTEGER_ARG
import dynq.cli.command.option.OptionValidator
import dynq.cli.command.option.postprocess.OptionPostprocessor
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KType
import kotlin.reflect.full.*
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf
import kotlin.system.exitProcess

class CommandBinding<T : Command>(
    val cls: KClass<T>,
    val executor: CommandExecutor<T>
) {
    companion object {

        var global: Command = object : Command {
            override fun quiet() = true
            override fun logMode() = null
            override fun colorize() = false
            override fun monochrome() = false
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
            ?: throw Exception("Failed to infer ${KFunction::class.qualifiedName} from ${Method::class.qualifiedName}")
        val type = fn.returnType
        val values: Array<String> = commandLine.getOptionValues(anno.long)
            ?: if (anno.default.isNotEmpty()) arrayOf(anno.default) else emptyArray()

        @Suppress("UNCHECKED_CAST")
        return (anno.postprocessor.createInstance() as OptionPostprocessor<Any>).apply(
            when {
                satisfies<Boolean>(type) ->
                    commandLine.hasOption(anno.long)

                isEnum(fn.returnType) ->
                    ObjectMapper().readValue("\"${commandLine.getOptionValue(anno.long)}\"", type.jvmErasure.java)

                values.isEmpty() ->
                    null

                satisfies<Int>(type) ->
                    OptionValidator.Int.validate(fn, values[0].toInt())

                satisfies<Array<String>>(type) ->
                    OptionValidator.StringArray.validate(fn, values)

                else ->
                    OptionValidator.String.validate(fn, values[0])
            }
        )
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
                    .desc(buildDescription(anno, type))
                    .also { builder ->
                        buildArgName(anno, type)?.also { builder.argName(it) }
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
            ?.let { throw Exception("--${anno.long} $reason $it") }
    }
    anno.precludes.rejectIf("is incompatible with") { commandLine.hasOption(it) }
    anno.requires.rejectIf("requires") { !commandLine.hasOption(it) }
}

private inline fun <reified T> satisfies(type: KType): Boolean {
    return typeOf<T>().isSubtypeOf(type)
}

private fun isEnum(type: KType): Boolean {
    return type.jvmErasure.isSubclassOf(Enum::class)
}

private fun mapTypeToArgName(type: KType): String? {
    return when {
        satisfies<Int>(type) -> INTEGER_ARG
        else -> null
    }
}

private fun buildDescription(anno: CliOption, type: KType): String {
    val lines = mutableListOf<String>()
    if (anno.desc.isNotBlank()) {
        lines.add(anno.desc)
    }
    if (isEnum(type)) {
        lines.add("one of ${type.jvmErasure.java.enumConstants.map { it.toString().lowercase() }}")
    }
    if (anno.default.isNotBlank()) {
        lines.add("default ${anno.default}")
    }
    return lines.joinToString(separator = ";\n")
}

private fun buildArgName(anno: CliOption, type: KType): String? {
    return if (anno.args.isNotEmpty()) {
        anno.args.joinToString("> <")
    } else {
        mapTypeToArgName(type)
    }
}
