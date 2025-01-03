package dynq.cli.route

import dynq.cli.anno.CliCommand
import kotlin.reflect.full.findAnnotation

fun registerCommand(vararg bindings: CommandBinding<*>) {
    bindings.forEach(::registerCommand)
}

fun lookupCommand(name: String? = null): CommandBinding<*>? {
    if (name == null) {
        return rootCommand
    }
    return namedCommands[name]
}

private var rootCommand: CommandBinding<*>? = null
private val namedCommands: MutableMap<String, CommandBinding<*>> = HashMap()

private fun registerCommand(binding: CommandBinding<*>) {
    val cls = binding.cls
    val anno = cls.findAnnotation<CliCommand>() ?: throw Exception()

    if (anno.name.isBlank()) {
        if (anno.root) {
            registerRootCommand(binding)
        } else {
            throw Exception("No name specified for non-root command ${binding.cls}")
        }
    } else {
        if (namedCommands[anno.name] == null) {
            namedCommands[anno.name] = binding
        } else {
            throw Exception(
                "Multiple commands found with name ${anno.name}: "
                        + "${binding.cls}, ${namedCommands[anno.name]!!.cls}"
            )
        }
    }
}

private fun registerRootCommand(binding: CommandBinding<*>) {
    if (rootCommand == null) {
        rootCommand = binding
    } else {
        throw Exception("Multiple root commands found: ${rootCommand!!.cls}, ${binding.cls}")
    }
}