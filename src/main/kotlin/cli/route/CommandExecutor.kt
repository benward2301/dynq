package dynq.cli.route

import dynq.cli.command.Command

fun interface CommandExecutor<T : Command> {
    fun accept(command: T)
}