package dynq.logging

import dynq.cli.route.CommandBinding
import kotlinx.coroutines.*
import org.jline.terminal.TerminalBuilder
import org.jline.utils.InfoCmp
import java.util.*
import kotlin.math.ceil

class LogEntry private constructor(
    val indent: Int,
    var label: (() -> String)?,
    val pos: Int,
    val override: Boolean
) {

    companion object {

        var enabled: Boolean = true
            get() = field && !CommandBinding.global.quiet()

        private val terminal = TerminalBuilder.builder()
            .system(false)
            .streams(System.`in`, System.err)
            .build()
        private val writer = terminal.writer()

        private val entries = TreeSet(
            Comparator.comparing<LogEntry, Int> { le -> le.pos }
                .thenComparing(LogEntry::hashCode)
        )
        private val nextPos
            get() = entries.lastOrNull()?.let { it.pos + 1 } ?: 0

        private val spinners = arrayOf('⠹', '⠼', '⠶', '⠧', '⠏', '⠛')
        private var ticks = 0

        private val rendering = CoroutineScope(Dispatchers.Default).launch {
            while (true) {
                delay(30)
                render()
                clear()
            }
        }

        @Synchronized
        fun new(
            indent: Int = 0,
            label: (() -> String)? = null,
            pos: Int = nextPos,
            override: Boolean = false
        ): LogEntry {
            return LogEntry(indent, label, pos, override).also { entries.add(it) }
        }

        @Synchronized
        fun render() {
            entries.removeIf { it.print() && CommandBinding.global.logMode() == LogMode.APPEND }
            writer.flush()
            ticks++
        }

        fun transform(fn: (String?) -> String?) {
            entries.forEach { it.content = fn(it.content) }
        }

        suspend fun close() {
            render()
            enabled = false
            rendering.cancelAndJoin()
        }

        private fun clear() {
            if (CommandBinding.global.logMode() == LogMode.RENDER) {
                terminal.puts(InfoCmp.Capability.cursor_to_ll)
                repeat(entries.sumOf { it.lines }) {
                    terminal.puts(InfoCmp.Capability.clr_eol)
                    terminal.puts(InfoCmp.Capability.cursor_up)
                }
            }
        }

    }

    private var content: String? = null
    private var lines: Int = 0
    private val enabled: Boolean
        get() = LogEntry.enabled || override

    fun log(message: () -> String) {
        if (enabled) {
            content = message()
        }
    }

    private fun print(): Boolean {
        if (enabled) {
            format()?.also { output ->
                writer.println(output)
                lines = output.replace(Regex("$ESCAPE\\[\\d*m"), "")
                    .split("\n")
                    .sumOf {
                        ceil((indent + it.length.toDouble()) / terminal.width)
                    }.toInt()
                return@print true
            }
        }
        return false
    }

    private fun format(): String? {
        return content?.let {
            escape(DEFAULT) + " ".repeat(indent) + (
                if (CommandBinding.global.logMode() == LogMode.RENDER)
                    it.replace(SPINNER, spinners[ticks % spinners.size])
                else it
            ) + escape(RESET)
        }
    }

}