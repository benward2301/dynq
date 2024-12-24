package dynq.cli.logging

import dynq.cli.route.CommandBinding
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.jline.terminal.TerminalBuilder
import org.jline.utils.InfoCmp
import java.util.*
import kotlin.math.ceil

class LogLine private constructor(
    val indent: Int,
    var label: (() -> String)?,
    val pos: Int
) {

    companion object {

        var enabled: Boolean = true
            get() = field && !CommandBinding.global.quiet()

        private val terminal = TerminalBuilder.builder()
            .system(true)
            .build()
        private val writer = terminal.writer()

        private val lines = TreeSet(
            Comparator.comparing<LogLine, Int> { ll -> ll.pos }
                .thenComparing(LogLine::hashCode)
        )
        private val nextPos
            get() = lines.lastOrNull()?.let { it.pos + 1 } ?: 0

        private val spinners = arrayOf('⠹', '⠼', '⠶', '⠧', '⠏', '⠛')
        private var ticks = 0

        init {
            CoroutineScope(Dispatchers.Default).launch {
                while (true) {
                    delay(30)
                    render()
                }
            }
        }

        @Synchronized
        fun new(
            indent: Int = 0,
            label: (() -> String)? = null,
            pos: Int = nextPos
        ): LogLine {
            return LogLine(indent, label, pos).also { lines.add(it) }
        }

        @Synchronized
        fun render() {
            if (enabled) {
                lines.forEach(LogLine::print)
                writer.print(escape(RESET))
                writer.flush()
                clear()
                ticks++
            }
        }

        private fun clear() {
            terminal.puts(InfoCmp.Capability.cursor_to_ll)
            repeat(lines.sumOf { it.rows }) {
                terminal.puts(InfoCmp.Capability.clr_eol)
                terminal.puts(InfoCmp.Capability.cursor_up)
            }
        }

    }

    private var content: String? = null
    private var rows: Int = 0

    fun log(message: () -> String) {
        if (enabled) {
            content = message()
        }
    }

    private fun print() {
        format()?.also { output ->
            writer.println(output)
            rows = output.replace(Regex("$ESCAPE\\[\\d*m"), "")
                .split("\n")
                .sumOf {
                    ceil((indent + it.length.toDouble()) / terminal.width)
                }.toInt()
        }
    }

    private fun format(): String? {
        return content?.let {
            "${" ".repeat(indent)}${it.replace(SPINNER, spinners[ticks % spinners.size])}"
        }
    }

}