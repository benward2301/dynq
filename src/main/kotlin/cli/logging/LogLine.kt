package dynq.cli.logging

import dynq.cli.route.CommandBinding
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.jline.terminal.TerminalBuilder
import org.jline.utils.InfoCmp
import java.util.*

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
            InfoCmp.Capability.cursor_down
            return LogLine(indent, label, pos).also { lines.add(it) }
        }

        private fun render() {
            if (enabled) {
                terminal.puts(InfoCmp.Capability.cursor_to_ll)
                repeat(lines.filter { it.visible }.size) {
                    terminal.puts(InfoCmp.Capability.cursor_up)
                }
                for (ll in lines) {
                    terminal.puts(InfoCmp.Capability.clr_eol)
                    val formatted = ll.format()
                    ll.visible = formatted != null
                    if (ll.visible) writer.println(formatted)
                }
                writer.print(escape(RESET))
                writer.flush()
                ticks++
            }
        }

    }

    private var content: String? = null
    private var visible: Boolean = false

    fun log(message: () -> String) {
        if (enabled) {
            content = message()
        }
    }

    private fun format(): String? {
        return content?.let {
            "${" ".repeat(indent)}${it.replace(SPINNER, spinners[ticks % spinners.size])}"
        }
    }

}