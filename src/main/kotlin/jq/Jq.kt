package dynq.jq

import com.arakelian.jq.ImmutableJqLibrary
import com.arakelian.jq.ImmutableJqRequest
import com.arakelian.jq.JqLibrary
import com.arakelian.jq.JqRequest
import java.io.File

fun jq(
    input: String,
    filter: String? = null,
    pretty: Boolean = false,
    colorize: Boolean = false,
    sortKeys: Boolean = false
): String? {
    if (listOf(filter != null, pretty, colorize, sortKeys).none { it }) {
        return input
    }
    val output = PatchedJqRequest(
        ImmutableJqRequest.builder()
            .lib(lib)
            .input(input)
            .filter(filter ?: ".")
            .pretty(pretty)
            .indent(JqRequest.Indent.SPACE)
            .sortKeys(sortKeys)
            .build(),
        colorize
    ).execute().output
    if (output.isEmpty()) {
        return null
    }
    return output
}

private val lib = ImmutableJqLibrary.of()

private class PatchedJqRequest(
    private val from: JqRequest,
    private val colour: Boolean
) : JqRequest() {

    override fun getInput(): String {
        return this.from.input
    }

    override fun getFilter(): String {
        return this.from.filter
    }

    override fun getLib(): JqLibrary {
        return this.from.lib
    }

    override fun getModulePaths(): MutableList<File> {
        return this.from.modulePaths
    }

    override fun isSortKeys(): Boolean {
        return this.from.isSortKeys
    }

    override fun getDumpFlags(): Int {
        var flags = 0

        if (this.from.isPretty) {
            flags = flags or 1
        }

        if (this.colour) {
            flags = flags or 4
        }

        when (this.from.indent) {
            Indent.TAB -> flags = flags or 64
            Indent.SPACE -> flags = flags or 512
            Indent.TWO_SPACES -> flags = flags or 1024
            Indent.NONE, null -> {}
        }

        if (this.from.isSortKeys) {
            flags = flags or 8
        }

        return flags
    }

}