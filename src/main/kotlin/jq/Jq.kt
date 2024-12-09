package dynq.jq

import com.arakelian.jq.*
import java.io.File

fun jq(
    input: String,
    filter: String? = null,
    pretty: Boolean = false,
    colorize: Boolean = false,
    sortKeys: Boolean = false,
    label: String? = null
): String {
    if (listOf(filter != null, pretty, colorize, sortKeys).none { it }) {
        return input
    }
    val response = PatchedJqRequest(
        ImmutableJqRequest.builder()
            .lib(lib)
            .input(input)
            .filter(filter ?: ".")
            .pretty(pretty)
            .indent(JqRequest.Indent.SPACE)
            .sortKeys(sortKeys)
            .build(),
        colorize
    ).execute()
    if (response.hasErrors()) {
        throw Error(buildErrorMessage(response, label))
    }
    return response.output
}

private fun buildErrorMessage(response: JqResponse, label: String?): String {
    val nl = "\n  "
    return listOfNotNull("bad", label, "filter").joinToString(" ") +
            response.errors.map { it.replace("\n", " ") }.joinToString(nl, prefix = nl)
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