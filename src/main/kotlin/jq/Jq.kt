package dynq.jq

import com.arakelian.jq.ImmutableJqLibrary
import com.arakelian.jq.ImmutableJqRequest
import com.arakelian.jq.JqRequest
import com.arakelian.jq.JqResponse
import org.apache.commons.lang3.StringEscapeUtils

fun jq(
    input: String,
    filter: String? = null,
    pretty: Boolean = false,
    colorize: Boolean = false,
    sortKeys: Boolean = false,
    onError: (response: JqResponse) -> Unit = throwJqError("bad filter")
): String {
    if (listOf(filter != null, pretty, colorize, sortKeys).none { it }) {
        return input
    }
    val response = PatchedJqRequest(
        ImmutableJqRequest.builder()
            .lib(ImmutableJqLibrary.of())
            .input(sanitizeControlChars(input))
            .filter(filter ?: ".")
            .pretty(pretty)
            .indent(JqRequest.Indent.SPACE)
            .sortKeys(sortKeys)
            .build(),
        colorize
    ).execute()
    if (response.hasErrors()) {
        onError(response)
    }
    return response.output
}

private fun sanitizeControlChars(input: String) = input.replace(Regex("[\u0000-\u001F]")) { StringEscapeUtils.escapeJson(it.value) }

