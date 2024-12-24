package dynq.jq

import com.arakelian.jq.*

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
            .input(input)
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

