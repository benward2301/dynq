package dynq.jq

import com.arakelian.jq.JqResponse

fun wrapJqError(message: String) = fun(response: JqResponse): Exception {
    val nl = "\n  "
    return Exception(
        message + response.errors.map {
            it.replace("\n", " ")
        }.joinToString(nl, prefix = nl)
    )
}

fun throwJqError(message: String) = fun(response: JqResponse) {
    throw wrapJqError(message)(response)
}

