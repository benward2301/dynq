package dynq.logging

fun style(vararg codes: Int) = fun(text: String): String {
    return escape(RESET) +
            codes.joinToString("", transform = ::escape) +
            text +
            escape(RESET) +
            escape(DEFAULT)
}

fun escape(code: Int) = "$ESCAPE[${code}m"

const val RESET = 0
const val BOLD = 1
const val FAINT = 2

const val RED = 31
const val GREEN = 32
const val YELLOW = 33
const val BLUE = 34
const val PURPLE = 35
const val CYAN = 36
const val WHITE = 37

const val DEFAULT = FAINT

