package cli.logging.fmt

fun formatProgressMessage(
    icon: String,
    label: (() -> String)?,
    message: String = ""
): String {
    return "$icon ${label?.let { "${it()} " } ?: ""}$message"
}