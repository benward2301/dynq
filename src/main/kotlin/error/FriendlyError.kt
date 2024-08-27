package dynq.error

class FriendlyError(message: String) : RuntimeException(message) {

    fun printMessage() {
        System.err.print("Error: ${this.message}")
    }

}