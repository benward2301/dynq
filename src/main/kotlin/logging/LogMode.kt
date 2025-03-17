package dynq.logging

import com.fasterxml.jackson.annotation.JsonCreator

enum class LogMode {
    RENDER,
    APPEND;

    companion object {
        @JvmStatic
        @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
        fun forValue(name: String): LogMode? {
            return entries.find { it.name == name.uppercase() }
        }
    }

}
