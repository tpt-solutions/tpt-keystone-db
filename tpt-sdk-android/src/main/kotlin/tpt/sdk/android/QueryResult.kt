package tpt.sdk.android

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive

/** Result of a Canvas query: `columns` plus rows of arbitrary JSON cells. */
@Serializable
data class QueryResult(
    val columns: List<String>,
    val rows: List<JsonArray>,
)

/** Exposes a JSON cell as a plain Kotlin value (String/Boolean/Double/null). */
fun JsonElement.asValue(): Any? = when (this) {
    is JsonNull -> null
    is JsonPrimitive -> when {
        booleanOrNull != null -> boolean
        isString -> content
        else -> content.toDoubleOrNull()
    }
    else -> this
}
