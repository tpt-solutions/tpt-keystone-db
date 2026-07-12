package tpt.sdk.android

import android.content.SharedPreferences
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/** Offline-cache storage. Default [SharedPreferencesStorage] persists across
 *  app restarts; [InMemoryStorage] does not. */
interface Storage {
    suspend fun getString(key: String): String?
    suspend fun putString(key: String, value: String)
    suspend fun remove(key: String)
}

/** SharedPreferences-backed [Storage] (Android's standard key-value store). */
class SharedPreferencesStorage(private val prefs: SharedPreferences) : Storage {
    override suspend fun getString(key: String): String? = prefs.getString(key, null)
    override suspend fun putString(key: String, value: String) {
        prefs.edit().putString(key, value).apply()
    }
    override suspend fun remove(key: String) {
        prefs.edit().remove(key).apply()
    }
}

/** Non-persistent in-memory [Storage], handy for previews and unit tests. */
class InMemoryStorage : Storage {
    private val store = mutableMapOf<String, String>()
    override suspend fun getString(key: String): String? = store[key]
    override suspend fun putString(key: String, value: String) {
        store[key] = value
    }
    override suspend fun remove(key: String) {
        store.remove(key)
    }
}

@Serializable
private data class CacheWrapper(val result: QueryResult, val ts: Long)

internal fun cacheKey(query: String, params: List<Any?>): String =
    "tpt:" + Json.encodeToString(
        mapOf("q" to query, "p" to params.map { it.toString() }),
    )

internal fun decodeCacheEntry(raw: String?, ttlMs: Long): QueryResult? {
    if (raw == null) return null
    return try {
        val wrapper = Json.decodeFromString<CacheWrapper>(raw)
        if (System.currentTimeMillis() - wrapper.ts > ttlMs) null else wrapper.result
    } catch (_: Exception) {
        null
    }
}

internal fun encodeCacheEntry(result: QueryResult, ttlClockMs: Long): String =
    Json.encodeToString(CacheWrapper(result, ttlClockMs))
