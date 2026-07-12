package tpt.sdk.android

import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume

/**
 * Client for TPT Keystone's Canvas HTTP/JSON query bridge (`wire::http_query.rs`)
 * with offline-first caching, plus Flux WebSocket streaming (`wire::websocket.rs`,
 * Phase 11). The Canvas bridge is non-streaming (one JSON response per request);
 * Flux is the streaming surface.
 *
 * This module depends on OkHttp (see build.gradle.kts) for both HTTP and the
 * Flux WebSocket — the conventional Android choice. The SDK never imports
 * `androidx.compose` directly, so it works in both Views and Compose apps.
 */
class KeystoneClient(
    private val url: String,
    private val fluxUrl: String? = null,
    private val storage: Storage = InMemoryStorage(),
    private val cacheTtlMs: Long = 5 * 60 * 1000,
    private val headers: Map<String, String> = emptyMap(),
    private val httpClient: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .build(),
) {
    private val json = Json { ignoreUnknownKeys = true }

    /** Runs [query] against Canvas. On network failure, returns the last
     *  fresh cached result; otherwise rethrows. Successes refresh the cache. */
    suspend fun query(query: String, params: List<Any?> = emptyList()): QueryResult =
        queryInternal(query, params, offline = false)

    /** Forces a network round-trip, bypassing and refreshing the cache. */
    suspend fun refresh(query: String, params: List<Any?> = emptyList()): QueryResult {
        val result = fetch(query, params)
        writeCache(query, params, result)
        return result
    }

    /** Drops any cached result for a (query, params) pair. */
    suspend fun invalidate(query: String, params: List<Any?> = emptyList()) {
        storage.remove(cacheKey(query, params))
    }

    private suspend fun queryInternal(
        query: String,
        params: List<Any?>,
        offline: Boolean,
    ): QueryResult {
        val key = cacheKey(query, params)
        if (!offline) {
            try {
                val result = fetch(query, params)
                writeCache(key, params, result)
                return result
            } catch (e: Exception) {
                decodeCacheEntry(storage.getString(key), cacheTtlMs)?.let { return it }
                throw e
            }
        }
        return decodeCacheEntry(storage.getString(key), cacheTtlMs)
            ?: throw IllegalStateException("offline and no fresh cache for \"$query\"")
    }

    private fun fetch(query: String, params: List<Any?>): QueryResult {
        val bodyJson = buildJsonObject {
            put("query", JsonPrimitive(query))
            put(
                "params",
                JsonArray(params.map { JsonPrimitive(it?.toString()) ?: JsonNull }),
            )
        }.toString()

        val request = Request.Builder()
            .url(url)
            .post(bodyJson.toRequestBody("application/json".toMediaType()))
            .apply { headers.forEach { (k, v) -> addHeader(k, v) } }
            .build()

        val response: Response = httpClient.newCall(request).execute()
        if (!response.isSuccessful) {
            throw RuntimeException("Canvas responded ${response.code}: ${response.body?.string()}")
        }
        val text = response.body?.string().orEmpty()
        return json.decodeFromString(QueryResult.serializer(), text)
    }

    private suspend fun writeCache(key: String, params: List<Any?>, result: QueryResult) {
        storage.putString(key, encodeCacheEntry(result, System.currentTimeMillis()))
    }

    /**
     * Subscribes to a Flux [topic]. Returns a [WebSocket] handle whose [WebSocket.cancel]
     * closes the stream. Throws if [fluxUrl] is null.
     */
    fun subscribeFlux(
        topic: String,
        onRecord: (FluxRecord) -> Unit,
        onError: ((Throwable) -> Unit)? = null,
    ): WebSocket {
        val wsUrl = fluxUrl
            ?: throw IllegalStateException("KeystoneClient.subscribeFlux: no fluxUrl configured")

        return httpClient.newWebSocket(
            Request.Builder().url(wsUrl).build(),
            object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: okhttp3.Response) {
                    webSocket.send(Json.encodeToString(mapOf("subscribe" to topic)))
                }

                override fun onMessage(webSocket: WebSocket, text: String) {
                    try {
                        val obj = JSONObject(text)
                        onRecord(
                            FluxRecord(
                                offset = obj.optLong("offset"),
                                key = obj.optString("key", null),
                                value = obj.optString("value", null),
                                ts = obj.optLong("ts"),
                            ),
                        )
                    } catch (e: Exception) {
                        onError?.invoke(e)
                    }
                }

                override fun onFailure(
                    webSocket: WebSocket,
                    t: Throwable,
                    response: Response?,
                ) {
                    onError?.invoke(t)
                }
            },
        )
    }
}
