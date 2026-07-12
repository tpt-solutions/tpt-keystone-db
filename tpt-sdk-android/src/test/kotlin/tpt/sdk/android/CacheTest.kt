package tpt.sdk.android

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class CacheTest {
    private fun sample() = QueryResult(
        columns = listOf("id"),
        rows = listOf(JsonArray(listOf(JsonPrimitive("1")))),
    )

    @Test
    fun `encode then decode within ttl returns same result`() {
        val result = sample()
        val raw = encodeCacheEntry(result, System.currentTimeMillis())
        val decoded = decodeCacheEntry(raw, 5 * 60 * 1000)
        assertEquals(result.columns, decoded?.columns)
        assertEquals("1", (decoded?.rows?.first()?.get(0) as JsonPrimitive).content)
    }

    @Test
    fun `decode beyond ttl returns null`() {
        val result = sample()
        val raw = encodeCacheEntry(result, System.currentTimeMillis() - 10 * 60 * 1000)
        assertNull(decodeCacheEntry(raw, 5 * 60 * 1000))
    }

    @Test
    fun `decode of garbage returns null`() {
        assertNull(decodeCacheEntry("not json", 5 * 60 * 1000))
        assertNull(decodeCacheEntry(null, 5 * 60 * 1000))
    }

    @Test
    fun `cache key is stable for identical query and params`() {
        assertEquals(cacheKey("SELECT 1", listOf(1)), cacheKey("SELECT 1", listOf(1)))
    }
}
