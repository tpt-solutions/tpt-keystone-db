package tpt.sdk.android

/** A single record pushed over a Flux stream. */
data class FluxRecord(
    val offset: Long,
    val key: String?,
    val value: String?,
    val ts: Long,
)
