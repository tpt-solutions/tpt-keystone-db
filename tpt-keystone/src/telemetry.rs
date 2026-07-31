//! Tracing/OpenTelemetry initialization (Phase 12 — production hardening).
//!
//! Spans always go to stdout via `tracing_subscriber::fmt`, same as before.
//! If `OTEL_EXPORTER_OTLP_ENDPOINT` is set *and* this build has the `otlp`
//! Cargo feature (default on), spans are additionally exported over
//! OTLP/gRPC to a collector (Jaeger, Tempo, the OTel Collector, etc.). With
//! no endpoint configured, the OTel layer is simply omitted — a node with no
//! collector pays nothing beyond the pre-existing `tracing` overhead. The
//! `otlp` feature exists because OTLP export drags in `tonic`/`hyper`/`h2`/
//! `prost` on top of the `opentelemetry` crates themselves — a build that
//! doesn't need collector export shouldn't pay to compile/link that stack.

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

fn filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

#[cfg(feature = "otlp")]
pub fn init() {
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use opentelemetry_sdk::Resource;

    let fmt_layer = tracing_subscriber::fmt::layer();

    let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") else {
        tracing_subscriber::registry()
            .with(filter())
            .with(fmt_layer)
            .init();
        return;
    };

    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
    {
        Ok(exporter) => exporter,
        Err(e) => {
            eprintln!("failed to build OTLP exporter for {endpoint}: {e}; tracing spans will not be exported to a collector");
            tracing_subscriber::registry()
                .with(filter())
                .with(fmt_layer)
                .init();
            return;
        }
    };

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name("tpt-keystone")
                .build(),
        )
        .build();
    let tracer = opentelemetry::trace::TracerProvider::tracer(&provider, "tpt-keystone");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(filter())
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    // Leaked deliberately: this provider's background batch-export task must
    // outlive `init()` for the process's whole lifetime, and there's no
    // natural owner to hold it (main() doesn't otherwise keep long-lived
    // handles around) — not a "forgot to free" leak.
    std::mem::forget(provider);
}

#[cfg(not(feature = "otlp"))]
pub fn init() {
    let fmt_layer = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry()
        .with(filter())
        .with(fmt_layer)
        .init();

    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        tracing::warn!(
            "OTEL_EXPORTER_OTLP_ENDPOINT is set but this build was compiled without the \"otlp\" \
             feature; tracing spans will only go to stdout (rebuild with `--features otlp` to export)"
        );
    }
}
