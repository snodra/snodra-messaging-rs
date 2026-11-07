//! The metrics export (ADR 0026): kraken-messaging-rs records its metrics through the
//! OpenTelemetry API; the application installs the meter provider. With `KRAKEN_OTLP_ENDPOINT`
//! set (OTLP/gRPC, e.g. `http://otel-lgtm:4317`, as files-service), an OTLP meter provider becomes
//! the global one before the messaging is built, exporting every 60 s
//! (`OTEL_METRIC_EXPORT_INTERVAL` in milliseconds changes it); unset, none is installed and the
//! library records nothing.
//!
//! The resource is the identity Prometheus labels every series with (`job`, `instance`):
//! `service.name` the service name the hub sees, `service.instance.id` the instance id the hub
//! sees (`Config.instance_id`). No `service.namespace` (it would change `job`).

use kraken_messaging::{Config, MessagingError};
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;

/// Owns the meter provider; dropping it exports what is pending and shuts the exporter down.
pub struct MetricsGuard(Option<SdkMeterProvider>);

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.0.take()
            && let Err(e) = provider.shutdown()
        {
            tracing::debug!(error = %e, "meter provider shutdown");
        }
    }
}

/// The resource of `config`'s instance.
pub fn resource(config: &Config) -> Resource {
    Resource::builder_empty()
        .with_service_name(config.service_name.clone())
        .with_attribute(KeyValue::new(
            "service.instance.id",
            config.instance_id.clone(),
        ))
        .build()
}

/// Installs the global OTLP meter provider when `endpoint` is set. Must run inside the tokio
/// runtime, before `KrakenMessaging::builder`.
pub fn init_metrics(
    config: &Config,
    endpoint: Option<&str>,
) -> Result<MetricsGuard, MessagingError> {
    let Some(endpoint) = endpoint else {
        return Ok(MetricsGuard(None));
    };
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .build()
        .map_err(|e| MessagingError::Transport(format!("OTLP metric exporter: {e}")))?;
    let provider = SdkMeterProvider::builder()
        .with_resource(resource(config))
        .with_periodic_exporter(exporter)
        .build();
    global::set_meter_provider(provider.clone());
    tracing::info!(endpoint, "exporting metrics over OTLP");
    Ok(MetricsGuard(Some(provider)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_resource_names_the_service_and_the_instance_the_hub_sees() {
        let mut config = Config::new("service-c", "hub:7000", "kafka:19092", "mongodb://m/db");
        config.instance_id = "pod-7".into();
        let resource = resource(&config);
        let get = |key: &'static str| {
            resource
                .get(&opentelemetry::Key::from_static_str(key))
                .map(|v| v.to_string())
        };
        assert_eq!(get("service.name").as_deref(), Some("service-c"));
        assert_eq!(get("service.instance.id").as_deref(), Some("pod-7"));
        assert_eq!(get("service.namespace"), None, "it would change `job`");
    }

    #[tokio::test]
    async fn without_an_endpoint_no_provider_is_installed() {
        let config = Config::new("service-c", "hub:7000", "kafka:19092", "mongodb://m/db");
        let guard = init_metrics(&config, None).unwrap();
        assert!(guard.0.is_none());
    }

    #[tokio::test]
    async fn with_an_endpoint_the_provider_is_built_without_connecting() {
        let config = Config::new("service-c", "hub:7000", "kafka:19092", "mongodb://m/db");
        let guard = init_metrics(&config, Some("http://127.0.0.1:1")).unwrap();
        assert!(guard.0.is_some());
    }
}
