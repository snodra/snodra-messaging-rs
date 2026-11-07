//! An OpenTelemetry meter whose data points the tests read back (the SDK's in-memory exporter),
//! shared by the unit tests (`src/metrics.rs` includes this file) and the integration tests.
#![allow(dead_code)]

use std::collections::BTreeMap;

use opentelemetry::metrics::{Meter, MeterProvider};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

/// What an instrument exported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Counter,
    UpDownCounter,
    Gauge,
    Histogram,
}

/// One exported data point: a sum or gauge value, or a histogram's count and sum.
#[derive(Clone, Debug)]
pub struct Point {
    pub name: String,
    pub unit: String,
    pub kind: Kind,
    pub attributes: BTreeMap<String, String>,
    pub value: f64,
    pub count: u64,
}

impl Point {
    /// Whether the point has exactly `labels` (in any order).
    pub fn has(&self, labels: &[(&str, &str)]) -> bool {
        self.attributes.len() == labels.len()
            && labels
                .iter()
                .all(|(k, v)| self.attributes.get(*k).is_some_and(|a| a == v))
    }
}

pub struct TestMeter {
    exporter: InMemoryMetricExporter,
    provider: SdkMeterProvider,
}

impl Default for TestMeter {
    fn default() -> Self {
        TestMeter::new()
    }
}

impl TestMeter {
    pub fn new() -> TestMeter {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        TestMeter { exporter, provider }
    }

    /// A meter of this provider, under the library's scope name.
    pub fn meter(&self) -> Meter {
        self.provider.meter("kraken-messaging-rs")
    }

    /// Every data point exported now (the observable instruments' callbacks run).
    pub fn points(&self) -> Vec<Point> {
        self.exporter.reset();
        self.provider.force_flush().expect("flush");
        let finished = self.exporter.get_finished_metrics().expect("metrics");
        let mut points = Vec::new();
        for metric in finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
        {
            let (name, unit) = (metric.name().to_string(), metric.unit().to_string());
            let mut push = |kind, attributes: BTreeMap<String, String>, value: f64, count| {
                points.push(Point {
                    name: name.clone(),
                    unit: unit.clone(),
                    kind,
                    attributes,
                    value,
                    count,
                })
            };
            macro_rules! points_of {
                ($data:expr) => {
                    match $data {
                        MetricData::Sum(sum) => {
                            let kind = if sum.is_monotonic() {
                                Kind::Counter
                            } else {
                                Kind::UpDownCounter
                            };
                            for p in sum.data_points() {
                                push(kind, attributes(p.attributes()), p.value() as f64, 0);
                            }
                        }
                        MetricData::Gauge(g) => {
                            for p in g.data_points() {
                                push(Kind::Gauge, attributes(p.attributes()), p.value() as f64, 0);
                            }
                        }
                        MetricData::Histogram(h) => {
                            for p in h.data_points() {
                                push(
                                    Kind::Histogram,
                                    attributes(p.attributes()),
                                    p.sum() as f64,
                                    p.count(),
                                );
                            }
                        }
                        MetricData::ExponentialHistogram(_) => {
                            panic!("{name}: no exponential histogram expected")
                        }
                    }
                };
            }
            match metric.data() {
                AggregatedMetrics::U64(data) => points_of!(data),
                AggregatedMetrics::I64(data) => points_of!(data),
                AggregatedMetrics::F64(data) => points_of!(data),
            }
        }
        points
    }

    /// The points of the instrument `name`.
    pub fn points_of(&self, name: &str) -> Vec<Point> {
        self.points()
            .into_iter()
            .filter(|p| p.name == name)
            .collect()
    }

    /// The point of `name` with exactly `labels`.
    pub fn point(&self, name: &str, labels: &[(&str, &str)]) -> Option<Point> {
        self.points_of(name).into_iter().find(|p| p.has(labels))
    }

    /// The value of `name` with exactly `labels` (a histogram's: its count).
    pub fn value(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        self.point(name, labels).map(|p| match p.kind {
            Kind::Histogram => p.count as f64,
            _ => p.value,
        })
    }
}

fn attributes<'a>(
    kvs: impl Iterator<Item = &'a opentelemetry::KeyValue>,
) -> BTreeMap<String, String> {
    kvs.map(|kv| (kv.key.to_string(), kv.value.to_string()))
        .collect()
}
