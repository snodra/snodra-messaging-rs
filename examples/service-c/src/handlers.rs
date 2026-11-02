//! The pure part of service C: what Shout and Reverse do to a text, what Inventory answers, what
//! Rows streams.

use futures::stream::BoxStream;
use kraken_messaging::MessagingError;

use std::collections::HashMap;
use std::sync::Mutex;

use crate::messages::{Counter, CounterChanged, InventoryLevel, Row, SubscriptionCount};

/// service-c's fixed count for every item (service-b answers 7).
pub const INVENTORY_COUNT: i64 = 3;

/// `Shout`: upper-case the text.
pub fn shout(text: &str) -> String {
    text.to_uppercase()
}

/// `Reverse`: reverse by `char` (Unicode scalar value), not by byte, so multi-byte text survives.
/// Combining marks are not kept with their base character (no grapheme segmentation), by design.
pub fn reverse(text: &str) -> String {
    text.chars().rev().collect()
}

/// `Inventory`: service-c's share of the shared query; `answered` is this instance's running count.
pub fn inventory(service: &str, instance: &str, item: &str, answered: i64) -> InventoryLevel {
    InventoryLevel {
        service: service.to_string(),
        instance: instance.to_string(),
        item: item.to_string(),
        count: INVENTORY_COUNT,
        answered,
    }
}

/// `Rows`: the rows `1..=count`, each naming this service and instance, lazily (one per pull, so
/// the library's credit pacing holds the handler back). With `fail_at > 0` the item for that row
/// is an `Err` (`HANDLER_ERROR` "Rows failed at row <n>") and the stream ends there: the rows
/// before it were streamed.
pub fn rows(
    service: &str,
    instance: &str,
    count: i32,
    fail_at: i32,
) -> BoxStream<'static, Result<Row, MessagingError>> {
    let service = service.to_string();
    let instance = instance.to_string();
    // The state is the next row, `None` once there is none: after the error item, and past
    // `i32::MAX` (`checked_add`), which `count + 1` could not express without overflowing.
    Box::pin(futures::stream::unfold(Some(1), move |n| {
        let service = service.clone();
        let instance = instance.clone();
        async move {
            let n = n?;
            if n > count {
                None
            } else if fail_at > 0 && n == fail_at {
                let error = MessagingError::RemoteHandler {
                    type_name: "examples.Rows".to_string(),
                    message: format!("Rows failed at row {n}"),
                    details: vec![],
                };
                Some((Err(error), None)) // the next pull finds the end
            } else {
                Some((
                    Ok(Row {
                        service,
                        instance,
                        n,
                    }),
                    n.checked_add(1),
                ))
            }
        }
    }))
}

/// The in-memory counters of `examples.WatchCounter`, one per name. The initial result of a
/// subscription is the current value (0 for a name nobody incremented); `increment` adds one and
/// calls `notify` with the new value **while holding the lock**, so two increments notify in the
/// order of their values (a subscriber must see 1, then 2). `notify` must not block: the
/// emitter's calls never wait.
pub struct Counters {
    service: String,
    instance: String,
    values: Mutex<HashMap<String, i64>>,
}

impl Counters {
    pub fn new(service: &str, instance: &str) -> Counters {
        Counters {
            service: service.to_string(),
            instance: instance.to_string(),
            values: Mutex::new(HashMap::new()),
        }
    }

    fn counter(&self, name: &str, value: i64) -> Counter {
        Counter {
            service: self.service.clone(),
            instance: self.instance.clone(),
            name: name.to_string(),
            value,
        }
    }

    /// `WatchCounter`'s initial result.
    pub fn current(&self, name: &str) -> Counter {
        let values = self.values.lock().expect("counters lock");
        self.counter(name, values.get(name).copied().unwrap_or(0))
    }

    /// Adds one to the counter `name`, hands the change to `notify`, returns the new value.
    pub fn increment(&self, name: &str, notify: impl FnOnce(&CounterChanged)) -> Counter {
        let mut values = self.values.lock().expect("counters lock");
        let value = values.entry(name.to_string()).or_insert(0);
        *value += 1;
        let counter = self.counter(name, *value);
        notify(&CounterChanged {
            service: counter.service.clone(),
            instance: counter.instance.clone(),
            name: counter.name.clone(),
            value: counter.value,
        });
        counter
    }
}

/// `OpenSubscriptions`: service-c's share of the shared query; `open` is the emitter's count.
pub fn subscription_count(service: &str, instance: &str, open: usize) -> SubscriptionCount {
    SubscriptionCount {
        service: service.to_string(),
        instance: instance.to_string(),
        open: i64::try_from(open).unwrap_or(i64::MAX),
    }
}

/// `IncrementCounter` is addressed to one service: refuse it when that is not this one.
pub fn check_addressee(own: &str, addressed: &str) -> Result<(), MessagingError> {
    if own == addressed {
        return Ok(());
    }
    Err(MessagingError::RemoteHandler {
        type_name: "examples.IncrementCounter".to_string(),
        message: format!("IncrementCounter is for {addressed}, this is {own}"),
        details: vec![],
    })
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use kraken_messaging::MessagingError;

    use super::{
        Counters, INVENTORY_COUNT, check_addressee, inventory, reverse, rows, shout,
        subscription_count,
    };

    #[test]
    fn shout_uppercases() {
        assert_eq!(shout("ada lovelace"), "ADA LOVELACE");
    }

    #[test]
    fn reverse_handles_unicode() {
        assert_eq!(reverse("héllo"), "olléh");
    }

    #[test]
    fn reverse_empty_is_empty() {
        assert_eq!(reverse(""), "");
    }

    #[test]
    fn inventory_answers_with_the_service_and_a_fixed_count() {
        let level = inventory("service-c", "c1", "widget", 2);
        assert_eq!(level.service, "service-c");
        assert_eq!(level.instance, "c1");
        assert_eq!(level.item, "widget");
        assert_eq!(level.count, INVENTORY_COUNT);
        assert_eq!(level.answered, 2);
    }

    fn collected(count: i32, fail_at: i32) -> Vec<Result<i32, String>> {
        futures::executor::block_on(
            rows("service-c", "c1", count, fail_at)
                .map(|r| r.map(|row| row.n).map_err(|e| e.to_string()))
                .collect(),
        )
    }

    #[test]
    fn rows_streams_one_to_count_naming_the_service_and_instance() {
        let first = futures::executor::block_on(rows("service-c", "c1", 3, 0).next())
            .unwrap()
            .unwrap();
        assert_eq!(first.service, "service-c");
        assert_eq!(first.instance, "c1");
        assert_eq!(first.n, 1);
        assert_eq!(collected(3, 0), vec![Ok(1), Ok(2), Ok(3)]);
        assert_eq!(collected(0, 0), vec![]);
    }

    #[test]
    fn fail_at_ends_the_stream_with_a_handler_error_after_the_rows_before_it() {
        assert_eq!(
            collected(10, 3),
            vec![Ok(1), Ok(2), Err("Rows failed at row 3".to_string())]
        );
        let error = futures::executor::block_on(rows("service-c", "c1", 1, 1).next())
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(error, MessagingError::RemoteHandler { .. }),
            "{error:?}"
        );
    }

    #[test]
    fn fail_at_on_the_largest_count_ends_without_overflowing() {
        // The state after the error must say "done" even when `count + 1` does not exist.
        assert_eq!(
            collected(i32::MAX, 1),
            vec![Err("Rows failed at row 1".to_string())]
        );
    }

    #[test]
    fn rows_is_lazy() {
        // A pull at a time: nothing is produced for the rows the consumer never asks for.
        let two: Vec<_> =
            futures::executor::block_on(rows("service-c", "c1", i32::MAX, 0).take(2).collect());
        assert_eq!(two.len(), 2);
    }

    #[test]
    fn a_counter_nobody_incremented_is_zero() {
        let counters = Counters::new("service-c", "c1");
        let initial = counters.current("fresh");
        assert_eq!(initial.service, "service-c");
        assert_eq!(initial.instance, "c1");
        assert_eq!(initial.name, "fresh");
        assert_eq!(initial.value, 0);
    }

    #[test]
    fn increment_counts_every_name_on_its_own_and_current_follows() {
        let counters = Counters::new("service-c", "c1");
        assert_eq!(counters.increment("a", |_| {}).value, 1);
        assert_eq!(counters.increment("a", |_| {}).value, 2);
        assert_eq!(counters.increment("b", |_| {}).value, 1);
        assert_eq!(counters.current("a").value, 2);
        assert_eq!(counters.current("b").value, 1);
    }

    #[test]
    fn increment_notifies_with_the_new_value_in_order() {
        let counters = Counters::new("service-c", "c1");
        let mut seen = Vec::new();
        counters.increment("a", |c| {
            seen.push((
                c.service.clone(),
                c.instance.clone(),
                c.name.clone(),
                c.value,
            ))
        });
        counters.increment("a", |c| {
            seen.push((
                c.service.clone(),
                c.instance.clone(),
                c.name.clone(),
                c.value,
            ))
        });
        assert_eq!(
            seen,
            vec![
                (
                    "service-c".to_string(),
                    "c1".to_string(),
                    "a".to_string(),
                    1
                ),
                (
                    "service-c".to_string(),
                    "c1".to_string(),
                    "a".to_string(),
                    2
                ),
            ]
        );
    }

    #[test]
    fn subscription_count_names_the_service_and_the_open_subscriptions() {
        let count = subscription_count("service-c", "c1", 3);
        assert_eq!(count.service, "service-c");
        assert_eq!(count.instance, "c1");
        assert_eq!(count.open, 3);
    }

    #[test]
    fn increment_counter_is_refused_by_a_service_it_is_not_addressed_to() {
        assert!(check_addressee("service-c", "service-c").is_ok());
        let error = check_addressee("service-c", "service-b").unwrap_err();
        assert!(
            matches!(error, MessagingError::RemoteHandler { .. }),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            "IncrementCounter is for service-b, this is service-c"
        );
    }
}
