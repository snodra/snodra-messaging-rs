//! The pure part of service C: what Shout and Reverse do to a text, what Inventory answers, what
//! Rows streams.

use futures::stream::BoxStream;
use kraken_messaging::MessagingError;

use crate::messages::{InventoryLevel, Row};

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
    Box::pin(futures::stream::unfold(1, move |n| {
        let service = service.clone();
        let instance = instance.clone();
        async move {
            if n > count {
                None
            } else if fail_at > 0 && n == fail_at {
                let error = MessagingError::RemoteHandler {
                    type_name: "examples.Rows".to_string(),
                    message: format!("Rows failed at row {n}"),
                    details: vec![],
                };
                Some((Err(error), count + 1)) // the next pull finds the end
            } else {
                Some((
                    Ok(Row {
                        service,
                        instance,
                        n,
                    }),
                    n + 1,
                ))
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use kraken_messaging::MessagingError;

    use super::{INVENTORY_COUNT, inventory, reverse, rows, shout};

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
    fn rows_is_lazy() {
        // A pull at a time: nothing is produced for the rows the consumer never asks for.
        let two: Vec<_> =
            futures::executor::block_on(rows("service-c", "c1", i32::MAX, 0).take(2).collect());
        assert_eq!(two.len(), 2);
    }
}
