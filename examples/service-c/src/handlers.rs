//! The pure part of service C: what Shout and Reverse do to a text, what Inventory answers.

use crate::messages::InventoryLevel;

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

#[cfg(test)]
mod tests {
    use super::{INVENTORY_COUNT, inventory, reverse, shout};

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
}
