//! The pure part of service C: what Shout and Reverse do to a text.

/// `Shout`: upper-case the text.
pub fn shout(text: &str) -> String {
    text.to_uppercase()
}

/// `Reverse`: reverse by character, not by byte, so multi-byte text survives.
pub fn reverse(text: &str) -> String {
    text.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::{reverse, shout};

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
}
