//! How segments and positions (the event store's opaque strings) are stored: in the
//! fields they always had (`partition`, `offset`, and a replay range's `from` and `until`), as an
//! Int32 (a segment) or an Int64 (a position) when the string is a canonical decimal number, as
//! the library always wrote them for Kafka, else as the string itself. Documents written before
//! the event store became an add-on are read, found and deleted as before, without a migration.
//! Positions are compared in queries as MongoDB compares the stored values: numbers by value
//! (every first-wave event store's positions are decimal), strings by their bytes.

use mongodb::bson::Bson;

/// `segment` as stored: an Int32 when it is one written canonically, else the string.
pub(crate) fn segment(segment: &str) -> Bson {
    match segment.parse::<i32>() {
        Ok(n) if n.to_string() == segment => Bson::Int32(n),
        _ => Bson::String(segment.to_string()),
    }
}

/// `position` as stored: an Int64 when it is one written canonically, else the string.
pub(crate) fn position(position: &str) -> Bson {
    match position.parse::<i64>() {
        Ok(n) if n.to_string() == position => Bson::Int64(n),
        _ => Bson::String(position.to_string()),
    }
}

/// A stored segment or position as its string; `None` for another type.
pub(crate) fn text(value: &Bson) -> Option<String> {
    match value {
        Bson::Int32(n) => Some(n.to_string()),
        Bson::Int64(n) => Some(n.to_string()),
        Bson::String(s) => Some(s.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_numbers_are_stored_as_numbers_anything_else_as_text() {
        assert_eq!(segment("3"), Bson::Int32(3));
        assert_eq!(position("41"), Bson::Int64(41));
        assert_eq!(position("0"), Bson::Int64(0));
        for odd in ["p-0", "007", "+1", "", "99999999999999999999"] {
            assert_eq!(position(odd), Bson::String(odd.into()), "{odd}");
        }
        assert_eq!(segment("2147483648"), Bson::String("2147483648".into()));
        for s in ["3", "p-0", "007"] {
            assert_eq!(text(&segment(s)).as_deref(), Some(s));
            assert_eq!(text(&position(s)).as_deref(), Some(s));
        }
        assert_eq!(text(&Bson::Null), None);
    }
}
