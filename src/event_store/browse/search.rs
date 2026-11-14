//! A search page over an event store whose positions in a segment are whole numbers that rise by
//! one per event appended: the segments read backwards in chunks and merged newest first, the
//! cursor that continues it, and [`SearchPlan`], which turns a [`SearchRequest`] into the bounds
//! of one page. An add-on with other positions answers
//! [`EventBrowser::search`](super::EventBrowser::search) its own way, with the same
//! [`SearchPage`].

use std::cmp::Ordering;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

use super::filters::Filters;
use super::queries::{BrowseSettings, SearchPage, SearchRequest, StoppedBy};
use super::records::EventSummary;
use super::{BrowseError, DEFAULT_LIMIT};
use crate::event_store::{MAX_SEGMENT_BYTES, segment_order};

/// Records a segment is read backwards by, per read, at most...
pub const CHUNK: usize = 500;
/// ...and at first in a page: each next chunk of a segment is twice the last, so every segment
/// has its newest records in hand early even on a slow link.
pub const FIRST_CHUNK: usize = 50;
/// The longest cursor accepted, in bytes.
pub const MAX_CURSOR: usize = 64 * 1024;

/// Where a search goes on: for every segment, the position below which the next page reads (the
/// lowest examined so far), and the digest of the search ([`Filters::digest`]). Opaque to the
/// caller: base64url (no padding) of `{"f": <digest>, "p": [[segment, position], ...]}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    #[serde(rename = "f")]
    pub digest: String,
    #[serde(rename = "p")]
    pub positions: Vec<(String, i64)>,
}

impl Cursor {
    pub fn new(digest: String, positions: Vec<(String, i64)>) -> Self {
        Self { digest, positions }
    }

    /// Opaque base64url (no padding) of its JSON.
    pub fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).expect("a cursor serializes"))
    }

    /// The cursor `s` of the search whose digest is `digest`; [`BrowseError::Invalid`] when it
    /// is malformed or belongs to another search.
    pub fn decode(s: &str, digest: &str) -> Result<Self, BrowseError> {
        let malformed = || BrowseError::Invalid("the cursor is malformed".to_string());
        if s.len() > MAX_CURSOR {
            return Err(malformed());
        }
        let bytes = URL_SAFE_NO_PAD.decode(s.trim()).map_err(|_| malformed())?;
        let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| malformed())?;
        if cursor
            .positions
            .iter()
            .any(|(segment, position)| segment.len() > MAX_SEGMENT_BYTES || *position < 0)
        {
            return Err(malformed());
        }
        if cursor.digest != digest {
            return Err(BrowseError::Invalid(
                "the cursor belongs to another search: send the source and filters it came with"
                    .to_string(),
            ));
        }
        Ok(cursor)
    }

    /// The position the cursor holds for `segment`.
    pub fn position(&self, segment: &str) -> Option<i64> {
        self.positions
            .iter()
            .find(|(s, _)| s == segment)
            .map(|(_, p)| *p)
    }
}

/// One segment's positions to search: from `lower` (inclusive) up to `upper` (exclusive).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range {
    pub segment: String,
    pub lower: i64,
    pub upper: i64,
}

/// A record a search read: its position, its time (-1 without one) and its summary when it
/// matched the filters.
#[derive(Debug, Clone, PartialEq)]
pub struct Examined {
    pub position: i64,
    pub timestamp: i64,
    pub hit: Option<EventSummary>,
}

/// A chunk a search reads: the records of `segment` from `start` up to (not including) `end`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Want {
    pub segment: String,
    pub start: i64,
    pub end: i64,
}

/// Reads records for a search.
pub trait Chunks {
    type Error;
    /// The records of every chunk of `wants`, read at once: per chunk, in its order, the records
    /// in position order, each examined against the filters (positions with no record are simply
    /// absent), or `None` when `deadline` passed before that chunk was complete.
    fn read(
        &mut self,
        wants: &[Want],
        deadline: Instant,
    ) -> Result<Vec<Option<Vec<Examined>>>, Self::Error>;
}

/// The result of [`merge`].
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    /// The matching records, newest first.
    pub results: Vec<EventSummary>,
    /// Records read from the event store.
    pub scanned: u64,
    /// Every segment reached its lower bound: there is no next page.
    pub exhausted: bool,
    /// Why the page ended early; `None` when exhausted.
    pub stopped_by: Option<StoppedBy>,
    /// Per segment, the position below which the next page reads.
    pub positions: Vec<(String, i64)>,
}

/// The bounds of one [`merge`].
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub limit: usize,
    pub max_scanned: u64,
    pub deadline: Instant,
}

struct Lane {
    range: Range,
    /// Every position at or above it was examined and its match, if any, returned: the next
    /// page's upper bound.
    next: i64,
    /// The start of the chunk in `buffer`.
    loaded_from: i64,
    /// The matches of the chunk not yet returned, oldest first (popped from the end).
    buffer: Vec<Examined>,
    /// Records the next chunk reads, at most.
    size: i64,
}

impl Lane {
    fn done(&self) -> bool {
        self.buffer.is_empty() && self.next <= self.range.lower
    }

    /// Where the next page starts: just above the newest match not yet returned, else the
    /// chunk's start (the positions between were examined and match nothing).
    fn settle(&mut self) {
        self.next = self
            .buffer
            .last()
            .map_or(self.loaded_from, |e| e.position + 1);
    }
}

/// Newest first: the time; ties by segment ([`segment_order`]), then position, both descending.
fn newer(a: &Lane, b: &Lane) -> Ordering {
    let (x, y) = (
        a.buffer.last().expect("a head"),
        b.buffer.last().expect("a head"),
    );
    x.timestamp
        .cmp(&y.timestamp)
        .then_with(|| segment_order(&a.range.segment, &b.range.segment))
        .then_with(|| x.position.cmp(&y.position))
}

/// Reads `ranges` backwards in chunks (of [`FIRST_CHUNK`] records first, then twice the last,
/// up to [`CHUNK`]), every segment that needs one in the same read, and merges their matches
/// newest first (time; ties by segment, then position, descending) until `budget.limit`
/// results, `budget.max_scanned` records read or `budget.deadline`, whichever comes first.
///
/// A page reads at least one record per segment that is not done, whatever `max_scanned` says:
/// with fewer, the segments past the budget would never be read and a search whose first
/// segments hold matches would never move on.
///
/// Within a segment the order is the position order: when the times of a segment do not rise,
/// the merged list is not a strict global sort. A page that ends before it returns anything
/// still moves on: a segment whose chunk arrived is positioned just above its newest match
/// (below the chunk when it has none). A segment keeps only the summaries of its newest matches
/// that the page can still return (the rest is read again by the next page), so a page holds at
/// most segments x `limit` summaries; records read but not returned are read (and counted)
/// again by the next page.
pub fn merge<C: Chunks>(
    reader: &mut C,
    ranges: &[Range],
    budget: Budget,
) -> Result<Page, C::Error> {
    // At least one record per segment (see above).
    let max_scanned = budget.max_scanned.max(ranges.len() as u64);
    // Enough per chunk that one round over the segments fits the scan budget.
    let chunk = (max_scanned / (ranges.len().max(1) as u64)).clamp(1, CHUNK as u64) as i64;
    let mut lanes: Vec<Lane> = ranges
        .iter()
        .map(|r| Lane {
            range: r.clone(),
            next: r.upper.max(r.lower),
            loaded_from: r.upper.max(r.lower),
            buffer: Vec::new(),
            size: chunk.min(FIRST_CHUNK as i64),
        })
        .collect();
    let mut results = Vec::new();
    let mut scanned = 0u64;
    let mut stopped_by = None;
    loop {
        // Every segment that is not done needs a match in hand before the newest is known.
        let needy: Vec<usize> = (0..lanes.len())
            .filter(|i| lanes[*i].buffer.is_empty() && !lanes[*i].done())
            .collect();
        if !needy.is_empty() {
            if scanned >= max_scanned {
                stopped_by = Some(StoppedBy::ScanBudget);
                break;
            }
            if Instant::now() >= budget.deadline {
                stopped_by = Some(StoppedBy::TimeBudget);
                break;
            }
            // The scan budget left, shared by the segments of this read (one record each at
            // least, so fewer segments when fewer records are left).
            let left = max_scanned - scanned;
            let needy = &needy[..needy.len().min(usize::try_from(left).unwrap_or(usize::MAX))];
            let share = i64::try_from(left / needy.len() as u64).unwrap_or(i64::MAX);
            let wants: Vec<Want> = needy
                .iter()
                .map(|i| {
                    let lane = &lanes[*i];
                    Want {
                        segment: lane.range.segment.clone(),
                        start: (lane.next - lane.size.min(share)).max(lane.range.lower),
                        end: lane.next,
                    }
                })
                .collect();
            let chunks = reader.read(&wants, budget.deadline)?;
            let mut late = false;
            for ((i, want), read) in needy.iter().zip(&wants).zip(chunks) {
                let Some(read) = read else {
                    late = true;
                    continue;
                };
                scanned += read.len() as u64;
                let lane = &mut lanes[*i];
                lane.size = (lane.size * 2).min(chunk);
                lane.loaded_from = want.start;
                lane.buffer = read
                    .into_iter()
                    .filter(|e| e.hit.is_some() && (want.start..want.end).contains(&e.position))
                    .collect();
                // This page returns at most `wanted` more results, so no segment needs a summary
                // beyond its newest `wanted` matches: the older ones are let go (and read again
                // by the next page), which bounds a page's memory by segments x limit.
                let wanted = budget.limit - results.len();
                if lane.buffer.len() > wanted {
                    let cut = lane.buffer.len() - wanted;
                    lane.loaded_from = lane.buffer[cut].position;
                    lane.buffer.drain(..cut);
                }
                lane.settle();
            }
            if late {
                stopped_by = Some(StoppedBy::TimeBudget);
                break;
            }
            continue;
        }
        let Some(lane) = lanes
            .iter_mut()
            .filter(|l| !l.buffer.is_empty())
            .max_by(|a, b| newer(a, b))
        else {
            break;
        };
        let examined = lane.buffer.pop().expect("a lane with a head");
        lane.settle();
        results.push(examined.hit.expect("only matches are kept"));
        if results.len() >= budget.limit {
            stopped_by = Some(StoppedBy::Limit);
            break;
        }
    }
    let exhausted = lanes.iter().all(Lane::done);
    Ok(Page {
        results,
        scanned,
        exhausted,
        stopped_by: if exhausted { None } else { stopped_by },
        positions: lanes
            .iter()
            .map(|l| (l.range.segment.clone(), l.next.max(l.range.lower)))
            .collect(),
    })
}

/// One page of a search, checked and bounded: what an add-on reads and how it answers.
#[derive(Debug, Clone)]
pub struct SearchPlan {
    /// The source searched.
    pub source: String,
    pub filters: Filters,
    /// Results at most.
    pub limit: usize,
    /// The page it continues.
    pub cursor: Option<Cursor>,
    /// Records read at most.
    pub max_scanned: u64,
    /// The time the page may take.
    pub timeout: Duration,
}

impl SearchPlan {
    /// The plan of `request` under `settings`: the source (the first of `settings.sources` when
    /// the request names none), the filters, `limit` (default [`DEFAULT_LIMIT`], 1 to
    /// `max_results`), the cursor (of this search only), and the scan and time budgets (the
    /// request's, at most the settings'). [`BrowseError::Invalid`] for anything it cannot honour.
    pub fn new(request: &SearchRequest, settings: &BrowseSettings) -> Result<Self, BrowseError> {
        let source = settings.source(request.source.as_deref())?;
        let filters = Filters::parse(&request.filters, false)?;
        let max = settings.max_results;
        let limit = match request.limit {
            None => DEFAULT_LIMIT.min(max),
            Some(n) if (1..=max).contains(&n) => n,
            Some(_) => {
                return Err(BrowseError::Invalid(format!(
                    "limit must be a number from 1 to {max}"
                )));
            }
        };
        let cursor = request
            .cursor
            .as_deref()
            .filter(|c| !c.is_empty())
            .map(|c| Cursor::decode(c, &filters.digest(&source)))
            .transpose()?;
        let max_scanned = match request.max_scanned {
            None => settings.max_scanned,
            Some(0) => return Err(BrowseError::Invalid("maxScanned must be at least 1".into())),
            Some(n) => n.min(settings.max_scanned),
        };
        let timeout = match request.timeout_ms {
            None => settings.timeout,
            Some(0) => return Err(BrowseError::Invalid("timeoutMs must be at least 1".into())),
            Some(ms) => Duration::from_millis(ms).min(settings.timeout),
        };
        Ok(Self {
            source,
            filters,
            limit,
            cursor,
            max_scanned,
            timeout,
        })
    }

    /// The upper bound of `segment` on this page, whose range is `lower..upper`: the cursor's
    /// position when it continues a search (`lower`, nothing, for a segment the cursor does not
    /// name: it was not there on the first page), else `upper`.
    pub fn upper(&self, segment: &str, lower: i64, upper: i64) -> i64 {
        match &self.cursor {
            None => upper,
            Some(cursor) => cursor.position(segment).map_or(lower, |p| p.min(upper)),
        }
    }

    /// The bounds of the page's [`merge`], from `started`.
    pub fn budget(&self, started: Instant) -> Budget {
        Budget {
            limit: self.limit,
            max_scanned: self.max_scanned,
            deadline: started + self.timeout,
        }
    }

    /// The answer of `page`, with the cursor of the next page unless it is exhausted.
    pub fn answer(&self, page: Page) -> SearchPage {
        let cursor = (!page.exhausted)
            .then(|| Cursor::new(self.filters.digest(&self.source), page.positions).encode());
        SearchPage {
            source: self.source.clone(),
            missing: false,
            results: page.results,
            scanned: page.scanned,
            exhausted: page.exhausted,
            stopped_by: page.stopped_by,
            cursor,
        }
    }

    /// The answer when the source does not exist.
    pub fn missing(&self) -> SearchPage {
        SearchPage {
            source: self.source.clone(),
            missing: true,
            results: Vec::new(),
            scanned: 0,
            exhausted: true,
            stopped_by: None,
            cursor: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::event_store::browse::filters::EventFilter;

    #[test]
    fn a_cursor_round_trips_and_only_continues_its_own_search() {
        let digest = "0123456789abcdef".to_string();
        let cursor = Cursor::new(
            digest.clone(),
            vec![
                ("0".into(), 120),
                ("1".into(), 0),
                ("5".into(), 9_000_000_000),
            ],
        );
        let text = cursor.encode();
        assert!(
            text.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "{text}"
        );
        assert_eq!(Cursor::decode(&text, &digest), Ok(cursor));
        let err = Cursor::decode(&text, "fedcba9876543210").unwrap_err();
        assert!(err.message().contains("another search"), "{err}");
        let long_segment = "s".repeat(MAX_SEGMENT_BYTES + 1);
        for bad in [
            "%%%".to_string(),
            URL_SAFE_NO_PAD.encode("{}"),
            URL_SAFE_NO_PAD.encode(format!("{{\"f\":\"{digest}\",\"p\":[[\"0\",-5]]}}")),
            URL_SAFE_NO_PAD.encode(format!("{{\"f\":\"{digest}\",\"p\":[[0,5]]}}")),
            URL_SAFE_NO_PAD.encode(format!(
                "{{\"f\":\"{digest}\",\"p\":[[\"{long_segment}\",5]]}}"
            )),
            "A".repeat(MAX_CURSOR + 1),
        ] {
            assert!(
                matches!(Cursor::decode(&bad, &digest), Err(BrowseError::Invalid(_))),
                "{bad}"
            );
        }
    }

    fn settings() -> BrowseSettings {
        BrowseSettings {
            sources: vec!["a".into(), "a.dlt".into()],
            ..BrowseSettings::default()
        }
    }

    #[test]
    fn a_plan_checks_the_request_and_bounds_it_by_the_settings() {
        let s = settings();
        let plan = SearchPlan::new(&SearchRequest::default(), &s).unwrap();
        assert_eq!(plan.source, "a", "the first source is the default");
        assert_eq!(plan.limit, DEFAULT_LIMIT);
        assert_eq!((plan.max_scanned, plan.timeout), (s.max_scanned, s.timeout));
        let plan = SearchPlan::new(
            &SearchRequest {
                source: Some("a.dlt".into()),
                limit: Some(100),
                max_scanned: Some(10_000_000),
                timeout_ms: Some(250),
                ..SearchRequest::default()
            },
            &s,
        )
        .unwrap();
        assert_eq!(plan.source, "a.dlt");
        assert_eq!(plan.limit, 100);
        assert_eq!(plan.max_scanned, s.max_scanned, "at most the setting");
        assert_eq!(plan.timeout, Duration::from_millis(250));
        let small = BrowseSettings {
            max_results: 20,
            ..settings()
        };
        assert_eq!(
            SearchPlan::new(&SearchRequest::default(), &small)
                .unwrap()
                .limit,
            20
        );
        for bad in [
            SearchRequest {
                source: Some("b".into()),
                ..SearchRequest::default()
            },
            SearchRequest {
                limit: Some(0),
                ..SearchRequest::default()
            },
            SearchRequest {
                limit: Some(101),
                ..SearchRequest::default()
            },
            SearchRequest {
                max_scanned: Some(0),
                ..SearchRequest::default()
            },
            SearchRequest {
                timeout_ms: Some(0),
                ..SearchRequest::default()
            },
            SearchRequest {
                cursor: Some("%%".into()),
                ..SearchRequest::default()
            },
        ] {
            assert!(
                matches!(SearchPlan::new(&bad, &s), Err(BrowseError::Invalid(_))),
                "{bad:?}"
            );
        }

        // A cursor continues its own search only, and bounds the segments it names.
        let page = Page {
            results: vec![],
            scanned: 3,
            exhausted: false,
            stopped_by: Some(StoppedBy::Limit),
            positions: vec![("0".into(), 7)],
        };
        let first = SearchPlan::new(&SearchRequest::default(), &s).unwrap();
        let answer = first.answer(page);
        let next = SearchPlan::new(
            &SearchRequest {
                cursor: answer.cursor.clone(),
                ..SearchRequest::default()
            },
            &s,
        )
        .unwrap();
        assert_eq!(next.upper("0", 2, 100), 7);
        assert_eq!(next.upper("1", 2, 100), 2, "not there on the first page");
        assert_eq!(first.upper("1", 2, 100), 100);
        let other = SearchRequest {
            cursor: answer.cursor,
            filters: EventFilter {
                type_name: Some("x".into()),
                ..EventFilter::default()
            },
            ..SearchRequest::default()
        };
        let err = SearchPlan::new(&other, &s).unwrap_err();
        assert!(err.message().contains("another search"), "{err}");
    }

    // ---- merge ---------------------------------------------------------------------------

    /// Records of one segment: `(position, timestamp, matches)`; positions may have gaps.
    type Records = Vec<(i64, i64, bool)>;

    /// Segments in memory.
    struct Fake {
        segments: BTreeMap<String, Records>,
        reads: usize,
        slow_after: Option<usize>,
        /// A slow link: records it delivers before the time budget runs out (`None`: no limit).
        allowance: Option<usize>,
    }

    impl Fake {
        fn new(segments: &[(i32, Records)]) -> Self {
            Self {
                segments: segments
                    .iter()
                    .map(|(s, r)| (s.to_string(), r.clone()))
                    .collect(),
                reads: 0,
                slow_after: None,
                allowance: None,
            }
        }

        fn ranges(&self) -> Vec<Range> {
            let mut ranges: Vec<Range> = self
                .segments
                .iter()
                .map(|(s, recs)| Range {
                    segment: s.clone(),
                    lower: recs.first().map_or(0, |r| r.0),
                    upper: recs.last().map_or(0, |r| r.0 + 1),
                })
                .collect();
            ranges.sort_by(|a, b| segment_order(&a.segment, &b.segment));
            ranges
        }
    }

    impl Chunks for Fake {
        type Error = ();
        fn read(
            &mut self,
            wants: &[Want],
            _deadline: Instant,
        ) -> Result<Vec<Option<Vec<Examined>>>, ()> {
            self.reads += 1;
            if self.slow_after.is_some_and(|n| self.reads > n) {
                return Ok(vec![None; wants.len()]);
            }
            // A slow link shares what it delivers between the chunks read at once.
            let share = self.allowance.map(|a| a / wants.len());
            let mut out = Vec::new();
            for w in wants {
                let records: Vec<&(i64, i64, bool)> = self.segments[&w.segment]
                    .iter()
                    .filter(|(o, ..)| (w.start..w.end).contains(o))
                    .collect();
                if share.is_some_and(|share| records.len() > share) {
                    out.push(None);
                    continue;
                }
                if let Some(allowance) = &mut self.allowance {
                    *allowance -= records.len();
                }
                out.push(Some(
                    records
                        .into_iter()
                        .map(|(position, ts, hit)| Examined {
                            position: *position,
                            timestamp: *ts,
                            hit: hit.then(|| EventSummary {
                                source: "t".into(),
                                segment: w.segment.clone(),
                                position: position.to_string(),
                                timestamp: Some(*ts),
                                key: None,
                                key_truncated: false,
                                headers: BTreeMap::new(),
                                size: 0,
                            }),
                        })
                        .collect(),
                ));
            }
            Ok(out)
        }
    }

    /// One page over every record of `fake`.
    fn run(fake: &mut Fake, budget: Budget) -> Page {
        let ranges = fake.ranges();
        merge(fake, &ranges, budget).unwrap()
    }

    fn budget(limit: usize, max_scanned: u64) -> Budget {
        Budget {
            limit,
            max_scanned,
            deadline: Instant::now() + Duration::from_secs(60),
        }
    }

    fn places(page: &Page) -> Vec<(i32, i64)> {
        page.results
            .iter()
            .map(|s| (s.segment.parse().unwrap(), s.position.parse().unwrap()))
            .collect()
    }

    fn positions(page: &Page) -> Vec<(i32, i64)> {
        page.positions
            .iter()
            .map(|(s, p)| (s.parse().unwrap(), *p))
            .collect()
    }

    /// The next page's ranges: the same lower bounds, the page's positions as upper bounds.
    fn after(ranges: &[Range], page: &Page) -> Vec<Range> {
        ranges
            .iter()
            .map(|r| Range {
                upper: page
                    .positions
                    .iter()
                    .find(|(s, _)| *s == r.segment)
                    .unwrap()
                    .1,
                ..r.clone()
            })
            .collect()
    }

    #[test]
    fn segments_merge_newest_first_with_ties_by_segment_then_position() {
        // Segment 0 is busy and old, 1 sparse and new, 2 has gaps and ties with 0, and 10
        // ties with 2 (by value, not by text).
        let mut fake = Fake::new(&[
            (0, (0..5).map(|o| (o, 100 + o * 10, true)).collect()),
            (1, vec![(0, 500, true), (1, 900, true)]),
            (2, vec![(3, 120, true), (8, 140, true), (9, 140, true)]),
            (10, vec![(0, 140, true)]),
        ]);
        let page = run(&mut fake, budget(100, 1000));
        assert_eq!(
            places(&page),
            vec![
                (1, 1),
                (1, 0),
                // All at 140: the higher segment first, by value.
                (10, 0),
                (2, 9),
                (2, 8),
                (0, 4),
                (0, 3),
                // Both at 120: the higher segment first.
                (2, 3),
                (0, 2),
                (0, 1),
                (0, 0)
            ]
        );
        let times: Vec<i64> = page.results.iter().map(|s| s.timestamp.unwrap()).collect();
        assert!(times.windows(2).all(|w| w[0] >= w[1]), "{times:?}");
        assert!(page.exhausted);
        assert_eq!(page.stopped_by, None);
        assert_eq!(page.scanned, 11);
    }

    #[test]
    fn pages_continue_from_the_cursor_without_skipping_or_repeating_a_record() {
        // Uneven segments, gaps, non-matching records and times that interleave.
        let mut data = Vec::new();
        for p in 0..4i32 {
            let records: Vec<(i64, i64, bool)> = (0..(40 + p as i64 * 37))
                .filter(|o| (o + p as i64) % 7 != 0)
                .map(|o| {
                    (
                        o,
                        (o * (p as i64 + 3) * 11) % 1500 + o * 5,
                        (o + p as i64) % 3 != 0,
                    )
                })
                .collect();
            data.push((p, records));
        }
        let mut whole = Fake::new(&data);
        let all = run(&mut whole, budget(10_000, 1_000_000));
        assert!(all.exhausted);
        let expected = places(&all);
        assert!(expected.len() > 100);

        for (limit, max_scanned) in [(7, 1_000_000), (100, 23), (3, 9), (50, 4)] {
            let mut fake = Fake::new(&data);
            let mut ranges = fake.ranges();
            let mut got = Vec::new();
            for _ in 0..10_000 {
                let page = merge(&mut fake, &ranges, budget(limit, max_scanned)).unwrap();
                assert!(page.results.len() <= limit);
                assert!(page.scanned <= max_scanned, "within the scan budget");
                got.extend(places(&page));
                if page.exhausted {
                    assert_eq!(page.stopped_by, None);
                    break;
                }
                assert!(page.stopped_by.is_some());
                ranges = after(&ranges, &page);
            }
            assert_eq!(got, expected, "limit {limit}, scan budget {max_scanned}");
        }
    }

    /// Many segments on a slow link: the time budget ends every page long before one chunk of
    /// each segment arrives one by one. Every page still moves on, and the pages together are
    /// the whole list, in order, nothing twice.
    #[test]
    fn many_segments_on_a_slow_link_make_progress_page_by_page() {
        let data: Vec<(i32, Records)> = (0..24)
            .map(|p| {
                let records = (0..300)
                    .map(|o| (o, (o * 24 + p as i64) * 10, (o + p as i64) % 5 == 0))
                    .collect();
                (p, records)
            })
            .collect();
        let expected = places(&run(&mut Fake::new(&data), budget(10_000, 10_000_000)));
        assert_eq!(expected.len(), 24 * 60);

        let mut fake = Fake::new(&data);
        let mut ranges = fake.ranges();
        let mut got = Vec::new();
        for pages in 1.. {
            assert!(pages < 1000, "{} of {} found", got.len(), expected.len());
            fake.allowance = Some(2_000);
            let page = merge(&mut fake, &ranges, budget(50, 50_000)).unwrap();
            got.extend(places(&page));
            if page.exhausted {
                break;
            }
            let next = after(&ranges, &page);
            assert!(
                !page.results.is_empty() || next != ranges,
                "page {pages} made no progress: {:?}",
                page.positions
            );
            ranges = next;
        }
        assert_eq!(got, expected);
    }

    /// A chunk full of matches keeps only the summaries the page can still return (here 3 of
    /// 400 per segment); the rest is read again by the next page, so nothing is lost.
    #[test]
    fn a_chunk_full_of_matches_is_trimmed_without_losing_a_record() {
        let mut fake = Fake::new(&[
            (0, (0..400).map(|o| (o, o * 2, true)).collect()),
            (1, (0..400).map(|o| (o, o * 2 + 1, true)).collect()),
        ]);
        let page = run(&mut fake, budget(3, 10_000));
        assert_eq!(places(&page), vec![(1, 399), (0, 399), (1, 398)]);
        assert_eq!(positions(&page), vec![(0, 399), (1, 398)]);
        let next = merge(
            &mut fake,
            &[
                Range {
                    segment: "0".into(),
                    lower: 0,
                    upper: 399,
                },
                Range {
                    segment: "1".into(),
                    lower: 0,
                    upper: 398,
                },
            ],
            budget(3, 10_000),
        )
        .unwrap();
        assert_eq!(places(&next), vec![(0, 398), (1, 397), (0, 397)]);
    }

    #[test]
    fn a_budget_beyond_any_position_reads_everything() {
        let mut fake = Fake::new(&[(0, (0..10).map(|o| (o, o, true)).collect())]);
        let page = run(&mut fake, budget(100, u64::MAX));
        assert_eq!(page.results.len(), 10);
        assert!(page.exhausted);
    }

    /// A scan budget below the number of segments still reads every segment (one record each):
    /// pages move on even when the first segments hold matches at their heads.
    #[test]
    fn a_budget_below_the_segments_still_moves_on() {
        let data: Vec<(i32, Records)> = (0..4)
            .map(|p| (p, (0..5).map(|o| (o, o * 4 + p as i64, true)).collect()))
            .collect();
        let expected = places(&run(&mut Fake::new(&data), budget(100, 1_000)));
        let mut fake = Fake::new(&data);
        let mut ranges = fake.ranges();
        let mut got = Vec::new();
        for pages in 1.. {
            assert!(pages <= 100, "no progress: {got:?}");
            let page = merge(&mut fake, &ranges, budget(100, 2)).unwrap();
            assert!(
                page.scanned <= 4,
                "one record per segment: {}",
                page.scanned
            );
            got.extend(places(&page));
            if page.exhausted {
                break;
            }
            ranges = after(&ranges, &page);
        }
        assert_eq!(got, expected);
    }

    #[test]
    fn the_scan_budget_stops_a_search_and_counts_what_was_read() {
        let mut fake = Fake::new(&[
            (0, (0..300).map(|o| (o, o, false)).collect()),
            (1, (0..300).map(|o| (o, o, false)).collect()),
        ]);
        let page = run(&mut fake, budget(10, 100));
        assert_eq!(page.stopped_by, Some(StoppedBy::ScanBudget));
        assert!(!page.exhausted);
        assert_eq!(page.scanned, 100);
        assert!(page.results.is_empty());
        // Progress: the positions moved down from 300.
        assert!(
            page.positions.iter().all(|(_, o)| *o < 300),
            "{:?}",
            page.positions
        );
    }

    #[test]
    fn the_time_budget_stops_a_search_with_what_it_found() {
        let mut fake = Fake::new(&[(0, (0..2000).map(|o| (o, o, true)).collect())]);
        fake.slow_after = Some(1);
        let page = run(&mut fake, budget(5000, 1_000_000));
        assert_eq!(page.stopped_by, Some(StoppedBy::TimeBudget));
        assert_eq!(page.results.len(), FIRST_CHUNK);
        assert_eq!(positions(&page), vec![(0, 2000 - FIRST_CHUNK as i64)]);

        let mut fake = Fake::new(&[(0, vec![(0, 0, true)])]);
        let past = Budget {
            deadline: Instant::now(),
            ..budget(10, 10)
        };
        let page = run(&mut fake, past);
        assert_eq!(page.stopped_by, Some(StoppedBy::TimeBudget));
        assert_eq!(
            positions(&page),
            vec![(0, 1)],
            "nothing read, nothing skipped"
        );
    }

    #[test]
    fn a_search_hitting_the_limit_on_its_last_record_is_exhausted() {
        let mut fake = Fake::new(&[(0, vec![(0, 0, true), (1, 1, true)])]);
        let page = run(&mut fake, budget(2, 100));
        assert!(page.exhausted);
        assert_eq!(page.stopped_by, None);
        let mut fake = Fake::new(&[(0, vec![(0, 0, true), (1, 1, true), (2, 2, true)])]);
        let page = run(&mut fake, budget(2, 100));
        assert!(!page.exhausted);
        assert_eq!(page.stopped_by, Some(StoppedBy::Limit));
        assert_eq!(positions(&page), vec![(0, 1)]);
    }

    #[test]
    fn empty_ranges_are_exhausted_at_once() {
        let mut fake = Fake::new(&[(0, vec![]), (1, vec![])]);
        let page = run(&mut fake, budget(10, 10));
        assert!(page.exhausted);
        assert_eq!(page.scanned, 0);
        assert_eq!(fake.reads, 0);
        // A range that is empty because from/to resolved past the end.
        let page = merge(
            &mut fake,
            &[Range {
                segment: "0".into(),
                lower: 50,
                upper: 10,
            }],
            budget(10, 10),
        )
        .unwrap();
        assert!(page.exhausted);
    }
}
