//! Congestion control for the server: the OH-Baseline algorithm of spec 4.3.1.
//!
//! Clients report how many packets of ours arrived within a window they were
//! asked to measure. To turn that into a loss ratio the server has to know how
//! many packets it sent during exactly that window, which is what this module
//! keeps track of: when each feedback request went out, and how many packets
//! had been sent by any given point in time.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::net::IpAddr;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use std::time::Instant;

use log::debug;
use log::trace;

static MIN_RATE: u64 = 10000; // Minimum rate in bytes per second
static START_RATE: u64 = 1000000; // Start rate in bytes per second
static MAX_RATE: u64 = 100000000000; // Maximum rate in bytes per second
pub static FEEDBACK_COUNT: u16 = 10; // Number of feedback packets to request per feedback request packet
pub static FEEDBACK_TIMESPAN: u16 = 500; // Time span in milliseconds to request feedback for
static LOSS_RATE_THRESHOLD: f64 = 0.05; // Loss rate threshold to decrease rate 5%
static LOSS_RATE_PERCENTILE: f64 = 95.0; // The Percentile of loss rates to consider when adjusting the rate

/// Factor the rate grows by while there is no congestion.
static SLOW_INCREASE: f64 = 1.05;
/// Factor the rate grows by once the path has been free of congestion for
/// [`FAST_INCREASE_AFTER`].
static FAST_INCREASE: f64 = 1.5;
static FAST_INCREASE_AFTER: Duration = Duration::from_secs(10);

/// How long the record of sent packets is kept. Must comfortably exceed one
/// feedback time span, since feedback arrives after the window it reports on.
static SEND_HISTORY: Duration = Duration::from_secs(5);
/// Resolution of that record. Packets sent closer together than this share a
/// sample, which bounds the memory the record needs at high sending rates.
static SEND_HISTORY_RESOLUTION: Duration = Duration::from_millis(1);
/// Feedback is accepted for this many of the most recent feedback requests.
/// Clients answer a request over its whole time span, so answers to the
/// previous request are still arriving when the next one goes out.
const TRACKED_REQUESTS: usize = 4;

/// A feedback request we sent and still accept feedback for.
#[derive(Clone, Copy)]
struct SentRequest {
    id: u16,
    /// When the request went out, i.e. when the window it measures starts.
    sent_at: Instant,
    timespan: Duration,
    /// Number of packets sent before the request went out.
    packets_before: u64,
}

struct Inner {
    /// `(time, packets sent up to then)`, oldest first.
    sent: VecDeque<(Instant, u64)>,
    /// Packets sent before the oldest sample we still keep.
    dropped_packets: u64,
    sent_packets: u64,
    requests: VecDeque<SentRequest>,
    /// Loss ratios reported since the last rate adjustment.
    losses: Vec<f64>,
    /// When the rate last had to be decreased; the server's start time until
    /// that happens for the first time.
    last_congestion: Instant,
}

impl Inner {
    /// Number of packets that had been sent at `time`.
    fn packets_sent_by(&self, time: Instant) -> u64 {
        match self.sent.binary_search_by(|(sample, _)| sample.cmp(&time)) {
            Ok(index) => self.sent[index].1,
            // Older than every sample we still have, so all we know is what was
            // sent before them.
            Err(0) => self.dropped_packets,
            Err(index) => self.sent[index - 1].1,
        }
    }
}

pub struct CongestionControl {
    /// Current sending rate in bytes per second. Read by the sender for every
    /// packet, hence an atomic instead of a lock.
    rate: AtomicU64,
    feedback_id: AtomicU16,
    inner: Mutex<Inner>,
}

impl CongestionControl {
    pub fn new() -> Self {
        Self {
            rate: AtomicU64::new(START_RATE),
            feedback_id: AtomicU16::new(0),
            inner: Mutex::new(Inner {
                sent: VecDeque::new(),
                dropped_packets: 0,
                sent_packets: 0,
                requests: VecDeque::new(),
                losses: Vec::new(),
                last_congestion: Instant::now(),
            }),
        }
    }

    fn inner(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The rate the server must not exceed, in bytes per second.
    pub fn rate(&self) -> u64 {
        self.rate.load(Relaxed)
    }

    /// Records that one packet went out just now.
    ///
    /// Every packet the server sends has to be recorded, not just data packets:
    /// clients count everything they receive from us, so anything left out here
    /// shows up as negative loss.
    pub fn record_sent_packet(&self) {
        let now = Instant::now();
        let mut inner = self.inner();
        inner.sent_packets += 1;
        let count = inner.sent_packets;
        match inner.sent.back_mut() {
            Some((sample, sample_count)) if now.duration_since(*sample) < SEND_HISTORY_RESOLUTION => *sample_count = count,
            _ => inner.sent.push_back((now, count)),
        }
        while inner.sent.front().is_some_and(|(sample, _)| now.duration_since(*sample) > SEND_HISTORY) {
            let (_, dropped) = inner.sent.pop_front().expect("just checked that there is a sample");
            inner.dropped_packets = dropped;
        }
    }

    /// The id the next feedback request has to carry. Request ids increase and
    /// wrap around (spec 3.9).
    pub fn next_feedback_id(&self) -> u16 {
        self.feedback_id.fetch_add(1, Relaxed).wrapping_add(1)
    }

    /// Remembers the window a feedback request measures.
    ///
    /// Call this right *after* the request went out and was recorded with
    /// [`CongestionControl::record_sent_packet`]: the window covers the packets
    /// sent after the request (spec 4.3.1.1), and the client starts counting
    /// when the request arrives, so the request itself is on neither side.
    pub fn feedback_request_sent(&self, id: u16, timespan: Duration) {
        let mut inner = self.inner();
        let packets_before = inner.sent_packets;
        inner.requests.push_back(SentRequest { id, sent_at: Instant::now(), timespan, packets_before });
        while inner.requests.len() > TRACKED_REQUESTS {
            inner.requests.pop_front();
        }
    }

    /// Turns one feedback packet into a loss ratio (spec 4.3.1.1).
    pub fn handle_feedback(&self, received_packets: u16, packet_index: u16, request_id: u16, src: IpAddr) {
        let mut inner = self.inner();
        let Some(request) = inner.requests.iter().find(|request| request.id == request_id).copied() else {
            debug!("Ignoring feedback from {src} for feedback request {request_id}, which is not one of the recent ones");
            return;
        };

        // The packet index says which part of the request's time span this
        // feedback covers (spec 3.10), and so which of our packets it counted.
        let covered = request.timespan.mul_f64(f64::from(packet_index) / f64::from(u16::MAX));
        let expected = inner.packets_sent_by(request.sent_at + covered).saturating_sub(request.packets_before);
        if expected == 0 {
            trace!("Ignoring feedback from {src} for request {request_id}: we sent nothing in the window it reports on");
            return;
        }

        let loss = (1.0 - f64::from(received_packets) / expected as f64).clamp(0.0, 1.0);
        trace!("Feedback from {src} for request {request_id}: {received_packets} of {expected} packet(s) arrived, loss {loss:.3}");
        inner.losses.push(loss);
    }

    /// Adjusts the sending rate to the loss the clients reported since the last
    /// adjustment (spec 4.3.1.1).
    pub fn adjust_rate(&self) {
        let now = Instant::now();
        let mut inner = self.inner();
        if inner.losses.is_empty() {
            debug!("No feedback received, not adjusting the sending rate");
            return;
        }

        let mut losses = std::mem::take(&mut inner.losses);
        losses.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        // A percentile rather than a mean: it decides how many slow clients the
        // server is willing to leave behind, instead of letting the aggregation
        // decide implicitly.
        let target_loss = percentile(&losses, LOSS_RATE_PERCENTILE);

        let rate = self.rate();
        let new_rate = if target_loss > LOSS_RATE_THRESHOLD {
            inner.last_congestion = now;
            ((rate as f64 * (1.0 - target_loss)) as u64).max(MIN_RATE)
        } else {
            // Ramp up carefully at first and faster once the path has proven to
            // be free of congestion for a while.
            let factor = if now.duration_since(inner.last_congestion) > FAST_INCREASE_AFTER { FAST_INCREASE } else { SLOW_INCREASE };
            ((rate as f64 * factor) as u64).min(MAX_RATE)
        };
        self.rate.store(new_rate, Relaxed);

        debug!(
            "Sending rate {rate} -> {new_rate} bytes/s ({LOSS_RATE_PERCENTILE}th percentile loss {target_loss:.3} over {} feedback packet(s))",
            losses.len()
        );
    }
}

/// The `p`th percentile of `sorted`, by nearest rank.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p / 100.0 * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn client() -> IpAddr {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    }

    /// Feedback that accounts for every packet sent in the reported window must
    /// not look like loss, no matter how much was sent before the window.
    #[test]
    fn no_loss_when_everything_arrived() {
        let cc = CongestionControl::new();
        for _ in 0..500 {
            cc.record_sent_packet();
        }
        let id = cc.next_feedback_id();
        cc.feedback_request_sent(id, Duration::from_millis(FEEDBACK_TIMESPAN.into()));
        for _ in 0..20 {
            cc.record_sent_packet();
        }

        // A feedback covering the whole time span, reporting all 20 packets.
        cc.handle_feedback(20, u16::MAX, id, client());
        let rate_before = cc.rate();
        cc.adjust_rate();
        assert!(cc.rate() > rate_before, "the rate must grow when nothing is lost");
    }

    #[test]
    fn half_the_packets_lost_halves_the_rate() {
        let cc = CongestionControl::new();
        let id = cc.next_feedback_id();
        cc.feedback_request_sent(id, Duration::from_millis(FEEDBACK_TIMESPAN.into()));
        for _ in 0..100 {
            cc.record_sent_packet();
        }

        cc.handle_feedback(50, u16::MAX, id, client());
        let rate_before = cc.rate();
        cc.adjust_rate();
        assert!(cc.rate() < rate_before / 2 + rate_before / 10, "the rate must roughly halve at 50% loss, got {}", cc.rate());
    }

    /// Feedback for a window that ended before some packets were sent must not
    /// count those packets as lost.
    #[test]
    fn partial_window_only_counts_packets_sent_within_it() {
        let cc = CongestionControl::new();
        let id = cc.next_feedback_id();
        cc.feedback_request_sent(id, Duration::from_millis(500));
        std::thread::sleep(Duration::from_millis(5));
        for _ in 0..10 {
            cc.record_sent_packet();
        }

        // A feedback that covers the first millisecond of the time span, which
        // ended before any of those packets went out.
        let one_millisecond = u16::MAX / 500;
        cc.handle_feedback(0, one_millisecond, id, client());
        cc.adjust_rate();
        assert_eq!(cc.rate(), START_RATE, "feedback about a window we sent nothing in must be ignored");

        // A feedback that covers the whole time span does count them.
        cc.handle_feedback(0, u16::MAX, id, client());
        cc.adjust_rate();
        assert!(cc.rate() < START_RATE, "total loss over the whole time span must lower the rate");
    }

    #[test]
    fn feedback_for_forgotten_requests_is_ignored() {
        let cc = CongestionControl::new();
        let stale = cc.next_feedback_id();
        cc.feedback_request_sent(stale, Duration::from_millis(500));
        for _ in 0..=TRACKED_REQUESTS {
            let id = cc.next_feedback_id();
            cc.feedback_request_sent(id, Duration::from_millis(500));
        }
        cc.record_sent_packet();
        cc.handle_feedback(0, u16::MAX, stale, client());
        cc.adjust_rate();
        assert_eq!(cc.rate(), START_RATE);
    }

    #[test]
    fn percentile_picks_the_worst_clients() {
        let losses = [0.0, 0.0, 0.0, 0.0, 0.5];
        assert_eq!(percentile(&losses, 95.0), 0.5);
        assert_eq!(percentile(&losses, 50.0), 0.0);
        assert_eq!(percentile(&[], 95.0), 0.0);
    }
}
