use std::sync::atomic::AtomicU64;

static MIN_RATE: u64 = 1000; // Minimum rate in bytes per second 10 kbit
static MAX_RATE: u64 = 10000000000; // Maximum rate in bytes per second 10 gbit

pub struct CongestionControl {
    rate: u64,
    packet_count: AtomicU64,
}

impl CongestionControl {
    pub fn new() -> Self {
        CongestionControl { rate: MIN_RATE, packet_count: AtomicU64::new(0) }
    }
}
