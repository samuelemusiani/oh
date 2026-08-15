use std::{
    cmp::Ordering,
    net::IpAddr,
    sync::atomic::{AtomicU16, AtomicU64},
};

use log::debug;
use tokio::sync::{Mutex, RwLock};

static MIN_RATE: u64 = 10000; // Minimum rate in bytes per second
static START_RATE: u64 = 10000000; // Start rate in bytes per second 
static MAX_RATE: u64 = 100000000000; // Maximum rate in bytes per second
pub static FEEDBACK_COUNT: u16 = 10; // Number of feedback packets to request per feedback request packet
pub static FEEDBACK_TIMESPAN: u16 = 500; // Time span in milliseconds to request feedback for
static LOSS_RATE_THRESHOLD: f64 = 0.05; // Loss rate threshold to decrease rate 5%
static LOSS_RATE_PERCENTILE: u8 = 95; // The Percentile of loss rates to consider when adjusting the rate
pub struct CongestionControl {
    rate: RwLock<u64>,
    packet_count: AtomicU64,
    feedback_id: AtomicU16,
    loss_rates: Mutex<Vec<f64>>,
    successful_increases: AtomicU64,
}

impl CongestionControl {
    pub fn new() -> Self {
        CongestionControl {
            rate: RwLock::new(START_RATE),
            packet_count: AtomicU64::new(0),
            feedback_id: AtomicU16::new(0),
            loss_rates: Mutex::new(Vec::new()),
            successful_increases: AtomicU64::new(0),
        }
    }
    pub async fn get_rate(&self) -> u64 {
        let rate = self.rate.read().await;
        *rate
    }
    pub fn add_packet(&self) {
        self.packet_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    pub async fn handle_feedback(&self, received_packets: u16, _: u16, request_id: u16, src: IpAddr) {
        let current_feedback_id = self.feedback_id.load(std::sync::atomic::Ordering::SeqCst);
        if request_id != current_feedback_id {
            debug!(
                "Ignoring feedback packet from {} because request_id {} does not match current feedback_id {}",
                src, request_id, current_feedback_id
            );
            return;
        }
        let loss_rate = 1.0 - (received_packets as f64 / self.packet_count.load(std::sync::atomic::Ordering::SeqCst) as f64);
        let mut loss_rates = self.loss_rates.lock().await;
        loss_rates.push(loss_rate);
    }
    pub fn get_feedback_id(&self) -> u16 {
        self.feedback_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn adjust_rate(&self) {
        let mut loss_rates: tokio::sync::MutexGuard<'_, Vec<f64>> = self.loss_rates.lock().await;
        if loss_rates.is_empty() {
            debug!("No feedback received, not adjusting rate");
            return;
        }
        loss_rates.sort_by(|a, b| self.cmp_f64(a, b));
        let percentile_index = loss_rates.len() as f64 * LOSS_RATE_PERCENTILE as f64 / 100.0;
        let loss_rate: f64;
        if percentile_index == percentile_index.floor() {
            let lower_index = percentile_index.floor() as usize;
            let upper_index = percentile_index.ceil() as usize;
            let lower_value = *loss_rates.get(lower_index).unwrap_or(&0.0);
            let upper_value = *loss_rates.get(upper_index).unwrap_or(&0.0);
            let interpolated_value = (lower_value + upper_value) / 2.0;
            loss_rate = interpolated_value;
        } else {
            let index = percentile_index.floor() as usize;
            loss_rate = *loss_rates.get(index).unwrap_or(&0.0);
        }
        let mut rate = self.rate.write().await;
        if loss_rate > LOSS_RATE_THRESHOLD {
            *rate = std::cmp::max((*rate as f64 * (1.0 - loss_rate)) as u64, MIN_RATE);
        } else {
            if self.successful_increases.load(std::sync::atomic::Ordering::Relaxed) > 10 {
                *rate = std::cmp::min((*rate as f64 * 1.5) as u64, MAX_RATE);
            } else {
                *rate = std::cmp::min((*rate as f64 * 1.05) as u64, MAX_RATE);
            }
            self.successful_increases.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        loss_rates.clear();
    }

    //This will violently explode on some values (NaN, inf, -inf) but those should never happen in this context so we don't care
    fn cmp_f64(&self, a: &f64, b: &f64) -> Ordering {
        if a < b {
            return Ordering::Less;
        } else if a > b {
            return Ordering::Greater;
        }
        return Ordering::Equal;
    }
}
