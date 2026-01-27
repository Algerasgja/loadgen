#[derive(Clone, Debug)]
pub enum ArrivalProcess {
    FixedIntervalMs(u64),
    NormalIntervalMs {
        mean_interval_ms: f64,
        stddev_ms: f64,
        min_interval_ms: u64,
    },
}
