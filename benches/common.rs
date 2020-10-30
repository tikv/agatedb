use rand::{distributions::Alphanumeric, Rng};

#[allow(dead_code)]
pub fn rand_value() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .collect::<String>()
}
