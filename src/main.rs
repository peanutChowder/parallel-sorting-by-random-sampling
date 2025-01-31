use std::time::Instant;
use rand::Rng;

fn generate_data(n: usize, start: u32, end: u32) -> Box<[u32]> {
    let timeStart = Instant::now();
    let mut data: Box<[u32]> = vec![0; n].into_boxed_slice();
    let mut rng = rand::rng();

    for i in 0..n {
        data[i] = rng.random_range(start..end);
    }

    let duration = timeStart.elapsed();
    println!("Time elapsed: {:?}", duration);

    return data;
}

fn main() {
    let data = generate_data(10_000_000, 0, 50);
}
