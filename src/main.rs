use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;
use rand::Rng;

const LOG_SPECIFIC: bool = false;

fn generate_data(n: usize, start: u32, end: u32) -> Box<[u32]> {
    let time_start = Instant::now();
    let mut data: Box<[u32]> = vec![0; n].into_boxed_slice();
    let mut rng = rand::rng();

    for i in 0..n {
        data[i] = rng.random_range(start..end);
    }

    let duration = time_start.elapsed();
    println!("Time elapsed for generation: {:?}", duration);

    data
}

fn thread_quicksort_partition(data: &mut [u32], low: usize, high: usize) -> usize {
    let pivot = data[high];
    let mut i = low;
    for j in low..high {
        if data[j] < pivot {
            data.swap(i, j);
            i = i + 1;
        }
    }
    data.swap(i, high);
    i
}

fn thread_quicksort(data: &mut [u32], low: usize, high: usize) {
    if low < high {
        let pivot = thread_quicksort_partition(data, low, high);
        if pivot > low {
            thread_quicksort(data, low, pivot - 1);
        }
        thread_quicksort(data, pivot + 1, high);
    }
}

fn psrs(data: &mut [u32], p: usize) {
    let block_len = data.len() / p;
    let mut handles: Vec<std::thread::JoinHandle<()>>  = vec![];

    for chunk in data.chunks_mut(block_len) {
        let mut chunk = chunk.to_vec();
        let chunk_len = chunk.len();
        let handle = thread::spawn(move || {
            thread_quicksort(&mut chunk[..], 0, chunk_len - 1);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn verify_sorted(data: &[u32]) -> bool {
    let mut is_valid: bool = true;
    for i in 0..(data.len() - 1) {
        if data[i] > data[i + 1] {
            if LOG_SPECIFIC {
                println!("INVALID entry at indices data[{:?}] and data[{:?}]: values {:?} and {:?}", i, i + 1, data[i], data[i + 1]);
            }
            is_valid = false;
        }
    }
    is_valid
}

fn main() {
    let array_len = 100_000;
    let mut warm_ups = 1;

    for i in 0..4 {
        if warm_ups > 0 {
            println!("WARMUP!!")
        } else {
            println!("---------------------------");
            println!("Run #{}", i - warm_ups);
        }
        let mut data = generate_data(array_len, 0, 50);

        let start = Instant::now();
        psrs(&mut data, 5);
        let duration = start.elapsed();
        println!("Time elapsed in quicksort: {:?}", duration);

        let start = Instant::now();
        let success = verify_sorted(&data);
        let duration = start.elapsed();
        println!("Time elapsed in verification: {:?}", duration);

        if warm_ups > 0 {
            warm_ups -= 1;
        } else {
            println!("\nRun #{} success status: {}", i - warm_ups, if success {"success."} else {"FAIL."});
        }
    }
}
