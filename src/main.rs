use std::cell::UnsafeCell;
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;
use rand::Rng;

use crossbeam::thread;
use std::cmp::Ordering;

const LOG_SPECIFIC: bool = false;
const ARRAY_LEN: usize = 10_000;
const P: usize = 10;

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

fn psrs(data: &mut [u32]) {
    let block_len = ARRAY_LEN / P;
    // let mut handles: Vec<std::thread::JoinHandle<()>>  = vec![];
    let global_samples = Arc::new(UnsafeCell::new([0u32; P * P]));
    let pivots = Arc::new(UnsafeCell::new([0u32; P - 1]));
    const OMEGA: usize = ARRAY_LEN / (P * P);

    thread::scope(|scope| {
        let barrier = Arc::new(Barrier::new(P));
        let pivot_clone = Arc::clone(&pivots);
        for thread_id in 0..P {
            let barrier_clone = barrier.clone();
            let start_idx = thread_id * block_len;
            let end_idx = if thread_id == P - 1 {
                data.len()
            } else {
                (thread_id + 1) * block_len
            };
            let chunk = &mut data[start_idx..end_idx];
            let global_samples_clone = Arc::clone(&global_samples);

            scope.spawn(move |_| {
                // Phase 1: quicksort in parallel on chunks
                thread_quicksort(chunk, 0, chunk.len() - 1);
                barrier_clone.wait();

                // Phase 2: Obtain regular sample in parallel
                let global_samples_ref = unsafe { &mut *global_samples_clone.get()};
                for i in 0..P {
                    let j = i * OMEGA + 1;
                    if j < chunk.len() {
                        global_samples_ref[thread_id * P + i] = chunk[j];
                    }
                }
                barrier_clone.wait();


                if thread_id == 0 {
                    // Phase 2: A dedicated thread sorts the regular sample
                    thread_quicksort(global_samples_ref, 0, global_samples_ref.len() - 1);
                    let pivot_ref = unsafe { &mut *pivots.get()};

                    // Phase 2: dedicated thread collects the pivots
                    for i in 0..P {
                        pivot_ref[i - 1] = global_samples_ref[i * (global_samples_ref.len() / P)];
                    }
                }
                barrier_clone.wait();

                // Phase 3: bins are created based on the pivots
                let mut partitions: Vec<Vec<u32>> = vec![vec![]; P];
                let pivot_ref = unsafe { &mut *pivots.get()};



            });
        }
    }).unwrap();
}

fn verify_sorted(data: &[u32]) -> bool {
    let mut is_valid: bool = true;
    for i in 0..(ARRAY_LEN - 1) {
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
    let mut warm_ups = 1;

    for i in 0..4 {
        if warm_ups > 0 {
            println!("WARMUP!!")
        } else {
            println!("---------------------------");
            println!("Run #{}", i - warm_ups);
        }
        let mut data = generate_data(ARRAY_LEN, 0, 50);

        let start = Instant::now();
        psrs(&mut data);
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
