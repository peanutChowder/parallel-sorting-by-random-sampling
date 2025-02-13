use rayon::prelude::*;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};
use quicksort::quicksort;

const LOG_RUN_INFO: bool = false;

fn generate_data(n: usize, start: u32, end: u32) -> Vec<u32> {
    let time_start = Instant::now();
    let mut data = Vec::with_capacity(n);
    let mut rng = rand::rng();

    for _ in 0..n {
        data.push(rng.random_range(start..end));
    }

    let duration = time_start.elapsed();
    if LOG_RUN_INFO {
        println!("Time elapsed for generation: {:?}", duration);
    }
    data
}

/// Performs a k‑way merge of several sorted slices using a binary heap.
fn k_way_merge(slices: &[&[u32]]) -> Vec<u32> {
    let mut heap = BinaryHeap::new();
    // Each heap entry is (value, slice_index, index_in_slice).
    // We load up the heap with the first elements of each slice.
    for (i, slice) in slices.iter().enumerate() {
        if !slice.is_empty() {
            heap.push(Reverse((slice[0], i, 0)));
        }
    }

    // Create the final sorted array by selecting the smallest element
    // of our slices given by the min heap.
    let mut merged = Vec::new();
    while let Some(Reverse((val, slice_idx, idx_in_slice))) = heap.pop() {
        merged.push(val);
        let slice = slices[slice_idx];
        let next_idx = idx_in_slice + 1;
        if next_idx < slice.len() {
            heap.push(Reverse((slice[next_idx], slice_idx, next_idx)));
        }
    }
    merged
}

/// The PSRS implementation using Rayon for parallelism.
fn psrs(data: &mut [u32], p: usize) {
    let n = data.len();
    let block_size = n / p;

    // Phase 1: Sort each chunk in parallel.
    data.par_chunks_mut(block_size)
        .for_each(|chunk| {
            quicksort(chunk);
        });

    // Phase 2: From each sorted chunk, take p regular samples.
    let mut samples: Vec<u32> = data
        .par_chunks(block_size) // Assign a chunk to each thread
        .flat_map(|chunk| {
            let m = chunk.len();
            let omega = m / p;

            (0..p) // Each thread gathers its respective local samples from its chunk
                .into_par_iter()
                .map(move |i| {
                    // Choose index; ensure we don’t go out-of-bounds.
                    let idx = if i * omega + 1 < m { i * omega + 1 } else { m - 1 };
                    chunk[idx]
                })
        })
        .collect();

    // The main thread sorts the local samples
    quicksort(&mut samples);

    // Choose p-1 pivots.
    let pivots: Vec<u32> = (1..p).map(|i| samples[i * p]).collect();

    // Phase 3: Compute partition boundaries for each chunk.
    let boundaries: Vec<Vec<usize>> = data
        .par_chunks(block_size)
        .map(|chunk| {
            let mut b = Vec::with_capacity(p + 1);
            b.push(0);
            for &pivot in &pivots {
                // partition_point returns the first index where x > pivot.
                let pos = chunk.partition_point(|&x| x <= pivot);
                b.push(pos);
            }
            b.push(chunk.len());
            b
        })
        .collect();

    // Phase 4: For each partition index, merge the corresponding partitions.
    let merged_partitions: Vec<Vec<u32>> = (0..p)
        .into_par_iter()
        .map(|part_idx| {
            let slices: Vec<&[u32]> = data
                .chunks(block_size)
                .zip(boundaries.iter())
                .map(|(chunk, b)| {
                    let start = b[part_idx];
                    let end = b[part_idx + 1];
                    &chunk[start..end]
                })
                .collect();
            k_way_merge(&slices)
        })
        .collect();

    // Concatenate the merged partitions into one sorted output.
    let mut output = Vec::with_capacity(n);
    for part in merged_partitions {
        output.extend(part);
    }
    data.copy_from_slice(&output);
}

fn verify_sorted(data: &[u32]) -> bool {
    data.windows(2).all(|w| w[0] <= w[1])
}

fn run_tests(name: &str, mut warm_ups: i32, num_runs: i32, data_len: usize, min_val: u32, max_val: u32, p: usize) -> Vec<u128> {
    let mut runtimes = Vec::new();
    if LOG_RUN_INFO {
        println!("-------------------{name}--------------------------------------");
    }
    for i in (-warm_ups + 1)..(num_runs + 1) {
        if LOG_RUN_INFO {
            if warm_ups > 0 {
                println!("WARMUP!!");
            } else {
                println!("---------------------------");
                println!("Run #{i} {name}");
            }
        }

        let mut data = generate_data(data_len, min_val, max_val);

        let start = Instant::now();
        if name == "psrs" {
            psrs(&mut data, p);
        } else {
            quicksort(&mut data);
        }
        let duration = start.elapsed();
        if LOG_RUN_INFO {
            println!("Time elapsed in {name}: {:?}", duration);
        }
        runtimes.push(duration.as_millis());

        let start = Instant::now();
        let success = verify_sorted(&data);
        let duration = start.elapsed();
        if LOG_RUN_INFO {
            println!("Time elapsed in verification: {:?}", duration);
        }

        if warm_ups > 0 {
            warm_ups -= 1;
        } else if LOG_RUN_INFO {
            println!(
                "\nRun #{} success status: {}",
                i,
                if success { "success." } else { "FAIL." }
            );
        }
        if !success {println!("!!!!!!!!!!!!!!!WARNING!!!!!!!!!!!!!!!!!!!!!!!! Incorrect sort output!")}
    }
    if LOG_RUN_INFO {
        println!("------------------------------------------");
    }

    runtimes
}

fn main() {
    let num_runs = 5;

    // let num_threads = 50;
    // for data_len in (0..100_000_001).step_by(10_000_000) {
    //     if data_len == 0 {
    //         continue;
    //     }
    //     let psrs_runs = run_tests("psrs", 2, num_runs, data_len, 0, 50, num_threads);
    //     let serial_runs = run_tests("serial", 2, num_runs, data_len, 0, 50, num_threads);
    //
    //     let psrs_avg = psrs_runs.iter().sum::<u128>() / psrs_runs.len() as u128;
    //     let serial_avg = serial_runs.iter().sum::<u128>() / serial_runs.len() as u128;
    //
    //     println!("{data_len}\t{psrs_avg}\t{serial_avg}")
    // }

    let serial_runs = run_tests("serial", 2, num_runs, 100_000_000, 0, 50, 1);
    let serial_avg = serial_runs.iter().sum::<u128>() / serial_runs.len() as u128;
    println!("serial baseline {}", serial_avg);
    for num_threads in [4, 8, 16, 32, 64, 128] {
        let psrs_runs = run_tests("psrs", 2, num_runs, 100_000_000, 0, 50, num_threads);
        let psrs_avg = psrs_runs.iter().sum::<u128>() / psrs_runs.len() as u128;
        println!("{num_threads}\t{psrs_avg}")
    }
}
