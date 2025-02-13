use rayon::prelude::*;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Instant;

const LOG_SPECIFIC: bool = false;
const ARRAY_LEN: usize = 10_000_000;
const P: usize = 10;

fn generate_data(n: usize, start: u32, end: u32) -> Vec<u32> {
    let time_start = Instant::now();
    let mut data = Vec::with_capacity(n);
    let mut rng = rand::thread_rng();

    for _ in 0..n {
        data.push(rng.gen_range(start..end));
    }

    let duration = time_start.elapsed();
    println!("Time elapsed for generation: {:?}", duration);
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
fn psrs(data: &mut [u32]) {
    let n = data.len();
    let p = P;
    let block_size = n / p;

    // Phase 1: Sort each chunk in parallel.
    data.par_chunks_mut(block_size)
        .for_each(|chunk| {
            chunk.sort_unstable();
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
    samples.sort_unstable();

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

fn run_tests(name: &str, mut warm_ups: i32, num_runs: i32, min_val: u32, max_val: u32) {
    println!("-------------------{name}--------------------------------------");
    for i in (-warm_ups + 1)..(num_runs + 1) {
        if warm_ups > 0 {
            println!("WARMUP!!");
        } else {
            println!("---------------------------");
            println!("Run #{} PSRS", i);
        }
        let mut data = generate_data(ARRAY_LEN, min_val, max_val);

        let start = Instant::now();
        if name == "psrs" {
            psrs(&mut data);
        } else {
            data.sort_unstable();
        }
        let duration = start.elapsed();
        println!("Time elapsed in psrs: {:?}", duration);

        let start = Instant::now();
        let success = verify_sorted(&data);
        let duration = start.elapsed();
        println!("Time elapsed in verification: {:?}", duration);

        if warm_ups > 0 {
            warm_ups -= 1;
        } else {
            println!(
                "\nRun #{} success status: {}",
                i,
                if success { "success." } else { "FAIL." }
            );
        }
    }
    println!("------------------------------------------");
}

fn main() {
    run_tests("psrs", 2, 5, 0, 50);
    run_tests("serial", 2, 5, 0, 50);
}
