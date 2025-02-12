use std::cell::UnsafeCell;
use std::cmp::Reverse;
use std::sync::{Arc, Barrier};
use std::time::Instant;
use rand::Rng;

use crossbeam::thread;

use std::collections::BinaryHeap;

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
    let global_samples = Arc::new(UnsafeCell::new([0u32; P * P]));
    let pivots = Arc::new(UnsafeCell::new([0u32; P - 1]));
    let partitions = Arc::new(UnsafeCell::new(vec![vec![Vec::new(); P]; P]));
    let partition_lens = Arc::new(UnsafeCell::new(vec![0; P]));
    const OMEGA: usize = ARRAY_LEN / (P * P);

    thread::scope(|scope| {
        let barrier = Arc::new(Barrier::new(P));
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
            let global_partitions_clone = Arc::clone(&partitions);
            let partition_lens_clone = Arc::clone(&partition_lens);

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

                // Phase 3: partitions in each thread chunk are created based on the pivots
                // let mut partitions: Vec<Vec<u32>> = vec![vec![]; P];
                let pivot_ref = unsafe { &mut *pivots.get()};

                let mut prev = 0;
                let global_partitions_ref = unsafe { &mut *global_partitions_clone.get()};
                for (i, &pivot_val) in pivot_ref.iter().enumerate() {
                    let ind = match chunk.binary_search(&pivot_val) {
                        Ok(idx) => idx + 1,
                        Err(idx) => {idx}
                    };
                    global_partitions_ref[thread_id][i].extend_from_slice(&chunk[prev..ind]);
                    prev = ind;
                }
                global_partitions_ref[thread_id][P - 1] = chunk[prev..chunk.len()].to_vec();
                barrier_clone.wait();

                // Part 4: Create a min heap for a k-way merge within each thread
                let mut min_heap = BinaryHeap::new();
                for (partitionIndex, partition) in global_partitions_ref.iter().enumerate() {
                    if !partition.is_empty() {
                        // Each thread 'thread_id' picks the first element from every other
                        // thread's partition in order to start the k-way merge
                        min_heap.push(Reverse((partition[thread_id][0], partitionIndex, 0)));
                    }
                }

                // Part 4: Begin the k-way merge
                let mut final_local_partition = Vec::new();
                let partition_lens_ref = unsafe { &mut *partition_lens_clone.get()};
                while let Some(Reverse((val, partitionIndex, prevIndex))) = min_heap.pop() {
                    // We use partitionIndex to track which sub-vector we just popped from,
                    // so we must push in the next element in that sub-vector to our min heap, IF
                    // it isn't empty
                    final_local_partition.push(val);
                    if prevIndex + 1 < global_partitions_ref[partitionIndex][thread_id].len() {
                        min_heap.push(Reverse((global_partitions_ref[partitionIndex][thread_id][prevIndex + 1], partitionIndex, prevIndex + 1)));
                    }
                }
                partition_lens_ref[thread_id] = final_local_partition.len();
                barrier_clone.wait();

                let mut offset = 0;
                if thread_id > 0 {
                    for i in 0..thread_id {
                        offset += partition_lens_ref[i];
                    }
                }

                for i in 0..final_local_partition.len() {
                    data[offset + i] = final_local_partition[i];
                }
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
