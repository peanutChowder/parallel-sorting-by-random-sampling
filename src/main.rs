use std::time::Instant;
use rand::Rng;

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

fn thread_quicksort_partition(data: &mut Box<[u32]>, low: usize, high: usize) -> usize {
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

fn thread_quicksort(data: &mut Box<[u32]>, low: usize, high: usize) {
    if low < high {
        let pivot = thread_quicksort_partition(data, low, high);
        if pivot > 0 {
            thread_quicksort(data, low, pivot - 1);
        }
        thread_quicksort(data, pivot + 1, high);
    }
}

fn psrs(data: &[u32]) {
    println!("data: {:?}", data);
    todo!()
}

fn verify_sorted(data: &[u32]) -> bool {
    let mut is_valid: bool = true;
    for i in 0..(data.len() - 1) {
        if data[i] > data[i + 1] {
            println!("INVALID entry at indices data[{:?}] and data[{:?}]: values {:?} and {:?}", i, i + 1, data[i], data[i + 1]);
            is_valid = false;
        }
    }
    is_valid
}

fn main() {
    let array_len = 500_000;
    for i in 0..3 {
        println!("---------------------------");
        println!("Run #{}", i + 1);
        let mut data = generate_data(array_len, 0, 50);

        let start = Instant::now();
        thread_quicksort(&mut data, 0, array_len - 1);
        let duration = start.elapsed();
        println!("Time elapsed in quicksort: {:?}", duration);

        let start = Instant::now();
        let success = verify_sorted(&data);
        let duration = start.elapsed();
        println!("Time elapsed in verification: {:?}", duration);
        println!("\nRun #{} success status: {}", i + 1, if success {"success."} else {"FAIL."});
    }
}
