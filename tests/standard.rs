extern crate RoommateStable;

use RoommateStable::solve_sort;

fn random_score_table(n: usize) -> Vec<f32> {
    let mut score_table: Vec<f32> = Vec::with_capacity(n*n);
    for _ in 0..(n*n) {
        score_table.push(0f32);
    }
    for i in 0..n {
        for j in 0..i {
            let v = rand::random::<f32>();
            score_table[i*n+j] = v;
            score_table[j*n+i] = v;
        }
    }
    score_table
}

fn print_score_table(n: usize, t: &Vec<f32>) {
    println!("n: {}", n);
    println!("s: {}", t.len());
    for i in 0..n {
        for j in 0..n {
            print!("|{}|", (t[(i*n+j) as usize] * 100f32).round() as u32);
        }
        println!();
    }
}

fn verify(n: usize, score_table: &Vec<f32>, result: Vec<usize>, min: f32) {
    //stdout().write_fmt(format_args!("results: {:?}\n", result)).unwrap();
    let prefers_other = |obj: usize, d_other: usize| -> bool {
        let b = {
            if result[obj] == std::usize::MAX {
                score_table[obj*n+ d_other] > min
            } else {
                score_table[obj*n+ d_other] > score_table[obj*n+result[obj]]
            }
        };
        if b {
            //stdout().write_fmt(format_args!("{} prefers {}\n", obj, d_other)).unwrap();
        }
        b
    };

    let will_elope = |obj1: usize, obj2: usize| -> bool {
        prefers_other(obj1, obj2) && prefers_other(obj2, obj1)
    };

    for i in 0..result.len() {
        let v = result[i];
        if (i == v) || ((v > n) && (v != std::usize::MAX)) {
            panic!("Results out of bounds")
        }
        if (v != std::usize::MAX) && (score_table[i*n+v] <= min) {
            panic!("Should not be matched")
        }
    }

    for i in 1..n {
        for j in 0..i {
            if (will_elope)(i, j) {
                panic!(format!("Found elope: ({}, {})", i, j))
            }
        }
    }
}

fn run(n: usize) {
    let s = random_score_table(n);
    print_score_table(n, &s);
    let min = 0.1f32;
    let res = solve_sort(
        n,
        &mut |v1, v2| s[v1 * n + v2] > min,
        &mut |r, v1, v2| s[r * n + v1].partial_cmp(&s[r * n + v2]).unwrap()
    );
    //stdout().write_fmt(format_args!("Done, verifying...\n")).unwrap();
    verify(n, &s, res, min);
}

#[test]
fn attempt() {
    for n in 0..=50 {
        for _ in 0..100 {
            run(n);
        }
    }
}