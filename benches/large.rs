#![feature(test)]

extern crate RoommateStable;
extern crate test;

use RoommateStable::solve_sort;
use test::Bencher;

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

#[bench]
fn large(b: &mut Bencher) {
    let n = 500;
    let s = random_score_table(n);
    let min = 0.1f32;
    b.iter(|| {
        solve_sort(
            n,
            &mut |v1, v2| s[v1 * n + v2] > min,
            &mut |r, v1, v2| s[r * n + v1].partial_cmp(&s[r * n + v2]).unwrap()
        );
    });
}