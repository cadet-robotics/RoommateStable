#![allow(non_snake_case)]

use std::fmt::{Display, Error, Formatter};
use std::cmp::Ordering;

extern crate rand;
extern crate rayon;
extern crate parking_lot;
extern crate crossbeam;

use rayon::prelude::*;
use rayon::{scope, Scope};
use std::collections::{HashSet, BTreeSet};
use core::sync::atomic::AtomicUsize;
use std::intrinsics::transmute;
use std::sync::atomic::Ordering::Relaxed;
use std::cell::RefCell;
use core::mem::{MaybeUninit, replace};
use parking_lot::{Mutex, MutexGuard};
use std::ops::DerefMut;
use std::time::Duration;
use crossbeam::queue::ArrayQueue;
use std::thread::Thread;
use std::hash::BuildHasherDefault;

#[derive(Copy, Clone, Eq, PartialEq)]
enum MatchStatus {
    Unmatched,
    Matched(usize),
    Failed
}

impl Display for Row {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self.ret {
            MatchStatus::Failed => f.write_str("[ XXX ]"),
            MatchStatus::Unmatched => f.write_fmt(format_args!("{:?}", &self.row_data[self.first..=self.last])),
            MatchStatus::Matched(v) => f.write_fmt(format_args!("|{}|", v))
        }
    }
}

impl Display for FunMatrix {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        for (i, r) in self.rows.iter().enumerate() {
            f.write_fmt(format_args!("Row {}: {}\n", i, r.lock().borrow_mut()))?;
        }
        Ok(())
    }
}

fn create_array<T>(size: usize) -> Box<[MaybeUninit<T>]> {
    unsafe {
        let mut v = Vec::with_capacity(size);
        v.set_len(size);
        v.into_boxed_slice()
    }
}

unsafe fn transform_array<T>(a: Box<[MaybeUninit<T>]>) -> Box<[T]> {
    transmute(a)
}

#[inline(always)]
fn new_array_with<T: Send, F: (Fn(usize) -> T) + Send + Sync>(f: &mut F, cnt: usize) -> Box<[T]> {
    let mut a = create_array(cnt);
    a.par_iter_mut().enumerate().for_each(|args: (usize, &mut MaybeUninit<T>)| {
        let pos = args.0;
        let v = args.1;
        replace(v, MaybeUninit::new((f)(pos)));
    });
    unsafe {transform_array(a)}
}

fn new_array<T: Clone + Send + Sync>(v: &T, cnt: usize) -> Box<[T]> {
    new_array_with(&mut |_| v.clone(), cnt)
}

struct FunMatrix {
    rows: Box<[Mutex<RefCell<Row>>]>,
    cnt: usize,
    left: AtomicUsize
}

struct Row {
    ret: MatchStatus,
    row_data: Box<[usize]>,
    lookup: Box<[usize]>,
    rejects: Box<[bool]>,
    first: usize,
    second: usize,
    last: usize
}

impl Row {
    fn new_sort<F1: Fn(usize) -> bool, F2: (Fn(usize, usize) -> Ordering) + Sync>(row_cnt: usize, row: usize, exists: F1, sort: F2) -> (Self, bool) {
        let mut row_data = Vec::with_capacity(row_cnt - 1);
        let mut cnt = 0;
        while (cnt as usize) < row_cnt {
            if (row != cnt) && (exists)(cnt) {
                row_data.push(cnt)
            }
            cnt += 1;
        }
        row_data.par_sort_unstable_by(|v1, v2| (sort)(*v1, *v2).reverse());
        Row::new(row_cnt, row_data.into_boxed_slice())
    }

    fn new(row_cnt: usize, row_data: Box<[usize]>) -> (Self, bool) {
        let row_data_len = row_data.len();
        if row_data_len > 0 {
            let mut lookup: Box<[usize]> = new_array(&std::usize::MAX, row_cnt);
            for e in row_data.iter().enumerate() {
                lookup[*e.1] = e.0
            }
            (Row {
                ret: MatchStatus::Unmatched,
                row_data,
                lookup,
                rejects: new_array(&false, row_data_len),
                first: 0,
                second: 1,
                last: row_data_len - 1
            }, false)
        } else {
            (Row {
                ret: MatchStatus::Failed,
                row_data,
                lookup: vec![].into_boxed_slice(),
                rejects: vec![].into_boxed_slice(),
                first: 0,
                second: 0,
                last: 0
            }, true)
        }
    }

    fn could_match(&self, row: usize) -> bool {
        if self.ret == MatchStatus::Unmatched {
            let index = self.lookup[row];
            if (self.first..=self.last).contains(&index) {
                !self.rejects[index]
            } else {
                false
            }
        } else {
            false
        }
    }

    fn rejected_by(&mut self, row: usize, counter: &AtomicUsize) {
        //stdout().write_fmt(format_args!("---> Rejecting {}\n", row)).unwrap();
        let index = self.lookup[row];
        if (self.first..=self.last).contains(&index) {
            if self.first == self.last {
                self.ret = MatchStatus::Failed;
                counter.fetch_sub(1, Relaxed);
            } else if self.second >= self.last {
                self.first = self.last;
            } else if index == self.first {
                self.first = self.second;
                while {
                    self.second += 1;
                    self.rejects[self.second]
                } {}
            } else if index == self.last {
                while {
                    self.last -= 1;
                    self.rejects[self.last]
                } {}
            } else {
                self.rejects[index] = true;
            }
        }
    }

    fn match_with<F: FnMut(usize)>(&mut self, other: usize, callback: &mut F) {
        for i in self.first..=self.last {
            if (!self.rejects[i]) && (self.row_data[i] != other) {
                (callback)(self.row_data[i])
            }
        }
        self.ret = MatchStatus::Matched(other)
    }

    fn reject_below<F: FnMut(usize)>(&mut self, row: usize, callback: &mut F) {
        let index = self.lookup[row];
        if index < self.first {
            panic!("Someone we rejected wants us")
        } else if index <= self.last {
            for i in (index + 1)..=self.last {
                if !self.rejects[i] {
                    (callback)(self.row_data[i])
                }
            }
            self.last = index;
            while self.rejects[self.last] {
                self.last -= 1;
            }
        }
    }

    #[inline(always)]
    fn get_first(&self) -> Option<usize> {
        if self.ret == MatchStatus::Unmatched {
            Some(self.row_data[self.first])
        } else {
            None
        }
    }

    #[inline(always)]
    fn get_last(&self) -> Option<usize> {
        if self.ret == MatchStatus::Unmatched {
            Some(self.row_data[self.last])
        } else {
            None
        }
    }

    #[inline(always)]
    fn get_second(&self) -> Option<usize> {
        if (self.ret == MatchStatus::Unmatched) && (self.first != self.last) {
            Some(self.row_data[self.second])
        } else {
            None
        }
    }

    #[inline(always)]
    fn is_done(&self) -> bool {
        self.ret != MatchStatus::Unmatched
    }
}

pub fn solve<F1: (Fn(usize) -> Box<[usize]>) + Send + Sync>(n: usize, row_callback: &mut F1) -> Vec<usize> {
    FunMatrix::solve(n, row_callback)
}

pub fn solve_sort<F1: (Fn(usize, usize) -> bool) + Sync, F2: (Fn(usize, usize, usize) -> Ordering) + Sync>(n: usize, exists: &mut F1, sort: &mut F2) -> Vec<usize> {
    FunMatrix::solve_sort(n, exists, sort)
}

impl FunMatrix {
    fn solve<F1: (Fn(usize) -> Box<[usize]>) + Send + Sync>(n: usize, row_callback: &mut F1) -> Vec<usize> {
        if n == 0 {
            return vec![]
        } else if n == 1 {
            return vec![std::usize::MAX]
        }
        if ((n as f64) * ((n as f64) - 1.0)) > ((std::usize::MAX - 1) as f64) {
            panic!("FunMatrix is too big, you broke the warranty")
        }
        let left = AtomicUsize::new(n);
        let rows = new_array_with(&mut |pos| {
            let r = Row::new(n, (row_callback)(pos));
            if r.1 {
                left.fetch_sub(1, Relaxed);
            }
            Mutex::new(RefCell::new(r.0))
        }, n);
        FunMatrix {
            rows,
            cnt: n,
            left
        }._solve()
    }

    fn solve_sort<F1: (Fn(usize, usize) -> bool) + Sync, F2: (Fn(usize, usize, usize) -> Ordering) + Sync>(n: usize, exists: &mut F1, sort: &mut F2) -> Vec<usize> {
        if n == 0 {
            return vec![]
        } else if n == 1 {
            return vec![std::usize::MAX]
        }
        if ((n as f64) * ((n as f64) - 1.0)) > ((std::usize::MAX - 1) as f64) {
            panic!("FunMatrix is too big, you broke the warranty")
        }
        let left = AtomicUsize::new(n);
        let rows = new_array_with(&mut |pos| {
            let r = Row::new_sort(n, pos, |v| (exists)(pos, v), |v1, v2| (sort)(pos, v1, v2));
            if r.1 {
                left.fetch_sub(1, Relaxed);
            }
            Mutex::new(RefCell::new(r.0))
        }, n);
        FunMatrix {
            rows,
            cnt: n,
            left
        }._solve()
    }

    fn _solve(self) -> Vec<usize> {
        //stdout().write_fmt(format_args!("{}", self)).unwrap();
        self.step_one();
        //stdout().write_fmt(format_args!("> Finished step one...\n")).unwrap();
        //stdout().write_fmt(format_args!("{}", self)).unwrap();
        self.step_three();
        //stdout().write_fmt(format_args!("> Finished step three...\n")).unwrap();
        //stdout().write_fmt(format_args!("{}", self)).unwrap();
        Vec::from_par_iter(self.rows.into_vec().par_iter_mut().map(|v| {
            let r = v.get_mut().get_mut();
            match r.ret {
                MatchStatus::Matched(v) => v,
                MatchStatus::Unmatched | MatchStatus::Failed => std::usize::MAX
            }
        }))
    }

    #[inline(always)]
    fn get_row(&self, row: usize) -> MutexGuard<RefCell<Row>> {
        self.rows[row].lock()
    }

    fn get_row_opt(&self, row: usize) -> Option<MutexGuard<RefCell<Row>>> {
        self.rows[row].try_lock_for(Duration::from_millis(1))
    }

    #[inline(always)]
    fn pair<F: FnMut(usize)>(&self, r1: (&mut Row, usize), r2: (&mut Row, usize), callback: &mut F) {
        let mut h = HashSet::new();
        r1.0.match_with(r2.1, &mut |v| {h.insert(v);});
        r2.0.match_with(r1.1, &mut |v| {h.insert(v);});
        self.left.fetch_sub(2, Relaxed);
        h.iter().for_each(|v| (callback)(*v));
    }

    #[inline(always)]
    fn invalidate_pair(&self, r1: (&mut Row, usize), r2: (&mut Row, usize)) {
        r1.0.rejected_by(r2.1, &self.left);
        r2.0.rejected_by(r1.1, &self.left);
    }

    /**
     * Triggers proposal logic for a row
     * Returns the number of rows removed
     */
    fn propose<F: FnMut(usize)>(&self, row: usize, callback: &mut F) {
        //stdout().write_fmt(format_args!("-> Performing {}\n", row)).unwrap();
        let mut row_data_guard = self.get_row(row);
        loop {
            let mut row_data = row_data_guard.borrow_mut();
            let other = match row_data.get_first() {
                Some(v) => v,
                None => break
            };
            //stdout().write_fmt(format_args!("--> Other: {}\n", other)).unwrap();
            let other_data_guard = match self.get_row_opt(other) {
                Some(v) => v,
                None => {
                    drop(row_data);
                    MutexGuard::bump(&mut row_data_guard);
                    continue
                }
            };
            //stdout().write_fmt(format_args!("--> Got other\n")).unwrap();
            let mut other_data = other_data_guard.borrow_mut();
            if !other_data.could_match(row) {
                // They've rejected us
                row_data.rejected_by(other, &self.left);
                // No need to notify others
                // Lets keep going
            } else if row == other_data.get_first().unwrap() {
                // We can match
                // Notify everyone else we need to and stop
                self.pair((row_data.deref_mut(), row), (other_data.deref_mut(), other), callback);
                return
            } else {
                // Propose to other
                // Notify people and stop
                other_data.reject_below(row, callback);
                return
            }
            drop(row_data);
            MutexGuard::bump(&mut row_data_guard);
        }
    }

    /**
     * Makes sure that everyone is proposed to at least someone
     */
    #[inline(never)]
    fn step_one(&self) {
        let mut queue = HashSet::new();
        for i in 0..self.cnt {
            self.propose(i, &mut |v: usize| {
                if v < i {
                    //stdout().write_fmt(format_args!("--> Adding {}\n", v)).unwrap();
                    queue.insert(v);
                }
            });
            while let Some(v) = queue.iter().next() {
                let vv = *v;
                queue.remove(&vv);
                self.propose(vv, &mut |v: usize| {
                    if v <= i {
                        //stdout().write_fmt(format_args!("--> Adding {}\n", v)).unwrap();
                        queue.insert(v);
                    }
                });
            }
        }
    }

    // Step two is for inefficient people
    // (It happens in step one)

    /**
     * Closes loops
     */
    #[inline(never)]
    fn step_three(&self) {
        for start_row in 0..self.cnt {
            loop {
                let start_row_guard = self.get_row(start_row);
                let mut start_row_data = start_row_guard.borrow_mut();
                if start_row_data.is_done() {
                    break
                } else if start_row_data.first == start_row_data.last {
                    start_row_data.ret = MatchStatus::Matched(start_row_data.first);
                    break
                }
                let mut cur_row = start_row_data.get_second().unwrap();
                loop {
                    let cur_row_guard = self.get_row(cur_row);
                    let mut cur_row_data = cur_row_guard.borrow_mut();
                    let last = cur_row_data.get_last().unwrap();
                    let last_row_guard = self.get_row(last);
                    let mut last_row_data = last_row_guard.borrow_mut();
                    self.invalidate_pair((cur_row_data.deref_mut(), cur_row), (last_row_data.deref_mut(), last));
                    if last == start_row {
                        break
                    }
                    cur_row = last_row_data.get_second().unwrap();
                }
            }
        }
    }
}