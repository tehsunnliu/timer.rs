//! A simple timer, used to enqueue operations meant to be executed at
//! a given time or after a given delay.

extern crate time;

use std::cmp::Ordering;
use std::thread;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::mpsc::{channel, Sender};
use std::collections::BinaryHeap;
    
use time::{Duration, SteadyTime};

/// An item scheduled for delayed execution.
struct Schedule {
    /// The instant at which to execute.
    date: SteadyTime,

    /// The callback to execute.
    cb: Box<Fn() + Send>
}
impl Ord for Schedule {
    fn cmp(&self, other: &Self) -> Ordering {
        self.date.cmp(&other.date).reverse()
    }
}
impl PartialOrd for Schedule {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.date.partial_cmp(&other.date).map(|ord| ord.reverse())
    }
}
impl Eq for Schedule {
}
impl PartialEq for Schedule {
    fn eq(&self, other: &Self) -> bool {
        self.date.eq(&other.date)
    }
}

/// An operation to be sent across threads.
enum Op {
    /// Schedule a new item for execution.
    Schedule(Schedule),

    /// Stop the thread.
    Stop
}

/// A timer, used to schedule execution of callbacks at a later date.
///
/// In the current implementation, each timer is executed as two
/// threads. The _Scheduler_ thread is in charge of maintaining the
/// queue of callbacks to execute and of actually executing them. The
/// _Communication_ thread is in charge of communicating with the
/// _Scheduler_ thread (which requires acquiring a possibly-long-held
/// Mutex) without blocking the caller thread.
pub struct Timer {
    tx: Sender<Op>
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.tx.send(Op::Stop).unwrap();
    }
}

impl Timer {
    /// Create a timer.
    ///
    /// This immediatey launches two threads, which will remain
    /// launched until the timer is dropped. As expected, the threads
    /// spend most of their life waiting for instructions.
    pub fn new() -> Self {
        Self::with_capacity(32)
    }

    /// As `new()`, but with a manually specified initial capaicty.
    pub fn with_capacity(capacity: usize) -> Self {
        let waiter_rcv = Arc::new((Mutex::new(Vec::with_capacity(capacity)), Condvar::new()));
        let waiter_snd = waiter_rcv.clone();

        // Spawn a first thread, whose sole role is to dispatch
        // messages to the second thread without having to wait too
        // long for the mutex.
        let (tx, rx) = channel();
        thread::spawn(move || {
            use Op::*;
            for msg in rx.iter() {
                let (ref mutex, ref condvar) = *waiter_snd;
                let mut vec = mutex.lock().unwrap();
                match msg {
                    Schedule(sched) => {
                        vec.push(Schedule(sched));
                        condvar.notify_one();
                    }
                    Stop => {
                        vec.clear();
                        vec.push(Op::Stop);
                        condvar.notify_one();
                        return;
                    }
                }
            }
        });

        // Spawn a second thread, in charge of scheduling.
        let mut heap = BinaryHeap::with_capacity(capacity);
        thread::Builder::new().name("Timer thread".to_owned()).spawn(move || {
            let (ref mutex, ref condvar) = *waiter_rcv;
            loop {
                let mut lock = mutex.lock().unwrap();

                // Pop all messages.
                for msg in lock.drain(..) {
                    match msg {
                        Op::Stop => return,
                        Op::Schedule(sched) => heap.push(sched),
                    }
                }

                // Pop all the callbacks that are ready.
                let mut delay = None;
                loop {
                    let now = SteadyTime::now();
                    if let Some(sched) = heap.peek() {
                        if sched.date > now {
                            // First item is not ready yet, so nothing is ready.
                            // We assume that `sched.date > now` is still true.
                            delay = Some(sched.date - now);
                            break;
                        }
                    } else {
                        // No item at all.
                        break;
                    }
                    let sched = heap.pop().unwrap(); // We just checked that the heap is not empty.
                    (sched.cb)();
                }

                match delay {
                    None => {
                        let _ = condvar.wait(lock);
                    },
                    Some(delay) => {
                        let sec = delay.num_seconds();
                        let ns = (delay - Duration::seconds(sec)).num_nanoseconds().unwrap(); // Cannot be > 1_000_000_000.
                        let duration = std::time::Duration::new(sec as u64, ns as u32);
                        let _ = condvar.wait_timeout(lock, duration);
                    }
                }
            }
        }).unwrap();
        Timer {
            tx: tx
        }
    }

    /// Schedule a callback for execution after a delay.
    ///
    /// Callbacks are guaranteed to never be called before the
    /// delay. However, it is possible that they will be called a
    /// little after the delay.
    ///
    /// If the delay is negative, the callback is executed as soon as
    /// possible.
    ///
    /// # Performance
    ///
    /// The callback is executed on the Scheduler thread. It should
    /// therefore terminate very quickly, or risk causing delaying
    /// other callbacks.
    ///
    /// # Failures
    ///
    /// Any failure in `cb` will scheduler thread and progressively
    /// contaminate the Timer and the calling thread itself. You have
    /// been warned.
    ///
    /// # Example
    ///
    /// ```
    /// extern crate timer;
    /// extern crate time;
    /// use std::sync::mpsc::channel;
    ///
    /// let timer = timer::Timer::new();
    /// let (tx, rx) = channel();
    ///
    /// timer.schedule_with_delay(time::Duration::seconds(3), move || {
    ///   tx.send(()).unwrap();
    /// });
    ///
    /// rx.recv().unwrap();
    /// println!("This code has been executed after 3 seconds");
    /// ```
    pub fn schedule_with_delay<F>(&self, delay: Duration, cb: F)
        where F: 'static + Fn() + Send {
        self.schedule_with_date(SteadyTime::now() + delay, cb)
    }

    /// Schedule a callback for execution at a given date.
    ///
    /// Callbacks are guaranteed to never be called before their
    /// date. However, it is possible that they will be called a
    /// little after it.
    ///
    /// If the date is in the past, the callback is executed as soon
    /// as possible.
    ///
    /// # Performance
    ///
    /// The callback is executed on the Scheduler thread. It should
    /// therefore terminate very quickly, or risk causing delaying
    /// other callbacks.
    ///
    /// # Failures
    ///
    /// Any failure in `cb` will scheduler thread and progressively
    /// contaminate the Timer and the calling thread itself. You have
    /// been warned.
    pub fn schedule_with_date<F>(&self, date: SteadyTime, cb: F)
        where F: 'static + Fn() + Send {
        self.tx.send(Op::Schedule(Schedule {
            date: date,
            cb: Box::new(cb)
        })).unwrap();
    }
}

#[test]
fn test_schedule_with_delay() {
    let timer = Timer::new();
    let (tx, rx) = channel();

    // Schedule a number of callbacks in an arbitrary order, make sure
    // that they are executed in the right order.
    let mut delays = vec![1, 5, 3, -1];
    let start = SteadyTime::now();
    for i in delays.clone() {
        println!("Scheduling for execution in {} seconds", i);
        let tx = tx.clone();
        timer.schedule_with_delay(Duration::seconds(i), move || {
            println!("Callback {}", i);
            tx.send(i).unwrap();
        });
    }

    delays.sort();
    for (i, msg) in (0..delays.len()).zip(rx.iter()) {
        let elapsed = (SteadyTime::now() - start).num_seconds();
        println!("Received message {} after {} seconds", msg, elapsed);
        assert_eq!(msg, delays[i]);
        assert!(delays[i] <= elapsed && elapsed <= delays[i] + 3, "We have waited {} seconds, expecting [{}, {}]", elapsed, delays[i], delays[i] + 3);
    }

    // Now make sure that callbacks that are designed to be executed
    // immediately are executed quickly.
    let start = SteadyTime::now();
    for i in vec![10, 0] {
        println!("Scheduling for execution in {} seconds", i);
        let tx = tx.clone();
        timer.schedule_with_delay(Duration::seconds(i), move || {
            println!("Callback {}", i);
            tx.send(i).unwrap();
        });
    }

    assert_eq!(rx.recv().unwrap(), 0);
    assert!(SteadyTime::now() - start <= Duration::seconds(1));
}
