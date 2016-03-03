//! A simple timer, used to enqueue operations meant to be executed at
//! a given time or after a given delay.

extern crate chrono;

use std::cmp::Ordering;
use std::thread;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::mpsc::{channel, Sender};
use std::collections::BinaryHeap;
use chrono::{Duration, DateTime, UTC};

/// An item scheduled for delayed execution.
struct Schedule {
    /// The instant at which to execute.
    date: DateTime<UTC>,

    /// The callback to execute.
    cb: Box<FnMut() + Send>,

    /// A mechanism to cancel execution of an item.
    guard: Guard,

    /// If `Some(d)`, the item must be repeated every interval of
    /// length `d`, until cancelled.
    repeat: Option<Duration>
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

/// A mutex-based kind-of-channel used to communicate between the
/// Communication thread and the Scheuler thread.
struct WaiterChannel {
    /// Pending messages.
    messages: Mutex<Vec<Op>>,
    /// A condition variable used for waiting.
    condvar: Condvar,
}
impl WaiterChannel {
    fn with_capacity(cap: usize) -> Self {
        WaiterChannel {
            messages: Mutex::new(Vec::with_capacity(cap)),
            condvar: Condvar::new(),
        }
    }
}

struct Scheduler {
    waiter: Arc<WaiterChannel>,
    heap: BinaryHeap<Schedule>,
}

impl Scheduler {
    fn with_capacity(waiter: Arc<WaiterChannel>, capacity: usize) -> Self {
        Scheduler {
            waiter: waiter,
            heap: BinaryHeap::with_capacity(capacity),
        }
    }
    fn run(&mut self) {
        let ref waiter = *self.waiter;
        loop {
            let mut lock = waiter.messages.lock().unwrap();

            // Pop all messages.
            for msg in lock.drain(..) {
                match msg {
                    Op::Stop => {
                        return;
                    }
                    Op::Schedule(sched) => self.heap.push(sched),
                }
            }

            // Pop all the callbacks that are ready.
            let mut delay = None;
            loop {
                let now = UTC::now();
                if let Some(sched) = self.heap.peek() {
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
                let mut sched = self.heap.pop().unwrap(); // We just checked that the heap is not empty.
                if !sched.guard.should_execute() {
                    // Skip this callback.
                    continue;
                }
                (sched.cb)();
                if let Some(delta) = sched.repeat {
                    // This is a repeating timer, so we need to enqueue the next
                    // call, unless it has been cancelled.
                    assert!(delay.is_none());
                    delay = Some(delta);
                    self.heap.push(Schedule {
                        date: sched.date + delta,
                        cb: sched.cb,
                        guard: sched.guard,
                        repeat: Some(delta)
                    });
                }
            }

            match delay {
                None => {
                    let _ = waiter.condvar.wait(lock);
                },
                Some(delay) => {
                    let sec = delay.num_seconds();
                    let ns = (delay - Duration::seconds(sec)).num_nanoseconds().unwrap(); // This `unwrap()` asserts that the number of ns is not > 1_000_000_000. Since we just substracted the number of seconds, the assertion should always pass.
                    let duration = std::time::Duration::new(sec as u64, ns as u32);
                    let _ = waiter.condvar.wait_timeout(lock, duration);
                }
            }
        }
    }
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
    /// Sender used to communicate with the _Communication_ thread. In
    /// turn, this thread will send 
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
        let waiter_send = Arc::new(WaiterChannel::with_capacity(capacity));
        let waiter_recv = waiter_send.clone();

        // Spawn a first thread, whose sole role is to dispatch
        // messages to the second thread without having to wait too
        // long for the mutex.
        let (tx, rx) = channel();
        thread::spawn(move || {
            use Op::*;
            let ref waiter = *waiter_send;
            for msg in rx.iter() {
                let mut vec = waiter.messages.lock().unwrap();
                match msg {
                    Schedule(sched) => {
                        vec.push(Schedule(sched));
                        waiter.condvar.notify_one();
                    }
                    Stop => {
                        vec.clear();
                        vec.push(Op::Stop);
                        waiter.condvar.notify_one();
                        return;
                    }
                }
            }
        });

        // Spawn a second thread, in charge of scheduling.
        thread::Builder::new().name("Timer thread".to_owned()).spawn(move || {
            let mut scheduler = Scheduler::with_capacity(waiter_recv, capacity);
            scheduler.run()
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
    /// If the delay is negative or 0, the callback is executed as
    /// soon as possible.
    ///
    /// This method returns a `Guard` object. If that `Guard` is
    /// dropped, execution is cancelled.
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
    /// extern crate chrono;
    /// use std::sync::mpsc::channel;
    ///
    /// let timer = timer::Timer::new();
    /// let (tx, rx) = channel();
    ///
    /// let _guard = timer.schedule_with_delay(chrono::Duration::seconds(3), move || {
    ///   // This closure is executed on the scheduler thread,
    ///   // so we want to move it away asap.
    ///
    ///   let _ignored = tx.send(()); // Avoid unwrapping here.
    /// });
    ///
    /// rx.recv().unwrap();
    /// println!("This code has been executed after 3 seconds");
    /// ```
    pub fn schedule_with_delay<F>(&self, delay: Duration, cb: F) -> Guard
        where F: 'static + FnMut() + Send {
        self.schedule_with_date(UTC::now() + delay, cb)
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
    /// This method returns a `Guard` object. If that `Guard` is
    /// dropped, execution is cancelled.
    ///
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
    pub fn schedule_with_date<F, T>(&self, date: DateTime<T>, cb: F) -> Guard
        where F: 'static + FnMut() + Send, T : chrono::offset::TimeZone
    {
        self.schedule(date, None, cb)
    }

    /// Schedule a callback for execution once per interval.
    ///
    /// Callbacks are guaranteed to never be called before their
    /// date. However, it is possible that they will be called a
    /// little after it.
    ///
    /// This method returns a `Guard` object. If that `Guard` is
    /// dropped, repeat is stopped.
    ///
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
    /// extern crate chrono;
    /// use std::thread;
    /// use std::sync::{Arc, Mutex};
    ///
    /// let timer = timer::Timer::new();
    /// // Number of times the callback has been called.
    /// let count = Arc::new(Mutex::new(0));
    ///
    /// // Start repeating. Each callback increases `count`.
    /// let guard = {
    ///   let count = count.clone();
    ///   timer.schedule_repeating(chrono::Duration::milliseconds(5), move || {
    ///     *count.lock().unwrap() += 1;
    ///   })
    /// };
    ///
    /// // Sleep one second. The callback should be called ~200 times.
    /// thread::sleep(std::time::Duration::new(1, 0));
    /// let count_result = *count.lock().unwrap();
    /// assert!(190 <= count_result && count_result <= 210,
    ///   "The timer was called {} times", count_result);
    ///
    /// // Now drop the guard. This should stop the timer.
    /// drop(guard);
    ///
    /// // Let's check that the count stops increasing.
    /// let count_start = *count.lock().unwrap();
    /// thread::sleep(std::time::Duration::new(1, 0));
    /// let count_stop =  *count.lock().unwrap();
    /// assert_eq!(count_start, count_stop);
    /// ```
    pub fn schedule_repeating<F>(&self, repeat: Duration, cb: F) -> Guard
        where F: 'static + FnMut() + Send
    {
        self.schedule(UTC::now() + repeat, Some(repeat), cb)
    }

    
    /// Schedule a callback for execution at a given time, then once
    /// per interval. A typical use case is to execute code once per
    /// day at 12am.
    ///
    /// Callbacks are guaranteed to never be called before their
    /// date. However, it is possible that they will be called a
    /// little after it.
    ///
    /// This method returns a `Guard` object. If that `Guard` is
    /// dropped, repeat is stopped.
    ///
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
    pub fn schedule<F, T>(&self, date: DateTime<T>, repeat: Option<Duration>, cb: F) -> Guard
        where F: 'static + FnMut() + Send, T : chrono::offset::TimeZone
    {
        let guard = Guard::new();
        self.tx.send(Op::Schedule(Schedule {
            date: date.with_timezone(&UTC),
            cb: Box::new(cb),
            guard: guard.clone(),
            repeat: repeat
        })).unwrap();
        guard
    }
}

#[derive(Clone)]
pub struct Guard {
    should_execute: Arc<AtomicBool>
}
impl Guard {
    fn new() -> Self {
        Guard {
            should_execute: Arc::new(AtomicBool::new(true))
        }
    }
    fn should_execute(&self) -> bool {
        self.should_execute.load(AtomicOrdering::Relaxed)
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.should_execute.store(false, AtomicOrdering::Relaxed)
    }
}

#[test]
fn test_schedule_with_delay() {
    let timer = Timer::new();
    let (tx, rx) = channel();
    let mut guards = vec![];

    // Schedule a number of callbacks in an arbitrary order, make sure
    // that they are executed in the right order.
    let mut delays = vec![1, 5, 3, -1];
    let start = UTC::now();
    for i in delays.clone() {
        println!("Scheduling for execution in {} seconds", i);
        let tx = tx.clone();
        guards.push(timer.schedule_with_delay(Duration::seconds(i), move || {
            println!("Callback {}", i);
            tx.send(i).unwrap();
        }));
    }

    delays.sort();
    for (i, msg) in (0..delays.len()).zip(rx.iter()) {
        let elapsed = (UTC::now() - start).num_seconds();
        println!("Received message {} after {} seconds", msg, elapsed);
        assert_eq!(msg, delays[i]);
        assert!(delays[i] <= elapsed && elapsed <= delays[i] + 3, "We have waited {} seconds, expecting [{}, {}]", elapsed, delays[i], delays[i] + 3);
    }

    // Now make sure that callbacks that are designed to be executed
    // immediately are executed quickly.
    let start = UTC::now();
    for i in vec![10, 0] {
        println!("Scheduling for execution in {} seconds", i);
        let tx = tx.clone();
        guards.push(timer.schedule_with_delay(Duration::seconds(i), move || {
            println!("Callback {}", i);
            tx.send(i).unwrap();
        }));
    }

    assert_eq!(rx.recv().unwrap(), 0);
    assert!(UTC::now() - start <= Duration::seconds(1));
}

#[test]
fn test_guards() {
    println!("Testing that callbacks aren't called if the guard is dropped");
    let timer = Timer::new();
    let called = Arc::new(Mutex::new(false));

    for i in 0..10 {
        let called = called.clone();
        timer.schedule_with_delay(Duration::milliseconds(i), move || {
            *called.lock().unwrap() = true;
        });
    }

    thread::sleep(std::time::Duration::new(1, 0));
    assert_eq!(*called.lock().unwrap(), false);
}
