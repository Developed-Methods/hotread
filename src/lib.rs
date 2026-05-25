use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Weak},
};

use parking_lot::Mutex;

const QUIESCENT_GENERATION: u64 = u64::MAX;

pub trait HotReadState: Send + Sync + Clone + 'static {
    type Action: Clone + Send + Sync + 'static;

    fn apply_update(&mut self, update: &Self::Action);
}

pub struct HotRead<T>
where
    T: HotReadState,
{
    copies: [CopySlot<T>; 2],
    generation: AtomicU64,
    state: Mutex<UpdateState<T::Action>>,
    workers: Mutex<Vec<Weak<WorkerSlot>>>,
}

struct CopySlot<T> {
    value: UnsafeCell<T>,
    applied_seq: AtomicU64,
}

struct WorkerSlot {
    generation: AtomicU64,
}

struct UpdateState<U> {
    log: VecDeque<QueuedUpdate<U>>,
    next_seq: u64,
}

#[derive(Clone)]
struct QueuedUpdate<U> {
    seq: u64,
    update: U,
}

pub struct HotReadHandle<'a, T>
where
    T: HotReadState,
{
    owner: &'a HotRead<T>,
    slot: Arc<WorkerSlot>,
    generation: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublishedUpdateResult {
    pub first_seq: u64,
    pub last_seq: u64,
    pub queued_updates: usize,
    pub applied_to_inactive: usize,
    pub ready_to_publish: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaintenanceResult {
    pub applied_updates: usize,
    pub pruned_updates: usize,
    pub ready_to_publish: bool,
    pub blocked_by_workers: bool,
}

unsafe impl<T> Sync for HotRead<T> where T: HotReadState {}

impl<T> HotRead<T>
where
    T: HotReadState,
{
    pub fn new(initial: T) -> Self {
        Self {
            copies: [
                CopySlot {
                    value: UnsafeCell::new(initial.clone()),
                    applied_seq: AtomicU64::new(0),
                },
                CopySlot {
                    value: UnsafeCell::new(initial),
                    applied_seq: AtomicU64::new(0),
                },
            ],
            generation: AtomicU64::new(0),
            state: Mutex::new(UpdateState {
                log: VecDeque::new(),
                next_seq: 1,
            }),
            workers: Mutex::new(Vec::new()),
        }
    }

    pub fn create_handle(&self) -> HotReadHandle<'_, T> {
        let slot = Arc::new(WorkerSlot {
            generation: AtomicU64::new(QUIESCENT_GENERATION),
        });
        self.workers.lock().push(Arc::downgrade(&slot));

        HotReadHandle {
            owner: self,
            slot,
            generation: QUIESCENT_GENERATION,
        }
    }

    pub fn queue_update(&self, update: T::Action) -> PublishedUpdateResult {
        self.queue_updates([update])
    }

    pub fn queue_updates<I>(&self, updates: I) -> PublishedUpdateResult
    where
        I: IntoIterator<Item = T::Action>,
    {
        let mut state = self.state.lock();
        let first_seq = state.next_seq;
        let mut last_seq = first_seq.saturating_sub(1);
        let mut queued_updates = 0;

        for update in updates {
            let seq = state.next_seq;
            state.next_seq += 1;
            last_seq = seq;
            queued_updates += 1;
            state.log.push_back(QueuedUpdate { seq, update });
        }

        if queued_updates == 0 {
            return PublishedUpdateResult {
                first_seq: 0,
                last_seq: 0,
                queued_updates: 0,
                applied_to_inactive: 0,
                ready_to_publish: false,
            };
        }

        let (applied_to_inactive, ready_to_publish) = self.apply_to_offline_locked(&state);
        self.prune_locked(&mut state);

        PublishedUpdateResult {
            first_seq,
            last_seq,
            queued_updates,
            applied_to_inactive,
            ready_to_publish,
        }
    }

    pub fn maintain(&self) -> MaintenanceResult {
        let mut state = self.state.lock();
        let offline = self.offline_index();

        if !self.workers_caught_up_to_published() {
            return MaintenanceResult {
                applied_updates: 0,
                pruned_updates: 0,
                ready_to_publish: false,
                blocked_by_workers: true,
            };
        }

        let (applied_updates, ready_to_publish) = self.apply_to_copy_locked(offline, &state);
        let pruned_updates = self.prune_locked(&mut state);

        MaintenanceResult {
            applied_updates,
            pruned_updates,
            ready_to_publish,
            blocked_by_workers: false,
        }
    }

    pub fn active_index(&self) -> usize {
        Self::index_for_generation(self.generation())
    }

    pub fn latest_seq(&self) -> u64 {
        self.state.lock().next_seq.saturating_sub(1)
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn copy_applied_seq(&self, index: usize) -> u64 {
        self.copies[index].applied_seq.load(Ordering::Acquire)
    }

    pub fn queued_update_count(&self) -> usize {
        self.state.lock().log.len()
    }

    fn offline_index(&self) -> usize {
        Self::index_for_generation(self.generation() + 1)
    }

    fn apply_to_offline_locked(&self, state: &UpdateState<T::Action>) -> (usize, bool) {
        if !self.workers_caught_up_to_published() {
            return (0, false);
        }

        self.apply_to_copy_locked(self.offline_index(), state)
    }

    fn apply_to_copy_locked(&self, index: usize, state: &UpdateState<T::Action>) -> (usize, bool) {
        let mut applied = 0;
        let mut applied_seq = self.copies[index].applied_seq.load(Ordering::Acquire);

        for queued in &state.log {
            if queued.seq <= applied_seq {
                continue;
            }

            // SAFETY: caller holds state, this copy is inactive, and worker slots were checked.
            let copy = unsafe { &mut *self.copies[index].value.get() };
            copy.apply_update(&queued.update);
            applied_seq = queued.seq;
            applied += 1;
        }

        self.copies[index]
            .applied_seq
            .store(applied_seq, Ordering::Release);

        let latest_seq = state.next_seq.saturating_sub(1);
        let ready_to_publish = applied_seq == latest_seq && latest_seq != self.active_applied_seq();
        if ready_to_publish {
            self.generation.fetch_add(1, Ordering::Release);
        }

        (applied, ready_to_publish)
    }

    fn active_applied_seq(&self) -> u64 {
        self.copies[self.active_index()]
            .applied_seq
            .load(Ordering::Acquire)
    }

    fn prune_locked(&self, state: &mut UpdateState<T::Action>) -> usize {
        let min_applied = self.copies[0]
            .applied_seq
            .load(Ordering::Acquire)
            .min(self.copies[1].applied_seq.load(Ordering::Acquire));
        let before = state.log.len();

        while state
            .log
            .front()
            .is_some_and(|queued| queued.seq <= min_applied)
        {
            state.log.pop_front();
        }

        before - state.log.len()
    }

    fn workers_caught_up_to_published(&self) -> bool {
        let generation = self.generation();
        let mut workers = self.workers.lock();
        let mut caught_up = true;

        workers.retain(|worker| {
            let Some(worker) = worker.upgrade() else {
                return false;
            };

            let worker_generation = worker.generation.load(Ordering::Acquire);
            if worker_generation != QUIESCENT_GENERATION && worker_generation < generation {
                caught_up = false;
            }

            true
        });

        caught_up
    }

    #[cfg(test)]
    fn worker_generation_count(&self, generation: u64) -> usize {
        let mut workers = self.workers.lock();
        let mut count = 0;

        workers.retain(|worker| {
            let Some(worker) = worker.upgrade() else {
                return false;
            };

            if worker.generation.load(Ordering::Acquire) == generation {
                count += 1;
            }

            true
        });

        count
    }

    fn index_for_generation(generation: u64) -> usize {
        (generation as usize) & 1
    }
}

impl<T> HotReadHandle<'_, T>
where
    T: HotReadState,
{
    pub fn current(&mut self) -> &T {
        loop {
            let generation = self.owner.generation();

            self.slot.generation.store(generation, Ordering::Release);
            self.generation = generation;

            if self.owner.generation() != generation {
                continue;
            }

            let index = HotRead::<T>::index_for_generation(generation);

            // SAFETY: this worker publishes its generation and verifies the owner generation did
            // not change before returning the reference. Maintenance only mutates the offline copy
            // after every active worker has either caught up to the published generation or gone
            // quiescent.
            return unsafe { &*self.owner.copies[index].value.get() };
        }
    }

    pub fn quiescent(&mut self) {
        self.slot
            .generation
            .store(QUIESCENT_GENERATION, Ordering::Release);
        self.generation = QUIESCENT_GENERATION;
    }

    pub fn copy_index(&self) -> Option<usize> {
        (self.generation != QUIESCENT_GENERATION)
            .then_some(HotRead::<T>::index_for_generation(self.generation))
    }

    pub fn generation(&self) -> u64 {
        self.slot.generation.load(Ordering::Acquire)
    }
}

impl<T> Drop for HotReadHandle<'_, T>
where
    T: HotReadState,
{
    fn drop(&mut self) {
        self.quiescent();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    };
    use std::time::{Duration, Instant};

    #[derive(Clone, Default)]
    struct TestTable {
        values: BTreeMap<u64, u64>,
    }

    #[derive(Clone)]
    enum TestUpdate {
        Set(u64, u64),
        Remove(u64),
    }

    impl HotReadState for TestTable {
        type Action = TestUpdate;

        fn apply_update(&mut self, update: &TestUpdate) {
            match *update {
                TestUpdate::Set(key, value) => {
                    self.values.insert(key, value);
                }
                TestUpdate::Remove(key) => {
                    self.values.remove(&key);
                }
            }
        }
    }

    #[test]
    fn read_returns_initial_copy() {
        let mut table = TestTable::default();
        table.values.insert(1, 10);
        let published = HotRead::new(table);
        let mut worker = published.create_handle();

        assert_eq!(worker.current().values.get(&1), Some(&10));
    }

    #[test]
    fn queue_update_applies_to_inactive_not_active() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut worker = published.create_handle();
        let current = worker.current();

        let result = published.queue_update(TestUpdate::Set(1, 10));

        assert_eq!(result.applied_to_inactive, 1);
        assert_eq!(current.values.get(&1), None);
    }

    #[test]
    fn next_read_publishes_ready_copy() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut worker = published.create_handle();
        published.queue_update(TestUpdate::Set(1, 10));

        assert_eq!(worker.current().values.get(&1), Some(&10));
    }

    #[test]
    fn old_worker_reference_continues_to_see_old_copy_after_publish() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut stale_worker = published.create_handle();
        let old = stale_worker.current();
        published.queue_update(TestUpdate::Set(1, 10));

        let mut fresh_worker = published.create_handle();
        assert_eq!(fresh_worker.current().values.get(&1), Some(&10));
        assert_eq!(old.values.get(&1), None);
    }

    #[test]
    fn maintain_blocks_when_stale_copy_has_worker() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut stale_worker = published.create_handle();
        let old = stale_worker.current();
        published.queue_update(TestUpdate::Set(1, 10));
        let mut fresh_worker = published.create_handle();
        assert_eq!(fresh_worker.current().values.get(&1), Some(&10));

        let result = published.maintain();

        assert!(result.blocked_by_workers);
        assert_eq!(old.values.get(&1), None);
    }

    #[test]
    fn maintain_catches_stale_copy_up_after_worker_quiescent() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut stale_worker = published.create_handle();
        {
            let old = stale_worker.current();
            published.queue_update(TestUpdate::Set(1, 10));
            let mut fresh_worker = published.create_handle();
            assert_eq!(fresh_worker.current().values.get(&1), Some(&10));
            assert_eq!(old.values.get(&1), None);
        }
        stale_worker.quiescent();

        let result = published.maintain();

        assert_eq!(result.applied_updates, 1);
        assert_eq!(published.queued_update_count(), 0);
    }

    #[test]
    fn updates_apply_in_sequence_order() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut worker = published.create_handle();
        published.queue_updates([TestUpdate::Set(1, 10), TestUpdate::Set(1, 20)]);

        assert_eq!(worker.current().values.get(&1), Some(&20));
    }

    #[test]
    fn batch_publish_increments_generation_once() {
        let published = HotRead::<TestTable>::new(TestTable::default());

        let result = published.queue_updates([TestUpdate::Set(1, 10), TestUpdate::Set(2, 20)]);

        assert!(result.ready_to_publish);
        assert_eq!(published.generation(), 1);
        assert_eq!(published.active_index(), 1);
        assert_eq!(published.copy_applied_seq(1), 2);
    }

    #[test]
    fn log_prunes_only_after_both_copies_apply() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut stale_worker = published.create_handle();
        {
            let old = stale_worker.current();
            published.queue_update(TestUpdate::Set(1, 10));
            let mut fresh_worker = published.create_handle();
            assert_eq!(fresh_worker.current().values.get(&1), Some(&10));
            assert_eq!(old.values.get(&1), None);
        }

        assert_eq!(published.queued_update_count(), 1);
        stale_worker.quiescent();
        published.maintain();
        assert_eq!(published.queued_update_count(), 0);
    }

    #[test]
    fn concurrent_workers_do_not_observe_partial_updates() {
        let published = std::sync::Arc::new(HotRead::<TestTable>::new(TestTable::default()));
        published.queue_updates((0..100).map(|i| TestUpdate::Set(i, i + 1)));

        let workers: Vec<_> = (0..4)
            .map(|_| {
                let published = published.clone();
                std::thread::spawn(move || {
                    let mut worker = published.create_handle();
                    for _ in 0..100 {
                        let len = worker.current().values.len();
                        assert!(len == 0 || len == 100);
                    }
                })
            })
            .collect();

        for worker in workers {
            worker.join().unwrap();
        }
    }

    #[test]
    fn worker_quiescent_clears_current_copy() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut worker = published.create_handle();
        worker.current();
        assert_eq!(published.worker_generation_count(published.generation()), 1);
        worker.quiescent();
        assert_eq!(published.worker_generation_count(published.generation()), 0);
    }

    #[test]
    fn remove_update_is_applied() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut worker = published.create_handle();
        published.queue_updates([TestUpdate::Set(1, 10), TestUpdate::Remove(1)]);
        assert_eq!(worker.current().values.get(&1), None);
    }

    #[test]
    fn worker_handle_reads_initial_copy() {
        let mut table = TestTable::default();
        table.values.insert(1, 10);
        let published = HotRead::new(table);
        let mut worker = published.create_handle();

        assert_eq!(worker.current().values.get(&1), Some(&10));
        assert_eq!(worker.copy_index(), Some(published.active_index()));
    }

    #[test]
    fn worker_refresh_publishes_ready_copy() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut worker = published.create_handle();

        assert_eq!(worker.current().values.get(&1), None);
        published.queue_update(TestUpdate::Set(1, 10));

        assert_eq!(worker.current().values.get(&1), Some(&10));
        assert_eq!(worker.generation(), 1);
    }

    #[test]
    fn maintain_blocks_while_worker_holds_stale_copy() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut stale_worker = published.create_handle();
        assert_eq!(stale_worker.current().values.get(&1), None);

        published.queue_update(TestUpdate::Set(1, 10));

        let mut fresh_worker = published.create_handle();
        assert_eq!(fresh_worker.current().values.get(&1), Some(&10));

        let result = published.maintain();
        assert!(result.blocked_by_workers);
    }

    #[test]
    fn maintain_catches_stale_worker_copy_after_quiescent() {
        let published = HotRead::<TestTable>::new(TestTable::default());
        let mut stale_worker = published.create_handle();
        assert_eq!(stale_worker.current().values.get(&1), None);

        published.queue_update(TestUpdate::Set(1, 10));

        let mut fresh_worker = published.create_handle();
        assert_eq!(fresh_worker.current().values.get(&1), Some(&10));
        assert!(published.maintain().blocked_by_workers);

        stale_worker.quiescent();
        let result = published.maintain();

        assert_eq!(result.applied_updates, 1);
        assert_eq!(published.queued_update_count(), 0);
    }

    #[test]
    fn busy_workers_receive_timely_updates_from_maintenance() {
        const WORKERS: usize = 4;
        const RUN_FOR: Duration = Duration::from_secs(5);
        const UPDATE_INTERVAL: Duration = Duration::from_millis(5);
        const FINAL_CATCH_UP_TIMEOUT: Duration = Duration::from_millis(500);
        const MIN_DISTINCT_UPDATES_PER_WORKER: u64 = 100;

        let published = Arc::new(HotRead::<TestTable>::new(TestTable::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let latest_queued = Arc::new(AtomicU64::new(0));
        let latest_observed = Arc::new((0..WORKERS).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());

        let maintainer = {
            let published = published.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                while !stop.load(AtomicOrdering::Acquire) {
                    published.maintain();
                    std::hint::spin_loop();
                }

                for _ in 0..10_000 {
                    if published.queued_update_count() == 0 {
                        break;
                    }
                    published.maintain();
                    std::hint::spin_loop();
                }
            })
        };

        let workers: Vec<_> = (0..WORKERS)
            .map(|index| {
                let published = published.clone();
                let stop = stop.clone();
                let latest_observed = latest_observed.clone();
                std::thread::spawn(move || {
                    let mut worker = published.create_handle();
                    let mut local_max = 0;
                    let mut observed_distinct_updates = 0;

                    while !stop.load(AtomicOrdering::Acquire) {
                        if let Some(value) = worker.current().values.get(&1).copied()
                            && value > local_max
                        {
                            local_max = value;
                            observed_distinct_updates += 1;
                            latest_observed[index].store(local_max, AtomicOrdering::Release);
                        }
                        std::hint::spin_loop();
                    }

                    worker.quiescent();
                    (local_max, observed_distinct_updates)
                })
            })
            .collect();

        let updater = {
            let published = published.clone();
            let latest_queued = latest_queued.clone();
            std::thread::spawn(move || {
                let start = Instant::now();
                let mut value = 0;

                while start.elapsed() < RUN_FOR {
                    value += 1;
                    published.queue_update(TestUpdate::Set(1, value));
                    latest_queued.store(value, AtomicOrdering::Release);
                    std::thread::sleep(UPDATE_INTERVAL);
                }

                value
            })
        };

        let final_value = updater.join().unwrap();
        let catch_up_started = Instant::now();

        while catch_up_started.elapsed() < FINAL_CATCH_UP_TIMEOUT {
            if latest_observed
                .iter()
                .all(|observed| observed.load(AtomicOrdering::Acquire) == final_value)
            {
                break;
            }

            std::thread::sleep(Duration::from_millis(1));
        }

        stop.store(true, AtomicOrdering::Release);

        let worker_observations: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        maintainer.join().unwrap();

        assert!(final_value > 0);
        assert_eq!(latest_queued.load(AtomicOrdering::Acquire), final_value);
        assert_eq!(published.queued_update_count(), 0);

        for (index, (observed, observed_distinct_updates)) in
            worker_observations.into_iter().enumerate()
        {
            assert!(
                observed_distinct_updates >= MIN_DISTINCT_UPDATES_PER_WORKER,
                "worker {index} observed only {observed_distinct_updates} distinct updates"
            );
            assert_eq!(
                observed, final_value,
                "worker {index} did not observe latest update {final_value} within {:?}",
                FINAL_CATCH_UP_TIMEOUT
            );
        }
    }
}
