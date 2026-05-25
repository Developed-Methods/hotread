# HotRead

HotRead is a small Rust library for reading dynamic state from hot paths without
taking a lock on every read.

It keeps two copies of the state. Readers use a `HotReadHandle` to borrow the
currently published copy, while updates are applied to the inactive copy and
published when active readers have moved forward. This means reads are cheap and
thread-safe, and workers may temporarily observe stale data.

This is intended for state where:

- reads are frequent and short lived
- updates can be applied incrementally
- temporary staleness is acceptable
- callers can periodically refresh their handle with `current()`

## Example

```rust
use hotread::{HotRead, HotReadState};
use std::collections::BTreeMap;

#[derive(Clone, Default)]
struct Table {
    values: BTreeMap<u64, u64>,
}

#[derive(Clone)]
enum TableUpdate {
    Set(u64, u64),
    Remove(u64),
}

impl HotReadState for Table {
    type Action = TableUpdate;

    fn apply_update(&mut self, update: &Self::Action) {
        match *update {
            TableUpdate::Set(key, value) => {
                self.values.insert(key, value);
            }
            TableUpdate::Remove(key) => {
                self.values.remove(&key);
            }
        }
    }
}

let table = HotRead::new(Table::default());
let mut reader = table.create_handle();

table.queue_update(TableUpdate::Set(1, 42));

let current = reader.current();
assert_eq!(current.values.get(&1), Some(&42));
```

## Maintenance

`queue_update()` and `queue_updates()` try to apply updates to the inactive copy
immediately. If a worker is still holding an older generation, publication may
be delayed until that worker calls `current()` again or becomes quiescent.

If updates stop while workers are stale, call `maintain()` periodically to apply
queued updates once readers have caught up:

```rust
let result = table.maintain();

if result.blocked_by_workers {
    // Some handle still points at an older generation.
}
```

Handles call `quiescent()` on drop. You can also call it explicitly when a
worker is idle and no longer needs to hold a published copy.

## Notes

- `current()` is the hot read path and does not take the update mutex.
- Readers may observe stale but internally consistent snapshots.
- The slowest live reader controls how quickly old copies can be reused.
- `Action` values must be cloneable, sendable, sync, and `'static`.
