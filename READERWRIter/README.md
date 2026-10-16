# Readers-Writers with a Monitor (C++20)

Files: `main.cpp` (monitor, workload, event recorder, HTML generator) and `readers_writers_monitor.html` (generated).

## Build and run
```bash
g++ -std=c++20 -O2 -Wall -Wextra -pthread main.cpp -o rw_monitor
./rw_monitor                      # writes readers_writers_monitor.html next to you
./rw_monitor my_trace.html        # optional: custom output name
```
Then open the HTML file directly in a browser (no server, no internet). Windows (MSVC): `cl /std:c++20 /EHsc main.cpp`. Every run produces a slightly different trace because the OS scheduler decides the interleaving. The program exits with code 0 only if all checks pass.

Optional race check: `g++ -std=c++20 -O1 -g -fsanitize=thread -pthread main.cpp -o rw_tsan && ./rw_tsan`

## Algorithm
Monitor state, protected by one mutex: `activeReaders`, `waitingWriters`, `writerActive`. Two condition variables: `canRead`, `canWrite`.

| Operation | Waits while | On exit |
|---|---|---|
| startRead | `writerActive \|\| waitingWriters > 0` | `activeReaders++` |
| endRead | - | `activeReaders--`; if 0 and a writer waits, `canWrite.notify_one()` |
| startWrite | `waitingWriters++`, then waits while `writerActive \|\| activeReaders > 0` | `waitingWriters--`, `writerActive = true` |
| endWrite | - | `writerActive = false`; if writers wait `canWrite.notify_one()`, else `canRead.notify_all()` |

**Writer preference:** once a writer is queued, new readers block, so the stream of readers drains and the writer gets in. This *reduces writer starvation* but is **not strictly fair**: a continuous stream of writers can starve readers, and the order among waiting writers is not guaranteed FIFO (`notify_one` picks any waiter).

**Notification is not admission:** a woken thread must reacquire the mutex and re-check its predicate in a `while` loop. The trace has separate `NOTIFY`, `CV_WAKE` and `ADMIT` events, and the page only shows a thread inside the database after `ADMIT`.

## How the trace is produced
The monitor calls the recorder while holding its own mutex, so each event's snapshot (readers, waitingWriters, writerActive) is exact. The recorder has its own mutex and assigns `seq` and a timestamp together. After joining all threads, `verifyTrace` replays the events and checks every invariant, then the events are embedded as JSON in the HTML. Delays in `main` only stagger when threads *arrive*; they do not script the event order.

## Viva preparation
- *Why `while`, not `if`, around `wait`?* Spurious wake-ups and another thread may change the state before we reacquire the lock.
- *What does `wait` do atomically?* Releases the mutex and blocks; reacquires the mutex before returning.
- *Why count waiting writers?* That is what lets readers defer to writers.
- *Why `notify_all` for readers?* Many readers can enter together; `notify_one` would admit them one at a time.
- *Why two condition variables?* Readers and writers wait on different predicates; separate queues avoid waking the wrong group.
- *Is it starvation-free?* No, see above.
- *Why log inside the lock?* Otherwise the logged order could differ from the real order of state changes.
