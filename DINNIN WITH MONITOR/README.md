# Dining Philosophers with a Monitor (C++20)

Four philosopher threads, one monitor, and an HTML replay generated from the real recorded run.

## Build and run
```bash
g++ -std=c++20 -O2 -Wall -Wextra -pthread main.cpp -o dp
./dp                                   # runs, verifies, writes dining_philosophers_monitor.html
```
Open `dining_philosophers_monitor.html` by double-clicking it (no server, CDN or internet needed). Exit code is 1 if any violation was detected.
Optional race check: `g++ -std=c++20 -O1 -g -fsanitize=thread -pthread main.cpp -o dp_tsan && ./dp_tsan`

## Algorithm
- `Monitor` is a class with private state: `state[4]` (THINKING/HUNGRY/EATING), `waiting[4]`, one `std::mutex` and one `std::condition_variable` per philosopher. Every method locks the mutex first, which gives monitor-style mutual exclusion.
- `pickup(i)`: set HUNGRY, check neighbours, then `while (!canEat(i)) cv[i].wait(lk);` and finally set EATING. `canEat(i)` is true when the left and right neighbours are not EATING.
- `putdown(i)`: set THINKING, then `notify_one` on each HUNGRY neighbour that can now eat.
- Eating and thinking (sleeps) happen outside the mutex, so the threads really run concurrently.
- Fork j (between philosopher j and j+1) is derived from the states: owned by j or j+1 if that philosopher is EATING, else available. Philosopher i uses forks `(i+3)%4` and `i`.

**Why neighbours never eat together:** the test "neighbours not eating" and the change to EATING happen in one lock hold. Two neighbours cannot both pass the test, because whichever enters the monitor second sees the first already EATING and waits.

**Spurious wake-ups:** the wait is in a `while` loop, so the predicate is rechecked after every wake-up. A notification only means "check again"; admission is logged separately as `ADMIT`.

**Starvation:** the monitor guarantees safety (no adjacent eating) and no deadlock. It does not guarantee fairness. A hungry philosopher can be overtaken repeatedly if its neighbours keep re-entering EATING at the wrong moments. It is unlikely in this run, because every philosopher makes only 5 meals and sleeps randomly, but it is not ruled out. A fix would be a ticket or queue ordering.

## Trace and animation
Each event (HUNGRY, CHECK, WAIT, WAKE, NOTIFY, ADMIT, DONE, THINK) is logged while the monitor mutex is held, with a sequence number from the logger and a full snapshot of states, cv-waiting flags and fork owners. The HTML embeds that trace as JSON and draws one event at a time from its snapshot. Stepping back, scrubbing and reset therefore redraw the full state; nothing is reversed or invented.
Colours: coral = hungry/waiting, gold = eating and current event (dashed ring), teal = available forks. Controls: Play/Pause, Step back/forward, Reset, speed, progress slider, clickable timeline.

## Testing actually performed (Linux, g++, one machine)
- Compiled with `-Wall -Wextra`: no warnings printed.
- 30 consecutive normal runs: 0 failures, all threads finished, 5 meals each, 0 runtime-invariant violations and 0 violations in the post-run trace check (no adjacent EATING in any snapshot; every ADMIT had both neighbours non-eating).
- 5 runs under ThreadSanitizer: no data-race reports, 0 violations.
- A typical run records about 130 to 150 events. Event counts vary between runs because the scheduling is real.
- The embedded JavaScript passed `node --check` (syntax only). **I did not open the page in a browser or screenshot it**, so open it and check the layout on desktop and mobile yourself.
- I did not observe or test a genuine spurious wake-up. The loop handles them by design.

## Viva questions
1. Why is a `while` loop used around `wait`? (Spurious or stale wake-ups; the condition must be rechecked.)
2. Why one condition variable per philosopher? (Wake only the neighbour that can proceed, no thundering herd.)
3. Why can't two neighbours eat together? (Check and state change are atomic under the mutex.)
4. Is the solution deadlock-free? Starvation-free? (Deadlock-free; not starvation-free.)
5. Does `notify` mean the thread eats immediately? (No; it must reacquire the mutex and recheck.)
6. Why is sleeping done outside the lock? (To allow real concurrency.)

## AI usage disclosure
This project was produced with Claude (Anthropic) from my brief. I should read, run and understand the code before submitting it and follow my institution's AI policy. References: Hoare, "Monitors: an operating system structuring concept" (1974); Silberschatz, Galvin and Gagne, *Operating System Concepts* (Dining Philosophers with monitors); cppreference pages for `std::condition_variable` and `std::mutex`.
