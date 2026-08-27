## Benchmarks

- `bench/concurrency_bench.rb` — end-to-end hot-path throughput (muxer round-trip,
  subscription-key cache, thread pool) across thread counts. No NATS server needed.
- `bench/muxer_resilience_bench.rb` — measures the response-muxer hot-path and
  self-healing fixes (both old/baseline and new/patched behavior in one process):
  - **A. Dispatch hot-path** — per-message `pending_size` accounting that was
    removed; the dispatch step is ~**2.7× faster** per message on JRuby once the
    per-message subscription lock is gone.
  - **B. nil-`@resp_sub` resilience** — during a restart window the old loop
    busy-spun (a `NoMethodError` + logged error/callback every iteration); the new
    loop parks, doing **~0.2%** of the old wasted work and emitting **0** errors.
  - **C. Self-healing crash counter** — a plain Integer mutated by N dispatcher
    threads loses ~**45%** of updates on JRuby (corrupting the exponential backoff);
    the `Concurrent::AtomicFixnum` replacement loses none.

  Run: `bundle exec ruby -Ilib bench/muxer_resilience_bench.rb`
- `bench/server_intake_bench.rb` — server intake fan-out + handler observability
  (old single-handler vs new N-handler intake, in one process):
  - **A. Intake throughput** — with a per-ACK publish cost, N drain threads scale
    intake ~linearly (measured **~8.5×** at 8 handlers on JRuby vs the old single
    intake thread).
  - **B. Head-of-line blocking** — behind one slow (0.5s) publish, 50 quick
    messages finished in **~505ms** with one handler vs **~0.4ms** with N.
  - **C. Observability demo** — with hung handlers the new notifications report
    `inflight_count` / `inflight_oldest_age_ms` / `overdue_handler_count` and fire
    `server.handler_overdue`, where before only `server.message_dropped` was visible.

  Run: `bundle exec ruby -Ilib bench/server_intake_bench.rb`
- `bench/soak.rb` — opt-in soak/chaos test: spawns its own `nats-server`, runs a
  real protobuf-nats server + client in-process under sustained concurrency
  (including deliberately long handlers), bounces the nats-server mid-run, and
  asserts recovery (≥90% success) while reporting the resilience signals. Skips
  if `nats-server` isn't on PATH.

  Run: `SOAK_DURATION=20 SOAK_BOUNCES=3 bundle exec ruby -Ilib bench/soak.rb`

---

## Running benchmarks (warm + reliable)

These numbers are meaningless cold. On JRuby the JVM has to load classes and JIT-compile the hot paths before it reaches steady state, so the first second(s) of any run are far slower than production. Always warm up, repeat, and compare like-for-like.

### 1. Use the production engine

Run on JRuby (what production uses); CRuby numbers differ because the GVL serializes the parallelism these benches exercise.

```
rbenv shell jruby-9.4.14.0   # or your deployed JRuby
ruby -v                       # confirm engine before trusting any number
```

### 2. Benchmarking JRUBY_OPTS

Fix the heap so GC resizing doesn't jitter the run, give the young gen room, and don't block on entropy:

```
export JRUBY_OPTS="-J-Xms4g -J-Xmx4g -J-Xmn1g --disable:did_you_mean -J-Djava.security.egd=file:/dev/./urandom"
```

- Set `-Xms == -Xmx` so the heap never resizes mid-measurement.
- Do **not** use `--dev` for benchmarking — it disables the JIT for fast startup and will understate performance.
- Optional faster warmup (compile sooner): add `-Xjit.threshold=10 -J-XX:CompileThreshold=10`. `-Xjit.threshold=0` forces immediate compilation — useful for profiling, but prefer real warmup for representative steady-state numbers.

### 3. Warm up, then measure

- `muxer_resilience_bench.rb` section A uses **benchmark-ips**, which warms up on its own (warmup then a timed window) — no extra flags needed.
- The loop-driven benches (`concurrency_bench.rb`, and the throughput sections of `server_intake_bench.rb`) measure a fixed window. Give them a real warmup and a longer window:

```
BENCH_WARMUP=5 BENCH_DURATION=10 BENCH_THREADS=1,4,8,16 bundle exec ruby -Ilib bench/concurrency_bench.rb
```

### 4. Repeat and take the median

JVM warmup and machine noise make any single run unreliable. Run each bench **3+ times**, discard the first (cold class-load/JIT), and report the **median**. Keep the machine quiet (close other apps, disable CPU throttling / keep laptops on AC) and run one bench at a time.

### Per-script tuning knobs

| Script | Env knobs (defaults) |
| --- | --- |
| `concurrency_bench.rb` | `BENCH_DURATION` (4), `BENCH_WARMUP` (2), `BENCH_THREADS` (`1,4,8,16`), `BENCH_POOL_WORKERS` (8) |
| `muxer_resilience_bench.rb` | none — benchmark-ips controls warmup/time |
| `server_intake_bench.rb` | `BENCH_HANDLERS` (cores), `BENCH_MSGS` (20000), `BENCH_PUBLISH_LATENCY_US` (50) |
| `soak.rb` | `SOAK_DURATION` (15), `SOAK_THREADS` (12), `SOAK_BOUNCES` (2), `SOAK_NATS_PORT` (4299) |

### Real end-to-end run (optional, needs a NATS server)

`bench/real_client.rb` drives the example app against a live server. Start a local nats-server (with monitoring) first:

```
nats-server -DV -m 8222 -p 4222          # or: /opt/homebrew/opt/nats-server/bin/nats-server ...
bundle exec ruby -Ilib bench/real_client.rb
```

`bench/soak.rb` spawns and bounces its own throwaway nats-server, so it needs only the `nats-server` binary on PATH (it self-skips otherwise).
