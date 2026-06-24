## Changelog

### 0.13.1.pre2
Additional edge-case fixes found while reviewing the 0.13.1 changes:

- **ResponseMuxer self-heal could drop to zero dispatchers.** When the sole dispatcher crashed fatally, its self-healing `start` counted the still-alive (but exiting) crashing thread, so it spawned no replacement — leaving zero dispatchers on CRuby (`dispatcher_count == 1`) and the muxer silently delivering no responses. The crashing thread now removes itself from the handler pool before re-topping it up.
- **Client connection is rebuilt after a terminal close.** `@client_nats_connection` was memoized once and never reset, so once nats-pure gave up and fired `on_close` every later request reused a dead client forever. `on_close` now drops the cached connection so the next request rebuilds.
- **Dropped error callbacks are now observable.** The bounded `notify_error_callbacks_async` executor silently discarded callbacks when saturated (exactly during an error flood). Drops now bump `Protobuf::Nats.error_callback_drop_count` and emit `error_callback_dropped`, without formatting/logging on the read thread.
- **Server no longer double-publishes on a response-publish failure.** A transport error while publishing a *successful* response fell into the handler rescue and emitted a second (error) response for the same request. The handler and the success-response publish are now in separate rescue scopes. The handler-failure error response also now sends a generic message ("Internal server error") instead of the raw `error.message`, so internal handler details aren't leaked to clients (the real error is still logged server-side).
- **Opt-in reclaim of overdue handlers.** Handlers are still never aborted by default. `PB_NATS_SERVER_RECLAIM_OVERDUE_HANDLERS=true` lets operators reclaim a pool slot held by an orphaned handler (one that outlived the client's `response_timeout`) by raising `Errors::HandlerOverdue` into it; emits `server.handler_reclaimed`.
- **TLS now verifies the NATS server certificate.** Previously, supplying a prepared `:tls` context made nats-pure skip its own `set_params`, leaving the OpenSSL default `VERIFY_NONE` in force — any certificate was accepted (MITM exposure) — and `tls_ca_cert` was configured but read nowhere. `Config#new_tls_context` now sets `verify_mode = VERIFY_PEER` and trusts the configured `tls_ca_cert` (falling back to the system trust store when none is set). **Breaking for misconfigured deployments:** a server whose certificate does not chain to the trusted CA, which previously connected insecurely, will now be rejected. (Hostname/SAN verification is still not enabled — see known gaps.)

#### Known gaps noted (not changed here)
- **TLS hostname (SAN/CN) is not verified.** Chain verification is now on, but nats-pure only sets the SSLSocket hostname for a context it builds itself, and a single static hostname would be wrong for a multi-server cluster that reconnects across hosts. Plumbing per-connection hostname verification is tracked separately.
- **`TLS1_3_VERSION` is assumed defined.** Fine on the JRuby targets; an old MRI/OpenSSL build without the constant would raise `NameError`.

### 0.13.1
Fixes a production regression and a set of related issues, all of the same class: assumptions left over from the JNats → nats-pure migration in 0.13.0 that became silently wrong.

- **Dropped-connection retries were silently disabled.** Dropping JNats collapsed `Errors::IOException` to the never-raised `MriIOException`, so the client's reconnect/retry `rescue` became dead code. A dropped NATS connection then escaped immediately as an `RPC_ERROR` (surfacing as a 500) instead of being retried. The client now rescues the transport errors `nats-pure` and the socket layer actually raise (`EOFError`, `IOError`, `Errno::ECONNRESET`/`EPIPE`/`ECONNREFUSED`/`ETIMEDOUT`, `NATS::IO::ConnectionClosedError`, and `java.io.IOException` on JRuby) via `Errors::RETRYABLE_TRANSPORT_ERRORS` and rides them out with the existing `reconnect_delay` retry loop.
- **Response muxer `pending_size` drift (could silently drop all responses).** nats-pure increments a subscription's `pending_size` (synchronized) for every inbound message and uses it to enforce the slow-consumer byte limit; for a callback-less subscription it never decrements it, so the muxer would be the sole consumer. Rather than mirror that accounting with a lock on every message, the muxer now **disables the byte-based limit** on its response subscription and relies on the message-count limit (the `SizedQueue` depth, tracked accurately for free). This removes the per-message lock from the dispatch hot path (~**2.7× faster** per message on JRuby — see `bench/muxer_resilience_bench.rb`) and eliminates the drift bug entirely.
- **Dispatcher no longer busy-spins during a restart window.** If `@resp_sub` was briefly `nil` while the muxer restarted, the dispatch loop raised `NoMethodError` every iteration — busy-spinning and emitting a logged error + error-callback per spin. It now parks briefly (~0.2% of the old wasted work, zero errors).
- **Self-healing backoff counter is now thread-safe.** The shared dispatcher crash counter was a plain `Integer` mutated by multiple dispatcher threads (it lost ~45% of updates under true parallelism on JRuby, corrupting the exponential backoff). It is now a `Concurrent::AtomicFixnum` that decays once a dispatcher is healthy.
- **Client connection lifecycle hardening.** Connection callbacks (`on_disconnect`/`on_reconnect`/`on_close`/`on_error`) are now registered before `connect`, so handshake-time events are observed; and a failed handshake closes the half-open client so nats-pure's reader/flusher threads aren't leaked.
- **Removed the dead `:disable_reconnect_buffer` connect option.** nats-pure has no such option (it was a JNats concept), so it was silently ignored. Transient disconnects are now handled by the client's transport-error retry path and `ack_timeout`.
- **Server no longer leaves clients hanging on handler/publish failure.** If processing a request fails after the ACK was sent, the server now publishes an encoded `RPC_ERROR` response so the client fails fast instead of blocking until `response_timeout` (60s).
- **Config no longer crashes when the YAML file has no section for the current environment** (or is empty); it falls back to defaults.
- **TLS now floors at 1.2 and ceilings at 1.3** (replacing the deprecated `ssl_version = :TLSv1_2` hard pin), so TLS 1.3 is used when the server supports it and a TLS-1.2-only transport still negotiates down to 1.2. Verified on JRuby 9.4 and 10.0.
- **Server request intake is now parallelized.** `SuperSubscriptionManager` drained the shared intake queue with a single thread that also published every ACK/NACK, so on JRuby intake was pinned to one core and one slow publish (e.g. nats-pure's buffer during a reconnect) head-of-line blocked *every* subject. Intake now fans out to `PB_NATS_SERVER_SUBSCRIPTION_HANDLERS` threads (default `processor_count` on JRuby, 1 on CRuby) with per-thread self-healing backoff. NATS queue-group semantics and subscription counts are unchanged — each request is still delivered to exactly one consumer. Measured **~8.5× intake throughput** and head-of-line stall **~505ms → ~0.4ms** at 8 handlers (`bench/server_intake_bench.rb`).
- **Client retry is bounded and jittered.** `PB_NATS_CLIENT_MAX_RETRIES` (default 3) and `PB_NATS_CLIENT_RECONNECT_DELAY_SPLAY_LIMIT` (default 1000ms) make retries configurable, and the reconnect sleep now adds random jitter so a fleet hitting the same outage doesn't reconnect in lockstep.
- **More transient errors are retried.** `ConnectionPool::TimeoutError` (subscription-pool exhaustion during a reconnect) is now treated as transient instead of surfacing as an `RPC_ERROR`.
- **`connection_options` only forwards nats-pure-recognized keys** (servers, max_reconnect_attempts, connect_timeout, tls); app-level settings are read via their own accessors and no longer leak into `nats.connect`. YAML config now uses `safe_load`.
- **Thread-pool robustness.** `wait_for_termination` prunes under its mutex and returns a real drained/timed-out result; a new `replenish` (called each server tick) respawns a worker killed by a non-StandardError. On shutdown the drain timeout tracks `handler_overdue_ms` so a legitimate long handler isn't killed mid-flight, and abandoned in-flight handlers are logged/instrumented.
- **Error callbacks run off the read loop.** The nats `on_error` hooks dispatch via a bounded executor (`notify_error_callbacks_async`) so a slow user callback can't stall message processing for every subject.
- **Server handler observability (long operations are first-class).** Handlers are never aborted — long-running operations (up to and beyond a minute) are allowed. The server now tracks in-flight handlers and emits `server.inflight_count`, `server.inflight_oldest_age_ms`, `server.overdue_handler_count`, `server.handler_overdue`, `server.pending_intake_queue_size`, `server.slow_handler` (opt-in via `PB_NATS_SERVER_SLOW_HANDLER_THRESHOLD_MS`), and `server.thread_pool_saturated`. A handler is only flagged "overdue" once it outlives the client's `response_timeout` (`PB_NATS_SERVER_HANDLER_OVERDUE_MS`, default 65s), so normal long ops are not mislabeled. Server duration metrics now use a monotonic clock.

### 0.13.0
This is a large overhaul of the client and server internals.

#### Highlights
- Removed JNats / the forked java-nats client. `nats-pure` is now used on both JRuby and CRuby (it is fast enough for parallel work), so there is a single NATS client implementation (`NATS::IO::Client`).
- Added the `ResponseMuxer`: a single wildcard subscription multiplexes all client responses (similar to the Golang client) instead of one subscription per request. This replaces the previous per-request subscribe/unsubscribe cycle and significantly reduces subscription churn.
- Added the `SuperSubscriptionManager` on the server for managing RPC endpoint subscriptions.
- Switched to `concurrent-ruby` primitives for lock-free response delivery (`Concurrent::Map`) and performance gains.
- Switched request tokens to UUIDv7 (via the `uuid7` gem, see `UUIDv7Helper`) for time-ordered, more robust request correlation.
- Added instrumentation/logging when encountering unexpected messages.
- More robust periodic cleanup, locking, restart handling, and error handling in the client and server.

#### New environment variables
- `PB_NATS_RESPONSE_MUXER_DISPATCHERS` - Number of dispatcher threads draining the shared response subscription. Defaults to `Concurrent.processor_count` on JRuby (true parallelism) and `1` on CRuby (the GVL makes extra dispatchers pointless). Minimum of 1.

#### Dependencies / requirements
- Now requires Ruby `>= 3.1.0`.
- Bumped `nats-pure` to `~> 2` (from `~> 0.3`).
- Bumped `activesupport` to `>= 6.1` (from `>= 3.2`).
- Added `concurrent-ruby` (`~> 1.3.6`, pinned so `logger` is included) and `uuid7` runtime dependencies.
- Pinned `i18n` to `< 1.15.0` in the Gemfile (workaround for ruby-i18n/i18n#735).

