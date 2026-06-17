## Changelog

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

