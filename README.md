Protobuf::Nats
==============

An rpc client and server library built using the `protobuf` gem and the NATS protocol.

## Requirements

- Ruby `>= 3.1.0` (CRuby or JRuby)
- A reachable [NATS](https://nats.io/) server

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'protobuf-nats'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install protobuf-nats

## Configuring

### Environment Variables

You can also use the following environment variables to tune parameters:

`PB_NATS_SERVER_MAX_QUEUE_SIZE` - The size of the queue in front of your thread pool (default: thread count passed to CLI).

`PB_NATS_SERVER_PAUSE_FILE_PATH` - If this file exists, the server will pause by unsubscribing all services. When the
file is removed it will resubscribe and restart slow start (default: `nil`).

`PB_NATS_SERVER_SLOW_START_DELAY` - Seconds to wait before adding another round of subscriptions (default 10).

`PB_NATS_SERVER_SUBSCRIPTIONS_PER_RPC_ENDPOINT` - Number of subscriptions to create for each rpc endpoint. This number is
used to allow JVM based servers to warm-up slowly to prevent jolts in runtime performance across your RPC network
(default: 10). Each subscription joins the NATS queue group for its endpoint, so every request is still delivered to
exactly one consumer — this knob controls subscription/interest count, not duplicate delivery.

`PB_NATS_SERVER_SUBSCRIPTION_HANDLERS` - Number of threads that drain the shared intake queue and publish ACK/NACKs
(see [How it works](#how-it-works)). Defaults to `Concurrent.processor_count` on JRuby and `1` on CRuby. This is the
*consumer* parallelism for messages this server has already received; it does not change how many topics are subscribed
to or the queue-group delivery semantics. Minimum of 1.

`PB_NATS_SERVER_SLOW_HANDLER_THRESHOLD_MS` - If set (> 0), emit `server.slow_handler` when a handler runs longer than this
many milliseconds. Informational/SLA only — handlers are never aborted (default: 0, off).

`PB_NATS_SERVER_HANDLER_OVERDUE_MS` - A handler still running past this many milliseconds is reported as "overdue"
(`server.handler_overdue` + `server.overdue_handler_count`) — i.e. the client has already given up (`response_timeout`)
so the work is orphaned. Defaults above the client's 60s response timeout so legitimate long operations are not flagged
(default: 65000). **This should track your clients' `PB_NATS_CLIENT_RESPONSE_TIMEOUT`** — set it to roughly that value (plus
a small grace). If clients use a longer response timeout, raise this so handlers aren't flagged overdue while a client is
still waiting; if shorter, lower it so orphaned work is surfaced promptly.

`PB_NATS_SERVER_RECLAIM_OVERDUE_HANDLERS` - When `"true"`, actively reclaim the thread-pool slot held by an overdue
handler (one past `PB_NATS_SERVER_HANDLER_OVERDUE_MS`, whose client has already given up) by raising
`Errors::HandlerOverdue` into the worker; emits `server.handler_reclaimed`. **Off by default** — the documented contract
is that handlers are never aborted, since killing a thread mid-handler can corrupt state. Enable only when orphaned work
is saturating the pool and the server is NACKing healthy traffic (default: false).

`PB_NATS_CLIENT_ACK_TIMEOUT` - Seconds to wait for an ACK from the rpc server (default: 5 seconds).

`PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS` - Array of milliseconds to wait between NACK retries (default: "0,1,3,5,10").

`PB_NATS_CLIENT_NACK_BACKOFF_SPLAY_LIMIT` - Milliseconds to add to the NACK backoff timeout to avoid bursting retries
(default: 10 milliseconds).

`PB_NATS_CLIENT_RESPONSE_TIMEOUT` - Seconds to wait for a non-ACK response from the rpc server (default: 60 seconds).

`PB_NATS_CLIENT_RECONNECT_DELAY` - When a request hits a transient transport error (e.g. the NATS connection drops or is reset), the client sleeps this many seconds before retrying to give the connection time to re-establish (default: the ACK timeout). See [Resilience](#resilience).

`PB_NATS_CLIENT_RECONNECT_DELAY_SPLAY_LIMIT` - Random jitter (milliseconds, `0..limit`) added to the reconnect delay so a fleet hitting the same NATS outage does not reconnect in lockstep (default: 1000). Set to 0 to disable jitter.

`PB_NATS_CLIENT_MAX_RETRIES` - Number of attempts for ack-timeouts and transient transport errors before raising (default: 3).

`PB_NATS_CLIENT_SUBSCRIPTION_POOL_SIZE` - If subscription pooling is desired for the request/response cycle then the pool size maximum should be set; the pool is lazy and therefore will only start new subscriptions as necessary (default: 0)

`PB_NATS_RESPONSE_MUXER_DISPATCHERS` - Number of dispatcher threads draining the shared response subscription (see [ResponseMuxer](#how-it-works)). Defaults to `Concurrent.processor_count` on JRuby (true parallelism) and `1` on CRuby (the GVL makes extra dispatchers pointless). Minimum of 1.

`PB_NATS_CONNECTION_NAME` - A friendly name for the NATS connection, surfaced in NATS server monitoring, error
reporting, and debugging. Shared by both the client and server connections. Precedence: this env var > the
`connection_name` yaml key > the hostname (`Socket.gethostname`), so the connection is never nameless (default: hostname).

`PROTOBUF_NATS_CONFIG_PATH` - Custom path to the config yaml (default: "config/protobuf_nats.yml").

### YAML Config

The client and server are configured via environment variables defined in the `nats-pure` gem. However, there are a
few params which cannot be set: `servers`, `uses_tls`, `subscription_key_replacements`, and `connect_timeout`, so those must be defined in a yml file.

The library will automatically look for a file with a relative path of `config/protobuf_nats.yml`, but you may override
this by specifying a different file via the `PROTOBUF_NATS_CONFIG_PATH` env variable.

The `subscription_key_replacements` feature is something we have found useful for local testing, but it is subject to breaking changes.

An example config looks like this:
```
# Stored at config/protobuf_nats.yml
---
  production:
    servers:
      - "nats://127.0.0.1:4222"
      - "nats://127.0.0.1:4223"
      - "nats://127.0.0.1:4224"
    max_reconnect_attempts: 500
    connection_name: "my-service"
    uses_tls: true
    tls_client_cert: "/path/to/client-cert.pem"
    tls_client_key: "/path/to/client-key.pem"
    tls_ca_cert: "/path/to/ca.pem"
    connect_timeout: 2
    server_subscription_key_only_subscribe_to_when_includes_any_of:
      - "search"
      - "create"
    server_subscription_key_do_not_subscribe_to_when_includes_any_of:
      - "old_search"
      - "old_create"
    subscription_key_replacements:
      - "original_service": "replacement_service"
```

When `uses_tls` is set, the client negotiates TLS with a floor of 1.2 and a ceiling of 1.3: it uses TLS 1.3 where the NATS server supports it and falls back to 1.2 otherwise (verified on JRuby 9.4 and 10.0).

The client **verifies the NATS server's certificate chain** (`verify_mode = VERIFY_PEER`). When `tls_ca_cert` is set, only certificates chaining to that CA are trusted (the private-CA case); otherwise the system trust store is used. **Note:** a server whose certificate does not chain to the configured CA will be rejected — if you are upgrading from a release that did not verify (`< 0.13.1`), make sure `tls_ca_cert` points at the CA that signed your NATS server certificate. Hostname (SAN/CN) verification is not yet enabled (chain verification still ensures the certificate is signed by your trusted CA).

## Usage

This library is designed to be an alternative transport implementation used by the `protobuf` gem. In order to make
`protobuf` use this library, you need to set the following env variable:

```
PB_SERVER_TYPE="protobuf/nats/runner"
PB_CLIENT_TYPE="protobuf/nats/client"
```

## Example

NOTE: For a more detailed example, look at the `warehouse` app in the `examples` directory of this project.

Here's a tl;dr example. You might have a protobuf definition and implementation like this:

```ruby
require "protobuf/nats"

class User < ::Protobuf::Message
  optional :int64, :id, 1
  optional :string, :username, 2
end

class UserService < ::Protobuf::Rpc::Service
  rpc :create, User, User

  def create
    respond_with User.new(:id => 123, :username => request.username)
  end
end
```

Let's assume we saved this in a file called `app.rb`

We can now start an rpc server using the protobuf-nats runner and client:

```
$ export PB_SERVER_TYPE="protobuf/nats/runner"
$ export PB_CLIENT_TYPE="protobuf/nats/client"
$ bundle exec rpc_server start ./app.rb
...
I, [2017-03-24T12:16:02.539930 #12512]  INFO -- : Creating subscriptions:
I, [2017-03-24T12:16:02.543927 #12512]  INFO -- :   - rpc.user_service.create
...
```

And we can start a client and begin communicating:

```
$ export PB_SERVER_TYPE="protobuf/nats/runner"
$ export PB_CLIENT_TYPE="protobuf/nats/client"
$ bundle exec irb -r ./app
irb(main):001:0> UserService.client.create(User.new(:username => "testing 123"))
=> #<User id=123 username="testing 123">
```

And we can see the message was sent to the server and the server replied with a user which now has an `id`.

If we were to add another service endpoint called `search` to the `UserService` but fail to define an instance method
`search`, then `protobuf-nats` will not subscribe to that route.

## How it works

`protobuf-nats` uses a single NATS client implementation (`NATS::IO::Client` from `nats-pure`) on both CRuby and JRuby.

- **ResponseMuxer** (`lib/protobuf/nats/response_muxer.rb`) — the client uses a single wildcard subscription to multiplex
  all RPC responses (similar to the Golang NATS client) instead of subscribing/unsubscribing per request. One or more
  dispatcher threads drain the shared subscription and route each reply to the waiting caller via a `Concurrent::Map`,
  keyed by a UUIDv7 request token. Tune the dispatcher count with `PB_NATS_RESPONSE_MUXER_DISPATCHERS`. Slow-consumer
  protection on the response subscription is by message count (the queue depth); the dispatch hot path does no per-message
  locking. Dispatcher threads self-heal: a crashed dispatcher is restarted with exponential backoff (capped at 60s) that
  decays once healthy.
- **SuperSubscriptionManager** (`lib/protobuf/nats/super_subscription_manager.rb`) — the server manages the lifecycle of
  RPC endpoint subscriptions (NATS queue groups, so each request is delivered to one consumer), including slow start,
  pausing, and resubscription. All subscriptions feed one shared intake queue drained by `PB_NATS_SERVER_SUBSCRIPTION_HANDLERS`
  handler threads, so a slow ACK publish on one message can't head-of-line block every other subject. Handler threads
  self-heal with exponential backoff.
- **Server observability** — beyond the thread-pool gauges, the server emits in-flight handler metrics
  (`server.inflight_count`, `server.inflight_oldest_age_ms`, `server.overdue_handler_count`, `server.handler_overdue`,
  `server.pending_intake_queue_size`, `server.thread_pool_saturated`). Long-running handlers are allowed and never aborted
  by default; a handler is only "overdue" once it outlives the client's `response_timeout` (see
  `PB_NATS_SERVER_HANDLER_OVERDUE_MS`). Overdue handlers can optionally be reclaimed (emitting `server.handler_reclaimed`)
  via `PB_NATS_SERVER_RECLAIM_OVERDUE_HANDLERS`.
- **Error-callback observability** — `on_error` callbacks run on a bounded background executor so a slow callback can't
  stall message processing. If that executor saturates under an error flood, dropped callbacks are counted
  (`Protobuf::Nats.error_callback_drop_count`) and emit `error_callback_dropped` rather than being lost silently.

## Resilience

The client is built to ride out transient NATS hiccups rather than surface them as request failures:

- **Transient transport errors are retried.** If a request hits a dropped/reset/closed connection (`EOFError`,
  `IOError`, `Errno::ECONNRESET`/`EPIPE`/`ECONNREFUSED`/`ETIMEDOUT`, `NATS::IO::ConnectionClosedError`, or a Java
  `IOException` on JRuby — see `Errors::RETRYABLE_TRANSPORT_ERRORS`), the client sleeps `PB_NATS_CLIENT_RECONNECT_DELAY`
  and retries (up to 3 attempts) while `nats-pure` re-establishes the connection in the background.
- **Missing ACKs and NACKs are retried** with their own timeouts/backoff (`PB_NATS_CLIENT_ACK_TIMEOUT`,
  `PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS`).
- **Server-side failures fail the caller fast.** If the server cannot process a request after it has ACKed, it publishes
  an encoded RPC error response (a generic message; the real error is logged server-side) so the client raises immediately
  instead of blocking until `PB_NATS_CLIENT_RESPONSE_TIMEOUT`. A transport failure while publishing a successful response
  no longer triggers a duplicate error response.
- **The client connection self-heals after a terminal close.** A permanently closed NATS connection is dropped and
  rebuilt on the next request instead of being reused.
- **The response dispatcher self-heals.** A crashed muxer dispatcher restarts with exponential backoff, and a brief
  subscription-restart window won't busy-spin the dispatch loop.

See `bench/muxer_resilience_bench.rb` for microbenchmarks of the dispatch hot path and these resilience paths.

## Delivery semantics (at-least-once)

**Current design choice:** RPC delivery is **at-least-once**, and the gem does **not** deduplicate requests. The resilience features above are the reason: when the client retries on an ACK/response timeout or a transient transport error, the server may have *already received and processed* the original request, so a single client call can run a handler **more than once**. (NATS queue groups guarantee each *delivered* message goes to one consumer, but they do not prevent the client from re-sending after a timeout.)

The gem deliberately favors at-least-once over at-most-once: dropping work on a transient blip is usually worse than occasionally repeating it. Making this safe is therefore the **service author's responsibility** — handlers that have side effects should be written to be idempotent:

- Key writes on a natural/business id or a client-supplied idempotency token (upsert / `find_or_create`) rather than blind inserts.
- Make external side effects (charges, emails, downstream RPCs) safe to repeat, or guard them with your own dedup keyed on a request id you put in the message.
- Naturally idempotent operations (reads, idempotent upserts) need no special handling.

**Why no built-in dedup (yet):** correct dedup across a horizontally-scaled service requires a *shared* store (a retry can land on a different server instance), a tuned TTL, and a cached response to replay on duplicates — and it only helps RPCs that aren't already idempotent. A future, **opt-in per-RPC** dedup with a pluggable store may be added; it will not be the default. Until then, treat handlers as potentially re-run.

## Future Improvements (locked behind ruby version)
- Migrate from the `uuid7` gem to native `Random#uuid_v7` once the minimum Ruby version supports it (see `UUIDv7Helper`).

## Benchmarks

Microbenchmarks live in `bench/` and measure both the old and new behavior in one process (no NATS server required). See `bench/bench.md` for details. Highlights on JRuby:

- `bench/muxer_resilience_bench.rb` — response-muxer dispatch hot path (~2.5× faster per message with the per-message lock removed), restart-window resilience, and crash-counter accuracy.
- `bench/server_intake_bench.rb` — server intake fan-out (~8× throughput, head-of-line stall ~505ms → ~2ms) and the handler-exhaustion observability.

```
bundle exec ruby -Ilib bench/server_intake_bench.rb
bundle exec ruby -Ilib bench/muxer_resilience_bench.rb
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).


## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/mxenabled/protobuf-nats.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
