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

## Usage

This library is an alternative transport for the `protobuf` gem. Point `protobuf` at it with:

```
PB_SERVER_TYPE="protobuf/nats/runner"
PB_CLIENT_TYPE="protobuf/nats/client"
```

## Example

For a more detailed example, see the `warehouse` app in the `examples` directory.

```ruby
# app.rb
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

Start a server, then call it from a client:

```
$ export PB_SERVER_TYPE="protobuf/nats/runner"
$ export PB_CLIENT_TYPE="protobuf/nats/client"
$ bundle exec rpc_server start ./app.rb

$ bundle exec irb -r ./app
irb> UserService.client.create(User.new(:username => "testing 123"))
=> #<User id=123 username="testing 123">
```

An rpc without a matching instance method (e.g. an unimplemented `search`) is simply not subscribed to.

## Configuring

### Environment variables

Numeric variables are parsed strictly: a malformed value (e.g. `PB_NATS_CLIENT_ACK_TIMEOUT=5s`) is logged and the
default is used, instead of silently becoming `0`.

#### Client

| Variable | Default | Description |
| --- | --- | --- |
| `PB_NATS_CLIENT_ACK_TIMEOUT` | `5` | Seconds to wait for the server's ACK. |
| `PB_NATS_CLIENT_RESPONSE_TIMEOUT` | `60` | Seconds to wait for the RPC response. |
| `PB_NATS_CLIENT_MAX_RETRIES` | `3` | Attempts for ACK timeouts and transient transport errors. Retries re-send the request — see [Delivery semantics](#delivery-semantics-at-least-once). |
| `PB_NATS_CLIENT_NACK_BACKOFF_INTERVALS` | `0,1,3,5,10` | Milliseconds to wait between NACK retries (one attempt per interval). |
| `PB_NATS_CLIENT_NACK_BACKOFF_SPLAY_LIMIT` | `10` | Random jitter (ms) added to each NACK backoff. |
| `PB_NATS_CLIENT_RECONNECT_DELAY` | ACK timeout | Seconds to sleep before retrying after a transient transport error — see [Resilience](#resilience). |
| `PB_NATS_CLIENT_RECONNECT_DELAY_SPLAY_LIMIT` | `1000` | Random jitter (ms, `0..limit`) added to the reconnect delay so a fleet doesn't retry in lockstep. `0` disables. |
| `PB_NATS_RESPONSE_MUXER_DISPATCHERS` | CPUs on JRuby, `1` on CRuby | Threads draining the shared response subscription (min 1). |

#### Server

| Variable | Default | Description |
| --- | --- | --- |
| `PB_NATS_SERVER_MAX_QUEUE_SIZE` | thread count | Queue in front of the handler thread pool; requests beyond it are NACKed. |
| `PB_NATS_SERVER_SUBSCRIPTION_HANDLERS` | CPUs on JRuby, `1` on CRuby | Threads draining the shared intake queue and publishing ACK/NACKs (min 1). Consumer parallelism only — does not change queue-group delivery. |
| `PB_NATS_SERVER_INTAKE_QUEUE_SIZE` | `65536` | Capacity of the shared intake queue. Smaller turns overload into prompt drops-and-retries instead of a deep stale backlog; tune down alongside `PB_NATS_SERVER_STALE_REQUEST_MS`. |
| `PB_NATS_SERVER_SUBSCRIPTIONS_PER_RPC_ENDPOINT` | `10` | Subscriptions created per endpoint (lets JVM servers warm up gradually). Queue groups still deliver each request to exactly one consumer. |
| `PB_NATS_SERVER_SLOW_START_DELAY` | `10` | Seconds between slow-start subscription rounds. |
| `PB_NATS_SERVER_PAUSE_FILE_PATH` | `nil` | While this file exists the server unsubscribes from all services; it resubscribes (with slow start) when the file is removed. |
| `PB_NATS_SERVER_SLOW_HANDLER_THRESHOLD_MS` | `0` (off) | Emit `server.slow_handler` when a handler runs longer than this. Informational only. |
| `PB_NATS_SERVER_HANDLER_OVERDUE_MS` | `65000` | Age at which a still-running handler is reported overdue (its client has already given up). Keep ≈ your clients' response timeout plus a small grace. |
| `PB_NATS_SERVER_RECLAIM_OVERDUE_HANDLERS` | `false` | `"true"` reclaims an overdue handler's pool slot by aborting it — see note below. |
| `PB_NATS_SERVER_STALE_REQUEST_MS` | `0` (off) | Drop requests older than this at intake (the client has already retried or given up) — see note below. |
| `PB_NATS_SERVER_SHUTDOWN_DRAIN_TIMEOUT` | overdue ms / 1000 + 5 | Seconds to let in-flight handlers finish during shutdown before abandoning them. |

#### Shared

| Variable | Default | Description |
| --- | --- | --- |
| `PB_NATS_CONNECTION_NAME` | hostname | Connection name shown in NATS server monitoring. Precedence: env var > yaml `connection_name` > hostname. |
| `PROTOBUF_NATS_CONFIG_PATH` | `config/protobuf_nats.yml` | Path to the yaml config. |

**Overdue handlers are never aborted by default** — killing a thread mid-handler can corrupt state. Enable
`PB_NATS_SERVER_RECLAIM_OVERDUE_HANDLERS` only when orphaned work (whose clients already gave up) is saturating the
pool and healthy traffic is being NACKed.

**Stale-request shedding trusts client clocks.** The request age comes from the UUIDv7 reply token, which encodes the
*client's* wall-clock time — enable only with sane NTP across hosts, and keep the threshold comfortably above the
client ACK timeout. Requests without a UUIDv7 token (foreign clients) are never shed.

### YAML config

Connection-level settings live in a yaml file, keyed by environment (`RAILS_ENV` / `RACK_ENV` / `APP_ENV`, default
`development`). The default path is `config/protobuf_nats.yml`; override with `PROTOBUF_NATS_CONFIG_PATH`.

```yaml
# config/protobuf_nats.yml
---
  production:
    servers:
      - "nats://127.0.0.1:4222"
      - "nats://127.0.0.1:4223"
      - "nats://127.0.0.1:4224"
    max_reconnect_attempts: 500 # -1 reconnects forever
    reconnect_time_wait: 2      # seconds between reconnect attempts (nats-pure default: 2)
    ping_interval: 120          # seconds between health-check PINGs (nats-pure default: 120)
    max_outstanding_pings: 2    # missed PINGs before the connection is declared dead (nats-pure default: 2)
    connection_name: "my-service"
    connect_timeout: 2
    uses_tls: true
    tls_client_cert: "/path/to/client-cert.pem"
    tls_client_key: "/path/to/client-key.pem"
    tls_ca_cert: "/path/to/ca.pem"
    server_subscription_key_only_subscribe_to_when_includes_any_of:
      - "search"
      - "create"
    server_subscription_key_do_not_subscribe_to_when_includes_any_of:
      - "old_search"
      - "old_create"
    subscription_key_replacements:
      - "original_service": "replacement_service"
```

`subscription_key_replacements` is useful for local testing but subject to breaking changes.

### TLS

With `uses_tls`, the client negotiates TLS 1.2–1.3 and **verifies the NATS server's certificate chain**
(`VERIFY_PEER`): certificates must chain to `tls_ca_cert` when set, or to the system trust store otherwise. If you are
upgrading from a release that did not verify (`< 0.13.1`), make sure `tls_ca_cert` points at the CA that signed your
NATS server certificate, or the connection will be rejected. Hostname (SAN/CN) verification is not yet enabled.

## How it works

`protobuf-nats` uses `nats-pure`'s `NATS::IO::Client` on both CRuby and JRuby.

- **ResponseMuxer** (client) — one wildcard subscription multiplexes all RPC responses instead of subscribing per
  request. Dispatcher threads route each reply to its waiting caller by UUIDv7 token, lock-free on the hot path, and
  self-heal with decaying exponential backoff if they crash.
- **SuperSubscriptionManager** (server) — manages the endpoint subscriptions (NATS queue groups: one consumer per
  request) including slow start and pause/resume. All subscriptions feed one shared intake queue drained by handler
  threads, so a slow ACK publish can't head-of-line block other subjects. Handlers self-heal like the muxer.
- **Observability** — thread-pool gauges plus in-flight handler metrics (`server.inflight_count`,
  `server.inflight_oldest_age_ms`, `server.overdue_handler_count`, `server.pending_intake_queue_size`,
  `server.thread_pool_saturated`). Error callbacks run on a bounded background executor; drops are counted
  (`Protobuf::Nats.error_callback_drop_count`) and emit `error_callback_dropped`.

## Resilience

The client rides out transient NATS hiccups rather than surfacing them as request failures:

- **Transient transport errors are retried** (`Errors::RETRYABLE_TRANSPORT_ERRORS`): the client sleeps
  `PB_NATS_CLIENT_RECONNECT_DELAY` (plus jitter) and retries up to `PB_NATS_CLIENT_MAX_RETRIES`, rebuilding a
  terminally closed connection — and moving the muxer's subscription onto it — before each retry.
- **A failing NATS node fails over automatically.** `nats-pure` reconnects through the whole `servers` pool and replays
  all subscriptions on the new node. A node that dies *silently* (no FIN/RST) is only detected by the PING health
  check — up to `ping_interval * max_outstanding_pings` (240s at defaults); lower those yaml keys for faster failover.
- **Missing ACKs and NACKs are retried** with their own timeouts/backoff. Every retry re-sends the request — see
  [Delivery semantics](#delivery-semantics-at-least-once).
- **Server-side failures fail the caller fast.** A failed handler publishes a generic RPC error response (details stay
  in server logs) so the client raises immediately instead of waiting out its response timeout.
- **The server exits rather than running deaf.** If its connection is terminally closed (reconnects exhausted), the run
  loop stops — emitting `server.connection_closed` — so a supervisor restarts the process. Use
  `max_reconnect_attempts: -1` to prefer in-process reconnects forever.

## Delivery semantics (at-least-once)

RPC delivery is **at-least-once** and the gem does **not** deduplicate: a client retry can re-send a request the server
already processed, so a single call can run a handler more than once. This is deliberate — dropping work on a transient
blip is usually worse than occasionally repeating it.

Making that safe is the service author's responsibility: key writes on a natural id or idempotency token
(upsert / `find_or_create`), and make external side effects (charges, emails, downstream RPCs) safe to repeat or guard
them with your own dedup. An opt-in per-RPC dedup with a pluggable store may be added later; it will not be the default.

## Future improvements

- Migrate from the `uuid7` gem to native `Random#uuid_v7` once the minimum Ruby version supports it (see `UUIDv7Helper`).

## Benchmarks

Microbenchmarks live in `bench/` and need no NATS server; see `bench/bench.md`. Highlights on JRuby: muxer dispatch
~2.5× faster with the per-message lock removed (`bench/muxer_resilience_bench.rb`), server intake fan-out ~8×
throughput with head-of-line stalls ~505ms → ~2ms (`bench/server_intake_bench.rb`).

```
bundle exec ruby -Ilib bench/server_intake_bench.rb
bundle exec ruby -Ilib bench/muxer_resilience_bench.rb
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can
also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the
version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version,
push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/mxenabled/protobuf-nats.

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
