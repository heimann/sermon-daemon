# Plan 26 - Daemon OTLP local collector

## Status

In progress. Phases 1-2 landed (config fields + log-record mapping, then the
localhost listen/parse/map/forward loop with socket tests, see "Phases");
everything stays gated behind `receiver_enabled = false`. The hosted receiver
this forwards to is sermon-web plan 23 (`POST /v1/{logs,metrics,traces}`,
Bearer token with scope `otlp_write`). Next is phase 3 (on-host persistence),
which must re-validate roll sizing first (see Risks).

## Decision

Give the daemon a localhost OTLP/HTTP receiver so apps on the host can emit
telemetry directly to it at full fidelity. The daemon parses OTLP/JSON, writes
it on-host (later phase), and forwards a sampled copy to the hosted `/v1/*`
endpoints using an account-scoped `otlp_write` Bearer token.

The minimal first version is deliberately small:

- **OTLP/HTTP, JSON encoding only.** OTLP/protobuf is the wire default for most
  SDKs but the binary decode is a much larger lift in Zig (no protobuf runtime
  in stdlib, and we add no dependencies). JSON-over-HTTP is a first-class OTLP
  transport (`Content-Type: application/json`), every major SDK can be pointed
  at it with one env var, and it parses with `std.json` we already use
  everywhere (`src/agent/push.zig`, `src/agent/main.zig` `loadConfig`).
- **Logs first**, then metrics, then traces. Logs map most directly onto the
  daemon's existing log/ingest representation (`logs.LogEntry` in
  `src/agent/logs.zig`) and onto the forwarder we already have.
- **Single-threaded synchronous accept loop** for v1, run on its own thread
  next to the existing collection loop. Explicitly NOT a connection pool. See
  Architecture for the concurrency tradeoff.
- **`receiver_enabled` defaults to `false`.** Opening a listening socket is a
  new attack surface and a new failure mode for a daemon that today only makes
  outbound calls. Off by default; an operator opts in.

## Why now

sermon-web is concurrently growing the hosted OTLP receiver (plan 23). That
gives us the forward target. The thesis-pure path for Sermon as a virtual SRE
is **on-host, full-fidelity** telemetry: apps emit to a localhost daemon, the
daemon keeps everything locally (queryable over SSH with no hosted dependency,
exactly like the existing `/proc` + journald data) and forwards only a sampled
copy upward. That mirrors the log-rules design already shipped: every log line
is written to the local hot tier at full fidelity, and rules gate hosted upload
only (see README "Log filtering rules"). OTLP is the same shape one tier up.

Standing up the config + mapping now, behind a default-off flag, lets the
hosted side and the daemon land independently without a flag day.

## Scope

### In

- A localhost OTLP/HTTP receiver listening on a configurable port (default
  `4318`, the OTLP/HTTP standard port).
- OTLP/JSON request parsing for **logs** (`POST /v1/logs`). Metrics and traces
  are structurally identical request envelopes and follow in later phases.
- A pure mapping from a decoded OTLP/JSON log record to the daemon's existing
  log representation (`logs.LogEntry`), reusing the push/forward client.
- Forwarding a sampled copy to the hosted `/v1/logs` with the `otlp_write`
  Bearer token.
- Config knobs: `receiver_enabled` (default `false`), `receiver_port` (default
  `4318`), and the hosted `otlp_write` token (`otlp_token`).

### Out (deferred)

- OTLP/protobuf decoding (the SDK wire default). JSON only to start.
- Metrics and traces ingest (envelope parsing + mapping). Logs first.
- On-host persistence of received OTLP into the parquet hot tier. v1 forwards
  only; local-write is a fast follow once mapping is proven (see Storage).
- gRPC OTLP (port 4317). HTTP only.
- Concurrency: multiplexed/threaded connection handling, backpressure, a bounded
  request queue. v1 is single-connection synchronous.
- TLS on the receiver. It binds localhost only; same-host apps reach it in the
  clear, consistent with "localhost daemon" framing.
- Auth on the *receiver* side (a local app proving identity to the daemon). v1
  trusts localhost. A future phase may add a shared local token.

## Architecture

A second loop, not folded into the collection loop. `src/agent/main.zig` runs a
single synchronous collection loop today (`while (running)`); that loop sleeps
`interval` seconds between cycles and must not block on network accept. So the
receiver runs on its own `std.Thread`, spawned at startup only when
`receiver_enabled` is true:

```
main collection loop (existing)        receiver thread (new, opt-in)
  collect /proc + journald               listen 127.0.0.1:receiver_port
  append to staging -> roll to parquet   accept ONE connection
  push sampled copy to /api/ingest       read request, parse OTLP/JSON logs
                                         map -> logs.LogEntry
                                         forward sampled copy -> /v1/logs
                                         respond 200, close, accept next
```

The receiver loop is **single-threaded and synchronous**: accept one
connection, fully service it (read body, parse, map, forward, respond), close,
then accept the next. `std.http.Server` in stdlib is a basic one-connection-at-
a-time server; it is not an async/multiplexed server. v1 embraces that.

**Concurrency tradeoff (called out explicitly):** with a single synchronous
accept loop, a slow or hung client (or a slow forward to the hosted endpoint,
if we forward inline) blocks every other producer on the host from delivering
telemetry until that request completes. For v1 that is acceptable because:

1. clients are same-host apps with an OTLP SDK that batches and retries, so a
   briefly unavailable receiver degrades to "telemetry buffered in the SDK,"
   not lost data;
2. forwarding should be decoupled from accept - the receive path writes locally
   (or to an in-process queue) and a separate step forwards, so a slow hosted
   endpoint never stalls accept;
3. it is dramatically simpler and safer to get correct than a thread-per-conn
   or evented design, and we can measure real concurrency needs before adding
   them.

The honest cost: under genuinely concurrent multi-producer load this serializes
all producers. The mitigation is the deferred "threaded accept / bounded queue"
item in Scope::Out, taken only once a benchmark shows it is needed (per the
"measure, don't guess" rule).

Relevant existing code to reuse / mirror:

- `src/agent/push.zig` - `pushMetrics` already builds a `std.http.Client`,
  POSTs JSON, and reads the response. The OTLP forwarder is the same shape with
  a different URL (`/v1/logs`), a different auth header (`Authorization: Bearer
  <otlp_token>` instead of `x-sermon-ingestion-key`), and an OTLP/JSON body
  instead of the daemon's bespoke payload.
- `src/agent/logs.zig` - `LogEntry` is the canonical on-host log record. The
  receiver maps OTLP `LogRecord` -> `LogEntry` so received logs flow through the
  same local-write and forward machinery as journald logs.
- `src/agent/main.zig` - `loadConfig` / the `Config` struct is where the new
  knobs land, following the existing `?T = null` optional-with-default pattern.

## Storage

v1 does **not** persist received OTLP on-host; it forwards only. This keeps the
first cut to "parse + map + forward" and avoids coupling the receiver to the
parquet hot tier (`src/agent/staging.zig` + `src/agent/roll.zig`) before the
mapping is proven.

On-host persistence is the very next phase and is the thesis-pure payoff. The
daemon already owns a durable append-log -> parquet pipeline: `staging.Staging`
exposes per-table `append*` methods inside a `beginCycle`/`endCycle` bracket
(one fdatasync per cycle), and `roll` rolls staging segments to parquet under an
EX lock. Received OTLP logs map to `logs.LogEntry`, which `staging.appendLogs`
already accepts - so the local-write path is "call `appendLogs` with the mapped
records," gated by the same EX-lock discipline the collection loop uses. The
open design question is how the receiver thread coordinates the staging lock
with the collection loop (see Open questions); the simplest safe answer is to
hand received records to the collection loop via a queue and let the existing
single writer append them, rather than appending from the receiver thread.

Metrics and traces will need their own staging tables (today's tables are
metrics/processes/disks/containers/container_metrics/logs); that schema work is
part of the later metrics/traces phases, not v1.

## Forwarding + auth

The forwarder mirrors `push.zig::pushMetrics` but targets the hosted OTLP
receiver (plan 23):

- **URL:** `<server_url>/v1/logs` (and `/v1/metrics`, `/v1/traces` later),
  trailing-slash-trimmed exactly like `buildIngestUrl`.
- **Auth:** `Authorization: Bearer <otlp_token>`, where `otlp_token` is the
  account-scoped token carrying scope `otlp_write`. This is a *different*
  credential from the existing `api_key` (`x-sermon-ingestion-key`), which gates
  `/api/ingest`. They are not interchangeable; the daemon holds both when both
  paths are enabled.
- **Body:** OTLP/JSON. For v1 the simplest correct forward is to re-serialize
  the mapped records into a minimal OTLP/JSON logs envelope rather than passing
  the raw received bytes through unmodified, so sampling/redaction can apply on
  the way out (consistent with how log rules gate hosted upload today). Passing
  bytes through verbatim is a possible optimization but skips the sampling gate,
  so v1 re-serializes.
- **Sampling:** reuse the spirit of the existing log-rules gate - everything is
  kept on-host (once persistence lands), only a sampled copy is forwarded. v1
  can start with "forward all" and add a sample knob alongside the existing
  `log_rules` mechanism.

A missing or empty `otlp_token` with `receiver_enabled = true` is a startup
warning and disables forwarding (receive-and-drop, or receive-and-store-only
once persistence lands), mirroring how `--server` without `--key` warns and
disables push in `main.zig`.

## Config

New fields on the `Config` struct in `src/agent/main.zig`, following the
existing optional-with-default convention (parsed with
`ignore_unknown_fields = true`, so older/newer config files stay compatible):

```json
{
  "receiver_enabled": false,
  "receiver_port": 4318,
  "otlp_token": "otlp_..."
}
```

- `receiver_enabled` (`?bool`, default `false`) - master switch. When false the
  receiver thread is never spawned and no socket is opened.
- `receiver_port` (`?u16`, default `4318`) - localhost port for the OTLP/HTTP
  listener. Binds `127.0.0.1` only (per the project rule: dev/daemon listeners
  bind localhost/Tailscale, not all interfaces, unless explicitly asked).
- `otlp_token` (`?[]const u8`, default `null`) - the hosted `otlp_write` Bearer
  token. Distinct from `api_key`.

The config loader reads into a fixed 4 KiB buffer (`loadConfig`); these three
small fields fit comfortably. CLI flags can mirror them later (`--otlp-port`,
etc.) but config-only is fine for v1.

## Phases

1. **Scaffold (this change, Part B of the daemon task).** Add `receiver_enabled`
   / `receiver_port` / `otlp_token` to `Config` + loader, with a parse test. Add
   `src/agent/otlp_receiver.zig` with a pure `mapLogRecord` (decoded OTLP/JSON
   log record -> `logs.LogEntry`) and a unit test. The `std.http.Server` listen
   loop is a documented TODO stub gated behind `receiver_enabled = false`. Build
   and `zig build test` stay green; no behavior change for existing installs.
2. **Listen + forward (logs). (LANDED)** Stand up the single-connection
   synchronous `std.http.Server` on `127.0.0.1:receiver_port` on its own
   thread, accept `POST /v1/logs`, parse OTLP/JSON, map via `mapLogRecord`,
   forward to `/v1/logs` with the `otlp_write` Bearer token. Wire into
   `main.zig` startup (spawn thread only when enabled; join on shutdown via the
   existing `running` flag, now atomic since two threads read it). As built:
   1 MiB body cap, 10 s read deadline per connection, one request per
   connection, per-request arena, a `Forwarder` seam so tests fake the hosted
   side, and real-socket tests for the round trip, shutdown join, and the
   400/404/405/413/415 reject paths. Forward failures log a warning but still
   200 the local producer (at-most-once upward; phase 3 persistence is what
   makes a dropped forward recoverable). Missing `server_url`/`otlp_token`
   means receive-and-drop with a startup warning.
3. **On-host persistence (logs).** Hand mapped records to the collection loop
   (queue) so the existing single staging writer appends them to the `logs`
   table; received logs become locally queryable exactly like journald logs.
4. **Metrics.** OTLP/JSON metrics envelope parsing, a staging metrics-OTLP
   table, forward to `/v1/metrics`.
5. **Traces.** OTLP/JSON traces envelope parsing, staging, forward to
   `/v1/traces`.
6. **OTLP/protobuf.** Add binary decode (the SDK wire default) without adding a
   dependency, or revisit the no-deps rule explicitly if a vetted protobuf
   library is warranted.

## Risks

- **`std.http.Server` is basic.** It is a minimal blocking server, not a
  hardened production HTTP stack: one connection at a time, limited
  request-size / header handling, and the stdlib HTTP API has churned across
  Zig releases (this repo is pinned to 0.15.2). We own request limits (max body
  size, header caps, read timeouts) ourselves or risk a trivial local DoS.
  Mitigation: bind localhost only, cap request body bytes (mirror
  `max_log_message_bytes` / `max_rules_file_bytes` ceilings already in the
  codebase), set read deadlines.
- **Concurrency vs the single-threaded design.** As detailed in Architecture, a
  single synchronous accept loop serializes producers and a hung client blocks
  others. Acceptable for v1 (SDKs batch/retry; forwarding is decoupled from
  accept), but it is a real ceiling and the mitigation (threaded accept /
  bounded queue) is explicitly deferred, not free.
- **DuckDB / hot tier under external load (later phases).** Today write volume
  is bounded by the daemon's own collection interval. An OTLP receiver makes
  write volume a function of *external* producers, which can burst far past the
  collector's steady cadence. The parquet roll triggers (`roll_max_bytes` /
  `roll_interval_s`) and the staging append path were sized for the collector's
  cadence; a chatty OTLP producer could roll segments far more often and grow
  the file count. The on-host persistence phase must re-validate roll sizing and
  add receiver-side backpressure / rate limiting before it ships, not after.
- **New listening surface on a previously outbound-only daemon.** Today the
  daemon opens no listening sockets. Adding one is a new attack/abuse surface
  even on localhost (any local user/process can post). Default-off
  (`receiver_enabled = false`) contains the blast radius; a future local-auth
  token is the follow-up.
- **Two credentials to manage.** `api_key` (ingest) and `otlp_token` (OTLP
  forward) are distinct and independently revocable. Operator confusion is a
  real risk; docs and startup warnings must make the distinction explicit.
- **Forwarding inline could stall accept.** If the receiver forwards
  synchronously inside the accept loop, a slow hosted endpoint stalls local
  producers. Mitigation: decouple forward from accept (queue), per Architecture.

## Open questions

- **Staging lock coordination.** Should the receiver thread append to staging
  directly (contending for the EX roll lock with the collection loop), or hand
  records to the single collection-loop writer via a queue? Leaning queue: one
  writer keeps the fdatasync-per-cycle and EX-lock discipline intact and avoids
  a second writer racing the roll. Needs a small design pass in the persistence
  phase.
- **Forward batching.** Forward each received request immediately, or batch on
  the daemon's existing push cadence? Batching amortizes hosted round-trips and
  reuses the sampling gate, but adds latency and a buffer to bound.
- **Sampling semantics for OTLP.** The log-rules engine matches on
  source/identifier/systemd_unit/message. OTLP records carry resource +
  scope attributes instead. Does the existing rules engine extend to OTLP
  attributes, or does OTLP get its own sampling config?
- **Resource/scope attribute mapping.** OTLP `LogRecord` lives under a
  `ResourceLogs` -> `ScopeLogs` envelope with resource attributes (e.g.
  `service.name`). Which of those map onto `LogEntry.identifier` /
  `systemd_unit` / `source`, and which need new columns? v1 mapping picks a
  pragmatic subset (see Phase 1); the full attribute story is open.
- **Protobuf.** When do we add OTLP/protobuf, and does it justify revisiting the
  no-new-dependencies rule, or do we hand-roll the subset of protobuf decode the
  OTLP logs message needs?
