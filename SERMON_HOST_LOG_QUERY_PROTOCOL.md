# Daemon retained-log query protocol v1

Status: hosted implementation only. The current published daemon, `v0.0.1-rc19`,
does **not** implement this protocol. Reserve **`v0.0.2`** as the minimum release
implementing this contract. A version string alone never enables queries: the
daemon must successfully advertise protocol 1 through the claim endpoint.
`dev`, older releases, and `0.0.2` prereleases do not qualify. This document is
the implementation handoff for `heimann/sermon-daemon`, not a claim that the
daemon changes have shipped.

## Threat boundary and hosted lifecycle

This is one observation-only operation: read filtered rows from the configured
local log store. No command text, SQL, path, environment, arbitrary file access,
network target, inbound port, or generic operation discriminator is accepted.
The daemon must independently enforce this closed contract, even if hosted
Sermon is compromised. Logs and returned text are untrusted evidence, never
instructions.

Hosted requests are created by the `query_host_logs` MCP/iMessage tool under the
authenticated account, never an account ID supplied in arguments. The MCP token
must have `read` scope. Audit provenance includes the token ID and approving
user ID for MCP, or authenticated user ID and agent identity for iMessage.
This expands `read` to include bounded local-store log reads, not host mutation.
An ingestion key may claim and submit results only for its already-bound server.
A stolen server key can falsify that server's evidence; it cannot request a read
or claim/read another server's work. The hosted service cannot independently
verify a daemon's completeness claim.

- One active (`queued` or `claimed`) request per server, enforced with a partial
  unique index and server-row locking. At most ten new requests per server per
  rolling hour; retries of the same request do not consume quota.
- `request_key` is a caller-generated UUID, unique per server. Reusing it with
  different normalized filters or a different authenticated requester is an
  `idempotency_conflict`. Generate a new key for a genuinely new query.
  OAuth and Amp token renewal retain requester identity within the same user
  and approved grant; manual tokens remain issuance-bound. Audit metadata keeps
  the original creating token, never rewrites it to the retrying token.
- A request expires 60 seconds after hosted creation. No lease extension,
  reassignment, or automatic execution on a different server.
- State transitions: `queued → claimed → completed | failed`, and
  `queued | claimed → expired`. Unsupported/unavailable requests are persisted
  directly as `unsupported_daemon_version` or `daemon_unavailable` and never
  later run. Issue a new request key after upgrading/reconnecting.
- No previous capability advertisement means `unsupported_daemon_version`;
  a last valid poll at least five minutes old means `daemon_unavailable`.
- Repeat claim returns the same request ID, claim token, filters, and deadline
  until terminal. Repeat completion cannot overwrite the first terminal result.
- Audit events are inserted transactionally into existing append-only
  `auth_events`: `host_log_query_requested` and `host_log_query_transition`.
  Metadata contains requester, server, redacted filters, timestamps, state,
  failure category, and hosted-computed row/byte counts. It excludes rows,
  messages, claim tokens, and credentials. Auth-event immutability is preserved.
- The hourly retention worker expires abandoned work and deletes request/result
  rows older than 24 hours after their deadline in bounded batches. Physical
  cleanup can lag the next hourly run or backlog; audit metadata remains under
  the existing auth-event retention policy. Retry deduplication lasts while the
  request row exists, not forever. Do not retry old request keys after 24 hours.
- Results are never merged into uploaded `log_entries`, so the two sources
  cannot be confused and do not distort uploaded-log coverage metrics.

## Transport and polling

Both endpoints use **outbound HTTPS POST** from daemon to its configured Sermon
origin. Use the existing ingestion credential:

```http
Content-Type: application/json
x-sermon-ingestion-key: <existing server-bound key>
```

Never put credentials in the URL, payload, logs, or result. Verify TLS normally;
do not follow redirects with this credential. There is no server/account field
in either request body. Unknown fields and wrong types are rejected; do not
coerce strings to numbers. HTTP headers are not echoed.

Poll `/api/daemon/log-queries/claim` every ten seconds with ±20% jitter when
idle. Poll independently of telemetry success; do not stop polling because
ingest failed. The shared ingestion rate limiter currently allows 30 requests
per key per minute **including** telemetry, claims, and completions. Do not
refresh telemetry `last_seen_at` from claim/completion traffic; the hosted side
tracks query-channel availability separately in `log_query_seen_at`. Do not
busy-poll after empty claims. On network errors or 5xx, exponential backoff
starting at two seconds, capped at 60 seconds, with jitter. On 429, honor
`retry_after_seconds` (currently 60). On 401, suspend control requests until
credentials are reloaded/rotated. On 404 claim endpoint or 422 unsupported
version, disable polling for five minutes; keep ordinary telemetry working.

Network work and query execution must not stall collection. Use a bounded
worker (one query at a time), five-second HTTP deadlines, a five-second local
query deadline, and at most 128 MiB query-engine memory. Accept claim response
bodies up to 8 KiB; completion acknowledgments up to 2 KiB. Never execute beyond
the request expiry. Use the earlier of the remaining expiry budget and local
query deadline. Check expiry again before posting; discard expired spooled
results. A clock too far out of sync must fail closed, not extend the deadline.

## Claim: `POST /api/daemon/log-queries/claim`

Request schema (all three keys required, no other keys):

```json
{"protocol_version":1,"daemon_version":"0.0.2","trace_id":false}
```

`protocol_version` is integer constant 1; `daemon_version` is a SemVer string
of at most 64 bytes, at least `0.0.2` (optional leading `v`); `trace_id` is boolean, true only when
the local query layer can actually filter structured trace IDs. A daemon without
trace support may implement v1 with false; a trace-filtered query then fails
`unsupported_filter`, never an unfiltered read. Old records without a trace ID
remain uncorrelatable even after upgrading.

200 response (idle):

```json
{"protocol_version":1,"request":null,"poll_after_seconds":10}
```

200 response (work):

```json
{
  "protocol_version": 1,
  "poll_after_seconds": 10,
  "request": {
    "id": "d8b16132-99db-4c75-b605-927592290403",
    "protocol_version": 1,
    "claim_token": "a5e3191f-a832-489b-8620-cf17a1ee124c",
    "expires_at": "2026-09-20T12:01:00.000000Z",
    "filters": {
      "since": "2026-09-20T10:00:00Z",
      "until": "2026-09-20T11:00:00Z",
      "systemd_unit": "api.service",
      "max_priority": 3,
      "max_rows": 100,
      "max_bytes": 131072
    }
  }
}
```

The `expires_at` value is a real UTC ISO8601 timestamp computed by hosted
Sermon, not a duration. Persist ID, claim token, filters and deadline before
execution. Lost claim responses are recovered by polling again. Do not execute
duplicate IDs concurrently. A completed local result must be durably spooled
before its first POST; replay the spooled result rather than re-running a query
against a newer snapshot. Use private files (0600) under the configured daemon
state root, not any remotely supplied path. If a crash occurs before a result
is durably recorded, re-execution of this read is allowed before expiry.

### Filter schema and local semantics

All keys are optional except `since` and `until`; hosted fills `max_rows` and
`max_bytes` before sending a claim. Optional keys must be absent, not null.
`additionalProperties: false` applies.

| Field | Type / bounds | Semantics |
|---|---|---|
| `since` | UTC ISO8601 string | Inclusive lower timestamp bound |
| `until` | UTC ISO8601 string | Exclusive upper bound; strictly after since; interval ≤24 hours |
| `unit` | nonempty UTF-8, ≤255 bytes, no NUL | Exact legacy `unit` column |
| `identifier` | same | Exact journal `SYSLOG_IDENTIFIER` |
| `systemd_unit` | same | Exact journal `_SYSTEMD_UNIT` |
| `service` | same | Exact match on any of `unit`, `identifier`, `systemd_unit` |
| `max_priority` | integer 0–7 | Keep `priority <= max_priority`; 0 is most severe; omit for all |
| `trace_id` | 32 lowercase hex, not all zeros | Exact structured trace ID; never message substring search |
| `max_rows` | integer 1–200, default 100 | Hard returned row cap |
| `max_bytes` | integer 1024–131072, default 131072 | Hard compact-JSON UTF-8 byte cap for the entire `rows` array |

Every supplied filter is ANDed; only `service` has an internal OR. No wildcard,
regex, glob, substring, pagination SQL, or raw query syntax. SQL metacharacters
in a unit name are literal data. Bind parameters in a fixed query; do not
concatenate unescaped values. Use newest-first timestamp ordering, deterministic
tie ordering by source/unit/identifier/systemd_unit/priority/message/trace ID.
Do not deduplicate equal log rows: repeated exceptions may be meaningful.
All timestamps (filters, rows, coverage) are at most 32 UTF-8 bytes and must
have zero UTC offset. Hosted canonicalizes them to UTC ISO8601 before storage.

Read both Parquet and committed staging using a fresh `ParquetQuery` snapshot
and the existing shared roll lock. Current daemon `queryLogs` lacks upper bound,
row limit and identifier filters and must be extended; do **not** invoke the
generic local SQL CLI as the remote implementation. Sampling/drop upload rules
do not apply to this read. Query only the configured store, never journalctl,
shell, arbitrary files, network sources, or alternate user-selected storage.

## Completion: `POST /api/daemon/log-queries/:id/complete`

The path ID is the claimed request UUID. Body schema is exactly one of:

```json
{
  "protocol_version": 1,
  "claim_token": "a5e3191f-a832-489b-8620-cf17a1ee124c",
  "result": {
    "rows": [
      {
        "timestamp": "2026-09-20T10:42:03.123456Z",
        "source": "systemd",
        "unit": "api",
        "identifier": "api",
        "systemd_unit": "api.service",
        "priority": 3,
        "message": "request failed password=<REDACTED:VALUE>",
        "trace_id": null
      }
    ],
    "truncated": false,
    "coverage": {
      "snapshot_at": "2026-09-20T12:00:10Z",
      "oldest_available_at": "2026-09-13T12:00:00Z",
      "newest_available_at": "2026-09-20T12:00:09Z",
      "complete": false,
      "gaps": ["unknown"]
    }
  }
}
```

or

```json
{"protocol_version":1,"claim_token":"a5e3191f-a832-489b-8620-cf17a1ee124c","error":"store_unavailable"}
```

Every object is closed to unknown fields. Result requires exactly `rows`,
`truncated` (boolean), and `coverage`. Each row requires UTC `timestamp`,
nonempty `source` (≤255 UTF-8 bytes), integer `priority` 0–7, and nonempty
`message` (≤4096 UTF-8 bytes). `unit`, `identifier`, `systemd_unit` are nullable
or absent, otherwise nonempty strings ≤255 bytes; `trace_id` is nullable/absent
or valid 32-character lowercase nonzero hex. No arbitrary attributes/maps or
stack-trace object is returned; stack text belongs in `message` subject to cap.
All strings must be valid UTF-8 and contain no NUL. Returned rows must match
all claimed filters; hosted verifies them before storage.

Coverage requires exactly the five shown keys. `snapshot_at` is UTC ISO8601;
`oldest_available_at` and `newest_available_at` describe the unfiltered store's
available collection-time boundaries (UTC ISO8601 or null when unavailable).
These boundaries alone do not prove complete collection. `complete` is boolean;
`gaps` is an array with at most four values from `retention`, `collection`,
`unknown`. Use `unknown` unless collection continuity for the whole requested
interval can be established. Empty rows with unknown coverage do not mean the
host had no errors. `complete: true` requires no gaps and no truncation.

`truncated: true` means a row, byte, or message cap omitted matching detail.
Fetch enough to detect whether additional matching rows exist; stopping at
exactly max_rows without checking must conservatively report truncated. Redact
**before** byte measurement and serialization, UTF-8 truncate messages to 4096
bytes, and stop before adding a row that would exceed `max_bytes`. Include JSON
array delimiters, commas, keys and escaping in the byte count. Hosted counts
compact JSON itself and rejects over-budget results rather than trimming them.
It applies its own redactor and enforces the byte budget again. If redaction
expands the data past the budget, return a smaller marked-truncated result.

Always apply existing outbound deterministic redaction, regardless of
`redact_local_store` (local rows are raw by default). Do not send raw results
on redaction failure. Hosted additionally redacts free-text row fields with
`SermonWeb.Privacy.Redactor`, preserving typed trace IDs and timestamps. It
rechecks individual text caps and aggregate byte caps after redaction; this is
pattern-based, not a promise that all PII is removed. Error bodies contain only
an enum, never raw exception text.

Allowed daemon error codes:

| Code | Meaning |
|---|---|
| `unsupported_filter` | Required structured field cannot be queried; do not omit it |
| `store_unavailable` | Cannot open/read a consistent local store snapshot |
| `query_timeout` | Local execution deadline reached |
| `redaction_failed` | Cannot safely prepare outbound rows |
| `query_failed` | Other query failure; details stay local |

200 acknowledgment: `{"status":"completed","accepted":true}` or
`{"status":"failed","accepted":true}`. A retry after a terminal state returns
200 with that existing state and `accepted:false`; discard the local spool.
Expired requests also return 200 `{"status":"expired","accepted":false}`.
Never overwrite or append to an earlier completion. A different body after a
lost acknowledgment cannot change the hosted result.

Other HTTP responses:

| Status | Body `error` | Daemon action |
|---|---|---|
| 401 | `invalid_ingestion_key` | Stop until credentials reload |
| 404 | `not_found` | Unknown/deleted/foreign request; discard spool |
| 422 | `unsupported_daemon_version` | Claim version/shape unsupported; back off |
| 422 | `stale_claim` | Active request lacks this claim; do not retry its result |
| 422 | `invalid_result` | Invalid fields, coverage, filters or cap; fix encoding or submit `query_failed` before expiry |
| 429 | `rate_limited`, `retry_after_seconds` | Honor retry interval, never extend expiry |
| 413 | Framework body limit | Do not retry oversized body; bounded result must be rebuilt |
| 5xx / disconnect | No reliable acknowledgment | Retry same spooled completion before expiry |

Use compact completion bodies ≤140 KiB (rows ≤128 KiB plus bounded metadata).
Hosted enforces the compact re-encoded envelope cap before and after redaction.
The endpoint also inherits the hosted JSON parser's global body admission cap;
it is not an exemption from the smaller operation-specific result limits.

## Hosted tool behavior

First use `recent_logs` and `log_facets`: they read uploaded retention and never
enqueue daemon work. `recent_logs` marks `source: "uploaded_logs"`. If more detail
is needed, call `query_host_logs` with `server_id`, fresh `request_key`, `filters`,
and optionally `wait_ms` 0–10000 (default 10000). No fallback happens implicitly.

Tool payload marks `source: "on_demand_daemon_query"`, `request_id`, actual
`request_status`, `daemon_result_received`, `expires_at`, `result`, `row_count`,
`byte_count`, `failure`, and minimum daemon version. `status` is one of
`completed`, `truncated`, `timed_out`, `unsupported_daemon_version`,
`daemon_unavailable`, `expired`, or `failed`.

`timed_out` is a wait outcome, **not** cancellation. Repeat the same tool call
with the same request key and filters to retrieve a late result. Disconnecting
the MCP/iMessage caller does not cancel durable work. Only `completed` or
`truncated` has a daemon result. Report that no daemon log result arrived for
all other outcomes, never “live logs show no errors.” Bounds apply regardless
of severity. A completed empty result is evidence only within reported coverage.
The wait uses nonlocking database snapshots, with each read limited to the
remaining deadline and no pool queueing. A database read failure returns
`timed_out` with `failure: "state_read_unavailable"`. Zero wait returns the
creation/retry snapshot without another database read. The wait budget excludes
request creation and authentication; ordinary database timeouts still apply there.
Tool creation errors include `invalid_arguments`, `invalid_uuid`,
`invalid_filters`, `server_not_found`, `idempotency_conflict`, `query_in_progress`,
and `rate_limited`. No request is created for these errors.

## Required daemon work: protected logs must not disappear into sampling

Current `push.zig` bypasses drop/sample rules for priority ≤3 but still only
uploads 20 rows and abandons failed HTTP uploads. That is not lossless delivery.
Current local rows contain timestamp/source/unit/identifier/systemd_unit/
priority/message/pid, but no trace ID. The follow-up must implement:

1. Preserve error/critical/emergency/alert logs (`priority <= 3`) and structured
   trace-correlated exception records independently of sampling rules **and**
   upload caps. For structured exception detection, use explicit exception
   fields/event names or producer severity, not a claim that arbitrary message
   text can always be recognized. Conservatively protect all structured
   trace-correlated rows if exception classification is uncertain.
2. Durably queue protected uploads before advancing their delivery cursor.
   Send batches of at most 100 to existing `POST /api/ingest`; retain/retry after
   non-2xx or mismatched acknowledgment. Ordinary telemetry may continue.
   Hosted now rejects >100 logs with 422 `log_batch_too_large` plus `max_logs:100`
   before persisting any sample/log from that request, rather than silently
   taking the first 100. Accepted responses include `log_count` and
   `log_rejected_count`; 503 `log_storage_unavailable` is retryable. Only remove
   a protected batch when log_count equals submitted count and rejected count
   is zero. Legacy ingest is at-least-once: retries after a lost acknowledgment
   may duplicate both logs and metric samples. Ingest persists the metric sample
   before logs, so a retryable log-storage failure can also duplicate metrics.
   Do not promise exactly-once upload. Every batch must include the existing
   metrics envelope: `collected_at` (Unix seconds) and `metrics.cpu_percent`
   (a valid numeric sample), plus `logs`. A logs-only body is rejected. Preserve
   the real original metrics envelope on retry; never fabricate a CPU reading.
   Minimal envelope shape: `{"collected_at":1789905600,"metrics":{"cpu_percent":12.5},"logs":[]}`.
3. Persist normalized structured trace IDs locally when available, evolve the
   staging/Parquet readers compatibly (old rows have null trace ID), and include
   optional `trace_id` in uploaded log rows. Hosted preserves valid lowercase
   nonzero 32-hex IDs for ordinary ingest as well as OTLP. Advertise trace query
   support only once the typed local accessor implements it.
4. Bound local storage. Queue exhaustion, collection drops, source gaps,
   oversized-message truncation, and retention eviction must produce explicit
   local diagnostics/counters and conservative query coverage, never be called
   “sampling” or silently ignored. No finite system guarantees unlimited log
   retention; surface loss and keep raw retained evidence where possible.
5. Implement private, durable claim/completion spooling and retry rules above;
   test disconnect before/after claim acknowledgment, before/after completion
   acknowledgment, restart during query, rotation/revocation, stale completion,
   expiry, cap pressure, trace capability mismatch, escaping, and redaction.

The hosted change alone does not satisfy protected-log delivery until that
daemon release is implemented and deployed. Historical sampled-out uploads
cannot be recovered after their local retention has expired.
