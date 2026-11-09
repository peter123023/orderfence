# orderfence

A small harness for testing order-API integrations. Go 1.24, no dependencies. At most one order per client id, however many requests it takes.

It contains an execution kernel (state machine, idempotency, adaptive rate limiting, latency probes, reconciliation) and an in-process venue that can drop acks, return 429s, reject post-only orders and fill resting quotes. Faults are seeded, so a run is repeatable.

It does not place real orders, and there is no exchange adapter in here.

## Why

Most integrations only get tested on the happy path: submit, get an ack, cancel. The expensive bugs are elsewhere.

| Failure | What usually happens | What it costs |
| --- | --- | --- |
| Ack lost after the order was accepted | client retries, venue books a second order | double exposure, then a manual cleanup |
| Rate limit hit mid-batch | half the batch rejected, callers treat it as a hard failure | missing quotes exactly when the market is busy |
| Cancel acknowledged but state overwritten by a stale report | local view says `open`, venue says `filled` | quotes that cannot be cancelled because the client thinks they already are |
| Process restart | in-memory order set is gone, venue still holds the quotes | orders nobody owns |

All four are state problems, not throughput problems. The kernel never invents a state, and anything it cannot prove is marked unknown instead of being assumed successful.

## Architecture

```
             SubmitAll / CancelAll
                     |
                     v
   +----------------- kernel -----------------+
   |  idempotency  ->  rate limiter (AIMD)    |
   |       |                 |                |
   |       v                 v                |
   |  state machine     latency probe         |
   |       |                 |                |
   |       +-------> journal (facts) <--------+
   |                         |
   |                    reconciler
   +-------------------------|----------------+
                             v
                    Venue interface
                 /                  \
        sim.Sim                    your adapter
   (fault injection)          (REST / WebSocket)
```

`Venue` has six methods: `Submit`, `SubmitBatch`, `Cancel`, `CancelBatch`, `Order`, `OpenOrders`. Nothing in the repo knows about a specific exchange; wiring a real API means mapping its fields onto `venue.Order`.

## Usage

```
make build
./bin/orderfence list
./bin/orderfence run                    # all scenarios, print markdown
./bin/orderfence run ackloss             # just one
./bin/orderfence run all -out docs/report.md
```

## Scenarios

| Name | What it exercises |
| --- | --- |
| `burst` | 64 concurrent post-only quotes, then cancel everything; asserts zero fills, zero leftovers, zero orphans |
| `concurrency` | the same 64 orders at concurrency 1 → 64; shows where wall time actually comes from |
| `ratelimit` | 120 orders against a 40 req/s venue; naive client vs. client that treats 429 as a signal |
| `ackloss` | 30% of acks dropped; targeted query first, reconciliation after; asserts nothing was re-sent |
| `postonly` | two groups of quotes, one crossing the touch and one behind it; asserts reject rather than fill |

Each section in `docs/report.md` ends with PASS/FAIL lines, so a regression shows up as a diff. The report is generated locally and not committed.

## Design notes

**Unknown is not failed.** A timeout means the order may or may not exist. The kernel returns `Unknown` and records `submit_unknown`; retrying there is the one bug that creates duplicate exposure. `ErrDuplicate` is different: the venue is telling you it already has the order, so querying it is safe.

**The fence is per process.** `Idem.Claim` keeps a client id from being sent twice while a request is in flight, and the claim outlives a timeout. It is in-memory, so a crash takes the fence with it; after restart reconciliation takes over and the venue's book decides what is still alive.

**The client id is the contract.** Orders are identified by a client-generated id before they leave the process. That is what makes retries idempotent and reconciliation a set comparison rather than a guess.

**Rate limits are feedback.** The limiter halves on 429, grows 5% after a run of successes, clamped between `rate/16` and `rate*4`.

**Reconciliation runs on startup, not on errors.** After a crash the only trustworthy source is the venue's book. `Reconciler` compares local and remote: local-only entries are ghosts, venue-only entries are orphans and get cancelled, quantity mismatches are copied from the venue.

**Latency is measured per phase.** `sent → ack` is the wire round trip, `start → ack` includes queueing and rate-limit waits. One merged number hides which half you are losing.

## Known limitations

- Matching in the simulator is price priority then time priority. No icebergs, no pro-rata, no self-trade prevention.
- The journal only lives in memory. Nothing is persisted yet, which is also why reconciliation is written to assume the local view is wrong.
- `SubmitBatch` / `CancelBatch` exist on the simulator, but the kernel still fans out single requests.
- Timings in `docs/report.md` come from one laptop. Absolute numbers will differ on yours; the PASS/FAIL lines are the part that matters.

## Layout

```
cmd/orderfence          CLI
internal/venue        interface and error taxonomy
internal/kernel       state machine, idempotency, limiter, probe, journal, reconciler
internal/sim          in-memory venue with fault injection
internal/scenarios    the five reproducible scenarios
internal/report       markdown rendering
```

## Tests

```
make test
```

The ones worth reading are in `internal/kernel/executor_test.go`: `TestTimeoutDoesNotResend` checks the venue ends up with exactly as many orders as requests were made, `TestReconcileCancelsOrphans` checks a restarted process cleans up after itself, and `TestCancelTerminalIsNoop` checks a second cancel never reaches the venue.

## License

MIT — see [LICENSE](LICENSE).
