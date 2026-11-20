# Surfe Usage V1.1 — Production reliability addendum

## Durability

| Question | Answer |
|---|---|
| Survives process restart? | Yes — JSON ledger + append-only JSONL under `data/group-demand-intelligence/surfe-usage/` |
| Survives host redeploy with persistent volume? | Yes, when the volume is retained |
| Multi-replica safe without shared volume? | **No** — requires shared persistent volume. Documented in summary `durability.multiReplica` |
| Airtable? | Not selected for V1 (rate/cost/schema overhead; filesystem matches other GDI ledgers) |

Mechanism: exclusive `wx` file lock (same pattern as HE durable budget ledger) + atomic tmp+rename + JSONL event log for restart rebuild.

## Concurrency

- Parallel appends serialize under lock; regression test writes 24 concurrent rows with unique `clientRequestId`s.
- Corrupt primary JSON rebuilds from JSONL.

## Credit certainty

| Class | When |
|---|---|
| `confirmed` | Only when `providerCreditsCharged` is present (Surfe does not expose this on enrich today; future-ready) |
| `estimated` | Success heuristic, or shared-account balance delta (never treated as confirmed) |
| `unknown` | Misses / errors without provider confirmation — **not assumed zero** |

Admin KPIs show confirmed · estimated · unknown separately. Incomplete accounting (`accountingFailureCount > 0`) sets `accountingComplete: false`.

## Idempotency

- Transport retries: dedupe on `clientRequestId` / `enrichmentId`
- Accidental double-click: client + server in-flight guards
- Legitimate later repeat for same person: **counted** (15s identity suppress removed)

## Privacy

Forbidden keys stripped; no emails/phones/raw payloads/API keys in ledger, failures JSONL, or admin API.

## UI

Admin Resources → Surfe Usage (`/app#/admin/surfe-usage`) shows certainty pills, accounting failures table, durability note.
