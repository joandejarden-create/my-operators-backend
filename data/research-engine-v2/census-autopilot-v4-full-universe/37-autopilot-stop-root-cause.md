# V4 Autopilot Stop — Root Cause

## Verdict

**ROOT CAUSE: FOUND**

The previous “ACTIVE / CONTINUOUS” run stopped because the production path was a **one-shot CLI session**, not a self-driving outer controller.

## Exact stop reason

| Item | Detail |
| --- | --- |
| Primary module | `scripts/v4-full-universe-continue-build.mjs` → `async function main()` |
| Secondary module | `scripts/v4-full-universe-next-discovery-wave.mjs` → `async function main()` |
| Pattern | **A. one-shot CLI design** + **B. missing outer while-loop** + **C. queue-drain treated as run completion** + **F. internal wave ceiling** (`SESSION_CAP` / staged list) + **I. process exits after checkpoint** |

### What happened in code

1. `v4-full-universe-continue-build.mjs` drained the freeze staged queue (~325), wrote ledger + next-queue artifacts, optionally inserted next verified-ready, wrote checkpoint `22-checkpoints/full-build-*.json`, then **`main()` returned** → Node process exit 0.
2. There was **no** `while (hasActionableUniverseWork())` outer loop.
3. `v4-full-universe-next-discovery-wave.mjs` used `SESSION_CAP` (default 500) as a **process completion ceiling**, not a batch-then-continue boundary. After one wave it re-listed production, wrote session JSON, and exited.
4. Residual wave (+120) was started only because the **agent manually invoked** another CLI — not because a controller scheduled it.
5. Checkpoint artifacts were treated operationally as “session done / report to Joan,” even though status JSON still said `ACTIVE`.

### Classification vs prompt options

- **A** Yes — one-shot CLI  
- **B** Yes — missing outer while-loop  
- **C** Yes — draining current queue ended the run  
- **D** Partial — Airtable pageSize 100 is paging only (not a stop)  
- **E** Soft — Cursor/agent session ended; not an in-code runtime ceiling in those scripts  
- **F** Yes — `V4_DISCOVERY_INSERT_CAP` / `SESSION_CAP` ended the discovery CLI  
- **G** Partial — first discovery used pilot-5 countries; later full-geo still exited after one wave  
- **H** Yes — discovery queue not auto-promoted into continuous insert loop  
- **I** Yes — checkpoint then process exit  
- **J** No async worker drain  
- **K** No explicit memory safeguard  
- **L** No — SerpApi was 0; not the stop  
- **M** No — sources were available  
- **Authorization** **NO** — standing authorization was active; Joan was not the stop

## What “ACTIVE” incorrectly meant before

`24-full-build-status.json` → `status: "ACTIVE"` meant **policy authorized / manually resumable**, not **operationally self-continuing**.

## Fix

Introduced:

- `lib/research-engine-v2/census-autopilot-v4/full-build-controller.js`
- `scripts/v4-full-build-controller.mjs` (outer loop)
- `scripts/v4-full-build-supervisor.mjs` (auto-resume ticket)
- npm: `census:v4-full-build`, `census:v4-full-build-supervisor`

Checkpoint now means **persistence boundary**; process may hit `INFRASTRUCTURE_RUNTIME_BOUNDARY` and write `50-auto-resume-ticket.json` for supervisor resume **without Joan**.
