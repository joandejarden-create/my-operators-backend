# Hotel Explorer 5.0 — Founder progress report

**Date:** 2026-11-27  
**Canonical Bethesda HPC:** `recLuxvwwxID7U2B8`  
**Active tabs:** 11 (Guest Sentiment `DISABLED_BY_FOUNDER`)  
**Progress PR:** [#622](https://github.com/joandejarden-create/my-operators-backend/pull/622) (stacked on #621)

## Gate board (fail-closed — YES only with completed validation)

| Gate | Status | Evidence / blocker |
|---|---|---|
| `GUEST_SENTIMENT_DISABLED_BY_FOUNDER` | **PENDING_MERGE** | PR [#621](https://github.com/joandejarden-create/my-operators-backend/pull/621) CI **green** / MERGEABLE; not on `main` yet |
| `ELEVEN_TAB_COMPONENT_PARITY` | **PENDING_MERGE** | Same PR #621 (registry 12 / active 11) |
| `CANONICAL_FIELD_MAPPING_COMPLETE` | **NO** | Live reconcile: 4 product fields already on HPC; 10 CONNECT rows remain (ownership/contacts/demand/access/sources) |
| `VERIFIED_RESEARCH_PERSISTENCE_COMPLETE` | **NO** | Bethesda rooms/meeting/year **persisted** on HPC; ownership/contacts connect incomplete |
| `BETHESDA_FULL_LIVE_VALIDATED` | **NO** | HPC→API rooms DTO wired in this PR; Radar 8080 Live tab pass not run |
| `EIGHT_HOTEL_AIRTABLE_READBACK_VALIDATED` | **NO** | Read-only product audit: 8/8 HPC exist; Bethesda richest (16/18 fields); peers thin on meeting/mix |
| `EIGHT_HOTEL_RADAR_BROWSER_VALIDATED` | **NO** | 88 hotel×tab Radar checks blocked until #619+#621 on synchronized 8080 |
| `FULL_HE_E2E_VALIDATED` | **NO** | Explicitly not auto-promoted |

## Workstream status

### WS1 — Disable Guest Sentiment
- **Code:** PR #621 OPEN — CI green. Sentiment gaps classify as `DISABLED_BY_FOUNDER` (not missing tabs).
- **Blocker:** Integration Controller merge + post-merge HE sync.

### WS3 — Contact Intelligence CX V2
- **Code:** PR [#619](https://github.com/joandejarden-create/my-operators-backend/pull/619) OPEN — CI green / MERGEABLE.
- **Blocker:** IC merge.

### WS2 — Bethesda recovery
**Live HPC proof (2026-11-27 read-back):**
- Rooms / Keys = **407**
- Opening Date = **1979-01-01** (year-built path)
- Total Meeting Space = **18,719** sq ft; Meeting Rooms = **27**
- Guestroom mix: Single **171** / Double **236** / Suites **8**
- Largest room: Grand Ballroom **4,592** sq ft

**Code connect this PR:** `he-hpc-guest-rooms-meeting-space-v1` maps HPC → Live ownership API `guestRoomsMeetingSpace` (no staged-JSON dependency for summary metrics).

**Still open (reuse / connect, no invent):** ownership legal/economic/AM, contacts channels, demand/access wiring, sources register, headline/description/star research, renovation SOURCE_CONFLICT.

Artifacts:
- `BETHESDA_LIVE_GAP_RECONCILE_V1.json`
- `BETHESDA_GAP_ACTION_QUEUE_V1.json` (refreshed from live)
- `FACTORY_DRY_RUN_bethesda_marriott.json`

### WS4 — Seven hotels
- `EIGHT_HOTEL_PRODUCT_FIELD_AUDIT_V1.json` — all 8 HPC IDs exist; 8/8 rooms DTO hydratable from at least Rooms/Keys; peers missing meeting/mix until research/persist.
- **No new paid research** this run ($0).

### WS5 — Intelligence Factory gates
- Module + dry-run runner: 10-step sequence for Bethesda (`PASS` / `DRY_RUN_ONLY` / `NOT_RUN` for Live).
- Idempotent; no Airtable writes; no legacy Hotel Census.

### WS6 — Full product acceptance (88 checks)
- Denominator = 8 × 11 = **88**.
- Sentiment = `DISABLED_BY_FOUNDER` (not FAIL).
- **Blocked** on IC merge + authenticated Radar 8080.

## Integration / safety

- Dirty HE / GDI / IC worktrees preserved.
- Exceptional WTs: `wt-he-contact-cx-v2`, `wt-he-guest-sentiment-disabled-v1`.
- No production deploy; paid ceiling untouched ($0 this run).

## Ask for Integration Controller

1. Merge **PR #619** (Contact CX V2) — CI green.  
2. Merge **PR #621** (Guest Sentiment disable) — CI green.  
3. Retarget/merge **PR #622** (HE50 progress + HPC rooms hydrate) onto `main` after #621.  
4. Run post-merge sync for HE stream (preserve dirty).
