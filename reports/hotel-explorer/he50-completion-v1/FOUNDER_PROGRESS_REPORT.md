# Hotel Explorer 5.0 — Founder progress report

**Date:** 2026-11-27  
**Canonical Bethesda HPC:** `recLuxvwwxID7U2B8`  
**Active tabs:** 11 (Guest Sentiment `DISABLED_BY_FOUNDER`)

## Gate board (fail-closed — YES only with completed validation)

| Gate | Status | Evidence / blocker |
|---|---|---|
| `GUEST_SENTIMENT_DISABLED_BY_FOUNDER` | **PENDING_MERGE** | PR [#621](https://github.com/joandejarden-create/my-operators-backend/pull/621) implements flag + 11-tab nav; CI in flight; not on `main` yet |
| `ELEVEN_TAB_COMPONENT_PARITY` | **PENDING_MERGE** | Same PR #621 (registry 12 / active 11) |
| `CANONICAL_FIELD_MAPPING_COMPLETE` | **NO** | HE50 field-gap matrix exists (48 rows); mapping/connect not finished for NOT_CONNECTED set |
| `VERIFIED_RESEARCH_PERSISTENCE_COMPLETE` | **NO** | 14 `EXISTING_NOT_CONNECTED` Bethesda rows still need dry-run → authorized apply → read-back |
| `BETHESDA_FULL_LIVE_VALIDATED` | **NO** | Contact CX V2 + GS disable not on Radar 8080 main; rooms/ownership connect incomplete |
| `EIGHT_HOTEL_AIRTABLE_READBACK_VALIDATED` | **NO** | Blocked on Bethesda factory loop + seven-hotel reuse audit |
| `EIGHT_HOTEL_RADAR_BROWSER_VALIDATED` | **NO** | Requires 88 hotel×tab Radar checks on canonical 8080 after merges |
| `FULL_HE_E2E_VALIDATED` | **NO** | Explicitly not auto-promoted |

## Workstream status

### WS1 — Disable Guest Sentiment
- **Code:** PR #621 OPEN — `HE_GUEST_SENTIMENT_ENABLED=false`, nav hide, API `DISABLED_BY_FOUNDER`, tests updated.
- **Blocker:** Integration Controller merge + post-merge home sync.

### WS3 — Contact Intelligence CX V2
- **Code:** PR [#619](https://github.com/joandejarden-create/my-operators-backend/pull/619) OPEN — **CI green** / MERGEABLE.
- Purpose sections, no Recommended First Contact hierarchy, Findymail audit, persistence designed only.
- **Blocker:** IC merge (prefer before or with #621; both needed on main).

### WS2 — Bethesda recovery (next)
Priority queue generated from `HE50-FIELD-GAP-MATRIX.json` → `BETHESDA_GAP_ACTION_QUEUE_V1.json`.

Top **CONNECT_PERSIST_READBACK** (reuse research, no rediscovery):
- Rooms: `suiteInventory`
- Hotel: `totalMeetingSpace`, `yearBuilt`
- Ownership: `legalOwner`, `economicOwner`, `assetManager` (honest pending certification)
- Contacts: `keyPeople`, `publicChannels` (no Surfe persist)
- Organization / relationships / demand / access / sources connect rows
- Sentiment platform rows → **DISABLED_BY_FOUNDER** (skip paid)

Human reconcile: `lastRenovation` SOURCE_CONFLICT.  
Research (free-first): `hotelHeadline`, `hotelDescription`, `starRating`.

### WS4 — Seven hotels
- Canonical HPC map locked in `HE50_CANONICAL_HPC_BY_PILOT_KEY`.
- **Not started** until Bethesda connect loop proves idempotent on main.

### WS5 — Intelligence Factory gates
- Module: `lib/hotel-intelligence/he-explorer-publication-gates-v1.js` (10-step sequence, fail-closed acceptance snapshot).
- Reuses existing HIF engine; does not create a second Explorer.

### WS6 — Full product acceptance (88 checks)
- Denominator = 8 hotels × 11 active tabs = **88**.
- Sentiment not counted as FAIL/missing — `DISABLED_BY_FOUNDER`.
- **Blocked** until #619 + #621 on synchronized Radar 8080.

## Integration / safety

- Dirty HE / GDI / IC worktrees preserved.
- Exceptional WTs: `wt-he-contact-cx-v2`, `wt-he-guest-sentiment-disabled-v1`.
- No production deploy; no unauthorized paid providers; no legacy Hotel Census.

## Ask for Integration Controller

1. Merge **PR #619** (Contact CX V2) — CI green.  
2. Merge **PR #621** (Guest Sentiment disable) after CI green.  
3. Run post-merge sync for HE stream (preserve dirty).  

Then resume Bethesda `CONNECT_PERSIST_READBACK` dry-runs on synchronized main.
