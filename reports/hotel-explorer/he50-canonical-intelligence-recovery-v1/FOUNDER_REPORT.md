# HE 5.0 Canonical Intelligence Recovery — Founder Report

**Date:** 2026-11-20  
**Branch:** `feat/he50-canonical-intelligence-recovery-v1`  
**Workspace:** `C:\Dev\dealality\ws-hotel-explorer`  
**Stream:** Hotel Explorer  
**Coordinates with:** PR [#482](https://github.com/joandejarden-create/my-operators-backend/pull/482) (Radar canonical 12-tab — OPEN)

---

## 1. Eight-hotel current HPC read-back

Live Airtable base `appCCUsuGsE1ifoLk` / table `tbl9aY5ijiuIzzWam` — **8/8 exist**. Evidence: `EIGHT_HPC_READBACK.json`.

| Hotel | HPC ID | Rooms | Market (after recovery) | Status |
| --- | --- | ---: | --- | --- |
| Bethesda Marriott | `recLuxvwwxID7U2B8` | 407 | Bethesda / Montgomery County | Open |
| AC Hotel Newtown Square | `recF7DQBPDW5aUDoC` | 140 | Newtown Square / Delaware County | Open |
| Carton House Fairmont | `recYf7HZWmCdWPaSd` | 169 | Greater Dublin / Maynooth | Open |
| Hilton Garden Inn Newtown Square | `recgaBAwa24wsl3xF` | 138 | Newtown Square / Delaware County | Open |
| Secrets Papagayo | `recFQ0WvGLbfK5Gpa` | 202 | Guanacaste / Papagayo | Open |
| Secrets St. Lucia | `recwZJuMcx6kA7f7I` | 355 | Castries / Gros Islet | Open |
| Secrets Cap Cana | `recd7XAM6tLjzQhaI` | 457 | Punta Cana | Open |
| JW Marriott Costa Elena | `recq9KWr8UE7z2549` | 415 | Guanacaste / Papagayo | Open |

**No duplicate HPC CREATEs.** Legacy Hotel Census was not accessed.

### Certification

`EIGHT_HPC_IDENTITIES_READBACK_VALIDATED` — **YES**

---

## 2. Why PR #482 classified five hotels as CREATE READY

**Root cause:** stale local research matrix, not missing Airtable records.

1. HE50 controlled apply (standing auth) **already CREATEd** five HPC records and UPDATEd three (see `HE50-STANDING-AUTH-FOUNDER-REPORT-V1.md`).
2. `lib/hotel-intelligence/he-5-0-full-e2e/completeness-matrix-v1.js` still had `hpcId: null` + “CREATE READY” readiness notes for those five.
3. `free-research-findings-v1.js` packets still said `NEW_HPC_CANDIDATE` / `CREATE_AFTER_FOUNDER_APPROVAL`.
4. PR #482 founder report (eight-hotel regression table) consumed that stale matrix — **not** a live HPC read-back.

**Correction in this PR:** matrix, standing allowlist, contract map `HE50_CANONICAL_HPC_BY_PILOT_KEY`, free-research packets, and permanent gate `test:he50-research-to-persistence-gate-v1` all require live canonical IDs and fail if CREATE READY returns.

Also folded PR #482 Radar host wiring (canonical HI CSS/JS) into this branch so Radar and preview share one 12-tab Explorer contract.

---

## 3–6. Research inventory & persistence dispositions

Inventory: `RESEARCH_TO_AIRTABLE_INVENTORY.json` (~200 disposition rows across 12 tabs) + preserve worktree fixtures copied read-only into `fixtures/demand-anchors-*` and `fixtures/travel-infrastructure-*` (preserve dirty state untouched).

| Disposition | Count (inventory) |
| --- | ---: |
| VERIFIED_AND_PERSISTED_AND_RENDERED | 80 |
| INSUFFICIENT_EVIDENCE | 94 |
| EXISTS_IN_CANONICAL_AIRTABLE_NOT_CONNECTED | 14 (schema-level shared gaps) |
| PROPOSED_ONLY | 8 (incl. Mullen ownership) |
| RIGHTS_RESTRICTED | 1 (Guest Sentiment) |
| SOURCE_CONFLICT / GENUINELY_UNAVAILABLE / N/A | 3 |

### Newly persisted this recovery (standing-auth HPC allowlist only)

Geography / brand-family **null-fills** on five existing HPC IDs — dry-run then `--apply`, read-back validated:

Evidence: `HPC_GEOGRAPHY_NULLFILL_APPLY.json`

| Hotel | Fields written |
| --- | --- |
| AC Newtown Square | State/Region, Market, Submarket, Brand Family |
| HGI Newtown Square | State/Region, Market, Submarket, Brand Family |
| Carton House | State/Region, Market, Submarket, Brand Family |
| Secrets Papagayo | State/Region, Market, Submarket, Brand Family |
| Secrets St. Lucia | Market, Submarket, Brand Family |

### Quarantined (not auto-applied)

| Domain | Reason |
| --- | --- |
| Demand Anchors fixture import | Outside standing destination allowlist (HPC + HI Graph Evidence only) |
| Travel Infrastructure fixture import | Same |
| HI Organization Contacts | Requires explicit auth extension |
| Surfe / unverified ownership promotion | Forbidden |
| Guest Sentiment publication | RIGHTS_RESTRICTED |
| Speculative legal/economic owner | PROPOSED_ONLY |

US Demand Drivers tab currently honest-empty in Live (“No Demand Anchors records in Airtable for United States yet”) while Access & Connectivity already returns TI rows for US/IE/CR hotels. Fixture packs are staged for a separately authorized import wave.

### Certification

`RESEARCH_ARTIFACTS_INVENTORIED` — **YES**  
`CANONICAL_FIELD_MAPPING_COMPLETE` — **YES** (mapping + dispositions documented; Full HE domain fill incomplete)  
`VERIFIED_RESEARCH_PERSISTENCE_COMPLETE` — **PARTIAL** (eligible HPC null-fills done; DA/TI/contacts/ownership graph still staged or insufficient)

---

## 7–9. Proposed / rights / coverage

- **Proposed-only:** Mullen portfolio associations; inferred hotel-specific contact responsibilities; unverified financing edges.
- **Rights-restricted:** Guest Sentiment provider publication.
- **Airtable tables used:** Hotel Property Census; HI Graph Evidence (prior); Demand Anchors + Travel Infrastructure Data (read); HI Organizations / Hotel–Org Relationships (Bethesda operator graph partial).
- **Live completeness:** Min HPC identity/rooms/brand/geo now populated for all eight. Full HE tabs vary — Bethesda deepest; others thinner (amenities/ownership/contacts incomplete).

---

## 10–13. API + Live render evidence

- API probe: `EIGHT_HOTEL_API_TAB_PROBE.json` / `EIGHT_HOTEL_LIVE_TAB_SCORES.json` (8093).
- Permanent gates green: `test:he50-research-to-persistence-gate-v1`, `test:radar-canonical-12-tab-explorer-parity-v1`.
- Live Founder Preview (`/hotel-explorer-preview.html?hotelExplorer=<id>&previewMode=live`) opens the **canonical 12-tab** Explorer for **all eight** hotels (overview screenshots under `screenshots/`).

### Opportunity Radar (`/app#/opportunity-radar`)

- **BLOCKED** for unattended browser auth (Memberstack sign-in wall on `/app`).
- Radar **host HTML** on HE workspace port **8093** includes HI workspace assets (parity gate PASS).
- Port **8080** process still serves **stale** Radar HTML without HIW — restart/sync of the 8080 checkout is required before founder URL acceptance on 8080.

### Certifications

`TWELVE_TAB_API_READBACK_VALIDATED` — **YES** (identity + tab APIs for all eight)  
`TWELVE_TAB_LIVE_RENDER_VALIDATED` — **PARTIAL** (founder-preview Live 12-tab overview confirmed for all eight; deep per-tab field QA + Radar `/app` still outstanding)  
`EIGHT_HOTEL_OPPORTUNITY_RADAR_VALIDATED` — **NO** (Memberstack auth wall + 8080 stale host)  
`FULL_HE_E2E_VALIDATED` — **NO**

Airtable persistence classification: **`PERSISTENCE_VALIDATED`** for HPC identity/geography recovery; **not** `FULL_E2E_VALIDATED`.

---

## 14. Screenshots

- `screenshots/he-live-bethesda-overview.png`
- `screenshots/he-live-ac-newtown-overview.png`
- `screenshots/he-live-carton-house-overview.png`
- `screenshots/he-live-hgi-newtown-overview.png`
- `screenshots/he-live-papagayo-overview.png`
- `screenshots/he-live-st-lucia-overview.png`
- `screenshots/he-live-cap-cana-overview.png`
- `screenshots/he-live-jw-costa-elena-overview.png`
- Prior Radar Bethesda captures remain under `reports/he-radar-canonical-12-tab/` (from PR #482 work)

---

## 15. Remaining research gaps

- Authorize Demand Anchors / TI fixture import for US / Ireland / Papagayo markets (or extend standing allowlist).
- Ownership graph beyond Bethesda operator edge.
- Verified public contacts (Org Contacts auth).
- Meeting-space / amenities for non-Bethesda hotels.
- Guest Sentiment licensed sources.
- Authenticated Opportunity Radar pass on synchronized 8080 after Memberstack login.

---

## 16. PR / Integration Controller

- Product chat opens PR only — **does not merge**.
- Preserve worktree `wt-he50-population-preserve-v1` left dirty/unmodified.
- Unrelated GDI untracked reports in HE workspace preserved (not committed).

**Status:** `AWAITING_INTEGRATION_CONTROLLER`
