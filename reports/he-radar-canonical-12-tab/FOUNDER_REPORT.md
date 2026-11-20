# Opportunity Radar → Canonical 12-Tab Hotel Explorer

**Date:** 2026-11-20  
**Branch:** `feat/he-radar-canonical-12-tab-explorer-v1`  
**PR:** [#482](https://github.com/joandejarden-create/my-operators-backend/pull/482) — OPEN  
**Workspace:** `C:\Dev\dealality\ws-hotel-explorer`  
**Pilot hotel:** Bethesda Marriott `recLuxvwwxID7U2B8`  
**HEAD:** `980cc4b51`

## 1. Exact Opportunity Radar popup implementation discovered

Opportunity Radar (`/app#/opportunity-radar`) embeds:

`/deal-capture-radar-with-ranked-list.html`

Hotel open path:

`hotel selection → HotelExplorer.open() (__DEALALITY_HOTEL_EXPLORER_PRIMARY) → paint()`

- Location tabs (`hotel`, `submarket`, `area`, `demand`, `access`) use `HotelDetailPanel.embed()` (five panels only).
- HI tabs (`rooms`, `ownership`, `contacts`, `organization`, `relationships`, `sentiment`, `sources`) require `HotelIntelligenceWorkspace` (+ ownership / contact / rooms modules).

## 2. Why it diverged

Radar host HTML loaded `hotel-explorer.js` + `hotel-detail-panel.js` but **omitted** the Hotel Intelligence workspace CSS/JS stack that standalone preview already had. Icons could still show 12 tabs while HI modules were absent → empty/reduced content. Stale 8080 checkout also served pre-fix HTML until restart + file sync.

## 3. Code paths consolidated

- `public/deal-capture-radar-with-ranked-list.html` — full canonical HE CSS/JS parity with preview (incl. HI workspace).
- `lib/hotel-intelligence/he-canonical-explorer-assets-v1.js` — shared asset + 12-tab contract.
- `scripts/test-radar-canonical-12-tab-explorer-parity-v1.mjs` — permanent regression gate.
- Wiring tests updated; no second 12-tab implementation inside Radar.

## 4. Twelve-tab parity matrix (Bethesda from Radar)

| # | Tab | Nav | Component | Live data observed |
|---|-----|-----|-----------|--------------------|
| 1 | Hotel Overview | YES | HDP embed | Description, facts, amenities |
| 2 | Guest Rooms & Meeting Space | YES | Rooms module / HIW | Mounted |
| 3 | Ownership Intelligence | YES | HIW + network | Marriott operator; economic owner unverified; no fabricated legal owner |
| 4 | Contact Intelligence | YES | HCI | Katherine Button / Jeff Mullen inferred; Find Email & Phone |
| 5 | Organization & Portfolio | YES | HIW | Mounted |
| 6 | Relationships & Structure | YES | HIW | Mounted |
| 7 | Submarket Snapshot | YES | HDP embed | Pooks Hill / Rockville Pike HPC cohort |
| 8 | Area Hotels | YES | HDP embed | Mayflower / Renaissance / Falls Church |
| 9 | Demand Drivers | YES | HDP embed | Demand Anchors (NIH, hospitals, HQ, …) |
| 10 | Access & Connectivity | YES | HDP embed | Travel Infrastructure Data |
| 11 | Guest Sentiment | YES | HIW mountGuestSentiment | Seven-platform shell / rights-aware |
| 12 | Sources & Evidence | YES | HIW | Mounted |

## 5–7. Restored components / animations / CSS

Canonical preview CSS/JS tokens and versions aligned (`hotel-detail-panel.js?v=72`, `hotel-explorer.js?v=69`, `hotel-intelligence-workspace.js?v=72`, ownership/contact/dossier/research/wave-loader). Modal open/close + tab transitions use the same HE shell — not a separate Radar reduced modal.

## 8. Airtable / API parity

- Identity: Hotel Property Census (`recLuxvwwxID7U2B8`)
- Demand: Demand Anchors
- Access: Travel Infrastructure Data
- Ownership/contacts: HI Organizations + HI Hotel–Org Relationships / Org Contacts
- No legacy Hotel Census runtime SoT on Radar host

## 9. Screenshots

- `reports/he-radar-canonical-12-tab/radar-he-bethesda-overview.png`
- `reports/he-radar-canonical-12-tab/radar-he-bethesda-contacts.png`
- `reports/he-radar-canonical-12-tab/radar-he-bethesda-ownership.png`
- (sentiment capture also under Temp screenshots → copy beside these)

## 10. Eight-hotel regression

> **Correction (HE50 recovery):** The five “null (CREATE READY)” rows below were a **stale completeness-matrix** error. Live HPC read-back confirms all eight IDs exist. See `reports/hotel-explorer/he50-canonical-intelligence-recovery-v1/FOUNDER_REPORT.md`.

| Hotel | HPC | 12-tab open from Radar HE |
|-------|-----|---------------------------|
| Bethesda Marriott | `recLuxvwwxID7U2B8` | PASS |
| JW Marriott Costa Elena | `recq9KWr8UE7z2549` | PASS |
| Secrets Cap Cana | `recd7XAM6tLjzQhaI` | PASS |
| AC Hotel Newtown Square | `recF7DQBPDW5aUDoC` | Recovered identity — re-validate on Radar after 8080 sync |
| Carton House Fairmont | `recYf7HZWmCdWPaSd` | Recovered identity — re-validate on Radar after 8080 sync |
| HGI Newtown Square Radnor | `recgaBAwa24wsl3xF` | Recovered identity — re-validate on Radar after 8080 sync |
| Secrets Papagayo | `recFQ0WvGLbfK5Gpa` | Recovered identity — re-validate on Radar after 8080 sync |
| Secrets St. Lucia | `recwZJuMcx6kA7f7I` | Recovered identity — re-validate on Radar after 8080 sync |

## 11. Legitimate Proposed / rights restrictions

- Economic owner for Bethesda remains unverified (Mullen not promoted to verified legal owner).
- Contact cards labeled inferred / property responsibility not confirmed.
- Guest Sentiment provider rights remain gated where unavailable.
- Hotels without HPC cannot invent Live identity.

## 12. Certifications

```
OPPORTUNITY_RADAR_CANONICAL_EXPLORER = YES
TWELVE_TAB_COMPONENT_PARITY = YES
TWELVE_TAB_DATA_PARITY = YES  (for equivalent Live authorization + available data)
ANIMATION_PARITY = YES
VISUAL_PARITY = YES
OPPORTUNITY_RADAR_BROWSER_E2E = YES
FULL_HE_E2E_VALIDATED = NO  (broader HE50 population / remaining hotels still incomplete)
```

## Gate

```bash
npm run test:radar-canonical-12-tab-explorer-parity-v1
```

Also included in `test:adp-phase1-radar-he-gates`.
