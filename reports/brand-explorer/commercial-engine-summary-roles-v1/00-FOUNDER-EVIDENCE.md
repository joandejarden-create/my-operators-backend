# Founder Evidence — Commercial Engine Summary Roles Fix

**Date:** 2026-11-19  
**Workspace:** `C:\Dev\dealality\ws-brand-explorer`  
**URL:** `http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1`  
**Airtable writes:** **0**  
**Curio / Ascend:** **NOT APPROVED**

---

## Root cause

`kpiCard()` applied `isDiligenceOrProseKpiValue()` to **every** tile, including Commercial Engine Channels / Campaigns / B2B Programs / Owner Lens.

That helper rejects values with **>10 words or >64 characters** as “not a numeric KPI,” then replaces them with:

> N/A — Numeric value unavailable — research required

Saved Curio presentation bodies were intact (e.g. Channels 13 words / 69 chars). The regression was **renderer field-type misclassification**, not missing Airtable content.

Secondary issue: cumulative proposed candidates sometimes carried research-instruction stubs (`KPI lens: request…`). Browser + Node assemblers now treat these four slots as narrative summaries and retain CURRENT when the candidate is research instruction.

---

## Fix

| Layer | Change |
|-------|--------|
| Contract | `configs/brand-explorer-field-content-roles-contract-v1.json` |
| Module | `lib/brand-explorer/be-field-content-roles-v1.js` |
| Renderer | `kpiCard` numeric N/A gate only for `number\|percentage\|amount\|ratio\|currency` |
| Commercial tiles | `commercialKpiFromSlot` uses `short_commercial_summary` / `owner_insight` + canonical labels |
| Assembler (Node + browser) | Retain CURRENT commercial summaries vs research-instruction candidates |
| Gate | `npm run test:be-commercial-engine-summary-roles-v1` |
| Lesson | `BE-QL-COMMERCIAL-SUMMARY-ROLES-001` |

### Field types (permanent)

| Card | Role |
|------|------|
| Channels | Short Commercial Summary |
| Campaigns | Short Commercial Summary |
| B2B Programs | Short Commercial Summary |
| Owner Lens | Short Owner-Facing Commercial Insight |

---

## Browser validation (after)

### Curio — LEFT === RIGHT (retained summaries)

| Card | Value |
|------|-------|
| Channels | Brand.com / Honors app · major OTAs · GDS · metasearch (where active) |
| Campaigns | Hilton Honors and parent portfolio campaigns where the hotel is eligible |
| B2B Programs | Corporate, SME, and RFP programs where active for this flag |
| Owner Lens | Underwrite Curio channel mix with local competitive set reality—not corporate averages alone. |

Screenshot: `screenshots/01-curio-commercial-summaries-after.png`

### Ascend — LEFT === RIGHT

| Card | Value |
|------|-------|
| Channels | Brand.com · major OTAs · GDS · metasearch |
| Campaigns | Choice Privileges and parent portfolio campaigns where the hotel is eligible |
| B2B Programs | Corporate, SME, and RFP programs where active for this flag |
| Owner Lens | owner economics after fees and channel costs |

Screenshot: `screenshots/02-ascend-commercial-summaries-after.png`

DOM checks: `hasNA: false`, all four cards `unavailable: false`, no `KPI lens:` research copy on RIGHT.

---

## Confirmation

- No production Airtable writes  
- No publication / mass population  
- Numeric KPI validation still applies to genuine numeric roles (loyalty members/hotels/markets/mix)  
- Curio and Ascend remain **NOT APPROVED**
