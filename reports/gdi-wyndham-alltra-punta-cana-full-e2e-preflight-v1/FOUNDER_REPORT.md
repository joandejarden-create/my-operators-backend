# Wyndham Alltra Punta Cana — Full E2E Preflight

**Reference date:** 2026-11-14  
**Canonical hotel ID:** `recXh4NgF8sDbi9cQ`  
**Hotel:** Wyndham Alltra Punta Cana · Dominican Republic · 620-room all-inclusive  
**Forbidden duplicate:** `reccax9nT5J90Sc0Y` (do not use)  
**Rejected substitution:** JW Marriott Monterrey Valle `recsn3BUKJ9PNfeZW` — not in Aimbridge three-hotel cohort

## Verdict

**Status: `PREFLIGHT_COMPLETE_RESEARCH_RECOVERED_AWAITING_AUTHORIZED_FULL_E2E`**

Existing Aimbridge research has been located in worktree `my-operators-backend-aimbridge-three-hotel-e2e` and copied into the primary checkout. **No paid research executed. No CUSTOMER_PUBLISHED. Sheraton staged package unchanged.**

## Recovered counts (verified)

| Hotel | ID | ORS dossiers | Unique candidate IDs | 14 segments | opportunities.json |
|---|---|---:|---:|---:|---|
| Sheraton Guadalajara Expo | `recCAGqkERSXgiku4` | **434** | **680** | 14 | yes (certified separate) |
| **Wyndham Alltra Punta Cana** | `recXh4NgF8sDbi9cQ` | **227** | **553** | **14** | **no** |
| voco Cancún Zona Hotelera (queued) | `recTk5I8VOzsPhxDz` | **381** | **646** | **14** | **no** |

These are research-history counts, not sales-ready opportunities.

## Wyndham quality snapshot

- Explicit dispositions in ORS: **18** (AD 4 · NMR 8 · Watch 5 · Rejected 1)
- Unclassified research dossiers: **209**
- Named contact signals: property group sales **3** · intermediary **1**
- All 14 segments: **PARTIAL / NOT_COMPLETE** (0 completedCandidateIds)
- Customer-visible opportunities today: **0**
- Target for Full E2E: **≥15** credible actionable future/ongoing opportunities (do not fabricate)

## Paid research budget (proposed — not authorized)

- Free historical recovery: **$0**
- Proposed Wyndham continuation ceiling: **$42** (hard max **$48**)
- Sheraton remaining budget: **not transferable**
- Surfe: on-click only
- **Do not initiate paid calls without Wyndham-specific authorization**

## Exact next command

```bash
npm run gdi:full-e2e -- --hotelId recXh4NgF8sDbi9cQ --mode preflight
```

## Queue

1. **Now:** Wyndham Alltra Punta Cana  
2. **Next:** voco Cancún Zona Hotelera `recTk5I8VOzsPhxDz` (research preserved; do not start full run yet)

## Protections

- Sheraton STAGED `gdi_pub_recCAGqkERSXgiku4_20261114_final_certification_v1` fingerprint `d4b2bfc7eb0419b863a561750a748227244bf759edf1307926f89749706e2a33` unchanged
- Bethesda approved publication must remain unchanged
- No live Airtable opportunity upserts without authorization
