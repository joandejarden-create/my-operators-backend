# Bethesda Marriott — NICE 2027 commercial progression

**Captured:** 2026-09-17  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Opportunity:** 18th Annual NICE Conference and Expo 2027 — location TBD (Jun 7–9)  
**Opportunity ID:** `gdi_opp_nice_2027`

## Original Dealality state

| Field | Value |
|-------|-------|
| Priority | `MEDIUM_PRIORITY` (unchanged) |
| Opportunity type | `PRIMARY_PURSUIT` |
| Venue / sourcing | `RFP_ACTIVE_SOURCING` |
| Validation | none invented — no `WORTH_PURSUING_NOW` written |

## Customer action

Francesca Moore (Senior Sales Executive, Bethesda Marriott) reported by email on **2026-09-17** that the hotel contacted the opportunity and was added to the sourced properties set.

Evidence type: `CUSTOMER_FIRST_PARTY_EMAIL`  
Source actor: Francesca Moore  
Source organization: Bethesda Marriott

## Observed progression

| Layer | Value |
|-------|-------|
| ACTION | `CONTACTED` |
| OUTCOME / PROGRESSION | `ADDED_TO_SOURCED_PROPERTIES` |
| Funnel stage | `ADDED_TO_SOURCING` |
| Customer status label | Added to sourced properties |
| Compact table badge | Added to sourcing |
| Causal claim | **NOT CLAIMED** (`causalConfidence: UNKNOWN`) — “Progression observed after hotel outreach” |

## Canonical Airtable (`appa2cE7FTRmIbB32`)

| Entity | ID |
|--------|----|
| Decision | `dec_mu5kws3w_c0eb082f` |
| ACTION event | `act_e35786f8608e6a32` (`recF2Gp6bRCWPe5xY`) |
| OUTCOME event | `out_365f5e7b8abb4c01` (`rec2O5a24St6GkKAr`) |

Re-apply is idempotent (second `--apply` → `actionCreated: false`, `outcomeCreated: false`).

## 21/29 outreach summary

**CAPTURED** as hotel-level usage observation (not per-opportunity CONTACTED fabrication):

- Path: `data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/customer-usage-observations.json`
- Observation ID: `gdi_usage_bad5aa7805d70379`
- Surfaced: 29 / Contacted: 21
- Source: Francesca Moore email 2026-09-17

## Product changes

- Reusable outcome enum `ADDED_TO_SOURCED_PROPERTIES` + commercial funnel stage `ADDED_TO_SOURCING`
- Customer-facing progression on auth + share opportunity detail
- Compact “Added to sourcing” badge on list/brief tiles (does not replace priority)
- Funnel metrics from canonical events only

## Deployment / production verification

Filled after Railway deploy of this change.

- Deploy commit: _pending_
- Railway deploy: _pending_
- Existing Bethesda client URL: unchanged (must remain working)
- Production verification: _pending_

## Apply artifact

See `bethesda-nice-commercial-progression-apply.json` for machine-readable readback.
