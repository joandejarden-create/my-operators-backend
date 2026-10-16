# Paid multi-segment continuation — answers A–H

Generated: 2026-10-15T09:15:00Z  
Same $11 authorization (not new). Historical Parallel $2 preserved.

## A. What the continuation added beyond the first paid pass

- **Geo gate fix applied** (`geographic-relevance-gate-v1.js`): `localConnection` in blob; hotel-aware foreign markets; local-office-in-territory accepted. Quorum smoke: `quarantine:false`.
- **Quarantine reprocess (no new Parallel):** 13 prior geo-quarantines → **9 cleared to publishable**, 0 still quarantined. Scaling cleared + published: **Quorum, Virtru** (May), **Shield AI, SOCi** (SD). Also cleared LMC (BADC, Rotary DC, FCBA, Harvard Club DC, WFPG) and PE May clubs (Arts Club, DACOR, Sulgrave, Cosmos → `needs_review`, not auto-published as scaling).
- **Scaling discovery v2 (new Parallel, distinct job segments):** May + SD Pro tasks completed; **11 new scaling accounts published** (5 May / 6 SD). Did **not** resubmit prior `scaling_discovery_*` run_ids.
- **Named buyers:** Unpaid web research found workplace/ops leads; Airtable applied. Surfe **not spent** (usable org email already present).

## B. New versus enriched, by hotel and segment

| Hotel | Segment | Change |
|-------|---------|--------|
| Mayflower | scaling | **New (reprocess):** Quorum, Virtru. **New (v2):** Corgan, Samuel Group Inc., Colliers, CoreWeave, Javelin Advisors LLC. **Enriched contact:** Wisedocs → Manal Abu-Shawar (People & Ops) |
| Mayflower | LMC / PE | Prior pass preserved; LMC geo-cleared rows not re-applied as new discovery in this continuation (scaling-only apply). PE May clubs geo-cleared to `needs_review` only |
| San Diego | scaling | **New (reprocess):** Shield AI, SOCi. **New (v2):** Avantus, MVE + Partners, Hanson Bridgett LLP, Everett Dorey LLP, Fennemore, JKZ LLP. **Enriched contact:** The Shipyard → Traci Hetherington (Office Manager) |
| San Diego | LMC / PE | Prior PE/LMC results preserved; not counted as new discovery |

**Explicitly not counted as new discovery:** Wisedocs, Shipyard, Lane, Meridian, NPC.

## C. What is actually visible on production

| Hotel | Facing before continuation (post first paid pass) | Facing after continuation | Total opps |
|-------|--------------------------------------------------:|--------------------------:|-----------:|
| Mayflower | 28 | **35** (+7) | 57 |
| San Diego Bayside | 38 | **46** (+8) | 77 |

Expected delta ≈ 4 reprocess scaling + 11 v2 scaling = 15 facing adds; observed +15 facing combined (7+8).

## D. Total spend and unused authorization

| Item | USD |
|------|----:|
| Authorized NEW | 11.00 |
| Historical pilot preserved | 2.00 |
| Estimated spent (ledger) | **2.85** |
| Available / unused of $11 | **8.15** |
| This continuation new Parallel | 0.70 (may_v2 0.35 + sd_v2 0.35) |
| This continuation Surfe | **0.00** (skipped: usable channel exists) |
| Prior Surfe AMWA | 0.25 (included in 2.85) |

Do not spend merely to exhaust the cap.

## E. What remains incomplete

- PE May clubs/embassy (Arts Club, DACOR, Sulgrave, Cosmos): geo cleared but qualification incomplete → `needs_review`; not hotel-facing published via this scaling apply path.
- Some v2 scaling rows lack published email (Corgan, Avantus, MVE + Partners, Everett Dorey, JKZ) — form/phone-only routes.
- Named-person **personal** emails for Wisedocs/Shipyard not purchased (Surfe correctly skipped while org inbox usable).
- Provider invoice line-items still not on Task payloads (ledger uses reserve estimates).
- Actual Parallel billed may differ from estimates when invoice arrives.

## F. Manual / bespoke steps still required

- Optional: Surfe personal-email chase for Manal / Traci only if product requires person-level email beyond org inbox (`forceSurfeDespiteExistingEmail`).
- PE May clubs: separate PE persist/reshape if founder wants them facing.
- Share-token QA if client share pages need sealing for newly published accounts.

## G. Recommended weekly research allocation

Unchanged proposal in `WEEKLY_ALLOCATION_PROPOSAL.md` — **not activated**. Prefer unpaid geo-cleared inventory + bounded scaling discovery over PE-heavy spend until scaling yield is saturated.

## H. Blockers / risks

- None blocking continuation delivery.
- Surfe skipped for named buyers by design (`usable_channel_exists_paid_enrichment_not_required`) — not a failure.
- Colliers contact Matt Gannon is a published intermediary (leasing EMD), not verified hotel-booking authority — outreach should request routing to office admin / travel owner.
- Do not resubmit known Parallel run_ids; v2 used distinct segments `scaling_discovery_may_v2` / `scaling_discovery_sd_v2`.
