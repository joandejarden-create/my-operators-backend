# GDI 20-Opp Live Research Prep V1

**Version:** 2026-11-19.1  
**Mode:** mock  
**Actual paid spend:** $0

## Parent close-out
- PR #411 merged (parent experiment)
- Scoring bias: Phase 1 20/20 positive deltas were structural scoring bias, not live quality lift.
- Eligibility flip: ISCT (rb_gdi_disc_dc428c1bd4c4e412) was SALES_READY under shared eligibility but the evidence challenger blocked sales-ready because verifiedNamedCount=0 (no verified named buyer). Agentic arm correctly demoted to RESEARCH_CONTINUATION — a quality correction, not an eligibility upgrade.

## Five cases
- **incomplete_demand_evidence** `disp_recLuxvwwxID7U2B8_aledade` @ Bethesda Marriott — Baseline gaps include MISSING_HOTEL_DEMAND_THESIS + SOURCES_INSUFFICIENT — incomplete commercial demand evidence.
- **missing_verified_contact** `gdi_lmc_00562bf81b1e1957` @ Wyndham San Diego Bayside — MISSING_NAMED_RELEVANT_BUYER + verifiedNamed=0 — contact gap is the primary commercial blocker.
- **forward_looking_demand** `gdi_opp_2027_ddaa_annual_conference_8` @ Bethesda Marriott — 2027 DDAA annual conference — forward-looking project/workforce-adjacent demand with thesis gap; potential undiscovered lodging need.
- **rejected_unsupported** `gdi_opp_ajas_hq_rockville_watch` @ Bethesda Marriott — Cohort slot rejected_unsupported (SPECULATIVE_WATCH-class). Tests whether live research correctly keeps or overturns rejection.
- **cross_hotel_booked** `gdi_opp_wrcamp_altaroma_2027_altaroma_organizer` @ W Rome — Already-contracted / organizer-placed Altaroma case — tests duplicate and cross-hotel reuse handling under live research.

## Totals
| Metric | Baseline | Agentic | Diff |
|--------|----------|---------|------|
| Avg Phase-2 rubric | 23.40 | 23.40 | 0.00 |
| Genuinely new verified | — | 0 | — |
| Recovery-only cases | — | 5 | — |
| Actual paid USD | 0 | 0 | 0 |

## Gated paid execution
**NOT_EXECUTED**

```
# Step A — $0 live official_page only (still denies Exa/SerpAPI/PDL/Surfe):
set GDI_20OPP_LIVE_PUBLIC_FOUNDER_AUTH=1
npm run gdi:20opp-live-research-prep-v1 -- --mode live_public

# Step B — PAID ≤$20 total / ≤$4 per case — REQUIRES SEPARATE FOUNDER AUTH (not enabled by this prep PR):
# set GDI_20OPP_LIVE_PAID_BUDGET_FOUNDER_AUTH=<founder-authorization-id>
# npm run gdi:20opp-live-research-prep-v1 -- --mode paid --authorize-paid-research
# (Currently hard-refuses paid execution even if env is set.)
```

## Success criteria
- Actual paid spend ≤ authorized ceiling (default $0 until founder auth)
- ≥1 genuinely new verified factual finding OR correct sustained rejection on rejected slot
- No false SALES_READY without challenger-confirmed demand + verified buyer/entry
- Cross-hotel/contracted case not promoted as exclusive hotel demand
- Per-case spend ≤ $4 and total ≤ $20 when paid auth is eventually granted
- Idempotent job: re-run does not double-spend reservations
- No Airtable / production / customer publication writes
