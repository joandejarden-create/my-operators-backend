# GDI Agent Test 001 — Phase 2 Live Research PREPARATION

**Experiment:** `gdi_20opp_live_research_prep_v1`  
**Parent:** PR #411 / `gdi_20opp_adaptive_investigation_experiment_v1`  
**Paid budget:** proposed ≤ `$20` total / ≤ `$4` per case — **NOT authorized for execution**

## A. PR #411 close-out

| Item | Status |
|------|--------|
| CI (gitleaks + GDI/ADP gates) | PASS |
| Merge commit | `b9b91d540f3991c04281561054201089607d75f0` |
| `ws/gdi` restored | HEAD == `origin/main` |
| Production/publication writes | none |
| Unintended GDI workflow edits | none (experiment harness + fixtures/reports/docs only) |

Integration Controller local checkout remains dirty elsewhere → **SKIP_INTEGRATION_DIRTY** for primary; product home synced via FF.

## B. Rubric scoring bias (Phase 1)

The Phase 1 **20/20 positive** deltas were **structural bias**, not live quality lift:

- `meaningfulNewVerifiedFindings` scored +3 whenever `recovery.applied`, including `A_ALREADY_SUPPORTED` local overlays
- `thesisCompleteness` rose from remapping, not new web evidence
- Contact quality lift = **0**; eligibility flips = **1** (a demotion)

Phase 2 rubric awards `newVerifiedFactualFindings` **only** for genuinely new verified retrievals. Recovery overlays are diagnostic only.

## C. Eligibility flip

| Field | Value |
|-------|-------|
| Opportunity | `rb_gdi_disc_dc428c1bd4c4e412` (ISCT / Radisson Blu Toronto) |
| Baseline | `SALES_READY` |
| Agentic | `RESEARCH_CONTINUATION` |
| Cause | Challenger override `blocked_unsupported_or_unverified_buyer` (`verifiedNamedCount=0`) |

This is a **correct demotion**, not an upgrade.

## D. Five selected opportunity IDs

1. `disp_recLuxvwwxID7U2B8_aledade` — incomplete demand evidence (Bethesda)
2. `gdi_lmc_00562bf81b1e1957` — missing verified contact (Wyndham San Diego)
3. `gdi_opp_2027_ddaa_annual_conference_8` — forward-looking demand (Bethesda)
4. `gdi_opp_ajas_hq_rockville_watch` — rejected/unsupported (Bethesda)
5. `gdi_opp_wrcamp_altaroma_2027_altaroma_organizer` — cross-hotel / already placed (W Rome)

Held-out evaluation labels are never passed into investigator inputs.

## E–I. Harness

```bash
# Mock / $0 (default — authorized by this PR)
npm run test:gdi-20opp-live-research-prep-v1
npm run gdi:20opp-live-research-prep-v1

# Live $0 official_page only — requires founder env
set GDI_20OPP_LIVE_PUBLIC_FOUNDER_AUTH=1
npm run gdi:20opp-live-research-prep-v1 -- --mode live_public

# Paid ≤$20 — intentionally refused by this prep harness
# npm run gdi:20opp-live-research-prep-v1 -- --mode paid --authorize-paid-research
```

ORS remains the sole supervisor. No second agent framework. No deploy/publish.
