# Bethesda Marriott — Commercial Surface Reconciliation (Founder Acceptance)

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Contract:** `gdi-bethesda-88-commercial-surface-reconciliation-v1`  
**Store SHA (59-row canonical):** `0c9a235bd47e6a19139089ec43ff44c01954b9edb36e8bc93ae1ef9fce036962`  
**Paid research:** none · **Customer publication:** unchanged / not authorized  

## Verdict

Success is **not** “88/88 processed.”  
Success is: known founder-visible defect fixtures fail closed **and** the default GDI commercial list shows only commercially grounded opportunities.

| Gate | Result |
|------|--------|
| Forbidden IDs on primary commercial list | **PASS** (0 leaks) |
| Marriott HQ theme excluded | **PASS** (`MARKET_INTELLIGENCE`) |
| AAO / ISCB / DQ set excluded | **PASS** (`ARCHIVE_DISQUALIFIED`) |
| NCI RNA demoted | **PASS** (`RESEARCH_PIPELINE`) |
| IFE / GBCC family collapse | **PASS** |
| Raw 88 IDs preserved | **PASS** |
| Authenticated browser UI | **AUTHENTICATED BROWSER NOT VERIFIED** |
| Localhost HTTP | **200** (health only — not acceptance) |

## Counts (post-correction primary surface)

| Surface | Count |
|---------|------:|
| True commercial opportunities (default list) | **68** |
| Research pipeline | **1** |
| Market intelligence / research themes | **1** |
| Archive / disqualified (excluded from default) | **11** |
| Family-collapsed list before surface filter | 81 |
| Underlying union IDs | **88** (59 canonical + 29 historical) |

Machine-readable ID diff: `02-displayed-id-diff.json` · verification: `04-post-apply-verification.json`.

## Answers to required founder questions

1. **Why did Corporate meetings near Marriott HQ pass previous Full E2E reviews?**  
   Research integrity treated `CATEGORY_WATCH_NO_SINGLE_BUYER` as honesty-compliant (contact-exempt). Commercial certification never required a named account for list eligibility. Sales-action resolution still projected a pursuit-style pill (`BUILD_ACCOUNT` / Build Account) onto a WATCHLIST theme. Full E2E certified **records**, not **commercial list eligibility**.

2. **Why did disqualified and out-of-market events remain in the visible list?**  
   The Bethesda internal-union API path deliberately bypassed `filterSalespersonView`, so `DISQUALIFIED` / destination-outside rows stayed in the ~83-card family-collapsed payload when Priority = All.

3. **Which other records suffered the same underlying defects?**  
   All `DISQUALIFIED` / destination-outside rows (AAN, ADA, AGB, FBA Qui Tam, NTCA, PTAB, ASA ADVANCE, AAO 2027/2028, ISCB Rotterdam, NIH Research Festival 2026), market-intelligence themes (Marriott HQ), unresolved-buyer research (NCI RNA), and uncollapsed IFE / GBCC duplicate-family cards.

4. **How many true commercial opportunities remain after correction?** **68**

5. **How many research-pipeline records?** **1** (NCI RNA Biology Symposium)

6. **How many market-intelligence themes?** **1** (Marriott HQ corridor)

7. **How many excluded/disqualified?** **11** archive/disqualified on the family list (plus linked duplicates hidden by family collapse before surface classify)

8. **Which records changed surface/disposition, and why?**  
   See `02-displayed-id-diff.json` → `removedFromCommercial` (13 IDs): DQ/out-of-market → `ARCHIVE_DISQUALIFIED`; Marriott HQ → `MARKET_INTELLIGENCE` (no account / no Build Account); NCI RNA → `RESEARCH_PIPELINE` (no verified named lodging buyer). Persisted patches on Marriott + NCI; runtime disposition for the rest.

9. **Do all eligible records have correct contacts and revenue types?**  
   Contact waves preserved (named coverage unchanged at 74 projected). Demand-type / `revenueStreamClass` from prior PR remains on store; commercial list carries class through DTO. Residual org-only / unresolved contacts stay on research or non-Sales-Ready paths — not invented.

10. **Have the specific known founder-visible defects disappeared from the normal commercial list?** **Yes** (`forbiddenStillVisible: []`).

11. **Have all 88 raw IDs and their evidence been preserved?** **Yes** (`rawIdsPreserved: true`). No deletes; archive/research/admin views retain history.

12. **Have all reusable code changes and tests been merged to main?**  
   Delivered via the consolidated commercial-surface PR on this mission (disposition module, API/UI gates, family collapse, adversarial fixtures, store SHA allowlist). Confirm merge status on GitHub after this report.

13. **Has the real application been verified beyond server health?**  
   Engine path + adversarial fixtures + dry-run recon against live store: **verified**.  
   Authenticated Memberstack browser walkthrough: **AUTHENTICATED BROWSER NOT VERIFIED**.  
   HTTP 200 on localhost alone is **not** acceptance.

## Root cause (permanent)

| Layer | Failure | Permanent rule |
|-------|---------|----------------|
| Discovery / research | Themes + DQ events stored as opportunities | OK to store; must stamp disposition |
| Quality / Full E2E | Integrity ≠ commercial list eligibility | `QUALITY_AXIS.COMMERCIAL_SURFACE` |
| API | Bethesda skipped salesperson filter | Default `BETHESDA_PRIMARY_COMMERCIAL` surface filter |
| Sales action | Themes got Build Account | Themes/archive → `MONITOR` only |
| UI | Trusted API “All” payload | Defense-in-depth hide `primaryCommercialEligible===false` |
| Families | IFE/GBCC dual cards | Event-family collapse |

## Admin surfaces (no data loss)

- Default auth list: commercial only  
- `?view=full` / `?surface=full`: full research ledger  
- `?surface=research` / `?surface=archive`: pipeline / archive lanes  

## Explicit non-claims

- No paid research  
- No new customer publication approval  
- No Sales Ready promotions  
- No destruction of store / 29 historical / Scaling Companies / contacts / shares  
