# GDI Contact Actionability V1 — Implementation & Free Recovery Report

Generated: 2026-11-20T17:02:00Z

**Status:** Implementation + free recovery complete · **STOP** for founder review (no merge / deploy / publication)

## Founder decisions honored

| Decision | Status |
|---|---|
| EXISTING_RULES_WITH_CONTACT_TIERS | Implemented (read-time labels) |
| STRICT_EMAIL not a publication gate | Enforced in contract + tests |
| Preserve 182 customer-visible opportunities | **182 → 182 (Δ 0)** |
| Paid provider spend | **$0** |
| Proposed $639 analyst budget | **Not authorized / not used** |
| No CURRENT / STAGED / Airtable writes | Confirmed |
| No invented source URLs | Confirmed |

## Phase 1 — Framework

### Delivered

- Contract module: `lib/group-demand-intelligence/presentation/gdi-contact-actionability-v1.js`
- Read-time attach: package presentation + `toOpportunityListDto`
- Share parity + signed-share fields for actionability
- Drawer badge + explanation + Direct buyer vs Commercial entry point labels
- Filter key: `filters.contactActionability`
- Tests: `npm run test:gdi-contact-actionability-v1` — **PASS**
- npm scripts registered in `package.json`

### Before / after actionability (frozen 182)

| Tier | Count |
|---|---:|
| SALES_READY | 5 |
| REACHABLE | 15 |
| CONTACT_RESEARCH_REQUIRED | 159 |
| CONTACT_IDENTIFICATION_REQUIRED | 3 |

Customer-visible inventory: **182** (unchanged).

### REACHABLE reconciliation (31 → 15)

Leakage-audit `VERIFIED_REACHABLE` (31) counted named person + phone **without** requiring commercial relevance.

Actionability `REACHABLE` (15) requires a **commercially relevant** named person (DIRECT_BUYER / RELEVANT_COMMERCIAL_ENTRY_POINT / remit-evidenced research candidate).

| Metric | Count |
|---|---:|
| Leakage VERIFIED_REACHABLE | 31 |
| Soft named-phone or verified-email (diagnostic) | 40 |
| Soft excluding SALES_READY | 35 |
| Actionability REACHABLE | **15** |
| Soft-only (named phone, not REACHABLE/SALES_READY) | 20 |

**Contract decision:** Keep commercial-relevance requirement. Soft diagnostic retained for transparency; do not inflate REACHABLE with non-relevant named phones.

Evidence: `REACHABLE_RECONCILIATION.json`

## Phase 2 — Bethesda free recovery

| Metric | Value |
|---|---|
| Cohort | 34 person-specific emails without source URL |
| Paid spend | **$0** |
| Evidence-supported source found | **0** |
| Upgrades to SALES_READY if applied | **0** |
| Publication writes | **0** |
| Elapsed | ~164s free HTTP |

All 34 stopped under free-fetch rules (`email_or_name_not_co_located_on_fetched_pages` / fetch failures). Domain homepage/contact/about probes did not co-locate name+email. No URLs invented. Results **not** applied to CURRENT.

Evidence: `BETHESDA_FREE_RECOVERY_RESULTS.json`

## Phase 3 — Controls

| Measure | Result |
|---|---|
| SALES_READY / REACHABLE change vs design baseline | Unchanged (5 / 15) — recovery not applied |
| Provenance preserved | Yes (read-time only) |
| Successful upgrades applied | 0 |
| Cost per upgrade | n/a (none) |
| Duplicate research prevented | Shared `personIdentityKey` wired; 0 cross-hotel dupes in frozen set |
| Inventory delta | **0** |

## UI changes

- Who-to-contact drawer: actionability badge + short explanation
- Buyer vs entry-point label (never mislabels entry point as Direct buyer)
- Unverified email quiet note when email present but not source-confirmed
- List filter support for `contactActionability`

## Tests / gates

```
npm run test:gdi-contact-actionability-v1  → PASS
```

Gates covered: no STRICT publication gate; unverified person_specific email; SALES_READY / REACHABLE / RESEARCH / IDENTIFICATION; buyer vs entry labeling; generic mailbox exclusion; 182 inventory freeze with attach.

## Recommendation — next controlled recovery wave

1. **Do not apply** empty free-recovery results to publication.
2. Authorize a **bounded public discovery wave** (Google-first / Apify Google **only if** separately budgeted) for the 34 Bethesda cohort, still forbidding invented URLs.
3. Optionally run a **relevance enrichment** pass on the ~20 soft-phone contacts so REACHABLE can rise honestly without weakening the commercial-relevance rule.
4. Merge/deploy only after founder review of this PR.

## STOP

No merge. No deploy. No customer publication pointer changes. Awaiting founder review.
