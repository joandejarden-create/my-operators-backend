# GDI Research Productivity — Metric Source Map

**Contracts:** `gdi-research-productivity-v1` · `gdi-research-effort-benchmark-contract-v1`  
**Benchmark version:** `v1.0.0-provisional`  
**Scope:** GDI Research Receipt / Executive Summary only  
**Future:** Documented for ADP, Hotel Explorer, Deal Opportunities — no cross-product engine

## Grains

| Grain | Use in productivity |
|---|---|
| Research Activity | Screened-out / weekly revalidation when ledgered; provider spend (internal) |
| Qualified Inventory | Primary volume source for discovery, deep, WHO/HOW, contacts, qualification |
| Commercial Engagement | Excluded from hours-saved (ordinary pursuit remains hotel work) |

## Customer-facing metrics

| Metric | Definition | Counting unit | Source | Period | Dedup | Audience | Availability |
|---|---|---|---|---|---|---|---|
| Estimated Research Hours Saved | Equivalent manual hours − residual hotel review fraction | hour | Activity volumes × provisional benchmarks | Package / reporting period | Activity-specific lines; no generic × FQO | Customer | Derived when inventory present |
| Equivalent Working Days Saved | Hours saved ÷ 8 | working day | Hours saved | Same | Same | Customer | Derived |
| Estimated Research Labor Value | Hours saved × authorized fully loaded $/hr | USD | Authorized labor rate only | Same period as investment compare | Same | Customer when authorized | `HOTEL_INPUT_REQUIRED` until authorized |
| Equivalent Manual Research Hours | Gross estimated analyst hours | hour | Same | Same | Same | Shown in methodology / RI | Derived |

## Internal-only metrics

| Metric | Notes |
|---|---|
| Research Productivity Multiple | Equivalent ÷ measured Dealality human hours; AI elapsed time excluded |
| Research Labor Value Relative To Investment | Only when labor value + authorized investment share period |
| Actual Provider Expenditure | Never on customer receipt |
| Cost Per Qualified Opportunity | Internal; spend ledger required |
| Benchmark Validation Status | `PROVISIONAL` until timed exercises |

## Deduplication rules

- Discovery vs deep: separate activity keys; deep only when deep markers present.
- WHO/HOW vs alt-entry: alt-entry counted only with alt-entry evidence.
- WHO/HOW vs contact verification: identification vs channel confirmation.
- Weekly revalidation / screened-out: unavailable without durable telemetry (not zero-filled).
- Shared-market vs hotel benefit: hotel-level benefit shown; portfolio must not sum shared effort as unique spend.
- Failed/retried provider jobs: not counted as additional customer time avoided without durable telemetry.

## Investment recovery split

| Category | Meaning |
|---|---|
| A. Estimated Research Productivity Value | Redeployable research time estimate |
| B. Verified Incremental Commercial Value | Hotel-reported / verified outcomes |

Never combine A + B into a single realized ROI claim. Redeployable time ≠ payroll savings.

## Future product note

When ADP / Hotel Explorer / Deal Opportunities implement Research Receipts, each product should adopt a **product-specific** effort benchmark + productivity module. GDI modules are a reference, not a shared reporting runtime.
