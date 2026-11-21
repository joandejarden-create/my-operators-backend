# HIF Foundation — Airtable Source → Destination Mapping

**Base:** `appCCUsuGsE1ifoLk` (HPC / HI canonical)  
**Live audit:** 2026-11-21 · 32 tables in base · CUSTOMER_BLOCKED  
**Contract:** `lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js`  
**No new tables. No invented field names.**

## Destinations (verified live)

| Destination | Table ID | Role | Idempotency |
|-------------|----------|------|-------------|
| Hotel Property Census | `tbl9aY5ijiuIzzWam` | Canonical hotel universe (~19,462 records) | Property Identity Key / record id |
| HI Hotel Discovery Inventory | `tblx77Nmh56h4xa9z` | Matched + unmatched discovery hotels | Discovery Property ID |
| HI Discovery Findings | `tblGjTWNm8G4UOF1e` | Discrete findings (contract/brand/mgmt/commercial) | Finding ID |
| HI Hotel Contracts | `tblMjUCBtXF2o3r9u` | Executed named-hotel agreements | Contract ID |
| HI Contract Milestones | `tbljzAYjYs3dyHWZk` | Dated milestones | Milestone ID |
| HI Organizations | `tbliXEfUOWk32AN9y` | Verified organizations | Org identity fields |
| HI Hotel–Org Relationships | `tblGiwN0foXXDESvZ` | HPC-linked relationships only | Hotel+Org+role |
| HI Graph Evidence | `tblc9JhKT1edr4grE` | HPC-linked evidence only | Evidence keys |

Live counts (2026-11-21): Inventory **541** · Findings **446** · Contracts **269** · Milestones **259** · Orgs **7** · HPC **19,462**

## Source → destination

| Research fact | Destination | Notes |
|---------------|-------------|-------|
| Verified hotel identity (name, geo, brand) | Discovery Inventory | Prefer match to HPC; **never auto-create HPC** from research |
| Verified HPC attribute enrichment | HPC | Non-destructive only; higher-confidence wins |
| Contract commencement/expiration/amendment | HI Hotel Contracts + Milestones + Findings | Preserve historical rows; do not overwrite originals |
| Brand conversion / franchise signal | Findings `Finding Type=BRAND_AFFILIATION` · Field Name `COMMERCIAL_OPPORTUNITY_BCT` | Uses **existing** select options |
| Management / operator signal | Findings `Finding Type=MANAGEMENT` · Field Name `COMMERCIAL_OPPORTUNITY_MCT` | Same |
| Adjacent concession/development | Findings Field Name `COMMERCIAL_SIGNAL_*_ADJACENT` | Not principal BCT/MCT |
| Owner / manager org (verified) | HI Organizations (+ Relationships after HPC match) | No registrant-as-owner inference |
| Unmatched hotel evidence | Inventory + Findings only | Graph Evidence / Relationships forbidden until HPC match |
| Contact email/phone from Surfe etc. | **Do not persist** | Contact governance |

## Write categories

- **AUTO_PERSIST_ELIGIBLE** — verified, existing destination, evidence + source URL  
- **STAGED_FOR_REVIEW** — needs validation / schema gap  
- **CONFLICT_HOLD** — identity or fact conflict  
- **RESEARCH_ONLY** — speculative, unmatched graph paths, HPC auto-create

## Schema gaps (proposal only — no Meta apply)

1. No dedicated Commercial Opportunity table — mapped via Findings Field Name convention.  
2. No Finding Type `COMMERCIAL_OPPORTUNITY` — reuse BRAND_AFFILIATION / MANAGEMENT.  
3. Accessibility / engagement route fields not first-class — encoded in Value Observed / Notes until founder authorizes additive fields.  
4. Additive EX-10 document-execution fields remain deferred (prior V5 proposal).
