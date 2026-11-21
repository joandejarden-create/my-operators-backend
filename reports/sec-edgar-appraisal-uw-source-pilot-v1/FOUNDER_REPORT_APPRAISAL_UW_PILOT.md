# FOUNDER REPORT — Appraisal / Underwriting Source Discovery Pilot

Piloted: 2026-11-21T02:13:43.242Z
Mode: **FREE_SOURCE_PREFLIGHT** (no metered Exa charged)
CUSTOMER_BLOCKED · no third product · no Meta · no paywall bypass

## Budget / authorization
- Existing V5 Search $25 ceiling: **not charged** (appraisal pilot not explicitly authorized for metered spend).
- Metered Exa Search expansion: **REQUEST APPROVAL** before permanent adapter work.
- Free SEC requests recorded: 55

## Pilot cohort
- Documents selected/fetched: **50** / **50**
- By source class: {"HOTEL_APPRAISAL_REPORT":4,"UNDERWRITING_SUMMARY_OR_ANNEX":44,"FRANCHISE_AGREEMENT_EXHIBIT":1,"VALUATION_OR_CRITERIA_CONTEXT":1}
- By region: {"US":49,"Europe":1}

## Yield (language/extraction — free preflight)
1. Franchise expiration **date-like** extractions: **1**
2. Management expiration **date-like** extracted: **0**
3. Notice/termination keyword hits: **42** (noisy; not verified notice clauses)
4. Owner keyword hits: **46** · Operator keyword hits: **36** (not property-verified ownership)
5. Independently corroborated new contract dates: **0**
6. Genuinely new contractual-fact docs vs HIF evidence: **0**
7. Additional qualified commercial prospects: **0**
8. Cost per incremental verified finding: **n/a** (spend $0)
- Appraisal assumption-only docs: **1** (e.g. Holiday Inn Canton continued-affiliation assumption)
- Source-priority / column-header pointer docs: **22** (schedule points to Franchise/Management Agreement — not a dated hotel fact by itself)

**Success criterion result:** no additional independently verified hotel-specific franchise/management contract intelligence beyond existing HIF UW/CMBS paths.

## Key finding
Hotel **appraisal reports** frequently assume continued franchise affiliation and rarely publish independently usable franchise/HMA expiration dates. **Underwriting annexes / CMBS schedules** that already feed HIF remain the higher-yield public path for hotel-specific franchise expiration columns. European free hits were mostly criteria/registry context, not hotel-specific contract schedules.

## Permanent adapter recommendation
- **DO_NOT_PRIORITIZE_STANDALONE_APPRAISAL_ADAPTER** — **FOUNDER ACCEPTED & LOCKED (2026-11-21)**
- 22 source-priority pointers routed into Franchise/Management discovery queues (`reports/sec-edgar-hif-v6-appraisal-policy/`)
- No standalone appraisal adapter · no paid appraisal research from this pilot · incidental appraisal evidence retained · valuation assumptions ≠ contractual terms
- Permanent adapter now: **false**
- Standalone hotel appraisal reports in this 50-doc free pilot predominantly assume continued franchise affiliation and did not yield independently usable hotel-specific franchise/HMA expiration dates.
- CMBS / underwriting annexes remain the high-yield public path when they contain Franchise Expiration Date columns — already covered by existing HIF CMBS extractors; this pilot did not justify a separate appraisal product.
- Source-priority / due-diligence exhibits explicitly map franchise and management expiration attributes to the underlying agreements (not the appraisal).
- Europe/Canada free hits were valuation criteria, registries, or non-hotel-specific disclosures — weak hotel-specific contract-date yield.
- Do not create a third commercial intelligence product. Lease remains deferred. Brand Conversion + Management Contract lists only.

## Next gate
OPTIONAL_FOUNDER_APPROVAL_FOR_LIMITED_METERED_SEARCH_SPOTCHECK
- If approved: metered Search for hotel-specific appraisal PDFs that cite explicit end dates; multilingual Europe/LatAm/Canada exhibit harvest; feed Brand/Management lists only.