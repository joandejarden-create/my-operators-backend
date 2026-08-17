# Tribute Portfolio — Targeted Source-Backed Extraction

Generated: 2026-07-09T06:41:38.740Z
Mode: **dry-run** · Airtable modified: **no**
Brand: Tribute Portfolio `recCvV0PuZOi8c3hC`

## Root cause

- Generic brand extraction is pilot-keyed. resolveBrandExtractionContext only resolves PILOT_BRANDS; Tribute is not a pilot, so getBrandFieldHints returned null for every field and the extractor emitted registry-wide data-gap placeholders.

**Evidence:**
- brand-extraction-context.js: resolveBrandExtractionContext → { resolved:false, pilotKey:null } for non-pilot brandId.
- brand-field-extraction-hints.js: getBrandFieldHints returns null when !pilotKey; BRAND_HINT_PROFILES only has kimptonHotels + curioCollection.
- brand-extract-rules.js: extractBrandFactsFromText pushes gapFact when tryExtractField returns null → all 24 fields became gaps.

**Not the cause:**
- Source text loading works: FDD ~1.3M chars, Bonvoy 13.5k, consumer 2.8k, brand page 3.6k, dev captures 3–6k.
- Source roles / extraction eligibility fine: 6/6 approved for Explorer + Extraction.
- Fact steward is correctly strict: it refused to approve gap/placeholder facts (protected governance).

## 1. Source inventory (readable text + key phrases)

| Source | Role | Extraction? | Chars | Key phrases | Error |
|--------|------|-------------|-------|-------------|-------|
| Tribute Portfolio — Official Tribute Po… | consumer_page | yes | 2353 | 3 | — |
| Tribute Portfolio — Captured Marriott p… | brand_page | yes | 3650 | 7 | — |
| Tribute Portfolio — Marriott brand-port… | development_page | yes | 6013 | 6 | — |
| Tribute Portfolio — Marriott developmen… | development_page | yes | 3399 | 2 | — |
| Tribute Portfolio — 2026 Tribute FDD — … | local_pdf | yes | 1308875 | 3 | — |
| Tribute Portfolio — Marriott Bonvoy loy… | bonvoy_page | yes | 10452 | 3 | — |

## 2. Existing 24 Pending fact audit

- Total brand facts: **41**
- Data-gap placeholders: **24**
- Held Internal Only (FDD economics): **3**
- With real value: **17**
- Prior targeted facts: **17**
- All non-held Pending facts are data-gap placeholders (dataGap=Yes). Leave them Pending for now; after targeted clean facts are approved, mark the placeholder rows Needs Review or Rejected in a later stewardship step (do not approve them).

## 3. Proposed targeted facts (source-backed)

- Proposed (v1 rules): **0** · approvable: **0** · human-review (AI-interpreted): **0**

| Field | Type | Conf | Approvable | Source | Value |
|-------|------|------|------------|--------|-------|

## 3b. v23 evidence-readiness candidate facts

- New candidates: **0**
- Source-backed: **0** · internal-only: **0**
- Slots supported: none
- Slots still lacking evidence: `loyalty.earn`, `loyalty.redeem`, `loyalty.elite`, `loyalty.proof`, `loyalty.kpi.hotels`, `loyalty.kpi.markets`, `loyalty.kpi.members`, `loyalty.kpi.mix`, `standards.last_reviewed`, `standards.requirement`, `overview.proof.1`, `overview.proof.2`, `overview.proof.3`, `overview.proof.4`, `overview.proof.5`, `overview.proof.6`, `overview.proof_operator`
- v23B review package can be built: **no**

| Field | Slots | Eligibility | Source | Value |
|-------|-------|-------------|--------|-------|

### v23 evidence excerpts


### v1 evidence excerpts


## 4. Facts held / not created

- Held field keys (Internal Only / human review, never targeted): `be.economics.royaltyPct`, `be.economics.initialFranchiseFee`, `be.economics.marketingFeePct`
- Not found in approved sources: none
- Skipped (duplicate/existing): `be.identity.brandName` (prior_targeted_fact_exists), `be.identity.parentCompany` (prior_targeted_fact_exists), `be.loyalty.programName` (prior_targeted_fact_exists), `be.positioning.summary` (prior_targeted_fact_exists), `be.positioning.tagline` (prior_targeted_fact_exists), `be.positioning.guestPromise` (prior_targeted_fact_exists), `be.overview.developmentModel` (prior_targeted_fact_exists), `be.overview.whyValue` (prior_targeted_fact_exists), `be.overview.typicalUseCase` (prior_targeted_fact_exists)
- FDD economics/fees/Item 19/legal are never targeted here — they remain Internal Only / human review. be.footprint.geoIntro is not targeted (no Tribute-specific footprint statement in approved sources).

## 5. Duplicate / supersession handling

- Existing placeholders: 24
- Recommendation: **leave_pending_then_needs_review**
- Do not delete or approve the 24 existing placeholder facts (3 of which are held Internal Only). After the targeted clean facts are approved, set the matching placeholder rows to Needs Review (or Rejected) so they don't clutter publish scope. Targeted facts are tagged with extractionRunId 'tribute-targeted-*' and Reviewer Notes 'tribute-targeted' to prevent duplicate creation on rerun.

## 6. Governance projection

- Would enable governance after approval: **no**
- Approvable: 0 (identity 0, substantive 0)
- Not enough approvable source-backed facts yet; strengthen sources or patterns before governance.

## 7. Apply

- Apply recommended: **no**
- No facts created (dry-run).

## 8. Exact next command

```bash
npm run brand-explorer-evidence-required-slot-readiness-plan -- --brand tribute-portfolio --dry-run
```

## Does not do

- Approve facts (all created as Pending) or publish governance
- Create gap/placeholder facts or facts from empty source text
- Target FDD economics / fees / Item 19 / legal (kept Internal Only / human review)
- Write Brand Setup content / hero / image / logo fields
- Set Company Validated or Company Validation Date; imply Marriott validation
- Use third-party sources or change UI/scoring/BAS/OAS/OCS/Deal Readiness/schema
