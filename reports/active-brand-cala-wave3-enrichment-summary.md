# Active Brand CALA — Wave 3 enrichment summary

**Generated:** 2026-07-24  
**Prior:** Waves 1–2  
**Scope:** SLH Property ID · Ascend / Choice open blanks · Autograph steward · Radisson Curico  
**Rule:** Official sources only · fill-blank · Affiliation = Brand Setup `Brand Name`

## Headline results

| Brand | Metric | Before → After |
|-------|--------|----------------|
| **Small Luxury Hotels of the World** | % Property ID | **0 → 100** (84 fills) |
| **Ascend Hotel Collection** | % Website / Property ID | **84.6 → 100** |
| **Radisson by Choice** | % Website / Property ID | **89.5 → 92.1** |
| Autograph Collection | Open blanks | 0 apply (all remaining blanks Pipeline/Closed) |

Coverage: `reports/active-brand-cala-enrichment-coverage-wave3-after.csv`

## Applied counts

| Action | Count |
|--------|------:|
| SLH Property ID (official SLH API hotel id via Website slug) | **84** |
| Ascend Website + Property ID (EC001 Puembo, DO013 Emotions Puerto Plata) | **2** |
| Radisson by Choice Website + Property ID (Curico CL010) | **1** |
| Autograph Website/ID | **0** (already complete for Open; blanks are Pipeline/Closed) |
| Park Inn apply | **skipped** (not Active Brand Setup) |

## Details

### SLH
- Script: `scripts/backfill-slh-cala-property-id.mjs`
- Matched all 84 CALA census rows with `slh.com/hotels/{slug}` Website to CALA catalog (82 open + extras via slug)
- Property ID = official SLH search API `id` (numeric)
- Artifacts: `reports/slh-cala-property-id-backfill-plan.json`, `…-apply-log.json`

### Ascend + Choice
- Regional enrichment found 2 Active applies; Park Inn stewarded out
- Manual verified Ascend opens from regional JSON-LD:
  - San Jose de Puembo → `ec001`
  - Emotions Puerto Plata → `do013`
- Steward (no safe open Choice listing found this pass):
  - Comfort Hotel & Suites Natal (do not map to Quality Natal BR058)
  - Quality Hotel Real San Jose
  - Quality Hotel Real Aeropuerto Santo Domingo
  - Ascend Mangrove (USVI Pipeline)
- Artifacts: `reports/choice-wave3-active-apply-log.json`, `reports/choice-wave3-open-blank-manual-plan.json`

### Autograph (Marriott)
- Sitemap enrichment: **0** Autograph fill-blank rows (Open inventory already complete)
- Remaining blanks: Pipeline/Closed only → steward (`reports/wave3-blank-website-pid-steward.json`)

## Change impact

**High** — 87 Property ID / Website census updates.

**Rollback:** clear Property ID on SLH apply-log records; revert Ascend/Radisson Website+PID from Choice apply logs.

## Manual QA

- [ ] SLH sample: Property ID numeric and matches slh.com hotel page identity
- [ ] Ascend Puembo / Emotions Puerto Plata open choicehotels.com property URLs
- [ ] Radisson Curico CL010
- [ ] No Park Inn Affiliation/Website write on Active-only path
