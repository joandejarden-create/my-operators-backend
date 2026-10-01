# Brand Explorer Pilot QA — Fairfield by Marriott

> **Status:** READY FOR CHATGPT QA
> **Generated:** 2026-10-01T11:26:53.398Z
> **Fixture:** `fixtures/brand-explorer-presentation-fairfield-by-marriott-full.json` (101 rows)
> **Brand source:** `fixture_stub`

## Selected brand

**Fairfield by Marriott** (`fairfield-by-marriott`, `recpUTDtwt1wPMDPj`)

## Benchmark brands

- **Kimpton Hotels** — IHG lifestyle gold bar (full tabs, momentum, gallery, value scenarios)
- **Everhome Suites** — Choice extended-stay Tier 1 full fixture reference
- **Radisson Individuals by Choice** — recent CALA-forward completion reference

## Gate results

| Gate | Result |
| --- | --- |
| Tab factory audit | PASS (0 fail findings) |
| Golden content | PASS |
| Source provenance | PASS |
| Section pattern parity | PASS |
| Image uniqueness | PASS |
| Image role match | PASS |
| Content quality (semantic) | PASS (0 issues) |
| External quality lock | DEFERRED (factory preview — expected until founder approval) |
| External display state | `factory_preview_internal` |
| Brand Website | `https://fairfield.marriott.com/` (PASS) |


## ChatGPT QA remediation (2026-10-01)

### Round 1
- Fixed grammar (`a efficient` → `an efficient`), double punctuation, and duplicated opening titles
- Removed repeated boilerplate (`Keep Fairfield by Marriott product and service responsibilities…`)
- Rewrote psychographics, `insight.similar` peer comparisons, and softened over-strong operating claims
- Added semantic content-quality gate: `npm run test:brand-explorer-pilot-content-quality`
- Remediation module: `lib/partner-intelligence/brand-explorer-fairfield-content-remediation.js`

### Round 2 (second independent ChatGPT QA)
- Fixed `footprint.portfolio_mix` run-on (`weak fit Curated sample mix` → proper paragraph break)
- Canonicalized Cancún property name to `Fairfield Inn & Suites Cancun Airport` in `footprint.region.cala`
- Removed unsourced `rather than lifestyle-hotel personality` contrast from psychographics
- Replaced `king-room prototype` with `rooms-focused select-service prototype` in `insight.similar`
- Expanded semantic gate (`pilot-content-quality-v2`) so malformed run-on joins fail automatically
- Report: `reports/brand-explorer-fairfield-pilot-remediation-round2.json`

### Round 3 (QA report consistency)
- Root cause: `evaluateTabFactoryFromPayload` counted `cleanly_unavailable` Brand Basics snapshot fields as `failFindings` while `completeness.auditPass` correctly treated them as resolved → `auditPass=true` with `failFindings=10`
- Root cause: pilot QA called `evaluateBrandExternalQualityLock(brand, options)` instead of `(brand, html, options)`, and the incomplete stub re-resolved production display without Brand Basics → false `draft_applied_with_defects`
- Fix: align `failFindings` with completeness governance (`auditPass` requires `failFindings === 0`); correct external-lock call; factory-preview stub uses `factory_preview_internal`; `readyForChatGptQa` blocks on fail findings, audit fail, or defect display states

### Round 4 (Brand Website)
- Fixture `brandWebsite` set to official brand-specific URL `https://fairfield.marriott.com/` (not parent root `https://marriott.com/`)
- Validation: parent-company root websites fail when an official brand_page URL exists (`brand-explorer-brand-website-preference.js`); wired into Fairfield pilot QA `readyForChatGptQa`

## Coverage

- Gallery images: **6**
- Scenario cards: **3**
- Value creation scenarios: **4**
- Openings cards: **3**
- Recent Momentum cards: **3**

## Production database writes

**None in this PR.** Fixture-only deliverable. To materialize in Airtable (Presentation table only):

```bash
node scripts/apply-brand-explorer-presentation-fixture.mjs --dry-run \
  --brand-record-id recpUTDtwt1wPMDPj \
  --fixture fixtures/brand-explorer-presentation-fairfield-by-marriott-full.json \
  --only-missing
```

## Preview for founder review

1. Start local server: `npm start`
2. Factory preview URL: `/brand-explorer.html?id=recpUTDtwt1wPMDPj&beInternalPreview=1&factoryPreview=1`
3. After staged Airtable apply (founder-approved): `/brand-explorer.html?id=fairfield-by-marriott`
