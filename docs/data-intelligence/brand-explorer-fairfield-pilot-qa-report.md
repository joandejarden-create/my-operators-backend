# Brand Explorer Pilot QA — Fairfield by Marriott

> **Status:** READY FOR CHATGPT QA
> **Generated:** 2026-10-01T09:10:34.309Z
> **Fixture:** `fixtures/brand-explorer-presentation-fairfield-by-marriott-full.json` (101 rows)

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
| External quality lock | DEFERRED (factory preview — expected until founder approval) |

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
