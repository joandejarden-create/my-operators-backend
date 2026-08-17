# Brand Explorer — Openings / Examples / Properties (Ascend template)

Permanent card template for **all** Brand Explorer brands (active + future AI builds).

**Gold reference:** Ascend Hotel Collection.

## Required structure

| Field | Ascend rule |
| --- | --- |
| Title | `{Property} {Brand} — {City}` e.g. `Amberes 64 Ascend Hotel Collection — Mexico City` |
| Body line 1 | Comma-separated chips |
| Body line 2 | Location (city, country / district) |
| Body line 3 | Meta / asset (country or conversion · keys · amenities) |
| Body line 4 | Scenario accent (optional; lime uppercase in UI) |
| Body line 5 | Property-specific opening teaser |
| Body line 6 | Optional `https://…` |
| Case Summary Tags | Blue pill backstop |
| Case Summary Overview | Modal overview / teaser backstop |

Blank-line (`\n\n`) paragraphs preferred; Ascend live data also uses single `\n` — the atelier parser accepts both.

## Geography selection (active brands)

1. Prefer **CALA** property openings for the brand when inventory exists.
2. If CALA count is below the minimum (3), fill with **same-brand** U.S./global openings.
3. Never substitute a sibling brand as a CALA stand-in (e.g. Quality Inn for Suburban).

Cohort: **all** Brand Explorer brands in the openings audit (18 active + 5 true-incomplete).

Remediation: `npm run brand-explorer-openings-ascend-cala-remediation -- --dry-run`

## Forbidden

- Titles ending in `— Property Example` / `— CALA Property Example`
- Generic teaser: “property example for owners comparing affiliation fit, design narrative…”

## Live audit + PR gate

```bash
npm run brand-explorer-openings-ascend-template-audit
npm run dealality:pr-check-suggest
```

- Report: `reports/brand-explorer-openings-ascend-template-audit.md`
- PR matrix id: `brand-openings-ascend` in `docs/dealality-pr-validation-matrix.md` / `lib/dealality-pr-check-matrix.js`
- Cohort expectation: **all** audited Brand Explorer brands (active + true-incomplete) pass Ascend structure; CALA-first when inventory exists.

AI / Lane 2 / CALA builders must call `buildOpeningsPropertyCard` / `buildOpeningsPropertyCardTitle`.
