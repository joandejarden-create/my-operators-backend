# COMMIT_LIST — Radisson Blu Toronto Customer Release V1

## Release branch (this PR)

| SHA | Summary |
|-----|---------|
| `dadc31e6a` | Publish Radisson Blu Toronto ADP customer release with Choice Privileges portfolio |
| `890f41279` | Register Toronto ADP report-archive and monthly-review current artifacts for customer release |
| `0504fe0de` | Merge origin/main into Toronto customer release branch |

## Toronto-specific ADP work packaged in `dadc31e6a`

Isolated from primary dirty checkout into this branch (no GDI dirt):

| Theme | Artifacts / modules |
|-------|---------------------|
| ADP Full E2E / forensic | `scripts/adp-toronto-measurement-forensic-v1.mjs`, `reports/adp/radisson-blu-toronto-measurement-forensic-v1/` |
| Airtable/persistence closeout | classic `data/ai-demand-positioning/published/adp_radisson_blu_toronto_downtown/`, runtime periods, fact-conflicts |
| Local dropdown fix | `fixtures/ai-demand-positioning/radisson-blu-toronto-downtown-property-profile.json` + `registerCertifiedAdpPropertyForCustomerDropdown` |
| AI Admin artifact registration | `reports/ai-demand-positioning/current-report-pdf/…`, AVA archive + production store, report-archive indexes |
| Core/Loyalty period-family fix | `scripts/test-adp-period-family-resolution-v1.mjs` + related lib resolution |
| Brand & Portfolio rendering fix | `lib/ai-demand-positioning/brand-portfolio/resolve-measurement-available-bpp-v1.js`, `bpp-peer-competitive-columns-v1.js` |
| PDF updates | Standard ADP PDF + AVA NOVEMBER2026 PDF under release + current-report-pdf |
| Tests | `test:adp-*-v1` suite listed in REGRESSION_QA.md |

## Historical GDI source branch (context only; not merged as GDI)

Prior `exec/gdi-radisson-blu-toronto-15usd-full-e2e-v1` lineage already on `main` via earlier PRs (#288–#291). This PR is **ADP customer publication only**, not a re-merge of GDI research packages.

## Primary checkout (preserved dirty; not the merge vehicle)

- PATH: `C:\Dev\dealality\my-operators-backend`
- BRANCH: `exec/gdi-radisson-toronto-final-operational-certification-v1`
- HEAD (pre-sync): `fb941f403`
- Unrelated GDI dirty work: **preserved** (not overwritten)
