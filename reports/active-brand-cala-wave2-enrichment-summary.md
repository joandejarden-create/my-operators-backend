# Active Brand CALA — Wave 2 enrichment summary

**Generated:** 2026-07-24  
**Prior:** Wave 1 (`reports/active-brand-cala-wave1-enrichment-summary.md`)  
**Scope:** Kimpton Hotels · Hotel Indigo · MGallery Collection · Curio Collection by Hilton · Tapestry Collection by Hilton  
**Rule:** Official brand sources only · fill-blank · Affiliation = exact Brand Setup `Brand Name`

## Coverage (Wave 2 brands)

| Brand | CALA after | % Website | % Property ID | Notes |
|-------|----------:|----------:|--------------:|-------|
| Kimpton Hotels | 11 | ↑ Website on Virgilio + Mas Olas | +SJDTD on Mas Olas | Most blanks are Pipeline |
| Hotel Indigo | 15 | ↑ Grand Cayman Website | — | PID often claimed on sibling row |
| MGallery Collection | 8 | unchanged (catalog already complete) | — | 3 steward extras not on Accor MGA catalog |
| Curio Collection by Hilton | ↑ +3 creates | open matched already complete | — | 9 unmatched census stewarded |
| Tapestry Collection by Hilton | ↑ +2 creates | open matched already complete | — | 6 unmatched census stewarded |

Baseline for delta: `reports/active-brand-cala-enrichment-coverage-wave1-after.csv`  
After: `reports/active-brand-cala-enrichment-coverage-wave2-after.csv`

## Applied counts

| Brand | Website | Property ID | Amenities | Hotel Description | Creates |
|-------|--------:|------------:|----------:|------------------:|--------:|
| Kimpton Hotels | 2 | 1 | 0 | 0 | 0 |
| Hotel Indigo | 1 | 0 | 0 | 0 | 0 |
| MGallery Collection | 0 | 0 | 0 | 0 | 0 |
| Curio Collection by Hilton | 3 (on create) | 3 (on create) | 3 (directory-suggested) | 3 (GraphQL shortDesc) | **3** |
| Tapestry Collection by Hilton | 2 (on create) | 2 (on create) | 2 | 2 | **2** |

**Wave 2 totals:** Website **8** · Property ID **6** · Description **5** · Amenities **5** · Creates **5**

## Details

### Kimpton + Hotel Indigo (IHG)
- Standard directory sync ready=0 (PIDs often already claimed / pipeline names absent from open directory).
- Forced verified applies (`scripts/run-ihg-wave2-forced-match.mjs`):
  - Hotel Indigo Grand Cayman → Website (GCMSM claimed elsewhere)
  - Kimpton Virgilio → Website (MEXPL claimed elsewhere)
  - Kimpton Mas Olas Todos Santos → Website + Property ID `SJDTD`
- Blocked false friend: Guadalajara Providencia ≠ Guadalajara Expo
- Steward: remaining blanks mostly Pipeline / not on public open directory

### MGallery (Accor)
- Accor catalog brand=MGA: **5** CALA hotels, all already complete on census.
- Steward extras (census MGallery not on open catalog): Mayaliah Tulum, Sofitel Valle Sagrado Cusco-MGallery, D'Hotels Jeri

### Curio + Tapestry (Hilton)
- Fill-blank on matched open rows: **0** needed (already URL-linked).
- Created official open CALA directory gaps (property URL from Hilton GraphQL `homeUrlTemplate`):
  - Amare Cancun (`CUNAAQQ`)
  - Punta Sal Suites and Bungalows (`PTLSBQQ`)
  - York Medellin (`MDEYKQQ`)
  - Chelsea Bogota Parque 93 (`BOGCHUP`)
  - Perla La Paz (`LAPITUP`)
- Unmatched census (pipeline / not on directory) stewarded in Hilton plan JSONs

## Steward leftovers

| Item | Action |
|------|--------|
| Kimpton/Indigo Pipeline blanks | Wait for IHG open listing |
| Kimpton Aluna Resort Tulum | No safe open directory match |
| MGallery extras ×3 | Confirm status / Accor listing |
| Curio unmatched census ×9 | Pipeline / not on hilton.com directory |
| Tapestry unmatched census ×6 | Pipeline / not on hilton.com directory |

## Artifacts

- `reports/ihg-wave2-forced-match-plan.json` / `ihg-wave2-forced-match-apply-log.json`
- `reports/mgallery-cala-affiliation-apply-plan.json` (dry-run; already complete)
- `reports/hilton-census-enrichment-plan-curio-collection-by-hilton.json`
- `reports/hilton-census-enrichment-plan-tapestry-by-hilton.json`
- `reports/hilton-wave2-cala-directory-create-plan.json` / `hilton-wave2-cala-directory-create-apply-log.json`
- `reports/active-brand-cala-enrichment-coverage-wave2-after.csv`

## Change impact

**High** — Hotel Census creates + Website / Property ID / Amenities / Hotel Description writes.

**Rollback:** delete create records from apply log; clear Website/PID on IHG forced-match apply log rows.

## Manual QA

- [ ] Curio/Tapestry new rows open hilton.com `/en/hotels/{ctyhocn}-…` Website + Property ID
- [ ] Kimpton Virgilio / Mas Olas / Indigo Grand Cayman have ihg.com Website
- [ ] No Affiliation drift from Brand Setup names
- [ ] Guadalajara Providencia still blank (false match blocked)
