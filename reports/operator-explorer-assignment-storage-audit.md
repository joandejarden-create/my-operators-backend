# Operator Explorer — Assignment Storage Audit (Case Studies)

**Date:** 2026-08-09  
**Live table:** `Operator Setup - Case Studies` (`tblAh1X0KDK8SeYK0`) — **13 fields**, **58 records**  
**Docs:** `docs/operator-case-study-airtable-fields.md`  
**Map:** `api/lib/operator-case-study-airtable-map.js`

---

## Original purpose

Owner-facing **case study / proof cards** for Operator Explorer (property story: situation, outcome, owner relevance, image).

## Current uses

| Use | Status |
| --- | ------ |
| Operator Explorer proof | Yes — primary consumer |
| Brand Explorer | No (separate brand comps) |
| Client/project CRM | No |
| Operator Fit numeric score | **Not** in numeric OAS historically; Fit v2 intends comparable relevance using Why Comparable / Comparability Strength |
| Structured hotel assignments | **Partial / weak** — missing keys, dates, brand links, current/historical, evidence strength |

## Current fields (live)

| Field | Type | Notes |
| ----- | ---- | ----- |
| property_name | text | Essential |
| display_order | number | Presentation |
| Operator | link → Master | Essential |
| branded_independent | singleSelect | **Polluted** — mixes Branded/Independent with brand names (Hilton, AC Hotels, …) |
| hotel_type | text | Soft |
| image_url | attachments | Presentation |
| outcome / owner_relevance / services | long text | Narrative |
| region | text | Soft geography |
| situation | singleSelect | **Heavily polluted** options (dozens of narrative tags) |
| Why Comparable | long text | Added 2026-08-03 for Fit |
| Comparability Strength | select High/Moderate/Limited | Added 2026-08-03 |

## Data volume / quality

- 58 rows — uneven coverage across operators  
- Strong as narrative Explorer cards for goldens  
- Weak as normalized assignment graph for derivation (geo, brands, conversion experience)

## Code dependencies

- Explorer profile builders / fixtures  
- Case study map + seed scripts (incl. Antillano Norte demo)  
- Fit comparable path references Why Comparable / strength  
- **Not** safe to delete

---

## Options

### Option A — Keep using Case Studies as-is

**Pros:** Zero migration; Explorer continues  
**Cons:** Cannot cleanly derive portfolio/experience/brand relationships; taxonomy pollution; conflates marketing story with assignment fact

### Option B — Extend Case Studies

Add assignment fields (keys, country, brand link, dates, current/historical, evidence, management structure, development type flags).  

**Pros:** One table; lower migration  
**Cons:** One object serving two jobs (story card vs intelligence assignment); Explorer UI noise; still inherits polluted selects

### Option C — Create `Operator Intelligence - Assignments` (recommended)

Keep Case Studies for **selected published stories**.  
Store **all verified hotel assignments** in a dedicated intelligence table.  
Optionally link Assignment → Case Study when a story card exists.

**Pros:** Clear SoT; supports derivation; Fit comps; brand relationship evidence; geography  
**Cons:** New table + dual-write period

---

## Assessment matrix

| Criterion | A | B | C |
| --------- | - | - | - |
| Data clarity | Low | Medium | **High** |
| Reusability | Low | Medium | **High** |
| Brand relationship derivation | Weak | Medium | **Strong** |
| Geography derivation | Weak | Medium | **Strong** |
| Comparable derivation | Weak | Medium | **Strong** |
| Development experience derivation | Weak | Medium | **Strong** |
| Scoring utility | Low today | Medium | **High** |
| Duplication risk | High (vs flat experience flags) | Medium | Controlled via DERIVE |
| Migration complexity | None | Medium | Medium–High (phased) |

## Verdict

**Recommend Option C** long-term.  
**Near-term:** keep Case Studies; stop overloading it; add Assignments in Phase 1 safe additions after founder approval.  
Do **not** create the table in this audit phase.
