# Current Brand Incident Expansion — Final Report

**Incident path:** `data/research-engine-v2/census-autopilot-v4-standing/geography-quality-incident-v1/`  
**V4 production writes:** **PAUSED** — do not resume.  
**Corrective Airtable writes:** **none** (dry-run only).

## Executive summary

V3 + V3.1 governed writes left **70/70 Choice-family** production rows with `Current Brand = "Choice"` (parent/source-family default). Official Choice property URLs already encode hotel-level brands (e.g. Sleep Inn). Root cause is code-path contamination, not Brand Explorer override.

**Damage (P0):** 70 Choice wrong affiliations.  
**Safe dry-run corrections prepared:** 70.  
**Parser/gate fixes landed in repo;** production apply deferred.

## CURRENT BRAND — Q50–72

| # | Question | Answer |
| ---: | --- | --- |
| 50 | Production records audited for Current Brand? | **400** |
| 51 | Current Brand blank? | **0** |
| 52 | Current Brand incorrect? | **70** (parent-as-brand; Choice-dominant) |
| 53 | Parent company incorrectly used as Current Brand? | **70** |
| 54 | Historical Brand incorrectly used as current? | **0** (not observed in this snapshot set) |
| 55 | Choice-family records audited? | **70** |
| 56 | Choice-family Current Brand incorrect? | **70** |
| 57 | Exact Choice root cause? | Choice discovery omitted brand; family fallback wrote Current Brand='Choice'; URL slug ignored |
| 58 | Was "Choice Hotels"/Choice used as brand default? | **YES** (value written: `Choice`) |
| 59 | Radisson Americas incorrectly represented? | Represented as Current Brand=Choice (incorrect collapse); regional Choice parent not wrong, hotel-level brand missing |
| 60 | Current Brand and Parent Company separate in canonical claims? | **YES** (gate module + claim shape) |
| 61 | Current affiliation temporal/history-aware? | Canonical claim model supports temporal periods; production Airtable still single Current Brand snapshot — reflags steward/temporal |
| 62 | Can medium/fuzzy property matches write Current Brand? | **NO** |
| 63 | Can Brand Explorer override current property-level affiliation? | **NO** |
| 64 | Can Parent Company automatically populate Current Brand? | **NO** |
| 65 | Current Brand corrections available? | **70** |
| 66 | Safe brand corrections? | **70** |
| 67 | Reflags requiring temporal update? | **0** |
| 68 | Steward review? | **0** |
| 69 | Any Cvent brand evidence used? | **NO** |
| 70 | Any legacy brand evidence used? | **NO** |
| 71 | Choice regression tests pass? | **YES** |
| 72 | Cross-family parent-vs-brand regression tests pass? | **YES** |

## Artifacts

| # | File |
| --- | --- |
| 21 | `21-current-brand-production-audit.json` |
| 22 | `22-choice-brand-audit.json` |
| 23 | `23-choice-radisson-regional-map.md` |
| 24 | `24-current-affiliation-source-policy.json` |
| 25 | `25-current-affiliation-gate.json` |
| 26 | `26-choice-brand-root-cause.json` |
| 27 | `27-brand-normalization-registry-audit.json` |
| 28 | `28-brand-corrections-research.json` |
| 29 | `29-brand-corrective-write-dry-run.json` |
| 30 | `30-brand-regression-tests.json` |
| 31 | `31-brand-explorer-directionality.md` |

## Code fixes (future inserts / non-production path)

- `lib/research-engine-v2/census-autopilot-v3/current-affiliation.js`
- Choice CALA adapter brand from `brandName` / URL slug
- Removed family→brand fallbacks in discovery, pilot-selection, dry-run
- Tests: `npm run test:census-autopilot-current-affiliation`

## Explicit non-actions

- **No** V4 resume
- **No** corrective Airtable apply
- **No** Cvent / legacy brand evidence
