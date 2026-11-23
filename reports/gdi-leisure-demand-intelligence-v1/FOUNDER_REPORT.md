# GDI Leisure Demand Intelligence V1 — Founder Report

**Date:** 2026-11-23  
**Authorization:** GO — $0 architecture and controlled implementation  
**Branch:** `feat/gdi-leisure-demand-intelligence-v1`  
**PR:** [#577](https://github.com/joandejarden-create/my-operators-backend/pull/577) — **OPEN**  
**HEAD:** `7668be3e267d900e9799d140cd6fdd48ce36c310`  
**Worktree:** `C:\Users\joand\AppData\Local\Temp\dealality-gdi-leisure-v1-20261123174605`  
**Paid spend:** **$0**  
**Findymail / Surfe / Airtable writes / publication / deploy:** **none**

---

## 1. Existing 14-segment overlap matrix

| LDI module | Coverage | Primary 14-segment reuse | Mode |
|---|---|---|---|
| `leisure_events` | PARTIAL | associations_conferences_events, social_special_events, corporate_celebrations_private_events | Wrapper + leisure qualification |
| `leisure_sports` | OVERLAP_WRAPPER | sports_entertainment (+ sports housing hunter) | Wrapper + participatory filter |
| `leisure_attractions` | PARTIAL | preferred_lodging_partnerships, travel_trade (+ venue lodging track) | Partnership qualification |
| `leisure_universities` | PARTIAL | education_scientific (+ university hunter) | VFR / graduation specialization |
| `leisure_destination` | GAP | social_special_events + need-period strategies | Specialization over shared queries |
| `leisure_distribution` | OVERLAP_WRAPPER | travel_trade, preferred_lodging, professional_event_planners | Referral / DMC wrapper |

**Conclusion:** Do **not** duplicate discovery engines. LDI modules wrap `runSegmentDiscoveryExecutor` / existing hunters with leisure subtype + pathway qualification. Machine-readable: `COVERAGE_MATRIX.json` via `--coverage-only`.

---

## 2. Six PowerShell module designs

| Script | Module id |
|---|---|
| `scripts/gdi-leisure/Invoke-GdiLeisureEvents.ps1` | leisure_events |
| `scripts/gdi-leisure/Invoke-GdiLeisureSports.ps1` | leisure_sports |
| `scripts/gdi-leisure/Invoke-GdiLeisureAttractions.ps1` | leisure_attractions |
| `scripts/gdi-leisure/Invoke-GdiLeisureUniversities.ps1` | leisure_universities |
| `scripts/gdi-leisure/Invoke-GdiLeisureDestination.ps1` | leisure_destination |
| `scripts/gdi-leisure/Invoke-GdiLeisureDistribution.ps1` | leisure_distribution |

Shared: `Invoke-GdiLeisureModule.ps1` → `scripts/gdi-leisure-demand-intelligence-v1.mjs`  
Supports: HotelId, profile JSON, research mode, budget (fail-closed), evidence sources, existing inventory, resume/replay flags, OutDir, auth switches (paid/publication refused in V1 cert path).

---

## 3. Canonical Full E2E integration

- Registry: `lib/.../gdi-leisure-module-registry-v1.js`
- Runner: `gdi-leisure-module-runner-v1.js` (fixture/$0)
- Portfolio: `Invoke-GdiPortfolioProcessing.ps1` gains `-LeisureModules`, `-LeisureOnly`, `-DisableLeisure`
- Default: leisure **off** → fourteen-segment path unchanged
- Combinations: LeisureOnly · LeisureModules CSV · Leisure then continue Full E2E when not LeisureOnly

---

## 4. Hotel-specific module selection

Presets (config examples — **not** live research auth):

| Hotel | HPC | Modules |
|---|---|---|
| LaLuna Grenada | `recaokE8psIbuVeuH` | distribution, attractions, destination, events, sports |
| Memmo Príncipe Real | `rec1t35e3sPerzxYu` | events, attractions, distribution, destination |
| Condesa DF | `rece0NdV9FCnaFlVS` | events, universities, attractions, distribution |

Else: characteristic heuristic from classification / commercial priorities. Override via `-LeisureModules` / `--modules`.

---

## 5. Shared opportunity data contract

`gdi-leisure-opportunity-contract-v1` — hotelId, opportunityId, demand generator, org, demandFamily, leisureSubtype, pathway, dates, geo, nonlocal evidence, room-night mechanism, hotel fit, need-period, competitive, Market Scope, contact/entry, sources, verification, recommended action, completeness, next step, segmentAssociations, optional ADP hint.  
Airtable: **reuse GDO/payload**; schema gaps proposed only (`proposeLeisureAirtableSchemaGaps`) — **no apply**.

---

## 6. Direct / partnership / campaign rules

| Pathway | Contact | Sales Ready |
|---|---|---|
| DIRECTLY_ADDRESSABLE | Named buyer or org entry | Candidate only with independent commercial gates |
| PARTNERSHIP_DRIVEN | Partnership / BD / distribution owner | Candidate with referral mechanism |
| CAMPAIGN_DRIVEN | Org/channel optional | **Never** Sales Ready from event existence |

---

## 7. Cross-segment deduplication

`gdi-leisure-cross-segment-dedupe-v1` — soft cluster org+year+title family; suppress peers; preserve segment + leisure module associations on canonical. Cert: Spice Mas Carnival appears once across events+sports fixtures.

---

## 8. Need-period integration

Statuses: `NEED_PERIOD_UNKNOWN` | `OVERLAP` | `PARTIAL_OVERLAP` | `NO_OVERLAP` | `UNAUTHORIZED_SENSITIVE`.  
Never invent ADR/occupancy. Unknown does **not** reject leisure demand.

---

## 9. Contact Intelligence integration

Reuses GDI person/org/entry fields. Pathways define contact mode. **No** Findymail/Surfe. Future licensed email via HI Contact Channels prep (separate).

---

## 10. Exact PowerShell commands

```powershell
# Standalone module
.\scripts\gdi-leisure\Invoke-GdiLeisureEvents.ps1 -HotelId recaokE8psIbuVeuH -ResearchMode fixture

# All six via portfolio leisure-only
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -HotelId recaokE8psIbuVeuH -LeisureOnly -Mode Preview

# Selected combination
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -HotelId recaokE8psIbuVeuH -LeisureModules "events,attractions,distribution" -LeisureOnly

# Coverage matrix
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -LeisureOnly -Mode AuditOnly

# Standard GDI unchanged (leisure off)
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Rehearsal -HotelsFile .\path\to\hotels.txt -DisableLeisure
```

```bash
npm run test:gdi-leisure-demand-intelligence-v1
node scripts/gdi-leisure-demand-intelligence-v1.mjs --hotel-id recaokE8psIbuVeuH --modules all --mode fixture
```

---

## 11. $0 certification results

`reports/gdi-leisure-demand-intelligence-v1/CERTIFICATION_RESULTS.json` — **all PASS**  
Demonstrates: independent invoke · orchestrator all six · leisure disabled unchanged · canonical contract · cross-segment dedupe · pathways · need-period optional · budget fail-closed · zero-paid replay · no publication/Airtable.  
**Not** live commercial certification.

---

## 12. Proposed first two-hotel live pilot (DO NOT EXECUTE)

**Hotels:** LaLuna (leisure resort) · Memmo Príncipe Real (urban boutique)  
**Modules (3):** cultural/events · attractions/experiences · leisure distribution  
**Why:** Highest Satya-aligned leisure commercial value; strong segment wrappers already exist; clearer partnership/campaign pathways than sports/VFR/destination-shift first.  
**Target:** 5–10 credible leisure opportunities / hotel where evidence supports.  
**Expansion later:** sports · universities/VFR · destination demand shifts.

---

## 13. Estimated research cost (pilot design only)

| Item | Estimate |
|---|---|
| Exa discovery (3 modules × 2 hotels, capped) | **$6–12** typical band (founder ceiling TBD) |
| Deep research slots | Prefer ≤4 candidates / hotel |
| Surfe / Findymail | **$0** (HOLD) |
| Airtable apply | **$0** until separate GO |

Exact ceiling requires separate founder spend phrase — not authorized here.

---

## 14. PR and CI status

| Item | Status |
|---|---|
| PR | [#577](https://github.com/joandejarden-create/my-operators-backend/pull/577) **OPEN** |
| HEAD | `7668be3e267d900e9799d140cd6fdd48ce36c310` |
| Local test | `test:gdi-leisure-demand-intelligence-v1` **PASS** |
| Local ci-precheck | **PASS** |
| Merge | Integration Controller owns merge |

`AWAITING_INTEGRATION_CONTROLLER`

---

## 15. Remaining approvals

1. Merge this PR (IC)  
2. Paid leisure pilot GO (budget + hotels + 3 modules)  
3. Optional Airtable typed leisure fields  
4. Customer-facing leisure publication  
5. Findymail / Surfe (unchanged HOLD)  
6. Weekly automation (forbidden until explicit)

---

## Final acceptance (this phase)

GDI supports **14 research segments + 6 independently executable leisure capabilities** through one orchestration system (`Invoke-GdiPortfolioProcessing.ps1` + LDI registry). Adding the next hotel does **not** require custom leisure programming — presets/heuristics + `-LeisureModules` overrides.

**STOP** — no paid leisure research or customer-facing activation until founder GO.
