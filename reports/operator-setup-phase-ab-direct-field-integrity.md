# Phase A+B — Direct Field Integrity (Operating Model / Management Availability)

## Verdict

**Both statements were true for different reasons.** The audit's 0% Production fill was a **methodology bug**, not empty Airtable data.

## Root cause of audit 0%

`scripts/operator-setup-full-population-audit.mjs` computed Production fill for **every** Setup table by filtering rows whose `Operator` link includes a Production Master ID.

**Master records do not have an `Operator` self-link.** Therefore `prodRows` for Master was **empty** → every Master field reported `productionPopulationPct: 0` (including `company_name` and `Record Purpose`), while `currentPopulationPct` correctly showed ~63% overall fill (29/46).

Live Production fill (this integrity check):

| Field | Field ID | Production filled | Production blank | Fill % |
| ----- | -------- | ----------------: | ---------------: | -----: |
| Operating Model | `fldGg9gr6212S6Zpb` | 28 | 8 | 77.8% |
| Management Availability | `fldcAsoeQvnXWHgk7` | 28 | 8 | 77.8% |

## Canonical fields

| Concept | Canonical table | Field ID | Type |
| ------- | --------------- | -------- | ---- |
| Operating Model | Operator Setup - Master (`tbl4YPJ3XhnYLHLsD`) | `fldGg9gr6212S6Zpb` | singleSelect |
| Management Availability | Operator Setup - Master | `fldcAsoeQvnXWHgk7` | singleSelect |

No duplicate OM/MA fields exist on Setup child tables. Related-but-different fields:

- **Deals**.`Current Operating Model` (`fldEIhePCk5UM0KPP`, singleSelect)
- **Deals**.`F&B Operating Model` (`fldvNfs3q0JXgO7RR`, singleSelect)
- **Strategic Intent - Operational - Key Challenges**.`Preferred Future Operating Model` (`fldPHnI0c0Vd3aN3i`, singleSelect)
- **Strategic Intent - Operational - Key Challenges**.`Operating Model` (`fldFq8o200xSbDV6B`, singleSelect)
- **Company Profile**.`Operating Model` (`fld7LRDMKcEMI9vVm`, singleSelect)
- **Company Profile**.`Third-Party Management Availability` (`fld1JaW1q86G9ZDbh`, singleSelect)
- **Operator Setup - Master**.`Operating Model` (`fldGg9gr6212S6Zpb`, singleSelect)
- **Operator Setup - Master**.`Management Availability` (`fldcAsoeQvnXWHgk7`, singleSelect)
- **Operator Setup - Profile & Positioning**.`primaryServiceModel` (`fldNCwIW4yTxcIpBb`, singleSelect)

## Previous Phase 1 target

Phase 1 (`operator-explorer-phase-1-apply.mjs`) wrote OM/MA to **Master** when present in calibration `entities.json` or `NEW_MASTER_CREATE_PLAN`. Masters that received only `Record Purpose` (e.g. Remington, Brittain, Arriva, OxoHotel, Grupo Marta, Royalton, Grupo Presidente, Tafer) were **not** given OM/MA because they lacked entity OM/MA metadata — explaining the remaining 8 Production blanks.

## Future SoT treatment

- **Operating Model** / **Management Availability** remain **DIRECT Master facts** (company axes).
- Do **not** duplicate onto Profile/Commercial.
- Assignment `Operating / Management Structure` is **property evidence**, not a substitute Master OM.
- Blank OM/MA stays blank until validated classification exists (no inference in Phase A).
