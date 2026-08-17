# Brand Explorer Radisson Individuals Opening Asset Approval Reconciliation v31M-R1

- Generated: 2026-07-10T23:23:09.209Z
- Brand: **Radisson Individuals by Choice** (`recRyvM8OmLlDj9G7`)
- v31M-R1 exists: **yes**
- Mode: **apply**

## Why approved images were not recognized before

Prior v31L/v31M writers used listRegistryAssetsForBrand (slim normalize) which omits Source Notes and Attachment URL — findDedicatedRegistry could not match v31L per-row registry rows. Multiple rows also shared one approved registry (rec1Mi8rEyewVTpS0) without per-row sourceNotes linkage, so dedicated-registry gates failed even though founder-approved duplicates exist.

- Slim registry normalize omitted sourceNotes and attachmentUrl used by findDedicatedRegistry
- Shared approved registry rec1Mi8rEyewVTpS0 linked to Bogotá/Cúcuta/Cali without per-row dedicated match
- v31L-created per-property registry rows exist as duplicates but were not chosen as canonical
- Reactivation required dedicated approved registry + materialized presentation image — both missing

## Duplicate registry groups

- Groups with 2+ assets: **9**

- `row:recVtiPqVGo8gUtpO|footprint.openings|https://cdn.prod.website-files.com/6584b78de41b4e29c2c2da03/665df9a633a3d91ce5d0`: 2 assets (rec1Mi8rEyewVTpS0, recaqOc53wLJQzDki)
- `row:recM0XfO2UlkNBd5x|footprint.openings|`: 2 assets (rec2OCD59EQNT4gBY, reckYX49nKaMzdcPM)
- `row:recFKCA1auFtGwwjY|footprint.openings|`: 2 assets (rec8U7z3rqNzb04FE, recSXXiJ0K7BLh9Xk)
- `prop:radisson individuals choice press kit source reference|footprint.openings|`: 2 assets (rec9B2pa235RNmWSV, recCF1BFEtJ50yMZG)
- `row:recto7QMu58eMf5jV|footprint.openings|https://cdn.prod.website-files.com/6584b78de41b4e29c2c2da03/665ca4ed2355dcc157a1`: 2 assets (recCJDhlwtXnWPr5q, recZjy0knVL4e2176)
- `row:rec0uiWsD44ePqr6M|footprint.openings|`: 2 assets (recDvHn96m3f3zGoM, reclgP15p3xIdSsro)
- `row:recLHEhgtaFWGjACc|footprint.openings|`: 3 assets (recExoTwc5RaaRSYl, recVCaKWa3bLeNLrR, recj72LAVVbMJeTvx)
- `row:rect0VNHSr1f5ImGx|footprint.openings|`: 3 assets (recF1GaBd9m0zll4n, recJOgTBdNrDt9TrV, recqC6rciqpLCF0Rt)
- `row:recA57HKv0Zd2bGnx|footprint.openings|https://cdn.prod.website-files.com/6584b78de41b4e29c2c2da03/66bd38158d08d40378c9`: 2 assets (recNTDXEHzfdakzw4, recpOeV10h0jpYC95)

## Canonical asset per opening row

- **Barranquilla Individuals context** (`rec0uiWsD44ePqr6M`) → canonical `none` (6 candidates)
- **Hotel Casa Don Luis by Faranda Boutique** (`recA57HKv0Zd2bGnx`) → canonical `recNTDXEHzfdakzw4` (6 candidates)
- **Panama City Individuals context** (`recFKCA1auFtGwwjY`) → canonical `none` (6 candidates)
- **Panama corridor Individuals context** (`recLHEhgtaFWGjACc`) → canonical `none` (9 candidates)
- **Medellín Individuals context** (`recM0XfO2UlkNBd5x`) → canonical `none` (6 candidates)
- **Hotel Faranda Bolivar Cucuta** (`recVtiPqVGo8gUtpO`) → canonical `rec1Mi8rEyewVTpS0` (6 candidates)
- **Cali Individuals context** (`rect0VNHSr1f5ImGx`) → canonical `none` (6 candidates)
- **Faranda Collection Bogota** (`recto7QMu58eMf5jV`) → canonical `recCJDhlwtXnWPr5q` (6 candidates)

## Opening row audit

### Radisson Individual — Barranquilla, Colombia
- Record: `rec0uiWsD44ePqr6M`
- Status: Show Trust Label · Image: materialized · Visible: true · Complete: true
- Registry: legacy `rec1Mi8rEyewVTpS0` · canonical `none`
- Reason: already_complete_visible

### Radisson Individuals — Cartagena, Colombia
- Record: `recA57HKv0Zd2bGnx`
- Status: Do Not Display · Image: empty · Visible: false · Complete: false
- Registry: legacy `rec1Mi8rEyewVTpS0` · canonical `recNTDXEHzfdakzw4`
- Reason: eligible_pending_apply

### Radisson Individuals — Panama City, Panama
- Record: `recFKCA1auFtGwwjY`
- Status: Do Not Display · Image: empty · Visible: false · Complete: false
- Registry: legacy `rec1Mi8rEyewVTpS0` · canonical `none`
- Reason: no_dedicated_approved_registry

### Radisson Individual — Panama, Panama
- Record: `recLHEhgtaFWGjACc`
- Status: Do Not Display · Image: empty · Visible: false · Complete: false
- Registry: legacy `rec1Mi8rEyewVTpS0` · canonical `none`
- Reason: no_dedicated_approved_registry

### Radisson Individuals — Medellín, Colombia
- Record: `recM0XfO2UlkNBd5x`
- Status: Do Not Display · Image: empty · Visible: false · Complete: false
- Registry: legacy `rec1Mi8rEyewVTpS0` · canonical `none`
- Reason: no_dedicated_approved_registry

### Radisson Individual — Cucuta, Colombia
- Record: `recVtiPqVGo8gUtpO`
- Status: Do Not Display · Image: empty · Visible: false · Complete: false
- Registry: legacy `rec1Mi8rEyewVTpS0` · canonical `rec1Mi8rEyewVTpS0`
- Reason: eligible_pending_apply

### Radisson Individual — Cali, Colombia
- Record: `rect0VNHSr1f5ImGx`
- Status: Do Not Display · Image: empty · Visible: false · Complete: false
- Registry: legacy `rec1Mi8rEyewVTpS0` · canonical `none`
- Reason: no_dedicated_approved_registry

### Radisson Individual — Bogota, Colombia
- Record: `recto7QMu58eMf5jV`
- Status: Do Not Display · Image: empty · Visible: false · Complete: false
- Registry: legacy `rec1Mi8rEyewVTpS0` · canonical `recCJDhlwtXnWPr5q`
- Reason: eligible_pending_apply

## Rows to materialize

- `recVtiPqVGo8gUtpO` **Radisson Individual — Cucuta, Colombia** via `rec1Mi8rEyewVTpS0`
- `recto7QMu58eMf5jV` **Radisson Individual — Bogota, Colombia** via `recCJDhlwtXnWPr5q`

## Rows to reactivate

- `recVtiPqVGo8gUtpO` **Radisson Individual — Cucuta, Colombia** (preferred)
- `recto7QMu58eMf5jV` **Radisson Individual — Bogota, Colombia** (preferred)

## Rows kept hidden

- `recFKCA1auFtGwwjY` — no_dedicated_approved_registry
- `recLHEhgtaFWGjACc` — no_dedicated_approved_registry
- `recM0XfO2UlkNBd5x` — no_dedicated_approved_registry
- `rect0VNHSr1f5ImGx` — no_dedicated_approved_registry

## Momentum apply status

- v31M fully applied: **yes**
- v31M momentum apply has landed — titles and property-specific URLs present.
- `rec0an5blfW4FtMfE`: applied — live title: "Radisson Individuals Expands Across CALA"
- `recb0WzRRu6jrev4c`: applied — live title: "Colombia Urban and Heritage Markets Add Individuals Properties"
- `recpIgmBNBEMXVEda`: applied — live title: "Panama Capital Corridor Extends Individuals Reach"

## Governance

- Images newly approved: **no**
- Company Validated untouched: **yes**
- Airtable modified: **yes**
- Dry-run clean: **yes**

## Expected UI result

- 1 complete visible (need 3) → 3 projected complete visible if materialization succeeds

## Exact apply command

```bash
npm run brand-explorer-radisson-individuals-opening-asset-approval-reconciliation-writer -- --brand radisson-individuals-by-choice --apply --approve-brand-explorer-v31M-R1-opening-asset-approval-reconciliation --confirm-founder-approved-opening-assets-already-reviewed --confirm-approved-assets-only --confirm-no-company-validation-claim
```
