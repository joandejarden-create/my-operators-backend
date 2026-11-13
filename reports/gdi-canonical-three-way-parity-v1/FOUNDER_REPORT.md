# GDI Three-Way Canonical UI Parity V1

**Verdict: CLIENT READY**

## Mandatory acceptance

| Check | Result |
|------|--------|
| **A.** Local authenticated customer (PACKAGE) ↔ Local share | **PASS** |
| **B.** Local share ↔ Railway production share | **PASS** |
| **C.** Approved publication package ↔ Production customer content | **PASS** — fingerprint `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f` |
| **D.** GitHub main ↔ Railway live tip | **PASS** — tip SHA `3645dacea98ae954521df90b5fb2b54ae65dd416` · Railway `83d649b1-3228-4d3d-b016-f822c04e6c51` · marker `gdi-three-way-canonical-v1` |

Share↔share alone is **not** sufficient. This certificate required **A↔B and B↔C**.

## Architecture

Authenticated GDI Opportunities default to the **approved publication package** (same attach as external share). Research browse: `?live=1`.

Shared modules: package attach, tile contact, evidence-backed revenue icons, canonical customer presentation contract.

## Lorna Kipfer / RIC (customer PACKAGE)

| Field | Value |
|------|-------|
| Role | **RIC Conference Contact** |
| Icons | guestrooms **active**, eventFnb **muted** |
| Class | `GUESTROOMS_ONLY` |
| Sales | **Qualify Opportunity** |

## Live vs package (intentional)

65 opportunities differ under `?live=1` vs PACKAGE. Recorded as `AUTH_LIVE_RECOMPUTE_DIFFERENCE` / **INTENTIONAL**. New fingerprint required to publish live values.

## Release identity

| Item | Value |
|------|-------|
| Presentation PR | #258 → `372b4e1a67ae7cd6124df54579cb066f9540eda9` |
| Certificate PR | #259 → `3645dacea98ae954521df90b5fb2b54ae65dd416` |
| Presentation Railway deploy | `4f25490e-8b6b-445e-9b52-d56b854530d9` |
| Certificate tip Railway deploy | `83d649b1-3228-4d3d-b016-f822c04e6c51` ([GHA 43445731749](https://github.com/joandejarden-create/my-operators-backend/actions/runs/43445731749)) |
| Inventory | 67 / 61 / 10 |
| HTML marker (A/B/C) | `gdi-three-way-canonical-v1` |

## Remaining intentional differences

- Auth `?live=1` vs PACKAGE (not customer certification baseline).
- Auth-only permission surfaces (feedback admin / research supervisor).

## Reports

`01`–`12` · permanent register `reports/gdi-component-difference-register/COMPONENT_DIFFERENCE_REGISTER.json` · visuals under `visual-baselines/` and `fixtures/gdi-canonical-ui-visual-baselines/`.
