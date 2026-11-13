# GDI Three-Way Canonical UI Parity V1

**Verdict: CLIENT READY**

## Mandatory acceptance

| Check | Result |
|------|--------|
| **A.** Local authenticated customer (PACKAGE) ↔ Local share | **PASS** (0 contract / UI mismatches) |
| **B.** Local share ↔ Railway production share | **PASS** (0 contract / UI mismatches) |
| **C.** Approved publication package ↔ Production customer content | **PASS** fingerprint `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f` |
| **D.** GitHub main presentation tip ↔ Railway live tip | **PASS** — HTML marker `gdi-three-way-canonical-v1` + package attach with canonical contract on Railway deploy `4f25490e-8b6b-445e-9b52-d56b854530d9` (merge `372b4e1a67ae7cd6124df54579cb066f9540eda9`) |

Share↔share alone is **not** sufficient. This certificate required **A↔B and B↔C**.

## What was wrong

Authenticated GDI defaulted to live-store recompute while external share used the immutable PACKAGE. Matching two gray share UIs did not match the authenticated customer presentation the founder approved.

## Architecture fix

1. Auth Opportunities default to **PUBLICATION_PACKAGE** (same attach as share). Research browse: `?live=1`.
2. Shared package attach — tile contact + evidence-backed revenue icons + canonical presentation contract.
3. COMBINED without event thesis demoted to `GUESTROOMS_ONLY` (no invented catering gold).
4. Permanent `npm run test:gdi-three-way-canonical-ui-parity` with separate A↔B and B↔C certificates.
5. Historical OPEN register rows retained and marked **RESOLVED** when re-verify matches (never deleted).

## Lorna Kipfer / RIC (customer PACKAGE)

| Field | Value |
|------|-------|
| Role | **RIC Conference Contact** (canonical evidence / package — not inferred Group Travel Coordinator) |
| Icons | guestrooms **active**, eventFnb **muted** |
| Class | `GUESTROOMS_ONLY` |
| Sales | **Qualify Opportunity** (frozen package) |

## Live vs package (intentional)

65 opportunities differ under `?live=1` vs PACKAGE (commercial actions / theses). Recorded as `AUTH_LIVE_RECOMPUTE_DIFFERENCE` / **INTENTIONAL**. Publishing those live values requires a **new fingerprint** and founder approval. Fingerprint unchanged in this release.

## Release identity

| Item | Value |
|------|-------|
| Presentation merge SHA | `372b4e1a67ae7cd6124df54579cb066f9540eda9` (PR #258) |
| GitHub Actions deploy | [43442332414](https://github.com/joandejarden-create/my-operators-backend/actions/runs/43442332414) |
| Railway deployment | `4f25490e-8b6b-445e-9b52-d56b854530d9` **SUCCESS** |
| Inventory | 67 / 61 / 10 |
| HTML marker (A/B/C) | `gdi-three-way-canonical-v1` |

Note: lean Railway uploads may leave `/health` `buildSha` stale; live identity for this product is the HTML cache marker + package attach fields.

## Remaining intentional differences

- Auth `?live=1` research browse vs customer PACKAGE (documented; not customer-facing certification baseline).
- Permission-only surfaces (feedback admin / research supervisor) remain auth-only by design.

## Reports

`01`–`12` in this directory · permanent register: `reports/gdi-component-difference-register/COMPONENT_DIFFERENCE_REGISTER.json` · visual baselines under `visual-baselines/` and `fixtures/gdi-canonical-ui-visual-baselines/`.
