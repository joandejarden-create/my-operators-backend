# GDI Three-Way Canonical UI Parity V1

## Critical correction

Prior CLIENT READY certificates compared **share↔share only**. That is insufficient.

This release certifies against the founder comparison:

**A = Local authenticated GDI (customer PACKAGE surface)**  
**B = Local external share**  
**C = Railway external share**

| Pair | Pre-deploy result |
|------|-------------------|
| **A↔B** | **PASS** (0 contract / UI mismatches) |
| **B↔C** | **FAIL until Railway deploy** (production still on prior tip — missing package icon attach) |
| Fingerprint | `441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f` unchanged |

**Verdict before deploy: NOT CERTIFIED** (B↔C open).  
**A↔B architectural unification: verified.**

## What was wrong

Authenticated GDI defaulted to **live-store recompute** (`?live` path). External share used the **immutable PACKAGE**. That produced:

| Symptom | Auth LIVE | Package / Share |
|---------|-----------|-----------------|
| Icons | Gold guestroom (`GUESTROOMS_ONLY`) | Often muted (no attach) |
| Commercial actions | e.g. Build Account | Qualify Opportunity (frozen) |
| Contacts | Fixed to RIC Conference Contact | Same after prior title fix |

Matching two gray share UIs did **not** match the authenticated application the founder approved visually.

## Architecture fix (this release)

1. **Auth Opportunities default to PUBLICATION_PACKAGE** (same attach as share). Research browse: `?live=1`.
2. **Shared package attach** — tile contact + evidence-backed revenue icons + canonical presentation contract.
3. **COMBINED without event thesis** demoted to `GUESTROOMS_ONLY` (RIC → gold guestroom / muted catering — no invented catering).
4. **Permanent** `npm run test:gdi-three-way-canonical-ui-parity` with separate A↔B and B↔C certificates.
5. **Component Difference Register** at `reports/gdi-component-difference-register/`.
6. **Visual baselines** under `fixtures/gdi-canonical-ui-visual-baselines/`.

## Lorna Kipfer / RIC (post-fix A)

- Role: **RIC Conference Contact**
- Icons: guestrooms **active**, eventFnb **muted**
- Sales: **Qualify Opportunity** (package — not live Build Account)

## Live vs package (intentional)

65 opportunities still differ when comparing `?live=1` to PACKAGE (commercial actions / theses). Recorded as `AUTH_LIVE_RECOMPUTE_DIFFERENCE` / **INTENTIONAL**. Publishing those live values requires a **new fingerprint** and founder approval.

## Next step

Merge → deploy merge SHA to Railway → re-run `npm run test:gdi-three-way-canonical-ui-parity` → CLIENT READY only when **A↔B and B↔C both PASS**.
