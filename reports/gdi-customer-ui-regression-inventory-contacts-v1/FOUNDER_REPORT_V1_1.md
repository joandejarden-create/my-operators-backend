# GDI Customer UI Regression V1.1 — Founder Report

**Date:** 2026-11-23  
**PR:** [#575](https://github.com/joandejarden-create/my-operators-backend/pull/575)  
**Decision:** Targeted fix approved — merge after green  
**Spend / Airtable / publication / deploy:** none  

---

## 1. CI failure root cause and correction

**Failure:** `GDI recovery regression suite` → `html_cache_marker_architecture_v1`

**Root cause:** `scripts/test-gdi-canonical-ui-architecture-offline-v1.mjs` asserted a **frozen** cache string `dealality-gdi-ui.js?v=gdi-no-login-alert-v1`. PR #575 correctly bumped the auth shell to `gdi-customer-ui-regression-inventory-contacts-v1`, so the architecture gate failed on a stale version literal — not on missing architecture.

**Correction:** Assert real wiring — both auth and share HTML load `/js/group-demand-intelligence/dealality-gdi-ui.js?v=<non-empty-marker>` and reject uncached loads. Cache markers may change when customer JS ships.

Also aligned share + auth to `?v=gdi-customer-ui-regression-v1-1` and updated dependent empty-state tests that still pinned the V1 marker.

---

## 2. Contact authorization flag semantics

### Historical meaning of `customerFacingNamed`

| Value | Contract meaning |
|---|---|
| `true` | Verified named person **authorized** for customer-facing Who-to-Contact / tile presentation |
| `false` | **Not** a customer-facing named contact — org-route gap, research gap, demoted placeholder, or non-customer authorization |

Missing email alone does **not** clear `customerFacingNamed` in the scaling two-contact standard (named people stay `true` without personal channels).

### V1.1 display authorization (`classifyCustomerContactDisplayAuth`)

| Auth class | Display name? | Preserve internally? |
|---|---|---|
| `AUTHORIZED_NAMED` | yes | yes |
| `VERIFIED_NAME_NO_CHANNEL` | yes (name + title) | yes |
| `INTERNAL_ONLY` (`customerFacingNamed===false` + name, no explicit restrict) | **no** | yes |
| `ORGANIZATION_ROUTE` | no (show org entry) | n/a |
| `RESTRICTED` (explicit suppress / visibility flags) | **no** | yes |
| `MISSING` | Contact Research Required | no |

**Explicit restriction signals:** `suppressCustomerName`, `suppressCustomerDisplay`, `customerFacingAuthorized===false`, `customerVisibility` ∈ `{RESTRICTED,HIDDEN,SUPPRESSED,INTERNAL_ONLY,…}`, `doNotDisplayName`, `publicationNameBlocked`.

Gap-label pollution fix preserved: `tileContactPreview.displayName = "Contact Research Required"` never overwrites a real `primaryContact.name`.

Sales Ready / Market Scope **unchanged**.

---

## 3. Named-contact before/after verification

| Surface | Result |
|---|---|
| Gap label vs primaryContact | Daniel Roy / Teddy Oberts still recover — **PASS** |
| Authorized named, no email | Displays — **PASS** |
| Explicit restricted name | Suppressed on tile + canonical contract — **PASS** |
| Incomplete verification (`customerFacingNamed false` + name) | Internal only; not customer-displayed — **PASS** |
| Genuine missing | Contact Research Required — **PASS** |
| Bethesda reconciliation/remediated packages (67) | **67/67** named presentation; **33** named with no email still visible; **0** restricted leaks |
| Bethesda research store (86) | **72** named preserved; **14** genuine gaps; **0** gap-label overwrites of person names |
| Wyndham sample | No falsely recovered / invented names |
| Cambridge | Research inventory present; staged/current still fail-closed |

---

## 4. Cambridge empty-state result

- Research opportunities remain (≥1; local store still present).
- `hasStagedPublication=false`, `hasCurrentPublication=false` — unpublished inventory still blocked.
- Customer empty copy no longer says “staged inventory / staged version”.
- View Research recovery control retained for authorized users.
- No auto-publish.

---

## 5. Tests and CI status (local)

| Check | Result |
|---|---|
| `test:gdi-canonical-ui-architecture-offline` | PASS (incl. fixed cache architecture) |
| `test:gdi-customer-ui-regression-inventory-contacts-v1` | PASS (+ restriction / auth cases) |
| `test:gdi-missing-inventory-empty-state-v1` | PASS |
| `test:gdi-founder-preview-missing-staged-nav-v1` | PASS |
| `test:gdi-opportunity-card-quality-v1` | PASS |
| `test:gdi-market-scope-toggle-v1` | PASS |
| Full GDI recovery suite scripts | PASS |
| Card dedupe | By stable opportunity **id** only (no title/org collapse) |

Await GitHub Actions green on the updated PR head.

---

## 6. Final PR HEAD and merge readiness

- Worktree: `C:\Users\joand\AppData\Local\Temp\dealality-gdi-ui-regression-v1-1-20261123`
- Branch pushed to update **PR #575** head (`feat/gdi-customer-ui-regression-inventory-contacts-v1`)
- Dirty `ws-gdi` preserved
- Integration Controller owns merge
- **Merge readiness:** READY after GitHub CI green on latest HEAD

**STOP** if CI fails or authorization protections cannot be verified. Otherwise hand **PR #575** to Integration Controller.
