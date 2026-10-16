# Full GDI E2E — Deploy status (incremental)

**Branch:** `fix/gdi-full-e2e-verify-harden`  
**Contract:** `gdi-full-e2e-contract-v1.4`  
**As of:** 2026-10-16

## Standard wrapper

```bash
npm run deploy:production          # clean main only
npm run deploy:production:dry-run  # same gates, no railway up
```

## Verified this pass

| Check | Result |
|-------|--------|
| Wrapper refuses non-`main` branch | **PASS** — `FAIL WRONG_BRANCH_GUARD` on `fix/gdi-full-e2e-verify-harden` |
| Regression `npm run test:gdi-full-e2e-contract` | **PASS** |
| Leak sanitize `npm run test:gdi-customer-provider-leak-sanitize-v3` | **PASS** (local; needs clean-main deploy) |
| Clean-main lean-stage deploy of v1.4 | **PENDING** — merge PR then `npm run deploy:production` |

Do **not** claim production code delivery of delivery-verify / eligibility / leak sanitize until clean-main deploy completes and detail re-scan is clean.

## Production data delivery (Ren — existing writes)

| Check | Result |
|-------|--------|
| Railway opportunities list | HTTP 200 · hotelId match · **count 48** (76th withdrawn) |
| Detail leak scan (pre-deploy) | **FAIL** — 7 legacy facing rows still emit `webhound` / `WEBHOUND_*` / `webhoundUsed` via unsanitized evidence path |
| Canonical SSSP | Present (`gdi_opp_prospect_mvavz7tu_4fc9f013`) |
| Legacy 76th detail | **404** (withdrawn from customer surface) |
| Build identity on production | Not asserted this pass (`requireBuildIdentity: false`) |

Fix is in shared `customer-presentation-sanitize-v3` on this branch; production runtime still pre-fix until deploy.
