# Local / External Parity and Publication

## YOTEL reference (CONFIRMED)

Prior incident: local 22 vs staged 42 → reconciliation → certification gate → CUSTOMER_PUBLISHED + share.

Current CURRENT.json:

- hotelId `recrPQcZg7SFARRb2`
- fingerprint `353430092ceea5626c85e04bffbeeb84d0738bf7a5382c5177d5fdf37413eba3`
- lifecycle `CUSTOMER_PUBLISHED`
- inventoryCounts `{ all: 42, hotel: 34, competitive: 8 }`

Modules: `gdi-local-external-customer-link-activation-gate-v1.js`, `scripts/gdi-yotel-geneva-external-client-publication-parity-v1.mjs`, activate script `gdi-activate-yotel-geneva-35343009-approved`.

## Other hotels (CONFIRMED CURRENT)

| Hotel | ID | Count (package) | Lifecycle |
|-------|-----|----------------:|-----------|
| Bethesda Marriott | recLuxvwwxID7U2B8 | 67 | CUSTOMER_PUBLISHED |
| Radisson Blu Toronto | recH9h3McgMNFQEla | 66 | CUSTOMER_PUBLISHED |
| YOTEL Geneva Lake | recrPQcZg7SFARRb2 | 42 | CUSTOMER_PUBLISHED |

## Strict parity requirement

Same approved snapshot: version, fingerprint, IDs, counts, Hotel/CMI split, titles, orgs, segments, stages, contacts, evidence, drawers, actions, weekly classifications, publication state.

Authorized differences: auth, permissions, internal-only metadata.

## Remaining risks (LIKELY)

1. Other hotels without YOTEL-strength activation gate enforcement.  
2. Local checkout behind `origin/main` serving stale packages (operational — primary sync discipline PR #344).  
3. Share token scope mismatches.  
4. Drawer truncation regressions (Radisson history).

## Prevention

Keep fingerprint + package resolution + browser/API parity tests per hotel before share issuance. Do not publish staged without founder phrase.

## Required result

Parity architecture is **credible for YOTEL**; treat as the template and harden regression for Bethesda/Radisson/Wyndham rather than inventing a second publication system.
