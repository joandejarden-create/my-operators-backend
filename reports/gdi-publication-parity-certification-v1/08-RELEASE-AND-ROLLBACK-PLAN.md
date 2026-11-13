# GDI Publication Parity — Release & Rollback Plan

## Compatibility / migration

- Hotels **without** a `CURRENT.json` CUSTOMER_PUBLISHED pointer continue on the legacy share path (Bethesda approved-67 selector, other hotels customerFacing filter).
- Hotels **with** a CUSTOMER_PUBLISHED pointer are served **only** from the immutable package. Broken pointer → HTTP 503 fail-closed (no Airtable substitution).
- Optional global hard mode: `GDI_REQUIRE_APPROVED_PUBLICATION_PACKAGE=1` fails closed for any hotel lacking a valid published package.
- Existing share tokens are preserved; package activation does not rotate tokens.
- Customer Validation / Action / Outcome feedback remains in the share-validation store and is merged at serve time — never written into the package.

## Release sequence (every hotel)

1. Build candidate package: `npm run gdi:build-customer-publication-package -- --hotel <id> --stage`
2. Run quality + parity gates; fix generation defects until quality passes (parity ≠ quality).
3. Founder approval receipt must cite exact `publicationId`, `publicationVersion`, and `contentFingerprintSha256`.
4. Deploy code that includes the package store + share resolver.
5. On production volume / deploy bundle: stage then `publishCustomerVisible` only after fingerprint match.
6. Independent production verification via public Railway share URL (API + browser).
7. Only then mark client-ready.

## Rollback / withhold

- `withholdCustomerPublication(hotelId)` flips CURRENT to WITHHELD without deleting package bytes or feedback.
- Prior package versions remain on disk for re-pointing.
- Do **not** delete Airtable opportunity records or feedback to “fix” parity.

## Bethesda first application (this wave)

- Candidate staged locally: `gdi_pub_recLuxvwwxID7U2B8_20261113_candidate`
- Fingerprint: `807b74be1e87411cf8ccf71e875b309a4fe21f1b62ef263c0ebcafc7783e7fdf`
- Inventory: All 67 / Hotel 61 / Competitive 10
- **Not** CUSTOMER_PUBLISHED — quality gate still blocking; founder fingerprint approval required after quality remediation + production verify.
