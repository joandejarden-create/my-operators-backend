# PVQL Failure Scrub

Targeted owner-facing Presentation hygiene (forbidden LOI/FDD/fee-stack language and raw URLs on non-announcement slots).

## Recent Momentum carve-out

`footprint.momentum` and `footprint.openings` **must keep** trailing announcement `https://` URLs. That is the permanent Brand Explorer Recent Momentum template (`brand-explorer-recent-momentum-contract.js`).

`--confirm-no-raw-urls` means: no raw URLs **outside** those slots — not that momentum Bodies are URL-free.

Owner-copy scrubber already peels/restores trailing announcement URLs on those slots.

## Commands

```bash
npm run brand-explorer-pvql-failure-scrub -- --brands comfort-inn-suites,country-inn-suites,hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world,suburban-studios,woodspring-suites --dry-run
npm run brand-explorer-pvql-failure-scrub -- --brands … --apply \
  --approve-pvql-failure-scrub \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-release-field-changes \
  --confirm-no-public-restore-fields \
  --confirm-visible-owner-facing-scrub-only \
  --confirm-no-raw-urls \
  --confirm-no-forbidden-owner-facing-language
```

If momentum URLs were historically stripped, restore with:

```bash
npm run brand-explorer-momentum-announcement-link-restore -- --brands <slugs> --dry-run
```
