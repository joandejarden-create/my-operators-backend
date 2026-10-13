# Changelog — Certification Inventory Reconciliation

## Added
- `adp-status-dimensions-v1.js` — orthogonal status enums + resolvers
- `adp-provider-completeness-policy-v1.js` — documented completeness policy + provider-level imbalance guard (same 10%/floor-2)
- `adp-certification-inventory-v1.js` — canonical inventory builder + count assertions
- Report pack under `reports/adp/certification-inventory-reconciliation/`

## Changed
- `certify-adp-period-v1.js` delegates completeness to policy module; review flags include provider imbalance details
- Prior founder pack wording: remove combined CERTIFIED/PASS label

## Not changed
- ADP methodology / scoring thresholds
- Historical period observation corpora
- No silent retro-certification of legacy periods
- YOTEL baseline left CERTIFIED (recheck pass)
