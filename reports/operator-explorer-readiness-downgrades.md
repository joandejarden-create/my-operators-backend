# Operator Explorer — Readiness Downgrades

**Downgrade events:** 16 operators

## GHL Hoteles (GHL Holding)

- Previous: **Strong Profile** (publishable=true, strong=true)
- Current: **Useful Profile** (publishable=true, strong=false)
- Failed gate(s): Phase1 Strong requires asg>=8 (have 6); dry-run Strong at asg>=5 + countries>=2 + brands>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/reciI2tYQBfMoMK9G.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 6→6 (held 0), mp 7, br 5, countries 7, brands 5
- Dry-run rules on Airtable data would yield: **Strong Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Aimbridge Hospitality (LATAM)

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires mp rows>=2 (have 1); dry-run used distinct countries>=1
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/recGWxIJqnYHkJZFD.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 6→6 (held 0), mp 1, br 6, countries 1, brands 6
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Playa Hotels & Resorts

- Previous: **Strong Profile** (publishable=true, strong=true)
- Current: **Useful Profile** (publishable=true, strong=false)
- Failed gate(s): Phase1 Strong requires asg>=8 (have 6); dry-run Strong at asg>=5 + countries>=2 + brands>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/rec3TUHT9Z4AnFp5P.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 6→6 (held 0), mp 3, br 4, countries 3, brands 4
- Dry-run rules on Airtable data would yield: **Strong Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Grupo Hotelero Santa Fe

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 2); dry-run Useful at asg>=2; Phase1 Useful requires mp rows>=2 (have 1); dry-run used distinct countries>=1
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/reckyv9O0Y3auYpJJ.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 2→2 (held 0), mp 1, br 2, countries 1, brands 2
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Driftwood Hospitality Management

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 4); dry-run Useful at asg>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/recKVILWcRLqrQlWs.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 4→4 (held 0), mp 4, br 0, countries 4, brands 0
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Atlantica Hotels International (AHI)

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 0); dry-run Useful at asg>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/recfwDdU5t9h4uFnZ.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 2→0 (held 2), mp 2, br 1, countries 2, brands 1
- Dry-run rules on Airtable data would yield: **Not Publishable** (pub=false)
- Classification: **Aggregate holdout consequence**
- Intentional? Partially (holds intentional; readiness impact side-effect)
- Material? **Yes** — changes Explorer Publishable / Strong

## Cenote Azul Operadores

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 4); dry-run Useful at asg>=2; Phase1 Useful requires mp rows>=2 (have 1); dry-run used distinct countries>=1
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/recQ6Cf8O2z0tiqBz.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 4→4 (held 0), mp 1, br 0, countries 1, brands 0
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Hyatt (Managed)

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 3); dry-run Useful at asg>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/provisional_operator_hyatt.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 3→3 (held 0), mp 3, br 2, countries 3, brands 2
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Sonesta International

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 1); dry-run Useful at asg>=2; Phase1 Useful requires mp rows>=2 (have 1); dry-run used distinct countries>=1
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/provisional_operator_sonesta.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 2→1 (held 1), mp 1, br 1, countries 1, brands 1
- Dry-run rules on Airtable data would yield: **Thin Profile** (pub=false)
- Classification: **Aggregate holdout consequence**
- Intentional? Partially (holds intentional; readiness impact side-effect)
- Material? **Yes** — changes Explorer Publishable / Strong

## Four Seasons Hotels and Resorts

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 3); dry-run Useful at asg>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/provisional_operator_four_seasons.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 3→3 (held 0), mp 3, br 1, countries 3, brands 1
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Rosewood Hotel Group

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 2); dry-run Useful at asg>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/provisional_operator_rosewood.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 2→2 (held 0), mp 2, br 1, countries 2, brands 1
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Mandarin Oriental Hotel Group

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 2); dry-run Useful at asg>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/provisional_operator_mandarin_oriental.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 2→2 (held 0), mp 2, br 1, countries 2, brands 1
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Meliá Hotels International

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 3); dry-run Useful at asg>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/provisional_operator_melia.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 3→3 (held 0), mp 3, br 1, countries 3, brands 1
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Auberge Resorts Collection

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 2); dry-run Useful at asg>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/provisional_operator_auberge.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 2→2 (held 0), mp 2, br 1, countries 2, brands 1
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Shangri-La Group

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 2); dry-run Useful at asg>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/provisional_operator_shangri_la.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 2→2 (held 0), mp 2, br 1, countries 2, brands 1
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Barceló Hotel Group

- Previous: **Useful Profile** (publishable=true, strong=false)
- Current: **Thin Profile** (publishable=false, strong=false)
- Failed gate(s): Phase1 Useful requires asg>=5 (have 2); dry-run Useful at asg>=2
- Dry-run source: `data/operator-explorer/calibration-01/profile-payloads/provisional_operator_barcelo.json` via `buildProfile` in `scripts/build-operator-explorer-calibration-01.mjs`
- Airtable source: `scripts/operator-explorer-phase-1-apply.mjs` generateAirtablePayloads thresholds
- Counts local→air: asg 2→2 (held 0), mp 2, br 1, countries 2, brands 1
- Dry-run rules on Airtable data would yield: **Useful Profile** (pub=true)
- Classification: **Readiness-rule inconsistency**
- Intentional? No — implementation inconsistency
- Material? **Yes** — changes Explorer Publishable / Strong

## Cause tallies (downgraded operators)

- Readiness-rule inconsistency: 14
- Aggregate holdout consequence: 2
