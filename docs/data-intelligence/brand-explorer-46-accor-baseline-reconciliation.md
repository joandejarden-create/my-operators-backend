# Brand Explorer — Protected 46 Accor baseline reconciliation

> Durable note: Wave 13 Accor Active/Live identity must not depend on factory-preview candidate maps (Wave 14+).

## Rule

After a wave leaves factory preview and is promoted Active/Live, keep durable
`wave13-active-identity-anchors-v1` anchors in
`lib/partner-intelligence/brand-explorer-wave13-active-identity-anchors.js`, wired through
`EXTRA_ACTIVE_IDENTITY_ANCHORS` / `resolveActiveUniverseRecordId`.

Aliases that must resolve to the same record id:

- `fairmont` ↔ `fairmont-hotels-and-resorts` → `recJhPaDVU3YUDQUt`
- `so` ↔ `so-hotels-and-resorts` → `recTJdPlr4mDs9app`

## Secondary code fix (Mercure)

Airtable CDN attachment path hashes can falsely match Accor DAM `wd*` type tokens
(`…_wDc2BQ…_7` → `meeting_event`). That flipped Mercure to
`draft_applied_with_defects` / not public-full after identity resolve worked.
Fix: skip loose Accor DAM regex on `airtableusercontent.com` and cap type-token length
in `brand-explorer-image-role-match.js`. No Presentation/image writes.

## Symptoms of regression

- `quality_freeze_count:40_expected_46`
- PVQL / footnote `brand_not_found` on Mama Shelter, Mercure, Novotel, Pullman, Fairmont, SO/
- Stale 24-tab quality `remediation_required` with sole blocker `PVQL lockPass=false`

## Fix class

Resolver / identity / detector / report refresh only — **not** Accor content patches, **not** Wave 14.

Ready token: `protected_46_accor_baseline_reconciled_wave14_may_resume`

