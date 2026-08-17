# Company Profile — Owner-Operator field backfill

**Date:** 2026-06-04  
**Table:** Company Profile (`tblItyfH6MlOnMKZ9` or `COMPANY_PROFILE_TABLE_ID`)

## What this fills (empty columns only)

| Airtable field | Source |
|----------------|--------|
| Company Type | Normalizes bad values (e.g. `owner_operator` → `Hotel Owner - Operator`) |
| Company Type Tags | Company Type + ecosystem role + overview keywords |
| Workspace Access | Derived tags / `normalizeWorkspaceAccess` |
| Operating Model | Tags + company type defaults |
| Third-Party Management Availability | Tags + operating model rules |
| Core / Owner / Operator / Developer Profile Status | `applyProfileStatusDefaults` (never overwrites Complete / Needs Review) |
| Potential Conflict Flags | `To Be Reviewed` when empty |

**Not auto-filled:** Competitive Sensitivity Notes (no reliable inference).

**Not changed:** Company Type when already a valid Airtable option (e.g. Arbor remains Hotel Management Company until you manually set **Hotel Owner - Operator**).

## Commands

```bash
# Audit — see empty counts and proposed patches per company
node scripts/audit-company-profile-owner-operator-fields.mjs
node scripts/audit-company-profile-owner-operator-fields.mjs --json=reports/company-profile-oo-fields-audit.json

# Dry-run (default)
node scripts/backfill-company-profile-owner-operator-fields.mjs

# Write to Airtable (batches of 10, typecast: false)
node scripts/backfill-company-profile-owner-operator-fields.mjs --apply

# Single company
node scripts/backfill-company-profile-owner-operator-fields.mjs --apply --id recXXXXXXXX
```

## Code

- `lib/company-profile-owner-operator-backfill.js` — inference rules
- `lib/company-profile-owner-operator-fields.js` — finalize + exact select labels
- `scripts/test-company-profile-owner-operator-backfill.mjs` — unit checks

## Manual follow-up (recommended)

After bulk backfill, review high-confidence **Owner-Operator** reclassifications in `reports/owner-operator-backfill-candidates.md` (e.g. Arbor Lodging, HE/CALA) and set **Company Type** = `Hotel Owner - Operator` where appropriate, then re-run backfill for those rows if needed.

## Apply run (2026-06-04)

- **77 / 77** Company Profile rows updated via `node scripts/backfill-company-profile-owner-operator-fields.mjs --apply`
- Log: `reports/company-profile-oo-fields-backfill-log.json`
- **Skipped column (not in Airtable yet):** `Potential Conflict Flags` — create the field in Airtable, then re-run backfill to populate `To Be Reviewed` defaults.
- **Left empty by design:** `Competitive Sensitivity Notes` (no inference); **Operating Model** / **Third-Party Management Availability** for many **Brand-only** rows until you add business rules.

## Post-apply audit notes

Re-run `node scripts/audit-company-profile-owner-operator-fields.mjs`:

- **Company Type Tags** and **Workspace Access** — filled for almost all rows (1 workspace gap — review in Airtable).
- **Potential Conflict Flags** — still show as empty in audit until the Airtable column exists.
- **Owner / Operator / Developer Profile Status** — may remain empty on **Brand-only** companies (only **Core Profile Status** was set).

## Audit snapshot (before apply)

- **77** companies in base
- **77** had empty extension fields (new columns)
- **0** had empty Company Type (legacy type already set)
