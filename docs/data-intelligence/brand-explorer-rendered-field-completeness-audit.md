# Rendered Field-by-Field Completeness Audit

True tab-by-tab / field-by-field audit of the **live rendered** Brand Explorer profile for:

- `hotel-indigo`
- `mgallery-collection`
- `small-luxury-hotels-of-the-world`

This does **not** treat “rows exist” or “tab renders” as complete. It inspects the Brand Library payload + atelier HTML field map.

## Audit (no writes)

```bash
npm run brand-explorer-rendered-field-completeness-audit -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run
```

Reports:

- `reports/brand-explorer-rendered-field-completeness-audit.json`
- `reports/brand-explorer-rendered-field-completeness-audit.md`
- `reports/brand-explorer-rendered-field-completeness-hotel-indigo.md`
- `reports/brand-explorer-rendered-field-completeness-mgallery.md`
- `reports/brand-explorer-rendered-field-completeness-slh.md`

Release-quality decision per brand:

- `field_complete` — `failFindings === 0` (`auditPass=true`)
- `field_complete_after_patch` — fails remain but every fail has a resolution/patch path (`patchPlanComplete=true`, **`auditPass=false`**)
- `not_field_complete` — fails without a resolution path

### Audit flags

- `auditComplete` — audit ran
- `patchPlanComplete` — every defect has a proposed fix / intentional handling
- `auditPass` — **`failFindings === 0`** after remediation (never true when fails remain)

The audit CLI exits `2` when the patch plan is incomplete, and `3` when `auditPass` is false (release gate not met).

These gates are **mandatory** before `founder_review_ready` / `active_profile_ready`. See `docs/data-intelligence/brand-explorer-mandatory-release-gates.md`.

## Remediation (separate apply)

```bash
npm run brand-explorer-rendered-field-completeness-remediation -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --dry-run
```

Apply (after founder review of the audit):

```bash
npm run brand-explorer-rendered-field-completeness-remediation -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world --apply \
  --approve-rendered-field-completeness-remediation \
  --confirm-keep-active-profile-ready \
  --confirm-no-company-validation-changes \
  --confirm-no-release-field-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-protected-brands-unchanged \
  --confirm-no-visible-empty-fields \
  --confirm-no-unsupported-metrics \
  --confirm-no-empty-bullets \
  --confirm-no-blank-cards \
  --confirm-no-duplicate-scenario-images \
  --confirm-brand-specific-copy
```

Allowed writes: Presentation Title/Body/Case Summary/chips; Basics display fields only when needed; image reassignment from existing inventory.

Forbidden: Company Validated, release/active-profile approval fields, Source Library status, Registry approval, protected brands.

## Test

```bash
npm run test:brand-explorer-rendered-field-completeness -- --brands hotel-indigo,mgallery-collection,small-luxury-hotels-of-the-world
```

## Change impact

**High** when remediation is applied (live Presentation writes). Audit-only is **Medium** (read path).

Rollback: restore Presentation rows from Airtable history / pre-apply remediation JSON.
