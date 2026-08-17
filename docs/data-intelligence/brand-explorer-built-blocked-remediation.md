# Built-but-Blocked Tab Factory Remediation

Field-level remediation for the 7 active brands that had Presentation depth but failed Tab Factory / PVQL gates (`content_remediation_needed`).

**Status (2026-07-22):** All 7 brands verify `contentReady=true` and `fullyReady=true`.  
**Not done:** founder visual review, active release / public-full visibility restore.  
**Untouched:** Company Validated, Source Library status, Registry approval/status, release fields, 11 public-full-clean brands, 5 true-incomplete brands.

## Targets

| Wave | Slugs | Notes |
| --- | --- | --- |
| 1 | `radisson`, `radisson-blu` | Content-only; images already passed |
| 2 | `radisson-red`, `quality-inn`, `country-inn-suites`, `woodspring-suites`, `suburban-studios` | Content + Presentation image reassignment from existing inventory |

## Run

```bash
# Dry-run (plan + defect tables)
npm run brand-explorer-built-blocked-remediation -- --brands country,quality-inn,radisson,radisson-blu,radisson-red,suburban,woodspring --dry-run

# Wave 1 apply
npm run brand-explorer-built-blocked-remediation -- --brands radisson,radisson-blu --apply \
  --approve-built-blocked-remediation \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-release-field-changes \
  --confirm-protected-public-full-unchanged \
  --confirm-field-level-fixes-only \
  --confirm-no-broad-rewrite

# Wave 2 apply
npm run brand-explorer-built-blocked-remediation -- --brands radisson-red,quality-inn,country,woodspring,suburban --apply \
  --approve-built-blocked-remediation \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-release-field-changes \
  --confirm-protected-public-full-unchanged \
  --confirm-field-level-fixes-only \
  --confirm-no-broad-rewrite

# Verify
npm run brand-explorer-built-blocked-remediation -- --brands country,quality-inn,radisson,radisson-blu,radisson-red,suburban,woodspring --verify-only
```

## Post-remediation verification (live)

| Brand | failFindings (actionable) | empty | uniq | role | g/s/p | fullyReady |
| --- | ---: | ---: | --- | --- | --- | --- |
| Country Inn & Suites | 0 | 0 | pass | pass | 9/3/3 | yes |
| Quality Inn | 0* | 0 | pass | pass | 6/3/3 | yes |
| Radisson | 0 | 0 | pass | pass | 6/3/4 | yes |
| Radisson Blu | 0 | 0 | pass | pass | 6/3/6 | yes |
| Radisson RED | 0 | 0 | pass | pass | 6/3/4 | yes |
| Suburban Studios | 0* | 0 | pass | pass | 6/3/4 | yes |
| WoodSpring Suites | 0 | 0 | pass | pass | 6/3/3 | yes |

\*Counter may show `fails=1` from non-actionable completeness statuses (`cleanly_unavailable` / suppress); `fullyReady` / Tab Factory auditPass is the gate.

## What was patched

- Presentation Body/Title/Case Summary for failing Tab Factory slots (field packs per brand).
- Limited Basics: audience / positioning / differentiators / customer promise (stub scrub only).
- Image: reassign existing Presentation Image attachments into gallery / openings / scenarios (no Registry status writes).
- Quarantine: empty / wrong-brand openings; empty materials.file meta fill.

## Explicitly not written

- Company Validated / Company Validation Date
- Source Library status
- Registry approval / status
- Founder Visual Review Pass / Active Profile Approved / Ready for Active Profile / release dates
- Public-full visibility restore

## Next (founder)

1. Founder visual review of the 7 remediated profiles.
2. Explicit active release / visibility restore command (separate gated writer).
3. Do **not** treat these as public-full until that restore.

## Reports

- `reports/brand-explorer-built-blocked-remediation.json`
- `reports/brand-explorer-built-blocked-remediation.md`
- Per-brand: `reports/brand-explorer-built-blocked-remediation-{country|quality-inn|radisson|radisson-blu|radisson-red|suburban|woodspring}.md`

## Change impact

**High** — Presentation (+ limited Basics) writes on 7 brands.

**Rollback:** Revert Presentation/Basics patches for the seven target record IDs only; protected public-full and true-incomplete brands were not modified.

## Regression checklist

- Retest Brand Explorer founder preview for all 7 remediated brands.
- Confirm protected public-full profiles still lockPass / unchanged.
- Confirm Autograph / Handwritten / Radisson Collection / Tapestry / Vignette still incomplete / untouched.
- Confirm Company Validated and Source Library / Registry fields unchanged.
- Image uniqueness + role-match still pass after any further Presentation Image edits.
