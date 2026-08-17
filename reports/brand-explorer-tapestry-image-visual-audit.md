# Tapestry — Image Visual Audit

Version: `tapestry-factory-promotion-v1` · Stage: **image-visual-audit** · Generated: 2026-07-23T23:33:00.324Z
Mode: **APPLY** · writePerformed: **true**

## Image snapshot

- galleryDistinct: **6**
- scenarioDistinct: **3**
- propertyDistinct: **3**
- uniquenessPass: **true**
- roleMatchPass: **true**
- repeatedRoleHeuristicHit: **true**
- alreadyPasses: **true**
- duplicateGroups: **0**

## Scenario roles

| Slot | Title | Detected role |
|------|-------|---------------|
| `overview.scenario.1` | independent upscale conversion | unknown |
| `overview.scenario.2` | boutique repositioning at moderate capital | unknown |
| `overview.scenario.3` | value-conscious independent-character hotel | unknown |

## Planned title patches (title-only, tapestry only)

- PATCH `recr8ZaKGQo9BLTbh` slot=`overview.scenario.2` reason=scenario_retitle_for_role_diversity fields=Title
- PATCH `rec5n9zh2hyUvHZWY` slot=`overview.scenario.3` reason=scenario_retitle_for_role_diversity fields=Title

## Apply results

- `recr8ZaKGQo9BLTbh` slot=`overview.scenario.2`: ok
- `rec5n9zh2hyUvHZWY` slot=`overview.scenario.3`: ok

## Materialization

- Attempted: **false**
- Reason: deferred_no_opt_in_or_gates_pass

## Guardrails

- tapestryOnly: true
- titleOnlyPreferred: true
- protectedBaselineUntouched: true
- companyValidatedWrites: false
- sourceLibraryWrites: false
- registryWrites: false
- brandStatusWrites: false
- releaseFieldWrites: false
- neverWriteFields: Company Validated, Company Validation Date, Source Library status, Registry approval/status

## Required apply flags

- `--approve-tapestry-image-cleanup`
- `--confirm-tapestry-only`
- `--confirm-image-issues-only`
- `--confirm-no-company-validation-changes`
- `--confirm-no-source-library-status-changes`
- `--confirm-no-registry-approval-changes`
- `--confirm-no-brand-status-changes`
- `--confirm-no-release-field-writes`
- `--confirm-no-protected-baseline-brand-changes`
