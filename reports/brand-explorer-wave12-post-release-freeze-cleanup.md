# Wave 12 Post-Release Freeze Cleanup

Version: `wave12-post-release-freeze-cleanup-v1` · Generated: 2026-07-25T00:57:18.159Z
Mode: **APPLY** · writePerformed: **true**

## Decision

- **bunkhouse-hotels:** scrub World of Hyatt identity phrasing; keep Hyatt as labeled parent-platform context
- **moxy-hotels:** case B false positive — parent-platform allowlist (`moxy` + Marriott parent); optional tags thicken
- **voco-hotels:** retitle scenarios to diversify role detection; thicken tags

## Patch counts

| Brand | Planned patches | Applied |
| --- | --- | --- |
| bunkhouse-hotels | 1 | 1 |
| moxy-hotels | 0 | 0 |
| voco-hotels | 1 | 1 |

## Guardrails

- targetBrandsOnly: true
- companyValidatedWrites: false
- sourceLibraryWrites: false
- registryWrites: false
- brandStatusWrites: false
- releaseFieldWrites: false
- publicRestoreRegistryWrites: false
- imageWrites: false
- otherActiveBrandChanges: false
- radissonCollectionChanges: false
- neverWriteFields: Company Validated, Company Validation Date, Source Library status, Registry approval/status

## Freeze readiness statement

**not_ready_to_freeze_39_active_public_full_baseline**

- bunkhouse + moxy: copy/allowlist cleared; live `active_profile_ready`.
- voco: copy cleared; freeze blocked by gallery Scene7 aspect-variant duplicates (4/6 distinct) — rematerialization required (outside caption-only image write flags).
- Full audit also shows out-of-scope remediations (avid / HIE / vignette uniqueness; SLH / Suburban / Trademark / WoodSpring `adr`).

See `reports/brand-explorer-wave12-post-release-freeze-cleanup-readiness.md`.
