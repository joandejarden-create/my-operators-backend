# Tapestry — Public Release

Version: `tapestry-factory-promotion-v1` · Stage: **public-release** · Generated: 2026-07-23T23:50:45.803Z
Mode: **APPLY** · writePerformed: **true**

Current Brand Status: **Active**

## Planned release fields (tapestry only)

- Active Profile Approved
- Ready for Active Profile
- Active Profile Approved Date
- Founder Visual Review Pass

## Public-restore plan (tapestry row)

```json
{
  "slug": "tapestry-collection-by-hilton",
  "recordId": "reccXxMHEh7NNRhIE",
  "name": "Tapestry Collection by Hilton",
  "lane": "lane2_full_build",
  "fullyReady": true,
  "verifyError": null,
  "verifySource": "final-public-restore-readiness",
  "verifyGates": null,
  "accidentalLegacyUnlockHold": false,
  "alreadyIntentional": true,
  "publicRestoreEligible": false,
  "visibilityPostureIfDryRun": "intentional_public_full",
  "plannedOnApply": {
    "addToIntentionalRegistry": false,
    "writeBasicsReleaseFields": true,
    "contentRewrites": false,
    "imageWrites": false,
    "companyValidatedWrites": false,
    "sourceLibraryWrites": false,
    "registryWrites": false
  },
  "action": "already_intentional_public_restore",
  "reason": "listed_in_intentional_restore_registry"
}
```

## Apply outcome

```json
{
  "applied": true,
  "reason": "public_release_applied_via_direct_fallback",
  "basicsPatched": {
    "recordId": "reccXxMHEh7NNRhIE",
    "fields": [
      "Active Profile Approved",
      "Ready for Active Profile",
      "Active Profile Approved Date",
      "Founder Visual Review Pass"
    ],
    "sanitizedPayloadPreview": {
      "Active Profile Approved": true,
      "Ready for Active Profile": true,
      "Active Profile Approved Date": "2026-07-23",
      "Founder Visual Review Pass": true
    },
    "response": {
      "id": "reccXxMHEh7NNRhIE"
    }
  },
  "intentionalRegistry": {
    "version": "public-restore-intentional-v1",
    "updatedAt": "2026-07-23T23:50:45.800Z",
    "note": "Slugs intentionally public-restored after founder visual review + public-restore-governance --apply.",
    "slugs": [
      "autograph-collection",
      "bw-premier-collection",
      "bw-signature-collection",
      "country-inn-suites",
      "handwritten-collection",
      "preferred-hotels-and-resorts",
      "quality-inn",
      "radisson",
      "radisson-blu",
      "radisson-collection",
      "radisson-red",
      "suburban-studios",
      "tapestry-collection-by-hilton",
      "vignette-collection",
      "woodspring-suites"
    ]
  }
}
```

## Guardrails

- tapestryOnly: true
- companyValidatedWrites: false
- sourceLibraryWrites: false
- registryWrites: false
- contentRewrites: false
- imageWrites: false
- protectedBaselineUntouched: true
- publicRestoreRegistryUpdate: true
- neverWriteFields: Company Validated, Company Validation Date, Source Library status, Registry approval/status

## Required apply flags

- `--approve-tapestry-public-release`
- `--confirm-founder-visual-review-passed`
- `--confirm-brand-status-active-or-live`
- `--confirm-fully-ready`
- `--confirm-public-visibility-quality-lock-passed`
- `--confirm-tapestry-only`
- `--confirm-no-company-validation-changes`
- `--confirm-no-source-library-status-changes`
- `--confirm-no-registry-approval-changes`
- `--confirm-no-content-rewrites`
- `--confirm-no-image-writes`
