# Brand Explorer — Profile in Preparation Visibility

## Problem

After Brand Explorer OS / active-profile gates, historically finished brands (Tribute, Ascend, Comfort, Curio, Autograph, …) could render the external **Profile in Preparation** shell when:

1. The brand was loaded by **Airtable record ID** (list/deep link) without resolving the legacy seed slug, so `legacyHistoricalApproved` was never set.
2. Brands were outside `PRIMARY_RELEASE_SLUGS` even though they had finished presentation rows under older Complete Build / Ready for Active Profile workflows.
3. Founder `?beInternalPreview=1` short-circuited `shouldRenderFullProfile` to `true`, which made `isExternalQualityLocked` false — so the founder preview banner never appeared.

## Fix (this pass)

| Area | Change |
| --- | --- |
| API lookup | Resolve legacy seed by **record ID**, slug, and brand name; set `legacyHistoricalApproved` |
| Display state | `legacyVisibilityUnlock` when historically approved + presentation rows + visual counts + image uniqueness (role-match not required for legacy visibility) |
| Atelier | Separate external lock from founder preview; banner: “Founder Preview · This profile is visible for internal review…” |
| Audit | `npm run brand-explorer-profile-preparation-visibility-audit` |
| Restore | `npm run brand-explorer-profile-preparation-visibility-fix` (dry-run default; gated `--apply`) |

## Allowed apply writes (migrate-ready only)

- `Active Profile Approved` / `Ready for Active Profile` / `Active Profile Approved Date`
- `Founder Visual Review Pass` / `Founder Visual Review` (only with historical evidence)
- Code cohort: `VISIBILITY_RESTORED_RELEASE_SLUGS` (Ascend, Comfort Inn & Suites, Curio, Tribute Portfolio)

## Forbidden

- Company Validated / Company Validation Date
- Source Library status
- Registry approval/status
- Owner-facing content rewrites
- Image reassignment

## Founder preview

```
?beInternalPreview=1
```

Renders full tabs when presentation rows exist, even if `active_profile_ready` is false, with the Founder Preview banner when external gates still fail.
