# Release readiness — GDI Local Master Publication V1/V2

## Ready now
- Frozen local master snapshot intact (same server build SHA as capture)
- v2 candidate staged: `df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66`
- Drawer allowlist corrected (`scalingDrawerModel`, `segmentOverviewModel`, …)
- CURRENT still prior `441773…`
- Bundled packages deployable via Railway lean (`lib/…` not ignored)
- PR #264 open; required CI green at last check

## Blocked on founder
- Exact approval of fingerprint `df8da65a…`
- Permission to merge / deploy / switch CURRENT

## After approval
1. Verify package bytes unchanged vs approval receipt
2. Merge PR #264
3. Deploy Railway artifact from merged main
4. Switch CURRENT → approved fingerprint (keep prior package)
5. Preserve share tokens + feedback
6. Browser-verify production share vs frozen local master (all 67)
7. Only then CLIENT READY

## Not CLIENT READY
External still serves `441773…` until CURRENT switch.
