# Repeatable commands

```powershell
# Audit / rematerialize (dry-run then apply)
node scripts/gdi-wyndham-contact-hydration-rematerialize-v1.mjs --dry-run
node scripts/gdi-wyndham-contact-hydration-rematerialize-v1.mjs --apply --auth "FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_CONTACT_HYDRATION_RECOVERY"

# Gates
npm run test:gdi-contact-research-hydration-v1
npm run test:gdi-list-view-contact-actions-v1
npm run test:gdi-cumulative-integrated-staging-non-regression-v1
npm run dealality:validate-gdi-cumulative-8080

# Canonical :8080
npm run dealality:gdi-local-8080:stop
npm run dealality:gdi-local-8080

# Stage runners (reuse — no new engine)
pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -Help
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -Help
```
