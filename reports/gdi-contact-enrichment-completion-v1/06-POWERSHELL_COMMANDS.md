# Repeatable commands — Contact Enrichment & Actions

```powershell
# Audit (read-only)
node scripts/gdi-contact-enrichment-completion-audit-v1.mjs
node scripts/gdi-wyndham-contact-enrichment-audit-v1.mjs

# Rematerialize (dry-run then apply)
node scripts/gdi-wyndham-contact-hydration-rematerialize-v1.mjs --dry-run
node scripts/gdi-wyndham-contact-hydration-rematerialize-v1.mjs --apply --auth "FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_CONTACT_ENRICHMENT_COMPLETION"

# Gates
npm run test:gdi-contact-enrichment-actions-v1
npm run test:gdi-list-view-contact-actions-v1
npm run test:gdi-contact-research-hydration-v1

# Canonical :8080
npm run dealality:gdi-local-8080:stop
npm run dealality:gdi-local-8080

# Stage runners (reuse — no parallel engine)
pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -Help
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -Help
```

**Findymail:** not executed this cycle — see `02-FINDYMAIL_ENRICHMENT_MANIFEST.md` (0 eligible; $0).
