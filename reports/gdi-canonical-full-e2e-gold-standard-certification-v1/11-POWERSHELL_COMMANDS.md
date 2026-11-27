# Exact repeatable PowerShell / Node commands

## Canonical five-stage + orchestrator (DryRun)

```powershell
pwsh -File scripts/gdi/Invoke-GdiDiscovery.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiDeepResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiQualification.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiValidateFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o
```

## Stage 4 Findymail (permanent component)

```powershell
# Planning / DryRun eligibility via CLI flag
node scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs --stage PROFESSIONAL_CONTACT_RESEARCH --hotel-id recnPB3Q02NO8Ma1o --mode DryRun --authorize-findymail

# Live Findymail email enrichment (credit hard-cap)
node scripts/gdi-canonical-full-e2e-gold-standard-certification-v1.mjs --findymail-live --credit-hard-cap 30

# Hydrate attribution-safe Findymail emails into STAGED (CURRENT unchanged)
node scripts/gdi-findymail-wyndham-hydrate-staged-v1.mjs --dry-run
node scripts/gdi-findymail-wyndham-hydrate-staged-v1.mjs --apply --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
```

## Gold-standard certification package

```bash
npm run test:gdi-findymail-stage4-permanent-v1
npm run test:gdi-canonical-full-e2e-gold-standard-certification-v1
npm run gdi:canonical-full-e2e-gold-standard-certification-v1
```
