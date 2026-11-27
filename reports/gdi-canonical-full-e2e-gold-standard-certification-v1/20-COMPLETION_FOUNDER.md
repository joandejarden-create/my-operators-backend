# GDI Gold Standard Completion — Wyndham

**Status:** `PARTIAL`  
**STAGED before:** `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_17efc1bf`  
**STAGED after:** `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_17efc1bf`  
**Generated:** 2026-11-27T22:50:28.487Z

## Scorecard
| Metric | Before | After (working set) |
|---|---:|---:|
| Opportunities | 26 | 26 |
| Primary email | 19 | 19 |
| Primary phone | 7 | 7 |
| Primary LinkedIn | 14 | 14 |
| Secondary contacts | 14 | 14 |
| alternativeEntryPoints[] | 14 | 14 |

## Airtable
- Configured: **true**
- Dry-run would write: **17**
- Applied: **false**
- Read-back: **STAGED_EMAILS_PRESENT_IN_AIRTABLE**

## Outstanding
- Airtable live persist not applied this run — dry-run write set in 12-AIRTABLE_WRITE_SET_DRY_RUN.json; apply with --airtable-apply --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
- Alt-entry array hydration planned (0) but not applied to STAGED — run --hydrate-alt-entry --apply-staged --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
- Primary email gaps: 7/26
- Primary phone gaps: 19/26 (public/Exa research required; Findymail email-only)
- Primary LinkedIn gaps: 12/26
- WHO/HOW unspecified without alt/secondary: 6 — recover settled WHO/HOW or complete mandatory alt-entry research
- Genuine Airtable→package reconstruction not yet executed in isolated non-publishing context
- Remaining Findymail live not run — --findymail-live --credit-hard-cap 15

## Commands
```powershell
node scripts/gdi-gold-standard-cert-completion-v1.mjs --airtable-dry-run
node scripts/gdi-gold-standard-cert-completion-v1.mjs --airtable-apply --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
node scripts/gdi-gold-standard-cert-completion-v1.mjs --hydrate-alt-entry --apply-staged --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
node scripts/gdi-gold-standard-cert-completion-v1.mjs --findymail-live --credit-hard-cap 15
```
