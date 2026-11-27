# GDI Full E2E Completion & Publication Lifecycle V1

**Status:** Permanent (founder decisions 2026-11-27)  
**Contracts:** `GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1` · `GDI_OPERATIONAL_STATUS`

## Canonical sequence

```
FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT
```

Applies to every existing and future GDI hotel.

## Binding operational statuses

| Status | Definition |
|---|---|
| RESEARCH IN PROGRESS | E2E research is incomplete. |
| HOLD | Additional commercial evidence or qualification is required. |
| STAGED | E2E completed, commercially qualified, durably persisted and visible on port 8080. |
| CURRENT | Founder approved and available to authorized customers. |

Internal technical classifications (Proposed Qualified, Write-Ready, QA Accepted, FQO Candidate, etc.) may remain for engineering. They **must not** introduce founder approval gates. Once qualification + approved persistence + package validation pass, they auto-progress to **STAGED**.

## Permanent completion doctrine

> A GDI Full E2E run cannot be declared complete until its qualified opportunities are durably persisted, STAGED, and visible in the actual integrated local application on port **8080**.

Generating research reports, passing unit qualification tests, or preparing proposed write-sets alone does **not** constitute operational E2E completion.

## Five research stages (unchanged)

1. Demand Discovery  
2. Opportunity Deep Research  
3. WHO/HOW Resolution  
4. Professional Contact Research  
5. Commercial Qualification  

## Operational completion steps (after qualification)

Not additional research stages:

1. Deduplicate against existing hotel intelligence  
2. Preserve existing qualified inventory  
3. Validate evidence and canonical opportunity identities  
4. Persist via approved durable publication-package architecture  
5. Generate/update the **complete** STAGED package (not incremental-only)  
6. Validate STAGED package integrity  
7. **Content Hydration & Presentation Validation** (permanent gate — not a sixth research stage)  
8. Refresh local inventory mirror where applicable  
9. Verify accessibility on port 8080  
10. Produce execution closeout  

Airtable GDO production apply remains separately gated unless explicitly authorized. STAGED package store progression is mandatory for operational E2E completion.

### Content Hydration & Presentation Validation (permanent)

An opportunity cannot be STAGED-certified merely because every field is non-empty. Customer-facing titles, demand classification, commercial narrative, WHO/HOW contact presentation, and List/Tiles/Drawer consistency must pass `gdi-content-hydration-presentation-gate-v1` before STAGED certification.

- Module: `lib/group-demand-intelligence/presentation/gdi-content-hydration-presentation-gate-v1.js`  
- Evidence-backed titles: `gdi-evidence-backed-commercial-title-v1.js`  
- Validate: `.\scripts\gdi\Invoke-GdiContentHydrationValidate.ps1` · `npm run test:gdi-content-hydration-presentation-gate-v1`  
- Hotel audit (DryRun): `npm run report:gdi-content-hydration-audit-v1 -- --hotelId <rec…>`  

Do not fabricate room nights, contacts, or buying authority to pass the gate. Optional unavailable fields may remain empty when unsupported.

## STAGED rules

- Complete eligible qualified inventory for the hotel  
- Preserve prior qualified IDs; Weekly Change records meaningful deltas  
- Never stage HOLD / rejected / speculative / incomplete as qualified  
- Never auto-modify CURRENT  
- Failed staging must not corrupt the prior valid package (atomic pointer updates)

## CURRENT rules

- Single founder approval gate  
- Approval bound to the exact reviewed `publicationVersion`  
- Material STAGED changes after review require re-approval  
- Customers see CURRENT only (auth + hotel ACL)

## Founder review

Authorized founder/admin session on `http://localhost:8080` — normal GDI app, STAGED by default when present. No Proposed UI, no required URL params, no raw JSON/static HTML as the primary review path.

## Exact PowerShell commands

```powershell
# DryRun Full E2E (non-mutating; validates STAGED package construction)
.\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode DryRun

# Live Full E2E (requires paid-research auth when spending; auto-stages qualified results)
.\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch

# Validate V3 fixtures + hotel DryRun + lifecycle gates + content hydration + :8080 STAGED visibility
.\scripts\gdi\Invoke-GdiValidateFullE2E.ps1 -HotelId <rec...>

# Content hydration / presentation gate only
.\scripts\gdi\Invoke-GdiContentHydrationValidate.ps1 -HotelId <rec...> -RequirePort8080

# Explicit CURRENT promotion (requires exact founder auth + package version; DryRun default)
.\scripts\gdi\Invoke-GdiPromoteCurrent.ps1 -HotelId <rec...> -PublicationVersion <ver> -Mode DryRun
.\scripts\gdi\Invoke-GdiPromoteCurrent.ps1 -HotelId <rec...> -PublicationVersion <ver> -Mode Live `
  -Auth "FOUNDER_GO AUTHORIZE_GDI_CURRENT_PROMOTION <hotelId> <publicationVersion>"
```

## Integration Controller acceptance

Reject GDI Full E2E COMPLETE unless:

1. Research completed within authorization  
2. Commercial qualification passed  
3. Durable STAGED persistence verified  
4. Complete STAGED package generated; opportunity IDs reconciled  
5. Content hydration & presentation gate passed (titles, taxonomy, mandatory commercial fields, WHO/HOW consistency)  
6. Founder-visible inventory verified on port 8080  
7. CURRENT unchanged unless explicitly approved for that version  
8. Previously accepted application functionality preserved  

Feature-branch tests alone are insufficient. Opportunity count alone is not commercial certification.

## Related

- [GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1.md](./GDI_CANONICAL_E2E_TO_STAGED_LIFECYCLE_V1.md)  
- Module: `lib/group-demand-intelligence/publication-package/gdi-canonical-e2e-to-staged-lifecycle-v1.js`  
- Gates: `npm run test:gdi-canonical-e2e-to-staged-lifecycle-v1` · `npm run test:gdi-cumulative-integrated-staging-non-regression-v1` · `npm run test:gdi-content-hydration-presentation-gate-v1`  
- Audit: `node scripts/gdi-product-wide-hotel-readiness-audit-v1.mjs` · `npm run report:gdi-content-hydration-audit-v1`
