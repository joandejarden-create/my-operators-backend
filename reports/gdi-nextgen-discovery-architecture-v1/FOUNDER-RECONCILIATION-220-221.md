# Founder Report — PR #220 / #221 Reconciliation

**Date:** 2026-11-12  
**Contract:** fail-closed sales inventory + independent nextgen hunters

## PR #220 authorization verdict

**PASS (fail-closed).** Hotel allowlist alone is not authorization.

Positive grant requires one of:

1. Dealality founder/admin role
2. Explicit `gdiSalesInventoryHotelIds` hotel grant
3. Server-issued / local founder sales-inventory capability

Pilot-unauth alone, unscoped Dealality users, forged `inventoryAudience`, and share surfaces fail closed to the **published 19**.

Follow-up merge: **PR #223** → main `34842a2ebb6cba1f987c382f807524419d9b7c15`  
(PR #220 itself merged earlier as `b724692b97bc9d7a0ba7dc1bf736c1adc72d2638`.)

### Local evidence (Bethesda `recLuxvwwxID7U2B8`)

| Surface | Authorized (local capability) | Unauthorized (capability off) |
|---|---|---|
| List | 67 / `SALES_TEAM_INVENTORY` | 19 / `PUBLISHED_CUSTOMER_FEED` |
| CMI filter | 7 | n/a (published subset) |
| All Commercial Intelligence | 67 unique | 19 |
| Unpublished detail | 200 | 404 |
| CSV | 64 columns; includes unpublished only when authorized | unpublished ID absent |
| Forged `inventoryAudience=SALES_TEAM` | still needs capability/admin/grant | stays 19 |
| Share without token | 403 `SHARE_MISSING` | 403 |

Browser: `http://localhost:8080/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8` showed **67 of 67**, CMI **7**, detail drawer opened (ACVNU Renal Week).

## PR #221 rebase / diff reconciliation

- Rebased `feature/gdi-nextgen-discovery-architecture-v1` onto main after #223.
- Diff vs main contains **only** nextgen discovery architecture (+ weekly soft wire). No Bethesda 67 inventory duplicate patches.

## Ten-hunter independence matrix

| Hunter | PS entry | Node module | Independent config | Zero-paid dry-run |
|---|---|---|---|---|
| Government Procurement | `Invoke-GdiGovernmentProcurementHunter.ps1` | `government-procurement-hunter-v1.js` | yes | certified historical path |
| Association Rotation | `Invoke-GdiAssociationRotationHunter.ps1` | `association-rotation-hunter-v1.js` | yes | stub/cache |
| Corporate Meeting Triggers | `Invoke-GdiCorporateMeetingTriggersHunter.ps1` | `corporate-meeting-triggers-hunter-v1.js` | yes | synthetic fixture |
| Clinical Trials / Medical | `Invoke-GdiClinicalTrialsMedicalHunter.ps1` | `clinical-trials-medical-hunter-v1.js` | yes | stub/cache |
| Project Workforce | `Invoke-GdiProjectWorkforceHunter.ps1` | `project-workforce-hunter-v1.js` | yes | synthetic fixture |
| Sports Housing | `Invoke-GdiSportsHousingHunter.ps1` | `sports-housing-hunter-v1.js` | yes | synthetic + Party Bus reject |
| University Demand | `Invoke-GdiUniversityDemandHunter.ps1` | `university-demand-hunter-v1.js` | yes | synthetic fixture |
| Legal / Regulatory | `Invoke-GdiLegalRegulatoryHunter.ps1` | `legal-regulatory-hunter-v1.js` | yes | stub/cache |
| Emergency / Displacement | `Invoke-GdiEmergencyDisplacementHunter.ps1` | `emergency-displacement-hunter-v1.js` | yes | stub/cache |
| Crew / Production | `Invoke-GdiCrewProductionHunter.ps1` | `crew-production-hunter-v1.js` | yes | stub/cache |

Shared only: `Invoke-GdiSpecialistHunter.ps1` + `shared/*` (config, evidence class, ORS handoff, execution).

Disabling sports via hotel override does **not** disable government (config isolation tested).

## Fixture vs real research

| Class | Meaning | Customer inventory | Auto publish |
|---|---|---|---|
| `SYNTHETIC_TEST_FIXTURE` | Cache/pilot illustrative rows | No | No |
| `HISTORICAL_RESEARCH_REPLAY` | Prior official notice, verifiable URL | ORS only | No |
| `LIVE_PUBLIC_DISCOVERY` | Fresh free/public retrieval | ORS only until qualified | No |
| `LIVE_PAID_DISCOVERY` | Paid Exa/Apify (not authorized this wave) | ORS only | No |

First-wave cache JSON is explicitly `SYNTHETIC_TEST_FIXTURE`.

## Zero-paid Government Procurement demonstration

- Source: SAM.gov NIH SBPO Vendor Outreach Session notice  
  `https://sam.gov/workspace/contract/opp/f4e0c6bfa2a14c2195c8476fb81d5f9c/view`
- Class: `HISTORICAL_RESEARCH_REPLAY`
- Extracted: NIH SBPO organization; Vendor Outreach Session activity; prior session 2026-04-29 at 6700 Rockledge Drive; `NIHSmallBusiness@od.nih.gov`
- Hotel thesis: Bethesda Marriott vendor lodging watch near Rockledge/NIH corridor
- Ecosystem: org + activity nodes created
- ORS handoff: `QUEUED_FOR_QUALIFICATION` (research-stage; **no** Sales Ready; **no** publication)
- Incomplete: named lodging buyer not verified; next session date unknown; live SAM.gov GET often requires interactive session (historical replay retained)

Report: `reports/gdi-nextgen-discovery-architecture-v1/gov-zero-paid-demo/`

## Apify / Exa status

- Apify registry recovered: **13 actors**
- Provider routing plan retained per hunter
- `providerLiveCertified: false` until live retrieval is exercised
- Paid flags fail closed (`-Execute` alone does not authorize spend)

## Full E2E integration

- Weekly supervisor soft-wires hotel-adaptive hunter selection into existing `weekly-refresh-orchestrator-v1`
- Fourteen demand segments preserved (no 15th)
- Hunters are discovery sources into existing ORS/qualification — not a parallel supervisor

## Tests

- `test:gdi-bethesda-67-inventory-access-control-v1` PASS
- `test:gdi-bethesda-67-sales-team-inventory-display-v1` PASS
- `test:gdi-bethesda-67-sales-baseline-cmi-v1` PASS
- `test:gdi-nextgen-discovery-architecture-v1` PASS (10 runners, Apify 13, gov historical + ORS)
