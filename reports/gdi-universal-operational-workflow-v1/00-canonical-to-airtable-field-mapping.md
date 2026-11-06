# GDI canonical → Airtable field mapping

**Authority:** `lib/group-demand-intelligence/opportunity-field-map.js`  
**Table:** `Group Demand Opportunities` (`tblRuReslJMwsfRQj`) on intelligence base `appa2cE7FTRmIbB32`  
**Ownership:** `lib/group-demand-intelligence/workflow/gdi-airtable-field-ownership-v1.js`

No new Airtable fields are invented by the operational workflow. Schema changes require `npm run ensure:gdi-opportunities-airtable-schema -- --apply` under existing authorization.

## Flat columns (research-owned unless noted)

| Canonical / opportunity key | Airtable field | Owner |
| --- | --- | --- |
| `id` / `opportunityId` | `opportunityId` (upsert key) | identity |
| `hotelId` | `hotelId` | identity |
| `title` | `opportunityName` | research |
| `organizationName` | `organizationName` | research |
| `opportunityType` | `opportunityType` | research |
| `priority` | `priority` | research |
| `actionStatus` / sales progression | `actionStatus` | **sales** when set |
| `eventStartDate` / `eventEndDate` | `eventStartDate` / `eventEndDate` | research |
| `segment` | `segment` | research |
| venue / sourcing | `venueStatus` / `sourcingStatus` | research |
| attendance / peak rooms | `attendance` / `peakRooms` + status cols | research |
| hotel fit score | `hotelFit` | research |
| qualification | `qualification` | research |
| narratives | `whyNow`, `whyThisMatters`, `hotelWinThesis`, `recommendedAction` | research |
| primary contact | `primaryContactName/Role/Email/Phone/Id` | research |
| sources | `sourceCount`, `sourceSummary` | research |
| research timestamps | `lastResearchedAt`, `researchVersion`, `runId` | research |
| full fidelity blob | `opportunityPayloadJson` | research + preserved sales/publication keys |
| `decisionId` | `decisionId` | **sales / decision-outcomes** |

## Payload-only keys (inside `opportunityPayloadJson`)

Sales-owned (never overwritten by research sync):  
`hotelSalesFeedback`, `salesFeedback*`, `salesOwner`, `assignedSalesperson`, `lastSalesActivityAt`, `nextFollowUpDate`, `contactedAt`, `workingStatus`, `alreadyKnown`, `notAFitReason`, `rejectionReason`, `hotelTeamNotes`, …

Publication-owned (stamped only after verified publish):  
`gdiPublicationStatus`, `gdiPublicationVersion`, `gdiPublicationId`, `gdiPublishedAt`, `gdiPublishedOpportunityIds`, `gdiCustomerShareReady`, `gdiFullContentParityStatus`, …

## Lifecycle mapping

Conceptual states (`DISCOVERED` → `PUBLISHED` → `SALES_FOLLOW_UP`, plus `REJECTED` / `EXCLUDED` / …) map into existing `actionStatus` **only when empty**. Human sales values are preserved.

Decisions + Decision Events remain the ADP/GDI feedback timeline (`lib/decision-outcomes`).

## What is not synced from Airtable into publication

Airtable must **never** reconstruct `approvedCustomerFacingDetail`.  
Publication uses one-click freeze from approved local / certified pack / registry snapshots only.
