# Brand Relationship Schema Stress Test

Rows: **51** · Brand Managed Capability: **24**

| Concept | Verdict |
| ------- | ------- |
| Relationship Type | **KEEP** — Currently Operates + Brand Managed Capability both used |
| Brand / Brand Parent | KEEP — parent often blank; allow text+link |
| Current/Historical | KEEP |
| Geography scope | KEEP — essential for BM Capability |
| Segment / hotel-type scope | Optional — rarely filled in dry-run |
| Approval Status | **DO NOT ADD** as default — Class 3 / outreach |
| Evidence / Publication / Conflict | KEEP |
| thirdPartyOwnerAvailability | KEEP on BM Capability rows |

## Redundancy
Profile.`brands` remains display list; typed intel table is SoT for typed edges.

## Gap
Need select enum for Relationship Type in Airtable (not free text).
