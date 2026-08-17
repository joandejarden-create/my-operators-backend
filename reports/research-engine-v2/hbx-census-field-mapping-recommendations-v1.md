# HBX → Hotel Property Census field mapping recommendations v1

Generated: 2026-08-09T14:45:28.303Z
**Do not create these fields yet** — recommendations only.

| Field | Type | Internal only | Public license needed | Existing coverage | Reason |
| --- | --- | --- | --- | --- | --- |
| HBX Hotel Code | singleLineText or number | true | false | false | Stable external identity for match/dedupe and source linkage |
| HBX Chain Code | singleLineText | true | false | Partial — Current Brand needs Brand Setup mapping | Preserve HBX chain provenance without writing Current Brand prematurely |
| HBX Category Code | singleLineText | true | false | false | Star/category code provenance from HBX |
| HBX Category Name | singleLineText | true | false | false | Human-readable category after master lookup |
| HBX Accommodation Type | singleLineText | true | false | false | Filter non-hotel inventory; lodging class |
| HBX Destination Code | singleLineText | true | false | No — Market is Dealality-defined | Geography hint; map later to Dealality Market |
| HBX Zone Code | singleLineText | true | false | No — Submarket is Dealality corridor | Possible Submarket hint |
| HBX Facility Summary | longText or multipleSelects | true | true | false | Internal amenity rollup |
| HBX Image Count | number | true | false | false | Content richness signal without storing media |
| HBX Description Available | checkbox | true | false | false | Flag description presence without storing copy |
| HBX Content Last Reviewed Date | date | true | false | false | Track lastUpdate freshness |
| HBX Content License Status | singleSelect | true | false | false | Gate coords/images/descriptions/facilities for storage & display |
| HBX Content Review Status | singleSelect | true | false | false | Workflow: candidate / approved_internal / blocked_license / rejected |

## Immediate existing-field candidates (policy pending)
- Property Name / Canonical Property Name ← HBX name
- Country ← countryCode
- City ← city
- Address ← address (internal policy)
- Official Property URL ← web
- Phone ← PHONEHOTEL only

## Explicit non-mappings
- Rooms / Keys ← **never** from rooms[], allotment, occupancy, or unverified text claims
- Phone ← never PHONEBOOKING / PHONEMANAGEMENT
- Current Brand ← never raw chainCode without Brand Setup mapping
- Market / Submarket ← never raw destination/zone without Dealality mapping
