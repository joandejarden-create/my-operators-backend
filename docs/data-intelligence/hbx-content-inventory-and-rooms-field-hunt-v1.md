# HBX Content Inventory + Rooms / Keys Field Hunt v1

**Status:** `production_census_hbx_content_inventory_and_rooms_field_hunt_v1_partial_hbx_support_confirmation_needed`  
**Objective:** `hbx-content-inventory-and-rooms-field-hunt-v1`  
**Generated:** 2026-08-09T14:45:28.303Z  
**Airtable writes:** **0** (must be 0)

## Verdict
- True hotel-level Rooms / Keys in HBX: **NO (not proven)**
- Rooms hunt class: `unsupported_or_needs_hbx_support_confirmation`
- HBX support confirmation needed: **yes**
- License policy needed for some content types: **yes**
- Artifact / quota mode: **wave1_pack_plus_smoke_plus_booking_when_available** (live Content hotels Quota exceeded; Wave1 pack + prior smoke + Booking used)

## Endpoints inspected
- `booking_status` GET `/hotel-api/1.0/status` → HTTP 403 (failed)
- `hotels_probe_skipped` GET `/hotel-content-api/1.0/hotels` → HTTP 403 (failed)
- `hotel_details_skipped` GET `/hotel-content-api/1.0/hotels/{code}/details` → HTTP 403 (failed)
- `prior_smoke_test_artifact` READ `reports/research-engine-v2/hbx-content-api-smoke-test-v1.json` → HTTP 200
- `master_countries` GET `/hotel-content-api/1.0/locations/countries` → HTTP 403 (failed)
- `master_destinations_skipped` GET `/hotel-content-api/1.0/locations/destinations` → HTTP 403 (failed)
- `master_destinations_zones_skipped` GET `/hotel-content-api/1.0/locations/destinations` → HTTP 403 (failed)
- `master_categories_skipped` GET `/hotel-content-api/1.0/types/categories` → HTTP 403 (failed)
- `master_groupcategories_skipped` GET `/hotel-content-api/1.0/types/groupcategories` → HTTP 403 (failed)
- `master_chains_skipped` GET `/hotel-content-api/1.0/types/chains` → HTTP 403 (failed)
- `master_accommodations_skipped` GET `/hotel-content-api/1.0/types/accommodations` → HTTP 403 (failed)
- `master_facilities_skipped` GET `/hotel-content-api/1.0/types/facilities` → HTTP 403 (failed)
- `master_facilitygroups_skipped` GET `/hotel-content-api/1.0/types/facilitygroups` → HTTP 403 (failed)
- `master_facilitytypologies_skipped` GET `/hotel-content-api/1.0/types/facilitytypologies` → HTTP 403 (failed)
- `master_imagetypes_skipped` GET `/hotel-content-api/1.0/types/imagetypes` → HTTP 403 (failed)
- `master_rooms_skipped` GET `/hotel-content-api/1.0/types/rooms` → HTTP 403 (failed)
- `master_boards_skipped` GET `/hotel-content-api/1.0/types/boards` → HTTP 403 (failed)
- `master_issues_skipped` GET `/hotel-content-api/1.0/types/issues` → HTTP 403 (failed)
- `master_terminals_skipped` GET `/hotel-content-api/1.0/types/terminals` → HTTP 403 (failed)
- `master_segments_skipped` GET `/hotel-content-api/1.0/types/segments` → HTTP 403 (failed)
- `cache_api_schema` NOTE `Cache/content-dump not exercised (no production writes; env may not include cache product)` → HTTP null (failed)
- `booking_availability` POST `/hotel-api/1.0/hotels` → HTTP 200

## Samples
- Broad HBX sample: **60** hotels (Wave 1 CALA countries)
- Known Rooms/Keys comparison: **30** Census matches
- Live hotel payloads scanned: **0**
- Booking availability hotels scanned: **5**

Countries in broad sample: {"Mexico":20,"Dominican Republic":10,"Colombia":10,"Costa Rica":10,"Panama":10}

## Field population (hotel content)
| Field | Pop rate | Classification | License |
| --- | ---: | --- | --- |
| code | 100.0% | write_candidate_new_field_needed | likely_internal_storage_ok |
| name | 100.0% | write_candidate_existing_field | likely_internal_storage_ok |
| chainCode | 78.3% | write_candidate_new_field_needed | likely_internal_storage_ok |
| categoryCode | 100.0% | write_candidate_new_field_needed | likely_internal_storage_ok |
| categoryGroupCode | 0.0% | internal_only_candidate | likely_internal_storage_ok |
| accommodationTypeCode | 100.0% | write_candidate_new_field_needed | likely_internal_storage_ok |
| countryCode | 100.0% | write_candidate_existing_field | likely_internal_storage_ok |
| stateCode | 100.0% | candidate_only | likely_internal_storage_ok |
| destinationCode | 100.0% | write_candidate_new_field_needed | likely_internal_storage_ok |
| zoneCode | 100.0% | write_candidate_new_field_needed | likely_internal_storage_ok |
| city | 100.0% | write_candidate_existing_field | likely_internal_storage_ok |
| address | 100.0% | write_candidate_existing_field | internal_storage_policy_needed |
| postalCode | 100.0% | candidate_only | likely_internal_storage_ok |
| coordinates.latitude | 96.7% | public_use_license_needed | do_not_store_until_license_confirmed |
| coordinates.longitude | 96.7% | public_use_license_needed | do_not_store_until_license_confirmed |
| web | 93.3% | write_candidate_existing_field | likely_internal_storage_ok |
| email | 90.0% | internal_only_candidate | internal_storage_policy_needed |
| phones.PHONEHOTEL | 100.0% | write_candidate_existing_field | likely_internal_storage_ok |
| phones.PHONEBOOKING | 100.0% | dangerous_or_ambiguous | not_useful |
| phones.PHONEMANAGEMENT | 100.0% | dangerous_or_ambiguous | not_useful |
| description | 100.0% | public_use_license_needed | do_not_store_until_license_confirmed |
| facilities | 100.0% | public_use_license_needed | license_policy_needed |
| facilities[].number | 0.0% | dangerous_or_ambiguous | not_useful |
| images | 100.0% | public_use_license_needed | do_not_store_until_license_confirmed |
| rooms | 100.0% | unsupported_for_census | license_policy_needed |
| rooms[].minPax/maxPax | 0.0% | dangerous_or_ambiguous | not_useful |
| boardCodes | 0.0% | internal_only_candidate | likely_internal_storage_ok |
| segmentCodes | 90.0% | internal_only_candidate | likely_internal_storage_ok |
| terminals | 90.0% | candidate_only | license_policy_needed |
| interestPoints | 0.0% | candidate_only | license_policy_needed |
| issues | 0.0% | internal_only_candidate | internal_storage_policy_needed |
| giataCode | 0.0% | candidate_only | likely_internal_storage_ok |
| lastUpdate | 100.0% | write_candidate_new_field_needed | likely_internal_storage_ok |
| S2C / ranking | 0.0% | internal_only_candidate | internal_storage_policy_needed |
| license | 0.0% | internal_only_candidate | likely_internal_storage_ok |

## Useful for immediate internal Census (candidates only — no writes this run)
- **code** → HBX Hotel Code (recommended new) (Stable HBX identity key for linkage/dedupe)
- **name** → Property Name / Canonical Property Name (Name candidate with normalize + steward review)
- **chainCode** → HBX Chain Code (recommended new); not Current Brand without mapping (Internal chain linkage; map via Brand Setup before public brand writes)
- **categoryCode** → HBX Category Code (recommended new) (Star/category provenance; ≠ Dealality brand tier)
- **categoryGroupCode** → HBX Category Group (optional new) (Category grouping for analysis)
- **accommodationTypeCode** → HBX Accommodation Type (recommended new) (Filter non-hotel; classify lodging type)
- **countryCode** → Country (ISO country for identity + geography)
- **destinationCode** → HBX Destination Code (recommended new) (Geography hint; ≠ Dealality Market)
- **zoneCode** → HBX Zone Code (recommended new) (Possible Submarket hint; mapping required)
- **city** → City (City candidate with Proper Case)
- **address** → Address (Internal address fill when High confidence / steward OK)
- **web** → Official Property URL (Website candidate after URL normalize)
- **email** → Email (if Census field exists) (Internal contact only if present)
- **phones.PHONEHOTEL** → Phone (Only PHONEHOTEL as property phone)
- **boardCodes** → none (Meal-plan catalog for commercial context)
- **segmentCodes** → none (Segment labels for analysis)
- **issues** → none (Operational notices; may change)
- **lastUpdate** → HBX Content Last Reviewed Date (recommended new) (Content freshness tracking)
- **S2C / ranking** → none (HBX commercial ranking signal only)
- **license** → none (Hotel operating license string if present)

## Useful but needing license decision
- **coordinates.latitude** — do_not_store_until_license_confirmed
- **coordinates.longitude** — do_not_store_until_license_confirmed
- **description** — do_not_store_until_license_confirmed
- **facilities** — license_policy_needed
- **images** — do_not_store_until_license_confirmed
- **rooms** — license_policy_needed
- **terminals** — license_policy_needed
- **interestPoints** — license_policy_needed

## Not useful / dangerous for Census Rooms or Phone
- **phones.PHONEBOOKING** — dangerous_or_ambiguous: Reject as hotel phone
- **phones.PHONEMANAGEMENT** — dangerous_or_ambiguous: Reject as hotel phone
- **facilities[].number** — dangerous_or_ambiguous: Facility quantity (beds/etc), not hotel keys
- **rooms** — unsupported_for_census: Room-type catalog only; never rooms.length as keys
- **rooms[].minPax/maxPax** — dangerous_or_ambiguous: Occupancy bounds per room type

## Rooms / Keys investigation
- Candidates scanned: **264**
- Semantic classes seen: {"room_type_catalog_only":130,"contracted_allotment":67,"available_inventory_for_dates":67}
- Comparison vs Census known rooms: **30** rows
- Matches where HBX rooms.length ≈ Census Rooms/Keys: **0** (still catalog-only; coincidence not proof)
- True total field proven: **false**

## Recommended next production write policy
Keep ENABLE_HBX_CENSUS_WRITES=0 and ENABLE_HBX_INSERTS=0 until license + write policy review. Do not write Rooms / Keys from HBX unless support confirms a true hotel-level total and validation vs Census High-confidence keys passes. Never write rooms.length, min/max pax, allotment, or booking rates.rooms as Rooms / Keys. Reject PHONEBOOKING and PHONEMANAGEMENT as Phone. Immediate internal candidates after policy: HBX Hotel Code (new), name, address, website, PHONEHOTEL, country/city, chainCode/category/accommodationType as provenance fields. Hold coordinates, descriptions, images, facilities public display pending license_policy decision. Send hbx-room-count-support-question-pack.md to Hotelbeds support.

## Confirmations
- No Airtable writes: **true**
- No Hotel Property Census writes: **true**
- No Brand Explorer / Brand Setup writes: **true**
- No inserts: **true**
- `rooms[]` not treated as Rooms / Keys: **true**
- Secrets not logged: **true**

## Artifacts
- `reports/research-engine-v2/hbx-content-inventory-and-rooms-field-hunt-v1.json`
- `reports/research-engine-v2/hbx-content-field-dictionary-v1.json`
- `reports/research-engine-v2/hbx-census-field-mapping-recommendations-v1.md`
- `reports/research-engine-v2/hbx-room-count-support-question-pack.md`
- `docs/data-intelligence/hbx-content-inventory-and-rooms-field-hunt-v1.md`
