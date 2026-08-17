# Clean Core Identity Repair

# Production Census — Clean Core Identity Repair

**Status:** `production_census_clean_core_identity_repair_partial_source_lookup_remaining`  
**Generated:** 2026-08-07T17:28:24.134Z  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Airtable writes:** no  
**Inserts applied:** 0  
**Cleanup existing only:** yes

## Before → After

| Metric | Count |
|--------|------:|
| Clean Core | — |
| Below Clean Core | — |
| Canonical blank | — |
| Unknown city | — |
| Descriptor city | — |
| All-caps city | — |
| All-lowercase city | — |
| City/state combined | — |
| State / Region complete | — |
| Source URL complete | — |
| Duplicate risk | — |
| Human Review Required | — |
| Coordinate blocked (dirty identity) | — |

## Applied

- Records fixed: 0
- Fields written: —

## Classification

- Clean Core: 1023
- Needs Source Lookup: 5
- Needs Steward Review: 26
- Duplicate Risk: 170
- Not Usable Yet: 0

## Top remaining gaps by parent

- **Accor**: below=40, unknown_city=3, canonical_blank=0
- **SLH**: below=30, unknown_city=0, canonical_blank=0
- **Hyatt**: below=22, unknown_city=0, canonical_blank=0
- **Unknown**: below=14, unknown_city=0, canonical_blank=0
- **Bahía Príncipe**: below=12, unknown_city=0, canonical_blank=0
- **Barceló**: below=12, unknown_city=0, canonical_blank=0
- **RIU**: below=11, unknown_city=0, canonical_blank=0
- **Hodelpa**: below=9, unknown_city=0, canonical_blank=0
- **IHG**: below=8, unknown_city=0, canonical_blank=0
- **Marriott International**: below=5, unknown_city=0, canonical_blank=0

## Examples before/after

_None_

## Next recommended action

Continue official source lookup for Unknown/descriptor cities and blank Canonical; steward duplicate risks. Keep address/Mapbox paused.

## Paused queues

- source_discovery_inserts
- address_confirmation
- coordinate_completion
- phone_number_enrichment
- rooms_keys
- description_extraction
- amenities_extraction


## Sprint rules

- Cleanup existing Hotel Property Census only — **no new inserts**
- **Paused:** address, Mapbox, phone, rooms, descriptions
- High-confidence City / State / Canonical fixes only
- No weak city inference from hotel name / coordinates / Google / Mapbox
