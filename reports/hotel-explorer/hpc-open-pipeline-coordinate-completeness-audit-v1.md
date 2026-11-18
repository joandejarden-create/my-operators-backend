# HPC Open/Pipeline Coordinate Completeness Audit v1

**Contract:** `HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1`
**Generated:** 2026-11-18T20:24:24.891Z
**Airtable writes:** none
**Paid geocoding:** none

## Totals

| Metric | Count |
| --- | ---: |
| Open + Pipeline | 21 |
| Open | 21 |
| Pipeline | 0 |
| Map-eligible (hard-pass) | 19 |
| Hard exceptions | 2 |
| Soft flags | 0 |
| Clean (no issues) | 19 |

## Hard exceptions

| HPC id | Name | Status | Issues | Lat | Lng | Address |
| --- | --- | --- | --- | --- | --- | --- |
| `recH9h3McgMNFQEla` | Radisson Blu Toronto Downtown | Open | MISSING_LATITUDE_OR_LONGITUDE | null | null | 249 Queens Quay West |
| `recHXCkkKD5Xwue3p` | UMusic Hotel Madrid | Open | MISSING_LATITUDE_OR_LONGITUDE | null | null | Calle de la Paz 11, 28012 Madrid, Spain |

## Soft flags (review)

_None._

## Proposed corrections (staged — no write)

### Radisson Blu Toronto Downtown (`recH9h3McgMNFQEla`)

| Field | Before | After |
| --- | --- | --- |
| Latitude | null | 43.6387 |
| Longitude | null | -79.3855 |
| Coordinate Source Type | null | third_party_map_consensus_pending_official_geocode |
| Coordinate Confidence | null | Medium |
| Geocode Provider | null | null |
| Geocode Method | null | null |
| Geocode Reviewed Date | null | null |

- Address evidence: 249 Queens Quay West, Toronto, Canada M5J 2N5
- ADP published coords reusable: **false**
- Confidence: `UNVERIFIED_PENDING_FOUNDER_GEOCODE_AUTH`
- Prefer official permanent geocode of 249 Queens Quay West, Toronto, ON M5J 2N5 before Airtable write. Do not use Toronto city-center (43.6532, -79.3832).

### UMusic Hotel Madrid (`recHXCkkKD5Xwue3p`)

| Field | Before | After |
| --- | --- | --- |
| Latitude | null | null |
| Longitude | null | null |
| note | null | No coordinates invented. Full street address present — ready for authorized permanent geocode only. |

- Address evidence: Calle de la Paz 11, 28012 Madrid, Spain, Madrid, Spain 28012
- ADP published coords reusable: **undefined**
- Confidence: `NO_COORDINATE_PROPOSAL_WITHOUT_GEOCODE_AUTH`
- Do not use Madrid city-center (40.4168, -3.7038). Authorize Mapbox permanent / Google storage-terms geocode of Calle de la Paz 11, 28012 Madrid, Spain.

## Machine-readable

`reports\hotel-explorer\hpc-open-pipeline-coordinate-completeness-audit-v1.json`
