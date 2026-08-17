# Multi-Provider Census Validation

`DEALALITY_MULTI_PROVIDER_CENSUS_VALIDATION_COMPLETE`

## Overall decision
**PROVIDER_STACK_READY_EXCEPT_ROOM_COUNT**

## Room count
`ROOM_COUNT_PROVIDER_GAP_CONFIRMED` (frozen sample)  
Hotelbeds = SUPPORTED in adapter but quota_exhausted · StayingAPI/SerpApi/HotelAPI.co = NOT_SUPPORTED

## Field recovery (400)

| Field | Missing Before | HBX | StayingAPI | SerpApi | Combined | Still Missing | Recovery % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| address | 299 | 0 | 0 | 83 | 83 | 216 | 27.8% |
| coordinates | 351 | 0 | 0 | 106 | 106 | 245 | 30.2% |
| phone | 295 | 0 | 0 | 79 | 79 | 216 | 26.8% |
| website | 38 | 0 | 0 | 5 | 5 | 33 | 13.2% |
| room_count | 375 | 0 | 0 | 0 | 0 | 375 | 0% |
| brand | 104 | 0 | 0 | 0 | 0 | 104 | 0% |
| parent_company | 95 | 0 | 0 | 0 | 0 | 95 | 0% |

## HotelAPI.co
**DO_NOT_INTEGRATE_HOTELAPI_CO** (LOW census value)

## Highest-value next step
Restore LIVE Hotelbeds content access and measure rooms/keys yield on the same frozen 400 sample — that is the only current path to close the Rooms/Keys gap.

## Safety
Airtable/Census/Brand Explorer writes: 0 · Secrets exposed: no · Synthesis from prior read-only runs
