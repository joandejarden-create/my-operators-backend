# False positive analysis

## Status: Test 6 said keep Pipeline; native proposed Open

- Hotel Indigo Mexico City Downtown: Pipeline → Open (confidence 0.65)
- Hotel Indigo Tulum: Pipeline → Open (confidence 0.65)
- Hotel Indigo Guadalajara Providencia: Pipeline → Open (confidence 0.65)

Likely cause: fuzzy IHG directory match attached a live bookable property page to the wrong census pipeline hotel.

## Likely bad reflag matches

- Hotel Indigo Mexico City Downtown: Hotel Indigo → intercontinental (confidence 0.65)
- Faranda Collection Cali A Member Of Radisson Individuals: Radisson Individuals Americas → Ascend Collection (confidence 0.25)
- Hotel Indigo Tulum: Hotel Indigo → holidayinn (confidence 0.65)
- Casa Francia, Autograph Collection: Autograph Collection → Tribute Portfolio (confidence 0.6)

Casa Francia Autograph→Tribute may be a true native-only finding (verify on marriott.com) — treat as review, not auto-false.
