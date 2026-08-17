# StayingAPI Benchmark V1 — Final Report

## MOST IMPORTANTLY

**NO**

Recommendation: **DO NOT INTEGRATE**

Can StayingAPI materially help close Address / Coordinate / Amenity gaps without unacceptable match risk?

**Answer: NO**

---

1. Hotels tested: **25**
2. Found (≥1 candidate): **5**
3. Exact: **0**
4. High: **3**
5. Medium: **0**
6. Low/Reject: **22**
7. False matches: **0**
8. Correct-property-first-result rate: **12%**
9. Address control agreement: **n/a%** (0/0 eligible controls)
10. Address gaps resolved: **20%** (3/15)
11. Coordinate control agreement (≤500m): **n/a%**
12. Coordinate gaps resolved: **20%**
13. Median coordinate distance vs controls: **n/a m**
14. Amenities coverage: tokens mapped via controlled taxonomy when present; absence=UNKNOWN
15. Amenities agreement: informational — official comparison limited; Yes-only proposals
16. Property Type usefulness: MAPPABLE for hotel/resort; REFERENCE for house/cottage
17. Total hotel Rooms / Keys exposed?: **NO** (`NOT_SUPPORTED`)
18. Did bedrooms map to Rooms / Keys?: **NO**
19. Total credits consumed: **40** (start available 295, end 220)
20. Credits per tested hotel: **1.6**
21. Credits per useful gap resolved: **6.7**
22. Underlying platforms in best matches: {"booking":5,"none":20}
23. Family Exact+High rates: {"IHG":{"n":9,"eh":3},"Hilton":{"n":9,"eh":0},"Choice":{"n":7,"eh":0}}
24. Sibling false-match risk flags: **0**
25. Allowed propose fields: Address, Lat/Lng, Property Type (mapped), Amenities (Yes-only), identity metadata
26. Never populate: Rooms/Keys, Owner, Operator, Opening, Floors, F&B counts, Meetings, images to production
27. Permanent Lane B?: **NO / NOT YET**
28. Independent discovery later?: **YES as architecture candidate** (not built)
29. Rights remaining: production persistence, customer-facing derived use, image reuse — all PENDING
30. Exact integration: Exact/High-only Lane B for Address/Coords/Amenities/Type after official ladder; never Rooms; no Autopilot change until rights confirmed

Identity Exact+High rate: **12%**

## Benchmark caveats (honest)

- Free-plan rate limits required backoff/retries; an earlier aborted run saw all-429 empty results (not used as verdict).
- Account available credits moved **295 → 220** while the response \`meta.creditsCharged\` sum was **40** — treat account delta as the conservative cost signal for budgeting (async settlement / platform fan-out may not fully appear in per-response meta).
- Control group (**B**): **0/10** Exact+High matches → control address/coordinate agreement metrics are **n/a** (cannot claim ≥95% control agreement).
- Search used \`platforms=booking\` only to conserve credits; Hilton/Choice Mexico coverage was weak under this constraint.
- When Exact/High succeeded (3 IHG gap hotels), Booking.com addresses looked street-level and useful as **candidate** enrichment only.
- \`STAYINGAPI_ROOMS_CAPABILITY = NOT_SUPPORTED\`; bedrooms never mapped to Rooms / Keys.
- Autopilot production ladder **not modified**.
