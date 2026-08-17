# GIATA MHG vs Tripadvisor room-count decision

**Marker:** `GIATA_TRIPADVISOR_ROOM_DECISION_COMPLETE`  
**Warning:** `TEST_SAMPLE_NOT_VALID_FOR_GEOGRAPHIC_COVERAGE`  
**Production writes:** 0

## 1. Executive summary

Tripadvisor can **find** room counts (v2: 100% candidate rate on matched CALA sample) but only **~2%** convert to independently verified primary/multi-source values.

GIATA MHG TEST confirms structured `num_rooms_total` (total property keys). That does **not** automatically justify a €4,950 / 24-month MHG commitment: TEST geography is random, Dealality trusted-room overlap is limited, and **TA↔GIATA source independence is UNCERTAIN**.

**Decision: `KEEP_GIATA_OPTION_OPEN`**

TEST entitlement returns random non-CALA properties; trusted Dealality∩GIATA∩Tripadvisor ground-truth overlap is too small to justify a 24-month €4,950 MHG commit. MHG num_rooms_total is schema-confirmed, but production CALA coverage/accuracy remains unproven. Tripadvisor finds candidates but verification conversion is ~2%; GIATA could help later if production sample proves accuracy + if independence is clarified — not BUY_NOW.

## 2. Phase 1 — GIATA access (no secrets)

| Product | Credentials | Auth |
| --- | --- | --- |
| MHG TEST | false | false |
| MultiCodes TEST | true | true |

Confirmed MHG fields: GIATA ID, name, city/country, `num_rooms_total`. Update/provenance metadata: **not observed** in TEST payloads.

## 3. Sample construction

- MHG-driven sample (random TEST hotels), MultiCodes enrichment for geo when available.
- Tripadvisor match via existing Actor pool (+ optional decision dataset).
- Dealality trusted rooms only when high-confidence name/country identity match.
- Not forced to CALA-only (TEST cannot support geographic validity).

| Metric | Value |
| --- | --- |
| OVERLAP_SAMPLE | 40 |
| GIATA with rooms | 29 |
| Both TA+GIATA rooms | 17 |
| Trusted Dealality rooms overlap | 0 |

## 4. GIATA vs trusted Dealality rooms

Tolerance: EXACT / NEAR (≤5 keys or ≤5%) / CONFLICT — same as Tripadvisor.

| Metric | Value |
| --- | --- |
| GIATA_ROOM_COVERAGE | 72.5% |
| GIATA_EXACT_ACCURACY | n/a% |
| GIATA_NEAR_MATCH | n/a% |
| GIATA_CONFLICT_RATE | n/a% |

Trusted rows: 0.

## 5. Tripadvisor vs GIATA

| Metric | Value |
| --- | --- |
| TA_GIATA_AGREEMENT (exact+near) | 76.5% |
| TA_GIATA_CONFLICT | 23.5% |
| Agree ∩ ground-truth accuracy | n/a% (n=0) |
| GIATA wins conflicts | 0 |
| Tripadvisor wins conflicts | 0 |
| Both wrong | 0 |

## 6. Simulated production waterfall (no writes)

Statuses: VERIFIED_PRIMARY_SOURCE → VERIFIED_TA_GIATA (only if independence allows) → GIATA_ONLY → TA_ONLY → CONFLICT_REVIEW_REQUIRED → UNRESOLVED.  
Agreements under uncertain independence → `SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT` (**not** counted as verified).

```json
{
  "CONFLICT_REVIEW_REQUIRED": 4,
  "SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT": 13,
  "GIATA_ONLY": 12,
  "TA_ONLY": 4,
  "UNRESOLVED": 7
}
```

- SIMULATED_VERIFIED_COVERAGE: **0%**
- SIMULATED_REVIEW_REQUIRED: **10%**

## 7. Source independence

**Assessment: `INDEPENDENCE_UNCERTAIN`**

- Tripadvisor hotelClassAttribution referencing Giata documents star/class provenance, not room inventory.
- In this decision TA pool, 29/31 hotels (~94%) show Giata class attribution — deep Tripadvisor↔Giata coupling for classification raises (but does not prove) shared-upstream risk for inventory fields.
- No Tripadvisor schema field or GIATA entitlement doc in this repo proves numberOfRooms ← MHG num_rooms_total.
- TA↔GIATA room conflicts on high-confidence matches show fields are not always identical (freshness/definition divergence possible even under partial shared upstream).
- Therefore TA+GIATA agreement must NOT auto-qualify as VERIFIED_MULTI_SOURCE / VERIFIED_TA_GIATA without additional independent primary evidence.

## 8. Economics

| Option | Notes |
| --- | --- |
| A. Tripadvisor-only candidates | Cheap discovery; not authoritative |
| B. TA + verification waterfall | Correct architecture; ~2% verified conversion today |
| C. GIATA MHG alone | Structured keys; €4,950 / 24mo; TEST coverage not CALA-proof |
| D. TA + GIATA hybrid | Attractive **if** independence proven; today agreement ≠ multi-source verify |

GIATA license reminders: store during license; delete on termination; independently verified facts / non-reconstructable analytics may remain; combine OK; attribution required; no DB redistribution.

## 9. Hotel-level overlap (TA+GIATA rooms)

| Hotel | Country | GIATA | TA | Trusted | TA vs GIATA | Simulated |
| --- | --- | ---: | ---: | ---: | --- | --- |
| Old Cataract, Aswan | Egypt | 123 | 138 | — | CONFLICT | CONFLICT_REVIEW_REQUIRED |
| Aracan Eatabe Luxor Hotel | Egypt | 314 | 314 | — | EXACT | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| Iberotel Luxor by JAZ | Egypt | 185 | 185 | — | EXACT | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| Hotel Sporting Rimini | Italy | 88 | 110 | — | CONFLICT | CONFLICT_REVIEW_REQUIRED |
| Hotel Pietra di Luna | Italy | 96 | 96 | — | EXACT | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| IL Gabbiano Hotel | Italy | 19 | 19 | — | EXACT | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| Grand Hotel Excelsior | Italy | 97 | 66 | — | CONFLICT | CONFLICT_REVIEW_REQUIRED |
| Barceló Puerto Vallarta | Mexico | 316 | 316 | — | EXACT | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| Auramar Beach Resort | Portugal | 287 | 287 | — | EXACT | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| Belver Boa Vista Hotel &amp; Spa | Portugal | 84 | 84 | — | EXACT | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| Hotel Indigo Albufeira | Portugal | 73 | 80 | — | CONFLICT | CONFLICT_REVIEW_REQUIRED |
| Vila Galé Cerro Alagoa | Portugal | 310 | 310 | — | EXACT | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| Vila Galé Atlantico | Portugal | 220 | 220 | — | EXACT | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| Pestana Blue Alvor Beach ALL INCLUSIVE | Portugal | 312 | 325 | — | NEAR_MATCH | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| Hotel La Fonda | Spain | 28 | 27 | — | NEAR_MATCH | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| Parador de Málaga Gibralfaro | Spain | 38 | 38 | — | EXACT | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |
| Parador de Málaga Golf | Spain | 88 | 88 | — | EXACT | SOURCE_INDEPENDENCE_UNCERTAIN_AGREEMENT |

## 10. Decision

```
DECISION: KEEP_GIATA_OPTION_OPEN
GIATA_24_MONTH_COST: €4,950
PRODUCTION_WRITES: 0
```
