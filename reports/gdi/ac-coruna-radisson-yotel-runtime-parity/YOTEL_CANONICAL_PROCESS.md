# YOTEL Canonical Live GDI Process

## Exact live orchestration path (production)

1. **Hotel onboard** — `config/group-demand-intelligence/hotels/recrPQcZg7SFARRb2.json` + ADP fixture
2. **Blind / independent discovery** — seedCandidates via discovery pipeline (not Bethesda seeds)
3. **`runGroupDemandResearch(hotelId)`** — profile → ingest candidates → RAD enrich → qualification precision
4. **Campaign ensure (YOTEL)** — `buildYotelTenGeneratorCampaigns` + upsert when `GDI_YOTEL_SECOND_GEN_DECOMP_P0` enabled (default on)
5. **Shared campaign decomposition** — `runHotelDemandCampaignDecompositions` → official-list / evidence packs → child accounts
6. **Second-generation seeds** — traveling entity · buyer function · contact path · lodging motion
7. **Packet quality** — `evaluateCompleteDemandPacket` / completion (Jev shadow)
8. **Ready / Watch gates** — `isGdiCustomerOpportunityReady` / `isValidFutureWatch` (**thresholds unchanged**)
9. **Customer surface** — filterCustomerFacing + list DTO
10. **Lodging-decision intelligence** — watch card fields (controller / selection / outreach readiness)
11. **Pursuit handoff** — Watch with OUTREACH_NOW/PREPARE or Ready → shared Pursuit workflow

## Ten Bases role

`runTenBasesForHotel` is the **shared report/admin research harness** (Apify opt-in only; default off).
YOTEL **customer yield** is campaign → second-gen decomposition for a hardened subset of bases, not a full live Ten Bases SERP sweep on every research run.

## Control snapshot (2026-10-07)

- Opportunities: 99
- Facing FUTURE_WATCH: 24
- Demand campaigns: 10
- Pursuits: 0
- Apify on live path: **not invoked**
