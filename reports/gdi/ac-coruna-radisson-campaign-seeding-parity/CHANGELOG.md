# CHANGELOG — AC/Radisson campaign seeding parity

- Shared `campaign-from-opportunity-v1` admission + builder
- Evidence packs accept persisted `campaign.evidenceSeeds` (hotel-agnostic)
- Seeded AC (2) + RAD (3) campaigns from validated Watches; pursuits linked
- Bounded multilingual SERP discovery (no auto-admit thin hits)
- Shared `runHotelDemandCampaignDecompositions` invoked for both hotels
- Fix: decomp no longer wipes linked `opportunityIds` when researchLeads empty
- Fix: NATIVE lane emits per-language queries (es+gl) so Galician is not starved
- Galician supplement SERP: 2 queries, hits confirmed
- No Apify · no Ready threshold change · no hotel forks · pursuits preserved
