# CHANGELOG — YOTEL Second-Generation Decomposition P0

## 2026-10-05

- Added `lib/group-demand-intelligence/demand-campaigns/yotel-second-generation-p0.js`
  - Target campaigns, traveling-entity mapping, contamination gate, buyer commercial relevance
  - Official-list seeds from `YOTEL_ACCOUNT_RESEARCH` + AI for Good curated 2026 partners
  - WHO/WHA/ECOSOC public-data-ceiling SIGNAL_ONLY (no invented member states)
- Wired second-gen merge into `campaign-evidence-packs.js` (live pack path)
- Admission + child stamps in `campaign-decomposition-orchestrator.js`
  - Blocks VENUE / ORGANIZER-without-housing / GENERIC_ORG_SHELL / NO_TRAVELING_ENTITY
  - Stamps travelingEntity*, contactPathClass, parent links, accountId, sourceEvidence
- Live research path: `research-orchestrator.js` runs YOTEL second-gen campaign decomp when enabled
- Env: `GDI_YOTEL_SECOND_GEN_DECOMP_P0` (default on; set 0 to disable)
- No Apify. No Ready threshold changes. Jev shadow/advisory only.
- Protected AidEx/CHI/SETAC Ready not demoted.
