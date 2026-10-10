# Minimum Change Plan

| Priority | Change | Root cause | Expected impact | Files | Risk | Measurement |
|---|---|---|---|---|---|---|
| P0 | Wire campaign→child decomp for generators with official exhibitor/sponsor packs | B | +named accounts/Ready without threshold change | demand-campaigns/campaign-decomposition-orchestrator.js; research-orchestrator.js | med | children admitted / Ready / placeholder% |
| P0 | Hard-DQ venue/operator accounts lacking housing-control evidence at admission | C | cut false Ready / research waste | account-quality-taxonomy-v1.js; child-account-gate.js | low | venue contamination% |
| P0 | Second-gen pass: named company → company travel/buyer function queries | B/D/J | convert QUALIFIED→ACTIONABLE like AidEx | confirmation/buyer-path; expansion search templates | med | buyer-path resolution%; forensic apply rate |
| P1 | Prefer official list sources over SERP calendars in base weighting | A/J | higher useful/100 signals | hotel-weighting.js; base-queries.js | low | SOURCE_YIELD usefulPer100 |
| P1 | Buyer commercial-relevance forensic class (GENERIC_INTERNAL_CONTACT) | D | prevent Sinn-type false ACTIONABLE | buyer-path-resolution-v2.js | low | false ACTIONABLE rate=0 |
| P1 | Auto re-research trigger for HIGH_POTENTIAL_WATCH only | I | Watch maturation | future-watch + scheduler flag off by default | med | Watch→Ready conversion |
| P2 | Stop default Apify Tripadvisor comps | K | save cost | apify-base-map.js | low | $ / Ready |
| P2 | Keep Jev shadow for next-blocker only | Jev | no false confidence | jev-active-advisor | low | blockers resolved >0 before expand |
| P2 | Packet pillar required before expensive lodging crawl | E | cost discipline | complete-demand-packet-v8 on orchestrator | med | $ / complete packet |
