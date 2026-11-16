# AI_DEMAND_REVIEWS_TRACE

Root cause of published_manifest_missing:
- resolveAdpMonthlyReviewEligibilityV1.loadManifest only read data/ai-demand-positioning/published/{id}/manifest.json
- listPublishedPropertyIds included Toronto via durable registry
- primary checkout lacked legacy published/ tree → blocker published_manifest_missing → coverage BLOCKED

Fix:
- loadPublishedManifest falls back to durable published-current
- eligibility uses loadPublishedManifest + loadPublishedReport
- Performance Review PDF attached for adp_mr_radisson_blu_toronto_dow_2026-11_v1

Was customer published manifest incorrectly required for admin visibility? YES (legacy path). Durable current is sufficient for internal admin eligibility.
