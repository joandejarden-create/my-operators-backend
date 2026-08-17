# Dealality Batch Learning Ledger

**Version:** `dealality-batch-learning-system-v1`  
**Generated:** 2026-08-09T18:38:48.334Z  
**Airtable writes:** false  
**Brand Explorer patches:** false

## Summary

| Metric | Value |
| --- | ---: |
| Entries | 62 |
| Census | 45 |
| Brand Explorer | 17 |
| Implemented/Validated | 45 |
| Proposed | 11 |
| Steward-only | 3 |
| Webhound | 2 |

## Classification types

- `learned_code_rule`
- `learned_validation_rule`
- `learned_source_pattern`
- `learned_block_reason`
- `steward_review_case`
- `Webhound_candidate`
- `do_not_learn_noise`

## Entries

### `census-coord-first-pass-132-safe`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_first_pass_enrichment |
| Issue type | `learned_validation_rule` |
| Status | `validated` |
| Source | `reports/research-engine-v2/production-census-first-pass-enrichment-apply.json` |
| Module | `lib/research-engine-v2/production-census-first-pass-enrichment.js` |
| Fixture | no |
| Test | no |

**Pattern:** Only write property-level coordinates with official directory/freeze evidence; reject 0,0; never mark held records public eligible.

**Proposed change:** Keep first-pass validation gates in production-census-first-pass-enrichment + coordinate resolver validation.

**Next:** Re-run coordinate validation on every Census geography batch before apply.

### `census-coord-active-missing-293`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_first_pass_enrichment |
| Issue type | `learned_block_reason` |
| Status | `proposed` |
| Source | `reports/research-engine-v2/production-census-first-pass-enrichment-apply.json` |
| Module | `lib/research-engine-v2/production-census-address-geocode-resolver.js` |
| Fixture | no |
| Test | no |

**Pattern:** Active-brand missing coordinates are the next enrichment lane — not Webhound bulk; use code resolver then address-first geocode.

**Proposed change:** Queue missing coords through production-census-coordinate-resolver then address-geocode-resolver dry-runs.

**Next:** Founder provider/terms decision → expand geocode dry-run → approved apply only.

### `census-marriott-hqv-source-pattern`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_coordinate_learning_sidecar_closed |
| Issue type | `learned_source_pattern` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/webhound-coordinate-learning-sidecar-closed.json` |
| Module | `lib/research-engine-v2/marriott-hqv-coordinate-client.js` |
| Fixture | no |
| Test | no |

**Pattern:** Marriott Mexico: seed MARSHA from sitemap `/hotels/([A-Z0-9]{5})-` then HQV GraphQL basicInformation.lat/lng; overview HTML usually has no coords.

**Proposed change:** Keep marriott-hqv-coordinate-client + extractor crawler rules; harvest MARRIOTT_GRAPHQL_OPERATION_SIGNATURE for Akamai/safelist.

**Next:** Harvest GraphQL signature via browser/XHR; retry HQV dry-run; do not restart Webhound full-census.

### `census-marriott-ihg-official-page-blocked`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_coordinate_resolver |
| Issue type | `learned_block_reason` |
| Status | `proposed` |
| Source | `reports/research-engine-v2/production-census-coordinate-resolver-dry-run.json` |
| Module | `lib/research-engine-v2/production-census-coordinate-resolver.js` |
| Fixture | no |
| Test | no |

**Pattern:** official_page_blocked / Akamai on Marriott+IHG Node fetch is a block reason — route to HQV/signature path or address-first geocode, not fabricate coords.

**Proposed change:** Classifier maps official_page_blocked → address-first lane or steward; never city centroid fallback.

**Next:** Prefer VIC/census street address geocode when HTML fetch blocked.

### `census-address-first-vic-78-street`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_address_geocode_resolver |
| Issue type | `learned_source_pattern` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-address-geocode-resolver-dry-run.json` |
| Module | `lib/research-engine-v2/production-census-address-geocode-resolver.js` |
| Fixture | no |
| Test | no |

**Pattern:** When Census Address is blank, VIC Address 1 claims are valid official street seeds for address-first geocode (still confirm city/country).

**Proposed change:** Address-first resolver already reads VIC claims; on apply, write Address only when blank and confirmed.

**Next:** Expand geocode-limit after provider/terms approval.

### `census-address-geocode-34-proposals-terms-block`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_address_geocode_resolver |
| Issue type | `learned_validation_rule` |
| Status | `validated` |
| Source | `reports/research-engine-v2/production-census-address-geocode-resolver-dry-run.json` |
| Module | `lib/research-engine-v2/production-census-geocoding-providers.js` |
| Fixture | yes |
| Test | yes |

**Pattern:** Geocode proposals may appear in dry-run under Google, but apply is blocked until Mapbox permanent or Google storage terms are reviewed.

**Proposed change:** Keep GEOCODING_PROVIDER + MAPBOX_PERMANENT_GEOCODING / GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED gates.

**Next:** Founder chooses Mapbox Permanent (recommended) or confirms Google terms; then re-run dry-run.

### `census-provider-prefer-mapbox-permanent`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_address_geocode_resolver |
| Issue type | `learned_code_rule` |
| Status | `implemented` |
| Source | `docs/data-intelligence/production-census-address-geocode-resolver.md` |
| Module | `lib/research-engine-v2/production-census-geocoding-providers.js` |
| Fixture | yes |
| Test | yes |

**Pattern:** Production default for stored Census coordinates: Mapbox with permanent geocoding enabled. Block Google apply until terms reviewed. Never use public Nominatim for bulk production.

**Proposed change:** Documented in geocoding providers + .env.example; enforce terms_block_apply in resolver status.

**Next:** Provision MAPBOX_ACCESS_TOKEN + MAPBOX_PERMANENT_GEOCODING=1 for production apply path.

### `census-schema-v113-geocode-provenance`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_address_geocode_resolver |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-address-geocode-resolver-dry-run.json` |
| Module | `lib/research-engine-v2/production-census-schema-v113-coordinate-provenance-fields.js` |
| Fixture | yes |
| Test | no |

**Pattern:** Coordinates and addresses require provenance fields before production geocode writes. Capture provider/method/confidence in dry-run until schema v1.1.3 is applied.

**Proposed change:** Create schema v1.1.3 provenance fields (schema-only); then map address-geocode apply to write them.

**Next:** Use provenance fields on next approved address-geocode apply; provider/terms decision still required.

### `census-schema-v113-provenance-fields-applied`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_schema_v113_coordinate_provenance |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-schema-v113-coordinate-provenance-fields.json` |
| Module | `lib/research-engine-v2/production-census-schema-v113-coordinate-provenance-fields.js` |
| Fixture | yes |
| Test | no |

**Pattern:** Coordinates and addresses require provenance fields before production geocode writes.

**Proposed change:** Schema v1.1.3 fields added on Hotel Property Census (101→108); leave blank until approved geocode apply.

**Next:** Founder provider/storage decision → address-geocode dry-run with provenance mapping → apply under confirm flags.

### `census-shared-campus-downgrade-later`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_coordinate_resolver |
| Issue type | `steward_review_case` |
| Status | `steward_only` |
| Source | `reports/research-engine-v2/production-census-coordinate-resolver-dry-run.json` |
| Module | `lib/research-engine-v2/production-census-coordinate-resolver.js` |
| Fixture | no |
| Test | no |

**Pattern:** Identical coordinates across adjacent campus properties → Medium confidence / shared_campus_pin; optional later Public Display Confidence downgrade — do not auto-delete coords.

**Proposed change:** Already flagged in coordinate resolver first-pass validation as downgrade_later.

**Next:** Steward decide whether to soften Public Display Confidence only (no coord wipe).

### `census-webhound-hard-case-only`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_coordinate_learning_sidecar_closed |
| Issue type | `Webhound_candidate` |
| Status | `Webhound_only` |
| Source | `reports/research-engine-v2/webhound-coordinate-learning-sidecar-closed.json` |
| Module | `docs/data-intelligence/dealality-batch-learning-system.md` |
| Fixture | no |
| Test | no |

**Pattern:** Webhound sample 10–25 for pattern discovery only; production writes from Webhound = 0; convert to code fixtures/tests before trust.

**Proposed change:** Do not start new full-census Webhound runs; optional tiny signature-harvest research only if code path still blocked.

**Next:** Keep Webhound closed for bulk coords; CALA listing learning completed 2026-08-05 — reopen only for new hard pattern classes (e.g. HQV signature harvest).

### `census-reject-pin-false-positive-fix`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_coordinate_resolver |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-coordinate-resolver-dry-run.json` |
| Module | `lib/research-engine-v2/production-census-coordinate-extractor.js` |
| Fixture | yes |
| Test | yes |

**Pattern:** Do not reject downtown/airport hotel pins solely for proximity to tourism centroids/airports when property name has locality cues; only reject null-island and coarse city-level precision without cues.

**Proposed change:** matchesRejectedPin with propertyName locality cues — already implemented in production-census-coordinate-extractor.js.

**Next:** Add unit fixtures for Centro/Airport vs true coarse centroid cases.

### `census-descriptions-still-zero`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_first_pass_enrichment |
| Issue type | `learned_block_reason` |
| Status | `proposed` |
| Source | `reports/research-engine-v2/production-census-first-pass-enrichment-apply.json` |
| Module | `lib/research-engine-v2/production-census-first-pass-enrichment.js` |
| Fixture | no |
| Test | no |

**Pattern:** Do not fabricate Hotel Description; wait for grounded official source text before description lane.

**Proposed change:** Keep description writes gated on VIC/source evidence only.

**Next:** Separate description extraction batch after official page text extractors improve.

### `census-blocked-queue-owner-rooms-dates`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_first_pass_enrichment |
| Issue type | `steward_review_case` |
| Status | `steward_only` |
| Source | `reports/research-engine-v2/production-census-first-pass-enrichment-apply.json` |
| Module | `lib/research-engine-v2/production-census-first-pass-enrichment.js` |
| Fixture | no |
| Test | no |

**Pattern:** Owner/operator/rooms/dates stay blank until dedicated validated lanes; queue is research-only, never silent write.

**Proposed change:** Preserve FORBIDDEN_WRITE_FIELDS across all Census enrichment modules.

**Next:** Prioritize separate owner/rooms research lanes after geography completeness.

### `census-population-lane-2-provenance-enrichment`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_population_lane_2 |
| Issue type | `learned_code_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-population-lane-2-apply.json` |
| Module | `lib/research-engine-v2/production-census-population-lane-2.js` |
| Fixture | no |
| Test | no |

**Pattern:** Backfill coordinate provenance from first-pass evidence without changing lat/lng; keep geocode apply behind provider/storage terms; enrich only VIC-grounded gaps; blank descriptions beat fabricated AI.

**Proposed change:** production-census-population-lane-2 module + npm command; Mapbox Permanent or Google terms before geocode writes.

**Next:** Founder provider/storage decision for 34 geocode proposals; separate official-page description extractor lane.

### `census-lane-2-descriptions-need-page-extractors`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_population_lane_2 |
| Issue type | `learned_block_reason` |
| Status | `proposed` |
| Source | `reports/research-engine-v2/production-census-population-lane-2-apply.json` |
| Module | `lib/research-engine-v2/production-census-population-lane-2.js` |
| Fixture | no |
| Test | no |

**Pattern:** VIC amenity/type claims are insufficient for descriptions; need official property-page text extractors (Hilton/Choice/IHG/Marriott) before description writes.

**Proposed change:** Add family-specific description extractors grounded in official HTML/JSON-LD; gate AI Summary on Source Text only.

**Next:** Build description extraction lane with fixtures; do not reopen Webhound for bulk descriptions.

### `census-next-lane-description-extraction-dry-run`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_description_extraction |
| Issue type | `learned_code_rule` |
| Status | `proposed` |
| Source | `reports/research-engine-v2/production-census-description-extraction-dry-run.json` |
| Module | `lib/research-engine-v2/production-census-description-extraction.js` |
| Fixture | yes |
| Test | yes |

**Pattern:** Do not let geocode provider decision block all Census enrichment; route to official-page description extraction; reject booking boilerplate; never treat JS 'captcha' string as page block.

**Proposed change:** production-census-description-extraction + extractor + next-lane router; puppeteer/API path still needed for Hilton/Choice/Marriott.

**Next:** Founder review IHG description dry-run; apply after confirms; set Mapbox Permanent for 34 geocodes separately.

### `census-ihg-description-extraction-apply`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | ihg_description_extraction_apply |
| Issue type | `learned_code_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-description-extraction-ihg-apply.json` |
| Module | `lib/research-engine-v2/production-census-description-extraction.js` |
| Fixture | yes |
| Test | yes |

**Pattern:** Apply description batches per family after dry-run approval; rebuild patches from official refetch keyed by identity_key; never include geocode/owner/rooms in description apply.

**Proposed change:** runIhgDescriptionApply with confirm-ihg-only + grounded-source-text gates.

**Next:** Next family description lane after safe fetch; Mapbox Permanent for 34 geocodes.

### `census-rooms-keys-queue-engine`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | rooms_keys_missing_queue |
| Issue type | `learned_code_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-rooms-keys-queue.json` |
| Module | `lib/research-engine-v2/production-census-rooms-keys-queue.js` |
| Fixture | yes |
| Test | yes |

**Pattern:** Treat Rooms/Keys as early Census queue with provenance; never invent or brand-average; reject booking-widget false positives; Hold mixed-use ambiguity.

**Proposed change:** census:queue-run rooms_keys_missing + extractor fixtures; schema v1.1.4 Rooms Source Type/Notes/Hold applied via production-census-schema-v114.

**Next:** Continue controlled Autopilot queues after first rooms apply; keep geocode blocked until provider decision.

### `census-autopilot-first-rooms-keys-apply`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_autopilot_first_rooms_apply |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-autopilot-first-rooms-apply.json` |
| Module | `lib/research-engine-v2/census-autopilot-approval-bundle-apply.js` |
| Fixture | yes |
| Test | no |

**Pattern:** First live Autopilot writes must be approval-bundle-bound: freeze dry-run High patches, re-read, idempotent blank write, no re-plan.

**Proposed change:** census:autopilot-first-rooms-apply — rooms-only from controlled dry-run.json

**Next:** Continue controlled Autopilot queues; property_name_cleanup next; keep geocode blocked until provider decision.

### `census-property-name-cleanup-queue`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | property_name_cleanup_queue |
| Issue type | `learned_code_rule` |
| Status | `proposed` |
| Source | `reports/research-engine-v2/production-census-property-name-cleanup-queue.json` |
| Module | `lib/research-engine-v2/production-census-property-name-cleanup-queue.js` |
| Fixture | yes |
| Test | yes |

**Pattern:** Property Name cleanup is an early Autopilot queue: detect marketing/tagline names; replace only with official-page High clean names; never invent.

**Proposed change:** property_name_cleanup queue + --queue targeted runs; controlled dry-run before apply.

**Next:** Founder review High name-cleanup proposals; approval-bundle-bound apply if clean.

### `census-property-name-cleanup-apply`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_property_name_cleanup_apply |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-property-name-cleanup-apply.json` |
| Module | `lib/research-engine-v2/census-autopilot-property-name-cleanup-apply.js` |
| Fixture | yes |
| Test | yes |

**Pattern:** Property Name cleanup applies must be approval-bundle-bound to founder-approved identity→name map; re-read + confirm malformed before write.

**Proposed change:** census:autopilot-property-name-cleanup-apply — 5 avid hotels from controlled dry-run

**Next:** Continue Autopilot controlled queues; keep geocode blocked until provider decision.

### `census-schema-v114-rooms-keys-provenance-applied`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_schema_v114_rooms_keys_provenance |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-schema-v114-rooms-keys-provenance-apply.json` |
| Module | `lib/research-engine-v2/production-census-schema-v114-rooms-keys-provenance.js` |
| Fixture | yes |
| Test | yes |

**Pattern:** Rooms / Keys writes require provenance fields and Hold on confidence before Autopilot apply.

**Proposed change:** Schema v1.1.4 fields on Hotel Property Census; leave blank until approved rooms Autopilot apply.

**Next:** Controlled Autopilot: --scope active-brand-setup --mode controlled --strategy fastest-safe.

### `census-autopilot-runner-parent-region`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_autopilot_runner |
| Issue type | `learned_code_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-autopilot-runner.json` |
| Module | `lib/research-engine-v2/census-autopilot-runner.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Founder specifies parent company and region; Autopilot plans/dry-runs/controlled/applies safe High-confidence Census queues in order; Webhound remains hard-case learning only.

**Proposed change:** census:autopilot + confidence/apply-guard/queue-router modules; v1.1.4 Rooms provenance plan.

**Next:** Run plan then dry-run per parent (IHG/Marriott); apply High-only after founder confirms; create v1.1.4 schema when approved.

### `census-autopilot-v2-batch-apply`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_autopilot_v2_batch_apply |
| Issue type | `learned_code_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-autopilot-v2-batch-apply.json` |
| Module | `lib/research-engine-v2/census-autopilot-runner.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Autopilot v2 batch-by-batch production Census writes: --batch-size chunks, --run-until-complete for full scope, idempotent re-read before write, hard cases route instead of blocking whole run.

**Proposed change:** census-autopilot-batch-engine + checkpoint + idempotent-writer + field-allowlist; --enable-production-writes for live apply.

**Next:** Founder runs controlled then apply --run-until-complete with confirms; create v1.1.4 Rooms provenance when approved.

### `census-autopilot-active-brand-setup-fastest-safe`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_autopilot_active_brand_setup_fastest_safe |
| Issue type | `learned_code_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-active-brand-setup-scope.json` |
| Module | `lib/research-engine-v2/census-autopilot-runner.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Autopilot uses Brand Setup Active/Live as read-only control list, matches Census, scores queues fastest-safe, batch-applies High-confidence Census fields until complete with checkpoint/resume and runtime guardrails.

**Proposed change:** active-brand-scope + brand-census-matcher + fastest-safe + runtime-guardrails modules; Brand Setup/Explorer never written.

**Next:** Founder runs plan/controlled with --scope active-brand-setup --strategy fastest-safe; apply only after approval + --enable-production-writes.

### `census-webhound-cala-source-discovery-learning`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_cala_source_discovery_learning |
| Issue type | `learned_source_pattern` |
| Status | `proposed` |
| Source | `reports/research-engine-v2/webhound-cala-source-discovery-learning.json` |
| Module | `lib/research-engine-v2/census-autopilot-source-discovery.js` |
| Fixture | no |
| Test | no |

**Pattern:** CALA listing discovery uses official family directories/sitemaps only. Marriott: /en-us/hotel-sitemap/{country}-hotel-sitemap → MARSHA5. IHG: destinations/{country}-hotels + bin hoteldetail XMLs. Hilton: /en/locations/{country}/. Choice: /en-uk/{country}/regional-hotels. Webhound learns patterns; code adapters populate Census.

**Proposed change:** Wire marriott country sitemap into Autopilot source_discovery first; then parameterize Hilton/Choice beyond Mexico; then wire IHG destination extract.

**Next:** Build Marriott Autopilot listing adapter (recommendation B); dry-run source_discovery only.

### `census-marriott-cala-sitemap-adapter-wired`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_marriott_discovery_adapter |
| Issue type | `learned_source_pattern` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-marriott-discovery-adapter.json` |
| Module | `lib/research-engine-v2/census-autopilot-source-discovery.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Autopilot source_discovery now crawls Marriott country hotel-sitemaps for priority CALA countries without HQV.

**Proposed change:** Keep marriott discovery adapter as listing SoT; HQV stays enrichment lane.

**Next:** Covered by multi-parent sprint; founder insert review for Marriott High candidates.

### `census-marriott-cala-sitemap-adapter-opportunity`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_marriott_discovery_adapter |
| Issue type | `learned_code_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-marriott-discovery-adapter.json` |
| Module | `lib/research-engine-v2/census-autopilot-marriott-discovery-adapter.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Marriott listing discovery uses country hotel-sitemap via countrySitemapUrl + __NEXT_DATA__; HQV is enrichment-only; identity ind_marriott_{cc}_{marsha} with cr≠co.

**Proposed change:** Implemented census-autopilot-marriott-discovery-adapter + Autopilot source_discovery wiring; coverage ready for priority CALA countries.

**Next:** Founder review Marriott approval-bundle (93 High); multi-parent Hilton/IHG/Choice adapters shipped in same sprint.

### `census-discovery-skip-be-slug-on-insert`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_marriott_discovery_adapter |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-marriott-discovery-adapter.json` |
| Module | `lib/research-engine-v2/census-autopilot-source-discovery.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Do not set Brand Explorer Slug on Census discovery inserts; Autopilot forbids BE-facing fields. Abort insert only when core identity fields are forbidden — not optional drops.

**Proposed change:** buildInsertFieldsFromDiscovered omits Brand Explorer Slug; approval bundle ignores non-fatal forbidden drops.

**Next:** Keep Brand Explorer untouched on all discovery insert paths.

### `census-hilton-choice-non-mexico-adapter-opportunity`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_multi_parent_cala_discovery_adapters |
| Issue type | `learned_code_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-multi-parent-cala-discovery-adapters.json` |
| Module | `lib/research-engine-v2/census-autopilot-source-discovery.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Hilton and Choice Mexico adapters generalize via country-parameterized CALA adapters; Autopilot source_discovery walks priority countries (MX/DR/CR/CO/PA).

**Proposed change:** Implemented census-autopilot-hilton-cala-discovery-adapter + census-autopilot-choice-cala-discovery-adapter; coverage matrix v3 marks priority 5×4 supported.

**Next:** Founder insert review; optional Choice Jamaica/Bolivia/USVI sitemap-only follow-up.

### `census-multi-parent-cala-discovery-adapters-wired`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_multi_parent_cala_discovery_adapters |
| Issue type | `learned_source_pattern` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-multi-parent-cala-discovery-adapters.json` |
| Module | `lib/research-engine-v2/census-autopilot-source-discovery.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Ship multi-parent CALA listing adapters together (Marriott/IHG/Hilton/Choice) so Autopilot source_discovery is not one-parent-at-a-time. Priority countries only for readiness=supported.

**Proposed change:** Wire IHG + Hilton/Choice non-Mexico + parent×country matrix; controlled discovery only — no apply.

**Next:** Founder review approval bundles (Marriott 93 / IHG 37 / Hilton 56 / Choice 28 / Active Setup 91); apply only with confirms.

### `census-ihg-cala-destination-adapter-wired`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_multi_parent_cala_discovery_adapters |
| Issue type | `learned_source_pattern` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-multi-parent-cala-discovery-adapters.json` |
| Module | `lib/research-engine-v2/census-autopilot-ihg-cala-discovery-adapter.js` |
| Fixture | no |
| Test | yes |

**Pattern:** IHG CALA listing discovery uses official destination country pages via extractIhgCalaDestinationDirectory; no JS room-count inference; ambiguous identity → steward.

**Proposed change:** Keep census-autopilot-ihg-cala-discovery-adapter as IHG listing SoT for Autopilot.

**Next:** Insert review for 37 IHG High candidates; do not invent descriptions from booking boilerplate.

### `census-choice-property-level-url-not-regional`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_multi_parent_cala_discovery_adapters |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-multi-parent-cala-discovery-adapters.json` |
| Module | `lib/research-engine-v2/census-autopilot-choice-cala-discovery-adapter.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Choice regional directory may discover hotels, but Address Source URL / official property URL must be property-level; never regional placeId URLs.

**Proposed change:** Choice CALA adapter rejects placeId regional URLs as property identity URLs.

**Next:** Keep property-level URL gate on all Choice apply/insert paths.

### `census-active-brand-coverage-gap-pass`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_active_brand_coverage_gap_pass |
| Issue type | `learned_source_pattern` |
| Status | `proposed` |
| Source | `reports/research-engine-v2/webhound-active-brand-coverage-gap-pass.json` |
| Module | `lib/research-engine-v2/census-autopilot-source-discovery.js` |
| Fixture | no |
| Test | no |

**Pattern:** After Marriott/IHG/Hilton/Choice Autopilot adapters, Active Brand Setup gaps are Accor/Wyndham/BWH/Preferred/SLH/Bunkhouse. Prefer wiring existing Accor+Wyndham extractors before new soft-brand adapters.

**Proposed change:** Queue Accor+Wyndham Autopilot discovery adapters; Preferred directory next; BWH/SLH/Bunkhouse steward.

**Next:** Implement Accor then Wyndham Autopilot source_discovery wiring (controlled only).

### `census-accor-wyndham-autopilot-adapter-opportunity`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_active_brand_coverage_gap_pass |
| Issue type | `learned_code_rule` |
| Status | `proposed` |
| Source | `reports/research-engine-v2/webhound-active-brand-coverage-gap-pass.json` |
| Module | `lib/accor-continent-directory-extract.js` |
| Fixture | no |
| Test | no |

**Pattern:** Accor: sitemap-fh.en.xml + /a/en/destination/continent/{slug}.html JSON-LD ItemList. Wyndham: sitemap.xml → *properties_*.xml → /overview; CALA via page country not path keywords.

**Proposed change:** Add census-autopilot-accor-cala-discovery-adapter + wyndham adapter; register in coverage matrix.

**Next:** Ship Accor+Wyndham Autopilot adapters as next low-effort pair.

### `census-blocked-bwh-live-property-pages-403`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_active_brand_coverage_gap_pass |
| Issue type | `learned_block_reason` |
| Status | `validated` |
| Source | `reports/research-engine-v2/webhound-active-brand-coverage-gap-pass.json` |
| Module | `lib/bwh-brand-directory-extract.js` |
| Fixture | no |
| Test | no |

**Pattern:** BWH hotels-details.xml is usable for URL/code discovery; live property HTML is often captcha/403 — steward seed + sitemap metadata only.

**Proposed change:** Do not auto-insert from blocked BWH property fetches; keep seed catalog path.

**Next:** Defer BWH Autopilot full adapter until unblockable official page path exists.

### `census-wyndham-path-cala-false-positive`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_active_brand_coverage_gap_pass |
| Issue type | `learned_validation_rule` |
| Status | `validated` |
| Source | `reports/research-engine-v2/webhound-active-brand-coverage-gap-pass.json` |
| Module | `lib/wyndham-brand-directory-extract.js` |
| Fixture | no |
| Test | no |

**Pattern:** Never classify Wyndham CALA from URL path keywords alone; panama-city-florida and new-mexico collide with country names. Use official page country / JSON-LD.

**Proposed change:** Wyndham Autopilot CALA filter must use page metadata countryNorm.

**Next:** Encode in Wyndham Autopilot adapter tests before wiring.

### `census-active-brand-parent-field-inference-gap`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_active_brand_coverage_gap_pass |
| Issue type | `learned_code_rule` |
| Status | `proposed` |
| Source | `reports/research-engine-v2/active-brand-discovery-coverage-gap-list.json` |
| Module | `lib/research-engine-v2/census-autopilot-active-brand-scope.js` |
| Fixture | no |
| Test | no |

**Pattern:** Active Brand Setup control list often lacks Parent Company on Brand Setup rows; Autopilot parent filters skip IHG/Accor unless slug→parent inference is applied.

**Proposed change:** Centralize PARENT_BY_SLUG (or Brand Setup Parent fill) in buildActiveBrandSetupControlList before discovery scope.

**Next:** Fix parent inference when wiring Accor/Wyndham/IHG Active Setup discovery.

### `census-webhound-gap-do-not-learn-noise`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_active_brand_coverage_gap_pass |
| Issue type | `learned_validation_rule` |
| Status | `validated` |
| Source | `reports/research-engine-v2/webhound-active-brand-coverage-gap-pass.json` |
| Module | `docs/data-intelligence/webhound-active-brand-coverage-gap-pass.md` |
| Fixture | no |
| Test | no |

**Pattern:** Treat Webhound hotel counts and conflicting structured-data claims as do-not-learn noise when Dealality code probes contradict; trust official directory parsers.

**Proposed change:** Document in gap-pass report; never write Webhound counts to Census.

**Next:** Stop further bulk listing Webhound unless a hard new source family blocks code adapters.

### `census-active-brand-insert-apply-75`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | production_census_active_brand_insert_apply |
| Issue type | `learned_code_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/production-census-active-brand-insert-apply.json` |
| Module | `lib/research-engine-v2/census-autopilot-discovery-insert-apply.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Apply only consolidated Active Brand Setup discovery approval bundle (not per-parent). Preflight + live rededupe; steward Choice 'a member of Radisson Individuals' names; skip fuzzy auto-insert.

**Proposed change:** Live discovery insert apply wired via approval-bundle + useLiveAirtable; partial apply continues after steward skips.

**Next:** Steward remaining 16 Choice Radisson Individuals member-of names; Accor/Wyndham adapters next.

### `census-blocked-marriott-deprecated-sitemap-hotels-xml`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_cala_source_discovery_learning |
| Issue type | `learned_block_reason` |
| Status | `validated` |
| Source | `reports/research-engine-v2/webhound-cala-source-discovery-learning.json` |
| Module | `lib/marriott-brand-directory-extract.js` |
| Fixture | no |
| Test | no |

**Pattern:** Deprecated Marriott sitemap-hotels.xml pattern is dead; only use /en-us/hotel-sitemap/{country}-hotel-sitemap (+ master hotel-sitemaps XML).

**Proposed change:** Add blocked-source rule / do-not-fetch list for *.sitemap-hotels.xml Marriott paths.

**Next:** Keep countrySitemapUrl as sole Marriott listing entrypoint.

### `census-webhound-cala-do-not-learn-noise`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_cala_source_discovery_learning |
| Issue type | `learned_validation_rule` |
| Status | `validated` |
| Source | `reports/research-engine-v2/webhound-cala-source-discovery-learning.json` |
| Module | `docs/data-intelligence/webhound-cala-source-discovery-learning.md` |
| Fixture | no |
| Test | no |

**Pattern:** Treat Webhound hotel counts, OTA commercial widgets, and ownership/date inferences as do-not-learn noise. Trust code probes + official directory parsers for inventory size.

**Proposed change:** Document in learning ledger; never write Webhound-derived counts to Census.

**Next:** Keep Webhound closed for bulk discovery; reopen only for hard enrichment edges.

### `census-webhound-cala-later-candidates`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | census |
| Batch | webhound_cala_source_discovery_learning |
| Issue type | `Webhound_candidate` |
| Status | `Webhound_only` |
| Source | `reports/research-engine-v2/webhound-cala-source-discovery-learning.json` |
| Module | `docs/data-intelligence/webhound-cala-source-discovery-learning.md` |
| Fixture | no |
| Test | no |

**Pattern:** Optional second Webhound samples (5–10) only for HQV signature harvest or Hilton timeout enrichment edges — never for Autopilot listing population.

**Proposed change:** Do not start another CALA listing Webhound run; build Marriott adapter from existing extractors first.

**Next:** Reopen only if code adapter work still blocks on a new hard pattern class.

### `be-safe-text-cleanup-batch-1A-applied`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | safe_text_cleanup_batch_1A |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1A-apply.json` |
| Module | `scripts/brand-explorer-62-safe-text-cleanup-batch-1A-apply.mjs` |
| Fixture | no |
| Test | yes |

**Pattern:** internal/source-process language must be removed from owner-facing Brand Explorer fields

**Proposed change:** Keep Batch 1A rewrite lint in brand-explorer-62-safe-text-cleanup-batch-1.mjs; expand shared owner-facing forbidden scan; apply only after founder approval with confirm flags.

**Next:** Batch 1B applied after 1A reconcile; keep 1A forbidden terms in PVQL/forbidden scan.

### `be-safe-text-cleanup-batch-1B-applied`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | safe_text_cleanup_batch_1B |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1B-apply.json` |
| Module | `scripts/brand-explorer-62-safe-text-cleanup-batch-1B-apply.mjs` |
| Fixture | no |
| Test | yes |

**Pattern:** remaining internal Census/source wording must be rewritten before owner-facing render

**Proposed change:** Apply Batch 1B only after 1A reconcile confirm + founder flags; never write Census; Kimpton refresh requires same-property censusSupport.

**Next:** MGallery quality minor resolved; proceed to child Brand Setup table validation (separate).

### `be-62-webhound-claim-validation-readonly`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | 62_webhound_claim_validation_readonly |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-explorer-62-webhound-claim-validation-readonly.json` |
| Module | `lib/partner-intelligence/brand-explorer-62-webhound-claim-validation.js` |
| Fixture | no |
| Test | yes |

**Pattern:** After quality-clean freeze, run a separate read-only Webhound claim validation lane on public-facing factual claims only; never write BE/Setup/Census from Webhound; classify claims with official→trusted hierarchy; keep remediation queue for a later founder-approved copy lane.

**Proposed change:** Keep brand-explorer-62-webhound-claim-validation-readonly gated with --dry-run; pack + merge Webhound dataset rows.

**Next:** Founder reviews claim remediation queue; approve targeted copy softening separately.

### `be-62-webhound-airtable-reconciliation-v1`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | 62_webhound_airtable_reconciliation_v1 |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-explorer-62-webhound-airtable-reconciliation-v1.json` |
| Module | `lib/partner-intelligence/brand-explorer-62-webhound-airtable-reconciliation.js` |
| Fixture | no |
| Test | yes |

**Pattern:** After Webhound claim validation completes, reconcile each row against live Airtable Brand Explorer Presentation / Brand Basics before any copy patch; classify Airtable vs evidence; queue only with no proposed wording writes.

**Proposed change:** Keep brand-explorer-62-webhound-airtable-reconciliation-v1 gated with --dry-run; next lane is founder-approved claim patch batch review.

**Next:** Founder reviews reconciliation remediation batches; approve patch wording lane separately.

### `be-62-webhound-claim-patch-batch-a-momentum-blockers`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | 62_webhound_claim_patch_batch_a_momentum_blockers |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.json` |
| Module | `lib/partner-intelligence/brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.js` |
| Fixture | no |
| Test | no |

**Pattern:** When founder approves Webhound↔Airtable rem Batch A, hide exact unsupported/stale footprint.momentum Presentation rows from public with Active:false + External Display Status Do Not Display; do not invent replacements; leave Batches B–F untouched.

**Proposed change:** Keep brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers gated with founder confirm flags; Presentation EDS/Active only.

**Next:** Review Batch B Recent Momentum other after gates stay clean.

### `be-62-webhound-public-tabs-batch-c-owner-facing-claims`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | 62_webhound_public_tabs_batch_c_owner_facing_claims |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims.json` |
| Module | `lib/partner-intelligence/brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims.js` |
| Fixture | no |
| Test | no |

**Pattern:** After Batch A momentum blockers, apply founder-approved Batch C public-tab softens from Webhound↔Airtable rem queue using curated source-backed wording; strip unsupported fees/ADR/internal language; never write Setup/Census/Status/CV.

**Proposed change:** Keep brand-explorer-62-webhound-public-tabs-batch-c-owner-facing-claims gated with founder confirm flags; Title/Body Presentation only.

**Next:** Loyalty/economic spotcheck or Batch B review after gates stay clean.

### `be-brand-setup-child-table-validation-62`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | brand_setup_child_table_validation_62_readonly |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-setup-child-table-validation-62-readonly.json` |
| Module | `lib/partner-intelligence/brand-setup-child-table-validation-62.js` |
| Fixture | no |
| Test | yes |

**Pattern:** After Active-62 quality-clean freeze, run read-only Brand Setup child-table validation separately from BE Presentation gates; never write child tables / Brand Status / CV during the audit; treat held brands as stale-link probes only.

**Proposed change:** Keep brand-setup-child-table-validation-62-readonly gated with --dry-run; remediation is a separate founder-approved lane.

**Next:** Founder reviews remediation queue; approve targeted child-table fixes separately.

### `be-62-quality-clean-freeze`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | 62_quality_clean_freeze |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-explorer-62-active-public-full-quality-clean-freeze.json` |
| Module | `lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Do not freeze Active-62 as quality-clean while any brand remains approve_after_minor_cleanup; empty accepted-minor list after founder-approved minor resolution; keep Flex/House/Morgans/Radisson Collection held/excluded; freeze is report-only (no Brand Status / CV / Census / child-table writes).

**Proposed change:** Keep FREEZE_DECISION_62 + write62QualityCleanFreezeReports; regression test must require quality_clean_flex_held.

**Next:** Brand Setup child-table validation (read-only separate program).

### `be-mgallery-quality-minor-resolved`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | mgallery_quality_minor |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-explorer-62-mgallery-quality-minor-apply.json` |
| Module | `scripts/brand-explorer-62-mgallery-quality-minor-apply.mjs` |
| Fixture | no |
| Test | yes |

**Pattern:** Missing major Presentation slots must be filled before quality freeze; chip fields require newline-separated tags (not middot joins); never write ADR/RevPAR shorthand in owner-facing copy; footprint.portfolio_mix must stay structured short lines (long prose without % trips semantic prose_market_note).

**Proposed change:** Keep brand-explorer-62-mgallery-quality-minor-apply.mjs gated; chip Body format = newline chips with minChips≥2; portfolio_mix = structured non-percentage lines.

**Next:** Start separate Brand Setup child-table validation program.

### `be-forbidden-language-lint-expansion`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | brand_explorer_62_background_validation |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-explorer-62-background-validation-plan.json` |
| Module | `lib/partner-intelligence/brand-explorer-62-background-validation.js` |
| Fixture | no |
| Test | yes |

**Pattern:** Owner-facing text must reject process/internal terms: census, listed on choicehotels.com, consumer site, source pack, CHD, Item 19, FDD, LOI, metadata, source data, source-capture, pipeline extraction, active property page, QA, governance, factory, stage, staging, sandbox, overlay, raw URLs, confirm fees/FDD.

**Proposed change:** Expand EXTRA_FORBIDDEN_RE / shared owner-facing lint; every approved safe_text_cleanup should add a reusable lint where possible.

**Next:** Keep forbidden-term regression in Batch 1A apply validation + PVQL/forbidden scan.

### `be-safe-text-cleanup-batch-1`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | brand_explorer_62_safe_text_cleanup_batch_1 |
| Issue type | `learned_code_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1.json` |
| Module | `scripts/brand-explorer-62-safe-text-cleanup-batch-1B-apply.mjs` |
| Fixture | no |
| Test | yes |

**Pattern:** Patch category safe_text_cleanup is proposal-only until founder approval; apply Batch 1A/1B separately under confirm flags.

**Proposed change:** Keep dry-run patch plans; convert recurring rewrite patterns into lint rules after approval.

**Next:** MGallery or child Brand Setup validation next; keep Batch 1 apply scripts gated.

### `be-property-examples-census-crosscheck`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | brand_explorer_62_background_validation |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/brand-explorer/brand-explorer-62-background-validation-plan.json` |
| Module | `lib/partner-intelligence/brand-explorer-62-background-validation.js` |
| Fixture | no |
| Test | no |

**Pattern:** 31 Census-confirmed examples are public-proof eligible only if not held/Brand-Unconfirmed; 7 need text refresh; 180 missing are mostly International Reference — do not auto-remove.

**Proposed change:** Property example classifier already encodes held/Brand-Unconfirmed blocks; add text-refresh patch category separately.

**Next:** Prepare property_example_update proposals for the 7 refresh cases only after founder approval.

### `be-no-global-count-from-mexico-census`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | brand_explorer_62_background_validation |
| Issue type | `learned_validation_rule` |
| Status | `validated` |
| Source | `reports/brand-explorer/brand-explorer-62-background-validation-plan.json` |
| Module | `lib/partner-intelligence/brand-explorer-62-background-validation.js` |
| Fixture | no |
| Test | no |

**Pattern:** Never update Brand Explorer global footprint/counts from Mexico Census alone; Census may support Mexico/CALA claims only.

**Proposed change:** footprintClaimClassification already returns no_action for global portfolio claims.

**Next:** Keep regression coverage in background validation dry-runs.

### `be-patch-categories-boundary`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | brand_explorer_62_background_validation |
| Issue type | `learned_code_rule` |
| Status | `proposed` |
| Source | `reports/brand-explorer/brand-explorer-62-background-validation-plan.json` |
| Module | `docs/data-intelligence/dealality-batch-learning-system.md` |
| Fixture | no |
| Test | no |

**Pattern:** Allowed proposal categories: safe_text_cleanup, property_example_update, Webflow_render_fix. Recent Momentum requires separate approval. Never create momentum from property existence alone.

**Proposed change:** Document categories in learning system + validation plan; no auto-apply.

**Next:** Enforce category allowlist in any future BE patch apply CLI.

### `be-mgallery-minor-quality-render`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | brand_explorer_62_background_validation |
| Issue type | `steward_review_case` |
| Status | `steward_only` |
| Source | `reports/brand-explorer/brand-explorer-62-background-validation-plan.json` |
| Module | `lib/partner-intelligence/brand-explorer-62-background-validation.js` |
| Fixture | no |
| Test | no |

**Pattern:** MGallery flagged approve_after_minor_cleanup — founder/steward review before freeze language changes; not auto-patch.

**Proposed change:** Track as Webflow_render_fix / minor cleanup proposal only.

**Next:** Founder approve minor MGallery cleanup patch plan when ready.

### `be-do-not-write-census-from-be`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | brand_explorer_62_background_validation |
| Issue type | `do_not_learn_noise` |
| Status | `rejected` |
| Source | `reports/brand-explorer/brand-explorer-62-background-validation-plan.json` |
| Module | `lib/partner-intelligence/brand-explorer-62-background-validation.js` |
| Fixture | no |
| Test | no |

**Pattern:** Noise: treating Brand Explorer validation findings as Census write instructions. Boundary: BE does not update Census; Census does not patch BE.

**Proposed change:** Keep airtableWrites/censusWrites/brandExplorerWrites=false on validation plans.

**Next:** No action — document boundary only.

### `be-gates-pass-baseline`

| Field | Value |
| --- | --- |
| Date | 2026-08-05 |
| Process | brand_explorer |
| Batch | brand_explorer_62_background_validation |
| Issue type | `learned_validation_rule` |
| Status | `validated` |
| Source | `reports/brand-explorer/brand-explorer-62-background-validation-plan.json` |
| Module | `docs/data-intelligence/dealality-batch-learning-system.md` |
| Fixture | no |
| Test | no |

**Pattern:** Any BE learning batch must re-confirm Active 62 / semantic / PVQL / footnote / momentum / mandatory gates before claiming ready.

**Proposed change:** Reuse quiet sequential PVQL + mandatory release gates in batch checklist.

**Next:** Run gates after every BE content change proposal cycle.

### `census-autopilot-full-cala-15k-census-shell-insert-v1`

| Field | Value |
| --- | --- |
| Date | 2026-08-09 |
| Process | census |
| Batch | production_census_full_cala_15k_shell_insert_v1 |
| Issue type | `learned_validation_rule` |
| Status | `implemented` |
| Source | `reports/research-engine-v2/full-cala-15k-census-shell-insert-v1.json` |
| Module | `lib/research-engine-v2/full-cala-15k-census-shell-insert-v1.js` |
| Fixture | no |
| Test | no |

**Pattern:** Shell-first 15K CALA: inventury+normalize+dedupe; insert Property Name/Country/City shells with Hold+HR; Cvent identity-only; no rooms/coords/media/owner; batch by country.

**Proposed change:** full-cala-15k-census-shell-insert-v1.js

**Next:** Apply remaining country batches after DO/CR/PA/CO/MX review

## Batch learning checklist

- [ ] Was a reusable pattern identified?
- [ ] Was code changed, or was the pattern documented as non-reusable?
- [ ] Was a fixture added?
- [ ] Was a regression test added?
- [ ] Did the next dry-run improve?
- [ ] Did false positives decrease?
- [ ] Did blocked records decrease?
- [ ] Did Webhound dependency decrease?
- [ ] Did the change avoid production writes without approval?
- [ ] Did Brand Explorer / Census boundaries remain intact?
