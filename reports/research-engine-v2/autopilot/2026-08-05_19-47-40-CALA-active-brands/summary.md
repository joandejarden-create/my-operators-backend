# Census Autopilot Summary (v2)

1. **Parent company:** (active-brand-setup)
2. **Region / country:** CALA
3. **Mode:** plan
4. **Total records in scope:** 0
5. **Total processed:** 0
6. **Total updated:** 0
7. **Total skipped:** 0
8. **Total blocked:** 0
9. **Fields populated:** (none)
10. **Confidence High/Medium/Low/Hold:** 0 / 0 / 0 / 0
11. **Runtime:** 39 ms
12. **Remaining queues:** description_extraction, amenities_extraction, radar_public_readiness, address_confirmation, property_type_asset_context, rooms_keys, source_discovery, steward_webhound_hard_cases, coordinate_resolution
13. **Completion status:** `complete`
14. **Resume command:** (n/a)
15. **Recommended next:**

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled --strategy fastest-safe --run-until-complete --batch-size 250
```

- **Batch size:** 250 (chunk only)
- **Max records:** (none — full scope)
- **Run until complete:** false
- **Status:** `production_census_autopilot_active_brand_setup_fastest_safe_ready_needs_v114_schema`
- **Airtable writes:** false → Hotel Property Census
- **Brand Explorer writes:** false
- **Webhound candidates:** 0
- **Steward cases:** 0

## Notes

Scope: active-brand-setup
Strategy: fastest-safe
Active brands: 62
Parents: n/a
Order: description_extraction → amenities_extraction → radar_public_readiness → address_confirmation → property_type_asset_context → rooms_keys → source_discovery → steward_webhound_hard_cases → coordinate_resolution
Why: Prioritize geography/radar and proven extractors with high expected High-confidence writes; soft-defer geocode without provider; rooms early but after lower-ambiguity description when scores dictate; hard cases last.
