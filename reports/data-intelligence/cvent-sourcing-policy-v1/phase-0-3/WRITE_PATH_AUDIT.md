# Write Path Audit — Cvent Venue/Hotel (Phase 1)

| Location | Before | After | Status |
|----------|--------|-------|--------|
| `lib/research-engine-v2/external-hotel-source-policy.js` | `cvent` approved for Rooms/Keys, Address, Phone, Description Source Text, Brand, Official URL, Canonical Name | Approvals removed; hard-blocked; `canPersistAsCanonical` fail-closed | **BLOCKED** |
| `lib/hotel-intelligence/research/event-space-depth-v2.js` | Cvent parse → HIGH commercial/event persist | Discovery candidates + LOW evidence; `continue` before persist | **BLOCKED** |
| `lib/hotel-intelligence/research/research-hotel-intelligence.js` | Cvent fields → proposedCommercial MEDIUM/T2 | Discovery candidates only; T4_DISCOVERY; NEEDS_SOURCE_REVIEW | **BLOCKED** |
| `scripts/census-choice-cvent-rooms-fill.mjs` | APPLY wrote Rooms/Keys Medium from Cvent | APPLY blocked unless emergency override; candidates retained | **BLOCKED** |
| `lib/research-engine-v2/census-cvent-choice-matcher.js` | Choice patch wrote Rooms/Keys, Address, Description, Meeting Flag, etc. | `filterCventVenueCensusPatch` → steward discovery notes only (`v3-discovery-only`) | **BLOCKED** |
| `lib/research-engine-v2/census-cvent-latam-matcher.js` | LATAM update/insert wrote Rooms/Keys + description prose | Same filter on update + Census Only insert (`v2-discovery-only`) | **BLOCKED** |
| `lib/data-intelligence/source-policy/v1/census-cvent-write-filter.js` | (new) | Shared strip helper for Choice/LATAM census writers | **ADDED** |
| `lib/hotel-intelligence/research/evidence-depth-v2.js` | `cvent.com/venues/` = Tier B structured | Tier D aggregator | **DOWNGRADED** |
| `lib/research-engine-v2/census-cvent-venue-client.js` | Fetch/parse/cache (no change to parser) | Still discovery tooling; writers gated upstream | **DISCOVERY_OK** |
| Autopilot v2.3 `cvent-firewall.js` | Fail-closed for independent discovery | Unchanged (complementary) | **PRESERVED** |
| GDI event URLs `web.cvent.com/event/...` | Event evidence | Still allowed via CVENT_EVENT_PLATFORM | **ALLOWED** |

No Airtable destructive patches in this phase.
