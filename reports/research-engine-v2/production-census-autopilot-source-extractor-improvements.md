# Production Census Autopilot — Source Extractor Improvements

**Status:** `production_census_autopilot_source_yield_improved_ready_for_controlled_review`

## Code changes

| Module | Change |
| --- | --- |
| `production-census-address-geocode-resolver.js` | Address-only High propose when geocode deferred/provider none; include records with coords + blank Address; skip VIC lat/lng re-propose when coords present; Autopilot `proposed_updates` includes `proposal` + unmasked IDs; validation skips lat/lng for address-only |
| `production-census-description-extraction.js` | Fetch rank IHG→Choice→Marriott→Hilton; family block circuit (3 consecutive); Address from official page snippet in patch allowlist |
| `production-census-rooms-keys-queue.js` | Same fetch ranking + circuit breaker |
| `production-census-rooms-keys-extractor.js` | `numberOfRooms` numeric string pattern |
| `census-autopilot-source-yield-diagnostic.js` | **New** — taxonomy A–P, apply recommendation (&lt;10 vs ≥10), Webhound candidates builder |
| `census-autopilot-queue-orchestrator.js` | Higher address fetch budget; apply_recommendation on bundle |
| `scripts/census-autopilot.mjs` | Persist `source-yield-diagnostic.*` + `webhound-candidates.json` |

## What we did **not** change

- Confidence threshold remains High-only for Autopilot would-writes  
- No Medium→High promotion of VIC rooms  
- No bot-bypass of Hilton/Marriott/Choice 403  
- No Brand Explorer / Brand Setup / owner / date / Company Validated writes  

## Extractor notes by family

### IHG
- Descriptions already populated for active scope  
- Pages fetch OK; JSON-LD address extractable; many pages have empty `numberOfRooms`  
- Rooms High only when explicit prose / numeric schema present  

### Hilton / Choice
- Corporate HTML blocked (403)  
- **Yield:** High VIC street Address claims → address-only proposals (107 combined with Hilton/Choice in improved run)  

### Marriott
- Corporate HTML blocked; no High VIC addresses for missing Address set  
- Descriptions/amenities remain source-blocked → Webhound learning candidates only  

## Webhound candidates (not run)

See run folder `webhound-candidates.json` — Hilton edge 403, Marriott 403, Choice 403, IHG empty numberOfRooms.
