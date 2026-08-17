# Autopilot V1.1 — Existing Capability Gap Map

## Already exists (reuse — do not rebuild)

| Capability | Location |
|---|---|
| Mode registry / field routing / priority / resume | `census-autopilot-v1/*` |
| Field contract (63 researchable) | `production-census-field-contract-v111.js` |
| Field research plans | `clean-census/field-research.js` |
| Family directory adapters (Hilton/Choice/IHG signals) | `census-autopilot-family-directory-adapters.js` |
| Official page deep extract | `extractDeepOfficialPageSignals` |
| IHG amenity extract | `ihg-hotel-amenities-extract.js` |
| IHG / Hilton adapters | `adapters/ihg.js`, `adapters/hilton.js` |
| Property identity / temporal / firewall | `clean-census/*` |
| Cvent/legacy quarantine adapters | `census-autopilot-v1/challenge-adapters.js` |
| Completeness / output classes | `completeness.js`, `output-classes.js` |

## Gap that blocked “research depth” in V1

| Gap | Impact | V1.1 fix |
|---|---|---|
| Orchestrator only read VIC compact index — **no live HTTP** | Completeness frozen at freeze-time | `live-deep-research.js` fetches official pages |
| Lane B not auto-triggered when directory lacks rooms/operator | Premature escalation | Ladder Level 3 standalone website |
| Hard fields not attacked systematically | 248 stuck in remediation | Per-field ladder + hard-field stats |
| Effort / stop levels not tracked per field | Could not prove autonomous stop | `resolution_level`, `research_effort_score` |
| Image rights collapsed data class (fixed in V1) | Mis-classification | Keep separate |

## Explicitly NOT built

- No new identity engine, steward queue, source registry, field registry, activation engine, or image engine
- No DataForSEO / paid geocoding / Webhound
- No Airtable writes
