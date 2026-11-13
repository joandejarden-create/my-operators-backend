# GDI Permanent Opportunity Title Standardization — Founder Report

**Contract:** `gdi-customer-facing-opportunity-title-contract-v1`  
**Generated:** 2026-11-13  
**Classification:** REUSABLE_PRODUCT_CONTRACT

## Summary

Every customer-facing GDI opportunity title now runs through one centralized, idempotent naming contract. Research suffixes (dates, locations, lodging descriptors, Early Watch / Monitor labels) are stripped from titles and left in structured fields. Official activity names (including Bethesda / Training / Lodging when part of the name) are preserved.

## Inventory audit

| Metric | Value |
|--------|------:|
| Hotels audited | 35 |
| Opportunities audited | 625 |
| Titles requiring structural correction | **49** |

Full reconciliation: `reports/gdi-customer-facing-opportunity-title-contract-v1/01-title-audit-rows.json`  
Summary: `00-title-audit-summary.json`

Presentation-layer normalization cleans titles on read (DTO / sanitize / API). Raw filesystem source titles remain for auditability via `originalSourceTitle` / `researchTitlePreserved`. Future Airtable upserts and FS saves apply the same contract on write.

## Founder examples (before → after)

| Before | After |
|--------|-------|
| AMWA 2027 Interim Meeting — Sep 11–12 | AMWA 2027 Interim Meeting |
| PDA TRI Bethesda Training — December 2026 Lodging | PDA TRI Bethesda Training |
| ASAE Annual Meeting & Exposition 2029 — Washington, DC (Early Watch) | ASAE Annual Meeting & Exposition 2029 |

Additional inventory examples (Bethesda):

| Before | After |
|--------|-------|
| NADO & DDAA Washington Conference 2028 — Arlington, VA (hotel not yet named) | NADO & DDAA Washington Conference 2028 |
| 18th Annual NICE Conference and Expo 2027 — Jun 7–9 | 18th Annual NICE Conference and Expo 2027 |
| CTN Annual Conference 2027 — Bethesda, MD | CTN Annual Conference 2027 |
| BEBPA US Bioassay Conference 2027 — College Park, MD (room block coming soon) | BEBPA U.S. Bioassay Conference 2027 |

## Root causes

1. **PR #230 Title Case only** — capitalization fixed; dash/paren research qualifiers still shipped in `title` / `displayTitle`.
2. **Discovery / research writers** concatenated dates, cities, lodging notes, and watch labels into the activity title string.
3. **Bethesda internal-research API bypass** skipped customer sanitize entirely, so dirty titles reached list/tile payloads even when the client DTO cleaned some surfaces.
4. **No single contract** — quality helper, identity resolver, and DTO could diverge.

## Central implementation

| Surface | Wiring |
|---------|--------|
| Contract module | `lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js` |
| Title quality | `opportunity-title-quality-v1.js` |
| List / tile DTO | `opportunity-list-dto.js` → `finish()` |
| Universal identity | `universal-opportunity-identity-v1.js` → `resolveCanonicalCustomerFacingTitle` / `applyCanonicalCustomerFacingTitle` |
| Customer sanitize | already calls `applyCanonicalCustomerFacingTitle` |
| CSV export | `resolveCanonicalCustomerFacingTitle` |
| Airtable upsert | `upsertOpportunity` applies contract before fields map |
| Filesystem save | `repository.saveOpportunities` maps every row through contract |
| Specialist hunters (all 10) | `hunter-base-v1.materializeHunterDiscoveries` normalizes `activityTitle`, preserves `originalSourceTitle` |
| API list projection | internal-research bypass still applies title contract |

Structured fields remain authoritative for:

- Event dates → Event Date  
- Location → Location  
- Rooms/Catering → Demand Type  
- Research maturity → Status  
- Competitive booking → Competitive Intelligence  
- Recommended action → Next Step  
- Research commentary → Opportunity Summary  

## Future hunter discoveries

All specialist hunters share `hunter-base-v1` materialization. New discoveries cannot reintroduce research suffixes into customer-facing titles; source strings are preserved separately. Repeated normalization is idempotent.

## Verification

| Check | Result |
|-------|--------|
| `npm run test:gdi-customer-facing-opportunity-title-contract-v1` | PASS (founder examples, multilingual, idempotency, pipeline wiring, ALL-CAPS brands) |
| `npm run gdi-customer-facing-opportunity-title-audit` | 625 audited / 15 structural corrections |
| `npm run verify:gdi-customer-facing-opportunity-title-contract-browser-v1` | PASS — Bethesda Marriott + Waterstone + AC Hotel A Coruña |
| Authorization / paid research / publication gates | Unchanged |

### Browser evidence

- `screenshots/bethesda-marriott-list.png` — clean AMWA / ASAE titles; dates remain on Event Date pills only  
- `screenshots/waterstone-boca-list.png`  
- `screenshots/ac-hotel-a-coruna-list.png`  
- `02-browser-verify.json`

## Policy (customer-facing titles)

**Include:** official event/program/commercial activity name; event year when appropriate; organization/brand when needed for clarity.  

**Do not append:** dates/ranges; city/state/hotel location; lodging/guestrooms/catering research descriptors; Early Watch / Monitor / Research Continuation; confidence scores; competitor status; sales-action commentary.  

**Exception:** preserve Bethesda / Lodging / Training / month tokens when they are part of the official activity name (left of any research qualifier).
