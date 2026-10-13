# Cvent Source Policy Phase 0–3 — Implementation Report

**Date:** 2026-10-03  
**Policy version:** `source-policy-v1`  
**Scope:** Policy module + write gates + downstream guards + read-only inventory  
**Destructive remediation:** none

---

## What shipped

### Phase 0 — Shared module
`lib/data-intelligence/source-policy/v1/`
- `source-roles.js` — SourceRole, SourceContentDomain, VerificationStatus
- `domain-classifier.js` — URL/adapter → domain (CVENT_VENUE_HOTEL vs CVENT_EVENT_PLATFORM)
- `gates.js` — `canPersistAsCanonical` / `canUseForScoring` / `canDisplayToCustomer` / `requiresIndependentVerification` / `resolveMixedProvenance` / `createDiscoveryResearchCandidate`
- `index.js` — public exports

### Phase 1 — Unsafe write approvals removed
| Path | Change |
|------|--------|
| `external-hotel-source-policy.js` | Removed `cvent` from field approvals; hard-blocked Rooms/Keys, Address, Phone, descriptions, brand, official URL; gate calls source-policy |
| `event-space-depth-v2.js` | Cvent venue parses → discovery candidates + LOW evidence; **no HIGH persist** into commercial/event spaces |
| `research-hotel-intelligence.js` | Cvent extracts → discovery candidates only; tier T4_DISCOVERY; no MEDIUM canonical promotion |
| `census-choice-cvent-rooms-fill.mjs` | APPLY of Rooms/Keys/Address from Cvent **blocked** (override flag only) |
| `census-cvent-choice-matcher.js` | Choice census patches stripped via `filterCventVenueCensusPatch` (`v3-discovery-only`) |
| `census-cvent-latam-matcher.js` | LATAM update + Census Only insert stripped (`v2-discovery-only`) |
| `source-policy/v1/census-cvent-write-filter.js` | Shared Cvent venue census write strip + steward discovery note |
| `evidence-depth-v2.js` | `cvent.com/venues/` authority downgraded from Tier B → Tier D aggregator |

### Phase 2 — Downstream guards
| Path | Change |
|------|--------|
| `build-adp-hotel-attributes.js` | `applyCventDiscoveryOnlyAdpGuard` → `usedInAdp=false`, `NEEDS_SOURCE_REVIEW` (no delete) |
| `hi-enriched-hotel-profile-v1.js` | Discovery-only / Cvent venue HI values excluded from verified GDI capability overlay |

### Phase 3 — Read-only inventory
- `scripts/cvent-sourcing-remediation-inventory-v1.mjs`
- Outputs: `REMEDIATION_INVENTORY.csv`, `REMEDIATION_SUMMARY.json` (+ markdown summary)

### Tests
`scripts/test-source-policy-cvent-v1.mjs` — **10/10 PASS**

---

## Production behavior changed?

**YES — write-prevention only:**
1. New Cvent venue/hotel facts cannot become canonical HI/Census/ADP verified truth.
2. New Cvent-only attributes cannot activate verified ADP use.
3. New Cvent-only HI capability cannot feed verified GDI fit scoring.
4. Cvent event-platform evidence remains allowed for GDI events.

**NO:**
- No record deletes
- No mass Airtable overwrites
- No ADP/GDI score recomputation batches
- No share-token or Bethesda changes
- No independent verification batch runs

---

## Final verdict

**PASS — Phase 0–3 complete.** Cvent venue/hotel is DISCOVERY_ONLY with enforceable gates; event-platform Cvent remains usable; remediation inventory is read-only.
