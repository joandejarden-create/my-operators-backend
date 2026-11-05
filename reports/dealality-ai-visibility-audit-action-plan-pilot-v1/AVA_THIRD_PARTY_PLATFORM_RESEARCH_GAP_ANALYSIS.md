# AVA — Third-Party Platform Research Gap Analysis

**Contract:** `AVA_THIRD_PARTY_PLATFORM_RESEARCH_GAP_ANALYSIS_V1`  
**Generated:** 2026-11-05  
**Paid research:** $0 · No new subscriptions · No deployment  

---

## Problem (confirmed)

Prior AVA Digital Content Readiness / platform coverage frequently emitted `IDENTIFIED_NOT_ASSESSED` for major platforms (GBP, Tripadvisor, Booking, Expedia, maps, Cvent) **without**:

- a resolved listing URL,
- documented multi-strategy research attempts, or
- an honest unresolved reason.

That is insufficient for a customer-facing digital visibility audit.

---

## Gap inventory

| Gap | Pre-fix state | Impact |
|-----|-----------------|--------|
| No mandatory baseline platform set | Ad-hoc inventory only | Hotels missing OTA/GBP/TA rows |
| Single-strategy discovery | One failed probe → unassessed | False “not assessed” |
| No hotel-scoped seed isolation | Shared pilot blob URLs | Cross-hotel contamination risk |
| Maps search ≡ verified listing | Search URLs presented as GBP | Overstated verification |
| Opaque Google Travel entity IDs | Shared across hotels in paid pilot artifacts | Wrong property linkage |
| Generic identity tokens (`hotel`, `resort`) | Matched unrelated URLs | False positives |
| Status vocabulary incomplete | Only IDENTIFIED_NOT_ASSESSED / VERIFIED_* | Cannot express access-restricted / not found after research |
| PDF presentation | Unassessed rows equal weight to corrections | Customer noise |
| Completeness gate | Technical PDF PASS without research coverage | False readiness signal |

---

## Permanent design (implemented)

**Module:** `lib/ai-visibility-audit-action-plan/platform-listing-discovery-v2.js`  
**Contract:** `AVA_PLATFORM_LISTING_DISCOVERY_V2`

### Baseline platforms (applicability-aware)

1. Brand property page  
2. Hotel-operated website  
3. Google Business Profile / Google Maps  
4. Tripadvisor  
5. Booking.com  
6. Expedia  
7. Apple Maps  
8. Bing Places  
9. Cvent (meetings/group demand only)  
10. Destination / tourism / CVB  

Irrelevant platforms → `NOT_APPLICABLE` with reason (not forced into defects).

### Discovery strategies (zero paid)

1. Prior research seeds **scoped to this `propertyId` only** (`AVA_15USD_PAID_PILOT_RESULTS.json` per-hotel blob)  
2. Payload evidence-graph URL harvest  
3. DuckDuckGo HTML `site:` queries  
4. Constructed Maps search URL as **discovery aid only**  
5. HTTP probe + identity token check (stopwords + hotel URL fingerprints)

### Research statuses

| Status | Meaning |
|--------|---------|
| `VERIFIED_CONSISTENT` | Reachable listing; identity matched; not claimed field-perfect |
| `VERIFIED_CORRECTION_REQUIRED` | Verified inconsistency / missing info |
| `VERIFIED_OPPORTUNITY` | Actionable inclusion/outreach |
| `LISTING_FOUND_ACCESS_RESTRICTED` | URL found; automated access blocked — not a public defect proof |
| `LISTING_NOT_FOUND_AFTER_RESEARCH` | Bounded strategies exhausted; no URL |
| `NOT_APPLICABLE` | Outside commercial baseline for this hotel |
| `RESEARCH_INCOMPLETE` | Aid-only (e.g. Maps search) or incomplete probe with log |

### Customer presentation

- Grouped platform assessment + **Digital Platform Correction Map**  
- Human-readable evidence labels  
- CQA: correction map presence, high-priority action evidence, discovery completeness gate  

### Hard rules

- No CAPTCHA bypass / ToS violation  
- No unauthorized paid spend  
- Inaccessible ≠ missing/incorrect  
- Technical PDF certification ≠ research-coverage certification  

---

## Residual gaps (honest)

1. Exact GBP place-ID resolution still needs authorized Maps/Places or manual confirmation.  
2. Apple Maps / Bing Places public discovery remains thin at $0.  
3. DuckDuckGo HTML is geo/incomplete — documented in research logs.  
4. Access-restricted brand/OTA pages still need human browser confirmation for field-level facts.  
5. Six-hotel pilot only — not yet Full E2E mandatory for all 38 until founder approval.

---

## Recommendation

Accept V2 discovery as the permanent baseline engine after founder review of the six-hotel results. Wire the completeness gate into Full E2E as a **distinct** research-coverage certificate before any 38-hotel expansion.
