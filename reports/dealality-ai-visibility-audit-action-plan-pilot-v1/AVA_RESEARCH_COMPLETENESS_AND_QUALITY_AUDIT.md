# AVA — Research Completeness & Quality Audit

**Contract:** `AVA_RESEARCH_COMPLETENESS_AND_QUALITY_AUDIT_V1`  
**Generated:** 2026-11-05  
**Paid research this pass:** $0 · **ADP:** unchanged · **Publish/deploy:** not authorized  

---

## 1. Baseline (what existed)

Prior V2 pilot certified “platform research completeness” when each applicable platform had *either* an assessment *or* a documented failed search. Founder judgment: **a completed search process ≠ completed research.**

Root causes of insufficient customer readiness:

| Root cause | Effect |
|------------|--------|
| DuckDuckGo-primary discovery | Missed Booking/Expedia/TA listings that clearly exist |
| Maps/search URLs as GBP stand-ins | Overstated or incomplete Google presence |
| Opaque Google Travel entity cross-reject | Discarded *correct* Castillo entity after shared-blob contamination scare |
| Listing existence conflated with content audit | Access-restricted pages treated as “assessed” or “not found” inconsistently |
| Process PASS used as customer readiness | Exhausted logs earned certification without exact high-priority URLs |
| Weak outbound harvest | Official sites not systematically mined for platform links |

Preserved: verified V3 PDF drafts, published PDFs, evidence graphs, Hotel Facts Register content, Digital Authority inventories, ADP measurements.

---

## 2. What changed (Phase A)

| Change | Location |
|--------|----------|
| Dual statuses: listing existence ≠ content assessment | `platform-listing-discovery-v3.js` |
| Hotel-scoped curated verified seeds (zero-cost independent verification) | `AVA_VERIFIED_PLATFORM_LISTING_SEEDS_V3.json` |
| Outbound link harvest from official/brand pages | V3 discovery |
| Maps/search never classified as exact GBP | V3 + seeds policy |
| Google Travel *hotel entity* accepted only when property-confirmed | Seeds + identity rules |
| Separate process vs customer readiness gates | V3 `processGate` + `customerResearchReadiness` |
| Six-hotel reassessment machine results | `AVA_PHASE_A_RESEARCH_QUALITY_PILOT_RESULTS.json` |

---

## 3. Before → after (high-priority platforms)

| Hotel | Before exact high-priority URLs (V2 era) | After exact verified (V3) | High-priority unresolved |
|-------|------------------------------------------|---------------------------|--------------------------|
| Waterstone | Brand, site, TA, Booking, Expedia; Maps=search aid | + Google Travel entity + Apple Maps place | None |
| AC A Coruña | Brand, TA, Booking, Expedia, CVB; Maps=search | + Google Travel entity (AC-specific) | None |
| Castillo | Brand, TA, Visit Palma; Booking/Expedia missing; Maps discarded | + Booking, Expedia, Google Travel entity | None |
| Sheraton | Brand, Visit Palma; TA/Booking/Expedia/Google missing | + TA, Booking, Expedia, Google Travel entity | None |
| Chapultepec | Brand, Booking, Cvent; TA/Expedia/Google missing | + TA, Expedia, Google Travel entity | None |
| Condesa | Site only mostly | + Brand, TA, Booking, Hotels.com/Expedia family | **Google exact entity still unresolved** |

---

## 4. Analysis quality posture

Finding classes for customer-facing use:

- `VERIFIED_CORRECTION`
- `CONTENT_IMPROVEMENT`
- `DISTRIBUTION_OPPORTUNITY`
- `DIGITAL_AUTHORITY_OPPORTUNITY`
- `RESEARCH_FOLLOW_UP`

Rules: no fabricated URLs; access-restricted ≠ missing; unlinked mention ≠ backlink; ordinary listings ≠ AI citations; no invented revenue uplift.

---

## 5. Unresolved research queue (paid plan — not executed)

| Unresolved question | Expected evidence | Provider | Est. cost | Why zero-cost insufficient |
|---------------------|-------------------|----------|-----------|----------------------------|
| Exact Google Maps *place* CID deep-links (vs Travel entity) for all 6 | `maps/place` or `g.page` URLs + NAP fields | DataForSEO Maps/SERP or Context extract | ~$3–8 / hotel | Travel entity ≠ place CID; Places API not in zero-cost path |
| Fiesta Americana Condesa Cancún Google Travel/Maps entity | Exact Google hotel entity URL | Exa site search + Context scrape | ~$1–3 | Public HTML search did not return stable entity URL this pass |
| Full field-level content audit on bot-protected brand/OTA pages | Structured amenities/meetings facts | Context.dev extract | ~15–40 credits/hotel | HTTP probes often 403/429 |
| Comprehensive referring-domain index | Verified backlink inventory | Ahrefs (not entitled) or DataForSEO backlinks | Quote before buy | Exa ≠ backlink index |

**No paid calls made.** Requires separate founder authorization.

---

## 6. Phase A gate summary

| Gate | Cohort verdict |
|------|----------------|
| Research Process Certification | **PASS** (6/6) |
| Customer Research Readiness | **PASS_WITH_MATERIAL_LIMITATIONS** (Condesa Google entity open; 5/6 full PASS including all 3 design-pilot hotels) |

Design-pilot hotels (Waterstone, AC, Castillo): **Customer Research Readiness PASS** → Phase B V4 design pilot is allowed for those three only.
