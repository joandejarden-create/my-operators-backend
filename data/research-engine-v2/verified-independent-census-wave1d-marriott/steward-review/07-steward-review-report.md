# Marriott Mexico VIC Wave 1D — Steward Review

**Status:** `wave1d_marriott_steward_review_minor_holds_ready_for_4_family_baseline_lock`  
**Reviewed:** 2026-08-04T23:09:12.505Z  
**Scope:** All **301** Marriott Mexico Wave 1D records (read-only; freeze untouched)

Constraints honored: No Airtable · No Webhound · No BE activation · No production overwrite · **No auto-merges** · No fake rooms/owners/open dates

---

## Executive verdict

Wave 1D is **safe to include** in the combined 4-family Mexico VIC baseline with **minor holds** documented below. Staging identity count **301 = 301 unique MARSHA** is sound. Material completeness remains sitemap-limited (40%).

---

## 1. Identity-risk pass (301/301)

| Risk | Count |
|------|------:|
| Low | 293 |
| Medium+ | 8 |
| MARSHA collisions | 0 |

Overlay: `steward-review/01-identity-risk-all-records.json`

---

## 2. Brand parsing QA

| Brand | Count | Parsing Confidence | Adjacent Brand Risk | Samples | Steward Action |
|-------|------:|--------------------|---------------------|---------|----------------|
| City Express by Marriott | 89 | High | Low — sibling brands (Plus/Junior/Suites/Centro) ordered correctly | City Express by Marriott Aguascalientes Sur; City Express by Marriott Leon; City Express by Marriott Irapuato | Accept |
| Courtyard by Marriott | 29 | High | Low | Courtyard by Marriott Leon at The Poliforum; Courtyard by Marriott Ciudad Juarez; Courtyard by Marriott Cancun Airport | Accept |
| Design Hotels | 25 | High | Medium — soft-brand URLs; city inference variable | Elena de Cobre, a Member of Design Hotels™; Hotel Matilda, a Member of Design Hotels™; Papaya Playa Project, Tulum, a Member of Design Hotels™ | Accept for census identity; BE hold until material fields |
| City Express Plus by Marriott | 24 | High | Low | City Express Plus by Marriott Leon Centro de Convenciones; City Express Plus by Marriott Cancún Aeropuerto Riviera; City Express Plus by Marriott Guadalajara Providencia | Accept |
| City Express Junior by Marriott | 21 | High | Low | City Express Junior by Marriott Aguascalientes Centro; City Express Junior by Marriott León Centro De Convenciones; City Express Junior by Marriott Cancun | Accept |
| Fairfield by Marriott | 12 | High | Low | Fairfield by Marriott Inn & Suites Aguascalientes; Fairfield by Marriott Inn & Suites Silao Guanajuato Airport; Fairfield by Marriott Inn & Suites Cancun Downtown | Accept |
| Four Points by Sheraton | 11 | High | Low — matched before Sheraton (Wave 1D fix verified) | Four Points by Sheraton Ciudad Juarez; Four Points by Sheraton Cancun Centro; Four Points by Sheraton Distrito Uno Chihuahua | Accept |
| Autograph Collection | 10 | High | Medium — soft brands / Royalton complexes; multi-property campuses | Cleviá, San Miguel de Allende, Autograph Collection; Royalton Hideaway Riviera Cancun, An Autograph Collection All-Inclusive Resort - Adults Only; Royalton CHIC Cancun, An Autograph Collection All-Inclusive Resort - Adults Only | Accept; campus pairs steward-noted |
| Marriott Hotels | 10 | High | Medium — titles with 'Marriott … Hotel' (intervening words) may miss map | Aguascalientes Marriott Hotel; Marriott Cancun, An All-Inclusive Resort; Culiacan Marriott Hotel | Accept with known miss → Mexico City Marriott Reforma (unconfirmed bucket) |
| Westin | 9 | High | Low — Baja Point kept distinct MARSHA | The Westin Lagunamar Ocean Resort Villas & Spa, Cancun; The Westin Los Cabos Resort Villas; The Westin Los Cabos Resort Villas - Baja Point | Accept |
| AC Hotels by Marriott | 8 | High | Low | AC Hotel Guadalajara, Mexico; AC Hotel Guadalajara Expo, Mexico; AC Hotel by Marriott Santa Fe | Accept |
| JW Marriott | 7 | High | Low — Casa Maat mapped JW via rule | JW Marriott Cancun Resort & Spa; JW Marriott Hotel Guadalajara; JW Marriott Hotel Monterrey Valle | Accept |
| The Luxury Collection | 5 | Medium | Review soft-brand / URL cues | Paraiso de la Bonita, a Luxury Collection Resort, Riviera Maya, Adult All-Inclusive; Almare, a Luxury Collection Resort, Isla Mujeres Cancun, Adult All-Inclusive; Las Alcobas, a Luxury Collection Hotel, Mexico City | Accept for census identity |
| Marriott Bonvoy — Brand Unconfirmed | 5 | N/A — intentional hold | High — soft brands + one Marriott Hotels miss | CASA MAYOR Saltillo, Hotel Hacienda; SJ Grand Hotel Monterrey; Gran Hotel de Puebla by HNF | Review individually (see §3) |
| Sheraton | 5 | High | Low — no Four Points leakage in sample | Sheraton León; Sheraton Chihuahua Soberano; Sheraton Guadalajara Expo | Accept |
| St. Regis | 4 | High | None | The St. Regis Kanai Resort, Riviera Maya; The St. Regis Costa Mujeres Resort, Cancun; The St. Regis Mexico City | Accept |
| Aloft Hotels | 4 | High | Low | Aloft by Marriott Cancun; Aloft by Marriott Playa del Carmen; Aloft by Marriott Guadalajara Sur | Accept |
| City Express Suites by Marriott | 4 | High | Low | City Express Suites by Marriott Ciudad de México Anzures; City Express Suites Puebla Finsa; City Express Suites by Marriott Querétaro | Accept |
| Residence Inn by Marriott | 4 | High | Low | Residence Inn by Marriott Playa del Carmen; Residence Inn by Marriott Cancun Hotel Zone; Residence Inn by Marriott Guadalajara Country Club | Accept |
| Delta Hotels | 3 | Medium | Review soft-brand / URL cues | Delta Hotels by Marriott Chihuahua; Delta Hotels Riviera Nayarit, An All-Inclusive Resort; Delta Hotels by Marriott Riviera Veracruz | Accept for census identity |
| City Centro by Marriott | 3 | High | Low | City Centro by Marriott Ciudad De México; City Centro by Marriott Oaxaca; City Centro by Marriott San Luis Potosi | Accept |
| W Hotels | 2 | Medium | Review soft-brand / URL cues | W Mexico City; W Punta de Mita | Accept for census identity |
| Tribute Portfolio | 2 | High | Low | Mystique Holbox by Royalton, A Tribute Portfolio Resort; Casa Nizuc, a Tribute Portfolio Resort | Accept |
| EDITION | 1 | Medium | Review soft-brand / URL cues | The Riviera Maya EDITION at Kanai | Accept for census identity |
| Le Méridien | 1 | Medium | Review soft-brand / URL cues | Le Méridien Mexico City Reforma | Accept for census identity |
| Renaissance Hotels | 1 | Medium | Review soft-brand / URL cues | Renaissance Cancun Resort & Marina | Accept for census identity |
| Moxy Hotels | 1 | High | Low | Moxy Tulum | Accept |
| Apartments by Marriott Bonvoy | 1 | Medium | Review soft-brand / URL cues | Bloom Tulum, Apartments by Marriott Bonvoy | Accept for census identity |

### Four Points / Sheraton verification

- Four Points by Sheraton: **11**
- Sheraton: **5**
- Four Points leaked into Sheraton: **0**
- **Verified clean:** YES

### City Express family verification

| Sub-brand | Count |
|-----------|------:|
| City Express by Marriott | 89 |
| City Express Plus by Marriott | 24 |
| City Express Junior by Marriott | 21 |
| City Express Suites by Marriott | 4 |
| City Centro by Marriott | 3 |

- Misparse count: **0**
- **Verified clean:** YES

---

## 3. Brand Unconfirmed (5)

| Property | MARSHA | Likely brand | Confidence | Classification | Action |
|----------|--------|--------------|------------|----------------|--------|
| CASA MAYOR Saltillo, Hotel Hacienda | SLWAK | — | Insufficient | `steward_manual_review_required` | Manual URL/brand page review before any brand confirmation |
| SJ Grand Hotel Monterrey | MTYJD | — | Insufficient | `steward_manual_review_required` | Manual URL/brand page review before any brand confirmation |
| Gran Hotel de Puebla by HNF | PBCDE | — | Insufficient | `exclude_from_brand_completion` | Keep Unconfirmed; exclude from BE completion; optional future property-page brand cue |
| Mexico City Marriott Reforma Hotel | MEXMC | Marriott Hotels | High | `confirm_brand` | Overlay confirm_brand → Marriott Hotels (do not mutate freeze; apply on baseline lock overlay if steward accepts) |
| Hotel Guadalajara Country Club by HNF | GDLCC | — | Insufficient | `exclude_from_brand_completion` | Keep Unconfirmed; exclude from BE completion; optional future property-page brand cue |

**Source-supported confirm candidate:** Mexico City Marriott Reforma Hotel → `confirm_brand` Marriott Hotels (map miss: intervening place token). Soft brands / HNF: keep unconfirmed; exclude from BE completion.

---

## 4. Physical identity (301 unique)

301 unique MARSHA / 301 records — staging identity count SAFE (campus annexes kept distinct)

| Metric | Value |
|--------|------:|
| Near-duplicate pairs reviewed | 103 |
| Campus / high-sim steward pairs | 9 |
| Confirmed same-physical merges | 0 |
| Auto-merges | **0** |

Campus / annex pairs (Solaz Residences, Casa Maat, Westin Baja Point, Royalton / Planet Hollywood siblings) classified **`insufficient_evidence_do_not_merge`** or **`distinct_physical_property`** — different MARSHA, no address/coords proof.

City Express Plus/Junior/Suites siblings in the same city: **`distinct_physical_property`**.

---

## 5. Cross-family vs locked 365 (not coords-only)

| Classification | Count |
|----------------|------:|
| Exact physical — steward review | 0 |
| Probable physical — steward review | 0 |
| Rejected fuzzy (city-corridor brands) | 6 |
| Insufficient evidence — no merge | 8 |
| Auto-merges | **0** |

Name/city/address used; missing Marriott coords do not invent 0,0 matches. City-corridor brand hotels classified rejected_fuzzy / insufficient — not merges.

Exact/probable steward pairs (if any): see `steward-review/05-cross-family-name-city-review.json`.

---

## 6. Completeness / migration risk

| Metric | Count |
|--------|------:|
| Data-eligible | 237 |
| Data-ineligible | 64 |
| Missing address | 301 |
| Missing coordinates | 301 |
| Missing rooms | 301 |
| Missing owner/operator | 301 |
| Missing open date | 301 |
| Sitemap-only | 301 |
| Property-page enriched | 0 |
| BE completion support candidates | 204 |
| Census-identity suitable | 301 |

- Staging migration ready: **YES** (identity fields only)
- Production overwrite ready: **NO**
- Fake fields created this review: **0**

---

## Minor holds (non-blocking for 4-family lock)

- 5 Brand Unconfirmed (1 confirm_brand overlay candidate)
- 9 campus/high-sim physical steward pairs (no merge)
- All 301 records missing coordinates (sitemap limitation) — staging OK
- Material completeness 40% — census identity only; not production-ready

## Blocking issues

_None_

---

## Recommended next step

1. Accept Wave 1D into **combined 4-family Mexico VIC baseline lock** (IHG 195 + Hilton 102 + Choice 68 + Marriott 301).
2. Optional overlay: confirm Mexico City Marriott Reforma → Marriott Hotels (do not rewrite freeze hash without explicit lock task).
3. Keep Brand Unconfirmed soft brands out of BE completion.
4. Do not migrate rooms/coords/owner/open date until first-party enrichment.

---

## Acceptance checklist

- [x] All 301 records reviewed at identity-risk level
- [x] Brand parsing QA complete (28 brands)
- [x] All Brand Unconfirmed reviewed
- [x] Four Points / Sheraton verified
- [x] City Express family verified
- [x] Duplicate / near-duplicate risk reviewed
- [x] Cross-family not coords-only
- [x] No auto-merges
- [x] No fake rooms/owners/open dates
- [x] No Airtable / Webhound / BE activation / production overwrite
- [x] Status: `wave1d_marriott_steward_review_minor_holds_ready_for_4_family_baseline_lock`
