# FOUR_HOTEL_INTELLIGENCE_PARITY_MATRIX

> Packet 2.8 · Frozen from existing four-hotel corpus only · **No new paid research**  
> Generated: 2026-09-08  
> Corpus: `HOTEL_INTELLIGENCE_LEARNING_CORPUS_V1` · Inventory: `RESEARCH_RUN_INVENTORY.md`  
> Learning audit: `HOTEL_INTELLIGENCE_LEARNING_EXTRACTION_AUDIT_V1.md`

## Corpus snapshot

| Metric | Value |
|--------|------:|
| Hotels | 4 |
| Webhound runs | 5 |
| Recorded spend | ~$25.4 |
| CALA census baseline | 5,956 |
| `finding_id` binding | **COMPLETE** — KGPV Full 8/8 · Cambridge 8/8 · Sheraton 6/6 · voco 6/6 · KGPV C&O 8/8 |
| Property-specific pipelines | **YES** (gsf / cambridge / mexico cohorts) |
| Shared assembler | **PARTIAL** stub (`hotel-explorer-assembler.js`) — four hotels not fully migrated |

### Hotels

| Hotel | Census ID | Ownership cohort | Primary adapters |
|-------|-----------|------------------|------------------|
| Krystal Grand Puerto Vallarta (KGPV) | `recUNycnMwOVFX0hc` | `gsf-cohort.js` | `from-kgpv-deep-research.js`, `from-webhound-kgpv-modules.js` |
| Cambridge Beaches Resort & Spa | `recIwaP1etgx2g9nA` | `cambridge-cohort.js` | `from-webhound-cambridge-full.js` |
| Sheraton Guadalajara Expo | `recsYJb2R1jarPpK3` | `mexico-explorer-demo-cohort.js` | `from-mexico-explorer-deep-research.js` |
| voco Cancún / fka Real Inn | `recTYaiA4S6fR6ixx` | `mexico-explorer-demo-cohort.js` | `from-mexico-explorer-deep-research.js` |

Completeness codes: **COMPLETE / STRONG / PARTIAL / MISSING / CONFLICTED**

---

## Domain parity matrix

For every domain: **data · source · normalizer · completeness · confidence · missing · hotel-specific code · shared code**

| Domain | KGPV | Cambridge | Sheraton GDL | voco / Real Inn |
|--------|------|-----------|--------------|-----------------|
| **PROPERTY FUNDAMENTALS** | Y · hotel/GSF · deep+census · **STRONG** · HIGH · — · Y (`gsf-cohort`) · Y (rooms helpers) | Y · Dovetail/GoToBermuda · deep `property_profile` · **CONFLICTED** (acres 20/23) · HIGH rooms · Y · Y | Y · Marriott · deep · **STRONG** · HIGH · Y (mexico) · Y | Y · IHG/OTA · **STRONG** rooms; amenities temporal Real Inn · HIGH · Y · Y |
| **OWNERSHIP** | GSF · annual report · **STRONG** · HIGH · Y · Y (MX-PUBCO) | Dovetail · press+first-party · **STRONG** · HIGH · Y · PARTIAL shared | HNF/Newton · PROFECO+press · **PARTIAL** · PROBABLE · Y · PARTIAL | Alliance package · IHG/trade · **PARTIAL** · HIGH package / UNKNOWN vehicle · Y · PARTIAL |
| **PROPCO** | IHVSF · filings · **STRONG** (candidate≠deed) · HIGH · Y · Y | CBHL · Tourism Order · **STRONG** (≠deed) · HIGH · Y · PARTIAL | Inmobiliaria HNF · PROFECO · **STRONG** (≠deed folio) · HIGH · Y · N (no READY playbook id) | **MISSING** / UNKNOWN · registry gap · UNKNOWN · Y · N |
| **ECONOMIC OWNER** | GSF · **COMPLETE** (pubco model) · HIGH · Y · Y | Dovetail · **STRONG** · HIGH · Y · PARTIAL | Newton sphere · **PARTIAL** · PROBABLE · Y · N | Alliance · **STRONG** package · HIGH · Y · PARTIAL |
| **SPONSOR / CONTROL** | Chartwell/Ancira influence · **PARTIAL** · PROBABLE · Y · Y | Phil/Karla journalism · **PARTIAL** · PROBABLE · Y · PARTIAL | Newton family · **PARTIAL** · PROBABLE · Y · N | Justo brothers · **PARTIAL** · PROBABLE · Y · N |
| **OPERATOR** | GSF owner-operator · **COMPLETE** · HIGH · Y · Y | Dovetail vs Pyramid residual · **CONFLICTED** · PROBABLE · Y · Y (temporal lane) | Aimbridge CURRENT; CAPITALI FORMER · **STRONG** · HIGH · Y · PARTIAL | Aimbridge CURRENT; Camino Real FORMER · **STRONG** · HIGH · Y · PARTIAL |
| **BRAND** | Krystal Grand CURRENT · **STRONG** · HIGH · Y · Y | Independent Cambridge · **STRONG** · HIGH · Y · Y | Sheraton/Marriott CURRENT · **STRONG** · HIGH · Y · Y | voco CURRENT; Real Inn FORMER · **STRONG** · HIGH · Y · Y |
| **BRAND HISTORY** | Hilton→Altitude→Grand; Breathless ANNOUNCED · **STRONG** · HIGH · Y · Y | Independent throughout · **PARTIAL** · HIGH · Y · N | HOTSSON→Gran HNF→Sheraton · **STRONG** · HIGH/PROBABLE interim · Y · N | Real Inn→voco · **STRONG** · HIGH · Y · N |
| **OPERATOR HISTORY** | GSF continuous · **PARTIAL** · HIGH · Y · Y | Benchmark→residual · **CONFLICTED** · — · Y · Y | CAPITALI→Aimbridge · **PARTIAL** (handoff UNKNOWN) · HIGH · Y · N | Camino Real→Aimbridge · **PARTIAL** · PROBABLE/HIGH · Y · N |
| **ORGANIZATION** | GSF + Chartwell + Hyatt + Promotora · **STRONG** · HIGH · Y · Y | Dovetail + CBHL + Frascati + Benchmark/Pyramid + Butterfield · **STRONG** · HIGH · Y · PARTIAL | HNF + Aimbridge + Marriott + CAPITALI + HOTSSON · **STRONG** · HIGH · Y · PARTIAL | Alliance + Aimbridge + IHG + Camino Real/Vazol · **STRONG** · HIGH · Y · PARTIAL |
| **PORTFOLIO** | Adjacent Resort + GSF owned set · **STRONG** · HIGH · Y · Y | Dovetail assets · **STRONG** · PROBABLE · Y · PARTIAL | HNF three-hotel Marriott package · **STRONG** · HIGH · Y · PARTIAL | Alliance six-hotel voco package · **STRONG** · HIGH · Y · PARTIAL |
| **PEOPLE** | 10 named · **STRONG** · HIGH titles / authority NV · Y · Y | 5 named · **STRONG** · mixed profiles · Y · Y | 6 role_category · **STRONG** · HIGH · Y · PARTIAL | 9 role_category · **STRONG** · HIGH · Y · PARTIAL |
| **PROFESSIONAL PROFILES** | Several LinkedIn verified · **PARTIAL** · — · Y · Y | Phil/Clarence VERIFIED; others mixed · **PARTIAL** · — · Y · Y | Julieta + others VERIFIED · **STRONG** · — · Y · Y | Multiple VERIFIED; GM NOT_FOUND · **PARTIAL** · Y · Y |
| **RELATIONSHIPS** | 12 edges · **STRONG** · HIGH · Y · Y | 7 rows · **STRONG** · mixed · Y · PARTIAL | 8 edges · **STRONG** · HIGH/PROBABLE · Y · PARTIAL | 6 edges · **STRONG** · HIGH/PROBABLE · Y · PARTIAL |
| **TRANSACTIONS** | 2014 Chartwell buyout; development JV · **STRONG** · HIGH · Y · Y | 2021 acquisition · **STRONG** · HIGH · Y · PARTIAL | Development/opening; no PropCo sale · **PARTIAL** · HIGH · Y · N | 2025 six-hotel sale press · **PARTIAL** · PROBABLE · Y · PARTIAL |
| **DEVELOPMENT** | 2012 open; 2018 Hacienda · **STRONG** · HIGH · Y · Y | Tourism Order renovation · **PARTIAL** (completion uncertified) · — · Y · Y | 2019–2022 build ~$28M · **STRONG** · HIGH · Y · N | Conversion phases / Oct 2026 targets · **PARTIAL** · HIGH · Y · N |
| **CAPITAL / FINANCING** | Public disclosures; related-party leases · **PARTIAL** · HIGH · Y · Y | Butterfield named; amount UNKNOWN · **PARTIAL** · HIGH · Y · PARTIAL | PROFECO contacts; mortgages UNKNOWN · **PARTIAL** · — · Y · N | Financing/liens UNKNOWN · **MISSING** · — · Y · N |
| **SUBMARKET** | Narrative only · **MISSING/PARTIAL** · — · Y · N | West End/Sandys · **STRONG** · — · Y · N | Expo corridor · **PARTIAL** · — · Y · N | Zona Hotelera · **PARTIAL** · — · Y · N |
| **AREA HOTELS** | Adjacent Resort disambiguation · **PARTIAL** · HIGH · Y · N | 8 census-linked comps · **STRONG** · — · Y · N | Sibling Aloft/Delta · **PARTIAL** · HIGH · Y · N | 5 Alliance siblings · **STRONG** · HIGH · Y · N |
| **DEMAND DRIVERS** | Timing/renovation/Breathless (addendum) · **PARTIAL** · — · Y · N | Explicit demand_drivers · **STRONG** · — · Y · N | commercial_pursuit only · **PARTIAL** · — · Y · N | commercial_pursuit · **PARTIAL** · — · Y · N |
| **ACCESS** | Contact/location · **PARTIAL** · — · Y · N | BDA + ferry/marina · **STRONG** · — · Y · N | Address/phone · **PARTIAL** · — · Y · N | Address; phones null · **PARTIAL** · — · Y · N |
| **SOURCES** | Dossier 20; deep curated 11 · **STRONG** · — · Y · Y | webhound_source_count 35 · **STRONG** · — · Y · Y | Dossier 15; store 8 (drift) · **STRONG/CONFLICTED counts** · — · Y · Y | Dossier 15; store 9 · **STRONG/CONFLICTED counts** · — · Y · Y |
| **DEEP RESEARCH** | Fixture + module archive · **COMPLETE** · — · Y · N (hotel adapter) | Fixture + raw MD · **COMPLETE** · — · Y · N | Fixture · **COMPLETE** · — · Y · N (shared Mexico) | Fixture · **COMPLETE** · — · Y · N (shared Mexico) |
| **OPPORTUNITY / CHANGE** | Dedicated C&O addendum **COMPLETE** (8 findings) · HIGH · Y · PARTIAL | Embedded commercial_pursuit · **PARTIAL** (no separate addendum) · — · Y · N | commercial_pursuit · **PARTIAL** · — · Y · N | commercial_pursuit / pre-opening · **PARTIAL** · — · Y · N |

---

## Pipeline verdict

| Question | Answer | Evidence |
|----------|--------|----------|
| Still property-specific primary pipelines? | **YES** | `gsf-cohort` / `cambridge-cohort` / `mexico-explorer-demo-cohort` still route Explorer ownership |
| Same shared assembler for all four? | **NO** (stub only) | `hotel-explorer-assembler.js` exists; four hotels not migrated onto it |
| Shared Mexico adapter for Sheraton + voco? | **YES** | `from-mexico-explorer-deep-research.js` |
| `finding_id` migration | **COMPLETE** | 8/8, 8/8, 6/6, 6/6, 8/8 across Full HI + C&O |

---

## Hard stops honored

- Webhound runs this packet: **0**
- New provider spend: **$0**
- Hotel #5 / 25-hotel research execution: **not started** (selection-only until founder approval)
