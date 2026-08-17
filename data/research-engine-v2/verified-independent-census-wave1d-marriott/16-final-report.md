# Verified Independent Census Wave 1D — Marriott Mexico

**Status:** `wave1d_marriott_mexico_vic_complete_ready_for_combined_4_family_baseline`  
**Baseline prerequisite:** `mexico_vic_baseline_locked_ready_for_marriott_wave1d`  
**Staging only** · No Airtable · No Webhound · No BE activation · No production overwrite · No cross-family auto-merge

---

## 1. Executive summary

Independently reconstructed **301** Marriott Mexico hotels from the official Marriott country hotel-sitemap (**301** unique physical properties). Core **97%** · Material **40%** · Data-eligible **237**. Property Identity V1 and Temporal Affiliation V1 applied. Cross-family auto-merges: **0**.

---

## 2. Source strategy

1. Official Marriott Mexico country hotel-sitemap (primary)  
2. Official property overview URLs from sitemap  
3. Optional property-page enrichment (`RE_V2_MARRIOTT_PROPERTY_PAGES=1`) — default off (Akamai/403 risk)  
4. Blocked pages classified **Blocked**, not closed  

Not used: legacy as proof, Webhound, Airtable, unverified third-party lists.

Sitemap URL: `https://www.marriott.com/en-us/hotel-sitemap/mexico-hotel-sitemap`

---

## 3–4. Totals

| Metric | Value |
|--------|-------|
| Source / independent records | 301 |
| Unique physical properties | 301 |
| Sitemap duplicates rejected | 0 |
| Intra-identity collapses | 0 |

---

## 5. Brand coverage

| Brand | Count |
|-------|------:|
| AC Hotels by Marriott | 8 |
| Aloft Hotels | 4 |
| Apartments by Marriott Bonvoy | 1 |
| Autograph Collection | 10 |
| City Centro by Marriott | 3 |
| City Express Junior by Marriott | 21 |
| City Express Plus by Marriott | 24 |
| City Express Suites by Marriott | 4 |
| City Express by Marriott | 89 |
| Courtyard by Marriott | 29 |
| Delta Hotels | 3 |
| Design Hotels | 25 |
| EDITION | 1 |
| Fairfield by Marriott | 12 |
| Four Points by Sheraton | 11 |
| JW Marriott | 7 |
| Le Méridien | 1 |
| Marriott Bonvoy — Brand Unconfirmed | 5 |
| Marriott Hotels | 10 |
| Moxy Hotels | 1 |
| Renaissance Hotels | 1 |
| Residence Inn by Marriott | 4 |
| Sheraton | 5 |
| St. Regis | 4 |
| The Luxury Collection | 5 |
| Tribute Portfolio | 2 |
| W Hotels | 2 |
| Westin | 9 |

---

## 6. Source split

| Source | Count |
|--------|------:|
| Country sitemap / directory | 301 |
| Property-page enriched | 0 |
| Blocked property pages | 0 |

---

## 7. Property Identity V1

- Unique physical: **301** / 301  
- Fuzzy-name-only merges: **0**  
- Rejected merge log: `06-rejected-merge-log.json`

---

## 8. Temporal Affiliation V1

- Current affiliations seeded: **301** (As of discovery)  
- Fake start dates: **0**  
- Future/opening candidates: **0** (none independently dated this run)

---

## 9–10. Completeness & data-eligible

| Metric | Value |
|--------|-------|
| Core | 97% |
| Material | 40% |
| Data-eligible | 237 |
| Missing rooms | 301 |
| Missing open date | 301 |
| Missing coordinates | 301 |
| Missing management | 301 |
| City present (title-inferred) | 237 |

Unknown preferred over fabrication.

---

## 11. Legacy comparison (post-freeze only)

| Class | Count |
|-------|------:|
| Exact match | 1 |
| Probable | 6 |
| Independent-only | 294 |
| Legacy-only | 17 |

Legacy is comparison-only — never proof.

---

## 12. Cross-family vs locked 365 baseline

| Class | Count |
|-------|------:|
| Exact physical — steward review | 0 |
| Probable physical — steward review | 0 |
| Rejected fuzzy | 0 |
| Auto-merges | **0** |

---

## 13–14. Reflags / rejected fuzzy

See `11-cross-family-steward-queue.json`. No automatic reflags.

---

## 15. Brand Explorer completion readiness

**PARTIAL — steward review** — **no activation**.

---

## 16. Migration readiness

**Staging migration only** · Production overwrite: **No**

---

## 17. Gaps and limitations

- Rooms / open date / owner / operator / coordinates largely Unknown (sitemap lacks structured geo/amenities)  
- City often Medium-confidence title inference  
- Some soft brands remain `Marriott Bonvoy — Brand Unconfirmed`  
- Overview pages often Akamai-blocked  

---

## 18. Recommended next step

1. Steward review cross-family queue  
2. Lock **combined 4-family Mexico VIC baseline** (IHG+Hilton+Choice+Marriott)  
3. Optional: safe Marriott content enrichment for rooms/coords without treating 403 as closed  

---

## Acceptance

- [x] Independent Marriott Mexico reconstruction  
- [x] Source + unique physical counts  
- [x] Brand coverage  
- [x] Core / material / data-eligible  
- [x] Property Identity V1 + Temporal Affiliation V1  
- [x] No fake rooms / open dates / owners / start dates  
- [x] No cross-family fuzzy auto-merges  
- [x] Legacy comparison-only  
- [x] No Airtable / BE activation / production overwrite / Webhound  
- [x] Status: `wave1d_marriott_mexico_vic_complete_ready_for_combined_4_family_baseline`

Runtime ~7s · cost $0 · firewall pre-freeze blocked: true
