# Identity & profile readiness register

**Generated:** see `IDENTITY_PROFILE_READINESS.json` / `EXISTING_ADP_BASELINE.json`  
**Coordinates with:** `reports/gdi-six-hotel-e2e/HOTEL_IDENTITY_RESOLUTION.md`  
**No HPC or ADP records created in this preparation task.**

## Disposition summary

| # | Hotel | HPC | ADP propertyId | Readiness |
|---|-------|-----|----------------|-----------|
| 1 | Renaissance NY Times Square | `recG66DQJKP2c0UNh` | `adp_renaissance_times_square` | **READY_FOR_REFRESH_OR_REMEASURE** |
| 2 | Falls Church Marriott Fairview Park (WASFP) | VERIFIED_ABSENT | none (proposed slug only) | **BLOCKED_IDENTITY** |
| 3 | Renaissance Arlington Capital View (WASPY) | VERIFIED_ABSENT | none | **BLOCKED_IDENTITY** |
| 4 | Charlotte Marriott City Center (CLTCC) | VERIFIED_ABSENT | none | **BLOCKED_IDENTITY** |
| 5 | Tampa Airport Marriott (TPAAP) | VERIFIED_ABSENT | none | **BLOCKED_IDENTITY** |
| 6 | Waterstone Boca | `recgMYovrrZDJMqzX` | `adp_waterstone_boca_raton` | **READY_FOR_RESEARCH_PUBLICATION_GATED** (external distribution HOLD on existing publish) |

Proposed ADP slugs for absent hotels (`adp_falls_church_marriott_fairview_park`, `adp_renaissance_arlington_capital_view`, `adp_charlotte_marriott_city_center`, `adp_tampa_airport_marriott`) are **labels for future fixtures only** — not census IDs and not live config.

## HPC requirement for ADP

ADP census join uses `census-links-v1.json` → HPC `rec…`. Same stewardship gate as GDI: create HPC via gated census steward workflow, then add ADP profile fixture + census link. Do not invent `rec…` or duplicate hotels.

## Profile verification posture (existing two)

### Renaissance (`adp_renaissance_times_square`)

| Fact | Value | Source posture |
|------|-------|----------------|
| Name / brand | Renaissance New York Times Square Hotel · Renaissance/Marriott | Official Marriott URL NYCRN in fixture |
| Address / market | Midtown / Times Square | Profile geography |
| Guestrooms | **310** | Distinct from meeting inventory |
| Meeting space | **5000** sq ft · 4 rooms · Broadway Ballroom 2400 | ADP fixture; re-verify on activation |
| Declared comps | Marquis, W TS, Westin TS, Crowne Plaza TS, Hilton TS, Knickerbocker | Declared set ≠ STR official; ≠ AI-observed set |
| Confusable exclusions | Renaissance Midtown / Chelsea / Harlem / Downtown | Enforced in identity aliases |

### Waterstone (`adp_waterstone_boca_raton`)

| Fact | Value | Source posture |
|------|-------|----------------|
| Name / brand | Waterstone Resort & Marina · Curio / Hilton | Official Hilton + owned domain |
| Guestrooms | **139** | Distinct from meetings |
| Meeting / events | **18899** sq ft + outdoor terraces | Resort/marina social + meetings — not convention hotel |
| Declared comps | Boca Raton / Boca Beach Club / Renaissance Boca / etc. | Use-case dependent vs large convention hotels |

### Four Marriott absences — marketing leads only

From `reports/gdi-six-hotel-e2e/OFFICIAL_PROPERTY_FACT_LEADS.json` (unverified until canonical profile path):

- **WASFP:** Fairview Park address; neighborhood shuttle ≠ airport; meeting sq ft marketing claim.  
- **WASPY:** Distinct from Residence Inn Capital View; DCA/metro shuttle claims — verify.  
- **CLTCC:** Conflicting meeting sq ft claims; M Beta unconfirmed.  
- **TPAAP:** Terminal connection claimed; **not** unrestricted gate access; rooms map-PDF lead 298 needs High verify; do not assume crew/disruption demand.

Unknown amenities stay unknown. No recommendations from unsupported facts.

## Stewardship next steps (blocked hotels)

1. GDI/HPC steward create (payloads in GDI identity resolution) — **not this ADP task**.  
2. Author ADP property profile fixture (mirror Renaissance/Waterstone schema).  
3. Register `census-links-v1.json` after real `rec…` exists.  
4. Entity registry / core governance / subject aliases as required by baseline preflight.  
5. Then `--preflight-only` on a hotel-specific baseline module or dry-run `run-ai-demand-positioning.mjs`.
