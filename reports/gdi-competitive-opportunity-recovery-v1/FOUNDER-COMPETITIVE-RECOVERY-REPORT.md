# Bethesda Competitive Opportunity Recovery V1

Generated: 2026-11-12

## Headline

| Metric | Count |
|--------|------:|
| Existing verified CMI (prior #222 audit floor) | **4** |
| Additional bookings independently verified this recovery | **+2** (ICON Awards, CASSS CGTP) |
| **Verified Competitive (A)** | **6** |
| Needs Verification (B) — commercial surface | **4** |
| Historical Competitive (C) — commercial surface | **1** |
| **Competitive eligible (commercial surface)** | **11** |
| Research-universe competitive signals (pre-surface) | 46 |
| Not Competitive (D) in research universe | 42 |
| Duplicates excluded | 0 |
| Unique All inventory (Hotel sales baseline) | **67** |
| Unique All inventory (internal research union) | **88** |
| Published customer authorization boundary | **19** (unchanged) |

> Competitive total ≠ verified-booking total. Status filter separates confidence.

## What was wrong

Market Scope Competitive previously exposed **only** `cmiVerified` rows (4).  
Competitor-hosted research already existed in the 67 baseline / historical reconstruction (ICON, CASSS, IFE, AFCEA historical lodging, etc.) but was invisible in Competitive because Needs Verification / Historical were not eligible populations.

## Recovered verified bookings (beyond #222 seven)

| ID | Event | Competitor | Evidence |
|----|-------|------------|----------|
| `gdi_s4912_6c8a62552bfa` | Maryland Tech Council ICON Awards 2027 | Bethesda North Marriott | MTC calendar listing |
| `gdi_s4912_732496c2f8aa` | CASSS Cell & Gene Therapy Symposium 2027 | Bethesda North Marriott | casss.org hotel-information |

Prior verified (retained): ACVNU → The Bethesdan; NADO 2027/2028 → Crystal Gateway; ECS 251 → Marriott Marquis.

## Needs Verification (commercial surface examples)

- U.S. IFE 2027 — Bethesda North thesis without official housing URL yet
- AAD 2028 / World Biomaterials 2028 — venue/city without verified hotel block
- Other competitor-named overflow theses without edition-specific booking proof

## Historical Competitive

- AFCEA Bethesda Health IT Summit — **2026** lodging verified at Bethesda North Marriott; **2027** edition not confirmed (Hyatt claim previously demoted)

## Market Scope UI

Preserved: **All / Hotel / Competitive**

Within Competitive, secondary status filter:

- All Competitive
- Verified
- Needs Verification
- Historical

Tiles show explicit **Verified Competitive / Needs Verification / Historical Competitive** pills.  
No fifteenth demand segment. Hotel and Competitive remain independent (a record may qualify for both).

## Guards preserved

- Bethesda 67 baseline IDs
- Published 19 authorization boundary
- Fourteen demand segments
- 64-column CSV contract
- No auto-publish / no paid research
- CMI is not a demand segment

## Browser verification (local :8095)

- Market Scope Competitive (11) with status filter 11 / 6 / 4 / 1
- Verified filter shows ACVNU, NADO×2, ECS, ICON, CASSS
- ICON + CASSS visible with competitor titles
- CSV export remains 64 columns
