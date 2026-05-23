# Radisson Blu — Choice CALA / Brand Explorer reference

**Sources:** `RADBLU_OnePager_New_Final.pdf`, `RB_PitchDeck_Final.pdf` (Choice CALA regionalization), [choicehotels.com/radisson-blu](https://www.choicehotels.com/radisson-blu), Brand DNA slide (Real Estate KPIs). Use for **presentation slots** and owner-education copy; **Legal/Comms** approves customer-facing wording.

**Airtable brands (two rows, same slot-key mapping):**

| Brand Name | Role |
|------------|------|
| **Radisson Blu** | **Radisson Hotel Group** global / EMEA–APAC–centric brand (separate company & region). Brand Basics: Parent Company = Radisson Hotel Group. Explorer presentation TBD or RHG-specific fixtures. |
| **Radisson Blu (Choice)** | **Choice / Alpha Brand Studios** Americas development (mirrors **Radisson (Choice)** Explorer). All CALA pitch-deck fixtures target this name. |

Duplicate: `node scripts/duplicate-brand-explorer-brand.mjs --from "Radisson Blu (Choice)" --to …` or clone presentation via `clone-brand-explorer-presentation.mjs`.

---

## Brand DNA (from pitch / one-pager)

| Layer | Themes |
|-------|--------|
| **Core** | Feel the difference — upper-upscale, style + substance |
| **Experience** | Enticing Moments — belonging, discovery, memorable stays |
| **Design** | Nordic Nouveau — Scandinavian-inspired, dynamic spaces |
| **Service** | Curatorial Warmth — gallery-curator warmth, confident team |
| **Key experiences** | Social spaces, F&B, studios/apartments, wellness, functional spaces |

### Real estate KPIs (illustrative — verify in agreement / FDD)

| KPI | Value |
|-----|--------|
| **Keys** | 150+ (prototype / program reference from Brand DNA slide) |
| **Positioning** | Upper Upscale |
| **Locations** | Key capital cities, airport gateways, major leisure destinations |
| **Product** | Urban / airport / resorts / serviced apartments |
| **Design style** | Memorable / stylish / purposeful |
| **GIA per key** | ~55–75 sqm |

---

## One-pager highlights

- **Tagline:** Think in Black & White Blu  
- **Guest:** The Inspired Professional — allergic to boring; traditional spaces feel uninspired  
- **Development types:** New construction, adaptive reuse, conversions  
- **Target markets:** Top urban and resort destinations  
- **Americas presence (2024 one-pager):** 10 open (3 domestic, 7 international); pipeline 1 U.S., 4 international (YE 2023)  
- **Contacts:** development@choicehotels.com · upscalebychoice.com  

---

## Pitch deck highlights (verify before external use)

- Largest upper-upscale brand in Europe narrative; **339 hotels globally** (footnote: many operated by Radisson Hotel Group, unaffiliated with Choice)  
- **Americas:** 10 hotels (Chicago, Bloomington, Fargo, Canada, Brazil, Chile, Aruba, etc.)  
- **U.S. performance (FDD Item 19, YE 2023 cited in deck):** illustrative ADR / occupancy / RevPAR — individual results vary  
- **Heritage:** 1960 Arne Jacobsen design hotel origin; Americas reimagination  
- **Choice platform:** Choice Privileges, ROCs revenue support, Choice University, procurement, distribution  

---

## Brand Explorer load sequence (repo)

1. **Audit:** `node scripts/audit-brand-explorer-basics.mjs --name "Radisson Blu"`  
2. **Clone slot structure from Radisson (Choice):**  
   `node scripts/clone-brand-explorer-presentation.mjs --from "Radisson (Choice)" --to "Radisson Blu" --only-missing`  
3. **Brand Basics:**  
   `node scripts/apply-radisson-brand-basics-brand-on-a-page.mjs --brand-name "Radisson Blu" --fixture fixtures/brand-basics-radisson-blu.json`  
4. **Push all Blu fixtures (after clone):** see [radisson-blu-choice-fixtures.md](./radisson-blu-choice-fixtures.md).  
   `npm run build-radisson-blu-fixtures && npm run apply-radisson-blu-choice-fixtures`  
   Uses `--brand-record-id recWPEvxBQxVVzSq3` (required on Windows—do not rely on `--brand-name "Radisson Blu (Choice)"` via npm or it may match **Radisson** only).
5. **Images:** attach hero photos on `footprint.openings` and `materials.gallery.*` rows in Airtable.

---

## Slot mapping

Unchanged from [brand-explorer-presentation-slots.md](./brand-explorer-presentation-slots.md) and Radisson (Choice) — see [radisson-choice-cala-one-pager-reference.md](./radisson-choice-cala-one-pager-reference.md) for shared Choice portfolio mechanics (loyalty naming, footprint vs static PDF counts).
