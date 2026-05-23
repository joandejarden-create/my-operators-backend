# CALA primary Dealality demo sample opportunities (12)

**Governance:** [docs/sample-opportunity-deal-governance.md](../../docs/sample-opportunity-deal-governance.md)  
**Proof-of-process fixture:** `aeropuerto-cancun-select-service.example.json` (approved)  
**Generator:** `node scripts/build-cala-sample-deals.mjs` (11 fixtures; Cancún authored separately)

Europe/Amsterdam samples (e.g. `harborline-airport-amsterdam.example.json`) are **technical test fixtures only** — not part of this set.

---

## Status — all 12 ready for review (no Airtable import)

| # | Fictional project | Fixture | Readiness (expected) | Target list | Gaps |
| --- | --- | --- | --- | ---: | ---: |
| 1 | Proyecto Reforma Urban Conversion | `proyecto-reforma-urban-conversion.example.json` | Advancing | 7 | 5 |
| 2 | Playa Dorada Resort Repositioning | `playa-dorada-resort-repositioning.example.json` | Ready for External Review | 7 | 4 |
| 3 | Cartagena Walled City Collection | `cartagena-walled-city-collection.example.json` | Ready for External Review | 7 | 3 |
| 4 | Mérida Centro Select-Service | `merida-centro-select-service.example.json` | Ready for External Review | 7 | 2 |
| 5 | San Juan Bay Turnaround | `san-juan-bay-turnaround.example.json` | Advancing | 6 | 5 |
| 6 | Panama City Mixed-Use Hotel Component | `panama-city-mixed-use-hotel-component.example.json` | Advancing | 7 | 4 |
| 7 | Aeropuerto Cancún Select-Service Hotel | `aeropuerto-cancun-select-service.example.json` | Ready for External Review | 8 | 6 |
| 8 | Cusco Heritage Palace Hotel | `cusco-heritage-palace-hotel.example.json` | Advancing | 6 | 4 |
| 9 | Colonial City Lifestyle Conversion | `colonial-city-lifestyle-conversion.example.json` | Ready for External Review | 7 | 3 |
| 10 | Riviera Maya Wellness Resort Repositioning | `riviera-maya-wellness-resort-repositioning.example.json` | Advancing | 7 | 4 |
| 11 | Andean Business Hotel Reflag | `andean-business-hotel-reflag.example.json` | Ready for External Review | 7 | 3 |
| 12 | Cascadas Lifestyle Hotel Component | `cascadas-lifestyle-hotel-component.example.json` | Ready for External Review | 7 | 3 |

---

## Per-fixture docs

| Fixture | Summary | Airtable map |
| --- | --- | --- |
| proyecto-reforma | [docs/sample-deals/proyecto-reforma-urban-conversion.md](../../docs/sample-deals/proyecto-reforma-urban-conversion.md) | [proyecto-reforma-urban-conversion-airtable-map.md](../../docs/sample-deals/proyecto-reforma-urban-conversion-airtable-map.md) |
| playa-dorada | [playa-dorada-resort-repositioning.md](../../docs/sample-deals/playa-dorada-resort-repositioning.md) | [playa-dorada-resort-repositioning-airtable-map.md](../../docs/sample-deals/playa-dorada-resort-repositioning-airtable-map.md) |
| cartagena | [cartagena-walled-city-collection.md](../../docs/sample-deals/cartagena-walled-city-collection.md) | [cartagena-walled-city-collection-airtable-map.md](../../docs/sample-deals/cartagena-walled-city-collection-airtable-map.md) |
| merida | [merida-centro-select-service.md](../../docs/sample-deals/merida-centro-select-service.md) | [merida-centro-select-service-airtable-map.md](../../docs/sample-deals/merida-centro-select-service-airtable-map.md) |
| san-juan | [san-juan-bay-turnaround.md](../../docs/sample-deals/san-juan-bay-turnaround.md) | [san-juan-bay-turnaround-airtable-map.md](../../docs/sample-deals/san-juan-bay-turnaround-airtable-map.md) |
| panama | [panama-city-mixed-use-hotel-component.md](../../docs/sample-deals/panama-city-mixed-use-hotel-component.md) | [panama-city-mixed-use-hotel-component-airtable-map.md](../../docs/sample-deals/panama-city-mixed-use-hotel-component-airtable-map.md) |
| cancun | [aeropuerto-cancun-select-service.md](../../docs/sample-deals/aeropuerto-cancun-select-service.md) | [aeropuerto-cancun-airtable-map.md](../../docs/sample-deals/aeropuerto-cancun-airtable-map.md) |
| cusco | [cusco-heritage-palace-hotel.md](../../docs/sample-deals/cusco-heritage-palace-hotel.md) | [cusco-heritage-palace-hotel-airtable-map.md](../../docs/sample-deals/cusco-heritage-palace-hotel-airtable-map.md) |
| colonial-city | [colonial-city-lifestyle-conversion.md](../../docs/sample-deals/colonial-city-lifestyle-conversion.md) | [colonial-city-lifestyle-conversion-airtable-map.md](../../docs/sample-deals/colonial-city-lifestyle-conversion-airtable-map.md) |
| riviera-maya | [riviera-maya-wellness-resort-repositioning.md](../../docs/sample-deals/riviera-maya-wellness-resort-repositioning.md) | [riviera-maya-wellness-resort-repositioning-airtable-map.md](../../docs/sample-deals/riviera-maya-wellness-resort-repositioning-airtable-map.md) |
| andean | [andean-business-hotel-reflag.md](../../docs/sample-deals/andean-business-hotel-reflag.md) | [andean-business-hotel-reflag-airtable-map.md](../../docs/sample-deals/andean-business-hotel-reflag-airtable-map.md) |
| cascadas | [cascadas-lifestyle-hotel-component.md](../../docs/sample-deals/cascadas-lifestyle-hotel-component.md) | [cascadas-lifestyle-hotel-component-airtable-map.md](../../docs/sample-deals/cascadas-lifestyle-hotel-component-airtable-map.md) |

---

## Validate / print field map

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/<fixture>.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/<fixture>.example.json
```

Regenerate the 11 generated fixtures after editing `scripts/build-cala-sample-deals.mjs` or `scripts/cala-sample-deals-data.mjs`:

```bash
node scripts/build-cala-sample-deals.mjs
```

---

## Airtable import (dry-run → seed)

### 1. Dry-run (no writes)

```bash
node scripts/dry-run-cala-sample-deal-import.mjs
```

Writes `data/cala-sample-import-dry-run/<slug>.import.json` + `manifest.json` — routed fields for Deals, Location & Property, Market - Performance, Strategic Intent, Contact & Uploads, and Target List rows.

### 2. Review

Inspect `data/cala-sample-import-dry-run/manifest.json` and per-slug `.import.json` files before applying.

### 3. Seed Airtable (requires `.env`)

```bash
node scripts/seed-cala-sample-deals.mjs --apply
node scripts/seed-cala-sample-deals.mjs --apply --clean   # remove prior "Sample — CALA demo" deals first
node scripts/seed-cala-sample-deals.mjs --apply --only merida-centro-select-service
```

Optional env:

- `CALA_SAMPLE_USER_RECORD_ID` — link deals to a Users record  
- `CALA_SAMPLE_COMPANY_RECORD_ID` — link Company Profile  

Results: `data/cala-sample-import-results.json` (deal `rec…` IDs per sample).

**Routing:** `lib/sample-deal-airtable-import.js` uses the same field→table classification as Deal Setup PATCH.

### Import rules

1. Merge `fictionalDeal.fields` over `referenceProperty.fields` (fictional wins on conflict).  
2. Create linked rows: Location & Property, Market - Performance - Deal & Capital Structure, Strategic Intent, Contact & Uploads.  
3. Load target list as review candidates only — never as recommendations.  
4. Deals tagged `Deal Status` = `Sample — CALA demo` for cleanup via `--clean`.
