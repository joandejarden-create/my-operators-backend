# CALA demo set — build report (2026-05-21)

**Scope:** 12 primary CALA fixtures (11 new + approved Cancún proof). **No Airtable import.**

---

## Validation — all passed

```
OK: proyecto-reforma-urban-conversion.example.json
OK: playa-dorada-resort-repositioning.example.json
OK: cartagena-walled-city-collection.example.json
OK: merida-centro-select-service.example.json
OK: san-juan-bay-turnaround.example.json
OK: panama-city-mixed-use-hotel-component.example.json
OK: cusco-heritage-palace-hotel.example.json
OK: colonial-city-lifestyle-conversion.example.json
OK: riviera-maya-wellness-resort-repositioning.example.json
OK: andean-business-hotel-reflag.example.json
OK: cascadas-lifestyle-hotel-component.example.json
OK: aeropuerto-cancun-select-service.example.json
```

---

## Fixture files created/updated

| File | Sample ID |
| --- | --- |
| `fixtures/sample-deals/proyecto-reforma-urban-conversion.example.json` | `cala-proyecto-reforma-urban-conversion-001` |
| `fixtures/sample-deals/playa-dorada-resort-repositioning.example.json` | `cala-playa-dorada-resort-repositioning-001` |
| `fixtures/sample-deals/cartagena-walled-city-collection.example.json` | `cala-cartagena-walled-city-collection-001` |
| `fixtures/sample-deals/merida-centro-select-service.example.json` | `cala-merida-centro-select-service-001` |
| `fixtures/sample-deals/san-juan-bay-turnaround.example.json` | `cala-san-juan-bay-turnaround-001` |
| `fixtures/sample-deals/panama-city-mixed-use-hotel-component.example.json` | `cala-panama-city-mixed-use-hotel-component-001` |
| `fixtures/sample-deals/cusco-heritage-palace-hotel.example.json` | `cala-cusco-heritage-palace-hotel-001` |
| `fixtures/sample-deals/colonial-city-lifestyle-conversion.example.json` | `cala-colonial-city-lifestyle-conversion-001` |
| `fixtures/sample-deals/riviera-maya-wellness-resort-repositioning.example.json` | `cala-riviera-maya-wellness-resort-repositioning-001` |
| `fixtures/sample-deals/andean-business-hotel-reflag.example.json` | `cala-andean-business-hotel-reflag-001` |
| `fixtures/sample-deals/cascadas-lifestyle-hotel-component.example.json` | `cala-cascadas-lifestyle-hotel-component-001` |
| `fixtures/sample-deals/aeropuerto-cancun-select-service.example.json` | `cala-aeropuerto-cancun-select-service-001` |

**Tooling:** `scripts/build-cala-sample-deals.mjs`, `scripts/cala-sample-deals-data.mjs`

---

## Intentional gaps by sample

| Sample | Intentional gaps (fields) |
| --- | --- |
| Proyecto Reforma | PIP / CapEx Status; PIP Budget Range; Soft vs Hard Brand Preference; Key Competitors; Is the hotel currently branded? |
| Playa Dorada | Plan to Self-Manage or Hire Third Party?; PIP / CapEx Status; Soft vs Hard Brand Preference; Group vs Transient Mix |
| Cartagena | PIP Budget Range; Key Competitors; Estimated or Actual RevPAR |
| Mérida | Estimated or Actual RevPAR; Key Competitors |
| San Juan Bay | Estimated or Actual RevPAR; PIP / CapEx Status; Plan to Self-Manage…; Decision Timeline; Preferred Third-Party Operators |
| Panama Mixed-Use | Current Form of Site Control; Zoning Status; Plan to Self-Manage…; Key Competitors |
| Aeropuerto Cancún | Key Competitors; RevPAR; PIP / CapEx Status; Access to Transit; F&B Program Type; Preferred Third-Party Operators |
| Cusco Heritage | PIP / CapEx Status; PIP Budget Range; Plan to Self-Manage…; Regulatory or Permitting Issues? |
| Colonial City | Soft vs Hard Brand Preference; F&B Program Type; Proposal Deadline |
| Riviera Maya Wellness | RevPAR; PIP / CapEx Status; Soft vs Hard Brand Preference; Plan to Self-Manage… |
| Andean Business Reflag | Franchise history field; Primary Demand Drivers Other; PIP Budget Range |
| Cascadas Lifestyle | Ownership Structure; Preferred Third-Party Operators; Key Competitors |

---

## Expected readiness stage by sample

| Sample | `expectedReadinessStage` |
| --- | --- |
| Proyecto Reforma | Advancing |
| Playa Dorada | Ready for External Review |
| Cartagena | Ready for External Review |
| Mérida | Ready for External Review |
| San Juan Bay | Advancing |
| Panama Mixed-Use | Advancing |
| Aeropuerto Cancún | Ready for External Review |
| Cusco Heritage | Advancing |
| Colonial City | Ready for External Review |
| Riviera Maya Wellness | Advancing |
| Andean Business Reflag | Ready for External Review |
| Cascadas Lifestyle | Ready for External Review |

---

## Target list row counts

| Sample | `targetListRows` count |
| --- | ---: |
| Proyecto Reforma | 7 |
| Playa Dorada | 7 |
| Cartagena | 7 |
| Mérida | 7 |
| San Juan Bay | 6 |
| Panama Mixed-Use | 7 |
| Aeropuerto Cancún | 8 |
| Cusco Heritage | 6 |
| Colonial City | 7 |
| Riviera Maya Wellness | 7 |
| Andean Business Reflag | 7 |
| Cascadas Lifestyle | 7 |

---

## Schema / field notes (no blockers)

| Topic | Note |
| --- | --- |
| **Merged intake** | `buildAirtableFieldMap()` uses fictional-over-reference merge (`lib/sample-opportunity-deal-schema.js`). |
| **Franchise field typo** | Form key uses *agreement*; many Airtable bases store *agreeement* — see `api/schemas/deal-setup-fields.js`. |
| **Open brands field** | Form `Are you open to considering other brands…` → Airtable `Are you open to lesser-known or emerging brands…`. |
| **Preferred brands strings** | Some entries use shorthand (e.g. `Hyatt Ziva`) vs full legal brand names in Brand Setup — normalize on import. |
| **All-Inclusive brands** | `Hilton All-Inclusive` / `Marriott All-Inclusive` are illustrative owner inputs, not official flag names — map to AI programs on import. |
| **Multi-market Andean sample** | Fictional deal is Bogotá-based; AC Hotel Lima is **market reference only** in `referenceProperty.sources`. |
| **Linked table rows** | Each fixture includes sample rows for Location, Market Performance, Strategic Intent, Contact — verify column names match base before import. |

---

## Public reference facts — verification caveats

Facts are **public_reference** or **inferred_from_reference** where sourced from brand/OTA sites; **fictional_sample_assumption** for all deal economics, owner contacts, and planned key counts.

| Reference hotel | Caveat |
| --- | --- |
| Downtown Mexico (Design Hotels) | Room count / meeting SF not verified to single public figure — comp band inferred for CDMX upscale boutique class. |
| W Punta Cana | Used as primary AI class reference; exact key count for *fictional* 224-key project is assumption. |
| Hyatt Place San Juan | Public URL points to Bayamón property — verify correct metro property if tightening San Juan comp. |
| AC Hotel Lima Miraflores | Used only as secondary **market** comp for Andean sample (deal is Bogotá). |
| Nômade Tulum / Be Tulum / Habitas | Independent resorts — limited standardized public key counts; 90–130 band is inferred. |
| Belmond Hotel Monasterio | Ultra-luxury reference only — not in target list. |

No fixture text implies reference hotels are for sale, seeking brand/operator, or on Dealality.

---

## Next steps (after review)

1. Editorial pass on `fieldSources` and public URLs.  
2. Dry-run import JSON (Deals + linked tables + target list).  
3. Seed Airtable only after explicit approval.
