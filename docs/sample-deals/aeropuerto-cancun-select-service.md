# CALA demo sample — Aeropuerto Cancún Select-Service Hotel

**Fixture:** `fixtures/sample-deals/aeropuerto-cancun-select-service.example.json`  
**Sample ID:** `cala-aeropuerto-cancun-select-service-001`  
**Region:** CALA (primary demo set — not Europe/Amsterdam)

> Sample deal for product demonstration only. Reference properties are public comps for factual context only; they are not offered for sale and are not participating in Dealality.

---

## 1. Markdown summary

| Item | Value |
| --- | --- |
| **Fictional project** | Aeropuerto Cancún Select-Service Hotel |
| **Owner entity (fictional)** | Grupo Aeropuerto Cancún Hospitality, S.A. de C.V. |
| **Scenario** | 150–170-key greenfield select-service / upper-midscale near Cancún International Airport |
| **Dealality use cases** | Airport demand, select-service brand comparison, fee sensitivity, F&B/market requirements, shuttle/transport, distribution credibility |
| **Expected readiness** | Ready for External Review (with intentional limiting gaps) |
| **Project type** | New Build |
| **Stage** | Land Under Control Only |

### Reference property layer (public comps only)

| Role | Hotel | Use |
| --- | --- | --- |
| Primary | Hilton Garden Inn Cancún Airport | Airport select-service prototype — keys, F&B, meeting space, shuttle |
| Secondary | Fairfield Inn & Suites Cancun Airport | Fee-sensitive / upper-midscale comp |
| Secondary | Courtyard Cancun Airport | Upscale-hard airport prototype |

### Fictional deal layer (Dealality sample)

- **162 keys** planned (within 150–170 band; comp lists ~186 at HGI for reference only)
- Hard-brand preference with fee discipline; franchise-only structure
- Owner-preferred review brands: HGI, Courtyard, Hyatt Place, Fairfield
- Commercial concerns framed as **assumptions to confirm** — not implied key money offers

---

## 2. Expected Brand Alignment Snapshot behavior

- **Higher alignment signals (typical):** Hilton Garden Inn, Hyatt Place, Fairfield Inn & Suites, Hampton by Hilton — airport select-service, chain scale, project type, owner preference inputs.
- **Conditional signals (typical):** Courtyard, Four Points, Aloft, Holiday Inn Express — standards/F&B or lifestyle vs stated hard-brand preference.
- **Owner-facing rationale:** Paragraph + What Supports / Needs Validation / Could Weaken; technical factors under **Alignment Factors Reviewed**.
- **Key Consideration (page 1):** Business-readable (e.g. select-service airport path, validate F&B/shuttle/commercial assumptions) — not factor-name lists.
- **No recommendation language:** review candidates only.

---

## 3. Intentional gaps (before/after demo)

| Field | Why weak/blank |
| --- | --- |
| Key Competitors | Competitive set not completed on deal record |
| Estimated or Actual RevPAR | Performance anchor missing |
| PIP / CapEx Status | Preliminary budget only |
| Access to Transit or Highway | Shuttle route/frequency TBD |
| F&B Program Type | Brand minimums vs sample program TBD |
| Preferred Third-Party Operators (names) | Operator shortlist not set |

---

## 4. Target list / brand review candidates (8)

| Brand | Parent | Review set source | Why included |
| --- | --- | --- | --- |
| Hilton Garden Inn | Hilton | owner_preferred | Owner preference; primary comp pathway |
| Courtyard by Marriott | Marriott International | owner_preferred | Secondary comp; upscale-hard airport |
| Fairfield Inn & Suites | Marriott International | sample_demo | Fee-sensitive stress-test |
| Hyatt Place | Hyatt | owner_preferred | Select-service operating model |
| Hampton by Hilton | Hilton | pipeline | Fee-efficient hard brand |
| Holiday Inn Express | IHG | pipeline | Lean F&B prototype |
| Four Points by Sheraton | Marriott International | sample_demo | Higher standards / conditional path |
| Aloft | Marriott International | sample_demo | Lifestyle-leaning vs hard-brand preference |

---

## 5. Source list / reference URLs

1. https://www.hilton.com/en/hotels/cunrogi-hilton-garden-inn-cancun-airport/hotel-info/
2. https://www.travelweekly.com/Hotels/Cancun/Hilton-Garden-Inn-Cancun-Airport-p57536849
3. https://www.marriott.com/en-us/hotels/cunfi-fairfield-inn-and-suites-cancun-airport/overview/
4. https://www.marriott.com/en-us/hotels/cuncy-courtyard-cancun-airport/overview/

---

## 6. Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/aeropuerto-cancun-select-service.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/aeropuerto-cancun-select-service.example.json
```

---

## 7. Schema / field notes

- **Merged intake (import rule):** On import, use `fictionalDeal.fields` as the canonical Deals row; use `referenceProperty.fields` only where the fictional layer does not set the same key (physical/market comp facts).
- **Print script:** `print-sample-deal-airtable-map.mjs` lists both layers separately for audit — not duplicate rows in production import.
- **Select values:** `Primary Goal for the Hotel` uses Airtable option `Maximize Cash Flow`; `Project Type` = `New Build`; `Stage of Development` = `Land Under Control Only`.
- **Linked tables in `airtableRows`:** Location & Property, Market - Performance - Deal & Capital Structure, Strategic Intent, Contact & Uploads — mirror Deal Setup write path (`api/schemas/deal-setup-fields.js`).
- **Field name mapping:** Form key `Are you open to considering other brands with favorable terms?` maps to Airtable column `Are you open to lesser-known or emerging brands with favorable terms?` on write (`api/schemas/deal-setup-fields.js`).
- **Not in primary CALA set:** `harborline-airport-amsterdam.example.json` remains a technical/conditional-signal test fixture only.
