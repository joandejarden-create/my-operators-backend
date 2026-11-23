# Track C — CALA Discovery V1 Canonical Hotel & Owner Integration

**As of:** 2026-11-23 · **CUSTOMER_BLOCKED** · **Paid research:** $0  
**Source:** Track A `dealsignal-global-source-cala-discovery-v1`  
**Geography rule:** hotel property location (not owner HQ) · Mexico first

## Verdict

All **four named Mexico hotels matched existing HPC** — **0 new creates** (duplicate prevention enforced). Cabo Rojo kept as destination parent; Santander left unresolved. Inventory ownership notes + HE handoffs written; idempotent re-apply stable.

## 1. Matched HPC identities

| Hotel | HPC ID | Match notes |
|-------|--------|-------------|
| Courtyard by Marriott Playa del Carmen (ex Wyndham Garden) | `rechcqksn3VfFD3c6` | Brand-directory Courtyard (`cunpy`); alias collision with Wyndham Garden `recf65FZ1FuVVqIrz` (same Playacar address) — **no third create**; steward merge candidate recorded |
| The Yucatán Resort Playa del Carmen | `recY9Cxxo1iAbJgOt` | Existing Tapestry Collection by Hilton record |
| Holiday Inn Monterrey Valle | `recJRNuLr8fEiGImt` | Existing HI Monterrey Valle (San Pedro Garza García) |
| Holiday Inn Puebla La Noria | `recfpT5RJBZAORXs2` | Disambiguated from Holiday Inn Puebla Finsa via Juan Pablo II address + `pueln` URL |

## 2. Approved new HPC creations

**None.** CREATE path aborted on multi-candidate collision; geo/URL disambiguation selected existing records instead.

## 3. Duplicate / collision checks

- Live collision + address / brand-code gate.
- Courtyard: MULTIPLE_STRONG_MATCHES → resolved to Courtyard HPC; Wyndham Garden retained as `samePropertySiblingHpc=recf65FZ1FuVVqIrz` (historical brand, same asset).
- Puebla: La Noria vs Finsa → address+`pueln` selected La Noria only.
- Ambiguous name-only accepts: **0**.

## 4. Verified owners and operators

| Hotel | Owner | Operator / relationship |
|-------|-------|-------------------------|
| Courtyard PDC | Fibra Inn (FINN13 / Trust DB/1616) | Servicios Hoteleros FINN (internalized); Marriott Courtyard **franchise** (brand ≠ operator) |
| Yucatán Resort PDC | FibraHotel (FIHO 12) | Lease-model transition after 2026-04-30 suspension; prior Playa AI op — **current lease operator TBD** |
| HI Monterrey Valle | Fibra Inn | Servicios Hoteleros FINN + IHG franchise |
| HI Puebla La Noria | Fibra Inn | Servicios Hoteleros FINN + IHG franchise (non-renewal signal monitored) |

## 5. Portfolio relationships

- **Fibra Inn portfolio bridge:** Courtyard PDC + HI Monterrey Valle + HI Puebla La Noria share verified owner; parent inventory `disc_ds_cala_v1_finn_internalization_parent_2026` notes-only (no hotel HPC).
- **FibraHotel:** Yucatán Resort linked; lease-portfolio context preserved (Villahermosa/Cd. del Carmen pattern referenced in sources, not invented as hotels here).
- Courtyard ↔ Wyndham Garden: same-property historical alias, not separate ownership.

## 6. Hotel Explorer deep-link availability

Artifact: `HOTEL_EXPLORER_HANDOFF.json` — **4 READY** items.

Target pattern: `http://localhost:8080/app#/opportunity-radar` + open HE by HPC ID  
(`rechcqksn3VfFD3c6`, `recY9Cxxo1iAbJgOt`, `recJRNuLr8fEiGImt`, `recfpT5RJBZAORXs2`)

No placeholder hotels for drawer links.

## 7. Missing / deferred

| Item | Reason |
|------|--------|
| Cabo Rojo destination | DESTINATION_PARENT_NOT_HOTEL — wait for named hotels |
| Santander Bucaramanga concesión | HOTEL_IDENTITY_UNPROVEN — SECOP aggregator only |
| Yucatán Resort current lease operator | Not named in tested FIHO disclosures |
| Courtyard / Wyndham HPC steward merge | Two HPC rows, one physical hotel — IC/steward |

## 8. Supporting source URLs (selected)

- Marriott Courtyard PDC: `https://www.marriott.com/en-us/hotels/cunpy-courtyard-playa-del-carmen/overview/`
- BMV Courtyard conversion: `https://www.bmv.com.mx/docs-pub/eventfid/eventfid_1593193_1616_1.pdf`
- Hilton Yucatán / Tapestry: `https://www.hilton.com/en/hotels/czmpdup-the-yucatan-playa-del-carmen/`
- FIHO Yucatán lease: `https://bmv.com.mx/docs-pub/eventfid/eventfid_1574632_1596_1.pdf`
- HI Monterrey Valle: `https://www.ihg.com/holidayinn/hotels/us/en/monterrey/mtyzv/hoteldetail`
- HI Puebla La Noria: `https://www.ihg.com/holidayinn/hotels/us/en/puebla/pueln/hoteldetail`
- FINN internalization (ES): `https://fibrainn.mx/storage/docs/fibra-inn-finaliza-proceso-de-internalizacion-de-la-operacion-en-28-hoteles.pdf`
- Cabo Rojo Pro-Pedernales (ES): Pro-Pedernales award + Grupo Puntacana Fitur notes (see inventory)
- Santander: aggregator only — limitation preserved

## 9. Airtable persistence

| Metric | Result |
|--------|--------|
| Mode | APPLY_WITH_HPC_CREATE (creates unused — matches only) |
| HPC creates | **0** |
| HPC matches | **4** |
| Inventory linked / notes | **7** (4 hotels + 3 parent/watch) |
| Read-back | 4/4 hotels |
| Idempotency MATCHED | 4/4 |
| Failures | 0 |
| Schema | none new |
| Classification | **PERSISTENCE_VALIDATED** (CUSTOMER_BLOCKED; not FULL_E2E / not customer-published) |

## 10. Integration Controller handoff

Artifact: `IC_HANDOFF.json`

Return canonical IDs above to Track A discoveryPropertyIds. Deferred Cabo Rojo + Santander explicitly. Prior DealSignal global identity wave preserved. Exa cache/ledger + branch dirty work untouched.

## Artifacts

- `scripts/hif-track-c-cala-discovery-v1-identity.mjs`
- `reports/sec-edgar-hif-track-c-cala-discovery-v1-identity/`
