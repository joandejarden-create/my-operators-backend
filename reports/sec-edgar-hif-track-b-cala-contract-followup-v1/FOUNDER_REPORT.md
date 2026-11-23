# Track B — CALA Contract Intelligence Follow-up V1

**As of:** 2026-11-23  
**Publication:** CUSTOMER_BLOCKED · **Cost:** $0  
**Starts from:** Track A `dealsignal-global-source-cala-discovery-v1`

## 1. Hotel-specific franchise dates found
- **Holiday Inn Monterrey Valle:** Prior franchise expired; new Holiday Inn franchise executed (FY2025 approval). New expiration: UNKNOWN. Source: https://fibrainn.mx/assets/docs/2025-corporate-governance-letters-updated_69d5ae218a163.pdf
- **Wyndham Garden Guadalajara Acueducto:** Conversion to Courtyard by Marriott approved. New expiration: UNKNOWN. Source: https://fibrainn.mx/assets/docs/2025-corporate-governance-letters-updated_69d5ae218a163.pdf
- **Portfolio IHG/Hilton/Wyndham/Marriott:** Only 10–20 year term range disclosed. New expiration: NOT_HOTEL_SPECIFIC. Source: https://fibrainn.mx/assets/docs/f1616-fibra-inn-notas-ingles-4t2025_6a875905dc66b.pdf — Do not assign range to individual hotels

## 2. Management agreements still potentially open
- **The Westin Monterrey Valle** — Aimbridge Latam, 174 rooms, as of 2026-09-30; expiration UNKNOWN. Co-owned; Bajo acuerdo de gestión; room-count match to Aimbridge residual 424
- **JW Marriott Monterrey Valle** — Aimbridge Latam, 250 rooms, as of 2026-09-30; expiration UNKNOWN. Co-owned; internalization-at-expiry policy constrains competing MCT

Closed (not open MCT): Holiday Inn Puebla La Noria HMA non-renewal → terminated 2025-07-24 → internalized 2025-08-01; Holiday Inn Monterrey Valle ops internalized 2025-09-01; 28-hotel Aimbridge buyout completed 2026-01-15 (prior ISE wave)

## 3. Lease versus HMA reconciliation
- **Villahermosa:** prior MANAGEMENT + FRANCHISE terminated → successor LEASE (arrendamiento) InHouse from 2026-07-01
- **Ciudad del Carmen:** prior Fiesta Inn affiliation → successor LEASE fixed-rent InHouse from 2026-04-01
- **The Yucatán Resort Playa del Carmen:** prior Ops suspended 2026-04-30 for lease portfolio → successor LEASE target; counterparty/brand unnamed

## 4. Owners / parties
- Fibra Inn: Trust DB/1616 / FINN13; Servicios Hoteleros FINN; Aimbridge Latam (residual)
- FibraHotel: FIHO12; InHouse Hotels (lessee/brand)
- Cabo Rojo: Pro-Pedernales; Consorcio Cabo Rojo / Grupo Puntacana
- Santander: Departamento de Santander (asset type unconfirmed)

## 5–6. Sources, dates, limitations
See `RESEARCH_SUMMARY.json` and dossier `evidenceSnippet` fields. Spanish/English titles preserved. No generic 10–20 year franchise terms assigned to hotels.

## 7. Airtable persistence / read-back
- Dry-run: creates=26, noops=7, failures=0
- Apply: creates=27, noops=7, failures=0 (inventory 3, findings 9, contracts 8, milestones 4)
- Idempotent re-apply: creates=0, noops=34, failures=0
- Staged (non-persisted): franchise 10–20yr gap parent, Santander, Cabo Rojo destination
- CUSTOMER_BLOCKED; no new schema/enums

## 8. Track A commercial classification handoff
- Emerging: Westin + JW Marriott Monterrey Valle Aimbridge HMAs still active (expiration unknown; internalization-at-expiry policy)
- Research gap: no hotel-level FINN franchise expiry schedule
- Committed/closed: HI Monterrey Valle franchise renewed; Wyndham GDL→Courtyard; Puebla/Monterrey Valle HMA internalizations; FIHO Villahermosa + Cd Carmen leases
- Emerging lease (unnamed counterparty): Yucatán Resort
- Watchlist/context: Santander hotel unconfirmed; Cabo Rojo destination ≠ hotel HMA
- No auto-Verified conversions — Track A owns classification (`TRACK_A_HANDOFF.json`)

## 9. Track C property identity handoff
- Courtyard GDL alias for Wyndham Garden Acueducto
- JW Marriott Monterrey Arboleda vs Valle naming
- No HPC for Santander until hotel affirmed; Cabo Rojo destination parent only (`TRACK_C_HANDOFF.json`)

## 10. Integration Controller handoff
- Same branch as PR #556; preserve dirty `tmp/*` including `tmp/ic-fold-519-audit.mjs`
- Do not merge from Track B chat
