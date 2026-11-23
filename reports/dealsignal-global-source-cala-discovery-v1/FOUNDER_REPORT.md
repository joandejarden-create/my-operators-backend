# DealSignal — Global Source Expansion & CALA-First Discovery V1
## Founder Report (A–M)

**As of:** 2026-11-23  
**Workspace:** `C:\Dev\dealality\ws-hif-v6-track-a` · `feat/hif-v6-track-a-cb5-trigger-first-portfolio` @ `e8007e006`  
**Publication:** CUSTOMER_BLOCKED  
**Paid Exa:** **$0**  
**Airtable:** `PERSISTENCE_VALIDATED` (not FULL_E2E / not CUSTOMER_PUBLISHED)

---

## A. Source inventory: existing, new, tested, unavailable

### Existing (reused — not rebuilt)
- US SEC EDGAR + CMBS extractors (`EXISTING_INTEGRATED`)
- Track A source families / EX-10 / owner portfolios
- Owner IR/earnings tooling path
- Track C CALA geography helpers
- Prior DealSignal waves (Enrichment → OPEW V2), including FIHO Mexico operating-model parent

### New registry (`dealsignal-source-registry-v1.js`) — 13 entries
Includes Mexico BMV/FIBRA IR, Compras MX, RNT, SIGER/RPC, DGCP, Pro-Pedernales, SECOP II, Brazil CVM, FOHB/HotelInvest context.

| Readiness | Count |
|---|---:|
| EXISTING_INTEGRATED | 2 |
| ACCESSIBLE_VIA_CURRENT_TOOLING | 1 |
| MANUALLY_SEARCHABLE | 7 |
| CANDIDATE_REQUIRES_TECH_INTEGRATION | 2 |
| RESTRICTED_OR_UNAVAILABLE | 1 (SIGER — fees/access) |

**Tested productive this wave:** BMV/FIBRA IR, FINN IR, Pro-Pedernales, owner IR pattern, SEC (prior).

---

## B. CALA source coverage by country

| Band | Countries touched | Sources tested | Yield |
|---|---|---|---|
| A Mexico | Mexico | BMV, FINN IR, Compras MX, FIHO | **Highest** — FINN internalization + franchise conversions; FIHO lease shifts |
| B Caribbean | Dominican Republic | DGCP, Pro-Pedernales | Medium — Cabo Rojo COMMITTED destination partner; DGCP lodging tenders = false positives |
| C Central America | — | Not deep-tested | Deferred |
| D South America | Colombia, Brazil | SECOP II, CVM/FII, FOHB | Partial — Santander watch (hotel TBD); Brazil context watch only |

---

## C. Actual Mexico findings

1. **Fibra Inn internalization (28 hotels, Jan 15 2026)** — Aimbridge HMA buyout Ps.172.3m. Policy: remaining third-party HMAs → internal ops at expiry. **MCT path largely closed; franchise/BCT path remains.**  
2. **Holiday Inn Puebla La Noria** — non-renewal notice → internal ops (closed pattern).  
3. **Holiday Inn Monterrey Valle** — franchise renewed after prior expiry; ops internalized.  
4. **Wyndham Garden Playa del Carmen → Courtyard by Marriott** — COMMITTED (ops 5 Oct 2026).  
5. **FibraHotel Yucatán Resort Playa del Carmen** — closed 30 Apr 2026 for **lease portfolio** inclusion — Emerging lease/brand engagement.  
6. **Compras MX / GAFSACOMM** — PMS and Mundo Maya ops supervision = **adjacent**, not classic BCT.  
7. **Chautla ex-hacienda** — 20yr concession already awarded — closed false-open control.

---

## D. Additional Caribbean & Latin American findings

- **DR Cabo Rojo / Pedernales:** Consorcio Cabo Rojo (Grupo Puntacana lead) COMMITTED as destination partner — residual future hotel brand pipeline only.  
- **Colombia SECOP II:** Santander 20yr concesión (close ~26 Nov 2026) — **Watchlist** until hotel identity confirmed.  
- **Brazil:** FII/lease vs management market context — Watchlist; no hotel-specific open BCT.

---

## E. New verified / emerging / early-engagement

| Class | Count | Notes |
|---|---:|---|
| Verified open BCT/MCT | **0** | No first-100 inflation |
| Emerging | **2–3** | FINN franchise-flexibility parent; FIHO Yucatán lease |
| Early-engagement theses | **3** | FINN brands; FIHO lease; Cabo Rojo residual relationship |
| Watchlist | **2** | Santander TBD; Brazil FII context |
| Committed/Closed | **4** | Courtyard PDC; Cabo Rojo partner; Chautla; FINN 28-hotel internalization |

Prior qualified + OPEW records **preserved**.

---

## F. Owner and decision-maker intelligence

| Owner | Insight | People |
|---|---|---|
| Fibra Inn (FINN13) | Internal ops via Servicios Hoteleros FINN; franchise-led brand strategy | Technical Committee (named individuals not fully verified this wave) |
| FibraHotel (FIHO12) | Lease-model expansion | Administrator / IR |
| Consorcio Cabo Rojo | Destination ops | **Frank Elías Rainieri** (Grupo Puntacana) |
| Departamento de Santander | Possible hotel concesión | Entity-level only |

No Surfe. No invented contacts.

---

## G. Original source URLs and claim-linked evidence

Claim-linked sources live in `RESEARCH_CANDIDATES.json` `sources[]` (URL, publisher, date, language, claim, role). Examples:

- https://fibrainn.mx/storage/docs/fibra-inn-finaliza-proceso-de-internalizacion-de-la-operacion-en-28-hoteles.pdf  
- https://fibrainn.mx/assets/docs/2025-corporate-governance-letters-updated_69d5ae218a163.pdf  
- https://www.bmv.com.mx/docs-pub/eventfid/eventfid_1593193_1616_1.pdf  
- https://bmv.com.mx/docs-pub/eventfid/eventfid_1574632_1596_1.pdf  
- https://propedernales.do/...cabo-rojo...  

Syndicated lodging awards and software PMSs logged as **false positives**, not corroboration.

---

## H. Research productivity by source family

| Family | Productivity |
|---|---|
| Mexican FIBRA / BMV disclosures | **Highest** |
| Owner IR (global + MX) | High |
| Destination / PPP awards (DR) | Medium (committed + residual) |
| Procurement portals (Compras MX, DGCP, SECOP) | Medium with **high false-positive rate** |
| Brazil CVM / industry panorama | Low (context) |
| SIGER / RNT | Untested / restricted |

Multilingual lexicon (ES/PT/EN) shipped in `dealsignal-multilingual-query-lexicon-v1.js`.

---

## I. Airtable persistence and idempotency

| Step | Result |
|---|---|
| Dry-run | 12 creates planned |
| Apply | **12 creates** (6 inventory + 6 findings) |
| Idempotent | **12 NOOP** |
| first-100 | **0** |
| Schema | None |

Key IDs: FINN `rec246OEIvYAfRJFD` / `recp0bShKmxpOFQ2N` · Yucatán `recKfUmMDG5JJDUpP` / `rec3YUdLkWthuLmrM` · Cabo Rojo `recHxLD2MNFyj3U1d` / `recgql2bLPxnck3SJ`

---

## J. Track B contractual handoff

See `TRACK_B_HANDOFF.md` — FINN franchise schedules, residual HMAs, FIHO leases, Santander pliego verification.

---

## K. Track C canonical-property handoff

See `TRACK_C_HANDOFF.md` — Courtyard PDC alias, Yucatán Resort, no portfolio HPC for Cabo Rojo, no HPC for unverified Santander asset.

---

## L. Integration Controller handoff

See `IC_HANDOFF.md`. Reusable modules + test + persistence ledgers. No full automation. Status: `AWAITING_INTEGRATION_CONTROLLER`.

---

## M. Recommended source additions for eventual automation

1. Scheduled BMV FIBRA eventfid crawler (Mexico-first).  
2. FINN/FIHO IR PDF watcher for conversión / internalización / arrendamiento keywords.  
3. SECOP II / Compras MX adapters with **hotel-object classifiers** (suppress lodging-service false positives).  
4. DGCP filter: exclude government travel lodging CPV codes.  
5. Property-level franchise schedule extraction for Mexican FIBRAs (Track B).  
6. Paid Exa only with separate founder budget when schedules missing.

---

## Safety

Remained on Track A branch; no worktree/branch switch/reset/clean/stash/force-push. Prior DealSignal uncommitted research preserved. CUSTOMER_BLOCKED. No publish. No Surfe. No paid Exa.
