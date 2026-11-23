# DealSignal — Critical-Mass Expansion V3: Europe + CALA Discovery Wave
## Founder Report

**As of:** 2026-11-23  
**Workspace:** `C:\Dev\dealality\ws-hif-v6-track-a` · Track A  
**Publication:** CUSTOMER_BLOCKED · Paid research **$0**  
**Overall readiness:** **`BELOW_CRITICAL_MASS — EUROPE_YIELD_ADDED`** (ideal 20 not reached without relaxing standards)  
**Status:** `AWAITING_INTEGRATION_CONTROLLER`  
**Permanent rule:** `docs/data-intelligence/dealsignal-qualified-opportunity-classification-v1.md`

---

## Metrics requested (1–6)

| # | Metric | Result |
|---|---|---|
| 1 | **Starting Qualified count** | Founder-stated ≈ **7** · Permanent-rule confirmed open **2** (Voir Beach; Baha Mar fourth) |
| 2 | **New opportunities discovered** | **10** research candidates (Europe + CALA sweep) |
| 3 | **Newly verified Qualified** | **3** (Oromana; Águeda Passivhaus; Areia Branca REVIVE) |
| 4 | **Total Qualified after this wave** | Permanent-rule open **5** (2 preserved + 3 new). Do **not** inflate founder ≈7 with Emerging/Watchlist. Ideal **20** not met — standards held. |
| 5 | **Europe / CALA breakdown** | See below |
| 6 | **Chat B contract research needs** | See section F |

### Europe / CALA breakdown (permanent-rule open Qualified after wave)

| Region | Qualified | IDs |
|---|---:|---|
| **Europe** | **3** | Hotel Oromana (ES); Águeda Passivhaus eco-hotel (PT); Areia Branca REVIVE (PT) |
| **CALA** | **2** | Voir Beach (BS, preserved); Baha Mar fourth (BS, preserved) |
| **US / other** | **0** new | Prior Emerging/Watchlist retained; not promoted |

### Newly verified Qualified (detail)

| # | Opportunity | Trigger | Pathways |
|---|---|---|---|
| 1 | **Hotel Oromana** — Alcalá de Guadaíra (ES) | Open municipal concession C-2026/032; bids to **27 Nov 2026**; 40-yr MCT | **Management: YES** · Brand: possible via bidder |
| 2 | **Águeda Passivhaus eco-hotel** (PT) | EEN BOPT20261105031 — hybrid lease/management operator search (valid → Nov 2027) | **Management: YES** · Brand: soft-brand/distribution |
| 3 | **Areia Branca REVIVE** — Lourinhã (PT) | Turismo de Portugal 50-yr hotelaria concession; bids to **17 Mar 2027** | **Management: YES** · Brand: via concessionaire |

### Explicitly NOT Qualified this wave (standards held)

| Item | Classification | Why |
|---|---|---|
| Aena Barajas / El Prat | COMMITTED_CLOSED | Awarded Barceló + Momentum–Hyatt (May 2026) |
| Grand Lucayan Hilton anchor | COMMITTED_CLOSED | Gov’t confirmed Hilton 550-key partnership (May 2026) — corrects CM_V2 Early |
| LPIA Nassau airport EOI | COMMITTED_CLOSED | EOI closed 25 Jun 2025 |
| Torre de Uriz (Navarra) | AMBIGUOUS_REVIEW | Bid window closed 19 Nov 2026; award not confirmed |
| Bergantín residual / Hilton talks | EMERGING_PROSPECTIVE | Conversations only; no named parcel process |
| Cap Cana Spain Inmobiliaria | EMERGING_PROSPECTIVE | LinkedIn signal; needs Chat B teaser |
| Lucayan casino operator | EARLY_ENGAGEMENT | Gaming adjacency ≠ hotel BCT Qualified |

**Preserved prior Qualified:** Voir Beach; Baha Mar fourth. **No duplicates** created for those IDs.

---

## A. Discovery methodology

Same Track A DealSignal workflow as CM V1/V2:

1. Baseline permanent-rule inventory (`BASELINE_QUALIFIED.json`)  
2. Europe public-procurement + EEN + REVIVE sweep; CALA press / prior-lead refresh  
3. Classify via `classifyQualifiedOpportunity` / `applyQualifiedClassificationGuard`  
4. Persist CUSTOMER_BLOCKED to HIF discovery inventory + findings (dry → apply → idempotent)  
5. Handoffs to Track B / Track C / IC / HE  

**Not done:** FINN/FIHO volume rehash · paid Exa · schema changes · customer publish · relaxing gates to hit 20.

---

## B. Starting inventory note

`BASELINE_QUALIFIED.json` listed **6** IDs under a looser “strict” harvest. After permanent-rule scrub:

- **Open Qualified:** Voir Beach, Baha Mar fourth (**2**)  
- **Not open Qualified:** Justo six (COMMITTED); RLJ / SVC Crowne (contractual / Emerging); Bergantín residual (Emerging)

Founder ≈**7** is treated as a drawer estimate that may include Emerging labeled as Qualified pending IC eligibility fix. **Wave scoring uses permanent-rule confirmed 2 → 5.**

---

## C. Persistence

| Step | Artifact |
|---|---|
| Dry-run | `PERSISTENCE_LEDGER_DRY_RUN.json` |
| Apply | `PERSISTENCE_LEDGER_APPLY.json` / `PERSISTENCE_LEDGER_FIRST_APPLY.json` |
| Idempotent | `PERSISTENCE_LEDGER_IDEMPOTENT.json` |
| Publication | **CUSTOMER_BLOCKED** only |

---

## D. Source productivity

| Source family | Yield |
|---|---|
| ES/PT public procurement (PLACSP / DR / JOUE / Navarra) | **High** — 2 Qualified + 1 Ambiguous + 1 Committed correction |
| Enterprise Europe Network | **High** — 1 Qualified (Águeda) |
| CALA trade / government press | **Medium** — Hilton Lucayan correction; Bergantín Emerging; casino Early |
| LinkedIn owner posts | **Low** — Cap Cana Emerging only (Chat B) |
| Cold FINN/FIHO IR | **Skipped** |

Paid research: **$0**.

---

## E. Europe / CALA narrative

- **Europe** delivered the only **new** Qualified this wave (3), all MCT/concession or hybrid operator searches with verified open windows.  
- **CALA** preserved Bahamas Qualified duo; corrected Grand Lucayan hotel brand to **Hilton COMMITTED**; retained Bergantín residual as Emerging; Cap Cana gated pending teaser.  
- Mexico FINN/FIHO volume: **0** rehash.

---

## F. Opportunities requiring additional Chat B contract research

1. **Águeda Passivhaus** — legal entity name, CapEx schedule, draft hybrid lease/HMA terms (EEN partner intro).  
2. **Hotel Oromana** — full PCAP/PPT brand-affiliation rules, subrogation economics, restricted-procedure shortlist mechanics.  
3. **Areia Branca REVIVE** — VORTAL pieces, mandatory CapEx, brand freedom vs Turismo de Portugal constraints.  
4. **Cap Cana Spain Inmobiliaria** — project teaser, named parcels, shortlist status (promote only if gates clear).  
5. **Track B FINN** — hotel-level HMA/franchise notice dates (consume, do not re-extract SEC stack).  
6. **Bergantín fourth / residual** — named parcel + developer SPE if Hilton talks harden into an open process.  
7. **Torre de Uriz** — award notice or re-tender after 19 Nov 2026 close.

---

## G. Readiness vs ideal 20

| Target | Status |
|---|---|
| Ideal ~20 Qualified | **Not met** |
| Standards relaxed? | **No** |
| Path to 20 | Additional Europe tender waves + Chat B upgrades of Cap Cana / residual master-plans **only when** BCT/MCT + unresolved + pathway are verified |

---

## H. IC ask

Merge Track A CM V3 research pack + permanent Qualified classifier (if not already on `main`) after CI. Do **not** publish CUSTOMER_BLOCKED records. Coordinate Airtable eligibility enums separately.

**Cycle status:** `AWAITING_INTEGRATION_CONTROLLER`
