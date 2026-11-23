# IC Integration — Global Source Expansion + CALA-First Research
## Founder Report (items 1–11)

**As of:** 2026-11-23  
**Integration workspace:** `C:\Dev\dealality\wt-ic-dealsignal-source-expansion-v1`  
**Branch:** `feat/dealsignal-source-expansion-integration-v1` @ `125866476`  
**PR:** [#555](https://github.com/joandejarden-create/my-operators-backend/pull/555) — OPEN (CI in progress after gitleaks allowlist fix)  
**Publication:** `CUSTOMER_BLOCKED` (enforced on live reader)  
**Paid research:** $0 · **Surfe:** none · **New Airtable schema:** none · **Scheduler:** none

---

## 1. Integrated source registry and availability

Module: `lib/hotel-intelligence/dealsignal/dealsignal-source-registry-v1.js`  
Version: `dealsignal-source-registry-v1` · **13 sources** (extends Track A families; does not replace them)

| Readiness | Count |
|---|---:|
| EXISTING_INTEGRATED | 2 |
| ACCESSIBLE_VIA_CURRENT_TOOLING | 1 |
| MANUALLY_SEARCHABLE | 7 |
| CANDIDATE_REQUIRES_TECH_INTEGRATION | 2 |
| RESTRICTED_OR_UNAVAILABLE | 1 (SIGER) |

Tested productive this wave: **6** (BMV/FIBRA IR, FINN IR, Pro-Pedernales, owner-IR pattern, SEC/CMBS prior).

Intl readiness jurisdictions registered: GB, ES, PT, MX, DO, CALA, ME, APAC.

---

## 2. New source adapters / research capabilities

Reusable modules (no competing platform):

| Module | Role |
|---|---|
| `dealsignal-source-registry-v1.js` | Eligible official/credible source inventory + readiness |
| `dealsignal-source-selection-v1.js` | Family selection by commercial question |
| `dealsignal-cala-research-priority-v1.js` | Mexico-first / CALA queue tiers; urgent overrides |
| `dealsignal-multilingual-query-lexicon-v1.js` | EN / ES / PT research lexicon |
| `dealsignal-global-source-cala-discovery-v1-persist.mjs` | Idempotent Airtable persist (no re-CREATE) |
| Drawer V2 + reader wiring | Claim language/role, URL upgrade-dedupe, `researchQueueTier`, COMMITTED/CLOSED overlay |

---

## 3. CALA coverage and Mexico-first behavior

- Mexico preferred in research queue (`T2_HIGH_VALUE_CALA` for FINN; Mexico sources selected first in unit test).
- Other CALA countries remain prioritized; global opportunities stay eligible.
- Urgent deadlines override regional preference; geography does **not** change evidence confidence.
- Property location drives regional classification; portfolio parents keep hotel-level geography.
- **No permanent scheduler** in this phase — modules are callable from controlled research runs.

Unit gate: `npm run test:dealsignal-cala-research-priority-v1` → PASS  
(`sources:13`, `mexicoSelected` includes `mx_bmv_eventos`, `mx_fibra_issuer_ir`, `do_pro_pedernales`, `finnTier:T2_HIGH_VALUE_CALA`).

---

## 4. Track A / B / C handoff reconciliation

| Track | Status | Notes |
|---|---|---|
| **A** Source expansion | **Integrated in PR #555** | Registry, CALA priority, lexicon, selection, ledgers, claim mapping absorbed into DealSignal path |
| **B** Contractual / filings | **PR #549 OPEN** (CI green, MERGEABLE) — **not merged into #555** | Focused separate merge; IC must not overwrite newer HIF contracts |
| **C** Canonical identity | **PR #551 OPEN** (CI green, MERGEABLE) — **not merged into #555** | Shared geography helpers already reused; full canonical merge stays separate |
| Wave3 census | **PR #548 OPEN** | Out of scope for this integration PR |

Airtable persistence from Track A already applied earlier; IC persist path is **idempotent** (no duplicate CREATE of the 12-wave inventory).

---

## 5. Airtable and identity verification

Live reader (`GET /api/deal-opportunities`):

- `publicationState`: **CUSTOMER_BLOCKED**
- Opportunities: **338** (`qualified` 18 / `watch` 320)
- Status mix includes **Committed: 4** (overlay working — e.g. Courtyard Playa del Carmen, Cabo Rojo)
- No inappropriate promotion of Emerging → Verified
- Mexico rows present (**54** with MX/Mexico country)
- Owner context preserved on FINN / FIHO / AHIP / Bidwell samples
- Track C HE links remain pending where HPC identity not yet linked (explicit gaps in drawer)

Classification: **PERSISTENCE_VALIDATED** for source-expansion wave · **not** FULL_E2E_VALIDATED · **not** CUSTOMER_PUBLISHED

---

## 6. Source hyperlinks and claim-level citation coverage

Citation path verified: Discovery → Research candidates → Airtable → Reader → Drawer V2.

Browser-confirmed clickable originals:

| Opportunity | Links observed |
|---|---|
| Fibra Inn (Mexico Emerging) | 3× Fibrainn IR/PDF URLs |
| Cabo Rojo / Pedernales (DR) | propedernales.do + grupopuntacana.com.do (ES) |
| Courtyard Playa del Carmen | BMV PDF (Committed status in API) |
| AHIP MCT (US Verified/qualified) | GlobeNewswire settlement URL |
| Bidwell BCT (US) | SEC 10-K + Marriott property URL |
| Denver Courtyard (US owner) | Fool earnings transcript URL |

Claim mapping artifact: `reports/.../SOURCE_CLAIM_MAPPING.json` (TRIGGER / CORROBORATION vs CONTEXT roles).  
Related references are not treated as direct claim support.

---

## 7. DealSignal drawer / browser verification

Authenticated page: `http://localhost:8080/deal-opportunities.html` (IC WT server).

| Scenario | Result |
|---|---|
| Mexico opportunity (Fibra Inn) | PASS — Emerging, ES sources clickable |
| Caribbean/LatAm (Cabo Rojo) | PASS — Committed destination partner, ES sources |
| US owner/portfolio (Denver / Bidwell / AHIP) | PASS — sources + HE where linked |
| Verified BCT (Bidwell on Brand conversion) | PASS — SEC + brand URLs |
| Verified MCT (AHIP Aimbridge) | PASS — GlobeNewswire URL |
| Early-engagement thesis (FINN parent) | PASS — remains Emerging/watch (not Verified) |
| Sparse-data (gaps / HE pending) | PASS — “HPC identity not yet linked”, named exec required |
| Closed/Committed (Courtyard PDC API + Cabo Rojo) | PASS — status **Committed**, not Verified promotion |

CUSTOMER_BLOCKED retained on page chrome and API.

---

## 8. Tests and CI

Local:

- `npm run test:dealsignal-cala-research-priority-v1` — PASS  
- `npm run test:deal-opportunities-drawer-v2` — PASS  
- `npm run dealality:ci-precheck` — PASS (after `discoveryRecordId` allowlist)

PR #555 checks: re-running on `125866476` (gitleaks + ADP/GDI/Radar suites).

Suggested matrix scripts `test:batch1-cross-owner-access` / `test:deal-next-action` are **not present** on this checkout tip (stale suggest names); drawer-v2 + CALA priority used instead.

---

## 9. PRs and merge status

| PR | Title | Status |
|---|---|---|
| **#555** | feat(dealsignal): global source expansion + CALA-first research integration | **OPEN** · MERGEABLE · awaiting CI |
| #549 | Track B V3 Sotherly EX-10 / Mexico leases | OPEN · green · separate |
| #551 | Track C P0 + AHIP portfolio canonical | OPEN · green · separate |
| #548 | Track C wave3 census scale | OPEN · separate |
| #552 / #553 | Deal Opportunities + Drawer V2 | **MERGED** (prior) |

This IC cycle does **not** force-merge #555 until CI is green. Primary dirty checkout is **not** force-synced.

---

## 10. Outstanding source gaps

- SIGER / RPC restricted; RNT not deep-tested  
- Central America deferred  
- Brazil CVM / FII context-only  
- Procurement portals high false-positive rate (Compras MX, DGCP, SECOP)  
- Many CALA hotels still lack HPC / Hotel Explorer identity  
- Named decision-makers incomplete (no Surfe)  
- Track B contractual EX-10 and Track C canonical identity still on separate open PRs  
- Full Node/TS research orchestration **not** built this phase

---

## 11. Next steps for automated research orchestration

1. Merge #555 after CI green; run `dealality:post-merge-sync --stream adp` when Integration owns merge (preserve dirty primary).  
2. Merge #549 then #551 in sequence without overwriting newer HIF contracts.  
3. Add thin orchestrator that calls registry → selection → multilingual query → evidence retrieve → claim map → existing Airtable adapters (still no standing scheduler).  
4. Wire tested MANALLY_SEARCHABLE sources into adapters only after dry-run productivity gates.  
5. Track C identity backfill for FINN/Cabo Rojo portfolio children before customer promotion.  
6. Keep CUSTOMER_BLOCKED until founder authorizes customer publish.

---

## Completion state (this IC workstream)

`AWAITING_INTEGRATION_CONTROLLER` — PR #555 open; CI pending on latest commit; Track B/C remain separate; product homes not post-merge synced until merge.

**Airtable E2E classification:** `PERSISTENCE_VALIDATED`  
**Radar gate:** N/A (DealSignal page, not `/app#/opportunity-radar`)
