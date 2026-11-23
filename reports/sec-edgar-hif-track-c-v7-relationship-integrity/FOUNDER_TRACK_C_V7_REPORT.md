# HIF Track C V7 — Canonical Hotel Identity Integrity & DealSignal Relationship Repair

**As of:** 2026-11-23  
**Authorization:** Founder GO — controlled corrections (audit + mapping only)  
**PR #557:** MERGED (`06efe1eb7`) — identity matching **not** repeated  
**Airtable writes this wave:** **0**  
**CUSTOMER_BLOCKED** · No Surfe · No paid research · No customer publication · No weekly automation · No production deploy

---

## 1. Exact finding-ID mismatch causes

| Hotel | Claimed finding ID | Live Finding row? | In Track A corpus? | Cause |
|-------|-------------------|-------------------|--------------------|-------|
| Courtyard Playa del Carmen | `find_ds_cala_v1_finn_courtyard_pdc_committed_2026` | **Yes** `recVfM6WjPHIIKp3V` | Yes | None — linked to inventory `rec7o2TuInz6WdFAV` → HPC `rechcqksn3VfFD3c6` |
| Yucatán Resort Playa del Carmen | `find_ds_cala_v1_fiho_yucatan_resort_lease_2026` | **Yes** `rec3YUdLkWthuLmrM` | Yes | None — linked to inventory `recKfUmMDG5JJDUpP` → HPC `recY9Cxxo1iAbJgOt` |
| Holiday Inn Monterrey Valle | `find_ds_cala_v1_finn_hi_monterrey_valle_2026` | **No** | **No** | **`TRACK_C_SYNTHETIC_FINDING_ID_NEVER_IN_TRACK_A_CORPUS`** |
| Holiday Inn Puebla La Noria | `find_ds_cala_v1_finn_hi_puebla_la_noria_2026` | **No** | **No** | **`TRACK_C_SYNTHETIC_FINDING_ID_NEVER_IN_TRACK_A_CORPUS`** |

### Root cause (Monterrey / Puebla)

Not stale Airtable filtering and not a broken link to an existing finding.

Track A `RESEARCH_CANDIDATES.json` / Apply ledgers never emitted hotel-level findings for Monterrey Valle or Puebla La Noria. Those assets appear only as **claims inside** the parent FINN internalization finding (`find_ds_cala_v1_finn_internalization_franchise_2026` — franchise renewal / non-renewal evidence).

Track C’s identity script invented:

- `disc_ds_cala_v1_finn_hi_monterrey_valle_2026` / `find_ds_cala_v1_finn_hi_monterrey_valle_2026`
- `disc_ds_cala_v1_finn_hi_puebla_la_noria_2026` / `find_ds_cala_v1_finn_hi_puebla_la_noria_2026`

Inventory + HPC MATCHED links are valid. The synthetic **Finding IDs were never persisted** as DealSignal Finding rows — so DealSignal cards cannot resolve them as corpus findings.

**Classification:** source-version / handoff-scope gap (Track C identity expansion beyond Track A finding grain), **not** missing HPC.

---

## 2. Proposed relationship corrections (minimum, non-destructive)

| ID | Action | Live write? |
|----|--------|-------------|
| **CORR_1** | Map synthetic Monterrey/Puebla finding IDs → parent `find_ds_cala_v1_finn_internalization_franchise_2026` for DealSignal evidence navigation; keep `disc_*` → HPC for hotel identity | **No** (code map) |
| **CORR_2** | Courtyard / Yucatán finding↔inventory links already correct — **no patch** | No |
| **CORR_3** | Document Courtyard/Wyndham stewardship alias (below) — **no merge** | No |
| **Do not** | Create duplicate Finding rows solely to repair navigation | — |

Resolver shipped: `lib/hotel-intelligence/sec-edgar/hif-track-c-v7-dealsignal-relationship-map-v1.js`  
(`resolveDealSignalFindingId` · `createFinding: false` always for these IDs)

---

## 3. Courtyard / Wyndham canonical stewardship recommendation

| Role | HPC | Brand |
|------|-----|-------|
| **Canonical current** | `rechcqksn3VfFD3c6` | Courtyard by Marriott |
| **Historical sibling** | `recf65FZ1FuVVqIrz` | Wyndham Garden |

- Same physical Playacar property (Avenida Aviación Lote 5–6 / Manzana 29).
- Continuity: brand conversion (Wyndham Garden → Courtyard); Fibra Inn owner continuity.
- Dependencies: DealSignal inventory points at Courtyard HPC; Wyndham row remains census/history.
- **Safest relationship:** stewarded **SAME_PHYSICAL_PROPERTY_BRAND_CONVERSION** alias — prefer Courtyard for HE deep-links; preserve Wyndham as historical affiliation.
- **Forbidden without separate founder approval:** merge, delete, or overwrite either HPC row.

---

## 4. Remaining identity uncertainties

| Entity | Status | Evidence still required |
|--------|--------|-------------------------|
| Santander Bucaramanga | UNRESOLVED | Official SECOP II pliego confirming **hotel** use |
| Cabo Rojo | PARENT_ONLY | Named individual hotels (not destination-as-hotel) |
| Fibra Inn internalization | PARENT_ONLY | Keep portfolio separate from hotel-level facts |
| Yucatán operator | GAP | FIHO disclosure naming **current** lease counterparty (do not assume Playa after Apr 2026 suspension) |

---

## 5. Hotel Explorer navigation test results

| HPC | Research API | Result |
|-----|--------------|--------|
| `rechcqksn3VfFD3c6` Courtyard | `GET /api/hotel-intelligence/hotels/{id}/research` | **200** `ok:true` · `hotel_id` match |
| `recY9Cxxo1iAbJgOt` Yucatán | same | **200** `ok:true` |
| `recJRNuLr8fEiGImt` HI Monterrey Valle | same | **200** `ok:true` |
| `recfpT5RJBZAORXs2` HI Puebla La Noria | same | **200** `ok:true` |
| `recf65FZ1FuVVqIrz` Wyndham (sibling) | same | **200** `ok:true` (historical row still resolvable) |

Deep-link target pattern (unchanged):  
`http://localhost:8080/app#/opportunity-radar` + open HE by HPC ID.

**Note:** Research payload `hotel_name` may be null even when `hotel_id` resolves — HE naming enrichment is separate from identity integrity. Authenticated Radar UI click-through not required to prove API resolvability; full authenticated-app PASS remains HE-owned if UI polish is needed.

Inventory→HPC MATCHED: **4/4** hotels (read-back audit).

---

## 6. Deduplication & historical-data preservation

- **No new HPC creates** this wave.
- PR #557 matching preserved (4 matches, 0 creates).
- Courtyard + Wyndham both retained (no merge/delete).
- Track A Finding IDs / source URLs / evidence snippets untouched.
- Synthetic finding IDs retained only as **notes/aliases** in the relationship map — not promoted to new Finding rows.
- Idempotent unit suite: `npm run test:hif-track-c-v7-relationship-integrity` (0 Airtable writes).

---

## 7. Requested Airtable mutations (for approval) — **NONE this wave**

Audit found **0** unlinked corpus findings requiring PATCH.

Optional **future** (separate approval only):

| Mutation | Purpose | Status |
|----------|---------|--------|
| *(none required for V7 navigation repair)* | — | — |
| Optional later: Track A–authorized hotel-level findings for Monterrey/Puebla | True DealSignal grain | **Not requested** |
| Optional later: steward merge Courtyard←Wyndham | Deduplicate HPC | **Blocked — needs founder approval** |

---

## 8. PR and CI status

- Mapping + audit + dry-run test packaged for one focused PR.
- Integration Controller owns merge.
- No Airtable re-apply of previously verified records.
- Dirty product worktrees / Exa cache preserved on prior branch.

**STOP for founder review** before any destructive identity change, expanded live writes, HPC merge, or publication.

---

## Artifacts

- `scripts/hif-track-c-v7-relationship-integrity-audit.mjs` (read-only)
- `reports/sec-edgar-hif-track-c-v7-relationship-integrity/RELATIONSHIP_INTEGRITY_AUDIT.json`
- `lib/hotel-intelligence/sec-edgar/hif-track-c-v7-dealsignal-relationship-map-v1.js`
- `scripts/test-hif-track-c-v7-relationship-integrity-v1.mjs`
