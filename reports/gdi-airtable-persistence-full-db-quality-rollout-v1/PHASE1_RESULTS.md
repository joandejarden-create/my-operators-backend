# Phase 1 results — write/read-back certification

**Classification:** `PERSISTENCE_VALIDATED` (Sheraton full 79-opportunity inventory)  
**Not yet:** cross-hotel database-wide / `FULL_E2E_VALIDATED`

## Contract change shipped

Slice 2A slim payload extended (no new Airtable schema) to retain:

- `commercialFactCatalog`
- `commercialSourceRegister`
- compact `sectionCitations`

Fail-closed 95k unchanged. Publication keys stripped.

## Live Airtable evidence (base `appa2cE7FTRmIbB32`)

Research inventory table already held **43** Sheraton rows before this batch.

### Batch A — first 3 (ordered inventory head)

| Opportunity | Action | Airtable record | Read-back |
|-------------|--------|-----------------|-----------|
| `disc_93695f1fcafa` — 2026 Strategic Public-Works Package | UPDATED | `recgEQwqsJG4q2bHS` | OK — catalog+register present; 2 sources |
| `disc_2eb37f395de2` — 69.ª Cumbre Mexicana… | UPDATED | `recPFhhF3HTLUOfgc` | OK — catalog+register present; 2 sources |
| `paid3_b67ba22a9816` — 85º Congreso Nacional… | UPDATED | `recP98qzZp2qha5au` | OK — catalog+register present; 13 sources |

### Batch B — founder regression IDs

| Opportunity | Action | Airtable record | Read-back |
|-------------|--------|-----------------|-----------|
| `paid3_565ce4588e6d` — **EUROGUSS MEXICO 2027** | (see APPLY cert) | recorded in `SHERATON_APPLY_CERT.json` | OK |
| `cmi_sheraton_amhpac_rsa_20270303` — AMHPAC RSA 2027 | (see APPLY cert) | recorded in `SHERATON_APPLY_CERT.json` | OK |

Artifacts: `SHERATON_DRY_RUN_CERT.json`, `SHERATON_APPLY_CERT.json`.

## Honest gaps (do not over-claim)

1. **Commercial fact rows often empty (0 facts)** on these opportunities — source register + brief fields persist; Level 1/2 fact promotion needs richer `establishedFacts`/claim extraction before claiming complete fact-catalog density.
2. **GDO table count for hotel = 0** in this environment listing — research inventory is the certified write path for this milestone; GDO sync is a later batch step with sales/publication ownership preserved.
3. **Public contact recovery / Surfe** intentionally off during persistence certification (no paid spend). Contact hierarchy code is in #462; channels persist when already on the opportunity DTO.
4. **79-opportunity union** reconciled by canonical id — only a controlled sample written so far.

## Full Sheraton inventory apply (Phase 2 reference hotel)

| Metric | Value |
|--------|-------|
| Union opportunities | **79** |
| Slim OK / FAIL | 79 / 0 |
| Wrote + read-back OK | **79 / 79** |
| Read-back FAIL | 0 |
| Publication leaks | 0 |
| Batch certified | **YES** |

See latest `SHERATON_APPLY_CERT.json` for per-opportunity Airtable record IDs and actions (CREATE/UPDATE/UNCHANGED).

EUROGUSS MEXICO 2027 (`paid3_565ce4588e6d`) → research inventory `recqjHnN2XV56rvmZ` (read-back OK; source register retained).

## Next

1. **Integration Controller merges #462** (GDI chat does not merge) + post-merge sync `ws/gdi`.  
2. Improve fact extraction so `commercialFactCatalog.facts[]` is populated where verified evidence exists.  
3. Phase 3 — canonical inventory of **all** GDI hotels/opportunities, then restartable batches.  
4. Do not start Phase 8 Full E2E roadmap until Phase 7 database certification.
