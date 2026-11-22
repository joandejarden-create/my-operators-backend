# Track C Wave 3 — Accelerated Census Scale

**Publication:** CUSTOMER_BLOCKED · **Research cost:** $0 · **Branch:** `feat/hif-v6-track-c-wave3-census-scale`

## 1. Live HPC before / after

| Country | Before | After |
|---------|-------:|------:|
| United States | 53 | **89** |
| Mexico | 3772 | 3772 |
| Brazil | 6179 | 6179 |
| United Kingdom | 3 | **5** |
| Canada | 5 | **8** |

## 2–3. US discovery / persistence

- **C-U evaluated distinct US identities:** **603** (target ≥500 — met)
- **US HPC net:** +36 canonical creates (41 total HPC creates including UK/CA; 1 collision-blocked)
- Gates unchanged: address + official brand URL + collision-clear

## 4. Discovery by brand family (C-U)

See `WAVE3_C_U_EVALUATION.json` → `byBrandFamily` (UNKNOWN dominates staged inventory lacking brand; Marriott/Hilton/IHG present in enriched SEC57).

## 5. By state / market

See `WAVE3_C_U_EVALUATION.json` → `byStateTop40`.

## 6. Management-company coverage

Aimbridge public portfolio seed evaluated (operator disclosed by Aimbridge — **never inferred from brand**). HEI / Remington / Hotel Equities / Highgate: next source batch.

## 7. SEC→HPC (C-R)

| Metric | Count |
|--------|------:|
| SEC57 US | 57 |
| Official-enriched this wave | 37 |
| MATCHED_EXISTING_HPC | 18 |
| CREATE_READY | 37 |
| RESEARCH_REQUIRED (historical) | 2 |
| Collision review blocked create | 1 (Hilton Garden Inn Daphne) |

Deferred: **Courtyard Silver Spring North**, **Courtyard Denver Stapleton**.

## 8. CREATE_READY attrition

**C-R:** primary residual = missing address/URL on non-enriched / historical cases.  
**C-U inventory pool:** dominant attrition = missing physical address + brand ambiguity on staged SEC name-only rows.  
**Founder recommendation (no policy change implemented):** accept official brand-directory property URLs as satisfying the official-property-URL gate (already done); do **not** loosen address requirement. Optional future: allow government lodging-register address when brand URL also present — examples available in Wave3 enrichment fixture.

## 9. UK / Canada

UK 3→5 · CA 5→8 via official Hilton/Marriott/IHG pages + Aimbridge UK seeds (inventory).

## 10. CALA

Mexico 3772 / Brazil 6179 unchanged — preserved.

## 11. Source yield / economics

Free official brand directories + Discovery Inventory + SEC CMBS + Aimbridge public + Tier B. **$0** paid research.

## 12–13. Airtable outcomes

- Discovery Inventory: creates 43 · no-ops 24 · conflicts 0  
- HPC authorized creates: **41** · failures 1 (collision review) · inventory↔HPC links enriched 59  
- Idempotency: find-before-create; re-run yields no-ops for existing discoveryPropertyId

## 14. Research cost

**$0**

## 15. Tests / branch / IC

- `node scripts/test-hif-track-c-universe-expansion-v1.mjs` PASS  
- Standing auth `maxHpcCreateBatch` **25 → 100** (Wave 3 founder directive)  
- Prior PR #545 MERGED; this branch is successor for IC merge  
- Integration Controller owns merge

## 16. Checkpoint

- Resume: `feat/hif-v6-track-c-wave3-census-scale`  
- Next batch: Hilton / IHG / Wyndham / Choice official US directories + collision queue on Discovery Inventory CREATE_CANDIDATE rows with addresses  
- Deferred historical: Silver Spring North, Denver Stapleton
