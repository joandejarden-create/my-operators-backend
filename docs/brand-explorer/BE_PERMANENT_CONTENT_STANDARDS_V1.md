# Brand Explorer — Permanent Content, Formatting, Research & Collection Standards

**Contract:** `configs/brand-explorer-permanent-content-standards-contract-v1.json`  
**Validation matrix:** `reports/brand-explorer/permanent-standards/00-validation-matrix.json`  
**Loader:** `lib/brand-explorer/be-permanent-content-standards-v1.js`  
**Status:** Founder-required · Curio / Ascend **NOT APPROVED** · Gate 2 blocked until Gate 1 approved

These rules apply to **all current and future** Brand Explorer profiles. No Curio- or Ascend-specific exceptions.

---

## Acceptance sequence

| Gate | Purpose | Status |
|---|---|---|
| **1 — Renderer parity** | Identical saved payload renders equivalently in product BE and Comparison Studio | Awaiting founder review (see Phase 2H-A package) |
| **2 — Content reconstruction** | Universal factory recreates depth, collections, loyalty, imagery, competitors | **Blocked** until Gate 1 founder-approved |

Automated PASS ≠ founder approval.

---

## Validation matrix (summary)

| ID | Requirement | Blocks approval | When enforced |
|---|---|---|---|
| R1 | Title Case headings; no em dashes / NBSP / slot keys / boilerplate / near-dupes / padding | Yes | Gate 2 (+ sanitize) |
| R2 | Lifecycle **exactly 6** ordered stages; Opening path **exactly 5** ordered stages | Yes | Gate 2 + cardinality |
| R3 | Empty / placeholder / diligence-only sections fail; explicit per-component contracts | Yes | Gate 2 |
| R4 | Loyalty participants: **exactly 6** Key Benefits cards | Yes | Gate 2 + cardinality |
| R5 | Loyalty KPIs: sourced, dated; compact Not Disclosed | Yes | Gate 2 research |
| R6 | Loyalty tiers: actual program structure as cards/table | Yes | Gate 2 |
| R7 | Gallery: 6 distinct property-first images; browser load | Yes | Gate 2 |
| R8 | Similar Brands: one competitor per card | Yes | Gate 2 |
| R9 | Research categories must support population | Yes | Gate 2 |
| R10 | Rendered regression suite (not JSON-only) | Yes | Gate 2 |
| R11 | Gate 1 before Gate 2 | Yes | Now |
| R12 | BE worktree governance; no writes/population without founder auth | Yes | Always |

Full machine-readable rows: `reports/brand-explorer/permanent-standards/00-validation-matrix.json`.

---

## Fixed collections (canonical)

### Support Across the Lifecycle (EXACT 6)

1. Evaluation  
2. Conversion Design  
3. Pre-Opening  
4. Opening  
5. Ramp-Up  
6. Ongoing  

Slots: `valueOwners.lifecycle.1` … `.6`

### Opening & Conversion Path (EXACT 5)

1. Application & Feasibility  
2. Design & Standards  
3. Pre-Opening Planning  
4. Opening Support  
5. Stabilization  

Slots: `economics.opening.step.1` … `.5`

### Key Benefits & Program Strengths (CONDITIONAL EXACT 6)

When `conventionalLoyaltyProgram` — six distinct verified benefit cards.  
Independent / non-participation brands use canonical non-participation treatment (do not invent six benefits).

---

## Related contracts updated

- `configs/brand-explorer-collection-cardinality-contract.json` — loyaltyBenefits → **EXACT 6**
- `configs/brand-explorer-content-presentation-contract-v1.json` — loyalty.proof min/target **6**; editorial + gate blockers aligned

---

## Commands

```bash
npm run test:be-permanent-content-standards-v1
```

Gate 2 regeneration scripts must call `assertGate2Allowed()` and refuse until:

- `reports/brand-explorer/phase2h-a-renderer-parity/GATE1_FOUNDER_APPROVED.json` with `{ "gate": 1, "founderApproved": true }`, or  
- `BE_GATE1_FOUNDER_APPROVED=1`

---

## Immediate non-goals

- Do **not** regenerate Curio / Ascend content in this deliverable  
- Do **not** mass-populate brands  
- Do **not** write production Airtable  
- Do **not** treat this document as Gate 1 or Gate 2 founder approval  

**FOUNDER_APPROVED = false**
