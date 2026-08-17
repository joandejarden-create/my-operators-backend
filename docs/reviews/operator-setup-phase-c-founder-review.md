# Operator Setup Phase C — Founder Review

**Mode:** apply (controlled batches 1–3)

## Purpose

Roll out researched-summary writers so Setup reflects OE intelligence Dealality already has — without inventing data, filling obsolete fields, or changing Fit.

## Results snapshot

| Metric | Value |
| ------ | ----- |
| Writers audited | 12 |
| READY writers | 5 (+ scaffold) |
| OE adapters implemented | 2 (Brand Relationships, Operating Platform) |
| Writers not run (needs data / legacy) | 5 |
| Proposed NEW VALUE (final plan) | 264 (after schema filter) |
| Cumulative writes | **~314** (partial batch-1 50 + completed run 264) |
| No-ops | ~937 |
| Holds / DO NOT WRITE | ~117 (conflicts + unknown pack fields + sparse coverage) |
| Existing-value overwrites | **0** |
| Failures (final run) | **0** |
| OM fill | **28 → 35 / 36** |
| MA fill | **28 → 33 / 36** |
| RESEARCHED SUMMARY completeness | **36.9% → 77.2%** |
| Overall meaningful | **63.8% → 77.2%** |
| Fit Data Ready shadow | **4 → 4** |
| Golden regression | **PASS** (Arbor OP still 22) |
| OE regression | **PASS** (Assignments/Presence/Intel BR/Claims untouched) |

First apply attempt stopped mid–Batch 1 on unknown pack field names (`totalProperties`, etc.). Schema filtering + retry completed Batches 1–3 cleanly.

## Integrity / OM–MA eight-record result

| Outcome | Operators |
| ------- | --------- |
| SAFE WRITE OM+MA | Remington, Brittain, OxoHotel, Grupo Presidente, Grupo Marta |
| SAFE WRITE OM only (MA unknown) | Royalton, Arriva (Owner-Operator evidence; MA not inferred) |
| REMAIN UNKNOWN (hold) | Tafer (Coral Beach hold) |

## Country taxonomy

**No options added.** Proposed founder additions from live evidence: Barbados, Cayman Islands, Ecuador, France, Guatemala, Oman, Singapore, Turkey, UAE, United States Virgin Islands. Strategic Interest never used as Active Country.

## Numeric fields

All `locationType*` / `*Experience` portfolio-% numbers **held blank** — Assignments `Urban / Resort` empty; CALA sample ≠ global mix.

## Batches

| Batch | Content | Written |
| ----- | ------- | ------: |
| 1 | OM/MA + pack blank-fill (schema-valid fields only) | 16 (+ prior 50) |
| 2 | OE Brand Relationships section creates | 132 |
| 3 | OE Operating Platform thin capability creates | 116 |

## Are the Operator Setup tables now properly populated?

**Yes for the intended architecture — not for raw fill %.**

- **Populated:** Master identity + OM/MA where evidenced; Profile/Platform researched narratives from approved packs (blank-fill); Active Countries (Phase B); thin OE-backed Brand Relationship and Operating Platform section rows for Production operators that lacked section depth.
- **Intentionally blank:** Fit `bf_*`; workflow/Diligence QA; portfolio % numbers; Case Studies (prefer Assignments); Engagement/Infra/Leadership deep narratives.
- **Genuinely unknown:** Tafer OM/MA (hold); MA for Owner-Operator-only operators; leadership people.
- **Deprecate later:** Case Studies as evidence SoT; Setup BR naming vs Intel BR.

## Recommended next path

**Path A — Fit Adapter Remap + Operator Fit v2.1 Shadow**

Setup side of the bridge is mature enough; Fit Data Ready = 4 is primarily methodology/mapping, not missing OE research.

## Approvals needed

1. Country taxonomy additions (list above) — or keep filtered intersection
2. Remaining OM/MA unknowns stay blank
3. Fit adapter remap to prefer OE intel
4. Physical deprecation still withheld
5. Optional later: Engagement/Infra/Leadership researched-summary depth

## Confirmations

- Normalized OE evidence tables **not** overwritten
- No Operator Fit / scoring changes
- Owner pilot remains disabled
- My Deals remains unwired
- Webhound Track 2 **not** merged
