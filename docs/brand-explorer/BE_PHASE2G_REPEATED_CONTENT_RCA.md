# Phase 2G — Root cause: repeated / padded content

## Founder finding

Phase 2F candidates still showed repeated negotiation/economics paragraphs, generic Proof N titles, and internal generator language in customer-facing bodies.

## Root causes

1. **Metric-stamp generator (Phase 2E/2F)**  
   A single FDD / diligence paragraph was reused across `economics.*` slots, then lightly prefixed with the slot key (`Brand · economics.cash.ramp: …`). Prefixes changed the string enough to dodge weak uniqueness checks while remaining human-identical.

2. **Min-word padding suffixes**  
   Phase 2F near-duplicate repair appended phrases like “Focus this card on {leaf} timing and documentation only.” That maximized visual-gate pass rates without adding owner intelligence.

3. **Evidence≠presentation coupling**  
   Research disposition strings (`RESEARCH_REQUIRED`, “Evidence-grounded reconstruction”, bracketed `[slotKey]` tokens) leaked into Body/Title instead of staying in internal ledgers.

4. **Score gaming**  
   Visual A/B/C treated populated text as quality. Padding increased pass rate while parity vs production Curio declined.

## Corrective design (Phase 2G)

| Control | Mechanism |
|---|---|
| Forbidden customer language | `customerFacingForbiddenPatterns` in subsection parity contract |
| Anti-padding gate | Exact repeated sentences + ≥0.82 body similarity fail closed |
| Sanitize pipeline | Strip meta language; clear near-duplicates → research gap (no re-pad) |
| Comparison UX | Paired subsection rows (not dual full-page scroll) so padding/repetition is visible opposite production |

## Rule

If evidence is insufficient, **stop generation for that field** and record a research gap. Never satisfy min-words by repetition.
