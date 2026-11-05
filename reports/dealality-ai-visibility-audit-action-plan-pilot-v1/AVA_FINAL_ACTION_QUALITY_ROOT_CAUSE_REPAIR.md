# Final Action Quality — Root Cause & Repair

Generated: 2026-11-05T21:23:25.528Z

## Defect A — Title-only steps (Waterstone domain)

**Root cause:** Canonical register read `agencySteps` while payloads store `recommendedSteps`.

**Repair:** `extractImplementationSteps` reads `recommendedSteps` (+ completionCriteria fallback) and rejects title restatements.

## Defect B — AC title/step mismatch

**Root cause:** Loose `meeting_space` keyword theme forced meeting title onto lounge action group.

**Repair:** Semantic themes (`meeting_space_reconciliation` vs `positioning_content`); lounge/renovation stay separate; meeting merge only for capacity/fact conflicts.

## Defect C — Castillo duplicate spa actions

**Root cause:** Theme detection missed spa-size equivalence across different titles.

**Repair:** `spa_size_reconciliation` merges CSV-001 + CSV-DCR-001 into **Reconcile and Correct Spa Size Information**.

## Defect D — AC repetitive CVB mentions

**Root cause:** DA renderer emitted one card per source row.

**Repair:** `digital-authority-customer-v1` consolidates by host (acorunacb.com → 1 workstream, 4 evidence URLs).

## Defect E — Historical backlink destinations

**Root cause:** Any hyperlink treated as healthy maintain-as-is.

**Repair:** Destination classifier (`HISTORICAL_OR_LEGACY_DOMAIN` vs current brand/hotel); update only if broken/misleading.

## Defect F — Near-empty final page

**Root cause:** Methodology forced onto its own page after dense roadmap cards.

**Repair:** Compact methodology + table roadmap; `page-break-before:avoid`.

## Defect G — Formulaic language

**Repair:** Host-specific DA rationale; property-specific steps from payload `recommendedSteps`.
