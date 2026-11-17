# Defect and Risk Register

| ID | Severity | Area | Evidence | Current | Expected | Root cause | Confidence | Impact | Recommendation | Acceptance |
|----|----------|------|----------|---------|----------|------------|------------|--------|----------------|------------|
| FND-001 | P1 | Orchestration | Multiple Full E2E modules + PARTIAL_EXECUTOR registry | Campaign scripts + partial shared path | One instrumented canonical path | Historical accretion | CONFIRMED | Inconsistent hotels | Wire campaigns through shared chain + ledger | Single entry certifies G0–G1 |
| FND-002 | P1 | Observability | hist_* ledgers UNKNOWN segments | UNKNOWN kept | Live runs emit EXECUTED/SKIP/FAIL | Historical reconciliation only | CONFIRMED | Cannot prove 14-segment execution | Mandatory ledger hooks on discover | New paid run shows non-UNKNOWN |
| FND-003 | P1 | Apify | provider inventory + apify-fulle2e mock default; YOTEL ledger no Apify | Configured, not live | SKIP ledgered or live when authorized | Auth fail-closed default | CONFIRMED | Blind spot on scrape providers | Explicit SKIP_APIFY_UNAUTH in ledger | Ledger shows Apify state |
| FND-004 | P2 | Discovery | YOTEL segment mix; missing relocation/celebrations | Event-heavy | Balanced forward-looking | Exa/event bias LIKELY | LIKELY | Miss early demand | Segment-true missions + coverage KPIs | Non-event share ↑ |
| FND-005 | P2 | Drawer/quality | Sample opp next-step vs generic fit | Mixed | Hotel-specific + unknowns | Prompt/render variance | LIKELY | DOS usability | Rubric gate pre-stage | Rubric ≥ threshold |
| FND-006 | P2 | Contacts | ICPIC contact edition lag in prose | Stale role risk | Currency check | Research lag | CONFIRMED sample | Bad outreach | Role/edition validator | Gate fails on lag |
| FND-007 | P1 | Weekly Change | UI vs CHANGE_CLASS mismatch; empty weeklyDelta on YOTEL | Filters weak | Unified taxonomy + fields | Dual contracts | CONFIRMED | False NEW / unusable filter | Unify + emit deltas | UI←backend map tested |
| FND-008 | P1 | Persistence SoT | Packages vs Airtable confusion | Empty AT views confuse | Documented SoT + read-back | Architecture | LIKELY | Ops distrust | Runbook + optional reconcile | Founder trust restored |
| FND-009 | P2 | Parity | YOTEL fixed; others less gated | Drift possible | Gate every hotel | Uneven adoption | LIKELY | Client mismatch | Reuse YOTEL gate | Parity test green |
| FND-010 | P3 | Identity | Multiple ID schemes | Fragile weekly | Stable identity join | Accretion | LIKELY | Dupes | Identity contract | Dedup tests pass |
| FND-011 | P3 | Multi-hotel | Rediscovery cost | Paid repeat | Shared evidence objects | No shared layer | LIKELY | Cost | Incremental evidence share | Cost/opp ↓ |
| FND-012 | P0 | Confidentiality | Cross-hotel feedback leak | Not demonstrated | Isolation | — | NOT INSPECTED this pass | — | Explicit red-team API audit later | No cross-hotel private fields |

No P0 data-loss or unauthorized publication confirmed in this read-only pass on CURRENT packages.
