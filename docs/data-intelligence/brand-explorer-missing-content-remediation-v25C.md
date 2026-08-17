# Brand Explorer Missing Content Remediation v25C

- Generated: 2026-07-09T13:40:26.947Z
- Mode: **dry-run**
- Brand: **Tribute Portfolio** (`recCvV0PuZOi8c3hC`)
- Airtable modified: **no**
- Images untouched: **yes**
- Company Validated untouched: **yes**

## Section Status
- **Openings / Examples / Properties**: `suppress_until_ready` · rows found 0, missing 1
- **Recent Momentum**: `suppress_until_ready` · rows found 0, missing 1
- **Portfolio Mix**: `source_evidence_required` · rows found 1, missing 0
- **Portfolio Context**: `frontend_mapping_required` · rows found 0, missing 1
- **Standard Detail / Where Available**: `not_safe_to_populate` · rows found 0, missing 1
- **Demand Scenario View**: `source_evidence_required` · rows found 1, missing 1
- **Loyalty Program**: `founder_review_required` · rows found 0, missing 4
- **Geographic Footprint**: `founder_review_required` · rows found 5, missing 0

## Suppress Today
- Hide Openings / Examples / Properties if no complete footprint.openings row exists (title + image + location + summary + URL).
- Hide Recent Momentum when there are zero dated source-backed footprint.momentum rows.
- Hide Portfolio Mix pills when <=1 unsupported row or when no source-backed mix evidence exists.
- Hide Standard Detail placeholder when standards.requirement table rows are absent or not externally safe.
- Hide Demand Scenario View when only one or zero commercial.demand rows exist.
- Hide Loyalty mechanics/KPI blocks until v23 facts are approved and KPI numbers are source-backed.

## Safe Payloads
- none safe for immediate write

## Writer Sequence
- v25C-1 suppression writer: Implement conditional hide rules for Openings, Momentum, Mix, Standards placeholder, Demand Scenario, Loyalty fallback.
- v25C-2 safe row creation writer: Create/update only rows that are complete and source-backed (Openings, Momentum, optional Demand expansion).
- v25C-3 frontend mapping writer: Add Marriott-specific portfolio ladder mapping for overview.portfolio_context rendering.
- v25C-4 evidence/source capture writer: Capture/approve missing facts for loyalty mechanics + KPI, standards owner table, portfolio mix, demand, and footprint refinements.

## Exact next command
```bash
npm run brand-explorer-missing-content-remediation -- --brand tribute-portfolio --dry-run
```