# Readiness Consumer Audit

| Consumer | Previous Logic | Canonical Module Now? | Result |
| -------- | -------------- | --------------------- | ------ |
| `lib/operator-explorer/readiness.js` | SoT | Yes | Authoritative |
| `lib/operator-explorer/operator-universe.js` | Uses readiness | Yes | Authoritative |
| `scripts/operator-explorer-phase-1-apply.mjs` | Phase1 asg≥5/8 | Yes — `classifyExplorerReadiness` | Fixed |
| `scripts/operator-explorer-wave-01-operationalize.mjs` | — | Yes | Uses universe builder |
| `scripts/build-operator-explorer-calibration-01.mjs` `buildProfile` | Inline dry-run gates | **Pending route** (historical dry-run package) | Documented debt |
| `scripts/audit-operator-explorer-readiness-parity.mjs` | Comparative replicas | Audit-only | Keep for history |
| Operator Fit scoring | Separate | **No — not rewired** | Future debt |
