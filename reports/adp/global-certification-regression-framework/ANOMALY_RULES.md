# Anomaly Rules

Module: `adp-anomaly-rules-v1.js`

Extreme shifts (>50% relative consideration/presence), provider zero presence, scenario count jumps, owned-source zero despite citations, identity match rate issues → QA_REVIEW_REQUIRED with exact anomaly + `explainAdpAnomaly`.

New-hotel first runs: structural checks without trend comparison.

Cross-metric invalid states → QA_FAILED.

QA must not modify metrics simply because they look low/high.
