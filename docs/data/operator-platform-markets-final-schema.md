# Operator Setup — Platform & Markets Final Product Schema

**Policy:** operator-setup-no-optional-fields-v1 — **No OPTIONAL fields.** Every retained field is **REQUIRED — VALUE OR CONTROLLED STATE**.

**Question:** Where does this operator operate, and what is distinctive about its operating platform?

| Field | Storage | Class | Why |
| ----- | -------- | ----- | --- |
| company_name | platform | RETAIN — REQUIRED | Row identity |
| Active Countries | platform | RETAIN — REQUIRED | Verified CALA taxonomy countries OR controlled no-presence state |
| Market Presence Type | platform | RETAIN — REQUIRED | Active / pipeline / no known presence posture |
| specificMarkets | platform | RETAIN — REQUIRED | Non-taxonomy geography notes or controlled empty-note state |
| Active Markets / Cities | platform | RETAIN — REQUIRED | CALA city/corridor mapping OR controlled no-mapping state |
| cap_profile_operational | platform | RETAIN — REQUIRED | Operating platform narrative — Writer v2 + controlled no-evidence |

## Removed from retained product

- **cap_profile_commercial** — MOVE TO CLAIMS: No completed Writer v2 commercial contract; not justified as required Setup narrative before Fit
- **cap_profile_transition** — MOVE TO CLAIMS: Already MOVE TO CLAIMS per D.2/D.5; not Setup narrative

Geography: never use `Other`. Empty CALA taxonomy → `No verified CALA operating presence`.
