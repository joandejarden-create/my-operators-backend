# Universe Resolver Audit

## Duplicate logic found

| Location | What it decides | Risk |
| -------- | --------------- | ---- |
| `lib/operator-explorer/phase-1-universe.js` | Test Fixture IDs + Record Purpose map | Keep as SoT for fixtures |
| `scripts/operator-explorer-phase-1-apply.mjs` | Ad-hoc readiness thresholds (superseded) | **Was** inconsistent with dry-run |
| `scripts/build-operator-explorer-calibration-01.mjs` `buildProfile` | Dry-run readiness | Should call shared module |
| OE protected baseline / factory queues | Quality baselines, not universe Purpose | Separate concern |
| Operator Fit shortlist / candidate code | Fit candidates | **Do not rewire in this phase** — report only |

## Recommendation

**One shared module:** `lib/operator-explorer/operator-universe.js` + `lib/operator-explorer/readiness.js`.

Status: **created**. Fit production path **not** rewired.
