# Deal C — Execution Risk (−10) Deduction Audit

Both Santa Fe and Highgate: **penalty 10** · kinds are **unknown_validation only** (no confirmed_risk items).

| Risk / Concern | Status | Kind | Numerical Effect | Also penalized elsewhere? |
| -------------- | ------ | ---- | ---------------: | ------------------------- |
| missingStructureSupport | Owner structure preference empty; structure layer unknown | unknown_validation | **5** (half of 10) | Structure contributes **0** in 15% layer; coverage unknown weight **15**; Validate Next asks to capture structure |
| unconfirmedRegionalResources | No regional resources list | unknown_validation | **5** | Regional factor **unknown → 0** in denom (weight 6); coverage missing field; owner unknown text |

**Confirmed adverse facts in this −10?** **No.**

## Classification

| Item | Class |
| ---- | ----- |
| Structure unknown → risk −5 | **Possible double penalty** (zero layer + coverage + risk + validation) |
| Regional unknown → risk −5 | **Possible double penalty** (zero factor + coverage + risk + validation) |
| Moving 48.6 → 38.6 (Potential→Limited band) | Methodology tension: unknowns treated like soft risks |

## Sensitivity (audit simulation only)

| Mode | SF / HG Displayed | Band |
| ---- | ----------------: | ---- |
| Current | 38.6 | Limited |
| Confirmed risks only | 48.6 | Potential |
| Unknowns → validation only (0 risk pts) | 48.6 | Potential |
| Potential half (N/A here) | 48.6 | Potential |

## Verdict

The −10 is **entirely unknown-driven** on Deal C leaders and **overlaps** missing-data treatment already applied in factors/coverage. Not an “incorrect” bug vs current code, but a **methodology consistency / double-penalty** concern if unknowns should not push bands downward twice.
