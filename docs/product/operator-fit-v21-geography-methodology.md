# Operator Fit v2.1 — Geography Methodology

**Formula ID:** `balanced_depth_v1`  
**Top-level weight:** **22** (unchanged)

## Score (0–100)

```text
score = clamp(0, 100, presenceStrength + depthBonus)
```

Caps: Strategic Interest / Claimed Capability ≤ **22**; Historical ≤ **40**.

### Presence strength (Market Presence type in project country)

| Type | Strength |
| ---- | -------: |
| Current Managed Property | 92 |
| Current Operating Portfolio | 88 |
| Regional Office or Team | 80 |
| Active Development | 52 |
| Historical Presence | 32 |
| Strategic Interest | 16 |
| Claimed Capability | 12 |
| Unknown | 0 |

### Depth bonuses (additive)

| Signal | Bonus |
| ------ | ----: |
| City / metro match | +12 |
| Multiple / portfolio current ops | +8 |
| Single current property | +4 |
| Regional office with country ops | +3 |

### Fallback

If no Market Presence rows: Active Countries / markets fallback (city ≈ 90, country ≈ 78, else 12) — same spirit as v2, labeled as fallback.

### Eligibility unchanged

Strong presence types still establish geographic eligibility; Strategic / Historical / Claimed do not. Geography factor depth does **not** override hard eligibility gates.

### Formulation selection

Three formulations were considered (strict presence-only, balanced depth, aggressive city-first). **Balanced depth** selected: differentiates verified local depth without exceeding weight 22 influence and without inventing property counts from marketing claims.
