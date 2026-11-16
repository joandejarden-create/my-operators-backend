# LOYALTY / CORE ISOLATION

| Field | Value |
|------|------|
| CORE CURRENT PERIOD | `adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b` |
| CORE family | `CORE_ADP` |
| CORE scenarios | 63 (expected 63) |
| CORE observations | 252 (expected 252) |
| LOYALTY CURRENT PERIOD | `adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1` |
| LOYALTY family | `LOYALTY_BPP` |
| LOYALTY scenarios | 28 (expected 28) |
| LOYALTY observations | 112 (expected 112) |
| loadLatestCustomerPeriod | `adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b` (must be Core) |
| loadLatestLoyaltyPeriod | `adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1` |
| loadLatestPeriod(includeLoyalty:false) | `adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b` |
| Business missing periodId | `adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b` |
| Loyalty contamination of Core metrics | **NO** |
| Portfolio section family | `LOYALTY_BPP` |

## Isolation rules verified

- Core consideration / scenario presence / rank / territory / provider / Reality Gap / displacement / citations use Core `c8bd8b` only.
- Choice Privileges / Brand & Portfolio Position uses Loyalty `308ba1` (BPP).
- Evidence drawers for Core territories resolve published Core period first (not newest Loyalty file).
