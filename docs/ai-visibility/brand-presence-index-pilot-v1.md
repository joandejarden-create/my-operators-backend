# Brand AI Presence Index Pilot V1

> **Pilot version:** `brand_presence_index_pilot_v1`  
> **Dataset class:** `DEMO_VALIDATION` — not client production benchmark history  
> **Module:** `lib/ai-visibility/competitive-moat/brand-presence-index-pilot.js`

## Scope

Offline pilot for the **AI Presence Index** using founder-approved internal benchmark expansion:

- **Customer-visible subjects:** 19 showcase brands (unchanged)
- **Internal benchmark peer set:** `peers_uu_collection_lifestyle_owner_decision_v5` (22 brands = frozen v2 + 7 additions)
- **Provider calls:** 0 — re-extraction from stored responses only

## Approved internal additions (INTERNAL_BENCHMARK_ONLY)

1. Handwritten Collection (IHG)
2. Trademark Collection by Wyndham
3. BW Premier Collection
4. BW Signature Collection
5. Preferred Hotels & Resorts
6. DoubleTree by Hilton
7. Small Luxury Hotels of the World

Not added to customer dropdown, parent filter, entitlements, or showcase portfolio.

## Peer-set versioning

| Version | ID | Purpose |
|---------|-----|---------|
| v2 | `peers_uu_collection_lifestyle_owner_decision_v2` | Frozen live comparative rank |
| v3 | `peers_uu_collection_lifestyle_owner_decision_v3` | Longitudinal (v2 + Radisson) |
| v4 | `peers_showcase_portfolio_monitoring_v4` | 19-brand monitoring universe |
| **v5** | `peers_uu_collection_lifestyle_owner_decision_v5` | **Internal benchmark pilot (v2 + 7)** |

Do not mutate v2–v4.

## Historical re-extraction

- Source: stored provider response corpus (`legacy-language-backfill`, `presence-validation-candidates`, `presence-holdout-v3-candidates`)
- Preserves: original timestamp (when available), provider, prompt, geo, language, response ID, wave ID
- **No synthetic observations** — `synthetic: false`, `preservedFromStoredResponse: true`

## Leave-one-out stability thresholds

| State | Max index movement |
|-------|-------------------|
| STABLE | ≤ 7 points |
| MODERATELY_SENSITIVE | 8–14 points |
| FRAGILE | ≥ 15 points |

## API (additive — no UI in this phase)

| Endpoint | Auth | Payload |
|----------|------|---------|
| `GET /api/ai-visibility/brand/:brandId/benchmark` | Brand AI visibility | Customer allowlist only |
| `GET /api/ai-visibility/brand/:brandId/benchmark/diagnostics` | Internal runbook admin | Full cohort diagnostics |

## Commands

```bash
npm run brand-presence-index-pilot:run
npm run test:brand-presence-index-pilot-v1
```

## Output

- `reports/ai-visibility/brand-presence-index-pilot-v1.json`
- `data/ai-visibility/runtime/brand-presence-index-pilot/pilot-v1.json`

## Readiness gate

Pilot may reach `READY_FOR_CUSTOMER_PILOT` when:

- Majority of 19 subjects have VALID benchmark cohorts (target ≥70%)
- Median aggregation stable vs mean/trimmed mean
- Leave-one-out not FRAGILE
- Customer payload passes redaction audit
- **Cohort integrity certified** (`docs/ai-visibility/benchmark-cohort-integrity-audit-v1.md`) — commercially relevant peers, not N≥5 alone

Until then the index is **READY_FOR_INTERNAL_REVIEW**. UI cards for AI Presence Index are **not** enabled.
