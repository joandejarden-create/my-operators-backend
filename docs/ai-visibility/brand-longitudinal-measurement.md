# Brand AI Intelligence — Longitudinal Measurement

> **Version:** `brand_longitudinal_measurement_v1`  
> **Status:** Initial multi-parent wave executed `2026-08-18` — period `aiv_brand_longitudinal_period_20260818_6579d2`. Scheduler remains off.  
> **Cohort:** `BRAND_LONGITUDINAL_COHORT_V1`

## Objective

Start accumulating **real elapsed-time** Brand AI measurement history without synthetic backfill, UI redesign, or the full 133-prompt universe.

**SYNTHETIC_HISTORY = 0 · BACKDATED_HISTORY = 0 · FULL_133_PROMPT_RUN = 0**

---

## Core principle

Trend requires:

- Real elapsed time between measurement periods
- Consistent grain: **PROMPT × BRAND × PROVIDER × MODEL × GEOGRAPHY × LANGUAGE × MEASUREMENT DATE**
- Comparable prompts, providers, geography, language
- Stable semantic classifiers

Same-day repetitions (including Stage B stability runs) support **stability**, not separate trend dates.

---

## Baseline audit (as of foundation build)

| Field | Value |
|-------|-------|
| **PRIMARY_BASELINE_DATE** | `2026-08-14` |
| **REAL_DISTINCT_PERIODS** | `2` |
| **CLIENT_STATE** | `CURRENT_VS_PRIOR` |
| **PROVIDER_COVERAGE** | OpenAI, Gemini, Perplexity, Claude |
| **STORE_MODE** | `federated_measured_baseline` + isolated `brand-longitudinal/` periods |
| **DATASET_CLASS** | `DEMO_VALIDATION` (`demo_validation_multi_parent_brand_ai`) |

### Prompt universe (fixture audit)

| Count | Value |
|-------|-------|
| TOTAL_ACTIVE | 134 |
| MONITORING_ELIGIBLE | 122 |
| OBSERVED_ACTIVE | 9 |
| DERIVED_ACTIVE | 2 |
| SCENARIO_ACTIVE | 123 |

Observed/DERIVED prompts remain `monitoringEligible=false` until explicitly activated.

---

## Stage B vs longitudinal

| | Stability (Stage B) | Longitudinal |
|--|-------------------|--------------|
| **Purpose** | Same-cycle agreement | Change across dates |
| **Authoritative wave** | `aiv_stability_stage_b_20260817_f0a829` | N/A until period 2 |
| **Excluded wave** | `aiv_stability_stage_b_20260817_22c195` (audit only) | Never a trend period |
| **2026-08-17 repetitions** | Valid stability evidence | **Not** a second trend date |

Report path: `reports/ai-visibility/repeated-testing-stage-b-report-final-wave.json`

---

## Cohort V1 — `BRAND_LONGITUDINAL_COHORT_V1`

**Fixture:** `fixtures/ai-visibility/brand-longitudinal-cohort-v1.json`

| Tier | Count | Provider policy | Cadence |
|------|-------|-----------------|---------|
| CRITICAL | 16 | 4 providers | monthly |
| HIGH | 11 | OpenAI + Perplexity | monthly |
| STANDARD | 8 | OpenAI + Perplexity | quarterly |
| **Total core** | **35** | | |

### Observed demand classification

| Prompt | Classification | Rationale |
|--------|----------------|-----------|
| `p_obs_franquicia_hotelera_es_v1` | **PERIODIC_OBSERVED** | Stage B: 0/1 Brand resolution — retain for research, not core grain |
| `p_obs_contrato_de_gestion_hotelera_es_v1` | **PERIODIC_OBSERVED** | Spanish HMA literal — quarterly research |
| `p_obs_hotel_franchise_fees_derived_en_v1` | **PERIODIC** | Parent observed prompt already in HIGH core |
| `p_obs_franquicia_hotelera_derived_es_v1` | **RESEARCH_ONLY** | Until parent achieves Brand resolution |

### Brand scope

- **Portfolio:** Marriott showcase (`brand-ai-showcase-companies-v1.json`)
- **Geography:** CALA primary (+ global prompts for chain-scale context)
- **Environment:** Railway staging Brand AI service → `demo_validation_marriott_cala` namespace

Demo measurement history must **not** silently become client production history.

---

## Cost model (monthly cycle)

Historic per-call rates from four-provider baseline ledger (`stability-policy.js`):

| Provider | Historic $/call | Sample |
|----------|-----------------|--------|
| OpenAI | $0.457262 | 84 |
| Gemini | $0.078095 | 84 |
| Perplexity | $0.005595 | 84 |
| Claude | $0.692738 | 84 |

**Monthly cohort (CRITICAL + HIGH):** 27 prompts → **~$24.83** historic / cycle  
**Full cohort matrix:** 35 prompts → **~$28** historic  
**Target:** ≤ $75/month · **Stop:** > $100 historic-effective

**MAX_INITIAL_LONGITUDINAL_AI_SPEND = $75**

---

## Storage architecture

| Layer | Location | Role |
|-------|----------|------|
| **Observation store** | `data/ai-visibility/runtime/brand-longitudinal/{periodId}/` | Raw responses + evidence (not Airtable) |
| **Period manifest** | `{periodId}/period-manifest.json` | Period metadata + quality state |
| **Idempotency ledger** | `brand-longitudinal/measurement-ledger.json` | Duplicate-run protection |
| **Baseline reads** | Federated wave1 + provider-baselines | Existing 2026-08-14 baseline |
| **Airtable** | Prompt config, cadence, governance flags | **Not** high-volume response history |

---

## Quality gates

| Gate | Rule |
|------|------|
| **VALID period** | ≥ 95% planned calls successful |
| **PARTIAL_PERIOD** | Excluded from headline trend |
| **Retry policy** | Max 1 retry per call; preserve failed attempt |
| **Duplicate protection** | Idempotency key → `NO_SECOND_EXECUTION` |
| **Scheduler** | **OFF** until founder activation |

---

## Trend display rules (permanent copy)

| Distinct valid periods | Client state | Copy |
|------------------------|--------------|------|
| 1 | BASELINE_ONLY | "Baseline measurement" |
| 2 | CURRENT_VS_PRIOR | "Change since prior measurement" |
| 3 | EARLY_TREND | "Early trend" |
| 4–6+ | TREND | "Trend" |

**Never** label two points as "Trend improving."

---

## Metrics

### Trendable

AI Presence, Questions Missing, Citation Rate, Provider Presence, Competitive Gap persistence, Source mix

### Trendable with caution

Narratives, Associations, Perception gaps (require multiple genuine periods)

### Forbidden

Recommendation Rate, Recommendation Share, Questions Won, Win Rate, Average Recommendation Position, Owner Decision Share of Voice

---

## Module map

```
lib/ai-visibility/brand-longitudinal/
  grain.js              — canonical measurement grain
  comparability.js      — trend vs stability comparability contract
  common-cohort.js      — intersection denominators
  cohort-v1.js          — BRAND_LONGITUDINAL_COHORT_V1
  cost-model.js         — provider cost gates
  measurement-period.js — period manifests + quality qualification
  idempotency.js        — duplicate-run lock
  current-vs-prior.js   — movement semantics
  baseline-audit.js     — federated baseline read-only audit
  trend-display-rules.js — client copy thresholds
  metric-classification.js — trendable vs forbidden
  index.js              — foundation report builder
```

---

## Commands

```bash
npm run test:ai-visibility-brand-longitudinal-foundation
npm run ai-visibility-brand-longitudinal-audit -- --dry-run
```

Audit output: `reports/ai-visibility/brand-longitudinal-foundation-audit.json`

---

## Initial wave

**STATUS:** Completed `2026-08-18` (founder-approved multi-parent shared execution).

- Period: `aiv_brand_longitudinal_period_20260818_6579d2`
- Calls: 86/86 successful · spend $24.83 · cap $60
- Measurement peer set: `peers_uu_collection_lifestyle_owner_decision_v3` (frozen v2 + Radisson)
- Live Brand AI comparative reads remain on frozen peer v2
- Scheduler: **OFF**
- Next recommended measurement date: `2026-09-18`

---

## Regression invariants

All existing Brand AI Intelligence layers remain **DIFF = 0** for classifier/UI logic. New real-period observations may change measured values. Isolated store: `data/ai-visibility/runtime/brand-longitudinal/`. Live Executive Summary still reads federated 2026-08-14 baseline until a later governed read-path join.
