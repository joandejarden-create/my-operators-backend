# Benchmark Engine V1

> **Engine ID:** `BENCHMARK_ENGINE_V1`  
> **Supported metric:** Presence only  
> **Module:** `lib/ai-visibility/competitive-moat/benchmark-engine-v1.js`

## AI Presence Index

**Formula:**

```
AI Presence Index = (Subject comparable Presence ÷ Relevant benchmark comparable Presence) × 100
```

**Parity:** 100 = competitive parity  
**Example:** Subject 72%, Benchmark 60% → Index 120 (20% above benchmark)

## Benchmark Aggregation

**Recommended method:** **Median**

Rationale: Robust to outlier peers and single-scenario spikes. Mean would overweight one high-Presence collection brand. No secret weighting.

Alternatives audited but not selected for V1:
- Mean — sensitive to outliers
- Trimmed mean — requires larger cohorts

## Cohort Logic

Contextual / **scenario-specific** benchmark cohorts — never compare all entities indiscriminately.

**V2 contract (remediation):** [benchmark-cohort-contract-v2.md](./benchmark-cohort-contract-v2.md) · [scenario-peer-eligibility-v1.md](./scenario-peer-eligibility-v1.md) · [brand-presence-index-remediation-v1.md](./brand-presence-index-remediation-v1.md)

### Brand V1 cohort (historical pilot — not customer certified)

- Live comparative rank peer set: `peers_uu_collection_lifestyle_owner_decision_v2` (frozen)
- Internal benchmark pilot peer set: `peers_uu_collection_lifestyle_owner_decision_v5` (v2 + 7 INTERNAL_BENCHMARK_ONLY brands)
- Geography: comparable commercial region (default CALA for feasibility audit)
- Entity type: Brand only — **never mix with Operator**
- **UNION-grain Presence benchmarks are PROHIBITED for certification.** V1 pilot still documents UNION for integrity history; V2 uses pairwise scenario intersection.
- **Full-set fallback is PROHIBITED.** Too few commercially relevant peers → LIMITED / SUPPRESSED, not “use the whole universe.”

**Important:** Internal benchmark universe may exceed customer-visible universe (19 brands). Customer-visible portfolio must not expand merely to strengthen benchmarking. Vignette, Voco, Radisson, and Radisson Blu are **benchmark-eligible** even when absent from v5.

### Benchmark aggregation

Working candidate remains **Median**. **Mean vs median vs trimmed mean is DEFERRED until scenario cohorts are certified.** Headline index aggregation is **DEFERRED**.

### Operator cohort

- Primary monitored operators (9) minus subject
- Activated after `OPERATOR_SIGNAL_PRESENCE` production validation

### Common cohort rules

- Same prompt cohort where required
- Same provider panel where All Providers derived
- Compatible prompt versions
- Compatible classifier versions
- Commercial eligibility preserved
- Model-boundary awareness — do not interpret model changes as market movement

## Minimum Sample Policy

| Peers in cohort | Status | Customer display |
|-----------------|--------|------------------|
| ≥ 5 | VALID_BENCHMARK | Index value (rounded integer) |
| 3–4 | LIMITED_BENCHMARK | Index with limited-sample band |
| 0–2 | SUPPRESSED_INSUFFICIENT_DATA | "Not enough comparable observations yet" |
| Benchmark = 0 | INDEX_SUPPRESSED_ZERO_BENCHMARK | Suppressed |

## Gap to Benchmark

Customer may see:
- Index value
- Percent above/below benchmark
- Gap to benchmark (derived from index − 100)

Do **not** expose raw cross-competitor matrix.

## Gap to Leader

Shown only when:
- Leader from same valid benchmark cohort
- Sample sufficient
- Leader measurement directly comparable
- Access policy allows

Output: `-12 index points` (not full competitor table)

## Index Trend

| Periods | Allowed |
|---------|---------|
| 1 | Baseline only |
| 2 | Current vs Prior |
| 3 | Early trend |
| 4+ | Full trend per longitudinal contract |

Do not claim momentum from 2 points.

## Version Object

```json
{
  "engineId": "BENCHMARK_ENGINE_V1",
  "supportedMetric": "PRESENCE",
  "cohortLogicVersion": "contextual_cohort_v1",
  "aggregationMethod": "MEDIAN",
  "indexRounding": "nearest_integer",
  "suppressionRules": "min_sample_policy_v1",
  "modelBoundaryPolicy": "preserve_boundary_marker",
  "accessPolicyVersion": "competitive_data_access_v1"
}
```

## Internal vs Customer

| Internal | Customer |
|----------|----------|
| All cohort members | Subject only |
| All raw comparable metrics | Index + gap |
| Cohort selection explanation | Safe benchmark label |
| Normalization diagnostics | Sample band |
| Classifier versions | — |

## Feasibility Audit

Offline read-only audit against existing P0C gap report:

```bash
npm run competitive-moat:audit
```

Output: `reports/ai-visibility/competitive-moat-architecture-v1.json`

No provider calls. No UI deployment.
