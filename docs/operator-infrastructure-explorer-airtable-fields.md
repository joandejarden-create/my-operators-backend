# Infrastructure & Data — Explorer Airtable fields

**Table:** `Operator Setup - Governance, Delivery & Diligence`  
**Prefill:** Merged via `buildPrefillObjectFromNewBaseRows` (underscore keys overlay `prefill`).

## Top KPI snapshot (existing)

| Prefill key | Airtable field | Type |
|-------------|----------------|------|
| `infra_signal_uptime` | `infra_signal_uptime` | singleSelect |
| `infra_signal_incident` | `infra_signal_incident` | singleSelect |
| `infra_signal_adoption` | `infra_signal_adoption` | singleSelect |
| `infra_signal_refresh` | `infra_signal_refresh` | singleSelect |
| `risk_signal_audit` | `risk_signal_audit` | singleSelect |
| `risk_signal_bcp` | `risk_signal_bcp` | singleSelect |
| `risk_signal_control` | `risk_signal_control` | singleSelect |
| `risk_signal_insurance` | `risk_signal_insurance` | singleSelect |

## Structured subsections (JSON in long text)

| Prefill key | UI subsection | JSON shape |
|-------------|---------------|------------|
| `infra_technology_stack_json` | Technology Platform Stack | `[{ title, description, examples? }]` |
| `infra_services_offered_json` | Infrastructure Services Offered | `[{ title, description }]` |
| `infra_data_domains_json` | Data Domains Captured | `[{ title, items[] }]` |
| `infra_data_governance_json` | Data Governance, Security & Controls | `[{ title, description }]` |
| `infra_analytics_support_json` | Analytics & Decision Support | `[{ title, description }]` |
| `infra_technology_maturity_level` | Technology Maturity View | `Basic` \| `Structured` \| `Integrated` \| `Advanced` |

Optional: `infra_technology_maturity_json` as `{ currentLevel, summary?, evidence?[] }` (not required when level select is set).

## Scripts

```bash
node scripts/ensure-operator-infrastructure-explorer-schema.mjs --apply
node scripts/seed-operator-infrastructure-explorer-data.mjs --apply
```

Seed varies maturity level and KPI presets by row index; fills empty signal selects only.
