# Operating Platform — Explorer capability tiles (Airtable)

Owner-facing **custom capability tiles** (up to **6 per pillar**) on the Operating Platform tab. Each tile has an operator-defined **title** and **description**.

## Pillar JSON fields (prefill / `explorerProfileJson`)

| Pillar | Prefill / explorer key |
|--------|-------------------------|
| Commercial Engine | `op_commercial_engine_json` |
| Owner Reporting & Communication | `op_owner_reporting_json` |
| Pre-Opening & Transition Support | `op_preopening_transition_json` |
| Conversion & Repositioning | `op_conversion_repositioning_json` |
| F&B, Lifestyle & Resort Capability | `op_fb_lifestyle_resort_json` |

### JSON shape

```json
{
  "intro": "Optional one-line subsection summary for owners.",
  "items": [
    {
      "title": "Revenue Management",
      "description": "Operator-specific explanation of what they provide."
    }
  ]
}
```

- **`items`**: 1–6 objects; `title` required; `description` optional but recommended.
- **`intro`**: Overrides default subsection intro when present.

## Fallback (until JSON is populated)

1. Multiline **legacy** fields (each line → tile; `Title: description` split on first colon):
   - Commercial: `cap_profile_commercial`
   - Pre-opening: `cap_profile_transition`
   - Owner reporting: `cap_card_governance`
   - F&B: `cap_card_service_diff`
   - Conversion: `cap_deep_revenue_systems` (if used)
2. Capability bullets derived from Setup (`offeredServices`, experience fields) as **custom titles** (no shared catalog text).

## KPI row (unchanged)

Single-select levels: `revenueManagementCapability`, `ownerReportingLevel`, `preOpeningSupportCapability`, `conversionReflagExperience`, `fbCapabilityLevel` (+ `cap_kpi_*` fallbacks).

## Schema & Setup (2026-06)

- **Airtable columns:** `op_*` on **Operator Setup — Platform & Markets** (`multilineText`). Create via `node scripts/ensure-operator-dna-explorer-json-schema.mjs --apply`.
- **Bindings / writer:** `api/lib/third-party-operator-new-two-field-bindings.json` + `operator-setup-new-base-build-sheet-rows.json` (sync: `node scripts/sync-operator-dna-explorer-json-fields.mjs`).
- **Setup form:** JSON textareas on tab 2 (Operating Platform) in `third-party-operator-setup-new-two.html`.
