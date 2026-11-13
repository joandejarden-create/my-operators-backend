# GDI Regression Recovery — Consolidated PR Disposition

**Branch:** `fix/gdi-regression-recovery-v1`  
**Base:** `main`  
**Policy:** Do not merge/deploy without founder authorization.

## Older PRs — disposition

| PR | Title | Head | Contained in recovery branch? | Disposition |
|---|---|---|---|---|
| #230 | Recover GDI card contacts, titles, demand icons, and toolbar alignment | `fix/gdi-opportunity-card-quality-v1` | Yes (`80be5b88d` + merge `f5eb9f80e`) | Keep open until consolidated PR merges; then close as superseded |
| #231 | Restore GDI on-demand Surfe contact lookup | `fix/gdi-surfe-on-demand-contact-reveal-v1` | Yes (`1fdd835cc` + merge `a34ec4143`) | Keep open until consolidated PR merges; then close as superseded |
| #232 | GDI permanent customer-facing opportunity title contract | `fix/gdi-customer-facing-opportunity-title-contract-v1` | Yes (`8c586ba23`) | Keep open until consolidated PR merges; then close as superseded |

**Do not independently merge #230 / #231 / #232** once this consolidated PR is the merge vehicle — overlapping implementations are already reconciled here (title contract extended for competitor/host suffixes; Market Scope / Surfe / tour preserved).

## Feature-preservation checklist

| Feature | Status |
|---|---|
| PR #230 card quality + Market Scope toolbar height | Preserved |
| PR #231 Surfe on-demand Find Email/Phone | Preserved |
| PR #231 guided tour assets | Preserved |
| PR #232 customer-facing opportunity title contract | Preserved + extended (competitor/host/venue suffixes) |
| Reset View active-filter badge | Preserved |
| Group & Demand Opportunities nav title | Preserved |
| Removal of “X updated” header counters | Preserved |
| Simplified customer-facing research descriptions | Preserved |
| Competitive Market Scope (All / Hotel / Competitive) | Preserved |
| Opportunity Type classification remediation | Included |
| Title + sourcing + CMI commercial consistency | Included |
| Bethesda baseline Market Scope counts | Locked in tests (67 inventory) |

## CI protection

Workflow: `.github/workflows/gdi-regression-recovery-baseline.yml`  
Runs on GDI-touching PRs to `main`:

- `test:gdi-regression-recovery-v1` (includes opportunity-type + title/sourcing)
- `test:gdi-customer-facing-opportunity-title-contract-v1`
- `test:gdi-opportunity-card-quality-v1`
- `test:gdi-market-scope-toggle-v1`
- `test:gdi-surfe-on-demand-contact-reveal-v1`
- `test:gdi-guided-report-tour-v1`
- `test:gdi-hotel-toolbar-reset-view-only-v1`
