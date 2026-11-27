# GDI Leisure Demand Intelligence V1

LDI is a **capability layer** on Full E2E — not a separate product or opportunity database.

- Registry: `lib/group-demand-intelligence/leisure-demand-intelligence/`
- PowerShell: `scripts/gdi-leisure/Invoke-GdiLeisure*.ps1`
- Portfolio flags: `-LeisureModules`, `-LeisureOnly`, `-DisableLeisure` on `Invoke-GdiPortfolioProcessing.ps1`
- Gate: `npm run test:gdi-leisure-demand-intelligence-v1`
- Authorized live gate: `npm run test:gdi-leisure-authorized-live-v1`
- Executable commands: `docs/data-intelligence/gdi-leisure-authorized-live-and-full-e2e-commands-v1.md`
- Founder report: `reports/gdi-leisure-demand-intelligence-v1/FOUNDER_REPORT.md`

Defaults: fixture / $0 / no publication / no Findymail / no Surfe / no Airtable apply.

**Authorized live** (product-wide, not hotel-specific): DryRun/fixture remains default. Live requires `-AuthorizePaidResearch -AllowLivePilot -AuthorizationId FOUNDER_GO_… -HardCeilingUsd > 0 -BudgetUSD > 0` and `$env:GDI_LDI_LIVE='1'`. Reuses fourteen-segment discovery cells + durable paid budget ledger; settled-job reuse / resume; no parallel leisure engine. Use `-PreflightOnly` for $0 readiness.
