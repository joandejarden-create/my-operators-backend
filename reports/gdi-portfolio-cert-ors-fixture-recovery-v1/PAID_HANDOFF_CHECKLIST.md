# Design Hotels controlled paid pilot — handoff checklist (NOT EXECUTED)

## Cohort (exact)

| Hotel | HPC |
|-------|-----|
| Condesa DF Hotel | `rece0NdV9FCnaFlVS` |
| LaLuna Boutique Hotels & Villas | `recaokE8psIbuVeuH` |
| Memmo Príncipe Real | `rec1t35e3sPerzxYu` |

Hotels file: `reports/gdi-design-hotels-operational-pilot-v1/design-hotels-pilot-cohort-hotels.txt`

## Proposed budgets (founder-authorized separately)

- Portfolio maximum: **$12 USD**
- Per-hotel maximum: **$4 USD**
- Hotels: the three above only

## $0 commands (already verified PASS)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Preview -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt

.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Rehearsal -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt
```

## Prepared paid command (DO NOT RUN without separate founder GO)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode FullE2E `
  -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt `
  -BudgetUSD 12 `
  -PerHotelBudgetUSD 4 `
  -AuthorizePaidResearch `
  -AllowLivePilot
```

Fail-closed preconditions already enforced by the launcher:

- Nonzero budget requires `-AuthorizePaidResearch` and `-AllowLivePilot`
- Rehearsal refuses paid/publication
- Publication / production writes are separate switches (leave off)
- Canonical Node engine owns spend ledger + resume

## Preflight checklist before any paid GO

1. [ ] Integrated main contains ORS fixture recovery (this PR) and V1.1–V1.3 + #570/#571
2. [ ] Fresh checkout: `npm run test:gdi-portfolio-cert-v1-2-commercial-conversion-recovery` PASS
3. [ ] Fresh checkout: `npm run test:gdi-portfolio-cert-v1-3-evidence-integrity-actionability` PASS
4. [ ] Design Hotels Preview + Rehearsal 3/3 at $0
5. [ ] Confirm no Surfe / paid contact enrichment flags
6. [ ] Confirm no `-AuthorizePublication` / `-AuthorizeProductionWrites`
7. [ ] Confirm HotelsFile contains exactly the three HPC IDs
8. [ ] Confirm durable spend ledger path and resume plan
9. [ ] Separate founder spend confirmation for Design Hotels (do **not** reuse `FOUNDER_GDI_PORTFOLIO_CERT_V1_1_PAID_25` unless IC explicitly maps it)

## Explicit non-goals of this remediation

- No paid research executed
- No Airtable opportunity writes
- No customer publication
- No new hotel enrollment / portfolio expansion
