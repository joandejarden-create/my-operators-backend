# v40C — Economics Chrome + Residual Owner Copy Remediation

After v40 Presentation scrub and v40B internal-preview audit, three release candidates remained `not_owner_ready` because:

1. **Renderer economics chrome** injected FDD / Item 7 / LOI / disclosure / fee-stack / net-contribution language
2. **Residual Presentation** still contained ADR / RevPAR / URLs / mechanical scrub phrases

```bash
npm run brand-explorer-v40c-economics-chrome-remediation -- --brands everhome-suites,kimpton,radisson-individuals-by-choice --dry-run
```

## What v40C does

| Layer | Action |
|-------|--------|
| Renderer | Owner-safe economics / loyalty chrome defaults + Brand Setup note sanitizer (`brand-explorer-atelier-from-api.js`) |
| Presentation | Residual scrub patch plan (Title / Body / Case Summary) — dry-run only |
| Internal preview test | `test:brand-explorer-internal-preview-owner-copy` (founder path) |
| External lock | Must remain Profile in Preparation |

## Guardrails

- Dry-run by default; `--apply` refused in this pass (command designed in reports)
- No active approval / Company Validated / Source Library / Registry / image fields / unlock
- Incomplete brands remain locked

## Internal preview rule

External quality lock PASS only proves the profile is hidden. Founder review uses `?beInternalPreview=1` — that path must not show FDD/LOI/fee-stack/ADR/URL owner-unsafe copy.

## Modules

- `lib/partner-intelligence/brand-explorer-economics-chrome-remediation.js`
- `lib/partner-intelligence/brand-explorer-residual-owner-copy-remediation.js`
- `lib/partner-intelligence/brand-explorer-v40c-economics-chrome-remediation-run.js`
- `scripts/brand-explorer-v40c-economics-chrome-remediation.mjs`
- `scripts/test-brand-explorer-internal-preview-owner-copy.mjs`
