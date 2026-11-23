# GDI Portfolio Cert V1.1 — Frozen ORS Fixtures

Repository-safe historical Opportunity Research State (ORS) fixtures for **V1.2 / V1.3 offline certification**.

## Why these exist

Live ORS files under `data/group-demand-intelligence/hotels/*/opportunity-research-state/` are **gitignored** (commit `d719eca7a`). The V1.1 paid pilot therefore never committed the eight ORS JSON files that V1.2/V1.3 offline recovery needs on a fresh checkout.

## Sources

- `reports/gdi-portfolio-cert-v1-1-paid-pilot/**/deep-research-selection-register.json` — seed URLs, evidence, identity
- `reports/gdi-portfolio-cert-v1-2-commercial-conversion-recovery/offline-conversion-recovery.json` — dispositions, eligibility gaps, deep-research verdicts, contact audit
- Git history ORS for `disc_9334f4f1e3dd` (AHA) as structural reference

## Sanitization

- No personal emails or phones
- No live Exa API run identifiers (fixture sentinels only)
- No Airtable secrets
- Public org/LinkedIn routes only when already present on the committed recovery report

## Rebuild

```bash
npm run gdi:build-portfolio-cert-v1-1-frozen-ors-fixtures
```

Do not invent investigated URLs, Sales Ready, or customer publication fields.
