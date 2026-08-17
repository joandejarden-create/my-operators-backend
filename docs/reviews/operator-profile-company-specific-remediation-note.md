# Profile company-specific remediation

Founder feedback: populated fields must be company-specific and research-backed — not generic boilerplate.

## What was wrong

The first full-live apply filled blanks to 36/36 but used template language for many cells:

- identical `brand_*_json` (“Documented brand relationship…”)
- `Yes - Standard` / `Yes - Limited hours` ops selects
- diligence boilerplate for ESG/sustainability
- `figuresAsOf: August 2026 (full live Profile completion)`
- weak taglines (e.g. Barceló = company name)

## What we did

1. Built `lib/operator-setup/full-live-profile-company-specific-remediation.js` from D.4D research + live Profile narratives + official slogans.
2. Remodeled brand JSON builders to require company name + differentiators.
3. Applied remediation across all 36 Production operators.

## Results

| Metric | Before remediation | After |
| ------ | ------------------ | ----- |
| Generic marker cells (remediation field set) | 319 | **47** |
| Airtable writes | — | **36** operators, 0 failures |
| Backup | — | `backups/operator-setup/full-live-profile-company-specific/2026-08-11T14-12-06/` |

Remaining ~47 “generic” hits are mostly intentional controlled taxonomy (`brand_signal_audit = Not Measured / N/A`) and a few conversion-count N/As — not interchangeable narrative boilerplate.

## Examples of corrected values

- **Barceló tagline:** `We Care About People` (was company name)
- **Four Seasons brand JSON:** single-brand HMA-only portfolio (no franchise)
- **AADESA brand JSON:** Cyan/DON + Wyndham franchise platform language
- **Iberostar sustainability:** Wave of Change (named program)
- **HE/Arbor:** exemplars preserved; only thin signals / HE sustainability wording refined

## Preview

`docs/reviews/operator-profile-company-specific-remediation-preview.md`

## Scripts

```bash
node scripts/operator-setup-profile-company-specific-remediation.mjs --dry-run
node scripts/operator-setup-profile-company-specific-remediation.mjs --apply --approve-operator-setup-profile-company-specific
```

Platform / Fit still not started.
