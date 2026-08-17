# Active Brand CALA — Wave 10 summary

**Generated:** 2026-07-24  
**Prior:** Wave 9 (next-20 Choice sprint + 1/5 puppeteer pilot)  
**Scope:** Expand BWH puppeteer · Choice Chrome-channel pilot on next-20 sample

## Results

| Path | Outcome |
|------|---------|
| BWH remaining `bestwestern.com` blanks (8) | **0/8** — all blocked (403/captcha). Monterrey already filled in Wave 9. |
| Choice next-20 sample puppeteer (8) | **0/8** — all blocked. Verdict: **steward-only for Choice** |

## Conclusion

Automated browser fetch is **not viable** for the remaining Choice and BWH gaps. The only high-yield next step is the human Choice sprint already prepared:

1. Open **`reports/choice-next20-steward-opener.html`**
2. Save Webpage Complete for each row
3. `node scripts/backfill-choice-wave4-from-html.mjs --apply`

Artifacts:
- `reports/bwh-wave10-puppeteer-plan.json`
- `reports/wave10-choice-puppeteer-pilot-report.md`
- Scripts: `scripts/backfill-bwh-wave10-puppeteer.mjs`, `scripts/run-wave10-choice-puppeteer-pilot.mjs`

## Change impact

**Low** — 0 Airtable writes this wave.

## Manual QA

- [ ] Confirm Choice opener still lists 20 rows with valid URLs
- [ ] After first 5 steward saves, dry-run Choice HTML apply shows Ready ≥ 1
