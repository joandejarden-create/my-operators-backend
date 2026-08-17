# Brand AI Visibility — Post-Run Validation Checklist

Run after any new monitoring write. **Do not publish** until PASS (or explicit accepted PARTIAL with UI honesty).

## Automated tests / scripts (existing)

```bash
npm run test:ai-visibility-data-foundation-remediation
npm run test:ai-visibility-all-providers-client-state
npm run test:ai-visibility-language-purity
npm run test:ai-visibility-language-foundation
npm run test:ai-visibility-citation-calculation-fix
npm run test:ai-visibility-source-citation-frequency
npm run test:ai-visibility-source-mix
npm run test:ai-visibility-owned-domain-governance
npm run test:ai-visibility-brand-baseline-read
npm run ai-visibility:audit-brand-report
# Optional integrity harness (if present in workspace):
# node scripts/_tmp-aiv-final-two-tab-integrity.mjs
```

## Required manual / payload checks

| Gate | Requirement |
|------|-------------|
| Provider completion | Completed providers listed; missing ≠ 0% |
| Prompt counts | Slot Monitored = expected cohort size (CALA EN = 12) |
| Arithmetic | Present + Missing = Monitored; Presence = Present/Monitored |
| Language isolation | EN view uses EN only; ES view uses ES only |
| Geography isolation | CALA ≠ Global average; no silent geo substitution |
| Subject isolation | Brand Detail numerator = selected brand only |
| All Providers | Derived only; QM = missing across all comparable; disagreement preserved |
| Citations | Readable; Gemini lower coverage allowed if evidence-backed |
| Source frequency | Domain once per response |
| Watchlist | Context matches filters |
| Evidence | Drilldown: fact → observation → response → prompt → subject |
| Freshness | Last monitored + providers completed visible |
| Partial | Banner when N < 4 |

## Publication prerequisite

See [publication-gate.md](./publication-gate.md). Validation PASS + no P0/P1.
