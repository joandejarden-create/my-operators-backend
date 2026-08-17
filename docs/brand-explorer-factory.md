# Brand Explorer Factory

Semi-automated pipeline to move Choice P1 brands toward **L2 Blu parity**, starting from the existing Choice pipeline + restore scripts.

**Gold standards:** Radisson Blu (Choice), Kimpton, Curio — see `docs/choice-brand-explorer-completion-runbook.md`.

---

## Commands

```bash
# List manifest + P1 queue
npm run brand-explorer:factory -- --list

# Dry-run QA on entire P1 queue (no Airtable writes)
npm run brand-explorer:factory -- --queue p1 --qa-only

# Full factory dry-run (ensure fixtures + QA)
npm run brand-explorer:factory -- --queue p1

# Live apply + QA auto-loop (fills missing slots, runs L2 restore)
npm run brand-explorer:factory -- --queue p1 --apply --max-iterations 3

# Single brand
npm run brand-explorer:factory -- --brand "Park Plaza by Choice" --apply --with-images
```

---

## P1 queue (Airtable Brand Basics names)

1. Country Inn & Suites by Radisson
2. Park Inn by Choice
3. Park Plaza by Choice
4. Radisson Collection by Choice
5. Radisson Individuals by Choice

---

## What the factory does (per brand)

1. **Ensure full fixture** — creates `fixtures/brand-explorer-presentation-{slug}-full.json` if missing (Tier 1 or stub profile)
2. **Generate** — Tier 1 brands refresh via `generate-choice-tier1-explorer-full`
3. **Restore L2** — `restore-choice-tier1-brand-explorer.mjs --sync-full` (+ optional `--with-images`) when `--apply`
4. **QA loop** — gap audit via `lib/brand-explorer/brand-explorer-gap-audit.mjs`
5. **Auto-fix** — on failure with `--apply`, runs `apply-choice-explorer-presentation-gaps-batch` and retries restore

Reports: `reports/brand-explorer-factory-{slug}.json`, `reports/brand-explorer-factory-summary.json`

---

## Completion levels

| Level | Factory can achieve | Human still required |
|-------|---------------------|----------------------|
| **L1** | Yes — zero missing slot keys | Spot-check generic copy |
| **L2 Blu parity** | Partial — restore path + CALA overlays | Source PDFs, owner-voice case studies, gallery curation, economics verification |

Mark FPP tasks **Needs Review** after L1 pass + UI spot-check; not **Completed** until Joan approves L2.

---

## Parent registry (thin slice)

`lib/brand-explorer/brand-explorer-parent-registry.mjs` — CHI factory + IHG/Hilton gold references (Kimpton, Curio) for future generalization.

## Recent Momentum (required for every brand)

Openings/press card template is mandatory for active and future Brand Explorers — see `docs/data-intelligence/brand-explorer-section-pattern-parity.md` and `lib/partner-intelligence/brand-explorer-recent-momentum-contract.js`. New brands get a content pack from `brand-explorer-section-pattern-parity-content-_TEMPLATE.js`.

---

## Related

- `npm run choice-brand-explorer:pipeline` — phase-by-phase manual control
- `npm run choice-brand-explorer:manifest` — all 22 CHI brands
- `docs/partner-intelligence-repository-mvp-plan.md` — future staging/review/publish layer
