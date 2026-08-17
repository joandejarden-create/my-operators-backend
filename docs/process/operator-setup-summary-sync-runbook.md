# Operator Setup Summary Sync Runbook

## Commands

```bash
# Phase B derived Active Countries
node scripts/operator-setup-phase-ab-apply.mjs --dry-run
node scripts/operator-setup-phase-ab-apply.mjs --apply --approve-operator-setup-phase-ab-writes

# Phase C researched summaries (batched)
node scripts/operator-setup-phase-c-apply.mjs --dry-run
node scripts/operator-setup-phase-c-apply.mjs --apply --approve-operator-setup-phase-c-writes --batches 1,2,3

# Pack writers (blank-fill preferred via Phase C orchestrator)
npm run operator-setup-profile-deepen -- --dry-run --operators <slug>
npm run operator-setup-website-content-apply -- --dry-run --operators <slug>
```

## Rules

- Setup is downstream of OE intel
- Blank-fill over overwrite
- Never write numeric portfolio % without approved rule
- Never write Fit bf_* as general truth
- Protect golden section rows (≥5 OP / ≥3 BR)
- No automatic event-driven writes yet
