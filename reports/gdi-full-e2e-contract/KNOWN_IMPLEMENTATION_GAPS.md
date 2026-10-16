# Known implementation gaps (honest)

This release wires the **contract, evaluator, coordinator mode, status CLI, and regressions**. It does **not** auto-complete Mayflower/SD-class delivery for a new hotel without authorized research and recorded local/production proofs.

## Gaps / still manual

1. **Segment coverage evidence overlay** — Operator (or upstream scorecard writer) must populate `full-e2e-evidence.json` / `ledger.fullE2e.segmentCoverage` with applicability, execution mode, coverage status, depth failures, and reuse satisfaction. Derivation from segment-research-plan defaults remaining applicable rows to `plan_only` until research executes.
2. **Local / production browser proofs** — Stages `local_delivery_verify` and `production_delivery_verify` record operator-supplied verification status; they do not drive a headless browser themselves in this PR. Auth-blocked checks must be `blocked`/`not_run`.
3. **Share-link minting** — Contract requires a verified hotel-scoped share link; minting still uses existing share paths. Do not commit active tokens.
4. **PDF layout validation** — Required where PDF is generated; automated clipping/pagination checks remain the existing PDF quality tests / manual confirmation when a PDF route runs.
5. **Automatic scorecard→evaluator bridge** — Paid-MS / buyer-contact artifacts can be read when present; full automatic mapping of every Mayflower/SD scorecard row into evaluator input is incremental. Until present, use evidence overlay.
6. **Active Renaissance / Tampa / ADP** — Status CLI evaluates existing run dirs read-only. This PR does not mutate their journals or start a competing run.
7. **No new hotel activation / no new paid batch** authorized by this task.

## Not gaps (already enforced)

- Association-only + plan-only applicable → cannot `COMPLETE`  
- Dry-run blocks paid + production writes even with authorize flags  
- Weekly refresh distinct from Full E2E  
- Contact-depth stage remains on the Full E2E coordinator path  
- Founder/Admin must consume `computedOverallStatus` / `full-e2e-completion.json`
