## Summary
- Restore **buyer-WHO** orchestration after Exa deep seed: persist `deepResult.after.opportunity` + `buyerWhoFallback` (previously dropped), attach live/mocked researcher when authorized, emit explicit `BUYER_WHO_RESEARCH` stage.
- Reject junk primary-contact strings; add demand-cluster **lineage reuse** across LIVE_NEW vs prior inventory IDs (materialize wiring).
- Stacks on / preserves PR #583 presentation + strict org-route gate. $0 cert with mocked providers; no STAGED overwrite; no paid calls.

## Dependency
**PR #583** (`fix/gdi-best-suggested-contact-restore-v1`) — CI green / MERGEABLE. Prefer merge #583 first, then this; or merge this stack into `main`.

## Test plan
- [x] `npm run test:gdi-buyer-who-e2e-recovery-v1` → CERTIFICATION_PASS
- [x] `npm run test:gdi-best-suggested-contact-restore-v1` → CERTIFICATION_PASS
- [x] `npm run dealality:ci-precheck` → PASS
- [x] Mocked execution traces written (`BUYER_WHO_RESEARCH` present)
- [x] Zero-dollar DH reassess plan only (no STAGED pointer rewrite)
- [ ] CI on this PR

## Founder report
`reports/gdi-buyer-who-e2e-recovery-v1/FOUNDER_REPORT.md`

## Governance
No Exa/Findymail/Surfe/Airtable/CURRENT/publish/deploy. Integration Controller owns merge. STOP for founder review before paid buyer-WHO.
