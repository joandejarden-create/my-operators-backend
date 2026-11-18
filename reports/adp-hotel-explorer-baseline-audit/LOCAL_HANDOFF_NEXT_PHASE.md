# Local handoff — ADP / Hotel Explorer next phase

Use this file to continue **without rediscovering** the baseline.  
**Do not start implementation until founder approves Phase 1** (see audit §10).

---

## Exact Git identity

| Item | Value |
|---|---|
| Worktree path | `/agent/repos/my-operators-backend-adp-audit` |
| Branch | `cursor/adp-hotel-explorer-audit-1ed2` |
| Base / HEAD commit | `8533d7a4b7fb2a25e868ac444a6c15b41957619d` |
| Tracks | `origin/main` at audit time |
| Primary GDI checkout (DO NOT TOUCH) | `/agent/repos/my-operators-backend` · `main` · `c495897741ed869b7005db5ce584808bf0d79432` |

```bash
cd /agent/repos/my-operators-backend-adp-audit
git status -sb
git rev-parse HEAD
# Confirm primary still isolated:
git -C /agent/repos/my-operators-backend status -sb
git worktree list
```

---

## Audit deliverables (read first)

1. `reports/adp-hotel-explorer-baseline-audit/ADP_HOTEL_EXPLORER_BASELINE_AUDIT_V1.md` — full findings  
2. This handoff  
3. Test logs under `/opt/cursor/artifacts/adp-audit-*.log` (Cloud Agent machine; re-run commands below locally)

---

## Environment prerequisites

```bash
cd /agent/repos/my-operators-backend-adp-audit
npm ci
# Optional UI drawer test only:
# npx playwright install   # only if running drawer-visible gate
```

**Required for most free gates:** Node 22+, `npm ci` (dotenv, airtable, etc.).  
**Do not set** production publish / Airtable apply flags unless founder-authorized:

- `ADP_AIRTABLE_PUBLISH_APPLY` — leave unset/false  
- `ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0`  
- `ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH` — keep fail-closed for any publish experiments  
- Never run `railway up` / `deploy:production` without durability preflight + approval  

Share / HMAC secrets stay in deploy secrets — do not invent or print them.

---

## Relevant file paths (Phase 1 focus)

### Hotel Explorer ↔ Radar
- `public/js/hotel-explorer.js` — sets `__DEALALITY_HOTEL_EXPLORER_PRIMARY`
- `public/js/hotel-detail-panel.js` — delegates `open()` → HE when primary
- `public/css/hotel-explorer.css`, `public/css/hotel-detail-panel.css`
- `public/hotel-intelligence-golden-demo.html` — **reference host wiring**
- `public/hotel-explorer-share.html` — public packs (do not add hotels without approval)
- `public/deal-capture-radar-with-ranked-list.html` — has detail panel, **missing HE**
- `public/dealality-scout.html`, `public/radar-share.html` — **no HE / no detail panel scripts today**
- `lib/hotel-intelligence/factory/hotel-explorer-assembler.js` — Wave 0 VM

### Client-safe / attributes
- `lib/hotel-intelligence/dossier/client-safe/block-patterns.js`
- `lib/hotel-intelligence/dossier/client-safe/validate-client-safe.js`
- `lib/hotel-intelligence/adp-attributes/build-adp-hotel-attributes.js`
- `lib/ai-demand-positioning/intelligence/reality-gap.js`
- `docs/data-intelligence/hotel-intelligence-external-facing-report-copy.md`

### ADP OS / gates
- `lib/dealality-full-e2e-os/adp-full-e2e-workflow-v1.js`
- `lib/ai-demand-positioning/orchestration/adp-autonomous-hotel-full-e2e-v1.js`
- `scripts/test-adp-permanent-full-e2e-certification-v1.mjs` (**unwired**)
- `scripts/test-adp-e2e-persistence-contract-v1.mjs` (**unwired**)
- `scripts/test-adp-published-store-durability-v1.mjs` (**unwired**)
- `docs/production-deploy/PRODUCTION_BRANCH_DURABILITY.md`
- `config/production-required-assets.json`

### Bethesda (visibility frozen)
- `lib/ai-demand-positioning/execution/bethesda-marriott-foundation-v1.js`
- `data/ai-demand-positioning/published/adp_bethesda_marriott/`
- `config/group-demand-intelligence/hotels/recLuxvwwxID7U2B8.json`
- Census id: `recLuxvwwxID7U2B8` · ADP id: `adp_bethesda_marriott`

---

## Required commands (baseline re-verify)

```bash
cd /agent/repos/my-operators-backend-adp-audit

npm run assert:hotel-explorer-share-assets
npm run test:hotel-explorer-share-research-parity
npm run test:hotel-adp-attributes
npm run test:adp-certification-inventory-reconciliation-v1
npm run test:adp-global-certification-regression-framework-v1

# Known FAIL at audit time — re-check after fixes:
npm run test:ai-demand-positioning-published-foundation
npm run test:adp-customer-surface-contract

# Unwired permanence (invoke by path until npm aliases exist):
node scripts/test-adp-permanent-full-e2e-certification-v1.mjs
node scripts/test-adp-e2e-persistence-contract-v1.mjs
```

**Audit-time results:** share assets PASS · HE research parity PASS · hotel-adp-attributes PASS · cert inventory PASS · global cert framework PASS · published-foundation FAIL (Airtable field limit) · customer-surface-contract FAIL (missing baseline + BPP brand null).

---

## Proposed Phase 1 implementation checklist (after approval)

1. Confirm Radar host target with founder.  
2. Mirror golden-demo script/link order onto that host (minimal set that sets HE primary).  
3. Manual: open Golden Four census IDs → HE overlay, not compact-only.  
4. Add static test: host HTML includes `/js/hotel-explorer.js` and detail panel.  
5. Add `package.json` scripts for permanence/persistence/durability tests; update PR check suggest matrix.  
6. Search git history for `ADP_EXISTING_HOTEL_CERTIFICATION_BASELINE_V1.json`; restore if authoritative.  
7. **Do not** add Bethesda to `SHARE_PACKS` or issue new share tokens.  
8. **Do not** merge/deploy/Airtable-apply without separate approval.  
9. Keep work in this ADP worktree; never switch/stash primary GDI checkout.

---

## Unresolved questions

See audit §12. Blocking for Phase 1 start: **canonical Radar host** + confirmation that **no customer visibility changes** are intended in Phase 1.

---

## Isolation rules (non-negotiable)

- Work only in `/agent/repos/my-operators-backend-adp-audit`
- Do not modify `/agent/repos/my-operators-backend` (GDI chat)
- Do not create extra worktrees/branches unless founder approves
- No merge, deploy, production Airtable, or customer visibility changes without explicit approval
