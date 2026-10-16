# GDI pre-batch readiness decision

**Decision date:** 2026-10-16  
**Workroot:** `C:\Dev\dealality\my-operators-backend`  
**Code baseline verified:** `origin/main` @ `11379922`  
**Production deploy verified:** SUCCESS @ same SHA (`reports/deployments/latest-production.json`)  
**Defect-fix branch (this review):** `fix/gdi-pre-batch-readiness-defects` — merge + redeploy required before treating weekly/import fixes as live.

Six-hotel batch: **not started** (queued only).

---

## A. Is the process ready for one additional hotel E2E?

**Yes — conditionally**, after the defect-fix PR merges and deploys to clean main.

Customer research/publication path for active hotels is regression-green (qualification, contact depth, drawers, weekly isolation, inventory). First controlled E2E hotel: **Renaissance New York Times Square** (`recG66DQJKP2c0UNh`). Do not activate the six-hotel batch; do not start hotels missing `config/group-demand-intelligence/hotels/*.json`.

Authenticated AI Admin interaction remains **NOT RUN** — that is an **admin-operations** gap, not a research/publication blocker for one hotel E2E.

---

## B. What was newly fixed versus already completed?

### Already completed (accepted; not reapplied)

| Track | Evidence |
|-------|----------|
| A. Qualification/publication repair | PRs #101–#103 / b571d945 lineage; `authorizeProspectPublicationPromotion` + `preservePublicationMetadata`; nine restored records facing with `REVIEWED_PACKAGE_V1` |
| B. AI Admin More-menu portal | PR #104 @ `8148eed9`; automated table tests PASS; ADP Action Plan = overflow-shell only |
| C. Contact-depth | PR #99 closed; superseded by #106 + #107; 13/13 LMC/planner contacts facing; org-route display guards |

### Newly fixed this review

1. Weekly `invokeWeeklyDownstreamWorkflow` `opts is not defined` → Surfe/contact-depth params destructured.  
2. Reviewed-prospect import overwrite revived unexplained fit **55** → cleared unless scored.  
3. Reviewed-prospect unit fixture travel-need evidence (test drift after qualification integrity).  
4. Buyer-enrichment source assertion aligned to fixed weekly wiring.

---

## C. What was actually verified locally and in production?

### Local

- Focused GDI regressions: all PASS (see `FOCUSED_TEST_MATRIX.json` / `PASS_FAIL_MATRIX.json`).
- Weekly isolation: two cycles + future clock PASS after weekly fix.
- Drawer import contract: unexplained 55 cleared PASS after import fix.
- Inventory snapshot (Airtable via prior read): Mayflower facing **38**, SD facing **51**; nine restored **9/9** facing; thirteen contacts **13/13** facing; clubvip leaks **0** (`INVENTORY_AND_REGRESSION.json`).
- Six-hotel **read-only** config preflight (`SIX_HOTEL_PREFLIGHT.json`).
- Budget ledger read (`BUDGET_VERIFICATION.json`).

### Production

- Active deployment SHA `11379922` SUCCESS with asset/smoke/share gates PASS.
- Record-level facing inventory and nine/thirteen read-backs already proven against production Airtable in the contact-depth closeout (reused; not rewritten).
- Authenticated AI Admin live interaction: **NOT RUN**.
- Defect fixes in this branch are **not** on production until merge/deploy.

---

## D. Are contact depth, qualification, fit, complete drawers, and persistence standard stages?

**Yes.** Shared stages on the coordinator / weekly downstream path:

- use-case qualification / publication eligibility (filter before sanitize)
- hotel fit assessment
- `buyer_contact_enrichment` (mandatory contact depth; Surfe opt-in only)
- contact display surface → card/drawer contracts
- canonical persistence → DTO → customer renderer
- weekly refresh invokes the same downstream adapters; dry-run keeps unattended publish OFF

No bespoke hotel packages required for these stages on the active two hotels.

---

## E. Does authenticated AI Admin interaction pass?

**NOT RUN.** No legitimate Memberstack admin session was available.

### Manual check (short)

1. Sign in as a real Memberstack admin on production AI Admin.  
2. On **Reviews**, **GDI Reports**, and **Report Archive**: open More on first/middle/last rows; scroll; narrow + 200% zoom; Escape/outside close; focus return; one menu; open an existing report (no generate/archive/research).  
3. On **ADP Action Plan**: confirm overflow-shell only — do not expect full shared-menu conversion.  
4. Confirm disabled actions stay disabled; sorting/filter does not swap row identity.

---

## F. Which manual steps remain?

1. Merge `fix/gdi-pre-batch-readiness-defects` via normal PR; deploy clean main; confirm production SHA includes the weekly/import fixes.  
2. Authenticated AI Admin manual check (section E).  
3. Before paid E2E spend: reconcile budget ledger empty entries vs prior ~$4.50 remaining note.  
4. For any queued hotel other than Renaissance NY TS / Waterstone: add canonical hotel config JSON before research.  
5. Keep six-hotel execution prompt queued — do not activate.

---

## G. Which hotel should run first?

**Renaissance New York Times Square Hotel** — `recG66DQJKP2c0UNh`

Reason: only queued urban meetings hotel with complete canonical config, rooms/meeting inventory, market territory, and existing GDI data footprint (`opportunities.json` / `profile.json` / `runs`) without inventing missing Marriott configs for Falls Church / Arlington / Charlotte / Tampa. Waterstone is config-ready but is a resort/marina profile; less comparable to the Mayflower/Bethesda association+LMC proof path for the first controlled E2E.

---

## H. Remaining issues by capability

| Issue | Class |
|-------|--------|
| Weekly opts + import fit-55 defects (this PR) | **Research/publication correctness** — fixed in branch; deploy pending |
| Authenticated AI Admin interaction | **Admin-operations blocker** for claiming menu prod UX; not an E2E research blocker |
| Budget ledger shows $11 remaining / 0 entries vs prior ~$4.50 note | **Nonblocking for unpaid E2E**; **blocker for paid spend** until reconciled |
| Falls Church / Arlington Capital View / Charlotte / Tampa Airport missing hotel configs | **Research blocker for those hotels only** |
| Missing named buyers / honest research gaps on some accounts | **Nonblocking** — do not endless-review |
| ADP Action Plan not on full shared menu | **Nonblocking** — documented intentional partial scope |

**Do not label the entire system blocked.** Research/publication for one additional hotel E2E is ready after this PR deploys; admin menu prod interaction and four missing configs are scoped gaps.

---

## Inventory reconciliation (observational)

| Hotel | Facing | Reviewed-visible | Notes |
|-------|--------|------------------|-------|
| Mayflower `rec0eTEnCqlsGJJt9` | 38 | 33 | Matches last reported facing; not hardcoded |
| Wyndham SD Bayside `recnPB3Q02NO8Ma1o` | 51 | 48 | Matches last reported facing |

Nine restored + thirteen LMC/planner: all found, facing, `REVIEWED_PACKAGE_V1`. No rewrite performed.

## Weekly / resume

Isolated weekly proof PASS after fix: unchanged records not new discoveries; dry-run; Surfe default off; contact-depth stage executable without `opts` crash.

## AI Admin

Code/automated: PASS. Authenticated production: NOT RUN (manual steps above).
