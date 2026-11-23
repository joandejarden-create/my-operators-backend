# GDI — Restore Canonical E2E → Staging → Production Workflow

**Status:** `$0` audit + staging-default restore complete · **STOP** before paid research / Findymail / Airtable / publication  
**Contract:** `gdi-restore-canonical-staged-workflow-v1`  
**Branch:** `feat/gdi-restore-canonical-staged-workflow-v1`  
**Worktree:** `C:\Users\joand\AppData\Local\Temp\dealality-gdi-restore-staged-workflow-v1-20261123`  
**Safety:** No paid Exa · No Findymail/Surfe · No Airtable production writes · No customer publication · No production deployment

---

## 1. Original E2E / staging / publication contract

Canonical founder-approved flow:

1. Full E2E research (discovery → dedupe → deep research → verified demand → contact ID/verification → commercial qualification)
2. **Finalized opportunity package** written as a hotel `STAGED` publication package
3. Local GDI defaults to **STAGED** for founder visual review (`resolveFounderInventoryMode` → `"staged"` on localhost)
4. Explicit founder publication approval
5. Promote approved STAGED snapshot → `CURRENT` / production inventory (content-preserving)
6. Research store remains internal (`?inventory=research` / `?live=1`) — diagnostic only

Authority:

- Full E2E template step: authenticated GDI loads **STAGED** on localhost (`lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js`)
- Fail-closed API when STAGED missing: `STAGED_PUBLICATION_PREVIEW_UNAVAILABLE` (HTTP 409) — no silent fallback to research / CURRENT / other hotels
- Share / customer path never uses localhost research bypass

---

## 2. Exact regression root causes

| # | Root cause | Effect |
|---|------------|--------|
| A | Localhost inventory default flipped to **Research** (`if (isLocalFounderHost()) return "research"`) | Founder local UI showed raw discovery inventories instead of finalized STAGED packages |
| B | Founder URLs used `inventory=research&live=1` as the working view | Reinforced research-as-primary habit in browser sessions |
| C | Design Hotels Full E2E pilot never produced `STAGED.json` packages | Even a correct STAGED default correctly shows empty — research rows (82/50/56) are **inputs**, not staged opportunities |
| D | Deep research capped at ~2–4 opps/hotel with **0** named contacts + **0** customer narratives | Cards (when research-viewed) dominated by “Contact Research Required”; nothing meets finalized-stage gate |

---

## 3. Recent changes responsible

- Commit **`1570d69a5`** — “Restore localhost Research Preview without requiring STAGED” — inverted the Full E2E localhost default to Research.
- Design Hotels Live Research Pilot + visibility work — correctly materialised **research** inventories and local visibility, but did **not** generate finalized STAGED packages.
- Customer UI Regression / Repeatable Processing PRs preserved publication safeguards, but left the localhost default conflict in place until this restore.

---

## 4. Corrected architecture and implementation plan

**Shipped in this PR (workflow restore only):**

1. `public/js/group-demand-intelligence/app.js` — localhost default restored to `"staged"`; Research remains explicit-only (`?inventory=research` / `?live=1`).
2. Cache bust `app.js?v=gdi-restore-canonical-staged-workflow-v1`.
3. Gates: `scripts/test-gdi-restore-canonical-staged-workflow-v1.mjs` + aligned founder/UI regression tests.
4. Empty STAGED remains fail-closed (API 409 + UI empty state with explicit **View Research** admin path — no auto-stage, no research substitute).

**Not in this PR (requires separate founder GO):**

5. Full E2E commercial run for Design Hotels to produce ≥25 finalized/hotel → write STAGED packages.
6. Contact completeness (named decision-maker / qualifying commercial route) before stage.
7. Staged→CURRENT promotion only after explicit founder approval.
8. Findymail / Surfe remain disabled.

---

## 5. Verified contact-completeness requirements

Before an opportunity may enter STAGED:

- Named buyer or commercial decision-maker **or** explicitly approved qualifying commercial route
- Verified name, professional title, organization; relevance to the opportunity
- Source evidence preserved
- Public professional contact where supported; org contact routes OK when commercially actionable
- Personal email **not** required when named person + commercial route already verified
- No invented contacts; no org-name-only placeholders; no “Contact Research Required” as a finalized sales contact
- Sales Ready standards unchanged
- Findymail / Surfe off unless separately authorized

Unresolved contact research → remains in internal research continuation.

---

## 6. Finalized-stage eligibility rules

A staged opportunity must include all of:

1. Verified demand generator + organization  
2. Evidence-supported future demand  
3. Credible timing / demand dates  
4. Plausible room-night or meeting demand  
5. Verified hotel-market + capability fit  
6. Competitor sourcing assessment  
7. Verified decision-maker or qualifying commercial route  
8. Supporting evidence URLs  
9. Specific recommended commercial action  
10. Accurate opportunity classification  
11. Completed customer-facing narrative  
12. No unresolved placeholder / misleading research language  

Machine gate used in audit: deep research + named contact + evidence + mechanism + future timing + customer narrative. Speculative / volume-only staging is prohibited.

---

## 7. Current finalized-ready counts (Design Hotels)

Source: `FINALIZED_READY_AUDIT.json` (visibility research inventories).

| Hotel | Research inputs | Deep | Contact complete | Narrative | **Finalized ready** | Gap to min 25 |
|-------|-----------------|------|------------------|-----------|---------------------|---------------|
| Condesa DF `rece0NdV9FCnaFlVS` | 82 | 4 | 0 | 0 | **0** | 25 |
| LaLuna `recaokE8psIbuVeuH` | 50 | 2 | 0 | 0 | **0** | 25 |
| Memmo Príncipe Real `rec1t35e3sPerzxYu` | 56 | 3 | 0 | 0 | **0** | 25 |

**Do not stage the 188 research rows.** Approximate triage: needs-deep 46/25/34; exclude-weak 32/23/19; deep-but-contact-incomplete 4/2/3.

STAGED packages: **none** for all three hotels (`hasStagedPublication: false`).

---

## 8. Additional research required to reach 25 / hotel

Per hotel (evidence-backed, not volume padding):

1. Expand deep research across commercially promising candidates (well beyond the 4-opp certification pilot).
2. Complete contact identification/verification for each finalization candidate.
3. Write customer-facing narratives + commercial actions.
4. Cross-segment dedupe; drop weak/speculative rows.
5. Materialize **one STAGED package per hotel** only from finalized-ready rows.
6. Founder visual review on localhost STAGED → explicit publish GO → CURRENT.

Preferred band 30–40/hotel where evidence supports; floor 25.

---

## 9. Exact paid research budget proposal

**STOP — awaiting founder GO. No spend authorized by this restore.**

Recommended envelope (from Design Hotels Full E2E Commercial Research V2 preflight; unchanged intent):

| Scope | Cap |
|-------|-----|
| Aggregate Exa | **$54** max |
| Per-hotel Exa | **$18** max |
| Findymail / Surfe | **$0** (disabled) |
| Airtable production writes | **None** until separate GO |
| Publication / deploy | **None** until founder STAGED approval |

Prior Design Hotels live pilot actual spend was ~$3.20 against a $12 max — useful for discovery only; **Sales Ready 0 / Competitive 0 / Finalized 0**.

---

## 10. Staged-to-published parity tests

Preserved / re-verified:

- Bethesda `recLuxvwwxID7U2B8`: STAGED + CURRENT both present; same publication version identity (`gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2`)
- Deterministic package resolver: `resolvePreviewOpportunitiesFromPublicationPackage({ preferStaged: true })`
- Share path cannot load live research default
- Design Hotels: staged resolve `ok:false` (fail-closed) — no fabricated package
- Promotion contract unchanged: approved STAGED snapshot → CURRENT without rewriting opportunity content; subsequent staging edits do **not** auto-sync into production

---

## 11. Local browser verification

| Check | Result |
|-------|--------|
| Restore server `:8098` serves `app.js?v=gdi-restore-canonical-staged-workflow-v1` | PASS |
| Bethesda localhost default (no inventory param) → STAGED inventory, **67** opportunities | PASS |
| Design Hotels STAGED API (`?preview=staged`) → HTTP **409** `STAGED_PUBLICATION_PREVIEW_UNAVAILABLE` | PASS |
| Design Hotels UI default → **0** cards; empty-state + **View Research** (no 82/50/56 dump; no Contact Research Required) | PASS |
| Cambridge Beaches `recIwaP1etgx2g9nA` STAGED/CURRENT absent → 0 opportunities (publication controls undisturbed) | PASS |
| Existing published inventories not modified | PASS |
| `:8080` still on pre-restore marker `gdi-customer-ui-regression-v1-1` until merge + process restart | NOTED |

Evidence: `screenshots/condesa-staged-empty.png`, `TEST_RESULTS.json`.

---

## 12. PR / CI / integration status

| Item | Value |
|------|-------|
| PR | [#579](https://github.com/joandejarden-create/my-operators-backend/pull/579) |
| Status | **OPEN** |
| Head SHA | `fd7353348` |
| CI precheck | PASS (gitleaks + concurrency) |
| Merge | Integration Controller only |
| `ws-gdi` | Preserved dirty on unrelated Surfe/contact branch — **not switched** |
| Temp WT | `dealality-gdi-restore-staged-workflow-v1-20261123` |

### Final cycle state (this task)

**`AWAITING_INTEGRATION_CONTROLLER`**

After merge: run `dealality:post-merge-sync --stream gdi`, return `ws/gdi` to `origin/main`, then founder may authorize paid Full E2E for Design Hotels STAGED package generation.
