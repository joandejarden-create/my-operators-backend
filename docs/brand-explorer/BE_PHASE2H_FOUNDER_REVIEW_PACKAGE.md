# Brand Explorer — Phase 2H Founder Review Package

**Branch:** `feat/be-phase2h-component-parity-v1`  
**Workspace:** `C:\Dev\dealality\ws-brand-explorer`  
**Compare URL:** http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1  
**Status:** **NOT founder-approved** · Curio / Ascend **must not** publish · **do not** scale population  
**Related PRs:** #457 (Phase 2G) OPEN · #453 (Phase 2F) OPEN / founder-rejected — preserved

---

## Verdict (honest)

| Dimension | Result |
|---|---|
| Paired-subsection architecture | Retained (one scroll; left/right aligned) |
| **Actual product component rendering** | **Restored** — Comparison Studio now injects `BrandExplorerAtelierFromApi` OE HTML |
| Content depth vs live Curio | **Still inferior** on many quantitative / dossier claims that exist only in production Presentation |
| Automated priority gates (Curio) | Pass (0 priority blocked) — **≠ founder approval** |
| Ascend reuse | Structure works; gallery imagery still incomplete |
| Ready to publish / populate | **NO** |

Phase 2H proves **rendering parity infrastructure** and **stops the worst generation failures** (stamp padding, empty-after-dedupe, brand-wide HPC as regional counts). It does **not** yet recreate the full owner-intelligence density of the live Curio profile.

---

## Deliverables

1. RCA — `docs/brand-explorer/BE_PHASE2H_CONTENT_DEPTH_RCA.md`
2. Depth composition pipeline — `lib/brand-explorer/be-phase2h-depth-composition-v1.js`
3. Substantive parity gates + founder regressions — `lib/brand-explorer/be-phase2h-substantive-parity-gates-v1.js`
4. Shared atelier exports (`renderTabInnerHtml`, `extractSubsectionHtml`) — `public/js/brand-explorer-atelier-from-api.js`
5. Comparison Studio product-component mode — `public/brand-explorer-compare.html` + `brand-explorer-compare-studio.js`
6. Footprint SoT repair — no brand-wide HPC in `footprint.region.*`; `aggregateOpenPipelineCounts.byRegion`
7. Candidates — `public/brand-explorer-phase2e-preview/*-phase2h-candidate.json`
8. Registers — `reports/brand-explorer/phase2h-parity/`
9. Screenshots — `reports/brand-explorer/phase2h-parity/screenshots/`
10. This founder package

---

## Priority subsection disposition (Curio)

| Subsection | Visual / component parity | Content vs live | Disposition |
|---|---|---|---|
| Why Value Is Strongest | **Equivalent structure** (`oe-cluster` + bullets both sides) | Inferior — missing live metrics (e.g. open-count / loyalty-mix specifics) | **Inferior content** · improved vs 2G emptiness |
| Operating Model | **Equivalent** (KV grids / section titles) | Partial — named leaves restored; some production leaf depth still richer | **Improved / needs founder review** |
| Loyalty Architecture | **Equivalent shell** (KPI row, ecosystem, owner lens, implications) | Inferior — right lacks dated program proof cards / scale narrative density | **Inferior content** · structure restored |
| Geographic Footprint | **Equivalent region cards** | Improved SoT (no brand totals as regions) but thinner than live corridor narratives | **Improved SoT** · content still thinner |
| Brand Materials | Product gallery/file components available | Case-study dossiers not fully reconstructed from evidence pack | **Unvalidated / incomplete** |

Ascend: same renderer path; gallery fails automated imagery coverage — **blocked for materials.gallery**.

---

## Remaining differences (do not claim success)

1. Live Curio still carries richer quantitative owner claims not present in the authorized evidence pack — Phase 2H correctly **does not copy** Presentation bodies.
2. Key Differentiators / some card grids can show empty secondary bullets when candidate bodies are short.
3. Growth / momentum cards need steward press + opening research — stamps removed rather than invented.
4. Exact pixel parity with customer `/app` chrome is not claimed; OE panel CSS is shared, compare chrome remains internal.

---

## How to review

1. Open http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1  
2. Confirm badge: **Phase 2H · Product components**  
3. Walk Overview → Why Value, Ops, Loyalty, Footprint, Materials  
4. Toggle **Item-by-item** only when inspecting collection diffs  
5. Do **not** treat automated scorecards as approval  

Preview PID (typical): `node server.js` on **8082** with `DEALALITY_ALLOW_WORKTREE_SERVER=1`.

---

## STOP

- No publication of Curio or Ascend  
- No mass brand population  
- No production Airtable writes  
- Await founder review before expanding population or claiming parity complete  

**FOUNDER_APPROVED = false**
