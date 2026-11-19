# Brand Explorer — Full Local Application Parity & Final Synchronization

**Verdict: PASS (renderer + runtime parity)** · **Airtable writes: 0** · **Publication applied: false**  
**Generated:** 2026-11-19T23:21Z (parity harness) · nav smoke + 8080 ownership repair same cycle

---

## 1. Current `origin/main` commit

`54ee3d974076123d4f0a9325965c2afdcde7e3e3`  
Includes:

- PR **#465** (`aa1d0b802…`) — Phase 2H Comparison Studio, founder standards, publication gates
- PR **#466** (`54ee3d974…`) — post-merge Curio dry-run audit + PR #465 completion report

No unmerged BE product feature branches remain for this cycle (parity harness PR opens next).

---

## 2. BE preview serving commit (port 8082)

| Field | Value |
|------|--------|
| Path | `C:\Dev\dealality\ws-brand-explorer` |
| Branch | `ws/brand-explorer` (pre-parity-branch tip = main) / feature branch for harness only |
| HEAD | `54ee3d974076123d4f0a9325965c2afdcde7e3e3` |
| PID | `18556` |
| Dirty | 0 at sync time |

URL A: http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1

---

## 3. Local app serving commit (port 8080)

| Field | Value |
|------|--------|
| Path | `C:\Dev\dealality\my-operators-backend-canonical-main` |
| Branch | `local/canonical-main-8080` |
| HEAD | `54ee3d974076123d4f0a9325965c2afdcde7e3e3` (**== origin/main**) |
| PID | `22816` |
| Flag | `DEALALITY_CANONICAL_CHECKOUT=1` |

**Before repair:** port 8080 was `C:\Dev\dealality\my-operators-backend` detached at `c448e452…` (**273 commits behind**, **no** Compare Studio assets). Dirty primary WIP preserved untouched (`SKIP_INTEGRATION_DIRTY`).

**Repair:** FF existing `my-operators-backend-canonical-main` worktree to `origin/main`; stop only outdated 8080 PID; restart from canonical-main with `PORT=8080`. Did **not** restart 8082 / ADP / GDI / HE services. Did **not** mutate primary dirty tree.

---

## 4. Exact runtime ownership (8080 / 8082)

| Port | Owner cwd | Commit | Role |
|------|-----------|--------|------|
| **8080** | `my-operators-backend-canonical-main` | `54ee3d974…` | Canonical local Dealality application |
| **8082** | `ws-brand-explorer` | `54ee3d974…` | BE Comparison Studio / preview |
| primary `my-operators-backend` | dirty / detached `c448e452…` | **not serving** | Preserved WIP only |

---

## 5. Feature flag & asset version comparison

| Asset | 8080 SHA256 | 8082 | Match |
|-------|-------------|------|-------|
| `brand-explorer-atelier-from-api.js` | `6df30920…` | same | YES |
| `brand-explorer-compare-studio.js` | `14cfa324…` | same | YES |
| `brand-explorer-atelier-oe-panels.css` | `a9733e0e…` | same | YES |
| `brand-explorer-gold-surface.css` | `9c3946a7…` | same | YES |
| `brand-explorer-combined.html` | `d9711060…` | same | YES |
| `brand-explorer-compare.html` | `88b7eeef…` | same | YES |
| `brand-explorer-renderer-parity.html` | `e5534440…` | same | YES |

Internal preview: `beInternalPreview=1` used for Gate1 / factory surfaces. No production publication activation flags enabled.

---

## 6. Ten-tab rendering parity (identical staged payload)

Harness: `npm run test:brand-explorer-local-app-parity -- --screenshots`  
Payload: `public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json`  
Viewport: 1440×900  
Mount: `/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=…` on **both** ports

| Tab | Result |
|-----|--------|
| Overview | PASS |
| Value to Owners | PASS |
| Operating Model | PASS |
| Owner Considerations | PASS |
| Commercial Engine | PASS |
| Economics & Obligations | PASS |
| Loyalty Program | PASS |
| Footprint & Growth | PASS |
| Brand Materials | PASS |
| Dealality Insight | PASS |

Screenshots: `reports/brand-explorer/local-app-parity-v1/screenshots/app-*.png` and `preview-*.png`.

---

## 7. Content parity for identical payloads

| Content class | Hash | Notes |
|---------------|------|-------|
| **Staged Gate1 payload** | `3bdbd3927e8bf7cc…` | Used for renderer parity (PASS) |
| **Live Airtable Presentation (8080)** | `9a9e8c2b6d204995…` | 225 blocks |
| **Live Airtable Presentation (8082)** | `9a9e8c2b6d204995…` | Identical to 8080 |

**Staged ≠ live saved** — expected until authorized writeback. Classified as **publication gap**, not renderer failure.

Customer combined route (live saved) shows all **10 unique** atelier tabs on both ports (desktop+mobile nav may duplicate nodes → 20 DOM nodes; unique ids = 10).

---

## 8. Outstanding differences & root causes

| Difference | Root cause | Action |
|------------|------------|--------|
| Staged Gate1 hash ≠ live Presentation hash | Candidate not written to Airtable | Gate 2 + scorecard + controlled writeback (no bypass) |
| Primary checkout still dirty/detached | Preservation WIP | Leave alone; 8080 served from canonical-main |
| Combined nav node count 20 | Desktop + mobile tab strips | Acceptable; unique tabs = 10 |
| API `footprint.open/pipeline` null in shallow summary | Metrics live under `footprint.formValues` / Presentation / HPC UI paths | UI Footprint tab shows Open/Pipeline/Region/Country/Archetype correctly |

No renderer regressions isolated to unmerged BE-only branches for this tip.

---

## 9. Browser screenshots (both environments)

Directory: `reports/brand-explorer/local-app-parity-v1/screenshots/`

- Per-tab staged parity: `app-atelier-*.png` / `preview-atelier-*.png`
- Live combined Footprint: `app-combined-footprint.png`
- View Recent Openings → Footprint: `app-combined-after-recent-openings.png`
- Compare Studio: `app-compare-studio.png`, `preview-compare-studio.png`

---

## 10. Airtable publication status (preserved)

- `airtableWrites`: **0**
- `publicationApplied`: **false**
- Gate 1 visual: approved artifact present
- Gate 2 / scorecard / `productionWritesAuthorized`: **still blocking** production apply
- Dry-run audits preserved under `reports/brand-explorer/comparison-studio/publication-runs|snapshots/`

---

## 11. Required fix PR

| Item | Detail |
|------|--------|
| Purpose | Add `test:brand-explorer-local-app-parity` + this founder report/evidence |
| Product code change | None required for runtime parity after 8080 ownership repair |
| Runtime ops | Canonical-main FF + 8080 restart (local only; not a GitHub code fix) |

---

## 12. Verified local application URLs

**Canonical local app (8080)**

- http://127.0.0.1:8080/app
- http://127.0.0.1:8080/app#/brand-explorer-combined
- http://127.0.0.1:8080/brand-explorer-combined.html?id=receQkxgjlezsc1xg&beInternalPreview=1
- http://127.0.0.1:8080/brand-explorer-compare.html?beInternalPreview=1
- http://127.0.0.1:8080/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=curio-gate1-renderer-parity-payload.json

**BE preview (8082)**

- http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1
- http://127.0.0.1:8082/brand-explorer-combined.html?id=receQkxgjlezsc1xg&beInternalPreview=1
- http://127.0.0.1:8082/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=curio-gate1-renderer-parity-payload.json

---

## Navigation / HPC smoke (8080 combined)

- Brand selection / Curio load: OK  
- All ten tabs: OK  
- **View Recent Openings** → `data-be-jump-atelier-tab=atelier-footprint` → Footprint panel: OK  
- Footprint UI: Open, Pipeline, Region, Country, Asset Archetype language present  
- Compare Studio: 10 tabs, paired shell present  
- Reload: assets served from synced tip  

---

## Automated regression

```bash
npm run test:brand-explorer-local-app-parity
npm run test:brand-explorer-local-app-parity -- --screenshots
```

Script: `scripts/test-brand-explorer-local-app-parity-v1.mjs`  
JSON: `reports/brand-explorer/local-app-parity-v1/parity-report.json`

**Harness verdict: PASS**

---

## Success criterion assessment

For **identical staged content + viewport**, local app (8080) and BE preview (8082) are visually/structurally equivalent (ten tabs, components, styles, assets).

Legitimate **saved Airtable ≠ staged candidate** difference is explicitly identified and not papered over with unauthorized writeback.

Founder can proceed to **Curio Gate 2 + controlled Airtable writeback** knowing the finished renderer is on the canonical local application.
