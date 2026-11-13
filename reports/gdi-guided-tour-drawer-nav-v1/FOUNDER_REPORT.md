# GDI Guided Tour — Drawer Next/Back Fix V1.1e

**PR:** [#235](https://github.com/joandejarden-create/my-operators-backend/pull/235)  
**Branch:** `fix/gdi-guided-tour-drawer-next-v1` (reconciled onto `main` @ `ba5d3b780`)  
**Tested tip:** `1dd78efaaf30b8fb1f780b774eaf4db9a8f07e28`
**Policy:** Do not merge or deploy until founder authorization. Does not overwrite PR #234 semantic remediation.

## Reconciliation vs merged #234

| Check | Result |
|---|---|
| Base | `origin/main` = `ba5d3b78053d853f0e3436b5a5a38794e5710897` (#233 + #234) |
| Unique commit vs main | Single tour-fix commit (no duplicate semantic remediation diff) |
| Three-dot file set | Tour/engine/CSS/HTML cache-bust + browser suite + this report only |
| #234 presentation remediations | Untouched |

Stale `*-FAIL.png` screenshots from earlier failed attempts were removed after a clean re-run.

## Root cause

Native Opportunity Details uses `<dialog>.showModal()`. The guided tour coachmark uses `popover="manual"` + `showPopover()`.

**Modal dialogs win hit-testing over popovers.** Coachmarks stayed visible, but `document.elementFromPoint` at Next/Back returned `DIALOG.gdi-drawer`. Clicks never reached tour controls.

Fix: demote the tour-owned drawer to non-modal `show()` for the duration of the tour; repair pointer capture; queue rapid Next/Back; keep muted actions hittable; prefer left placement for right-side drawer highlights.

## Verification (re-run on reconciled tip)

Server: worktree `my-operators-backend-pr235` @ `PORT=8095` with tour assets `v=gdi-howto-read-v1-1e` / `GDI_GUIDED_REPORT_TOUR_V1_1e`.

| Suite | Result |
|---|---|
| `npm run test:gdi-guided-tour-drawer-navigation-browser-v1` | **PASS** — Bethesda full, Wyndham full, Waterstone short/empty, Bethesda mobile |
| `npm run test:gdi-guided-report-tour-v1` | **PASS** (`V1_1e`) |
| `npm run test:gdi-regression-recovery-v1` | **PASS** |
| `npm run test:gdi-semantic-commercial-remediation-v1` | **PASS** |
| `npm run test:gdi-market-scope-toggle-v1` | **PASS** (7/7) |
| `npm run test:gdi-surfe-on-demand-contact-reveal-v1` | **PASS** |

Drawer steps 6–8 (Open Details / Evidence / Hotel Validation): `drawerOpen=true`, `drawerModal=false`, `nextHit.inTour=true` (BUTTON `data-dgt=next`). Next/Back/Exit exercised through finish. No hotel feedback writes from the tour. Outside the tour, Opportunity Details still uses normal `showModal()`.

Screenshots: `reports/gdi-guided-tour-drawer-nav-v1/screenshots/*-step-6of10.png` … `8of10.png`.

## Production readiness (prep only — not deployed)

- Existing Bethesda share token `gdisht_47c25d74c79216021fb36150` remains the canonical client-share mechanism (`group-demand-intelligence-share.html?share=gdishare.v1.…`).
- Publication/authorization controls unchanged by this PR.
- After merge: deploy resulting `main` via `dealality-production-release` (GHA control-plane with detached-HEAD fix), then re-verify the share URL on Railway.

## Merge-readiness recommendation

**READY TO MERGE** after founder review of this report.  
Next authorized sequence: merge #235 → GHA production release of new `main` tip → verify Bethesda external share link on Railway.
