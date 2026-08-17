# Brand Explorer Wave 12 — Stages 9–10 Post-Release Validation

Generated: 2026-07-24 (local apply session)

## Stage outcomes

| Stage | Mode | Result |
| --- | --- | --- |
| 9 status-promotion | apply | **PASS** — 12/12 Brand Status `Under Review` → `Active` |
| 10 public-release | apply | **PASS** — 12/12 release fields + intentional restore registry |

### Convention
Protected-27 freeze used **Active** (0 Live). Wave 12 promoted to **Active**.

### Writes performed
- **Stage 9 only:** `Brand Status` = `Active` on Wave 12 Basics records
- **Stage 10 only:** `Active Profile Approved`, `Ready for Active Profile`, `Active Profile Approved Date`, `Founder Visual Review Pass` + `data/brand-explorer-public-restore-intentional.json` (+12 slugs)

### Forbidden surfaces untouched
Company Validated · Company Validation Date · Source Library status · Registry approval/status · owner-facing content · images · protected prior 27 · Radisson Collection

## Acceptance checks

| Check | Result |
| --- | --- |
| Active universe count | **39** |
| Wave 12 in Active universe | **12/12** |
| PVQL `--public-full-only` | **PASS** — overallPass=true, 39/39 public-full, 39/39 lockPass |
| shouldRenderFullProfile | **39/39 true** |
| displayState | **39/39 `active_profile_ready`** |
| Recent momentum evidence quality test | **PASS** |
| Mandatory release gates | **PASS** |
| Brand Explorer OS release-readiness | **PASS** (read-only) |
| Active-universe SoT dry-run | Count=**39**; tool still warns “not 27” until baseline-39 revises SoT |
| 24-tab section quality audit | Audited **39**; freeze decision **do_not_freeze** (see below) |
| Radisson Collection | Remains non-Active / excluded from public-full Active set |

## Freeze-ready gap (content — out of Stage 9–10 scope)

Quality audit recommendation counts: `approve_for_baseline_freeze` **36**, `remediation_required` **2**, `approve_after_minor_cleanup` **1**.

Wave 12 exceptions:
1. **bunkhouse-hotels** — `remediation_required` (major wrong-brand hits for `World of Hyatt` in Positioning / scenarios — real copy issue)
2. **moxy-hotels** — `remediation_required` (scanner flags `Marriott Bonvoy`; Moxy is Marriott family — allowlist gap / likely false positive)
3. **voco-hotels** — `approve_after_minor_cleanup` (thin/missing optional slots + caption minors)

**Do not freeze baseline-39** until these are cleaned (or scanner allowlists adjusted for Moxy). Public release itself is complete.

## Artifacts
- `reports/brand-explorer-wave12-status-promotion.{json,md}`
- `reports/brand-explorer-wave12-public-release.{json,md}`
- `reports/brand-explorer-public-visibility-quality-lock.json`
- `reports/brand-explorer-24-tab-section-quality-audit.json`
- `data/brand-explorer-public-restore-intentional.json` (29 slugs)
