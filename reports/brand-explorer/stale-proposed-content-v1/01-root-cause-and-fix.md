# Stale Proposed Content — Root Cause & Fix

## Founder defects (8080)

URL: `http://localhost:8080/app#/brand-explorer-combined?beInternalPreview=1`

1. `Commercial Platform And Lobby Experience` (must be `&`)
2. Why Value bullets without bold `Lead: explanation`
3. Not seeing finalized proposed content

## Runtime before fix

| Item | Value |
|------|--------|
| Port 8080 PID | foreign inline `import('./server.js')` (often Integration cwd) |
| Git tip on disk | `1b4ff3ad7` (PR #478) |
| Served atelier JS | **stale** (315196 bytes) vs fixed (317760) — wrong process cwd / predated asset sync |
| Effective mode on founder URL | **CURRENT SAVED** (route-state soft-default) |

## Root cause

1. **Mode:** `brand-explorer-route-state.js` soft-defaulted `beProfileMode=saved` when the query omitted the param. `getProfileMode()` trusted that soft default, so founder preview rendered **Airtable CURRENT** (`And` heading).
2. **Why Value:** Cumulative candidate `overview.why_value` thin fragment dump replaced CURRENT colon-insight bullets.
3. **Scenario title:** Candidate overwrote founder-approved `Commercial Platform & Lobby Experience` with `Lifestyle repositioning`.
4. **False cert:** PR #478 gate passed on section presence without checking rendered ampersand headings or bold insight leads.
5. **Restart required:** Yes — 8080 was serving a pre-fix process; restarted from `my-operators-backend-canonical-main` after asset sync.

## Fix

- Soft-default staged Curio/Ascend internal preview → **PROPOSED UPDATE** (route-state + getProfileMode + popup mount)
- Assembler retains CURRENT `overview.why_value` when candidate lacks colon leads
- Atelier `formatWhyValueBulletHtml` → `<strong>Lead:</strong> explanation`
- Candidate scenario.2 title restored to `Commercial Platform & Lobby Experience`
- Cert gate fails on `And` heading / missing bold leads / thin fragments
- Lesson `BE-QL-STALE-PROPOSED-CONTENT-001`; prior FULL cert **invalidated** then re-run

## After (8080 verified)

| Check | Result |
|-------|--------|
| Mode | `proposed` + cumulative |
| Content hash | `4f9ad87b193b261d80c30f942c39beee2feea72d1544265d6a58a7fe3ed17316` |
| Scenario 2 | `Commercial Platform & Lobby Experience` |
| Why Value | 5 bold colon leads |
| Screenshots | `screenshots/01-AFTER-overview-why-value-ampersand.png` |

## Classification

Re-certified after fix: see `../final-ten-tab-product-certification-v1/CERTIFICATION.json`
