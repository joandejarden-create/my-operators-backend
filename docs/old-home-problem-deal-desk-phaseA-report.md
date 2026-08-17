# Deal Desk Phase A Report — Old Home Problem section

**Date:** 2026-07-30  
**Page:** Old Home `/old-home` (`68108c2a063eeb5d1bd7ae90`)  
**Section:** `section#about[data-oh-problem="deal-desk"]`  
**Status:** Phase A complete — waiting for visual approval before Phase B animation.

## 1. Verification results

| Check | Result |
|--------|--------|
| Webflow MCP authenticated | Pass |
| Designer Bridge | Connected for build; later timed out for final canvas snapshot |
| Old Home open / page ID | Pass |
| `section#about` editable | Pass |
| Element snapshots (during build) | Pass |
| Prior Phase 1B recoverable | Pass — backup JSON + phase1/phase0 docs retained |
| Prior Problem runtime disabled | Pass — site footer `PHASE1 DISABLED` comment; page footer BootGuard only |
| No production publish | Pass |

## 2. Backup status

| Artifact | Path |
|----------|------|
| Pre–Deal Desk `#about` tree | `docs/old-home-problem-deal-desk-phaseA-backup-20260730.json` |
| Backup notes | `docs/old-home-problem-deal-desk-phaseA-backup-20260730.md` |
| Phase 1 / Phase 0 backups | retained (not deleted) |

## 3. Native Webflow elements retained

Editable in Designer:

- Eyebrow: **The Problem** / **Manual. Fragmented. Hard to Compare.**
- H2 headline (two lines)
- Supporting lead paragraph
- Chapter markers `01 Fragmented Outreach` / `02 Inconsistent Responses` / `03 Missed Upside` with `data-problem-chapter`
- Accessible SR paragraph (updated Deal Desk summary)
- Closing principles block **hidden** (visibility false) — conclusion lives in embed

## 4. Embed element created

- Type: **HtmlEmbed**
- ID: `a64ef2f7-2f5f-ab92-9711-5f43f9eeb3fa`
- DOM id: `#oh-deal-desk-embed`
- Parent: `.oh-problem-stage`
- Contains root `.dealality-problem-desk` (no native eyebrow/headline inside embed)
- Phase 1B scenes removed from stage

## 5. CSS location

- Repo SoT: `public/marketing/old-home-problem-deal-desk.v1.css`
- Old Home page head: `#oh-tt` (testimonials) +  
  `<link id="oh-deal-desk" rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bcad5d2a0492c49da9bd1_oh-deal-desk-phase-a.css">`
- Scoped under `#about[data-oh-problem="deal-desk"]` / `.dealality-problem-desk`
- No CodeBlocks; no raw CSS as page text

## 6. JavaScript status

- File stub only: `public/marketing/old-home-problem-deal-desk.v1.js` (empty IIFE — **not loaded**)
- **No animation** in Phase A

## 7. SVGs / visual assets

- Inline SVG hotel mark (fictional, not confidential)
- Inline SVG avatars / status marks
- CSS-built artifact shapes (email, PDF, sheet, deck, notes, waveform, term sheet)

## 8–11. Screenshots

| Viewport | Path |
|----------|------|
| Desktop | `docs/old-home-problem-deal-desk-snapshots/desktop.png` |
| Tablet | `docs/old-home-problem-deal-desk-snapshots/tablet.png` |
| Mobile | `docs/old-home-problem-deal-desk-snapshots/mobile.png` |
| ~200% zoom | `docs/old-home-problem-deal-desk-snapshots/zoom-200.png` |

**Note:** Live Designer canvas snapshot timed out after edits. Screenshots are from markup/CSS identical to the Webflow embed + CDN stylesheet. Reopen Designer Bridge to confirm on-canvas:  
https://mvp-deal-capture.design.webflow.com?app=dc8209c65e3ec02254d15275ca056539c89f6d15741893a0adf29ad6f381eb99

## 12. Accessibility checks (Phase A)

- Native h2 outside embed: yes
- Visually hidden storyboard description in embed + native SR: yes
- Status text not colour-only (labels Complete/Partial/Missing/…): yes
- Reduced-motion / Replay: deferred to Phase B
- Zone development labels clipped via CSS

## 13–15. Confirmations

| Item | Status |
|------|--------|
| No animation added | Confirmed |
| No publish | Confirmed |
| Root `/` untouched | Confirmed — `lastUpdated` still `2026-07-30T13:46:47.675Z` |

## 16. Known limitations

1. Deal Desk CSS is loaded via Webflow CDN asset link (inline head payload exceeded MCP size). Repo CSS remains source of truth; re-upload if CSS changes.
2. Designer Bridge intermittent timeouts — re-verify live Preview after reconnecting Bridge.
3. PVL collapse behaviour deferred to Phase B.
4. Capital lane tertiary artifacts may hide on dense tablet breakpoints by design.
5. Closing principles native block is hidden, not deleted (recoverable).

---

**Stopped for Phase A visual approval.** Do not start Phase B until approved.
