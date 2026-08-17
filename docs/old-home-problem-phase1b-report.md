# Old Home Problem — Phase 1B visual composition

**Date:** 2026-07-30  
**Page:** Old Home `/old-home` (`68108c2a063eeb5d1bd7ae90`)  
**Section:** `#about` stage `.oh-problem-stage`  
**Status:** Static visual rebuild complete — **stop for approval before Phase 2**

## Confirmations

| Check | Status |
|-------|--------|
| Timed animation added | **No** — no JS/timeline/scroll-trigger animation |
| Publish occurred | **No** — Designer draft only |
| Root `/` touched | **No** — Home `lastUpdated` remains `2026-07-30T13:46:47.675Z` |
| Visible copy in JS | **No** — all labels are Webflow text nodes |

## Snapshots

Composition previews (same markup/CSS as inserted; local full-page renders while Designer Bridge was intermittent):

1. **Desktop** — `docs/old-home-problem-phase1b-snapshots/desktop.png`
2. **Tablet (768)** — `docs/old-home-problem-phase1b-snapshots/tablet.png`
3. **Mobile (390)** — `docs/old-home-problem-phase1b-snapshots/mobile.png`

Local preview page: `docs/old-home-problem-phase1b-preview.html`  
Live Designer DOM verified: `data-scene` 1–6 present; editable strings include “Brand Team”, “Email Thread”, “Potential Value Stays Hidden.”

Reconnect Designer Bridge for canvas screenshots if needed:  
[Open Designer Bridge](https://mvp-deal-capture.design.webflow.com?app=dc8209c65e3ec02254d15275ca056539c89f6d15741893a0adf29ad6f381eb99)

## SVG / visual assets created

| Asset | Type | Where |
|-------|------|--------|
| Hotel / property mark | CSS geometric building + window grid (`.oh-p1b-hotel-mark`, `.oh-p1b-hotel-win`) | Scene 1 opportunity card |
| Path icons (Brand / Operator / Conversion / Repositioning / Capital / Independent) | Distinct geometric marks (`.oh-p1b-ico-*`) | Scene 1 branch paths |
| Branch hub + fan | Connecting path visual (`.oh-p1b-branch-hub`, `.oh-p1b-branch-fan`) | Scene 1 → futures |
| Role avatars | Head/body marks (`.oh-p1b-av-head`, `.oh-p1b-av-body`) | Scenes 2–3 |
| Duplicate brief chips | Dashed “Opportunity copy · v1” | Scene 2 lanes |
| Email window | Header bar + body lines (`.oh-p1b-art-email`) | Scene 3 Brand lane |
| Slide-deck thumbnail | Chart bars (`.oh-p1b-deck-thumb`) | Scene 3 Brand lane |
| PDF page + badge | Page surface + `PDF` badge | Scene 3 Operator lane |
| Attachment badge | Dashed attachment card | Scene 3 Operator / Capital |
| Advisor notes | Lined note card | Scene 3 Advisor lane |
| Call question bubble | Speech bubble (`.oh-p1b-bubble`) | Scene 3 Advisor lane |
| Spreadsheet fragment | Grid cells with warn/empty states | Scene 3 Capital lane |
| Misaligned comparison surface | Row/cell status system (complete / partial / missing / unclear / different basis / blank) | Scene 4 |
| Momentum rails | Progress tracks + fills (`.oh-p1b-rail`, `.oh-p1b-rail-fill.is-w*`) | Scene 5 |
| Ghost unexplored paths | Faint branch behind outcome | Scene 6 |
| Phase 1B CSS CodeBlocks | `#oh-p1b`, `#oh-p1b-2` | Stage embeds (Designer) |

Source files in repo: `docs/old-home-problem-phase1b-scene1.html` … `scene6.html`, `docs/old-home-problem-phase1b-visual-css.css`

## How each visual supports the story

1. **Central opportunity card** — One concrete hotel object (icon + name + four diligence dimensions), not a text panel.
2. **Branching futures** — Hub/fan + six distinct path icons show multiple credible routes from one asset.
3. **Participant lanes** — Avatars + duplicated brief chips show the same opportunity splitting into four workstreams.
4. **Document artifacts** — Email / PDF / sheet / deck / notes / bubble / attachment are different shapes (not pills), tied to lanes.
5. **Misaligned comparison** — Blank, partial, unclear, and different-basis cells make non-alignment obvious at a glance.
6. **Momentum tracks** — One advancing rail vs partial / awaiting / not tested — without implying the advancing path is wrong.
7. **Hidden-value outcome** — “Potential Value Stays Hidden.” with faint unused branches still visible.

## Change impact

- **Medium** — Old Home Problem stage read/visual composition only; no Airtable writes; no publish; no root page edit.
- **Rollback:** remove `data-scene` 1–6 blocks + `#oh-p1b` / `#oh-p1b-2` CodeBlocks from `.oh-problem-stage`; restore prior stage export if needed.

## Next (Phase 2 — only after approval)

Timed cinematic animation of these existing visual assets. Do not start until approved.
