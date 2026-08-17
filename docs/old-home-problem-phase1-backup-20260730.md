# Old Home Problem section — Phase 1 backup (2026-07-30)

Recoverable snapshot before the cinematic static rebuild on Old Home (`68108c2a063eeb5d1bd7ae90`).

## Scope lock

- Page: Old Home / `/old-home` — **not** root `/`
- Section: `section#about` (element `c4620de2-dd11-d6ea-4d1f-a1afe11b256d`)
- No intentional production publish required for Phase 1 Designer work (publish is a separate approval gate)

## Duplicate-ID resolution (Option B)

Hidden on-page `#about-backup-phase0` was **removed from the Old Home page DOM** so original IDs (`about-inner`, `hv-s1`–`s4`, etc.) cannot collide with the live section in Designer, Preview, or published HTML.

**External backup (authoritative restore source):**

| Artifact | Path |
|----------|------|
| Structured export | `docs/old-home-problem-phase0-backup-export-20260730.json` |
| This restore doc | `docs/old-home-problem-phase1-backup-20260730.md` |
| Runtime JS (CDN + repo) | `public/marketing/old-home-problem-storyboard.v20260729b.js` |
| Runtime CSS (CDN + repo) | `public/marketing/old-home-problem-storyboard.v20260729b.css` |

Confirmed absent from page DOM: `#about-backup-phase0`, `#about-inner`, `#hv-s1`.

### Restore steps

1. Open Old Home in Designer.
2. Rebuild or paste the pre-Phase-1 tree from the JSON export (copy + Owner Decision Path / `oh-about` / `oh-hv-*` classes).
3. Re-enable the storyboard script in site footer freeform (see below) **only** if restoring the dual-system runtime.
4. Remove or hide the cinematic Phase 1 shell (`oh-problem-*`) if replacing.

## Pre-rebuild hierarchy (historical `#about`)

```
section#about.oh-about
└── #about-inner.oh-about-inner
    ├── #about-copy
    │   ├── #about-badge.oh-section-badge  "The Problem"
    │   ├── #about-h2.oh-section-h2
    │   ├── #about-lead.oh-section-lead
    │   ├── #about-lead-2 (hidden, empty)
    │   └── #about-points.oh-about-points
    │       ├── #about-point-1 Fragmented outreach
    │       ├── #about-point-2 Slower comparison
    │       └── #about-point-3 Missed upside
    ├── #about-visual.oh-about-visual
    │   ├── #about-visual-label
    │   ├── #hv-s1 … #hv-s4 (Owner Decision Path)
    └── #about-close (hidden)
```

Element IDs (component page = `68108c2a063eeb5d1bd7ae90`):

| Role | element id |
|------|------------|
| section#about | `c4620de2-dd11-d6ea-4d1f-a1afe11b256d` |
| about-inner (historical) | `c4620de2-dd11-d6ea-4d1f-a1afe11b256c` |
| about-visual (historical) | `c4620de2-dd11-d6ea-4d1f-a1afe11b256b` |

## Classes (pre-rebuild)

`oh-about`, `oh-about-inner`, `oh-section-badge`, `oh-section-h2`, `oh-section-lead`, `oh-about-points`, `oh-about-point`, `oh-about-visual`, `oh-about-visual-label`, `oh-hv-s1`–`s4`, `oh-hv-chip`

## Runtime assets (pre-rebuild)

| Asset | URL |
|-------|-----|
| JS | `https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a78910fc7ccd49a1c9617_old-home-problem-storyboard.v20260729b.js` |
| CSS (loaded by JS) | `https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a774df6520a2f78161f69_old-home-problem-storyboard.v20260729b.css` |

### Footer freeform location

Site-wide custom code → **Footer** (Deal Capture MVP).

### Phase 1 disable method (applied 2026-07-30)

In **site footer freeform**, the storyboard script was wrapped in an HTML comment (not deleted):

```html
<!-- ohProblemStoryboard v20260729b: … -->
<!-- PHASE1 DISABLED 2026-07-30: cinematic Designer rebuild — do not delete; re-enable only to restore dual-system runtime
<script src="…/6a6a78910fc7ccd49a1c9617_old-home-problem-storyboard.v20260729b.js"></script>
-->
```

CDN JS/CSS files remain. CSS was only injected by that JS, so commenting the script also stops CSS overwrite. All other footer scripts were left unchanged.

**Re-enable:** remove the `PHASE1 DISABLED` comment wrapper so the `<script>` tag is live again.
