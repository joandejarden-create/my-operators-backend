# Phase 1.3 — Merge readiness (PR #364)

**STOP FOR FOUNDER REVIEW — no merge/deploy performed.**

## Git

| Item | Value |
|------|-------|
| Branch | `cursor/adp-hotel-explorer-audit-1ed2` |
| Base commit | `dfc93df33d662aede60d01cf5e290eb211eaf4a2` |
| Primary GDI checkout | Untouched |
| Port 8080 | Untouched |

### Uncommitted changes (reviewable)

**Preserved Phase 1.2**

- `.gitattributes` — LF fixture rule  
- `api/brand-presence.js` — identity aliases (+ HPC by-id fallback)  
- `reports/.../phase-1.2/` — validation evidence  

**Phase 1.3**

- `lib/hotel-intelligence/radar-internal-preview-hotels-v1.js`  
- `api/brand-presence-internal-preview.js`  
- `server.js` — authenticated internal-preview route  
- `middleware/requireDealalityUser.js` — local DEV_AUTH_BYPASS admin fallback  
- `public/brand-presence-mapping.js` — authorized merge  
- `public/deal-capture-radar-with-ranked-list.html` — auth-helper + cache bust  
- `package.json` — new gates in `test:adp-phase1-radar-he-gates`  
- `scripts/test-radar-app-opportunity-radar-he-wiring-v1.mjs`  
- `scripts/test-radar-internal-preview-hotels-v1.mjs`  
- `reports/.../phase-1.3/`  

## What works

1. Canonical `#/opportunity-radar` embeds the HE-wired ranked-list host (no second Radar app).  
2. Unified Hotel Explorer is primary on that host; five Radar tabs preserved.  
3. Bethesda is selectable in **authorized internal** Radar after HPC merge.  
4. Public brand-presence and share packs still exclude Bethesda.  
5. Phase 1 automated gates + new Phase 1.3 gates PASS locally.

## Remaining limitations

1. **App shell browser automation** blocked by Memberstack sign-in; canonical **embed URL** validated instead. Founder login required for full shell click-path.  
2. **Operator** `#/opportunity-radar` still uses `operator-intelligence-radar-with-list.html` without HE.  
3. Permanence Layer B / durable measured-dates still blocked locally (pre-existing).  
4. Bethesda amenities/description remain sparse on HPC (identity/geo/market OK) — not invented.

## Recommendation

**Conditional merge-ready** after founder review of uncommitted Phase 1.2 + 1.3 diffs and a manual login smoke of:

`http://localhost:8091/app?inventory=staged&preview=staged#/opportunity-radar`

Do **not** merge/deploy without founder approval.
