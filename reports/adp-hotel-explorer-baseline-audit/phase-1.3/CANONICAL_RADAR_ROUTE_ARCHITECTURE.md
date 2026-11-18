# Canonical Radar route architecture (Phase 1.3)

**Branch:** `cursor/adp-hotel-explorer-audit-1ed2` @ `dfc93df33` (+ uncommitted Phase 1.2/1.3)  
**Primary validation host:** `http://localhost:8091`

## Route

Founder URL:

`/app?inventory=staged&preview=staged#/opportunity-radar`

### Router

| Layer | Implementation |
|-------|----------------|
| Shell | `public/app.html` + `public/app.js` |
| Hash route | `#/opportunity-radar` |
| Route map | `ROUTES['/opportunity-radar'] = { file: '/deal-capture-radar-with-ranked-list.html', title: 'The Radar' }` |
| Embed | Same-origin iframe via `routeToEmbedUrl()` with `embed=1&appShell=1` |
| Query forward | Parent `/app?inventory=&preview=` and hash query keys are forwarded into the iframe URL |

Effective embed URL (owner/brand/admin):

`/deal-capture-radar-with-ranked-list.html?embed=1&appShell=1&inventory=staged&preview=staged`

### Operator exception

If nav role is `operator`, `#/opportunity-radar` embeds:

`/operator-intelligence-radar-with-list.html`

That host is a **separate** Radar implementation and does **not** currently load Hotel Explorer. Founder/admin localhost validation uses owner/admin role → ranked-list host.

## What the ranked-list host loads

From `public/deal-capture-radar-with-ranked-list.html`:

- Map/list: `brand-presence-mapping.js`, `radar-page.js`, filter/layer drawers
- Detail: `hotel-detail-panel.js` (five baseline Radar sections)
- Unified HE: `hotel-explorer.js` (sets `__DEALALITY_HOTEL_EXPLORER_PRIMARY=true`)
- Auth helper (Phase 1.3): `auth-helper.js` for authorized internal-preview merge
- Share mode helper: `opportunity-radar-share-mode.js`

Hotel selection path:

`pin/list click → HotelDetailPanel.open(hotel) → HotelExplorer.open(hotel)` when HE primary is set.

## Duplicate Radar implementations

| Host | Role | HE? |
|------|------|-----|
| `deal-capture-radar-with-ranked-list.html` | Canonical owner/admin Opportunity Radar | **Yes** |
| `operator-intelligence-radar-with-list.html` | Operator workspace Opportunity Radar | No |
| `opportunity-radar-share.html` | Customer/share | Share-gated; no Bethesda pack |
| Older `deal-capture-radar-*.html` | Legacy/archive | Not used by `/app` route |

**No new Radar application was created.** Phase 1.3 reuses the ranked-list host already mapped by `/opportunity-radar`.

## Staged inventory / preview

`inventory=staged` + `preview=staged` are **GDI publication inventory** controls in the app shell. They are forwarded into the Radar embed query string but do **not** change Hotel Census / brand-presence SoT for Opportunity Radar pins.

## Comparison: app Radar vs standalone ranked-list

| Concern | Standalone ranked-list | App `#/opportunity-radar` |
|---------|------------------------|-------------------------|
| HTML/JS/CSS | Same ranked-list host | Same host inside iframe |
| Pins / filters | Hotel Census via `/api/brand-presence` | Same |
| HE wiring | Primary | Same (owner/admin) |
| Shell chrome | None | App sidebar + Memberstack gate |
| Staged params | Optional | Forwarded; GDI-oriented, not census SoT |

## Conclusion

Canonical integration was already path-correct: `#/opportunity-radar` embeds the Phase 1 HE-wired ranked-list host. Phase 1.3 confirms that wiring, adds authorized Bethesda internal-preview pin merge, and documents the operator-host gap.
