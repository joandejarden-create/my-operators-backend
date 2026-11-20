# PR #474 — Authorization hardening (pre-merge gate)

## Hotel-scoped access

- New module: `lib/group-demand-intelligence/gdi-contact-reveal-authorization-v1.js`
- Requires **server-resolved `req.dealalityUser`** (Memberstack JWT alone is insufficient)
- Hotel membership from Dealality user hotel scope bags (same pattern as need-dates / sales inventory)
- Admin/founder may enrich; operators need hotel in scope + feature permission
- Share / pilot-unauth surfaces → `SHARE_VIEWER_BLOCKED`
- Cross-hotel → `HOTEL_ACCESS_DENIED` **before** provider / credits

## Route

`POST .../contacts/reveal` → `memberstackAuth` + `requireDealalityUser` + handler  
(no longer `gdiPilotReadAuth`)

## Mock / test injection

- Production customer hosts: mock **disabled** even if env accidentally set
- Isolated worktree / `GDI_CONTACT_REVEAL_MOCK=1` only
- URL `gdiRevealTestToken` / production header injection **removed**
- Anonymous never receives mock or live results

## Real session flow

`/app#/group-demand-intelligence` embeds GDI with **server-verified Memberstack JWT** as `msToken` (`public/app.js`). Standalone Railway URL without session shows **Sign in required** — no invented JWT / no client identity bypass.

## Live Surfe

**NOT_CERTIFIED** — 0 paid calls in this amendment.
