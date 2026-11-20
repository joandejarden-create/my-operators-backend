# GDI Find Email & Phone — auth diagnosis and fix

## Root cause

**Browser-side authentication failure before the POST left the client.**

On the external Railway GDI page (`/group-demand-intelligence.html`):

1. Opportunity GETs succeed under `gdiPilotReadAuth` pilot read (unauthenticated allowed).
2. Find Email & Phone uses `fetchFn` → `DealalityMemberstackAuth.fetchMyDealsApi` → `authFetch`.
3. That page loads `dealality-memberstack-auth.js` but **not** the Memberstack SDK / session.
4. `getAuthHeaders()` fails and throws `Error("Please log in again to continue.")` with `code=AUTH_REQUIRED`.
5. The reveal client `.catch` displayed `err.message` for **every** throw — so provider/credits/miss failures were also at risk of looking like login failures.

A successful opportunity GET is **not** proof the protected reveal POST is authorized.

| Layer | Outcome |
|---|---|
| Browser | **Failed first** — no JWT → AUTH_REQUIRED throw (LOGIN_MSG) |
| Middleware (`gdiPilotReadAuth`) | Never reached for the failing click path |
| Handler | Never reached for the failing click path |

## Triggering status / error

- Client: thrown `AUTH_REQUIRED` / message `Please log in again to continue.`
- No HTTP status from reveal POST on the observed failure (request aborted in `authFetch`)

## Corrections

| File | Change |
|---|---|
| `gdi-contact-reveal-client-v1.js` | Distinct states: sign-in / permissions / provider / credits / miss / found; Sign-in link; never reuse LOGIN_MSG for non-auth failures |
| `app.js` | `contactRevealFetch` converts AUTH_REQUIRED → JSON `401 SIGN_IN_REQUIRED` Response |
| `api/group-demand-intelligence.js` | Anonymous → `401 SIGN_IN_REQUIRED`; no Surfe key → `503 PROVIDER_UNAVAILABLE`; authorized test injection (signed-in + token) |
| `group-demand-intelligence.css` / `.html` | Provider status style + cache bust `gdi-contact-reveal-auth-v1` |

## Live-provider readiness

- **Not certified for live Surfe.** No paid calls in this investigation.
- Signed-in + missing `SURFE_API_KEY` → honest `PROVIDER_UNAVAILABLE` (not login).
- Mock / authorized test injection only; production customers never get public-read mock.

## Acceptance

See `RESULTS.json` and screenshots in this folder after local / Railway verification.
