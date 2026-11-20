# GDI — unwanted login popup (native alert)

## Root cause

`public/js/group-demand-intelligence/app.js` `fetchFn` treated every plain GET **401** as:

```text
plainFetch → 401 → memberFetch/fetchMyDealsApi → authFetch → notifyLoginRequired() → window.alert("Please log in again to continue.")
```

On production, any GDI opportunity request that returns 401 (or a secondary authenticated call without a ready JWT) therefore raised a **blocking native alert**, repeatedly, while browsing Opportunities — even when the user had not intentionally logged out, and even when Portfolio was never opened.

Portfolio Demand Intelligence already preferred `getAuthHeaders` (no alert). The Opportunities 401→`authFetch` path was the disruptive trigger.

## Fix

1. `DealalityMemberstackAuth.authFetch` accepts scoped `{ notifyLoginRequired: false | silent: true }` — no alert; still throws `AUTH_REQUIRED`.
2. Legacy My Deals callers keep default `window.alert` behavior.
3. GDI `fetchFn` retries 401 via silent `getAuthHeaders` + plain `fetch` (no alert). Missing JWT returns the original 401 for inline handling.
4. Hotel Commercial Priorities uses the same silent pattern.
5. Cache-bust `?v=gdi-no-login-alert-v1` (including `dealality-memberstack-auth.js`).

## Distinct states (unchanged / reinforced)

| Condition | UX |
|---|---|
| 401 / missing JWT | Inline sign-in required (no alert) |
| 403 | Access denied / operator not mapped |
| 503 / registry | Portfolio unavailable / not provisioned |
| Network | Retryable error |

No demo membership fallback. Security headers / Memberstack / operator checks unchanged.

## Tests

`npm run test:gdi-no-login-alert-v1`

## STOP

Await IC merge + deploy. External browser confirmation that the popup is gone is required before declaring complete.
