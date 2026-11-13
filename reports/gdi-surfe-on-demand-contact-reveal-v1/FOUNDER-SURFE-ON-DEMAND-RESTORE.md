# GDI — Restore On-Demand Surfe Contact Lookup

## Verdict

On-demand **Find Email / Find Phone / Find Email & Phone** is restored in Who to Contact across GDI. No second enrichment system was built — existing Surfe modules were repaired and re-wired.

## Why the buttons disappeared

Three stacked regressions:

1. **Customer sanitize deep-strip** (`stripInternalProviderKeys`) removed every key matching `surfe[_-]?enrich*`, including the UI affordance flags `surfeEnrichmentEligible` and `surfeEnrichmentChannel`. The two-contact presentation still computed them; the customer DTO dropped them before the drawer rendered.
2. **`postGdiContactReveal` was never registered** on `server.js` (handler existed in `api/group-demand-intelligence.js` only).
3. **No drawer click handler** in `app.js` for `data-gdi-contact-cta`.

## Existing code recovered / reused

| Module | Role |
|--------|------|
| `gdi-contact-on-demand-reveal-v1.js` | Ephemeral reveal orchestration + mock provider |
| `gdi-surfe-contact-provider-v1.js` | Live Surfe adapter (channel-scoped include) |
| `gdi-surfe-live-gate-v1.js` | Credits preflight, in-flight dedupe, accounting |
| `scaling-two-contact-standard-v1.js` | Slot eligibility + channel derivation |
| `unified-who-to-contact-v1.js` | Cross-segment Who to Contact hydration |
| `DealalityGdiUi.twoContactSlotHtml` | CTA rendering |
| `lib/surfe/client.js` | Surfe HTTP client |

## Files modified

- `lib/group-demand-intelligence/customer-presentation-sanitize-v1.js` — preserve Surfe UI affordance keys
- `public/js/group-demand-intelligence/dealality-gdi-ui.js` — CTA labels, channel fallback, referral copy cleanup, hide CTAs on share
- `public/js/group-demand-intelligence/app.js` — click handler, mock vs live fetch path
- `api/group-demand-intelligence.js` — deny live Surfe without signed-in user
- `server.js` — register `POST .../contacts/reveal`
- `scripts/test-gdi-surfe-on-demand-contact-reveal-v1.mjs` + npm script
- Cache-bust on `group-demand-intelligence.html`

## Bethesda named-contact audit (sales inventory)

| Metric | Count |
|--------|------:|
| Named contacts (primary + secondary slots) | 92 |
| Should show Find Email | 13 |
| Should show Find Phone | 29 |
| Should show Find Email & Phone | 33 |
| Named with both channels already | 17 |

Annette Hawks / Cohera email-only pattern is covered by unit sanitize + UI render (`Find Phone`). Live Bethesda inventory currently surfaces the same pattern on contacts such as Brad Roos (email present, phone missing).

## Safeguards

- Surfe runs **only** after explicit button click
- Live Surfe requires signed-in Dealality user + `authorizeLiveProvider`
- Share surface suppresses CTAs; share APIs do not expose this route
- Results are session-only (`persisted: false`, `surfePiiPersisted: false`)
- In-flight dedupe + credit preflight on live path
- Mock certification via `GDI_CONTACT_REVEAL_MOCK=1` + `?gdiContactRevealMock=1` (no live credits)

## Browser certification (localhost:8080)

- Bethesda Premier Cup / Brad Roos: **Find Phone** visible with existing email
- Mock click returned session-only phone (`+1-202-555-0199`) without persistence
- Screenshots: `reports/gdi-surfe-on-demand-contact-reveal-v1/screenshots/`

## Tests

```bash
npm run test:gdi-surfe-on-demand-contact-reveal-v1
```

PASS — sanitize preserves affordances, mock provider works, live blocked without auth, Bethesda audit counts recorded.

## Limitations

- Live Surfe credits were **not** spent in this certification (mock only)
- Annette Hawks is not currently in the Bethesda FS inventory; pattern verified via synthetic sanitize/UI + equivalent live contacts
- Opening a drawer does not call Surfe; only the CTA does
