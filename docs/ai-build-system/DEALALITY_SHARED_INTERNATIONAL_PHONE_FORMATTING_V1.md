# DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1

Presentation-only international telephone formatting for Dealality products.

## Contract

- **Library:** `libphonenumber-js@1.13.17`
- **Docs:** https://github.com/catamphetamine/libphonenumber-js
- **Node module:** `lib/dealality-shared/dealality-shared-international-phone-formatting-v1.js`
- **Browser facade:** `public/js/dealality-shared-international-phone-formatting-v1.js`
- **Vendor bundle:** `public/vendor/libphonenumber-js.min.js`

## Distinctions (permanent)

1. Original canonical telephone value — never rewritten by the formatter
2. Human-readable displayed telephone value
3. Validated E.164 dialing value / `tel:` URI
4. Existing verification status — unchanged
5. Existing provider provenance — unchanged
6. Existing persistence / retention — unchanged

## Formatting rules

- Explicit `+` calling code → library numbering plan (never hotel/market country inference)
- NANP (`+1`): `+1` + national format → e.g. `+1 (267) 475-4267`
- Other countries: library `formatInternational()`
- National numbers without `+`: only with verified contact-level country context
- Invalid / ambiguous → preserve original; no fabricated `tel:` link
- Extensions: kept in display; RFC 3966 via `getURI()` when available

## Consumers

| Product | Component | Shared formatter |
|---|---|---|
| GDI | Opportunity drawer / contact summary / Surfe / whoHow / tiles | `dealality-gdi-ui.js` |
| GDI | List View Contact Details | `gdi-commercial-list-table-v1.js` |
| Hotel Explorer | Contact Intelligence | `hotel-contact-intelligence.js` |

## Tests

```bash
npm run test:dealality-shared-international-phone-formatting-v1
npm run test:gdi-shared-international-phone-formatting-v1
DEALALITY_PHONE_BROWSER=1 npm run test:gdi-shared-international-phone-formatting-v1
npm run test:he-shared-international-phone-formatting-v1
DEALALITY_PHONE_BROWSER=1 npm run test:he-shared-international-phone-formatting-v1
```

## Not authorized

Airtable writes, research mutation, provider calls, STAGED/CURRENT pointer changes, CSV schema changes, template redesign.
