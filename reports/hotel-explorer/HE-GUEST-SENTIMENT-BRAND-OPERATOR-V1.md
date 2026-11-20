# Guest Sentiment & Reputation — Brand / Operator integration (v1)

## Placement

Inside **Brand / Operator** only (not a new tab):

1. Brand & Operator Overview (existing)
2. Hotel News & Articles (existing; hide when empty)
3. Guest Sentiment & Reputation (new; hide unless authorized / founder proposed staged)
4. Guest Experience Insights (expandable details inside #3)

## Bethesda Marriott pilot (`recLuxvwwxID7U2B8`)

| Mode | Expected |
|---|---|
| Live | Subsection **hidden** (`live_ui_authorized: false`) |
| Founder Proposed Preview | Shows Tripadvisor / Booking / Expedia native scores + theme insights + rights chip |

Staged packet: `fixtures/hotel-explorer/guest-sentiment/beth-marriott-staged-v1.json`

## KGPV / other hotels

No staged packet → subsection remains hidden (validated for `recUNycnMwOVFX0hc`).

## Airtable

Dry-run: `npm run he-guest-sentiment-airtable-dry-run` → `reports/hotel-explorer/HE-GUEST-SENTIMENT-AIRTABLE-DRY-RUN-V1.json`  
`applyAuthorized: false` — no production writes.

## Gates

- `npm run test:he-guest-sentiment-brand-operator-v1`
- `npm run test:he-product-quality-gates-v1` (includes HE-QL-022)
- Quality standard §9d · lesson **HE-QL-022**

## Provider / cost

See `reports/hotel-explorer/HE-GUEST-SENTIMENT-PROVIDER-ACCESS-V1.md`.
