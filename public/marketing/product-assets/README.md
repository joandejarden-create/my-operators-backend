# Dealality product marketing assets (live platform)

Published stills and short videos captured from the **real Dealality product UI**.

Harbour House HTML mock scenes under `scenes/` are **superseded** and are not the published assets.

## Demo projects used (already in the platform)

- **Alcove Gloria** — `deal-summary.html?id=demo`
- **Sample Coastal Conversion Opportunity** — `owner-diagnostic-sample.html`
- **Atelier North** — Brand Explorer demo profile
- **Command Center** — `app/home.html` sample deal pulse
- **Deal Compare** — existing real product screenshot (`screenshots/deal-compare.png`) because live Merida compare has no contacted brands without auth

CALA demo deal **Mérida Centro Select-Service** (`recqGVET08a8faagy`) is the marketing demo deal in Airtable. Full My Deals / Deal Setup / Deal Room data requires a signed-in `dealalitydemo@dealality.com` session.

## Regenerate

```bash
node scripts/capture-live-platform-product-assets.mjs
```

Optional: `--base=https://my-operators-backend-staging.up.railway.app` `--only=02-strategic-paths,05-proposal-comparison`

## Metadata

- `ASSET-REPORT.md` — inventory and limitations
- `manifest.json` / `website-placement.json` — captions, alt, playback recommendations
