# Dealality Marketing

## Landing page (screenshots)

**`dealality-landing-v5.html`** — full marketing landing page with real product screenshots.

| Local preview | `http://localhost:8080/marketing/dealality-landing-v5.html` |
| Screenshots | `public/marketing/screenshots/` — captured at **2× DPR** (~1140–1200 CSS px wide). Re-capture: `node scripts/capture-marketing-screenshots.mjs` (server on :8080). |
| Downloads copy | `dealality-landing-v5.html` + `screenshots/` folder (sibling paths) |

For Webflow: upload images to Webflow Assets and replace `screenshots/*.png` paths in the HTML embed.

### Webflow navbar + Railway body (recommended)

Keep the **navbar in Webflow**; embed the full landing (no nav/footer) from Railway.

| Piece | Where |
|-------|--------|
| Navbar | Webflow **Dealality Navbar** on `Home v7 (draft)` |
| Page body | Webflow **Embed** → paste `components-iframe/00-landing-full-body.html` |
| Landing HTML | Railway `/marketing/dealality-landing-v7.html?embed=1` |
| Home URL | Webflow site root `/` (override with `window.DEALALITY_LANDING_HOME_PATH` if different) |

Replace `YOUR-RAILWAY-HOST` in the embed snippet with your production Railway URL.

**Webflow nav link targets** (same-page hash — parent script scrolls the iframe):

| Link | `href` |
|------|--------|
| For Owners | `#owners` |
| For Brands | `#brands` |
| For Partners | `#partners` |
| How It Works | `#how` |
| FAQ | `#faq` |
| Request Early Access | `/signup-new` or Railway `/signup` |
| Sign In | `/log-in` (your Memberstack login page) |

**Navbar on signup/login (and other pages):** Site footer should load `dealality-webflow-account-notice.js` (after `dealality-webflow-me-bootstrap.js`). It auto-loads `dealality-landing-webflow-parent.js` when a Webflow navbar is present so section links navigate to `/#owners` (etc.) from non-landing pages.

**Deploy requirement:** Railway must serve `public/marketing/` (including screenshots). `?embed=1` hides the Railway nav/footer and allows iframe embedding on `dealality.com` / `*.webflow.io`.

**Local test:** open `dealality-landing-v7.html?embed=1` — nav and footer should be hidden.

---

# Dealality Marketing Embeds

Two approaches for Webflow landing pages:

1. **Live app iframes (recommended)** — real product UI with demo data
2. **Static snapshots** — platform-faithful HTML when iframes are not desired

## Live app iframes

Each marketing section loads the actual Dealality page in a framed iframe. Deal workflow frames use the CALA demo deal from `/api/marketing/demo-embeds`.

| Key | Live route |
|-----|------------|
| `heroDashboard` | `/app/home.html?embed=1&appShell=1` |
| `brandExplorer` | `/brand-education-atelier-north.html?embed=1` |
| `operatorExplorer` | `/operator-explorer-gold-mock.html?embed=1` |
| `marketIntelligence` | `/market-alerts.html?embed=1` |
| `termComparison` | `/deal-compare.html?embed=1&dealId={demoDealId}` |
| `dealRoom` | `/deal-room-owner.html?embed=1&dealId={demoDealId}&marketingEmbed=1` |
| `loiHandoff` | `/deal-setup.html?embed=1&id={demoDealId}&edit=1&marketingEmbed=1` |

### Files

| File | Purpose |
|------|---------|
| `dealality-live-embed.css` | Iframe frame styling |
| `dealality-live-embed.js` | Mounts iframes from `data-dealality-embed` |
| `dealality-live-showcase.html` | Local preview of all seven live frames |
| `components-iframe/*.html` | Webflow Embed snippets (replace `YOUR-RAILWAY-HOST`) |

### Webflow embed (per section)

Paste the matching file from `components-iframe/` into a Webflow **Embed** element. Replace `YOUR-RAILWAY-HOST` with your Railway app URL.

The script fetches `/api/marketing/demo-embeds` for canonical URLs and the demo deal id. Override the host with `data-app-base` on the wrapper div.

### Local preview

```
http://localhost:8080/marketing/dealality-live-showcase.html
```

### Env (optional)

```env
MARKETING_DEMO_DEAL_ID=recqGVET08a8faagy
MARKETING_DEMO_DEAL_NAME=Mérida Centro Select-Service
PUBLIC_URL=https://my-operators-backend.up.railway.app
```

## Static snapshots (fallback)

Static HTML that reuses live app CSS and class names — no API calls.

| File | Purpose |
|------|---------|
| `dealality-mockups.css` | Imports platform stylesheets + snapshot utilities |
| `dealality-mockups-showcase.html` | Preview all seven static components |
| `components/*.html` | Embeddable HTML fragments |

### Local preview

```
http://localhost:8080/marketing/dealality-mockups-showcase.html
```

### Webflow embed (static)

Link once in site head:

```html
<link rel="stylesheet" href="https://YOUR-RAILWAY-HOST/marketing/dealality-mockups.css">
```

Per section — paste contents of `components/*.html` into an Embed element. Root wrapper: `mkt-snapshot`.

## Notes

- Anonymized labels in static mockups only; live iframes show real demo fixtures (Atelier North, HE CALA operator, CALA sample deal).
- Deal Compare and Deal Room read paths work without login when `dealId` is set.
- Deal Setup may show limited data for anonymous viewers; signed-in viewers see full deal fields.
- Restart the server after changing `MARKETING_DEMO_DEAL_ID` or `PUBLIC_URL`.
