# Dealality Marketing

## Landing page (screenshots)

**`dealality-landing-v9.html`** — current production landing (Webflow embed source).

| Local preview | `http://localhost:8080/marketing/dealality-landing-v9.html` |
| Embed test | `http://localhost:8080/marketing/dealality-landing-v9.html?embed=1` |

**`dealality-landing-v7.html`** — alias of v9 (same content) for existing Railway/Webflow URLs.

| Local preview | `http://localhost:8080/marketing/dealality-landing-v7.html` |

**`dealality-landing-v8.html`** — earlier restructure preview (archived for comparison).

| Local preview | `http://localhost:8080/marketing/dealality-landing-v8.html` |

**`dealality-landing-v5.html`** — earlier landing with real product screenshots.

| Local preview | `http://localhost:8080/marketing/dealality-landing-v5.html` |
| Screenshots | `public/marketing/screenshots/` — captured at **2× DPR** (~1140–1200 CSS px wide). Re-capture: `node scripts/capture-marketing-screenshots.mjs` (server on :8080). |
| Downloads copy | `dealality-landing-v5.html` + `screenshots/` folder (sibling paths) |

For Webflow: upload images to Webflow Assets and replace `screenshots/*.png` paths in the HTML embed.

### Webflow navbar + Railway body (recommended)

Keep the **navbar in Webflow**; embed the landing body from Railway (nav hidden in iframe; footer shown).

| Piece | Where |
|-------|--------|
| Navbar | Webflow **Dealality Navbar** on `Home v7 (draft)` |
| Page body | Webflow **Embed** → paste `components-iframe/00-landing-full-body.html` |
| Landing HTML | Railway `/marketing/dealality-landing-v9.html?embed=1` (v7 URL kept as alias) |
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

**Signup/login background:** The same footer script applies the landing iframe skin (`#080f25`, diagonal grid, blob) on `/signup`, `/signup-new`, `/log-in`, and `/login`.

**Deploy requirement:** Railway must serve `public/marketing/` (including screenshots). `?embed=1` hides the Railway nav (Webflow navbar is used) but keeps the landing footer visible inside the iframe.

### Landing analytics (top 20 metrics)

`dealality-landing-analytics.js` records resonance events to:

- `POST /api/marketing/landing-events` → append-only JSONL log (default `data/marketing-landing-events.jsonl`)
- Microsoft Clarity custom events (if Clarity is on the landing page)
- GTM `dataLayer` (if present on parent or landing)

Disable server logging with `LANDING_ANALYTICS_ENABLED=0`.

**Persistent storage (Railway):** Railway’s default disk is ephemeral — redeploys wipe the log. Mount a [Railway Volume](https://docs.railway.app/guides/volumes) at `/data` and set:

```bash
LANDING_ANALYTICS_LOG_FILE=/data/marketing-landing-events.jsonl
```

The admin report banner shows whether storage is persistent.

**Admin report:** `https://<railway-host>/landing-analytics-report?key=<LANDING_ANALYTICS_REPORT_KEY>` — no Memberstack sign-in when the env key is set. Fallback: admin JWT via dealality.com.

**Site version filters (do not wipe history):** append-only JSONL keeps previous iframe landing (`v7`/`v9`) and new Webflow homepage (`old-home`) in the same file. The report toolbar supports:

- `version=all|previous|old-home`
- optional `cutover=YYYY-MM-DD` with `era=all|before|after`
- before/after KPI compare when a cutover is set

Example: `/landing-analytics-report?key=…&days=30&version=old-home&cutover=2026-08-01&era=after`

**New homepage tagging:** load `dealality-old-home-analytics.js` on `/` and `/es` (sets `landingVersion=old-home`). Partial helpers (scroll cue, video) already tag `old-home` on their events.

**Clarity in iframe:** set `CLARITY_PROJECT_ID` on Railway (same id as Webflow). Landing loads `dealality-landing-clarity.js` before analytics so custom events (`cta_click`, `scroll_depth`, etc.) reach Clarity.

**Local inspection:** `Get-Content data/marketing-landing-events.jsonl -Tail 20`

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
