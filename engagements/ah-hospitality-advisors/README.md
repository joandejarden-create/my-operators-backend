# AH Hospitality Advisors — Commercial Performance Hub (AO engagement)

**Not part of Deal Capture / Dealality.** Standalone concept work for Dean Auburn ([ahhospitalityadvisors.com](https://www.ahhospitalityadvisors.com/)).

## LONRP Tableau recreation

The mockup recreates the structure of **`LONRP_property_dashboard.twbx`** (Marriott-style property dashboard):

- **Left navigation pane** — 20 tabs matching Tableau (Intro, Info, Executive Overview, Finance, Booking Pace, Scorecard, Forecast Accuracy, etc.)
- **Sidebar parameters & filters** — Compare, Currency, Budget; Period Filter, Date (Tableau-style)
- **Light `#f5f5f5` shell** with white bordered tiles; bottom sheet tab bar (INFO, EXECUTIVE, FINANCE, …)
- **Default property** — London Marriott Hotel Regents Park (LONRP)
- **Tableau-fidelity tabs (snapshot data)** — **Info Page**, **Executive Overview** (finance KPIs, market share, loyalty, distribution, OTB pace, scorecard, geo/gross/premium charts)
- **Other workbook tabs** — illustrative shells until rebuilt tab-by-tab from `.twbx`
- **Booking Pace** / **Account Insights** — separate A&H pace workspace and decisions modules (nav still available)

Files: `commercial-performance-hub-mockup.html`, `commercial-performance-hub-mockup.js`, `lonrp-dashboard-views.js`, `lonrp-tableau-data.js`, `lonrp-executive-view.js`

## Mockup

Open in a browser:

- **Shareable (while your machine + tunnel are running):**  
  https://sections-sri-temp-meyer.trycloudflare.com/commercial-performance-hub/
- **After Railway deploy (persistent):**  
  https://my-operators-backend-staging.up.railway.app/commercial-performance-hub/
- **Local server:**  
  http://localhost:8080/commercial-performance-hub/  
  (short redirect) http://localhost:8080/commercial-performance-hub-mockup.html
- **Source + engagements path:**  
  http://localhost:8080/engagements/ah-hospitality-advisors/commercial-performance-hub-mockup.html

Static copy for hosting lives in `public/commercial-performance-hub/` (same assets as this folder).

## Standalone GitHub + Railway deploy (recommended for Dean/Osama link)

**Separate repo:** `joandejarden-create/ah-commercial-performance-hub`  
**Local folder:** `../ah-commercial-performance-hub` (sibling to `deal-capture-proxy`)

1. Railway → **New Project** → **GitHub Repository** → **`ah-commercial-performance-hub`**
2. No root directory override (repo root = service).
3. **Networking** → **Generate Domain** → permanent share URL.

**Update mockup after edits here:**

```bash
node engagements/ah-hospitality-advisors/sync-to-standalone-repo.mjs
# then in ah-commercial-performance-hub: git commit && git push
```

Legacy subfolder deploy (same monorepo): see **`railway/README.md`**.

## Collaboration framework

See **`AH-AO-collaboration-framework.md`** — AH prime / AO subcontractor structure, phased delivery, IP, rollout, and commercial anchors from the Jul 7 alignment call.

### Interactive data (v3)

- **4 properties** with distinct MTD / YTD / Forecast metrics in `commercial-performance-hub-mockup.js`
- **Portfolio** = sum of rooms revenue & GOP; **weighted** RevPAR, Occ, ADR, Direct %, RGI (by keys)
- **MTD | YTD | Fcst** toggles refresh KPIs, charts, alerts, pillars, pickup, owner brief
- **Property nav / scorecard row** switches to that asset’s numbers (channel chart → single property)
- **Links** show a toast (demo); **pillar click** opens detail modal with open action

### Features (v2)

| Feature | Location |
|---------|----------|
| **War room / Owner brief** | Top bar mode toggle |
| **Weekly commercial pack** | Button → prints owner brief (1-page summary) |
| **Exception inbox** | Top — pace, channel, rate, group, market alerts + € impact + links |
| **KPI strip** | Actual · Budget · STLY per metric |
| **Four pillar scores** | Sales, Distribution, Yield, Digital (0–100) |
| **Pace & pickup** | 90-day chart (OTB / STLY / Forecast) + 7d/14d pickup + need-period heatmap |
| **Market context** | Shown when a property is selected (sidebar or scorecard row) |
| **Initiative tracker** | AH work product with owner, due date, est. impact |
| **Editable commentary** | Audit trail on save (typing updates timestamp) |
| **KPI dictionary & source registry** | Sidebar buttons → modals |
| **Per-card as-of** | Top-right of each card (PMS, STR, RMS, etc.) |
| **Command tab** | Exceptions, KPIs, pace snapshot, pillars, scorecard, initiatives, commentary (financial detail collapsed) |
| **Pace & pickup tab** | Dark workspace: hero, charts, detail grid, forecast trust, unusual signals |
| **Decisions tab** | Displacement assist, scenario sliders, initiative attribution |

**AO services** drawer — use only when pitching AO, not for Dean/Osama client demo.

### Command → Pace → Decisions (demo script)

1. **Command** — 60-second scan: exceptions → KPIs → pace snapshot → pillars → scorecard → initiatives.
2. Click **Open pace workspace** → **Pace & pickup** (dark): hero, heatmap, detail grid, diagnostics row.
3. **Decisions** — after reviewing pace signals: displacement, scenarios, attribution.
4. **Owner brief** mode — printable 1-pager only (hides Command/Pace/Decisions).

## How to show Dean / Osama

1. **Command** tab, **War room** — exceptions → KPIs → pace snapshot + pillars → scorecard.
2. **Open pace workspace** — full dark pace view for booking pace depth.
3. **Decisions** — group accept/reject and scenarios (after pace review).
4. **Owner brief** mode — 1-pager only for owner meetings / weekly pack.
5. Say: *Illustrative data — layout and workflow we wire to PMS/STR/RMS.*

## Sales & partnership talking points (Joan)

### Hook (30 seconds)

> “Your site says you turn scattered data and complex channels into clear strategy. This hub is that story in one screen: summarized KPIs, weekly actions, four commercial pillars, every number linking back to STR, PMS, channel manager — not another database to maintain.”

### Why AO

| AH pain | AO offer |
|---------|----------|
| Data in many tools | Curated summary layer + link registry |
| Partner time on calls | Owner talking points + meeting mode |
| Scaling mandates | Portfolio roll-up + per-property drill-down |
| Dean testing Claude | AO delivers hosted, branded, refresh workflow + governance |

### Partnership options (pick one on the call)

1. **Project build** — Fixed phases 1–2; AH owns product; AO delivers.
2. **Build + retainer** — v1 build, then monthly refresh/automation support.
3. **Revenue share** — AO builds white-label hub; % of AH client implementation or new mandate introduced through the tool.
4. **Referral** — AH recommends AO for owner-side integrations; AO recommends AH for commercial strategy (no exclusivity required).

### Ballpark anchors (adjust before quoting)

| Phase | Scope | Indicative range (USD) |
|-------|--------|-------------------------|
| 1 | Discovery, source map, signed wireframe | $3k–$8k |
| 2 | v1 hub (4–8 properties, weekly refresh) | $15k–$40k |
| 3 | Automation + alerts + client view | $2k–$6k/mo retainer |

*Use only after Dean confirms property count, sources, and refresh cadence.*

### Questions that close business

- “How many properties on page one in year one?”
- “Who updates the weekly export — AH or AO?”
- “Is Osama’s bar a signed wireframe or working v1 with one live feed?”
- “Would AH want this under AH brand only, or co-branded with AO as technology partner?”

## After tomorrow’s call

- [ ] Send 1-page recap + link to mockup (or PDF via Print).
- [ ] Update mockup property names/sources from Dean’s answers.
- [ ] Schedule Osama demo with agreed deliverable (A/B/C from call prep).
