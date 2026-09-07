# ENRICH LABS — HELENA LIVE CMO DATA PULL
# Paste this into Enrich Labs Helena (NOT Cursor / NOT Zapier)

You are Helena operating inside Enrich Labs with your **native** marketing connectors.

Do **not** ask Joan to connect Zapier.
Do **not** send work to Cursor for GA4/GSC if you can read them natively.

## Objective
Produce a single machine-readable file:

`helena-cmo-live-metrics-v1.json`

and a short markdown summary of live reads.

## 1) Inventory your connected sources
List every connected integration: GA4, GSC, LinkedIn (Joan / Dealality / AO), Webflow, email, SEO, Airtable, others.

For each: CONNECTED? ACCOUNT/PROPERTY READ_ACCESS? LAST_SUCCESS LAST_DATA LIMITATIONS

## 2) Live-read proof
For each connected source, actually query it. Classify LIVE_READ_SUCCESS / PERMISSION_LIMITED / STALE_ONLY / FAILED / NOT_CONNECTED. Include PULL_TIMESTAMP, LATEST_DATA_DATE, REPORTING_PERIOD.

## 3) GA4 live pull (if connected)
Windows: last 7d, last 30d, previous 30d, last 90d.

Pull overview (users, active users, sessions, engaged sessions, engagement rate, avg engagement time, new vs returning), acquisition (source, medium, channel group, campaign, referral, landing page), pages, key events / CTA / forms / demo / pilot / product-interest, audience geo + device.

Return **actual values and trends**.

## 4) GSC live pull (if connected)
Windows: 7d, 28d, previous 28d, 90d.

Queries, pages, clicks, impressions, CTR, position, country, device.

Classify queries: BRANDED / OWNER INTENT / HOTEL DEALMAKING / BRAND SELECTION / OPERATOR SELECTION / ADP-AI DEMAND / COMMERCIAL / INFORMATIONAL / LOW VALUE.

## 5) LinkedIn / social (if connected)
Separate JOAN / DEALALITY / AO. Pull fullest available impressions/reach/reactions/comments/reposts/clicks/followers/posts/themes. Commercial outcome = UNKNOWN unless pipeline join exists.

## 6) Other native sources
Webflow CMS inventory, SEO tools, email, campaign reports — pull what you can.

## 7) Output contract
Every metric object:

metric_id, metric_name, category, source_system, source_account, reporting_period, current_value, prior_value, delta, unit, segment, pull_timestamp, latest_data_date, freshness, confidence, business_question, strategic_relevance, attribution_level, provenance

Also return residual gaps: ALREADY AVAILABLE / PERMISSION LIMITED / NOT AVAILABLE / NEEDS DEALALITY-CURSOR.

## 8) Return path
Save outputs to Enrich workspace and tell Joan to drop them into Dealality repo:

`reports/helena-cmo-native-data-pull-v1/enrich-returns/`

Cursor will ingest — **without rebuilding GA4/GSC** if your live pull succeeded.
