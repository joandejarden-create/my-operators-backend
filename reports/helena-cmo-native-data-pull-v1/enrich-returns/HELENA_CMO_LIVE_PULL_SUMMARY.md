# Helena CMO Live Metrics Pull v1

**Pull timestamp:** 2026-09-07T18:16:49+02:00 (Europe/Paris) / 2026-09-07T16:16:49Z UTC  
**Machine file:** `helena-cmo-live-metrics-v1.json`  
**Method:** Enrich Labs native connectors only (no Zapier, no Cursor for GA4/GSC)

## 1) Integration inventory (live)

| Source | Status | Account | Live read |
|---|---|---|---|
| GA4 | CONNECTED | Dealality property `530177196` | LIVE_READ_SUCCESS |
| GSC | CONNECTED | `sc-domain:dealality.com` (AO site listed, not selected) | LIVE_READ_SUCCESS |
| LinkedIn Joan | CONNECTED | personal, autopilot OFF | PERMISSION_LIMITED (public scrape) |
| LinkedIn AO | CONNECTED | organization, autopilot OFF | PERMISSION_LIMITED |
| LinkedIn Dealality co. | NOT_CONNECTED |, | NOT_CONNECTED |
| Webflow | CONNECTED | site `68108c29063eeb5d1bd7ae4a` | LIVE_READ_SUCCESS (46 Insights) |
| Airtable | CONNECTED | Owner Targets Table | LIVE_READ_SUCCESS (list only) |
| Microsoft Clarity | CONNECTED (3P) | GA shows clarity referrals | PERMISSION_LIMITED (API not fully pulled) |
| Email ESP | NOT_CONNECTED |, | NOT_CONNECTED |
| Enrich crons/calendar | CONNECTED | workspace | LIVE_READ_SUCCESS |

## 2) Executive signal

- Last 30d GA4 sessions fell vs prior 30d (97 vs 241) while GSC impressions exploded (page-sum ~1336 last 28d vs ~114 prior 28d) with only 6 clicks.
- Direct still dominates acquisition; Organic Search remains thin; Organic Social and AI Assistant are real but small live channels.
- LinkedIn UTM path is ATTRIBUTED in GA4 (linkedin/social + linkedin.com/referral = 8 sessions / 30d).
- Product paths AI Demand Positioning and Brand Explorer appear in GA4 page paths (internal/product usage signal).
- form_start=8 is the only conversion-like event; no demo/pilot key events in the event stream.

**If you remember only one thing:** If you remember only one thing: search is discovering Dealality pages (especially branded residences) far faster than it is converting, while site sessions are still mostly direct/social and not yet an owner-outcome engine.

## 3) GA4 (ATTRIBUTED)

| Window | Sessions | Direct (simple) | Google organic (simple) |
|---|---:|---:|---:|
| Last 7d | 25 | 15 | 0 |
| Last 30d | 97 | 61 | 4 |
| Previous 30d | 241 | 132 | 6 |
| Last 90d | 518 | 277 | 18 |

**Last 30d default channel groups:** Direct 57 · Referral 16 · Organic Search 10 · Organic Social 8 · AI Assistant 4 · Unassigned 1

**Last 30d source/medium highlights:**  
- `(direct)/(none)` 57 sess  
- `linkedin.com/referral` 5 + `linkedin/social` 3 = **8 LinkedIn-attributed sessions**  
- `google/organic` 7 · `bing/organic` 3 · `chatgpt.com/ai-assistant` 4  
- `clarity.microsoft.com/referral` 7 · `agent.enrichlabs.ai/referral` 9  

**Audience 30d:** desktop 84 / mobile 13 sessions · new 66 / returning 29 sessions (6 returning users)  
**Geo 30d leaders:** US 39 · Spain 30 · Germany 6  

**Events 30d:** page_view 393 · form_start **8** · click 2 · no named demo/pilot events in top stream  

**Notable pages 30d:** `/` 55 · `/es` 10 · `/hotel-owner/ai-demand-positioning` 7 · soft-brands guide 6 · key-money 4 · brand-explorer-new 1  

**90d channels:** Direct 261 · Organic Social 116 · Referral 85 · Organic Search 43 · AI Assistant 7  

## 4) GSC (ATTRIBUTED)

| Window | Clicks (page-sum) | Impressions (page-sum) | CTR |
|---|---:|---:|---:|
| Last 7d | 3 | 497 | 0.006 |
| Last 28d | 6 | 1376 | 0.0044 |
| Previous 28d | 2 | 114 | 0.0175 |
| Last 90d | 12 | 1551 | 0.0077 |

**Top pages last 28d:** home 2c/39i · soft brands 2c/165i · HMA vs franchise 1c/24i · key money 1c/18i · **branded residences 0c/789i pos~49** · AI visibility 0c/78i · CALA brand selection 0c/20i **pos 5**

**Query classes (sample):** branded-residence / LOW VALUE impressions dominate; HOTEL DEALMAKING and BRAND SELECTION present but deep; ADP-AI DEMAND queries impress without clicks.

**Device 28d:** Desktop 5c/1242i · Mobile 1c/80i  

## 5) LinkedIn / social

- **JOAN + AO** public scrape: 29 posts (from Jul 1 cutoff + recent)  
- Engagement (likes/comments/shares) **ATTRIBUTED** when present; impressions mostly **UNKNOWN**  
- **Dealality company page:** NOT_CONNECTED  
- **Commercial outcomes:** UNKNOWN (no pipeline join this pull)  
- GA4 proves LinkedIn→site path exists (8 sessions / 30d) = **ATTRIBUTED** at channel level, not post level  

## 6) Other native

- **Webflow Insights:** 46 posts  
- **Active crons:** Daily LinkedIn Post (Joan), Monday Morning Performance Digest, Weekly SEO Article, Monthly SEO Deep Dive, AO Weekly LinkedIn, Weekly Automation Suggestions  
- **Airtable:** Owner Targets Table connected (records not expanded this pull)  

## 7) Residual gaps

See JSON `residual_gaps`. Highlights: LinkedIn impressions PERMISSION LIMITED; L1 owner outcomes NOT AVAILABLE; email NOT AVAILABLE; full Clarity API PERMISSION LIMITED; GA key-event taxonomy may NEED follow-up.

## Drop path for Joan

Copy this folder into Dealality repo:

`reports/helena-cmo-native-data-pull-v1/enrich-returns/`

Files:
- `helena-cmo-live-metrics-v1.json`
- `HELENA_CMO_LIVE_PULL_SUMMARY.md` (this file)
