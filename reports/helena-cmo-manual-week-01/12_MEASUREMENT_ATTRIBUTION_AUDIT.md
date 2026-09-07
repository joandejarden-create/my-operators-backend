# Measurement + Attribution Audit

| Source | State | Notes |
|---|---|---|
| GA4 | PARTIAL | Phase 3B: 553 sessions; organic 19; LinkedIn not cleanly broken out |
| GSC | PARTIAL | Page snapshots exist; residences high impr/0 clicks |
| LinkedIn analytics | PARTIAL | Likes/comments scraped; impressions often null; commercial UNKNOWN |
| Webflow analytics | PARTIAL | Assumed via GA; event taxonomy incomplete |
| Forms | PARTIAL | Forms exist; UTM/first-touch persistence not verified as complete |
| Email/outreach attribution | NOT_CONNECTED | |
| CRM / lead records | DATA_GAP | Not joined to Marketing OS |
| Product usage analytics | PARTIAL_OR_INTERNAL | Not CMO-joined |
| ADP pilot data | NOT_CONNECTED | No scorecard instances |
| Marketing OS Performance | CONNECTED | 175 rows; Tier 3 heavy |

## Funnel (canonical)
AWARENESS → QUALIFIED VISIT → PRODUCT INTEREST → COMMERCIAL INTENT → LEAD → QUALIFIED ACCOUNT → CONVERSATION → DEMO/PILOT DISCUSSION → PROPOSAL → PAID PILOT/CUSTOMER → RETENTION/EXPANSION

## Metric hierarchy
- **Tier 1 business:** paid pilots, customers, qualified opps, conversion, retention, expansion, revenue
- **Tier 2 intent:** demos, pilot discussions, qualified leads, target-account engagement, founder conversations
- **Tier 3 diagnostic:** impressions, likes, clicks, sessions, rankings

## Final answers
1. GA4 sufficient? **PARTIAL**
2. Answerable today: sessions/source mix (coarse), GSC impr/clicks, LinkedIn engagement proxies, OS performance rows
3. Not answerable: LinkedIn→site→lead→pilot→revenue; which content creates paid pilots; ADP pilot success cohorts
4. Missing events: see `13_`
5. Missing attribution fields: UTMs/first-last touch/content_id/cta_id/gtm_track/icp on forms
6. Trace LI→…→revenue? **NO**
7. Measure ADP pilot success properly? **NO** until scorecard + delivery logging
8. P0 fixes: form attribution fields; GA4 key events; content/campaign IDs on links
9. Report weekly: WHAT WE DID / EXPECTED / HAPPENED / LEARNED / CHANGE + Tier1/2 if any
10. Stop treating as success: likes, impressions, sessions, rankings alone
