# Live metric summary

Source of truth: `reports/helena-cmo-live-data-reconciliation-v1/helena-cmo-live-metrics-v1.json` (merged) + Enrich raw in `enrich-returns/`.

Pull: 2026-09-07T16:16:49Z
Metrics in Enrich raw: 145 · Canonical merged: 154

## Headline
{
  "ga4_sessions_7d": 25,
  "ga4_sessions_30d": 97,
  "ga4_sessions_prior_30d": 241,
  "ga4_sessions_90d": 518,
  "ga4_channel_30d_direct": 57,
  "ga4_channel_30d_referral": 16,
  "ga4_channel_30d_organic_search": 10,
  "ga4_channel_30d_organic_social": 8,
  "ga4_channel_30d_ai_assistant": 4,
  "ga4_linkedin_attributed_sessions_30d": 8,
  "ga4_form_start_30d": 8,
  "gsc_clicks_28d": 6,
  "gsc_impressions_28d": 1376,
  "gsc_clicks_prior_28d": 2,
  "gsc_impressions_prior_28d": 114
}

## Calculated rates (Joan should not hand-calc)
- Sessions Δ 30d vs prior: -144 (-59.8%)
- GSC impression Δ 28d vs prior: 1262 (1107.0%)
- GSC CTR 28d: 0.44%
- Rough form_start / sessions 30d: 8.2%
- LinkedIn share of sessions 30d: 8.2%
- Direct share of channel sessions 30d: 59.4%

## Executive signal
- Last 30d GA4 sessions fell vs prior 30d (97 vs 241) while GSC impressions exploded (page-sum ~1336 last 28d vs ~114 prior 28d) with only 6 clicks.
- Direct still dominates acquisition; Organic Search remains thin; Organic Social and AI Assistant are real but small live channels.
- LinkedIn UTM path is ATTRIBUTED in GA4 (linkedin/social + linkedin.com/referral = 8 sessions / 30d).
- Product paths AI Demand Positioning and Brand Explorer appear in GA4 page paths (internal/product usage signal).
- form_start=8 is the only conversion-like event; no demo/pilot key events in the event stream.

**One thing:** If you remember only one thing: search is discovering Dealality pages (especially branded residences) far faster than it is converting, while site sessions are still mostly direct/social and not yet an owner-outcome engine.

## Do not use
Stale Jun–Sep GA4 snapshot as current traffic truth where live windows exist.
