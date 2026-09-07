# Exact answers

1. **Helena itself can access (Enrich-side, historical/asserted):** GA4 via `get_traffic`, GSC (export evidence), LinkedIn publish tools, Webflow CMS. **This Cursor session cannot invoke those Enrich tools.**

2. **Live reads succeeded here:** Webflow CMS · Airtable GTM/MOS. **Failed:** Enrich GA4/GSC/LinkedIn.

3. **Current GA4 metrics:** Live = **none**. Stale Enrich export: 553 sessions (direct 277, organic 19) for 2026-06-01→09-06.

4. **Current GSC metrics:** Live = **none**. Stale summary only.

5. **Current LinkedIn metrics:** Live = **none**. Stale scrape only.

6. **Other current metrics Helena/Dealality already has live:** Webflow CMS inventory · GTM Pilot Target aggregates · Marketing OS counts · ADP public page.

7. **Cursor does NOT need to rebuild:** GA4/GSC (pending Enrich) · Webflow CMS · Zapier GA4 auth path.

8. **True remaining gaps:** Enrich→Cursor analytics bridge · product usage · CTA attribution · ADP pilot telemetry.

9. **Gaps requiring Cursor:** product usage · landing/CTA attribution · ADP pilot instances · GTM→CMO normalization (not GA4).

10. **Deep Baseline V2 from live data?** **NO** — wait for Enrich-native GA4/GSC/LinkedIn artifact.
