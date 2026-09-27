#!/usr/bin/env node
/**
 * Re-apply mature customer enrichment + promote path for Hilton TS Hotel #4 bag.
 * Uses forceUpdate so drawer parity / geo catchment run on existing rows.
 *
 *   node scripts/gdi-hilton-ts-reenrich-promote.mjs --dry-run
 *   node scripts/gdi-hilton-ts-reenrich-promote.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  promoteQualifiedGdiOpportunity,
  PROMOTION_ACTION,
} from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import {
  enrichGdiOpportunityForCustomer,
  shouldHideAfterEnrichment,
  assessGeoConsistencyForHotel,
} from "../lib/group-demand-intelligence/enrich-gdi-opportunity-for-customer.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";

const HOTEL_ID = "rec35fExUxCClpOP6";
const APPLY = process.argv.includes("--apply");
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  path.resolve(__dirname, ".."),
  "reports/group-demand-intelligence/hotel4-hilton-times-square-v1/REENRICH.json"
);

async function main() {
  const config = loadHotelDemandConfig(HOTEL_ID);
  const doc = await loadOpportunitiesCanonical(HOTEL_ID);
  const all = doc.opportunities || [];
  const results = [];

  for (const o of all) {
    const geo = assessGeoConsistencyForHotel(o, config);
    const enriched = enrichGdiOpportunityForCustomer(o, { hotelId: HOTEL_ID });
    if (shouldHideAfterEnrichment(enriched)) enriched.customerVisible = false;

    let action = "DRY_ENRICH_ONLY";
    let written = enriched;
    if (APPLY) {
      const promo = await promoteQualifiedGdiOpportunity({
        candidate: enriched,
        existingOpps: all,
        hotelId: HOTEL_ID,
        runId: `hotel4_reenrich_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}`,
        method: "hotel4_drawer_parity_reenrich_v1",
        forceUpdateId: o.id,
        materialUpdateOnly: true,
        dryRun: false,
      });
      action = promo.action;
      written = promo.opportunity || enriched;
    }

    results.push({
      id: o.id,
      title: o.title,
      geoStatus: geo.status,
      farDestination: geo.farDestination,
      coreMarket: geo.coreMarketSignal,
      beforeVisible: o.customerVisible !== false,
      afterVisible: written.customerVisible !== false,
      afterPriority: written.priority,
      drawerReadiness: written.drawerReadiness || null,
      enrichmentVersion: written.customerEnrichmentVersion || null,
      action,
    });
  }

  const afterDoc = APPLY ? await loadOpportunitiesCanonical(HOTEL_ID) : { opportunities: results.map((r) => ({ id: r.id })) };
  const cf = APPLY
    ? filterCustomerFacingOpportunities(afterDoc.opportunities || [])
    : results.filter((r) => r.afterVisible);

  const summary = {
    apply: APPLY,
    total: all.length,
    outsideCatchment: results.filter((r) => r.geoStatus === "OUTSIDE_CATCHMENT").length,
    customerVisibleAfter: APPLY ? cf.length : results.filter((r) => r.afterVisible).length,
    actions: results.reduce((m, r) => {
      m[r.action] = (m[r.action] || 0) + 1;
      return m;
    }, {}),
    rows: results,
  };
  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
