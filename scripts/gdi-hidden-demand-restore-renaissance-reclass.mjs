#!/usr/bin/env node
/**
 * Restore Renaissance GDI bag after over-aggressive generator downgrade.
 * Re-runs fixed reclassify: lodging/overflow motions → KEEP_AS_WATCH (customer-visible).
 * Pure obvious generators without lodging motion stay INTERNAL_ONLY.
 */
import "../load-env.js";
import { loadOpportunitiesCanonical, saveOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { reclassifyExistingOpportunity } from "../lib/group-demand-intelligence/hidden-demand/classify.js";
import { RECLASS_LABEL, DISCOVERY_DEPTH } from "../lib/group-demand-intelligence/hidden-demand/constants.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";

const HOTEL_ID = "recG66DQJKP2c0UNh";
const APPLY = process.argv.includes("--apply");

async function main() {
  invalidateGdiHotelReadCache?.(HOTEL_ID);
  const doc = await loadOpportunitiesCanonical(HOTEL_ID);
  const before = filterCustomerFacingOpportunities(doc.opportunities || []).length;
  const next = (doc.opportunities || []).map((opp) => {
    const r = reclassifyExistingOpportunity(opp);
    if (r.action === "KEEP" || r.action === "KEEP_AS_WATCH") {
      const restoredState =
        opp.customerFacingState === "INTERNAL_ONLY" && opp.opportunityRole === "DEMAND_GENERATOR"
          ? opp.roomDemandStatus === "OVERFLOW_ONLY" || /OVERFLOW/i.test(opp.opportunityType || "")
            ? "WATCH"
            : opp.priority === "HIGH" || opp.priority === "PURSUE"
              ? "ACTIVE"
              : "WATCH"
          : opp.customerFacingState === "INTERNAL_ONLY"
            ? "WATCH"
            : opp.customerFacingState;
      return {
        ...opp,
        customerFacingState: restoredState || "WATCH",
        priority:
          opp.priority === "GENERATOR"
            ? /OVERFLOW/i.test(opp.opportunityType || "")
              ? "MEDIUM"
              : "WATCH"
            : opp.priority,
        opportunityRole: undefined,
        reclassLabel: r.label,
        discoveryDepth: r.depth,
        reclassNote:
          r.label === RECLASS_LABEL.DEEPER_MOTION_EXISTS
            ? "Restored: lodging/overflow motion under market generator — keep as hotel opportunity WATCH."
            : opp.reclassNote,
        reclassRestoredAt: new Date().toISOString(),
      };
    }
    // True generator-only
    return {
      ...opp,
      customerFacingState: "INTERNAL_ONLY",
      priority: "GENERATOR",
      opportunityRole: "DEMAND_GENERATOR",
      reclassLabel: RECLASS_LABEL.OBVIOUS_GENERATOR_ONLY,
      discoveryDepth: DISCOVERY_DEPTH.OBVIOUS_MARKET_DEMAND,
    };
  });

  const after = filterCustomerFacingOpportunities(next).length;
  const tallies = { KEEP: 0, KEEP_AS_WATCH: 0, DOWNGRADE_TO_GENERATOR: 0, RESEARCH: 0 };
  for (const opp of next) {
    const r = reclassifyExistingOpportunity(opp);
    tallies[r.action] = (tallies[r.action] || 0) + 1;
  }

  console.log(JSON.stringify({ hotelId: HOTEL_ID, beforeCf: before, afterCf: after, tallies, apply: APPLY }, null, 2));

  if (APPLY) {
    await saveOpportunitiesCanonical(HOTEL_ID, {
      ...doc,
      opportunities: next,
      hiddenDemandExpansion: {
        ...(doc.hiddenDemandExpansion || {}),
        reclassRestored: true,
        restoredAt: new Date().toISOString(),
      },
    });
    console.error("[restore] saved");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
