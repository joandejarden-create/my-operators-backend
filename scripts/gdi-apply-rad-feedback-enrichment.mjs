#!/usr/bin/env node
/**
 * Apply Rad/GM feedback product-quality enrichment to persisted Bethesda opportunities.
 * No Webhound spend. Does not touch ADP.
 *
 *   node scripts/gdi-apply-rad-feedback-enrichment.mjs
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyRadFeedbackEnrichmentPass,
  buildBethesdaMarriottProfileFromExistingKnowledge,
  PILOT_HOTEL_ID,
  filterSalespersonView,
  PRIORITY,
  getGdiDataRoot,
} from "../lib/group-demand-intelligence/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const hotelDir = path.join(getGdiDataRoot(), "hotels", PILOT_HOTEL_ID);
const oppPath = path.join(hotelDir, "opportunities.json");
const profilePath = path.join(hotelDir, "profile.json");

if (!fs.existsSync(oppPath)) {
  console.error("Missing opportunities.json — run pilot research first.");
  process.exit(1);
}

const doc = JSON.parse(fs.readFileSync(oppPath, "utf8"));
const before = doc.opportunities || [];
const { opportunities, enrichment } = applyRadFeedbackEnrichmentPass(before, PILOT_HOTEL_ID);

const active = filterSalespersonView(opportunities);
const counts = {
  high: active.filter((o) => o.priority === PRIORITY.HIGH).length,
  medium: active.filter((o) => o.priority === PRIORITY.MEDIUM).length,
  watchlist: active.filter((o) => o.priority === PRIORITY.WATCHLIST).length,
  disqualified: opportunities.filter((o) => o.priority === PRIORITY.DISQUALIFIED).length,
  qualified: active.length,
  sourcingUnknown: opportunities.filter((o) => o.sourcingStatus === "UNKNOWN").length,
  territoryCore: opportunities.filter((o) =>
    ["BETHESDA_MONTGOMERY_CORE", "NORTH_DC_MEDICAL_CORRIDOR"].includes(o.demandTerritoryFit)
  ).length,
  territoryDmv: opportunities.filter((o) =>
    ["DMV_COMPETITIVE", "DMV_STRETCH"].includes(o.demandTerritoryFit)
  ).length,
};

fs.mkdirSync(hotelDir, { recursive: true });
fs.writeFileSync(
  oppPath,
  JSON.stringify(
    {
      ...doc,
      hotelId: PILOT_HOTEL_ID,
      updatedAt: new Date().toISOString(),
      enrichment,
      opportunities,
    },
    null,
    2
  ) + "\n"
);

const profile = buildBethesdaMarriottProfileFromExistingKnowledge();
fs.writeFileSync(profilePath, JSON.stringify(profile, null, 2) + "\n");

console.log(
  JSON.stringify(
    {
      ok: true,
      hotelId: PILOT_HOTEL_ID,
      enrichment,
      counts,
      note: "Hotel feedback not yet collected — incremental value remains UNKNOWN.",
    },
    null,
    2
  )
);
