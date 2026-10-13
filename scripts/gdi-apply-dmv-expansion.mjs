#!/usr/bin/env node
/**
 * Apply DMV expansion discovery to persisted Bethesda opportunities.
 * L3 standard web only — $0 Webhound. Does not touch ADP.
 *
 *   node scripts/gdi-apply-dmv-expansion.mjs
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyDmvExpansionPass,
  applyRadFeedbackEnrichmentPass,
  buildBethesdaMarriottProfileFromExistingKnowledge,
  PILOT_HOTEL_ID,
  filterSalespersonView,
  PRIORITY,
  DEMAND_TERRITORY_FIT,
  getGdiDataRoot,
} from "../lib/group-demand-intelligence/index.js";
import {
  createId,
  saveResearchRun,
} from "../lib/group-demand-intelligence/repository.js";

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

function territoryBucket(list) {
  const buckets = {
    BETHESDA_MONTGOMERY_CORE: 0,
    NORTH_DC_MEDICAL_CORRIDOR: 0,
    DMV_COMPETITIVE: 0,
    DMV_STRETCH: 0,
    OUTSIDE_REALISTIC_TERRITORY: 0,
    OTHER: 0,
  };
  for (const o of list) {
    if (o.priority === PRIORITY.DISQUALIFIED) continue;
    const k = o.demandTerritoryFit;
    if (buckets[k] != null) buckets[k] += 1;
    else buckets.OTHER += 1;
  }
  return buckets;
}

const beforeTerritory = territoryBucket(before);
const expanded = applyDmvExpansionPass(before);
const { opportunities, enrichment } = applyRadFeedbackEnrichmentPass(
  expanded.opportunities,
  PILOT_HOTEL_ID
);

const active = filterSalespersonView(opportunities);
const afterTerritory = territoryBucket(opportunities);
const counts = {
  high: active.filter((o) => o.priority === PRIORITY.HIGH).length,
  medium: active.filter((o) => o.priority === PRIORITY.MEDIUM).length,
  watchlist: active.filter((o) => o.priority === PRIORITY.WATCHLIST).length,
  disqualified: opportunities.filter((o) => o.priority === PRIORITY.DISQUALIFIED)
    .length,
  qualified: active.length,
  territoryQualified: afterTerritory,
  territoryQualifiedBefore: beforeTerritory,
  addedIds: expanded.addedIds,
  rejectedExamples: expanded.rejectedExamples.length,
};

const runId = createId("gdi_run");
const completedAt = new Date().toISOString();
const run = {
  id: runId,
  hotelId: PILOT_HOTEL_ID,
  trigger: "cli_dmv_expansion_l3",
  status: "COMPLETED",
  startedAt: completedAt,
  completedAt,
  productVersion: "group-demand-intelligence-v1.0.0",
  configuration: {
    webhoundHardCapUsd: 15,
    webhoundSpendUsd: 0,
    note: "DMV expansion used standard web only; prior $15 Webhound cap already exhausted on earlier runs.",
  },
  cost: {
    totalUsd: 0,
    webhoundUsd: 0,
    webhoundHardCapUsd: 15,
    webhoundRemainingUsd: 0,
  },
  metrics: {
    opportunitiesDiscovered: opportunities.length,
    qualifiedCount: counts.qualified,
    highPriorityCount: counts.high,
    mediumPriorityCount: counts.medium,
    watchlistCount: counts.watchlist,
    dmvExpansionAdded: expanded.addedIds.length,
  },
  dmvExpansion: {
    addedIds: expanded.addedIds,
    rejectedExamples: expanded.rejectedExamples,
    researchNote: expanded.researchNote,
  },
  enrichment,
};

fs.mkdirSync(path.join(hotelDir, "runs", runId), { recursive: true });
saveResearchRun(PILOT_HOTEL_ID, run);

fs.writeFileSync(
  oppPath,
  JSON.stringify(
    {
      ...doc,
      hotelId: PILOT_HOTEL_ID,
      runId,
      updatedAt: completedAt,
      enrichment,
      dmvExpansion: {
        addedIds: expanded.addedIds,
        researchNote: expanded.researchNote,
        appliedAt: completedAt,
      },
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
      runId,
      counts,
      sampleNew: expanded.addedIds.slice(0, 5).map((id) => {
        const o = opportunities.find((x) => x.id === id);
        return {
          id,
          title: o?.title,
          territory: o?.demandTerritoryFit,
          priority: o?.priority,
          winThesis: o?.bethesdaWinThesis?.slice(0, 120),
        };
      }),
      webhoundRecommendation:
        "Optional +$5–$10 Webhound for deeper NoVA/DC association calendars and housing-bureau prospectuses — not required for this L3 expansion.",
    },
    null,
    2
  )
);

// silence unused import lint in some tooling
void DEMAND_TERRITORY_FIT;
void __dirname;
