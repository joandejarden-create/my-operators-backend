#!/usr/bin/env node
/**
 * Portfolio Opportunity Type audit (read-only).
 * Writes reports/gdi-opportunity-type-classification-v1/03-portfolio-remediation-audit.json
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import {
  annotateInventoryPopulations,
  BETHESDA_HOTEL_ID,
  selectBethesdaSalesOpportunityInventory,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER,
  findUnclassifiedCustomerVisibleOpportunities,
  opportunityTypeFilterOptions,
  countByOpportunityTypeFilter,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import { inferDemandSegmentIdFromResearchEvidence } from "../lib/group-demand-intelligence/legacy-demand-segment-alias-map-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-opportunity-type-classification-v1"
);
const beforePath = path.join(OUT_DIR, "00-multi-hotel-audit.json");
const hotelsDir = path.join(ROOT, "data/group-demand-intelligence/hotels");

const before = fs.existsSync(beforePath)
  ? JSON.parse(fs.readFileSync(beforePath, "utf8"))
  : [];
const beforeMap = new Map(before.map((r) => [r.hotelId, r]));
const hotels = fs.existsSync(hotelsDir)
  ? fs.readdirSync(hotelsDir).filter((d) =>
      fs.statSync(path.join(hotelsDir, d)).isDirectory()
    )
  : [];

const results = [];
const changes = [];
const stillResearch = [];
let totalOpps = 0;
let totalUnclassBefore = 0;
let corrected = 0;

for (const hotelId of hotels) {
  try {
    const doc = await loadOpportunitiesCanonical(hotelId);
    const opps = annotateInventoryPopulations(doc.opportunities || [], {
      hotelId,
    });
    totalOpps += opps.length;
    const bad = findUnclassifiedCustomerVisibleOpportunities(opps);
    const byType = countByOpportunityTypeFilter(opps);
    const prev = beforeMap.get(hotelId) || {
      otherCount: 0,
      unclassifiedIds: [],
    };
    totalUnclassBefore += prev.otherCount || 0;
    const prevIds = new Set(prev.unclassifiedIds || []);
    const stillIds = new Set(bad.map((o) => String(o.id)));
    const hotelChanges = [];
    for (const id of prevIds) {
      if (stillIds.has(id)) continue;
      const o = opps.find((x) => String(x.id) === id);
      if (!o) continue;
      const inferred = inferDemandSegmentIdFromResearchEvidence(o);
      corrected += 1;
      hotelChanges.push({
        id,
        title: o.displayTitle || o.title,
        from: "OTHER_UNCLASSIFIED",
        to: classifyOpportunityTypeFilter(o),
        demandSegmentId: inferred.demandSegmentId,
        source: inferred.source,
        fromField: inferred.from,
      });
    }
    for (const o of bad) {
      stillResearch.push({
        hotelId,
        id: o.id,
        title: o.displayTitle || o.title,
        segment: o.researchSegment || o.segment || null,
        opportunityType: o.opportunityType || null,
        inferred: inferDemandSegmentIdFromResearchEvidence(o),
      });
    }
    if (hotelChanges.length) {
      changes.push({
        hotelId,
        corrected: hotelChanges.length,
        rows: hotelChanges,
      });
    }
    const opts = opportunityTypeFilterOptions(byType);
    results.push({
      hotelId,
      total: opps.length,
      unclassified: bad.length,
      otherCount: byType.OTHER_UNCLASSIFIED || 0,
      previouslyUnclassified: prev.otherCount || 0,
      corrected: hotelChanges.length,
      dropdownHasOther: opts.some((o) => o.value === "OTHER_UNCLASSIFIED"),
      byType: Object.fromEntries(
        Object.entries(byType).filter(([, v]) => v > 0)
      ),
    });
  } catch (err) {
    results.push({ hotelId, error: err.message });
  }
}

const bethDoc = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      `data/group-demand-intelligence/hotels/${BETHESDA_HOTEL_ID}/opportunities.json`
    ),
    "utf8"
  )
);
const union = mergeBethesdaInternalResearchOpportunities(
  BETHESDA_HOTEL_ID,
  bethDoc.opportunities || []
);
const selected = selectBethesdaSalesOpportunityInventory(
  union.opportunities || [],
  { founderReview: true }
).selected;
const bethAnnotated = annotateInventoryPopulations(selected, {
  hotelId: BETHESDA_HOTEL_ID,
});

const summary = {
  hotelsAudited: results.length,
  totalOpportunitiesAudited: totalOpps,
  originallyUnclassified: totalUnclassBefore,
  corrected,
  stillRequiringResearch: stillResearch.length,
  hotelsStillWithUnclassified: results.filter((r) => (r.otherCount || 0) > 0)
    .length,
  dropdownOtherAnywhere: results.some((r) => r.dropdownHasOther),
  bethesda67: {
    count: bethAnnotated.length,
    unclassified: findUnclassifiedCustomerVisibleOpportunities(bethAnnotated)
      .length,
    otherCount:
      countByOpportunityTypeFilter(bethAnnotated).OTHER_UNCLASSIFIED || 0,
  },
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(
  path.join(OUT_DIR, "03-portfolio-remediation-audit.json"),
  JSON.stringify(
    { summary, results, changesByHotel: changes, stillRequiringResearch: stillResearch },
    null,
    2
  )
);
fs.writeFileSync(
  path.join(OUT_DIR, "00-multi-hotel-audit-post-remediation.json"),
  JSON.stringify(results, null, 2)
);
console.log(JSON.stringify(summary, null, 2));
if (summary.bethesda67.unclassified !== 0 || summary.dropdownOtherAnywhere) {
  process.exitCode = 1;
}
