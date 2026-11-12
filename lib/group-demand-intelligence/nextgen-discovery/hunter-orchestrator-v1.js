/**
 * Specialist hunter orchestrator — integrates with Full E2E supervisor selection.
 * Not a parallel GDI supervisor. Zero-paid by default.
 */

import fs from "node:fs";
import path from "node:path";
import {
  GDI_NEXTGEN_DISCOVERY_ARCHITECTURE_V1,
  FIRST_WAVE_HUNTERS,
  SPECIALIST_HUNTER_META,
} from "./specialist-hunter-contract-v1.js";
import { selectSpecialistHuntersForHotel } from "./hotel-adaptive-hunter-selection-v1.js";
import { buildHunterProviderRoutingMatrix } from "./hunter-provider-routing-v1.js";
import { getHunterRunner, listImplementedHunterIds } from "./hunters/index.js";
import { loadHunterCacheDiscoveries } from "./hunters/load-first-wave-cache-v1.js";

export const NEXTGEN_PILOT_HOTELS = Object.freeze([
  {
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
  },
  {
    hotelId: "recnPB3Q02NO8Ma1o",
    hotelName: "Wyndham San Diego Bayside",
  },
  {
    hotelId: "recdmo0CnhIZAwHGC",
    hotelName: "Westin Tampa Waterside",
  },
  {
    hotelId: "recG66DQJKP2c0UNh",
    hotelName: "Renaissance New York Times Square Hotel",
  },
]);

export async function runHunterForHotel({
  hunterId,
  hotelId,
  hotelName = "",
  commercialPriorities = null,
  reportDir = "",
  rootDir = process.cwd(),
  existingKeys = [],
} = {}) {
  const runner = getHunterRunner(hunterId);
  if (!runner) {
    return {
      ok: false,
      hunterId,
      hotelId,
      error: `hunter not implemented in first wave: ${hunterId}`,
      implemented: listImplementedHunterIds(),
    };
  }
  const cacheMeta = loadHunterCacheDiscoveries({ hunterId, hotelId, rootDir });
  const report = await runner.run({
    hotelId,
    hotelName: hotelName || cacheMeta.hotelName,
    territory: cacheMeta.territory,
    commercialPriorities,
    dryRun: true,
    cacheFirst: true,
    budgetUsd: 0,
    existingKeys,
    reportDir,
    rootDir,
  });
  return { ok: report.safety?.ok !== false, report };
}

export async function runFirstWavePilot({
  hotels = NEXTGEN_PILOT_HOTELS,
  reportRoot = "",
  rootDir = process.cwd(),
  commercialPrioritiesByHotel = {},
  wavePolicy = "FIRST",
} = {}) {
  const startedAt = new Date().toISOString();
  const hotelResults = [];

  for (const hotel of hotels) {
    const cp = commercialPrioritiesByHotel[hotel.hotelId] || {
      source: "PILOT_DEFAULT_FIRST_WAVE",
      targetSegments: [
        "government",
        "project",
        "sports",
        "corporate",
        "university",
      ],
      weekdayFocus: true,
      weekendFocus: true,
      guestroomPriority: true,
    };
    const selection = selectSpecialistHuntersForHotel({
      hotelId: hotel.hotelId,
      commercialPriorities: cp,
      wavePolicy,
      maxHunters: 5,
    });
    const hotelReportDir = reportRoot
      ? path.join(reportRoot, hotel.hotelId)
      : "";
    if (hotelReportDir) fs.mkdirSync(hotelReportDir, { recursive: true });

    const moduleRuns = [];
    for (const hunterId of selection.selectedHunterIds) {
      if (!FIRST_WAVE_HUNTERS.includes(hunterId)) continue;
      const result = await runHunterForHotel({
        hunterId,
        hotelId: hotel.hotelId,
        hotelName: hotel.hotelName,
        commercialPriorities: cp,
        reportDir: hotelReportDir,
        rootDir,
      });
      moduleRuns.push({
        hunterId,
        ok: result.ok,
        metrics: result.report?.metrics || null,
        discoveryEvidenceClass: result.report?.discoveryEvidenceClass || null,
        discoverMeta: result.report?.discoverMeta || null,
        qualificationHandoffSummary:
          result.report?.qualificationHandoff?.summary || null,
        rejectedSample: (result.report?.rejected || []).slice(0, 3),
        incrementalSample: (result.report?.incremental || [])
          .slice(0, 3)
          .map((h) => ({
            organizationName: h.organizationName,
            activityTitle: h.activityTitle,
            hypothesisStatus: h.hypothesisStatus,
            contactName: h.contactName,
            discoveryEvidenceClass: h.discoveryEvidenceClass || null,
          })),
      });
    }

    hotelResults.push({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName,
      selection,
      moduleRuns,
      totals: summarizeModuleRuns(moduleRuns),
    });
  }

  const summary = {
    schema: "gdi-nextgen-first-wave-pilot-report-v1",
    architecture: GDI_NEXTGEN_DISCOVERY_ARCHITECTURE_V1,
    startedAt,
    finishedAt: new Date().toISOString(),
    paidAuthorized: false,
    budgetUsdSpent: 0,
    hotels: hotelResults,
    providerRouting: buildHunterProviderRoutingMatrix(),
    portfolioTotals: hotelResults.reduce(
      (acc, h) => {
        acc.rawDiscoveries += h.totals.rawDiscoveries;
        acc.incrementalHypotheses += h.totals.incrementalHypotheses;
        acc.rejected += h.totals.rejected;
        acc.duplicates += h.totals.duplicates;
        acc.readyForOrs += h.totals.readyForOrs;
        acc.contacts += h.totals.contacts;
        return acc;
      },
      {
        rawDiscoveries: 0,
        incrementalHypotheses: 0,
        rejected: 0,
        duplicates: 0,
        readyForOrs: 0,
        contacts: 0,
        salesReady: 0,
        autoPublished: 0,
      }
    ),
    comparisonNote:
      "Compare incremental qualified yield to existing GDI baseline — not raw discovery counts.",
    safeguards: {
      noFifteenthSegment: true,
      noParallelSupervisor: true,
      noAutoPublish: true,
      noPaidResearch: true,
      qualificationUnchanged: true,
    },
  };

  if (reportRoot) {
    fs.mkdirSync(reportRoot, { recursive: true });
    fs.writeFileSync(
      path.join(reportRoot, "first-wave-pilot-summary.json"),
      JSON.stringify(summary, null, 2),
      "utf8"
    );
  }

  return summary;
}

function summarizeModuleRuns(moduleRuns = []) {
  return moduleRuns.reduce(
    (acc, m) => {
      const x = m.metrics || {};
      acc.rawDiscoveries += x.rawDiscoveries || 0;
      acc.incrementalHypotheses += x.incrementalHypotheses || 0;
      acc.rejected += x.rejected || 0;
      acc.duplicates += x.duplicates || 0;
      acc.readyForOrs += x.readyForOrs || 0;
      acc.contacts += x.contacts || 0;
      return acc;
    },
    {
      rawDiscoveries: 0,
      incrementalHypotheses: 0,
      rejected: 0,
      duplicates: 0,
      readyForOrs: 0,
      contacts: 0,
    }
  );
}

/**
 * Soft bridge for weekly supervisor — returns hunter plan without executing paid work.
 */
export function buildWeeklySpecialistHunterPlan({
  hotelId,
  commercialPriorities = null,
  priorHunterYield = {},
  wavePolicy = "FIRST",
} = {}) {
  const selection = selectSpecialistHuntersForHotel({
    hotelId,
    commercialPriorities,
    priorHunterYield,
    wavePolicy,
    maxHunters: 4,
  });
  return {
    schema: "gdi-weekly-specialist-hunter-plan-v1",
    architecture: GDI_NEXTGEN_DISCOVERY_ARCHITECTURE_V1,
    hotelId,
    enabled: true,
    executePaid: false,
    selection,
    mapsToExistingWorkflows: selection.selected.map((s) => ({
      hunterId: s.hunterId,
      workflowIds: SPECIALIST_HUNTER_META[s.hunterId]?.workflowIds || [],
    })),
    note: "Specialist hunters feed existing ORS/qualification — do not auto-publish.",
  };
}
