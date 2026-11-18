/**
 * Bethesda live Exa run → Airtable reconciliation (read-only / fixture-based).
 * Does not backfill 165 raw candidates. Does not write Airtable.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_AIRTABLE_TABLES,
  listCanonicalAirtableRoutingInventory,
} from "./gdi-full-e2e-airtable-persistence-contract-v1.js";
import { dryRunFullE2eAirtablePersistence } from "./gdi-full-e2e-airtable-persistence-dry-run-v1.js";

export const GDI_BETHESDA_EXA_AIRTABLE_RECONCILIATION_V1 =
  "gdi-bethesda-exa-airtable-reconciliation-v1";

export const BETHESDA_LIVE_EXA_RUN = Object.freeze({
  hotelId: "recLuxvwwxID7U2B8",
  hotelName: "Bethesda Marriott",
  runId: "gdi_e2e_14seg_mwladic1_ff9898",
  rawCandidates: 165,
  processed: 20,
  paidDeepOps: 18,
  airtableWritesReported: 0,
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function readJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Load processed candidates from immutable Bethesda live-run artifacts.
 */
export function loadBethesdaLiveExaProcessedCandidates(opts = {}) {
  const root = opts.root || ROOT;
  const tracePath = path.join(
    root,
    "reports/gdi-bethesda-exa-live-discovery-effectiveness-v1/live-run/chain-execution-trace.json",
  );
  const analysisPath = path.join(
    root,
    "reports/gdi-bethesda-exa-live-discovery-effectiveness-v1/analysis-core.json",
  );
  const trace = JSON.parse(fs.readFileSync(tracePath, "utf8"));
  const analysis = JSON.parse(fs.readFileSync(analysisPath, "utf8"));
  const labelByOpp = new Map();
  for (const row of analysis.processedRows || analysis.processed || []) {
    if (row.opportunityId) {
      labelByOpp.set(row.opportunityId, row.evaluationLabel || row.label || null);
    }
  }
  // analysis-core uses nested processed classifications in segments / top-level lists
  const classified = [];
  if (Array.isArray(analysis.processedOrganizations)) {
    for (const row of analysis.processedOrganizations) classified.push(row);
  }
  // Fall back: scan analysis for organizationName blocks near evaluation labels
  const processed = (trace.processed || []).map((row) => {
    const evaluationLabel =
      labelByOpp.get(row.opportunityId) ||
      (row.disposition === "WATCHLIST_RESEARCH"
        ? "NEW_UNIQUE"
        : row.disposition === "EXCLUDE"
          ? "REJECTED"
          : row.disposition === "DUPLICATE" ||
              String(row.opportunityId || "").includes("plp")
            ? "DUPLICATE"
            : null);
    return {
      ...row,
      evaluationLabel:
        evaluationLabel ||
        (row.disposition === "WATCHLIST_RESEARCH" ? "NEW_UNIQUE" : null),
      hotelQualified: row.disposition === "WATCHLIST_RESEARCH",
      retainGlobally: row.disposition !== "EXCLUDE",
    };
  });

  // Enrich labels from analysis-core classificationCounts companion lists if present
  const analysisProcessed = Array.isArray(analysis.processed)
    ? analysis.processed
    : [];
  for (const row of analysisProcessed) {
    const match = processed.find(
      (p) =>
        p.opportunityId === row.opportunityId ||
        p.organizationName === row.organizationName,
    );
    if (match && row.evaluationLabel) match.evaluationLabel = row.evaluationLabel;
  }

  return {
    runId: trace.runId || BETHESDA_LIVE_EXA_RUN.runId,
    hotelId: BETHESDA_LIVE_EXA_RUN.hotelId,
    fairSchedulerTotalQueued: trace.fairScheduler?.totalQueued || 165,
    airtableWritesReported: trace.airtableWrites ?? 0,
    processed,
    classifiedExtra: classified,
    analysisSummary: {
      extracted: analysis.discovery?.extracted ?? 165,
      processedCount: analysis.processedCount ?? processed.length,
      classificationCounts: analysis.classificationCounts || null,
      dispositions: analysis.dispositions || null,
    },
  };
}

function localHotelOpportunityCount(hotelId) {
  const doc = readJson(
    `data/group-demand-intelligence/hotels/${hotelId}/opportunities.json`,
  );
  return Array.isArray(doc?.opportunities) ? doc.opportunities.length : 0;
}

/**
 * Fixture-based reconciliation — expected Airtable destinations vs reported writes.
 * Live Airtable population counts come from prior schema audit (not mutated here).
 */
export function reconcileBethesdaLiveExaAirtable(opts = {}) {
  const loaded = loadBethesdaLiveExaProcessedCandidates(opts);
  const dry = dryRunFullE2eAirtablePersistence({
    hotelId: loaded.hotelId,
    displayName: BETHESDA_LIVE_EXA_RUN.hotelName,
    city: "Bethesda",
    runId: loaded.runId,
    candidates: loaded.processed,
  });

  const priorAudit = {
    // From reports/gdi-airtable-complete-data-model-audit-v1/01-AIRTABLE_TABLE_INVENTORY.csv
    groupDemandOpportunitiesBethesda: 86,
    demandGeneratorSignalsBethesda: 23,
    hotelDemandGeneratorFitBethesda: 20,
    researchTargetsBethesda: 53,
    researchRunsBethesda: 5,
    researchTargetRunsBethesda: 168,
    hotelVenueFitBethesda: 11,
    asOf: "2026-11-13 schema audit (pre-live-Exa campaign writes=0)",
  };

  const expectedFromProcessed = {
    demandGenerators: dry.byTable["Demand Generators"] || 0,
    privateEventVenues: dry.byTable["Private Event Venues"] || 0,
    groupDemandOpportunities:
      dry.byTable["Group Demand Opportunities"] || 0,
    researchTargets: dry.byTable["GDI Research Targets"] || 0,
    deferredAuditOnly: dry.deferredAuditOnly,
  };

  const rawNotBackfilled = {
    rawCandidates: loaded.analysisSummary.extracted,
    policy:
      "Do not automatically backfill all 165 raw candidates into Airtable. Unprocessed rows remain in bounded audit / global intelligence FS until screened.",
    expectedAirtableForRaw: 0,
  };

  return {
    version: GDI_BETHESDA_EXA_AIRTABLE_RECONCILIATION_V1,
    run: BETHESDA_LIVE_EXA_RUN,
    routingInventory: listCanonicalAirtableRoutingInventory(),
    localFs: {
      hotelOpportunitiesJson: localHotelOpportunityCount(loaded.hotelId),
    },
    campaignReported: {
      airtableWrites: loaded.airtableWritesReported,
      extracted: loaded.analysisSummary.extracted,
      processed: loaded.processed.length,
    },
    priorAirtableCoverageAudit: priorAudit,
    dryRunPlanForProcessed20: {
      plannedWriteCount: dry.plannedWriteCount,
      byTable: dry.byTable,
      deferredAuditOnly: dry.deferredAuditOnly,
      expectedFromProcessed,
    },
    rawNotBackfilled,
    findings: [
      {
        code: "CAMPAIGN_REPORTED_ZERO_AIRTABLE_WRITES",
        detail:
          "Live Exa run explicitly recorded airtableWrites=0 — not equivalent to empty Airtable historically.",
      },
      {
        code: "PRIOR_BETHESDA_OPPORTUNITY_ROWS_EXIST",
        detail: `Schema audit counted ${priorAudit.groupDemandOpportunitiesBethesda} Bethesda Group Demand Opportunities before this campaign.`,
      },
      {
        code: "PROCESSED_20_SHOULD_MAP_SELECTIVELY",
        detail:
          "Only hotel-qualified / watchlist / venue / planner identities get Airtable plans; EXCLUDE rows stay audit/global FS.",
      },
      {
        code: "DO_NOT_BACKFILL_165_RAW",
        detail: rawNotBackfilled.policy,
      },
      {
        code: "PRODUCTION_WRITE_NOT_AUTHORIZED",
        detail: "Reconciliation is read-only / dry-run; founder approval required before apply.",
      },
    ],
    referenceHotels: [
      {
        hotelId: "recrPQcZg7SFARRb2",
        name: "YOTEL Geneva Lake",
        localOpportunities: localHotelOpportunityCount("recrPQcZg7SFARRb2"),
      },
      {
        hotelId: "recnPB3Q02NO8Ma1o",
        name: "Wyndham San Diego Bayside",
        localOpportunities: localHotelOpportunityCount("recnPB3Q02NO8Ma1o"),
      },
      {
        hotelId: "rec82D9zpB8fede1I",
        name: "Castillo Hotel Son Vida",
        localOpportunities: localHotelOpportunityCount("rec82D9zpB8fede1I"),
      },
      {
        hotelId: "recH9h3McgMNFQEla",
        name: "Radisson Blu Toronto Downtown",
        localOpportunities: localHotelOpportunityCount("recH9h3McgMNFQEla"),
      },
    ],
    tablesInScope: Object.values(GDI_AIRTABLE_TABLES).map((t) => ({
      name: t.name,
      tableId: t.tableId,
    })),
    dryRun: dry,
  };
}

export default {
  GDI_BETHESDA_EXA_AIRTABLE_RECONCILIATION_V1,
  BETHESDA_LIVE_EXA_RUN,
  loadBethesdaLiveExaProcessedCandidates,
  reconcileBethesdaLiveExaAirtable,
};
