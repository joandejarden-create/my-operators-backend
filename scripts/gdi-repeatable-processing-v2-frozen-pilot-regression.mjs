#!/usr/bin/env node
/**
 * GDI Repeatable Processing V2 — zero-paid frozen Design Hotels pilot regression.
 * Uses only saved evidence / fixtures. No Exa spend. No Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { dedupeCandidatesAcrossSegments } from "../lib/group-demand-intelligence/research/gdi-cross-segment-opportunity-dedupe-v2.js";
import {
  selectCandidatesForDeepResearch,
  scoreHotelCapabilityFit,
} from "../lib/group-demand-intelligence/research/gdi-deep-research-selection-register-v1.js";
import { validateDesignHotelsPilotConfigProposals } from "../lib/group-demand-intelligence/workflow/gdi-hotel-config-proposal-v2.js";
import {
  analyzeDesignHotelsContactCoverage,
  evaluateNamedContactRetention,
} from "../lib/group-demand-intelligence/research/gdi-public-contact-research-policy-v2.js";
import { loadHotelGdiConfig } from "../lib/group-demand-intelligence/research/commercial-demand-discovery-v4.js";
import { parseHotelsInput } from "../lib/group-demand-intelligence/workflow/gdi-universal-cohort-processing-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports", "gdi-repeatable-processing-v2");
const PILOT = path.join(
  ROOT,
  "reports",
  "gdi-design-hotels-live-research-pilot-v1",
);
const CERT = path.join(
  ROOT,
  "reports",
  "gdi-design-hotels-commercial-certification-v1",
);

const HOTELS = [
  {
    hotelId: "rece0NdV9FCnaFlVS",
    name: "Condesa DF Hotel",
    runDir: "hotels/rece0NdV9FCnaFlVS/gdi_dh_live_20261123141330__rece0NdV9FCnaFlVS",
  },
  {
    hotelId: "recaokE8psIbuVeuH",
    name: "LaLuna Boutique Hotels & Villas",
    runDir: "hotels/recaokE8psIbuVeuH/gdi_dh_live_20261123141330__recaokE8psIbuVeuH",
  },
  {
    hotelId: "rec1t35e3sPerzxYu",
    name: "Memmo Príncipe Real",
    runDir: "hotels/rec1t35e3sPerzxYu/gdi_dh_live_20261123141330__rec1t35e3sPerzxYu",
  },
];

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(p, body) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(body, null, 2)}\n`, "utf8");
}

function loadEligibleFromPreDeep(runDir) {
  const p = path.join(PILOT, runDir, "pre-deep-candidate-screening.json");
  const doc = readJson(p);
  return doc.eligibleCandidates || [];
}

function loadHistoricalSelected(runDir) {
  const p = path.join(PILOT, runDir, "deep-research-selection-register.json");
  if (!fs.existsSync(p)) return [];
  const doc = readJson(p);
  return (doc.selected || []).map((o) => o.id || o.opportunityId);
}

function loadProcessedContacts(runDir) {
  const p = path.join(PILOT, runDir, "chain-execution-trace.json");
  if (!fs.existsSync(p)) return [];
  const doc = readJson(p);
  const processed = doc.processed || doc.opportunitiesProcessed || [];
  return processed;
}

fs.mkdirSync(OUT, { recursive: true });

// --- Hotels file for universal runner ---
const hotelsFile = path.join(OUT, "design-hotels-pilot-cohort-hotels.txt");
fs.writeFileSync(
  hotelsFile,
  `${HOTELS.map((h) => h.hotelId).join("\n")}\n`,
  "utf8",
);
const parsed = parseHotelsInput({ hotelsFile, root: ROOT });

// --- Config proposal validation (offline vs existing configs; skip live HPC) ---
const configValidation = await validateDesignHotelsPilotConfigProposals({
  skipHpc: true,
});

// --- Per-hotel dedupe + re-rank ---
const hotelResults = [];
for (const h of HOTELS) {
  const cfg = loadHotelGdiConfig(h.hotelId);
  const eligible = loadEligibleFromPreDeep(h.runDir);
  const historicalSelected = loadHistoricalSelected(h.runDir);
  const beforeCount = eligible.length;

  const dedupe = dedupeCandidatesAcrossSegments(eligible);
  const afterDedupe = dedupe.canonicalCandidates;

  const selection = selectCandidatesForDeepResearch(afterDedupe, {
    hotelId: h.hotelId,
    hotelProfile: cfg,
    hotelConfig: cfg,
    maxCandidates: 4,
    fairMinPerSegment: 1,
    remainingBudgetUsd: null,
  });

  const selectedIds = (selection.selected || []).map(
    (o) => o.id || o.opportunityId,
  );
  const fitNotes = (selection.selected || []).map((o) => {
    const fit = scoreHotelCapabilityFit(o, {
      hotelProfile: cfg,
      hotelConfig: cfg,
    });
    return {
      opportunityId: o.id || o.opportunityId,
      title: o.title,
      organizationName: o.organizationName,
      fit,
    };
  });

  // Contact coverage from frozen packages + retention policy smoke
  const packages = readJson(path.join(PILOT, "HOTEL_INTELLIGENCE_PACKAGES.json"));
  const hotelPkg = (Array.isArray(packages) ? packages : Object.values(packages)).find(
    (x) => x.hotelId === h.hotelId,
  );
  const contactAnalysis = analyzeDesignHotelsContactCoverage(
    hotelPkg ? [hotelPkg] : [{ hotelId: h.hotelId, opportunities: [] }],
  );

  // Example retention: Salesforce-style named contact without email
  const retentionSmoke = evaluateNamedContactRetention({
    name: "Example Verified Buyer",
    role: "Travel Procurement",
    employer: "Example Corp",
    sourceUrl: "https://example.com/team",
    email: "",
  });

  hotelResults.push({
    hotelId: h.hotelId,
    hotelName: h.name,
    candidatesBefore: beforeCount,
    candidatesAfterDedupe: afterDedupe.length,
    duplicatesRemoved: dedupe.suppressedCount,
    duplicateClusters: dedupe.duplicateClusters,
    dedupeDecisions: dedupe.decisions.filter((d) => d.suppressedIds.length),
    historicalDeepSelectedIds: historicalSelected,
    historicalDeepSelectedCount: historicalSelected.length,
    v2DeepSelectedIds: selectedIds,
    v2DeepSelectedCount: selectedIds.length,
    crossSegmentDuplicatesSkippedAtSelection:
      selection.crossSegmentDuplicatesSkipped || 0,
    hotelFitNotes: fitNotes,
    contactAnalysis: contactAnalysis.byHotel[0] || null,
    retentionSmoke,
    carphaCase:
      h.hotelId === "recaokE8psIbuVeuH"
        ? {
            beforeHadBoth:
              eligible.some((c) => (c.id || c.opportunityId) === "disc_a6def3588729") &&
              eligible.some((c) => (c.id || c.opportunityId) === "disc_023923963f08"),
            afterCanonicalOnly: (() => {
              const ids = afterDedupe.map((c) => c.id || c.opportunityId);
              const hasA = ids.includes("disc_a6def3588729");
              const hasB = ids.includes("disc_023923963f08");
              return (hasA || hasB) && !(hasA && hasB);
            })(),
            suppressedPeer:
              dedupe.decisions.find(
                (d) =>
                  d.suppressedIds.includes("disc_023923963f08") ||
                  d.suppressedIds.includes("disc_a6def3588729"),
              ) || null,
          }
        : null,
    advantageCase:
      h.hotelId === "rec1t35e3sPerzxYu"
        ? {
            beforeHadBoth:
              eligible.some((c) => (c.id || c.opportunityId) === "disc_9cd2c082c05d") &&
              eligible.some((c) => (c.id || c.opportunityId) === "disc_51073448c775"),
            afterCanonicalOnly: (() => {
              const ids = afterDedupe.map((c) => c.id || c.opportunityId);
              const hasA = ids.includes("disc_9cd2c082c05d");
              const hasB = ids.includes("disc_51073448c775");
              return (hasA || hasB) && !(hasA && hasB);
            })(),
            suppressedPeer:
              dedupe.decisions.find(
                (d) =>
                  d.suppressedIds.includes("disc_51073448c775") ||
                  d.suppressedIds.includes("disc_9cd2c082c05d"),
              ) || null,
          }
        : null,
  });
}

// --- Revised persistence write-set (dry only) ---
const priorWriteSet = readJson(
  path.join(CERT, "PERSISTENCE_WRITE_SET_DRY.json"),
);
const excludeIds = new Set([
  ...(priorWriteSet.dedupeBeforeApply?.excludeOpportunityIds || []),
]);
// Ensure V2 suppresses both known near-dupes
excludeIds.add("disc_023923963f08");
excludeIds.add("disc_51073448c775");

const revisedRows = (priorWriteSet.recommendedWriteSet || []).filter(
  (r) => !excludeIds.has(r.opportunityId),
);

const revisedWriteSet = {
  contract: "gdi-repeatable-processing-v2-persistence-write-set-dry",
  mode: "DRY_RUN_ONLY",
  applyAuthorized: false,
  publicationAuthorized: false,
  paidResearchAuthorized: false,
  basedOn: priorWriteSet.contract,
  destination: priorWriteSet.destination,
  classifications: {
    strongEvidenceBacked: revisedRows
      .filter((r) => r.commercialEligibilityStatus === "CUSTOMER_ELIGIBLE_CANDIDATE")
      .map((r) => r.opportunityId),
    legitimateIncompleteContinuations: revisedRows
      .filter((r) => r.founderRecommend === "INCLUDE_AS_EXPLORATORY")
      .map((r) => r.opportunityId),
    duplicatesToSuppress: [...excludeIds],
    ineligible: (priorWriteSet.excludeSet || priorWriteSet.excluded || [])
      .map?.((x) => x.opportunityId || x) || [],
    customerEligibleCandidates: revisedRows
      .filter((r) => r.commercialEligibilityStatus === "CUSTOMER_ELIGIBLE_CANDIDATE")
      .map((r) => r.opportunityId),
    salesReady: revisedRows.filter((r) => r.salesReady === true).map((r) => r.opportunityId),
  },
  note:
    "Preserve exploratory association deeps internally; suppress cross-segment duplicates; no customer publication without separate authorization. Sales Ready remains 0.",
  recommendedWriteSet: revisedRows.map((r) => ({
    ...r,
    internalPreserve: true,
    customerPublicationEligible:
      r.commercialEligibilityStatus === "CUSTOMER_ELIGIBLE_CANDIDATE" &&
      r.salesReady !== true
        ? "CANDIDATE_ONLY_NOT_PUBLISHED"
        : r.salesReady === true
          ? "SALES_READY_REQUIRES_SEPARATE_AUTH"
          : "INTERNAL_ONLY",
  })),
  excludedDuplicates: [
    {
      opportunityId: "disc_023923963f08",
      reason: "CROSS_SEGMENT_DUPLICATE_OF_disc_a6def3588729_CARPHA",
      preserveEvidence: true,
      customerPublication: false,
    },
    {
      opportunityId: "disc_51073448c775",
      reason: "CROSS_SEGMENT_DUPLICATE_OF_disc_9cd2c082c05d_ADVANTAGE",
      preserveEvidence: true,
      customerPublication: false,
    },
  ],
};

writeJson(path.join(OUT, "PERSISTENCE_WRITE_SET_DRY_V2.json"), revisedWriteSet);
writeJson(path.join(OUT, "FROZEN_PILOT_REGRESSION.json"), {
  contract: "gdi-repeatable-processing-v2",
  mode: "ZERO_PAID_FROZEN_REPLAY",
  paidSpendUsd: 0,
  hotelsFile: path.relative(ROOT, hotelsFile).split(path.sep).join("/"),
  hotelsFileParseOk: parsed.ok,
  hotelIds: parsed.hotelIds,
  configValidation,
  hotelResults,
  summary: {
    totalDuplicatesRemoved: hotelResults.reduce(
      (s, h) => s + (h.duplicatesRemoved || 0),
      0,
    ),
    carphaDeduped: hotelResults.find((h) => h.carphaCase)?.carphaCase?.afterCanonicalOnly === true,
    advantageDeduped:
      hotelResults.find((h) => h.advantageCase)?.advantageCase?.afterCanonicalOnly ===
      true,
    salesReadyCount: revisedWriteSet.classifications.salesReady.length,
    configValidationOk: configValidation.ok,
  },
});

console.log(
  JSON.stringify(
    {
      ok: true,
      paidSpendUsd: 0,
      out: path.relative(ROOT, OUT).split(path.sep).join("/"),
      duplicatesRemoved: hotelResults.reduce(
        (s, h) => s + (h.duplicatesRemoved || 0),
        0,
      ),
      carphaDeduped: hotelResults.find((h) => h.carphaCase)?.carphaCase
        ?.afterCanonicalOnly,
      advantageDeduped: hotelResults.find((h) => h.advantageCase)?.advantageCase
        ?.afterCanonicalOnly,
      configValidationOk: configValidation.ok,
    },
    null,
    2,
  ),
);
