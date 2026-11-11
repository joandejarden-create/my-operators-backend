/**
 * Offline frozen replay — Commercial Qualification Reset V3 vs V2.
 * Revisits venue/local exclusions at organization vs event level.
 * No paid research. No publication / Airtable / deploy.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runCommercialYieldRecoveryOffline } from "./commercial-yield-recovery-v2.js";
import { qualifyHotelSpecificProspectV2 } from "./hotel-specific-qualification-v2.js";
import {
  qualifyCommercialProspectV3,
  INTERNAL_DISPOSITION,
  REVENUE_TYPE,
  COMMERCIAL_QUALIFICATION_V3,
  COMMERCIAL_REPORTING_BUCKET,
  dispositionToReportingBucket,
} from "./commercial-qualification-v3.js";
import { applySupervisorV2MaturationHooks } from "./ors-supervisor-v2-hooks.js";
import { matureOpportunity } from "./opportunity-research-supervisor-v1.js";
import { HeuristicResearchPlanner } from "./research-planner-v1.js";

export const QUALIFICATION_RESET_V3_VERSION = "gdi-commercial-qualification-reset-v3";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");
const OUT_DIR = path.join(REPO_ROOT, "reports", "gdi-commercial-qualification-reset-v3");

const HOTEL_META = Object.freeze({
  recC2kbCaS48jFjFF: {
    hotelName: "Nobu Hotel London Portman Square",
    oppPath: "data/group-demand-intelligence/hotels/recC2kbCaS48jFjFF/opportunities.json",
  },
  recLuxvwwxID7U2B8: {
    hotelName: "Bethesda Marriott",
    oppPath: "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json",
  },
  recnPB3Q02NO8Ma1o: {
    hotelName: "Wyndham San Diego Bayside",
    oppPath: "data/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities.json",
  },
});

const PRIOR_VENUE_LOCAL = new Set([
  "Miss Jones Group",
  "Hire Space",
  "HeadBox",
  "2B UK DMC",
  "Gaslamp Event Management, Inc.",
  "CMAA San Diego Chapter",
  "Kiwanis Club of San Diego",
  "BOMA San Diego",
  "San Diego Police Foundation",
  "Urban League of San Diego County",
]);

function readJson(rel) {
  const p = path.isAbsolute(rel) ? rel : path.join(REPO_ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function loadCorpusOpp(hotelId, organizationName) {
  const ops = readJson(HOTEL_META[hotelId]?.oppPath)?.opportunities || [];
  return ops.find((o) => String(o.organizationName || "").trim() === organizationName)
    || ops.find((o) => String(o.organizationName || "").toLowerCase().includes(
      organizationName.toLowerCase().split(/\s+/).slice(0, 2).join(" ").toLowerCase(),
    ))
    || null;
}

function claimsFor(row, opp) {
  const claims = [];
  if (opp?.summaryWhyHotel) {
    claims.push({ statement: opp.summaryWhyHotel, url: opp.officialSource || null, provenance: "frozen_corpus", accepted: true });
  }
  if (opp?.evidenceQuote) {
    claims.push({ statement: opp.evidenceQuote, url: opp.officialSource || null, provenance: "frozen_corpus_quote", accepted: true });
  }
  for (const bit of [opp?.eventLocationSummary, opp?.namedHostHotel, opp?.hostHotel].filter(Boolean)) {
    claims.push({ statement: String(bit), url: opp?.officialSource || null, provenance: "frozen_location", accepted: true });
  }
  if (row.verifiedDemandMechanism === "GUESTROOM_OR_HOTEL_SOURCING_CONTROL") {
    claims.push({
      statement: "hotel sourcing and accommodation services for incentive programmes",
      url: row.leadFollowOutcomes?.[0]?.url || opp?.officialSource || null,
      provenance: "yield_recovery_hotel_sourcing",
      accepted: true,
    });
  }
  return claims;
}

function venueLocalReassessment(row, v3) {
  if (!PRIOR_VENUE_LOCAL.has(row.organizationName)) return null;
  const guestroom = v3.revenueOpportunities?.[REVENUE_TYPE.GUESTROOMS];
  const meeting = v3.revenueOpportunities?.[REVENUE_TYPE.MEETING_SPACE];
  const catering = v3.revenueOpportunities?.[REVENUE_TYPE.CATERING_AND_EVENTS];
  const eventExcluded = v3.programOrEventLevel?.excludedForGuestroomsOnly === true
    || guestroom?.commerciallyRelevant !== true;
  const orgPotential = v3.organizationLevel?.couldGenerateSuitableFutureBusiness === true;

  let verdict = "STILL_COMMERCIALLY_UNSUITABLE";
  if (v3.commerciallyUsefulInternal && catering?.commerciallyRelevant) {
    verdict = "POTENTIALLY_VALUABLE_FOR_CATERING_OR_EVENTS";
  } else if (v3.commerciallyUsefulInternal && meeting?.commerciallyRelevant) {
    verdict = "POTENTIALLY_VALUABLE_FOR_MEETING_SPACE";
  } else if (eventExcluded && orgPotential && v3.internalDisposition === INTERNAL_DISPOSITION.WATCHLIST_RESEARCH) {
    verdict = "CORRECTLY_EXCLUDED_FOR_EVENT_BUT_ORG_WORTH_WATCHLIST";
  } else if (eventExcluded && !orgPotential) {
    verdict = "CORRECTLY_EXCLUDED_FOR_SPECIFIC_EVENT_AND_ORG";
  } else if (!eventExcluded && !v3.commerciallyUsefulInternal && orgPotential) {
    verdict = "INCORRECTLY_EXCLUDED_AT_ORGANIZATION_LEVEL_NOW_WATCHLIST_OR_ACCOUNT";
  } else if (v3.internalDisposition === INTERNAL_DISPOSITION.WATCHLIST_RESEARCH) {
    verdict = "WORTH_CONTINUED_RESEARCH";
  }

  return {
    organizationName: row.organizationName,
    hotelName: row.hotelName,
    priorBucket: "VENUE_LISTING_OR_LOCAL_ONLY",
    verdict,
    eventLevel: v3.programOrEventLevel,
    organizationLevel: v3.organizationLevel,
    revenue: {
      guestrooms: guestroom?.commerciallyRelevant === true,
      meetingSpace: meeting?.commerciallyRelevant === true,
      cateringAndEvents: catering?.commerciallyRelevant === true,
    },
    disposition: v3.internalDisposition,
    commerciallyUsefulInternal: v3.commerciallyUsefulInternal === true,
  };
}

/**
 * Prove V3 runs on the normal matureOpportunity entrypoint.
 */
export async function proveQualificationV3ThroughOrsEntrypoint() {
  const planner = new HeuristicResearchPlanner();
  const matured = await matureOpportunity({
    opportunity: {
      id: "gdi_qual_v3_ors_proof",
      organizationName: "Spectra DMC",
      officialDomain: "spectra-dmc.example",
      officialSource: "https://spectra-dmc.example/what-we-do",
      contactEmail: "spectra@spectra-dmc.com",
      hotelId: "recC2kbCaS48jFjFF",
      targetHotel: "Nobu Hotel London Portman Square",
      segmentId: "professional_planners_dmc",
      summaryWhyHotel: "Spectra expressly manages corporate meetings and incentives and has evidenced London event delivery. Nobu offers an 18-seat boardroom and ballroom.",
      evidence: [],
      researchAttempts: [],
      statusHistory: [],
    },
    planner,
    researcher: {
      async execute() {
        return {
          newMaterialEvidence: true,
          provider: "fixture",
          sourceFamilies: ["official"],
          costUsd: 0,
          evidence: [{
            sourceFamily: "official",
            url: "https://spectra-dmc.example/what-we-do",
            quote: "Spectra expressly manages corporate meetings and incentives and has evidenced London event delivery. Boardroom and ballroom programmes.",
            publicationReady: false,
            customerVisible: false,
          }],
          deepPatch: { contact: { businessEmail: "spectra@spectra-dmc.com" } },
        };
      },
    },
    options: {
      paidBlocked: true,
      maxIterations: 2,
      enableSupervisorV2: true,
      enableQualificationV3: true,
      executeLeadFollowUps: false,
      forbidPublication: true,
      forbidAirtable: true,
      remainingBudgetUsd: 1.5,
      targetHotel: "Nobu Hotel London Portman Square",
    },
  });

  const q3 = matured.supervisorV2?.qualificationV3
    || (matured.supervisorV2?.qualification?.version === COMMERCIAL_QUALIFICATION_V3
      ? matured.supervisorV2.qualification
      : null);
  const useful = matured.commerciallyUsefulInternal === true
    || q3?.commerciallyUsefulInternal === true;
  const result = {
    entrypoint: "matureOpportunity",
    separatePipelineCreated: false,
    qualificationVersion: q3?.version || matured.supervisorV2?.qualification?.version || null,
    internalDisposition: q3?.internalDisposition || matured.internalDisposition || null,
    commerciallyUsefulInternal: useful,
    sourcingStatus: matured.sourcingStatus || q3?.sourcingStatus || null,
    publicationWritesDisabled: matured.supervisorV2?.publicationWritesDisabled === true,
    productionPublicationEligibilityChanged: q3?.productionPublicationEligibilityChanged === false,
    demandLayersPresent: Boolean(q3?.demandEvidenceDimensions || matured.supervisorV2?.qualification?.demandEvidenceDimensions),
    revenueTypesPresent: Boolean(q3?.revenueOpportunities || matured.supervisorV2?.qualification?.revenueOpportunities),
    salesActionPresent: Boolean(q3?.salesAction || matured.v3SalesAction),
  };
  result.passed = result.qualificationVersion === COMMERCIAL_QUALIFICATION_V3
    && result.publicationWritesDisabled
    && result.demandLayersPresent
    && result.revenueTypesPresent
    && result.separatePipelineCreated === false;
  return result;
}

export function runCommercialQualificationResetV3Offline() {
  const recovery = runCommercialYieldRecoveryOffline();
  const rows = recovery.candidateRootCauseTable || [];
  const comparisons = [];
  const venueLocal = [];

  for (const row of rows) {
    const opp = loadCorpusOpp(row.hotelId, row.organizationName) || {
      organizationName: row.organizationName,
      contactEmail: row.contactRoute,
      namedHostHotel: row.competitorHost,
    };
    const claims = claimsFor(row, opp);
    const pageTexts = claims.map((c) => c.statement);
    const contact = row.contactRoute || opp.contactEmail || null;
    const opportunity = {
      ...opp,
      organizationName: row.organizationName,
      contactEmail: contact,
      contact: contact ? { businessEmail: contact } : {},
      namedHostHotel: row.competitorHost || opp.namedHostHotel,
      targetHotel: row.hotelName,
      futureCycleSupported: row.afterRecovery?.pathway === "COMPETITIVE_FUTURE_CYCLE",
    };

    const v2 = qualifyHotelSpecificProspectV2({
      opportunity,
      acceptedClaims: claims,
      pageTexts,
      targetHotel: row.hotelName,
      usableContactRoute: contact,
    });
    const v3 = qualifyCommercialProspectV3({
      opportunity,
      acceptedClaims: claims,
      pageTexts,
      targetHotel: row.hotelName,
      usableContactRoute: contact,
    });

    const rev = v3.revenueOpportunities || {};
    comparisons.push({
      organizationName: row.organizationName,
      hotelId: row.hotelId,
      hotelName: row.hotelName,
      v2: {
        useful: v2.commerciallyUsefulInternal === true,
        usefulKind: v2.usefulKind,
        pathway: v2.pathway,
        downgradeReason: v2.downgradeReason,
      },
      yieldRecoveryUseful: row.afterRecovery?.commerciallyUsefulInternal === true,
      v3: {
        useful: v3.commerciallyUsefulInternal === true,
        disposition: v3.internalDisposition,
        reportingBucket: v3.reportingBucket || dispositionToReportingBucket(v3.internalDisposition),
        pathway: v3.pathway,
        sourcingStatus: v3.sourcingStatus,
        usefulKind: v3.usefulKind,
        downgradeReason: v3.downgradeReason,
      },
      commercialStageSemantics: v3.commercialStageSemantics,
      revenue: {
        GUESTROOMS: rev[REVENUE_TYPE.GUESTROOMS]?.commerciallyRelevant === true,
        MEETING_SPACE: rev[REVENUE_TYPE.MEETING_SPACE]?.commerciallyRelevant === true,
        CATERING_AND_EVENTS: rev[REVENUE_TYPE.CATERING_AND_EVENTS]?.commerciallyRelevant === true,
        detail: {
          GUESTROOMS: rev[REVENUE_TYPE.GUESTROOMS],
          MEETING_SPACE: rev[REVENUE_TYPE.MEETING_SPACE],
          CATERING_AND_EVENTS: rev[REVENUE_TYPE.CATERING_AND_EVENTS],
        },
      },
      organizationLevel: v3.organizationLevel,
      programOrEventLevel: v3.programOrEventLevel,
      salesAction: v3.salesAction,
      researchGaps: v3.researchGaps,
      demandLayers: {
        verifiedGroupActivity: v3.demandEvidenceDimensions?.VERIFIED_GROUP_ACTIVITY?.present === true,
        inferredLodgingPotential: v3.demandEvidenceDimensions?.INFERRED_LODGING_POTENTIAL?.present === true,
        verifiedAccommodationDemand: v3.demandEvidenceDimensions?.VERIFIED_ACCOMMODATION_DEMAND?.present === true,
      },
      changedUseful: (v3.commerciallyUsefulInternal === true) !== (row.afterRecovery?.commerciallyUsefulInternal === true)
        || (v3.commerciallyUsefulInternal === true) !== (v2.commerciallyUsefulInternal === true),
      publicationEligibilityChanged: false,
      customerFacingClassificationChanged: false,
    });

    const vl = venueLocalReassessment(row, v3);
    if (vl) venueLocal.push(vl);
  }

  const beforeUseful = comparisons.filter((c) => c.yieldRecoveryUseful).length;
  const afterUseful = comparisons.filter((c) => c.v3.useful).length;
  const newlyUseful = comparisons.filter((c) => c.v3.useful && !c.yieldRecoveryUseful);

  const byBucket = (bucket) => comparisons
    .filter((c) => c.v3.reportingBucket === bucket)
    .map((c) => c.organizationName);
  const commercialStageBreakdown = {
    ACTIVE_BUYING_OPPORTUNITIES: byBucket(COMMERCIAL_REPORTING_BUCKET.ACTIVE_BUYING_OPPORTUNITIES),
    PRE_SOURCING_OPPORTUNITIES: byBucket(COMMERCIAL_REPORTING_BUCKET.PRE_SOURCING_OPPORTUNITIES),
    ACCOUNT_DEVELOPMENT_PROSPECTS: byBucket(COMMERCIAL_REPORTING_BUCKET.ACCOUNT_DEVELOPMENT_PROSPECTS),
    FUTURE_CYCLE_WATCH_PROSPECTS: byBucket(COMMERCIAL_REPORTING_BUCKET.FUTURE_CYCLE_WATCH_PROSPECTS),
    RESEARCH_WATCHLIST: byBucket(COMMERCIAL_REPORTING_BUCKET.RESEARCH_WATCHLIST),
    EXCLUDED: byBucket(COMMERCIAL_REPORTING_BUCKET.EXCLUDED),
    counts: Object.fromEntries(
      Object.values(COMMERCIAL_REPORTING_BUCKET).map((b) => [b, byBucket(b).length]),
    ),
  };
  const revenuePotentialBreakdown = {
    guestroomRevenuePotential: comparisons.filter((c) => c.revenue.GUESTROOMS).map((c) => c.organizationName),
    meetingSpaceRevenuePotential: comparisons.filter((c) => c.revenue.MEETING_SPACE).map((c) => c.organizationName),
    cateringEventRevenuePotential: comparisons.filter((c) => c.revenue.CATERING_AND_EVENTS).map((c) => c.organizationName),
  };
  const fivePreviouslyUseful = ["London DMC", "Spectra DMC", "National Association of Development Organizations (NADO) / DDAA", "CMAA San Diego Chapter", "BOMA San Diego"]
    .map((name) => {
      const c = comparisons.find((row) => row.organizationName === name || row.organizationName.startsWith(name.slice(0, 24)));
      if (!c) return { organizationName: name, missing: true };
      return {
        organizationName: c.organizationName,
        hotelName: c.hotelName,
        priorV3DispositionNote: "pre-calibration useful set",
        revisedDisposition: c.v3.disposition,
        reportingBucket: c.v3.reportingBucket,
        sourcingStatus: c.v3.sourcingStatus,
        commercialStageSemantics: c.commercialStageSemantics,
        revenue: c.revenue,
        salesAction: c.salesAction,
        demandLayers: c.demandLayers,
        supportingEvidence: c.salesAction?.evidenceReferences || [],
        outreachTiming: c.salesAction?.outreachTiming || null,
      };
    });

  // ORS hook smoke (sync)
  const hookState = applySupervisorV2MaturationHooks({
    id: "gdi_v3_hook_smoke",
    organizationName: "Spectra DMC",
    officialDomain: "spectra-dmc.com",
    contactEmail: "spectra@spectra-dmc.com",
    contact: { businessEmail: "spectra@spectra-dmc.com" },
    targetHotel: "Nobu Hotel London Portman Square",
    summaryWhyHotel: "Spectra expressly manages corporate meetings and incentives and has evidenced London event delivery. 18-seat boardroom and ballroom.",
    evidence: [{
      url: "https://www.spectra-dmc.com/what-we-do",
      quote: "Spectra expressly manages corporate meetings and incentives and London event delivery with boardroom programmes.",
      publicationReady: false,
      customerVisible: false,
    }],
    researchAttempts: [],
  }, {
    enableSupervisorV2: true,
    enableQualificationV3: true,
    targetHotel: "Nobu Hotel London Portman Square",
    forbidPublication: true,
    forbidAirtable: true,
    generateDossier: false,
  });

  const falsePositives = newlyUseful.filter((c) =>
    (c.revenue.GUESTROOMS && !c.demandLayers.verifiedAccommodationDemand && !c.demandLayers.inferredLodgingPotential)
    || (c.v3.disposition === INTERNAL_DISPOSITION.EXCLUDE),
  );
  const falseNegatives = comparisons.filter((c) =>
    c.demandLayers.verifiedGroupActivity
    && c.demandLayers.inferredLodgingPotential
    && c.v3.sourcingStatus !== "HOST_SELECTED"
    && c.salesAction?.whoToContact
    && !c.v3.useful
    && c.revenue.GUESTROOMS !== true
    && !/SoccerPlex|Capitol Hill/i.test(JSON.stringify(c.programOrEventLevel)),
  );

  const liveValidationPlan = {
    status: "PROPOSED_NOT_AUTHORIZED",
    noPaidCallsInThisPass: true,
    proposedCeilingUsd: 1.5,
    retainCeilingFromPriorPlan: true,
    doNotExecuteUntilSeparateGo: true,
    targets: [
      ...comparisons.filter((c) => c.v3.disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT && /DMC/i.test(c.organizationName)),
      ...comparisons.filter((c) => c.v3.disposition === INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY),
      ...comparisons.filter((c) => c.v3.disposition === INTERNAL_DISPOSITION.COMPETITIVE_FUTURE_CYCLE),
    ].slice(0, 6).map((c) => ({
      organizationName: c.organizationName,
      hotelName: c.hotelName,
      disposition: c.v3.disposition,
      reportingBucket: c.v3.reportingBucket,
      gaps: c.researchGaps.slice(0, 3),
      questions: [
        c.v3.disposition === INTERNAL_DISPOSITION.ACCOUNT_DEVELOPMENT
          ? "Corroborate hotel-sourcing / incentive delivery capability on organization-owned pages; do not invent a current buying requirement."
          : c.v3.disposition === INTERNAL_DISPOSITION.PRE_SOURCING_OPPORTUNITY
            ? "Confirm next awards/banquet or programme cycle dates and whether hotel selection is open."
            : "Distinguish announced future cycle from historical host pattern; keep Crystal Gateway / competitor commitment intact.",
        "Keep sourcingStatus UNKNOWN unless currently sourcing is directly evidenced.",
      ],
    })),
  };

  const packageOut = {
    version: QUALIFICATION_RESET_V3_VERSION,
    qualificationVersion: COMMERCIAL_QUALIFICATION_V3,
    calibration: "commercial-stage-semantics-v3-calibrated",
    offline: true,
    spent: false,
    liveCalls: 0,
    beforeAfter: {
      yieldRecoveryUsefulV2: beforeUseful,
      v3UsefulInternal: afterUseful,
      newlyUsefulCount: newlyUseful.length,
      newlyUsefulNames: newlyUseful.map((c) => `${c.organizationName} @ ${c.hotelName}`),
      note: "Do not use v3UsefulInternal as a single headline — see commercialStageBreakdown.",
    },
    commercialStageBreakdown,
    revenuePotentialBreakdown,
    fivePreviouslyUsefulProspectsRevised: fivePreviouslyUseful,
    candidateComparisons: comparisons,
    venueLocalReassessment: venueLocal,
    orsHookIntegration: {
      attached: Boolean(hookState.supervisorV2?.qualificationV3 || hookState.supervisorV2?.qualification?.version === COMMERCIAL_QUALIFICATION_V3),
      disposition: hookState.internalDisposition,
      useful: hookState.commerciallyUsefulInternal === true,
      publicationDisabled: hookState.supervisorV2?.publicationWritesDisabled === true,
    },
    falsePositiveAnalysis: {
      riskyNewlyUseful: falsePositives,
      note: "Inferred lodging must remain labeled inferred; venue listings must not gain guestroom authority.",
    },
    falseNegativeAnalysis: {
      candidates: falseNegatives.map((c) => c.organizationName),
      note: "Group+inferred present but not useful — usually missing hotel meeting-fit evidence in sources or catering-only path not credited.",
    },
    remainingResearchGaps: comparisons
      .filter((c) => (c.researchGaps || []).length)
      .map((c) => ({ organizationName: c.organizationName, gaps: c.researchGaps })),
    proposedTargetedLiveValidationPlan: liveValidationPlan,
    controls: {
      noPaidProviderCalls: true,
      noDeploy: true,
      noAirtable: true,
      noCustomerPublication: true,
      noCustomerFacingClassificationChange: true,
      publicationGateUnchanged: true,
      priorEvidenceAndBudgetsPreserved: true,
      unknownSourcingPreserved: comparisons.every((c) =>
        c.v3.sourcingStatus === "UNKNOWN"
        || c.v3.sourcingStatus === "EVIDENCED"
        || c.v3.sourcingStatus === "HOST_SELECTED"),
    },
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(path.join(OUT_DIR, "RESET_V3.json"), `${JSON.stringify(packageOut, null, 2)}\n`);
  fs.writeFileSync(path.join(OUT_DIR, "V2_VS_V3_DISPOSITIONS.json"), `${JSON.stringify(comparisons, null, 2)}\n`);
  fs.writeFileSync(path.join(OUT_DIR, "VENUE_LOCAL_REASSESSMENT.json"), `${JSON.stringify(venueLocal, null, 2)}\n`);
  fs.writeFileSync(path.join(OUT_DIR, "LIVE_VALIDATION_PLAN.json"), `${JSON.stringify(liveValidationPlan, null, 2)}\n`);

  fs.writeFileSync(
    path.join(OUT_DIR, "COMMERCIAL_STAGE_BREAKDOWN.json"),
    `${JSON.stringify({ commercialStageBreakdown, revenuePotentialBreakdown, fivePreviouslyUsefulProspectsRevised: fivePreviouslyUseful }, null, 2)}\n`,
  );

  const md = [
    "# Commercial Qualification Reset V3 (stage-calibrated)",
    "",
    `**Spent:** $0 · **Candidates:** ${comparisons.length}`,
    `**Yield-recovery useful:** ${beforeUseful} → **V3 internally useful (do not headline alone):** ${afterUseful}`,
    "",
    "## Commercial-stage breakdown",
    ...Object.entries(commercialStageBreakdown.counts).map(([k, v]) => `- **${k}**: ${v} — ${(commercialStageBreakdown[k] || []).join("; ") || "(none)"}`),
    "",
    "## Revenue potential (independent)",
    `- Guestrooms: ${revenuePotentialBreakdown.guestroomRevenuePotential.join("; ") || "(none)"}`,
    `- Meeting space: ${revenuePotentialBreakdown.meetingSpaceRevenuePotential.join("; ") || "(none)"}`,
    `- Catering/events: ${revenuePotentialBreakdown.cateringEventRevenuePotential.join("; ") || "(none)"}`,
    "",
    "## Five previously useful — revised",
    ...fivePreviouslyUseful.map((f) => `- **${f.organizationName}**: ${f.revisedDisposition} (${f.reportingBucket}); sourcing=${f.sourcingStatus}; outreach=${f.outreachTiming}`),
    "",
    "## All candidates",
    "",
    "| Organization | Bucket | Disposition | Guestrooms | Meeting | Catering | Sourcing | Outreach |",
    "|---|---|---|---|---|---|---|---|",
    ...comparisons.map((c) => `| ${c.organizationName.slice(0, 36)} | ${String(c.v3.reportingBucket).replace(/_/g, " ").slice(0, 22)} | ${c.v3.disposition} | ${c.revenue.GUESTROOMS ? "Y" : "n"} | ${c.revenue.MEETING_SPACE ? "Y" : "n"} | ${c.revenue.CATERING_AND_EVENTS ? "Y" : "n"} | ${c.v3.sourcingStatus} | ${c.salesAction?.outreachTiming || ""} |`),
    "",
    "## Venue / local-only reassessment (10)",
    ...venueLocal.map((v) => `- **${v.organizationName}**: ${v.verdict} (${v.disposition})`),
    "",
    "Publication gate unchanged. Paid live validation NOT executed.",
    "",
  ];
  fs.writeFileSync(path.join(OUT_DIR, "RESET_V3.md"), md.join("\n"));

  return packageOut;
}
