/**
 * Offline certification: commercial improvement measurement + deep-research selection.
 * Replays saved live artifacts for Gaslamp / Potomac / NADO. Ranks live corpus.
 * No paid calls.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assessCommercialImprovement,
  commercialImprovementContract,
  collectPriorContactRoutes,
  IMPROVEMENT_OUTCOME,
  COMMERCIAL_IMPROVEMENT_MEASUREMENT_V1,
} from "./commercial-improvement-measurement-v1.js";
import {
  selectShortlistForNextValidation,
  rankDeepResearchCandidates,
  PRIOR_TEST_OPPORTUNITY_IDS,
  DEEP_RESEARCH_OPPORTUNITY_SELECTION_V1,
} from "./deep-research-opportunity-selection-v1.js";
import { HARDENING_REPORTS_DIR } from "./commercial-depth-hardening-v1.js";

export const COMMERCIAL_IMPROVEMENT_SELECTION_CERT_V1 = "gdi-commercial-improvement-selection-cert-v1";
export const CERT_REPORTS_DIR = "reports/gdi-commercial-improvement-selection-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const LIVE_DIR = path.join(ROOT, HARDENING_REPORTS_DIR, "live");

const LIVE_IDS = [
  "gdi_lmc_97fb727376abb83b",
  "gdi_opp_potomac_memorial_2027",
  "gdi_opp_nado_ddaa_washcon_2028",
];

function loadLivePair(opportunityId) {
  const dir = path.join(LIVE_DIR, opportunityId);
  return {
    opportunityId,
    before: JSON.parse(fs.readFileSync(path.join(dir, "BEFORE.json"), "utf8")),
    after: JSON.parse(fs.readFileSync(path.join(dir, "AFTER.json"), "utf8")),
    brief: JSON.parse(fs.readFileSync(path.join(dir, "SALES_BRIEF.json"), "utf8")),
    cert: JSON.parse(fs.readFileSync(path.join(dir, "CERTIFICATION.json"), "utf8")),
  };
}

/**
 * Replay one saved live investigation through the new improvement model.
 */
export function replayLiveImprovementAssessment(opportunityId) {
  const { before, after, brief, cert } = loadLivePair(opportunityId);
  const priorRoutes = collectPriorContactRoutes(before, {
    contactEmail: before.contactEmail,
    hotelOpportunityThesis: before.description,
  });
  const afterRoute = (after.contactOwnership?.route || brief.whoShouldIContact?.route || "")
    .toLowerCase();
  const wouldHaveBeenPhantom = Boolean(afterRoute)
    && priorRoutes.has(afterRoute)
    && !before.contactEmail; // old bug: missing contactEmail on BEFORE

  const assessment = assessCommercialImprovement({
    before,
    after,
    opportunity: {
      id: opportunityId,
      hotelId: cert.hotelId || before.hotelId,
      hotelName: cert.hotelName || before.hotelName,
      organizationName: cert.organizationName || before.organizationName,
      contactEmail: (before.contactRoutes || [])[0] || before.contactEmail,
      hotelOpportunityThesis: before.description || (before.demandEvidence || [])[0]?.statement,
      competingHotel: /nado/i.test(opportunityId) ? "Crystal Gateway Marriott" : null,
      namedHostHotel: /nado/i.test(opportunityId) ? "Crystal Gateway Marriott" : null,
      namedStakeholders: before.knownStakeholders || [],
    },
    result: {
      hotelSalesBrief: brief,
      after,
    },
    technicalPass: cert.technicalCertification === "PASS",
    salesBriefClean: cert.salesBriefIntegrity?.clean !== false
      && !/Principal\.|Nobu Portman|Rosewood London/i.test(JSON.stringify(brief)),
  });

  return {
    opportunityId,
    hotelName: cert.hotelName,
    organizationName: cert.organizationName,
    pathway: cert.pathway || before.pathway,
    priorContactRoutes: [...priorRoutes],
    afterContactRoute: afterRoute || null,
    phantomContactGuard: {
      priorRoutePresentInBeforeSnapshot: Boolean(afterRoute) && priorRoutes.has(afterRoute),
      oldBugWouldCountAsNewBecauseContactEmailMissing: wouldHaveBeenPhantom,
      buyerAccessibilityAdvanced: assessment.dimensions?.BUYER_ACCESSIBILITY?.advanced === true,
      newUsableRoute: assessment.dimensions?.BUYER_ACCESSIBILITY?.newUsableRoute === true,
      priorRouteRestated: assessment.dimensions?.BUYER_ACCESSIBILITY?.priorRouteRestated === true,
    },
    outcome: assessment.outcome,
    materialDimensionCount: assessment.materialDimensionCount,
    limitedDimensionCount: assessment.limitedDimensionCount,
    improvements: assessment.improvements,
    novelEvidenceCount: assessment.novelEvidenceCount,
    assessment,
    assertions: {
      // Prior routes must never produce advanced buyer-accessibility improvement
      noPhantomContactImprovement: !(afterRoute && priorRoutes.has(afterRoute)
        && assessment.dimensions?.BUYER_ACCESSIBILITY?.advanced === true),
      outcomeIsLimitedOrNoneNotFalseMaterial: assessment.outcome !== IMPROVEMENT_OUTCOME.MATERIAL_COMMERCIAL_IMPROVEMENT
        || assessment.materialDimensionCount >= 1,
      // For these three live runs we expect LIMITED (honest) — not false MATERIAL
      expectedLimitedForTheseLiveRuns: assessment.outcome === IMPROVEMENT_OUTCOME.LIMITED_IMPROVEMENT
        || assessment.outcome === IMPROVEMENT_OUTCOME.NO_IMPROVEMENT,
      roomBlockAbsenceDidNotBlockAssessment: assessment.roomBlockNotRequiredForMaterial === true,
      namedBuyerAbsenceDidNotAutoBlockMaterialRule: assessment.namedBuyerNotRequiredForMaterial === true,
    },
  };
}

export function runCommercialImprovementSelectionOfflineCert() {
  const contract = commercialImprovementContract();
  const liveReplays = LIVE_IDS.map(replayLiveImprovementAssessment);

  const phantomPass = liveReplays.every((r) => r.assertions.noPhantomContactImprovement);
  const noFalseMaterial = liveReplays.every((r) => r.assertions.expectedLimitedForTheseLiveRuns);
  const potomac = liveReplays.find((r) => r.opportunityId.includes("potomac"));
  const nado = liveReplays.find((r) => r.opportunityId.includes("nado"));
  const tournamentNotNew = potomac
    && potomac.priorContactRoutes.includes("tournament@potomacsoccer.org")
    && potomac.assessment.dimensions.BUYER_ACCESSIBILITY?.advanced !== true;
  const infoNotNew = nado
    && nado.priorContactRoutes.includes("info@nado.org")
    && nado.assessment.dimensions.BUYER_ACCESSIBILITY?.advanced !== true;

  const ranking = selectShortlistForNextValidation({
    preferPathwayDiversity: true,
    excludePriorTests: true,
  });
  const head = rankDeepResearchCandidates({ excludePriorTests: true, limit: 15 });

  // Selection quality gates — no hardcoded winners
  const shortlist = ranking.shortlist || [];
  const selectionGates = {
    threeSelected: shortlist.length === 3,
    noPriorTestIds: shortlist.every((s) => !PRIOR_TEST_OPPORTUNITY_IDS.includes(s.opportunityId)),
    noPrincipalGaslampPotomacNadoOrgs: shortlist.every((s) =>
      !/Principal|Gaslamp Event|Potomac Soccer|NADO|Development Organizations/i.test(s.organizationName || "")),
    allHaveCanonicalIds: shortlist.every((s) => s.opportunityId && s.hotelId),
    allHaveResearchQuestions: shortlist.every((s) => (s.researchQuestions || []).length >= 3),
    allHaveCostEstimate: shortlist.every((s) => Number(s.estimatedInvestigationCostUsd) > 0),
    notHardcoded: ranking.hardcodedWinners === false,
    scoresMonotonicInHead: head.length < 2 || head.every((r, i) => i === 0 || r.score <= head[i - 1].score + 1e-9),
    prefersIncompleteStrong: shortlist.filter((s) => s.incompleteButPotentiallyStrong || s.score >= 5).length >= 2,
    commerciallyWeakNotForced: shortlist.every((s) => !s.commerciallyWeak || s.score >= 4),
  };

  const allSelectionPass = Object.values(selectionGates).every(Boolean);
  const allReplayPass = phantomPass && noFalseMaterial && tournamentNotNew && infoNotNew
    && liveReplays.every((r) => r.assertions.roomBlockAbsenceDidNotBlockAssessment);

  const goNoGo = {
    decision: allReplayPass && allSelectionPass ? "GO" : "NO_GO",
    forSelectedOpportunities: allSelectionPass ? "GO" : "NO_GO",
    forMeasurementModel: allReplayPass ? "GO" : "NO_GO",
    paidResearch: "NO_GO",
    weeklyActivation: "NO_GO",
    note: allReplayPass && allSelectionPass
      ? "Offline measurement + selection certified. Selected opportunities are GO for a separately authorized live validation only — not auto-paid."
      : "Offline certification incomplete — do not authorize paid research.",
  };

  return {
    version: COMMERCIAL_IMPROVEMENT_SELECTION_CERT_V1,
    measurementVersion: COMMERCIAL_IMPROVEMENT_MEASUREMENT_V1,
    selectionVersion: DEEP_RESEARCH_OPPORTUNITY_SELECTION_V1,
    certifiedAt: new Date().toISOString(),
    rootCauses: [
      {
        id: "phantom_prior_contact",
        detail: "classifyHardeningLiveCommercialOutcome / earlier live classifier compared ownership.route to before.contactEmail, but captureOpportunityBeforeSnapshot only populated contactRoutes — leaving contactEmail undefined. Pre-existing tournament@ and info@ therefore looked newly discovered.",
      },
      {
        id: "binary_buyer_or_pages_proxy",
        detail: "MATERIAL was gated on named role-relevant DM or new contact + page counts, which both over-counted (phantom contacts) and under-specified true commercial dimensions (demand, timing, hotel relevance, sales action).",
      },
      {
        id: "effort_as_value",
        detail: "Related-entity executed counts and newLiveUrls were used as commercial improvement proxies — research effort ≠ commercial value.",
      },
    ],
    contract,
    liveReplays,
    regressionGates: {
      phantomPass,
      noFalseMaterial,
      tournamentNotNew,
      infoNotNew,
      allReplayPass,
    },
    shortlistPackage: ranking,
    rankingHead: head,
    selectionGates,
    allSelectionPass,
    goNoGo,
    remainingGaps: [
      "Public WHO still often cannot name housing/meeting directors — enrichment remains separately gated",
      "Selection scores use stored bag fields; checkpoint depth / prior investigated URLs are incomplete for many opportunities",
      "Pathway inference is heuristic from opportunityType/room/venue — some ACCOUNT_LED DMC cases may classify as EVENT_LED",
      "Live multidimensional assessment not yet re-run on a fresh paid wave (offline only here)",
    ],
  };
}
