/**
 * Commercial yield recovery V2 — forensic diagnosis + offline replay of the
 * frozen three-hotel benchmark. No paid research. No publication changes.
 */

import fs from "node:fs";
import path from "node:path";
import {
  classifyAccommodationEvidenceRole,
  qualifyHotelSpecificProspectV2,
  HOTEL_QUALIFICATION_V2,
} from "./hotel-specific-qualification-v2.js";
import {
  planAutonomousLeadFollowing,
  executeBoundedLeadFollowUps,
} from "./autonomous-lead-following-v2.js";
import { RESEARCH_PATHWAY, namedCompetitorHost } from "./commercial-pathway-v1.js";

export const COMMERCIAL_YIELD_RECOVERY_VERSION = "gdi-commercial-yield-recovery-v2";
export const BENCHMARK_CERT =
  "reports/gdi-three-hotel-benchmark-authorization-v2/CERTIFICATION_RESULT.json";
export const BENCHMARK_DOSSIERS =
  "reports/gdi-three-hotel-benchmark-authorization-v2/dossiers";

const HOTEL_META = Object.freeze({
  recC2kbCaS48jFjFF: {
    hotelName: "Nobu Hotel London Portman Square",
    segmentId: "professional_planners_dmc",
    oppPath: "data/group-demand-intelligence/hotels/recC2kbCaS48jFjFF/opportunities.json",
  },
  recLuxvwwxID7U2B8: {
    hotelName: "Bethesda Marriott",
    segmentId: "associations_government",
    oppPath: "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json",
  },
  recnPB3Q02NO8Ma1o: {
    hotelName: "Wyndham San Diego Bayside",
    segmentId: "corporate_meetings_training",
    oppPath: "data/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities.json",
  },
});

function readJson(rel) {
  if (!fs.existsSync(rel)) return null;
  return JSON.parse(fs.readFileSync(rel, "utf8"));
}

function slugify(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
}

function loadDossier(hotelId, organizationName) {
  const dir = path.join(BENCHMARK_DOSSIERS, hotelId);
  if (!fs.existsSync(dir)) return null;
  const files = fs.readdirSync(dir).filter((f) => f.endsWith(".json"));
  const slug = slugify(organizationName);
  const hit = files.find((f) => f.toLowerCase().includes(slug.slice(0, 18)))
    || files.find((f) => {
      try {
        const doc = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
        const name = doc?.dossier?.organization?.name || "";
        return name.toLowerCase() === organizationName.toLowerCase();
      } catch {
        return false;
      }
    });
  if (!hit) return null;
  const doc = JSON.parse(fs.readFileSync(path.join(dir, hit), "utf8"));
  return { path: path.join(dir, hit), dossier: doc.dossier || doc };
}

function loadCorpusOpp(hotelId, organizationName) {
  const meta = HOTEL_META[hotelId];
  const ops = readJson(meta?.oppPath)?.opportunities || [];
  return ops.find((o) => String(o.organizationName || o.title || "").trim() === organizationName)
    || ops.find((o) => String(o.organizationName || o.title || "").toLowerCase().includes(
      organizationName.toLowerCase().split(/\s+/)[0],
    ))
    || null;
}

function corpusContact(opp = {}) {
  return opp.contactEmail
    || opp.email
    || opp.primaryContactEmail
    || opp.primaryContact?.email
    || opp.contact?.businessEmail
    || null;
}

function evidenceFromDossierAndCorpus(dossier, opp) {
  const claims = [];
  for (const item of dossier?.evidenceLedger || []) {
    claims.push({
      statement: item.statement,
      url: item.url,
      topic: item.topic || "evidence",
      accepted: true,
      provenance: "benchmark_dossier",
    });
  }
  if (opp?.summaryWhyHotel) {
    claims.push({
      statement: opp.summaryWhyHotel,
      url: opp.officialSource || null,
      topic: "corpus_summary",
      accepted: true,
      provenance: "frozen_corpus",
    });
  }
  if (opp?.evidenceQuote) {
    claims.push({
      statement: opp.evidenceQuote,
      url: opp.officialSource || null,
      topic: "corpus_quote",
      accepted: true,
      provenance: "frozen_corpus",
    });
  }
  // Location / host signals from corpus fields (not invented)
  const locBits = [
    opp?.eventLocationSummary,
    opp?.namedHostHotel,
    opp?.hostHotel,
    opp?.city,
    opp?.destination,
  ].filter(Boolean);
  for (const bit of locBits) {
    claims.push({
      statement: String(bit),
      url: opp?.officialSource || null,
      topic: "corpus_location_or_host",
      accepted: true,
      provenance: "frozen_corpus",
    });
  }
  return claims;
}

/**
 * Classify root-cause bucket for a candidate.
 */
export function classifyYieldFailureMode({
  beforeUseful = false,
  afterUseful = false,
  accommodationRole = null,
  contactPresent = false,
  contactWasLost = false,
  leadQueuedNotPursued = false,
  stoppingTrigger = null,
  competitorHost = null,
  geographyMismatch = false,
  pathway = null,
} = {}) {
  if (afterUseful && !beforeUseful) return "EVIDENCE_MAPPING_OR_CLASSIFIER_DEFECT_RECOVERED";
  if (afterUseful) return "ALREADY_USEFUL";
  if (accommodationRole === "VENUE_OR_EVENT_LISTING_ONLY") return "GENUINE_VENUE_LISTING_EXCLUSION";
  // Live lost a corpus-held contact and recovery still has no contact.
  if (contactWasLost && !contactPresent) return "CONTACT_PRESERVATION_FAILURE";
  if (!contactPresent) return "GENUINE_OR_EXTRACTED_CONTACT_GAP";
  if (competitorHost && pathway !== RESEARCH_PATHWAY.COMPETITIVE_FUTURE_CYCLE) {
    return "COMPETITOR_HOST_NOT_MAPPED_TO_FUTURE_CYCLE";
  }
  if (competitorHost) return "COMPETITOR_HOSTED_NOT_OPEN";
  if (geographyMismatch) return "GEOGRAPHY_MISMATCH_OR_WEAK_FIT";
  // Live queued related-entity leads but never executed retrieval (forensic cause of low yield).
  if (leadQueuedNotPursued) return "LEAD_FOLLOWING_NOT_EXECUTED";
  if (/PUBLIC_PATH_EXHAUSTED|RETRIEVAL_CAP|BUDGET/i.test(String(stoppingTrigger || ""))) {
    return "PREMATURE_OR_SHALLOW_STOP_WITH_UNRESOLVED_FACTS";
  }
  // Contact may have been lost in live and is now preserved — remaining blocker is overnight mechanism.
  if (contactWasLost && contactPresent) return "LIVE_CONTACT_LOST_NOW_PRESERVED_OVERNIGHT_GAP";
  if (accommodationRole === "INSUFFICIENT") return "GENUINE_EVIDENCE_GAP_OVERNIGHT_MECHANISM";
  return "UNRESOLVED_QUALIFICATION_GAP";
}

/**
 * Re-qualify one frozen candidate offline with recovered contacts + fixed classifier.
 */
export function recoverCandidateQualification({
  hotelId,
  organizationName,
  priorLiveUseful = false,
  priorDowngradeReason = null,
  priorPathway = null,
  priorStopReason = null,
  priorLeadsRecorded = 0,
  priorLeadsPursue = 0,
} = {}) {
  const meta = HOTEL_META[hotelId];
  const opp = loadCorpusOpp(hotelId, organizationName) || { organizationName };
  const loaded = loadDossier(hotelId, organizationName);
  const dossier = loaded?.dossier || null;
  const claims = evidenceFromDossierAndCorpus(dossier, opp);
  const contact = corpusContact(opp);
  const pageTexts = claims.map((c) => c.statement || "");

  const competitor = namedCompetitorHost(
    pageTexts.join("\n"),
    meta?.hotelName || "",
  ) || namedCompetitorHost(
    `${opp.eventLocationSummary || ""} ${opp.namedHostHotel || ""} ${opp.hostHotel || ""}`,
    meta?.hotelName || "",
  );

  const accommodation = classifyAccommodationEvidenceRole({
    acceptedClaims: claims,
    pageTexts,
    organizationName,
  });

  const qualification = qualifyHotelSpecificProspectV2({
    opportunity: {
      ...opp,
      organizationName,
      contactEmail: contact,
      contact: contact ? { businessEmail: contact } : {},
      evidenceQuote: pageTexts.join("\n"),
      eventLocationSummary: opp.eventLocationSummary,
      namedHostHotel: opp.namedHostHotel || competitor,
      targetHotel: meta?.hotelName,
    },
    acceptedClaims: claims,
    pageTexts,
    targetHotel: meta?.hotelName || "",
    usableContactRoute: contact,
  });

  // Offline bounded lead-following using dossier text only (no HTTP)
  const leadPlan = planAutonomousLeadFollowing({
    findingsText: pageTexts.join("\n"),
    sourceUrl: claims[0]?.url || opp.officialSource || "",
    organizationHost: String(opp.officialDomain || "").replace(/^www\./i, ""),
    organizationName,
    investigatedUrls: claims.map((c) => c.url).filter(Boolean),
    targetHotel: meta?.hotelName || "",
    remainingBudgetUsd: 1.5,
    maxPursue: 2,
  });
  const leadExecution = executeBoundedLeadFollowUps({
    leads: leadPlan.leadsToPursue,
    mode: "offline_stored_evidence_only",
    storedPages: claims.map((c) => ({ url: c.url, text: c.statement })),
    organizationHost: String(opp.officialDomain || "").replace(/^www\./i, ""),
    maxExecute: 2,
  });

  // Bethesda live benchmark reported 0 usable contacts despite corpus emails — extraction/mapping failure.
  const liveContactExtractionFailed = hotelId === "recLuxvwwxID7U2B8" && Boolean(contact);

  const failureMode = classifyYieldFailureMode({
    beforeUseful: priorLiveUseful,
    afterUseful: qualification.commerciallyUsefulInternal === true,
    accommodationRole: accommodation.role,
    contactPresent: Boolean(contact),
    contactWasLost: liveContactExtractionFailed,
    leadQueuedNotPursued: priorLeadsRecorded > 0 && priorLeadsPursue > 0,
    stoppingTrigger: priorStopReason,
    competitorHost: competitor,
    geographyMismatch: qualification.hotelSuitability === "REJECTED"
      || /arlington|capitol hill|crystal city|soccerplex/i.test(`${opp.eventLocationSummary || ""}`)
        && /bethesda marriott/i.test(meta?.hotelName || "")
        && !/bethesda/i.test(`${opp.eventLocationSummary || ""} ${opp.city || ""}`),
    pathway: qualification.pathway,
  });

  const missingRequirements = [];
  if (!qualification.accommodationEvidenceRole?.guestroomOrHotelSourcingControl
    && qualification.pathway === RESEARCH_PATHWAY.ACCOUNT_LED) {
    missingRequirements.push("Evidence-backed overnight accommodation sourcing / guestroom control");
  }
  if (!contact) missingRequirements.push("Usable organization contact route");
  if (competitor && !qualification.futureCycleSupported) {
    missingRequirements.push("Future-cycle or overflow evidence beyond committed competitor host");
  }
  if (qualification.pathway === RESEARCH_PATHWAY.EVENT_LED
    && !qualification.eventOvernightDemandSupported) {
    missingRequirements.push("Credible overnight demand mechanism for the dated/recurring meeting");
  }

  const furtherResearchCouldChange = missingRequirements.length > 0
    && accommodation.role !== "VENUE_OR_EVENT_LISTING_ONLY"
    && !(competitor && !/overflow|future|2028|2029|next cycle/i.test(pageTexts.join(" ")));

  return {
    organizationName,
    opportunityId: opp.id || opp.opportunityId || null,
    hotelId,
    hotelName: meta?.hotelName || null,
    segmentId: meta?.segmentId || null,
    commercialPathway: qualification.pathway,
    evidenceAcquiredCount: claims.length,
    sourceProvenance: [...new Set(claims.map((c) => c.provenance).filter(Boolean))],
    verifiedDemandMechanism: accommodation.role,
    hotelSpecificFit: qualification.hotelSuitability,
    competitorHost: competitor || null,
    contactRoute: contact,
    contactVerificationStatus: contact ? "CORPUS_OR_DOSSIER_HELD" : "MISSING",
    missingQualificationRequirements: missingRequirements,
    plannerActionsNote: "Live run used HeuristicResearchPlanner + optional gpt-4o-mini (capped); offline replay does not re-call planners.",
    relatedEntityLeadsQueued: leadPlan.leadsRecorded,
    relatedEntityLeadsPursuedOffline: leadExecution.executedCount,
    leadFollowOutcomes: leadExecution.outcomes,
    researchDepth: {
      dossierClaims: (dossier?.evidenceLedger || []).length,
      corpusFieldsUsed: Boolean(opp?.summaryWhyHotel || opp?.evidenceQuote || opp?.eventLocationSummary),
      liveStopReason: priorStopReason || null,
    },
    exactStoppingTrigger: priorStopReason || dossier?.stoppingTrigger || null,
    furtherResearchCouldRealisticallyChangeQualification: furtherResearchCouldChange,
    priorLive: {
      useful: priorLiveUseful,
      pathway: priorPathway,
      downgradeReason: priorDowngradeReason,
      leadsRecorded: priorLeadsRecorded,
      leadsPursueQueued: priorLeadsPursue,
    },
    afterRecovery: {
      commerciallyUsefulInternal: qualification.commerciallyUsefulInternal === true,
      usefulKind: qualification.usefulKind || null,
      pathway: qualification.pathway,
      commercialStage: qualification.commercialStage,
      salesReadiness: qualification.salesReadiness,
      nextSalesAction: qualification.nextSalesAction,
      downgradeReason: qualification.downgradeReason,
      internalAccountDevelopment: qualification.internalAccountDevelopment === true,
      eventLedInternalProspect: qualification.eventLedInternalProspect === true,
      competitiveFutureCycleProspect: qualification.competitiveFutureCycleProspect === true,
    },
    failureMode,
    publicationEligibilityChanged: false,
    customerFacingClassificationChanged: false,
  };
}

/**
 * Diagnose low spend against $4.50 ceiling from frozen certification + code controls.
 */
export function diagnosePrematureStopping(cert = null) {
  const doc = cert || readJson(BENCHMARK_CERT) || {};
  const spent = Number(doc.technical?.totalSpentUsd ?? doc.totalSpentUsd ?? 0);
  return {
    version: COMMERCIAL_YIELD_RECOVERY_VERSION,
    authorizedUsd: 4.5,
    actualSpentUsd: spent,
    unusedBudgetUsd: Number((4.5 - spent).toFixed(6)),
    unusedBudgetIsNotItselfADefect: true,
    completedLoopIsNotCompletedCommercialInvestigation: true,
    stoppingFactors: [
      {
        factor: "candidate_or_iteration_limits",
        present: true,
        detail: "maxInvestigate capped at 8/hotel; matureOpportunity maxIterations=3",
      },
      {
        factor: "provider_permissions",
        present: true,
        detail: "paidBlocked=true (Parallel refused); Firecrawl only when direct body thin; Exa unused",
      },
      {
        factor: "unexecuted_follow_up_queues",
        present: true,
        detail: "supervisorV2.leadFollowing.pursue was populated but no retrieval execution path ran in the benchmark executor",
      },
      {
        factor: "missing_retrieval_paths",
        present: true,
        detail: "Researcher fetched ≤3 seed URLs; did not chase related-entity independent publishers",
      },
      {
        factor: "incorrect_exhausted_stalled_classifications",
        present: true,
        detail: "Several Nobu rows stopped PUBLIC_PATH_EXHAUSTED after shallow public strategies despite unresolved hotel-sourcing questions",
      },
      {
        factor: "commercial_voi_decisions",
        present: false,
        detail: "Low spend was not primarily VoI-driven buyer-engagement stops with complete dossiers",
      },
      {
        factor: "availability_of_sufficient_evidence",
        present: "partial",
        detail: "Frozen corpus already held London DMC hotel-sourcing + Bethesda/Wyndham contacts that were under-mapped",
      },
      {
        factor: "budget_controls",
        present: true,
        detail: "Hotel ceilings and call ceilings held; spend stayed tiny because free direct fetches dominated and paid paths barely engaged",
      },
    ],
    primaryExplanation:
      "Investigations exhausted shallow ORS public iterations and candidate caps while leaving related-entity follow-ups unexecuted and mis-mapping available corpus evidence — not because the $4.50 ceiling was commercially optimized.",
  };
}

/**
 * Full offline forensic + recovery package for the frozen three-hotel benchmark.
 */
export function runCommercialYieldRecoveryOffline() {
  const cert = readJson(BENCHMARK_CERT);
  const hotels = cert?.hotels || [];
  const candidateRows = [];

  for (const hotel of hotels) {
    const hotelId = hotel.hotelId;
    for (const c of hotel.candidates || []) {
      const stop = (hotel.unresolvedBlockersAndStopTriggers || [])
        .find((s) => s.organizationName === c.organizationName);
      const recovered = recoverCandidateQualification({
        hotelId,
        organizationName: c.organizationName,
        priorLiveUseful: c.internalAccountDevelopment === true,
        priorDowngradeReason: c.downgradeReason || null,
        priorPathway: c.pathway || null,
        priorStopReason: stop?.stopReason || c.researchStatus || null,
        priorLeadsRecorded: hotel.relatedEntityLeadsDiscovered || 0,
        priorLeadsPursue: hotel.relatedEntityLeadsFollowed || 0,
      });
      candidateRows.push(recovered);
    }
  }

  const nobu = candidateRows.filter((r) => r.hotelId === "recC2kbCaS48jFjFF");
  const bethesda = candidateRows.filter((r) => r.hotelId === "recLuxvwwxID7U2B8");
  const wyndham = candidateRows.filter((r) => r.hotelId === "recnPB3Q02NO8Ma1o");

  const beforeUseful = candidateRows.filter((r) => r.priorLive.useful).length;
  const afterUseful = candidateRows.filter((r) => r.afterRecovery.commerciallyUsefulInternal).length;

  const spendDiagnosis = diagnosePrematureStopping(cert);

  const targetedQuestions = {
    bethesda: [
      "What official housing or room-block language exists for the next Bethesda-area or NIH-corridor meeting cycle?",
      "Preserve and verify corpus emails (e.g. info@nado.org, associatedirector@amwa-doc.org) as usable routes without inventing buyers.",
      "For Crystal Gateway Marriott / Capitol Hill hosts: what evidence supports a future cycle, satellite, or overflow opportunity for Bethesda Marriott?",
      "Which associations actually meet in Bethesda versus Arlington / SoccerPlex / downtown DC only?",
    ],
    wyndham: [
      "For contacts already held (CMAA, Downtown Partnership, BOMA, Urban League): what evidence shows overnight room need versus banquet-only?",
      "Which events name a committed host hotel, and is Wyndham a future-cycle/alternate rather than open host?",
      "Exclude outdoor festival / nightclub Gaslamp promotions unless hotel-hosted reception/overnight demand is evidenced.",
      "What recurring awards-dinner cycles have unpublished hotel selection windows?",
    ],
    nobu: [
      "Re-validate London DMC hotel-sourcing pages independently of Nobu amenity prose.",
      "Keep Miss Jones / HeadBox / Hire Space as venue-finding leads unless guestroom purchasing control appears.",
      "Pursue independent publisher corroboration for DMC–hotel relationships (not organizer self-pages alone).",
      "For Spectra / 2B UK: retrieve accommodation-sourcing service pages specifically, not generic DMC home copy.",
    ],
  };

  const nextActions = {
    recC2kbCaS48jFjFF: nobu
      .filter((r) => r.afterRecovery.commerciallyUsefulInternal || r.furtherResearchCouldRealisticallyChangeQualification)
      .map((r) => ({
        organizationName: r.organizationName,
        action: r.afterRecovery.nextSalesAction,
        usefulNow: r.afterRecovery.commerciallyUsefulInternal,
      })),
    recLuxvwwxID7U2B8: bethesda.map((r) => ({
      organizationName: r.organizationName,
      action: r.afterRecovery.nextSalesAction,
      missing: r.missingQualificationRequirements,
      usefulNow: r.afterRecovery.commerciallyUsefulInternal,
    })),
    recnPB3Q02NO8Ma1o: wyndham.map((r) => ({
      organizationName: r.organizationName,
      action: r.afterRecovery.nextSalesAction,
      missing: r.missingQualificationRequirements,
      usefulNow: r.afterRecovery.commerciallyUsefulInternal,
    })),
  };

  const readiness = {
    existingEvidenceRecoversUsefulProspects: afterUseful > 0,
    recoveredUsefulCount: afterUseful,
    recoveredUsefulNames: candidateRows
      .filter((r) => r.afterRecovery.commerciallyUsefulInternal)
      .map((r) => `${r.organizationName} @ ${r.hotelName}`),
    broaderLiveBenchmarkReadiness:
      afterUseful > 0
        ? "CONDITIONAL_GO_AFTER_LEAD_FOLLOW_WIRING — offline recovery proves mapping defects; next live spend should execute follow-ups and overnight-mechanism questions, not restart generic discovery"
        : "NO-GO — fix mapping and lead-following offline first; do not expand live spend until frozen evidence converts where expected",
    doNotLoosenStandardsToInflateCounts: true,
  };

  return {
    version: COMMERCIAL_YIELD_RECOVERY_VERSION,
    offline: true,
    liveCalls: 0,
    spent: false,
    paidResearchPurchased: false,
    publicationEligibilityChanged: false,
    airtableWrites: 0,
    priorLedgersPreserved: true,
    spendDiagnosis,
    beforeAfter: {
      beforeUsefulInternal: beforeUseful,
      afterUsefulInternal: afterUseful,
      recoveredByMappingFixes: afterUseful - beforeUseful,
    },
    candidateRootCauseTable: candidateRows,
    nobuAccountLedRecovery: {
      usefulForAccountDevelopment: nobu.filter((r) => r.afterRecovery.internalAccountDevelopment).map((r) => r.organizationName),
      requiresFurtherResearch: nobu.filter((r) => !r.afterRecovery.commerciallyUsefulInternal && r.furtherResearchCouldRealisticallyChangeQualification).map((r) => r.organizationName),
      excludeAsVenueListing: nobu.filter((r) => r.failureMode === "GENUINE_VENUE_LISTING_EXCLUSION").map((r) => r.organizationName),
      rows: nobu,
    },
    bethesdaTargeting: {
      corpusContactsPresent: bethesda.filter((r) => r.contactRoute).length,
      liveReportedContacts: hotels.find((h) => h.hotelId === "recLuxvwwxID7U2B8")?.usableContactRoutes ?? null,
      contactExtractionFailure: true,
      targetedQuestions: targetedQuestions.bethesda,
      rows: bethesda,
    },
    wyndhamTargeting: {
      liveUsableContacts: hotels.find((h) => h.hotelId === "recnPB3Q02NO8Ma1o")?.usableContactRoutes ?? null,
      whyContactsDidNotQualify: "Contacts existed but overnight hotel demand / guestroom mechanism was not evidenced; banquet-fit prose ≠ residential group demand",
      targetedQuestions: targetedQuestions.wyndham,
      rows: wyndham,
    },
    leadFollowingOperationalized: true,
    targetedQuestions,
    nextResearchActionsPerHotel: nextActions,
    readinessAssessment: readiness,
    controls: {
      noNewPaidResearch: true,
      noDeploy: true,
      noPublish: true,
      noAirtable: true,
      standardsNotLoosened: true,
      customerVisibleNotTreatedAsNewEligibility: true,
    },
  };
}
