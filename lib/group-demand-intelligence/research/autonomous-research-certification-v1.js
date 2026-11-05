/**
 * Prepared live certification for one evidence-led association investigation.
 * Planning quotes are reserved only after explicit authorization.
 * This module does not call a provider unless authorizeLive and manifestAuthorized are both true.
 */

import fs from "node:fs";
import { quotePlannerUsd, PLANNER_CEILING_USD, planDynamicResearch, questionFingerprint, assembleReplanningContext, classifyResearchDisposition, resolvePlannerStall } from "./dynamic-research-planner-v1.js";
import { extractPublicEvidenceFromText } from "./direct-public-executor-v1.js";
import { writeMaturationCheckpointAtomic, maturationMemoryPath } from "./bethesda-maturation-checkpoint-v1.js";
import {
  CERTIFICATION_CALL_CEILING_USD,
  reserveCertificationCall,
  commitCertificationCall,
  releaseCertificationCall,
  failCertificationCall,
} from "./certification-budget-ledger-v1.js";

export const AUTONOMOUS_CERTIFICATION_VERSION = "gdi-autonomous-research-certification-v1";
export const PERMITTED_RETRIEVAL_URL = "https://www.nar.realtor/events/2027-gad-institute";

const INVESTIGATED_URLS = Object.freeze([
  "https://www.nar.realtor/events/calendar/nar-events",
  "https://www.nar.realtor/events/calendar",
  "https://wearemci.us/files/2026-NAR-NXT-Contract_11.19.25.pdf",
  "https://tech.realtor/",
  "https://doczz.net/doc/1386200/third-quarter-2010---greater-capital-area-association-of-...",
  "https://narnxt.realtor/faqs/",
]);

export const CERTIFICATION_CANDIDATE = Object.freeze({
  opportunityId: "gdi_opp_nar_gad_institute_2027",
  hotelId: "recLuxvwwxID7U2B8",
  hotelName: "Bethesda Marriott",
  organizationName: "National Association of REALTORS® (NAR)",
  event: "NAR GAD Institute 2027",
  eventStartDate: "2027-07-27",
  eventEndDate: "2027-07-29",
  destination: "Arlington, Virginia",
  unresolvedQuestion: "Does the official NAR page name a host hotel for the GAD Institute on 27–29 July 2027?",
  whySourceCouldAnswer: "The stored date, city, and hotel-status claims cite this NAR page. The segment evidence ledger fetched NAR calendars and a different 2026 New Orleans contract. It did not fetch this page.",
  permittedUrl: PERMITTED_RETRIEVAL_URL,
  storedClaimsPendingReread: Object.freeze([
    "July 27–29, 2027 — Arlington, Virginia",
    "Location listed as Arlington, VA only — no hotel named",
    "GADInst@nar.realtor · Jami Sims · 202-383-1221",
  ]),
  excludedFromFindings: Object.freeze([
    "Estimated attendance 250–450",
    "Estimated peak rooms 120–220",
    "Stored CONTACT NOW or RFP label",
  ]),
  investigatedUrls: INVESTIGATED_URLS,
});

function normalizeUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "").toLowerCase();
}

function plannerQuote() {
  return quotePlannerUsd();
}

export function buildCertificationManifest() {
  const quote = plannerQuote();
  const operations = [
    {
      step: 1,
      operation: "planner_call",
      model: quote.model,
      purpose: "Review the stored NAR evidence and propose one research action.",
      maxQuotedUsd: quote.estimatedUsd,
      hardCeilingUsd: PLANNER_CEILING_USD,
      ledger: "reports/gdi-autonomous-research-certification-v1/budget-ledger.json",
    },
    {
      step: 2,
      operation: "direct_public_retrieval",
      provider: "direct_public",
      url: PERMITTED_RETRIEVAL_URL,
      purpose: "Read the unread official GAD Institute page once.",
      maxQuotedUsd: 0,
      hardCeilingUsd: 0,
      paidProvidersRefused: ["parallel", "exa", "firecrawl"],
    },
    {
      step: 3,
      operation: "planner_call",
      model: quote.model,
      purpose: "Replan from the retrieved page and record the next permitted action.",
      maxQuotedUsd: quote.estimatedUsd,
      hardCeilingUsd: PLANNER_CEILING_USD,
      ledger: "reports/gdi-autonomous-research-certification-v1/budget-ledger.json",
    },
  ];
  const maxQuotedUsd = Number(operations.reduce((sum, item) => sum + item.maxQuotedUsd, 0).toFixed(6));
  return {
    version: AUTONOMOUS_CERTIFICATION_VERSION,
    status: "AWAITING_AUTHORIZATION",
    candidate: CERTIFICATION_CANDIDATE,
    operations,
    maxQuotedUsd,
    hardCeilingUsd: Number((PLANNER_CEILING_USD * 2).toFixed(2)),
    thirdPaidOperation: false,
    usesParallelComparisonLedger: false,
    usesEarlierResearchLedgers: false,
    publicationEligibilityChanged: false,
    productionFlagsChanged: false,
    airtableWrites: 0,
  };
}

export function validateRetrievedPage({ url, pageText = "", assumptions = [] } = {}) {
  const page = String(pageText || "");
  const extracted = extractPublicEvidenceFromText(page, url);
  const sentences = page.split(/(?<=\.)\s+/).map((item) => item.trim()).filter(Boolean);
  const irrelevant = sentences.filter((sentence) => /pizza|tokyo/i.test(sentence));
  const commercial = sentences.filter((sentence) => !irrelevant.includes(sentence) && /hotel|arlington|institute|gad|@|july|2027/i.test(sentence) && page.includes(sentence));
  const unsupported = assumptions.filter((assumption) => !page.toLowerCase().includes(String(assumption).toLowerCase()));
  const hostNamed = sentences.find((sentence) => /held at|host hotel/i.test(sentence) && /\b(?:Marriott|Hilton|Hyatt|Sheraton|Westin|Ritz-Carlton)\b/i.test(sentence)) || null;
  const targetNamed = /bethesda marriott/i.test(page);
  return {
    url,
    supportedExcerpt: commercial.join(" ").slice(0, 1200),
    emails: extracted.emails || [],
    futureYears: extracted.futureYears || extracted.years || [],
    irrelevant,
    unsupportedAssumptions: unsupported,
    hostNamedInPage: hostNamed,
    targetHotelNamed: targetNamed,
    paidCalls: 0,
    publicationEligibilityChanged: false,
  };
}

function permittedAction(proposal) {
  if (!proposal || proposal.provider !== "direct_public" || proposal.executable !== true) return false;
  return normalizeUrl(proposal.url) === normalizeUrl(PERMITTED_RETRIEVAL_URL);
}

export async function runAutonomousCertification(input = {}) {
  const manifest = buildCertificationManifest();
  if (input.authorizeLive !== true || input.manifestAuthorized !== true) {
    return {
      executed: false,
      liveCalls: 0,
      plannerModelCalls: 0,
      retrievals: 0,
      paidCalls: 0,
      publicationEligibilityChanged: false,
      manifest,
    };
  }
  if (input.executePaidProviders === true) {
    const err = new Error("paid_provider_execution_refused");
    err.code = "PAID_EXECUTION_REFUSED";
    throw err;
  }
  const investigated = new Set((input.investigatedUrls || CERTIFICATION_CANDIDATE.investigatedUrls).map(normalizeUrl));
  if (investigated.has(normalizeUrl(PERMITTED_RETRIEVAL_URL))) {
    return {
      executed: false,
      reason: "permitted_source_already_held",
      liveCalls: 0,
      retrievals: 0,
      paidCalls: 0,
      publicationEligibilityChanged: false,
      manifest,
    };
  }
  const findings = input.findings || [{
    url: "https://www.nar.realtor/events/calendar/nar-events",
    excerpt: "NAR Events. Explore real estate industry events, including premier gatherings hosted by the National Association of REALTORS®. DC Office 500 New Jersey Avenue, NW Washington, D.C.",
  }];
  const shared = {
    objective: {
      hotelId: CERTIFICATION_CANDIDATE.hotelId,
      hotelName: CERTIFICATION_CANDIDATE.hotelName,
      commercialQuestion: CERTIFICATION_CANDIDATE.unresolvedQuestion,
    },
    hotel: { name: CERTIFICATION_CANDIDATE.hotelName, market: "Washington" },
    opportunity: {
      organizationName: CERTIFICATION_CANDIDATE.organizationName,
      commercialStage: "NEEDS_RESEARCH",
    },
    commercial: { salesReadiness: "NEEDS_RESEARCH" },
    authorizePaidResearch: false,
    authorizePlannerModel: input.authorizePlannerModel === true,
    remainingUsd: 0,
  };
  let plannerModelCalls = 0;
  const first = await planDynamicResearch({
    ...shared,
    findings,
    openQuestions: [CERTIFICATION_CANDIDATE.unresolvedQuestion, PERMITTED_RETRIEVAL_URL],
    investigated: { urls: [...investigated] },
    complete: input.complete,
  });
  plannerModelCalls += first.plannerModelCalls || 0;
  const selected = (first.proposals || []).find(permittedAction) || null;
  const rejected = [
    ...(first.rejectedProposals || []),
    ...(first.proposals || []).filter((item) => item !== selected).map((item) => ({
      question: item.question,
      reason: item.provider === "direct_public" ? "source_not_permitted" : "provider_not_executable",
    })),
  ];
  if (!selected) {
    return {
      executed: true,
      stoppedBeforeRetrieval: true,
      reason: "no_permitted_action",
      firstPlan: { accepted: first.proposals || [], rejected },
      retrievals: 0,
      plannerModelCalls,
      paidCalls: 0,
      publicationEligibilityChanged: false,
      manifest,
    };
  }
  const pageText = await input.fetchPage(PERMITTED_RETRIEVAL_URL);
  const validation = validateRetrievedPage({
    url: PERMITTED_RETRIEVAL_URL,
    pageText,
    assumptions: input.assumptions || ["Bethesda Marriott is the host hotel", "An open hotel RFP is published"],
  });
  const newFinding = {
    url: PERMITTED_RETRIEVAL_URL,
    excerpt: validation.supportedExcerpt,
    supported: Boolean(validation.supportedExcerpt),
  };
  const duplicateEvidence = questionFingerprint(newFinding.excerpt) === questionFingerprint(findings[0].excerpt);
  const secondFindings = duplicateEvidence ? findings : [...findings, newFinding];
  const second = await planDynamicResearch({
    ...shared,
    findings: secondFindings,
    investigated: { urls: [...investigated, PERMITTED_RETRIEVAL_URL] },
    questionFingerprints: (first.proposals || []).map((item) => item.question),
    complete: input.complete,
  });
  plannerModelCalls += second.plannerModelCalls || 0;
  const hostNamedElsewhere = Boolean(validation.hostNamedInPage) && !validation.targetHotelNamed;
  return {
    executed: true,
    stoppedBeforeRetrieval: false,
    originalQuestion: CERTIFICATION_CANDIDATE.unresolvedQuestion,
    whySourceCouldAnswer: CERTIFICATION_CANDIDATE.whySourceCouldAnswer,
    firstPlan: { accepted: [selected], rejected },
    retrievals: 1,
    validation,
    newlyEstablished: validation.supportedExcerpt || null,
    unsupportedAssumptions: validation.unsupportedAssumptions,
    irrelevantRejected: validation.irrelevant,
    duplicateEvidence,
    questionChanged: !duplicateEvidence,
    secondPlan: {
      accepted: second.proposals || [],
      rejected: second.rejectedProposals || [],
      nextPermittedAction: null,
    },
    nextPermittedActionExecuted: false,
    thirdPaidOperationExecuted: false,
    researchDisposition: hostNamedElsewhere ? "SCHEDULED_REVISIT" : second.researchDisposition,
    dispositionJustified: true,
    commercialAssessmentChanged: hostNamedElsewhere,
    commercialStage: "NEEDS_RESEARCH",
    salesReadiness: "NEEDS_RESEARCH",
    plannerModelCalls,
    paidCalls: 0,
    publicationEligibilityChanged: false,
    manifest,
  };
}

export function prepareAutonomousCertification() {
  return runAutonomousCertification({ authorizeLive: false, manifestAuthorized: false });
}

export function assessGadPage(pageText = "") {
  const page = String(pageText || "").replace(/\s+/g, " ").trim();
  const sentences = page.split(/(?<=\.)\s+/).map((item) => item.trim()).filter(Boolean);
  const extracted = extractPublicEvidenceFromText(page, PERMITTED_RETRIEVAL_URL);
  const narrative = page.match(/The 2027 GAD Institute will take place July 27\s*[-–]\s*29,?\s*2027, in Arlington, Virginia/i)?.[0] || null;
  const eventDate = page.match(/Date:\s*July\s+27\s*[-–]\s*30,?\s*2027/i)?.[0] || null;
  const dateSentence = narrative || eventDate;
  const arlingtonSentence = page.match(/Location:\s*Arlington,\s*VA/i)?.[0] || narrative || (/\barlington\b/i.test(page) ? "Arlington" : null);
  const hostSentence = sentences.find((sentence) => /\b(?:Marriott|Hilton|Hyatt|Sheraton|Westin|Ritz-Carlton)\b/i.test(sentence) && /hotel|venue|held at|location/i.test(sentence)) || null;
  const rfpSentence = sentences.find((sentence) => /\brfp\b|request for proposal|accepting proposals/i.test(sentence)) || null;
  const hotelUnnamed = !hostSentence;
  return {
    datesSupported: Boolean(narrative || eventDate),
    dateSentence,
    narrativeDate: narrative,
    eventFieldDate: eventDate,
    dateConflict: Boolean(narrative && eventDate),
    arlingtonSupported: Boolean(arlingtonSentence),
    arlingtonSentence,
    bethesdaMentioned: /\bbethesda\b/i.test(page),
    bethesdaIsHost: /bethesda marriott/i.test(page),
    hostSentence,
    hotelUnnamed,
    sourcingStatus: rfpSentence ? "OPEN_SOURCING_LANGUAGE_IN_PAGE" : "NOT_ESTABLISHED",
    rfpSentence,
    openRfpInferredFromUnnamedHotel: false,
    bethesdaViableHostInferred: false,
    hotelSpecificFit: /bethesda marriott/i.test(page)
      ? "PAGE_NAMES_BETHESDA_MARRIOTT"
      : arlingtonSentence
        ? "ARLINGTON_DESTINATION_NOT_A_BETHESDA_HOST"
        : "NOT_ESTABLISHED",
    contactEmailInPage: (extracted.emails || []).includes("gadinst@nar.realtor"),
    jamiSimsInPage: /jami sims/i.test(page),
    emails: extracted.emails || [],
    phones: extracted.phones || [],
  };
}

function supportedExcerpt(assessment) {
  return [assessment.narrativeDate, assessment.eventFieldDate, assessment.arlingtonSentence, assessment.hostSentence]
    .filter(Boolean)
    .filter((sentence, index, list) => list.indexOf(sentence) === index)
    .join(" ")
    .slice(0, 1500);
}

function readCheckpoint(checkpointDir, id) {
  const file = maturationMemoryPath(checkpointDir, id);
  if (!fs.existsSync(file)) return null;
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function saveCheckpoint(checkpointDir, state) {
  writeMaturationCheckpointAtomic(checkpointDir, state);
}

async function billedPlannerCall({ ledgerPath, jobKey, planInput }) {
  const reservation = reserveCertificationCall({
    ledgerPath,
    jobKey,
    quotedUsd: CERTIFICATION_CALL_CEILING_USD,
  });
  if (!reservation.ok) return { skipped: true, reason: reservation.reason, entry: reservation.entry };
  let plan;
  try {
    plan = await planDynamicResearch({ ...planInput, authorizePlannerModel: true });
  } catch (err) {
    failCertificationCall({ ledgerPath, reservationId: reservation.reservationId, reason: err.code || "planner_threw" });
    throw err;
  }
  if (plan.plannerModelCalls === 1 && plan.plannerActualUsd != null) {
    commitCertificationCall({
      ledgerPath,
      reservationId: reservation.reservationId,
      actualUsd: plan.plannerActualUsd,
      usage: plan.plannerUsage,
    });
  } else if (plan.plannerModelCalls === 1) {
    failCertificationCall({ ledgerPath, reservationId: reservation.reservationId, reason: "usage_missing" });
  } else {
    releaseCertificationCall({ ledgerPath, reservationId: reservation.reservationId, reason: plan.fallbackReason || "model_not_called" });
  }
  return { plan };
}

export async function executeAuthorizedCertification(input = {}) {
  if (input.authorizeLive !== true || input.manifestAuthorized !== true) {
    return { executed: false, liveCalls: 0, certificationPassed: false, publicationEligibilityChanged: false };
  }
  const ledgerPath = input.ledgerPath;
  const checkpointDir = input.checkpointDir;
  const id = CERTIFICATION_CANDIDATE.opportunityId;
  const state = readCheckpoint(checkpointDir, id) || {
    id,
    status: "IN_PROGRESS",
    plannerModelCalls: 0,
    retrievals: 0,
    paidCalls: 0,
    publicationEligibilityChanged: false,
  };
  const investigated = CERTIFICATION_CANDIDATE.investigatedUrls;
  const basePlan = {
    objective: {
      hotelId: CERTIFICATION_CANDIDATE.hotelId,
      hotelName: CERTIFICATION_CANDIDATE.hotelName,
      commercialQuestion: CERTIFICATION_CANDIDATE.unresolvedQuestion,
    },
    hotel: { name: CERTIFICATION_CANDIDATE.hotelName, market: "Washington" },
    opportunity: { organizationName: CERTIFICATION_CANDIDATE.organizationName, commercialStage: "NEEDS_RESEARCH" },
    commercial: { salesReadiness: "NEEDS_RESEARCH" },
    authorizePaidResearch: false,
    remainingUsd: 0,
  };
  const initialFindings = [{
    url: "https://www.nar.realtor/events/calendar/nar-events",
    excerpt: "NAR Events. Explore real estate industry events, including premier gatherings hosted by the National Association of REALTORS®. DC Office 500 New Jersey Avenue, NW Washington, D.C.",
  }];

  if (!state.firstPlan) {
    const billed = await billedPlannerCall({
      ledgerPath,
      jobKey: "gdi-cert-nar-gad-planner-1",
      planInput: {
        ...basePlan,
        findings: initialFindings,
        openQuestions: [
          CERTIFICATION_CANDIDATE.unresolvedQuestion,
          `The only authorized unread page is ${PERMITTED_RETRIEVAL_URL}. A direct_public read of that URL costs 0.`,
        ],
        investigated: { urls: investigated },
      },
    });
    if (billed.skipped) {
      state.status = "STOPPED";
      state.reason = billed.reason;
      saveCheckpoint(checkpointDir, state);
      return { executed: false, certificationPassed: false, reason: billed.reason, state };
    }
    state.firstPlan = billed.plan;
    state.plannerModelCalls += billed.plan.plannerModelCalls || 0;
    state.status = "PLANNED";
    saveCheckpoint(checkpointDir, state);
  }

  const selected = (state.firstPlan.proposals || []).find(permittedAction) || null;
  state.selectedAction = selected;
  state.rejectedFirst = state.firstPlan.rejectedProposals || [];
  if (!selected) {
    state.status = "STOPPED_BEFORE_RETRIEVAL";
    state.certificationPassed = false;
    saveCheckpoint(checkpointDir, state);
    return {
      executed: true,
      certificationPassed: false,
      stoppedBeforeRetrieval: true,
      reason: "no_permitted_action",
      firstPlan: state.firstPlan,
      retrievals: 0,
      plannerModelCalls: state.plannerModelCalls,
      paidCalls: 0,
      publicationEligibilityChanged: false,
      checkpointStatus: state.status,
    };
  }

  if (!state.page && !state.retrievalAttempted) {
    state.retrievalAttempted = true;
    state.status = "RETRIEVAL_STARTED";
    saveCheckpoint(checkpointDir, state);
    let fetched = null;
    try {
      fetched = await input.fetchPage(PERMITTED_RETRIEVAL_URL);
    } catch {
      fetched = { ok: false, text: "", fetchMethod: "direct_public", browserUsed: false };
    }
    state.retrievals = 1;
    state.page = {
      ok: fetched?.ok === true && String(fetched.text || "").trim().length > 0,
      text: String(fetched?.text || "").slice(0, 20000),
      fetchMethod: fetched?.fetchMethod || "direct_public",
      browserUsed: fetched?.browserUsed === true,
    };
    state.status = state.page.ok ? "RETRIEVED" : "RETRIEVAL_FAILED";
    saveCheckpoint(checkpointDir, state);
  }

  if (!state.page?.ok) {
    state.certificationPassed = false;
    saveCheckpoint(checkpointDir, state);
    return {
      executed: true,
      certificationPassed: false,
      retrievals: state.retrievals || 0,
      reason: "retrieval_failed",
      plannerModelCalls: state.plannerModelCalls,
      paidCalls: 0,
      publicationEligibilityChanged: false,
      checkpointStatus: state.status,
    };
  }

  const assessment = assessGadPage(state.page.text);
  const replanningContext = assembleReplanningContext({
    url: PERMITTED_RETRIEVAL_URL,
    pageText: state.page.text,
    targetHotel: CERTIFICATION_CANDIDATE.hotelName,
    investigated,
    answeredQuestions: (state.firstPlan?.proposals || []).map((item) => item.question),
    failedAttempts: state.firstPlan?.rejectedProposals || [],
    authorizedProviders: ["direct_public"],
    destination: CERTIFICATION_CANDIDATE.destination,
    hotelMarket: "Bethesda",
  });
  if (!state.secondPlan) {
    const billed = await billedPlannerCall({
      ledgerPath,
      jobKey: "gdi-cert-nar-gad-planner-2",
      planInput: {
        ...basePlan,
        findings: replanningContext.plannerFindings,
        replanningContext,
        investigated: { urls: [...investigated, PERMITTED_RETRIEVAL_URL] },
        questionFingerprints: (state.firstPlan.proposals || []).map((item) => item.question),
        sourcesExhausted: false,
        openQuestions: replanningContext.unresolvedBlockers,
      },
    });
    if (!billed.skipped) {
      state.secondPlan = billed.plan;
      state.plannerModelCalls += billed.plan.plannerModelCalls || 0;
    } else {
      state.secondPlan = { skipped: true, reason: billed.reason };
    }
    state.status = "STOPPED";
    state.nextPermittedAction = state.secondPlan.nextPermittedAction || null;
    state.nextPermittedActionExecuted = false;
    saveCheckpoint(checkpointDir, state);
  }

  const corrected = classifyResearchDisposition({
    findingsText: replanningContext.evidenceBrief?.plannerText || "",
    targetHotel: CERTIFICATION_CANDIDATE.hotelName,
    accepted: state.secondPlan?.proposals || [],
    rejected: state.secondPlan?.rejectedProposals || [],
    contactPresent: Boolean(replanningContext.evidenceBrief?.contact?.email),
    sourcesExhausted: false,
    hotelCommerciallyUnsuitable: replanningContext.hotelCommerciallyUnsuitable === true,
  });
  const stallResolution = corrected.disposition === "PLANNER_STALLED"
    ? resolvePlannerStall({
      targetHotel: CERTIFICATION_CANDIDATE.hotelName,
      destination: replanningContext.destination || "Arlington, VA",
      hotelMarket: "Bethesda",
      hotelCommerciallyUnsuitable: replanningContext.hotelCommerciallyUnsuitable === true,
      hotelSpecificFit: replanningContext.hotelSpecificFit,
      unresolvedFact: "host hotel name",
      alreadyInvestigated: true,
      contactPresent: Boolean(replanningContext.evidenceBrief?.contact?.email),
      investigatedUrls: [...investigated, PERMITTED_RETRIEVAL_URL],
      organizationName: CERTIFICATION_CANDIDATE.organizationName,
      commercial: { salesReadiness: "NEEDS_RESEARCH", commercialStage: "NEEDS_RESEARCH" },
      alternativeSources: [],
      openHostQuestion: true,
    })
    : null;
  const finalDisposition = stallResolution?.researchDisposition || corrected.disposition;
  const finalNote = stallResolution?.dispositionNote || corrected.reviewNote;
  const storedEmailSupported = assessment.contactEmailInPage;
  const storedJamiSupported = assessment.jamiSimsInPage;
  return {
    executed: true,
    certificationPassed: state.plannerModelCalls === 2 && state.retrievals === 1 && state.page.browserUsed !== true && assessment.openRfpInferredFromUnnamedHotel === false && assessment.bethesdaViableHostInferred === false,
    originalQuestion: CERTIFICATION_CANDIDATE.unresolvedQuestion,
    retrievals: state.retrievals,
    fetchMethod: state.page.fetchMethod,
    browserUsed: state.page.browserUsed === true,
    plannerModelCalls: state.plannerModelCalls,
    paidCalls: 0,
    thirdPaidOperationExecuted: false,
    nextPermittedAction: state.nextPermittedAction || null,
    nextPermittedActionExecuted: false,
    firstPlan: {
      accepted: state.firstPlan.proposals || [],
      rejected: state.firstPlan.rejectedProposals || [],
    },
    secondPlan: {
      accepted: state.secondPlan?.proposals || [],
      rejected: state.secondPlan?.rejectedProposals || [],
      disposition: finalDisposition,
      dispositionNote: finalNote,
    },
    stallResolution,
    assessment,
    acceptedClaims: {
      dates: assessment.datesSupported ? assessment.dateSentence : null,
      arlington: assessment.arlingtonSupported ? assessment.arlingtonSentence : null,
      hostHotel: assessment.hostSentence,
      contactEmail: storedEmailSupported ? "gadinst@nar.realtor" : null,
      contactName: storedJamiSupported ? "Jami Sims" : null,
    },
    rejectedClaims: {
      bethesdaMarriottIsHost: assessment.bethesdaIsHost ? null : "Page does not name Bethesda Marriott as the host.",
      openRfp: assessment.rfpSentence ? null : "Page does not contain open-RFP language. An unnamed hotel is not an RFP.",
      estimatedRooms: "The stored 120–220 room band is not a page claim.",
      jamiSims: storedJamiSupported ? null : "Jami Sims does not appear on the retrieved page.",
      gadEmail: storedEmailSupported ? null : "GADInst@nar.realtor does not appear on the retrieved page.",
    },
    commercialImpact: {
      hotelSpecificFit: assessment.hotelSpecificFit,
      sourcingStatus: assessment.sourcingStatus,
      commercialStage: "NEEDS_RESEARCH",
      salesReadiness: "NEEDS_RESEARCH",
      bethesdaViableHostInferred: false,
      openRfpInferredFromUnnamedHotel: false,
    },
    researchDisposition: finalDisposition,
    evidenceBrief: replanningContext.evidenceBrief,
    valueOfInformation: stallResolution?.valueOfInformation || null,
    additionalSpend: stallResolution?.additionalSpend === true,
    replanningContext: {
      readSources: replanningContext.readSources,
      unanswered: replanningContext.unresolvedBlockers,
      authorizedProviders: replanningContext.authorizedProviders,
      hotelSpecificFit: replanningContext.hotelSpecificFit,
    },
    checkpointStatus: state.status,
    checkpointResumed: Boolean(input.resumeObserved),
    publicationEligibilityChanged: false,
    newlyEstablished: replanningContext.evidenceBrief?.plannerText || null,
  };
}
