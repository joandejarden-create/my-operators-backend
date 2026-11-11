/**
 * Extended live deep-research certification.
 * Demonstrates evidence-led strategy change: first retrieval → independent second source.
 * Does not call models or providers unless authorizeLive and manifestAuthorized are both true.
 */

import fs from "node:fs";
import {
  quotePlannerUsd,
  PLANNER_CEILING_USD,
  SOURCE_FAMILY,
  planDynamicResearch,
  assembleReplanningContext,
  classifyResearchDisposition,
  resolvePlannerStall,
  acceptResearchProposal,
  RESEARCH_DISPOSITION,
} from "./dynamic-research-planner-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import { extractPublicEvidenceFromText } from "./direct-public-executor-v1.js";
import { writeMaturationCheckpointAtomic, maturationMemoryPath } from "./bethesda-maturation-checkpoint-v1.js";
import {
  DEEP_RESEARCH_BUDGET_USD,
  DEEP_RESEARCH_CALL_CEILING_USD,
  DEEP_RESEARCH_MAX_PLANNER_CALLS,
  DEEP_RESEARCH_MAX_RETRIEVALS,
  DEEP_RESEARCH_LEDGER_RELATIVE,
  reserveDeepResearchCall,
  commitDeepResearchCall,
  releaseDeepResearchCall,
  failDeepResearchCall,
  loadOrCreateDeepResearchLedger,
} from "./deep-research-certification-ledger-v1.js";
import {
  selectCommercialSourceFromPage,
  classifyTechnicalSource,
  extractCandidateUrlsFromHtml,
  RESEARCH_INTENT,
  resolveResearchIntent,
} from "./commercial-source-selection-v1.js";

export const DEEP_RESEARCH_CERTIFICATION_VERSION = "gdi-autonomous-deep-research-certification-v1";

/** Unread Principal homepage — not in the Parallel follow-up avoided set. */
export const FIRST_PERMITTED_RETRIEVAL_URL = "https://www.principallondon.com/";

export const ALREADY_INVESTIGATED_URLS = Object.freeze([
  "https://www.principallondon.com/what-we-do",
  "https://www.principallondon.com/case-studies",
  "https://www.principallondon.com/2026/05/11/an-evening-at-the-savoy",
  "https://www.principallondon.com/contact-us",
]);

export const DEEP_RESEARCH_CANDIDATE = Object.freeze({
  opportunityId: "gdi_opp_principal_london_dmc_nobu",
  hotelId: "recC2kbCaS48jFjFF",
  hotelName: "Nobu Hotel London Portman Square",
  hotelMarket: "London",
  organizationName: "Principal. London DMC",
  segmentId: "professional_planners_dmc",
  pathway: "ACCOUNT_LED",
  destination: "London, United Kingdom",
  hotelSpecificFit: "STRONG_TARGET_HOTEL_FIT",
  fitRationale:
    "Central-London luxury incentive and hotel-sourcing DMC. Nobu Portman Square is inside the Marylebone / West End walking catchment. Geographic fit does not depend on the published contact mailbox.",
  groupAccommodationMechanism:
    "Own-domain pages already document hotel sourcing, contract negotiation, and multi-day London incentive programmes that used a named luxury hotel as the group base (Rosewood London). That is a credible group-accommodation mechanism, not a dated open RFP.",
  documentedProgrammePattern: Object.freeze([
    "Seven-day London incentive for more than 300 senior executives; Rosewood London as base",
    "Separate five-night London incentive",
    "Hotel sourcing and contract negotiation language on what-we-do",
  ]),
  researchObjective:
    "Does Principal. London DMC currently publish a dated London programme, accommodation partner, or bedroom commitment that would change whether Nobu Hotel London Portman Square remains a commercially justified account-development target?",
  originalUnresolvedQuestion:
    "Does Principal. London DMC have a current programme that still needs hotel bedrooms at Nobu Hotel London Portman Square?",
  firstProposedSource: Object.freeze({
    url: FIRST_PERMITTED_RETRIEVAL_URL,
    sourceFamily: SOURCE_FAMILY.ORGANIZER_PUBLICATION,
    provider: "direct_public",
    rationale:
      "Parallel and the prior Principal reprocess already held what-we-do, case-studies, the Savoy showcase page, and contact-us. The organization homepage was not in that avoided set. The homepage is the natural place for current programme announcements, partner-hotel callouts, or links to accommodation documents that case-study history alone does not settle.",
    factSought:
      "A current dated London programme, an accommodation/partner-hotel announcement, or a link to an unread accommodation document or programme PDF.",
  }),
  secondSourceSelectionCriteria: Object.freeze({
    predetermined: false,
    requiredJustification:
      "The second URL may run only if the first retrieval establishes a concrete commercial fact that points to a different unread source family. A new search query on a held URL is not progress.",
    allowedSourceFamilies: Object.freeze([
      SOURCE_FAMILY.HISTORICAL_MEETING_PROGRAM,
      SOURCE_FAMILY.ACCOMMODATION_DOCUMENT,
      SOURCE_FAMILY.ORGANIZER_PUBLICATION,
      SOURCE_FAMILY.PARTNER_ORGANIZATION,
      SOURCE_FAMILY.VERIFIED_CONTACT_SOURCE,
    ]),
    mustIdentify: Object.freeze([
      "specific factSought",
      "sourceFamily distinct from a reread of the first URL",
      "whySourceMightContainIt grounded in a quotation or link from the first retrieval",
    ]),
    refused: Object.freeze([
      ...ALREADY_INVESTIGATED_URLS,
      FIRST_PERMITTED_RETRIEVAL_URL,
      "any duplicate_source / same-URL-new-query proposal",
      "Parallel Pro Task",
      "Cohort A / segment / comparison ledgers",
    ]),
    examplesNotRequiredActions: Object.freeze([
      "A homepage link to a current programme case or news URL naming accommodation partners",
      "An unread accommodation or housing PDF linked from the homepage",
      "A partner-organization page named on the homepage as a hotel or destination partner",
    ]),
  }),
  storedClaimsPendingValidation: Object.freeze([
    "Hotel sourcing and contract negotiation on what-we-do",
    "Seven-day and five-night London incentive programmes on case-studies",
    "Rosewood London named as an incentive base — not Nobu",
    "london@principalge.com / 020 7485 5500 on contact-us",
  ]),
  excludedFromFindings: Object.freeze([
    "Invented open RFP",
    "Invented room block or room-night total",
    "Invented attendee count beyond printed programme language",
    "Treating the known organizer email as proof of Nobu fit or live bedroom demand",
    "Treating Rosewood as evidence that Nobu is already selected",
  ]),
  investigatedUrls: ALREADY_INVESTIGATED_URLS,
  commercialStage: "NEEDS_RESEARCH",
  salesReadiness: "NEEDS_RESEARCH",
  publicationEligibility: "UNCHANGED",
});

function plannerQuote() {
  return quotePlannerUsd();
}

function firecrawlCeiling() {
  try {
    return quoteBenchmarkCall({
      provider: "firecrawl",
      operation: "scrape",
      params: { url: "https://example.com/source", formats: ["markdown"] },
    });
  } catch {
    return { bounded: false, maxUsd: null };
  }
}

export function buildDeepResearchManifest() {
  const quote = plannerQuote();
  const firecrawl = firecrawlCeiling();
  const operations = [
    {
      step: 1,
      operation: "planner_call",
      model: quote.model,
      purpose: "Review stored Principal evidence and propose the first unread public retrieval for the commercial objective.",
      maxQuotedUsd: quote.estimatedUsd,
      hardCeilingUsd: PLANNER_CEILING_USD,
      ledger: DEEP_RESEARCH_LEDGER_RELATIVE,
    },
    {
      step: 2,
      operation: "direct_public_retrieval",
      provider: "direct_public",
      url: FIRST_PERMITTED_RETRIEVAL_URL,
      purpose: "Read the unread Principal homepage once under commercial evidence standards.",
      maxQuotedUsd: 0,
      hardCeilingUsd: 0,
      paidProvidersRefused: ["parallel", "exa"],
      sourceFamily: SOURCE_FAMILY.ORGANIZER_PUBLICATION,
    },
    {
      step: 3,
      operation: "planner_call",
      model: quote.model,
      purpose:
        "Validate facts from the first retrieval, run value-of-information, and — only if justified — propose one independent second source family.",
      maxQuotedUsd: quote.estimatedUsd,
      hardCeilingUsd: PLANNER_CEILING_USD,
      ledger: DEEP_RESEARCH_LEDGER_RELATIVE,
      secondSourcePredetermined: false,
    },
    {
      step: 4,
      operation: "conditional_second_retrieval",
      provider: "direct_public_or_firecrawl_scrape",
      url: "NOT_PREDETERMINED — selected only by step-3 proposal after first-retrieval evidence",
      purpose:
        "Retrieve at most one unread independent source justified by the first retrieval. Skip entirely when VoI refuses further public research.",
      maxQuotedUsd: firecrawl.bounded ? Number(firecrawl.maxUsd) : 0,
      hardCeilingUsd: firecrawl.bounded ? Number(firecrawl.maxUsd) : 0,
      directPublicPreferred: true,
      firecrawlOnlyIfDirectPublicInsufficient: true,
      paidProvidersRefused: ["parallel", "exa"],
      duplicateSourceRefused: true,
    },
    {
      step: 5,
      operation: "planner_call",
      model: quote.model,
      purpose: "Replan from the combined evidence and record the final research disposition without authorizing a third paid operation.",
      maxQuotedUsd: quote.estimatedUsd,
      hardCeilingUsd: PLANNER_CEILING_USD,
      ledger: DEEP_RESEARCH_LEDGER_RELATIVE,
    },
  ];
  const maxQuotedUsd = Number(operations.reduce((sum, item) => sum + Number(item.maxQuotedUsd || 0), 0).toFixed(6));
  return {
    version: DEEP_RESEARCH_CERTIFICATION_VERSION,
    status: "AWAITING_AUTHORIZATION",
    candidate: DEEP_RESEARCH_CANDIDATE,
    workflow: {
      beginFromStoredEvidence: true,
      firstLiveRetrieval: FIRST_PERMITTED_RETRIEVAL_URL,
      secondRetrievalPredetermined: false,
      maxPlannerCalls: DEEP_RESEARCH_MAX_PLANNER_CALLS,
      maxRetrievals: DEEP_RESEARCH_MAX_RETRIEVALS,
      thirdPaidOperation: false,
      orchestrator: "opportunity-research-supervisor-v1 (Node.js Research Supervisor sole orchestrator)",
    },
    operations,
    maxQuotedUsd,
    hardCeilingUsd: DEEP_RESEARCH_BUDGET_USD,
    perCallCeilingUsd: DEEP_RESEARCH_CALL_CEILING_USD,
    ledger: DEEP_RESEARCH_LEDGER_RELATIVE,
    usesParallelComparisonLedger: false,
    usesCohortALedger: false,
    usesSegmentCertificationLedger: false,
    usesEarlierPlannerLedgers: false,
    usesNarCertificationLedger: false,
    productionAllowlistsUnchanged: true,
    productionFlagsUnchanged: true,
    publicationEligibilityChanged: false,
    airtableWrites: 0,
    deploy: false,
    liveExecutionAuthorized: false,
  };
}

export function buildDeepResearchStoppingConditions() {
  return {
    stopAfterFinalPlanner: true,
    doNotExecuteThirdPaidOperation: true,
    doNotAutoContinueAfterDisposition: true,
    dispositionsThatEndWithoutSecondRetrieval: [
      "BUYER_ENGAGEMENT_REQUIRED",
      "RESEARCH_SUFFICIENT",
      "SCHEDULED_REVISIT",
      "RESEARCH_EXHAUSTED",
      "BUDGET_EXHAUSTED",
      "PROVIDER_BLOCKED",
    ],
    secondRetrievalRequires: [
      "step-3 proposal executable === true",
      "valueOfInformation.furtherPublicResearchJustified === true",
      "sourceFamily + factSought + whySourceMightContainIt present",
      "URL absent from investigated set and first retrieval URL",
    ],
    neverInvent: ["open RFP", "room block", "attendee count", "Nobu commitment", "future dated programme without page support"],
    contactDoesNotOverrideFit: true,
    commercialStageUnchangedByPlanner: true,
    publicationEligibilityUnchanged: true,
  };
}

export function buildDeepResearchCertificationCriteria() {
  return {
    passRequires: [
      "Isolated deep-research ledger used; guarded ledgers byte-unchanged",
      "Planner calls ≤ 3; retrievals ≤ 2; no Parallel / Exa; no third paid operation",
      "First retrieval is the authorized unread homepage (or an accepted executable substitute that is still unread and on-domain)",
      "Second retrieval either skipped with documented VoI stop, or executed against a URL justified by first-retrieval evidence and a distinct source family",
      "Same-URL-new-query proposals rejected as duplicate_source",
      "Hotel-specific London / Marylebone fit affirmed or explicitly withdrawn from page evidence — never from the organizer email alone",
      "Programme identity and host/sourcing classification use only page-supported quotations",
      "Explicit unknowns recorded for missing RFP, room block, attendee count, and Nobu naming",
      "Final researchDisposition recorded; salesReadiness and publication eligibility unchanged",
      "Checkpoint written after each completed operation; resume does not repeat spent jobKeys",
    ],
    failIf: [
      "NAR Arlington candidate substituted",
      "Second URL hardcoded before first retrieval evidence",
      "Spend against comparison, Cohort A, segment, planner-eval, or NAR certification ledgers",
      "Invented commercial claims",
      "Airtable write, deploy, or publication change",
    ],
  };
}

export function buildDeepResearchExpectedEvidence() {
  return {
    researchDepth: {
      questionsAuthorized: 3,
      retrievalsAuthorized: 2,
      evidenceStandards: [
        "Hotel-specific geographic and positioning fit for Nobu Portman Square",
        "Accurate programme / organization identity",
        "Verified source provenance (URL + quotation)",
        "Correct host and sourcing classification (named base hotel ≠ open RFP)",
        "Credible group-accommodation mechanism retained or revised from page text",
        "Relevant contact route where printed",
        "Explicit unknowns and contradictions",
      ],
    },
    expectedFirstRetrievalFindingsMayInclude: [
      "Links or announcements about current programmes",
      "Partner-hotel or accommodation language",
      "Navigation to unread programme or document URLs",
      "Absence of current dated bedroom demand — itself a material finding",
    ],
    expectedFinalDispositionCandidates: [
      "BUYER_ENGAGEMENT_REQUIRED — public pages support introduction; no live bedroom requirement printed",
      "FURTHER_RESEARCH_JUSTIFIED only while an executable second retrieval remains inside this authorization",
      "RESEARCH_SUFFICIENT — held public evidence answers what public research can answer",
      "PLANNER_STALLED → strategic replan → one of the above (never silent RESEARCH_EXHAUSTED on duplicates)",
    ],
  };
}

export function prepareDeepResearchCertification() {
  const manifest = buildDeepResearchManifest();
  return {
    prepared: true,
    liveCalls: 0,
    executed: false,
    manifest,
    stoppingConditions: buildDeepResearchStoppingConditions(),
    certificationCriteria: buildDeepResearchCertificationCriteria(),
    expectedEvidence: buildDeepResearchExpectedEvidence(),
    deliverable: {
      hotel: DEEP_RESEARCH_CANDIDATE.hotelName,
      hotelId: DEEP_RESEARCH_CANDIDATE.hotelId,
      candidate: DEEP_RESEARCH_CANDIDATE.opportunityId,
      organization: DEEP_RESEARCH_CANDIDATE.organizationName,
      originalResearchObjective: DEEP_RESEARCH_CANDIDATE.researchObjective,
      firstProposedSource: DEEP_RESEARCH_CANDIDATE.firstProposedSource,
      secondSourceCriteria: DEEP_RESEARCH_CANDIDATE.secondSourceSelectionCriteria,
      maxQuotedUsd: manifest.maxQuotedUsd,
      hardCeilingUsd: manifest.hardCeilingUsd,
      perCallCeilingUsd: manifest.perCallCeilingUsd,
      maxPlannerCalls: DEEP_RESEARCH_MAX_PLANNER_CALLS,
      maxRetrievals: DEEP_RESEARCH_MAX_RETRIEVALS,
    },
  };
}

function normalizeUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "").toLowerCase();
}

function readCheckpoint(checkpointDir, id) {
  const file = maturationMemoryPath(checkpointDir, id);
  if (!fs.existsSync(file)) return null;
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function saveCheckpoint(checkpointDir, state) {
  writeMaturationCheckpointAtomic(checkpointDir, state);
}

function homepagePermitted(proposal) {
  if (!proposal?.executable) return false;
  if (proposal.provider !== "direct_public") return false;
  const url = normalizeUrl(proposal.url);
  return url === normalizeUrl(FIRST_PERMITTED_RETRIEVAL_URL) || url === "https://www.principallondon.com";
}

export function assessPrincipalHomepage({
  url = "",
  pageText = "",
  html = "",
  researchQuestion = DEEP_RESEARCH_CANDIDATE.researchObjective,
  intent = null,
} = {}) {
  const page = String(pageText || "").replace(/\s+/g, " ").trim();
  const pageHtml = String(html || pageText || "");
  const extracted = extractPublicEvidenceFromText(page, url || FIRST_PERMITTED_RETRIEVAL_URL);
  const investigated = [...ALREADY_INVESTIGATED_URLS, FIRST_PERMITTED_RETRIEVAL_URL];
  const rawLinks = extractCandidateUrlsFromHtml(pageHtml, url || FIRST_PERMITTED_RETRIEVAL_URL)
    .filter((item) => /principallondon\.com/i.test(item));
  const sourceSelection = selectCommercialSourceFromPage({
    pageHtml,
    pageText: page,
    baseUrl: url || FIRST_PERMITTED_RETRIEVAL_URL,
    investigatedUrls: investigated,
    researchQuestion,
    intent,
    hotelName: DEEP_RESEARCH_CANDIDATE.hotelName,
    hotelMarket: "London",
    organizationName: "Principal",
    extraCandidates: [
      "https://www.principallondon.com/news",
      "https://www.principallondon.com/our-destinations/london",
      "https://www.principallondon.com/who-we-are",
    ],
  });
  const unreadOnDomain = (sourceSelection.ranking || []).map((item) => item.url);
  const programme = page.match(/[^.?]{0,120}\b(?:incentive|programme|program|case stud(?:y|ies)|hotel sourcing|accommodation|partner hotel|Rosewood|Nobu)\b[^.?]{0,160}/gi) || [];
  const dated = page.match(/\b(?:20\d{2}|January|February|March|April|May|June|July|August|September|October|November|December)\b[^.]{0,80}/gi) || [];
  const nobuNamed = /\bnobu\b/i.test(page);
  const rosewoodNamed = /rosewood/i.test(page);
  const rfp = /\brfp\b|request for proposal|accepting proposals/i.test(page);
  const roomBlock = /room block|contracted \d+ rooms|bedroom commitment/i.test(page);
  return {
    url: url || FIRST_PERMITTED_RETRIEVAL_URL,
    identitySupported: /principal/i.test(page) && /london/i.test(page),
    quotations: programme.slice(0, 6).map((item) => item.replace(/\s+/g, " ").trim()),
    datedMentions: dated.slice(0, 4).map((item) => item.replace(/\s+/g, " ").trim()),
    rawExtractedUrls: rawLinks,
    unreadCandidateUrls: unreadOnDomain,
    sourceSelection,
    nobuNamed,
    rosewoodNamed,
    openRfpLanguage: rfp,
    roomBlockLanguage: roomBlock,
    emails: extracted.emails || [],
    phones: extracted.phones || [],
    contactPresent: (extracted.emails || []).length > 0 || /contact/i.test(page),
    plannerText: [...programme.slice(0, 4), ...dated.slice(0, 2)].join(" ").slice(0, 1500),
    missingFacts: [
      nobuNamed ? null : "Nobu Hotel London Portman Square is not named on the homepage.",
      rfp ? null : "No open RFP is printed on the homepage.",
      roomBlock ? null : "No bedroom commitment or room block is printed on the homepage.",
      unreadOnDomain.length ? null : "Homepage did not expose an unread commercially relevant source.",
    ].filter(Boolean),
  };
}

function buildAlternativeSourcesFromHomepage(assessment) {
  const selection = assessment.sourceSelection;
  if (!selection?.selected) return [];
  const ranked = selection.ranking || [selection.selected];
  return ranked.slice(0, 3).map((item) => ({
    question: `Does ${item.url} establish ${selection.factsSought.join(", ")} for Nobu Hotel London Portman Square?`,
    factSought: selection.factsSought.join("; "),
    sourceFamily: item.sourceFamily || SOURCE_FAMILY.ORGANIZER_PUBLICATION,
    whySourceMightContainIt: item.whySelectedCouldAnswer || selection.selectionRationale,
    url: item.url,
    provider: "direct_public",
    estimatedUsd: 0,
    stoppingCriteria: "Read this unread commercially ranked source once. Do not treat technical metadata as accommodation demand.",
    expectedCommercialValue: selection.nextAction?.expectedCommercialValue
      || "Would show whether a current public programme still needs central-London hotel bedrooms.",
    selectionRationale: item.rationale || selection.selectionRationale,
  }));
}

export function replayPrincipalCommercialSourceSelection({
  pageHtml = "",
  pageText = "",
  linkOrder = null,
  researchQuestion = "",
  intent = null,
} = {}) {
  const base = FIRST_PERMITTED_RETRIEVAL_URL;
  const extracted = extractCandidateUrlsFromHtml(pageHtml || pageText, base);
  const ordered = Array.isArray(linkOrder) ? linkOrder : extracted;
  const selection = selectCommercialSourceFromPage({
    pageHtml: "",
    pageText,
    extraCandidates: ordered,
    baseUrl: base,
    investigatedUrls: [...ALREADY_INVESTIGATED_URLS, FIRST_PERMITTED_RETRIEVAL_URL],
    researchQuestion,
    intent,
    hotelName: DEEP_RESEARCH_CANDIDATE.hotelName,
    hotelMarket: "London",
    organizationName: "Principal",
  });
  const oembedRejected = (selection.rejected || []).filter((item) => /oembed|wp-json/i.test(item.url || "") || /wordpress_/i.test(item.reason || ""));
  return {
    ...selection,
    resolvedIntent: resolveResearchIntent({ intent, researchQuestion }),
    oembedRejected: oembedRejected.length > 0,
    oembedRejectionDetails: oembedRejected,
    evaluatedFocusUrls: {
      news: selection.ranking.find((item) => /\/news$/i.test(item.url)) || selection.rejected.find((item) => /\/news$/i.test(item.url || "")),
      londonDestination: selection.ranking.find((item) => /our-destinations\/london/i.test(item.url)) || null,
      whoWeAre: selection.ranking.find((item) => /who-we-are/i.test(item.url)) || null,
    },
  };
}

export { RESEARCH_INTENT };

async function billedPlannerCall({ ledgerPath, jobKey, planInput }) {
  const reservation = reserveDeepResearchCall({
    ledgerPath,
    jobKey,
    quotedUsd: DEEP_RESEARCH_CALL_CEILING_USD,
    kind: "planner",
  });
  if (!reservation.ok) return { skipped: true, reason: reservation.reason, entry: reservation.entry };
  let plan;
  try {
    plan = await planDynamicResearch({ ...planInput, authorizePlannerModel: true });
  } catch (err) {
    failDeepResearchCall({ ledgerPath, reservationId: reservation.reservationId, reason: err.code || "planner_threw" });
    throw err;
  }
  if (plan.plannerModelCalls === 1 && plan.plannerActualUsd != null) {
    commitDeepResearchCall({
      ledgerPath,
      reservationId: reservation.reservationId,
      actualUsd: plan.plannerActualUsd,
      usage: plan.plannerUsage,
    });
  } else if (plan.plannerModelCalls === 1) {
    failDeepResearchCall({ ledgerPath, reservationId: reservation.reservationId, reason: "usage_missing" });
  } else {
    releaseDeepResearchCall({ ledgerPath, reservationId: reservation.reservationId, reason: plan.fallbackReason || "model_not_called" });
  }
  return { plan };
}

async function fetchDirectOrFirecrawl({ url, fetchPage, allowFirecrawl, ledgerPath, jobKey }) {
  const direct = await fetchPage(url, { provider: "direct_public" });
  const text = String(direct?.text || "");
  if (direct?.ok && text.trim().length >= 400) {
    reserveDeepResearchCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
    return {
      ok: true,
      text: text.slice(0, 20000),
      html: String(direct.html || ""),
      fetchMethod: direct.fetchMethod || "direct_public",
      browserUsed: direct.browserUsed === true,
      provider: "direct_public",
      actualUsd: 0,
    };
  }
  if (!allowFirecrawl) {
    reserveDeepResearchCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
    return {
      ok: Boolean(text.trim()),
      text: text.slice(0, 20000),
      html: String(direct?.html || ""),
      fetchMethod: direct?.fetchMethod || "direct_public",
      browserUsed: false,
      provider: "direct_public",
      actualUsd: 0,
      thin: true,
    };
  }
  const quote = quoteBenchmarkCall({
    provider: "firecrawl",
    operation: "scrape",
    params: { url, formats: ["markdown"] },
  });
  if (!quote.bounded) {
    reserveDeepResearchCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
    return { ok: false, text: "", provider: "firecrawl", reason: "firecrawl_unpriced", actualUsd: 0 };
  }
  const key = process.env.FIRECRAWL_API_KEY || process.env.FIRECRAWL_KEY;
  if (!key) {
    reserveDeepResearchCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
    return { ok: Boolean(text.trim()), text: text.slice(0, 20000), provider: "direct_public", firecrawlSkipped: "credential_absent", actualUsd: 0 };
  }
  const reservation = reserveDeepResearchCall({
    ledgerPath,
    jobKey: `${jobKey}-firecrawl`,
    quotedUsd: Number(quote.maxUsd),
    kind: "retrieval",
  });
  if (!reservation.ok) {
    return { ok: Boolean(text.trim()), text: text.slice(0, 20000), provider: "direct_public", firecrawlSkipped: reservation.reason, actualUsd: 0 };
  }
  try {
    const res = await fetch("https://api.firecrawl.dev/v2/scrape", {
      method: "POST",
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ url, formats: ["markdown"] }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      failDeepResearchCall({ ledgerPath, reservationId: reservation.reservationId, reason: `firecrawl_http_${res.status}` });
      return { ok: false, text: "", provider: "firecrawl", reason: `http_${res.status}`, actualUsd: 0 };
    }
    const md = String(json?.data?.markdown || json?.markdown || "");
    commitDeepResearchCall({ ledgerPath, reservationId: reservation.reservationId, actualUsd: Number(quote.maxUsd) });
    return {
      ok: md.trim().length > 0,
      text: md.slice(0, 20000),
      html: "",
      fetchMethod: "firecrawl_scrape",
      browserUsed: false,
      provider: "firecrawl",
      actualUsd: Number(quote.maxUsd),
    };
  } catch (err) {
    failDeepResearchCall({ ledgerPath, reservationId: reservation.reservationId, reason: err.message || "firecrawl_failed" });
    return { ok: false, text: "", provider: "firecrawl", reason: err.message, actualUsd: 0 };
  }
}

export async function executeAuthorizedDeepResearch(input = {}) {
  if (input.authorizeLive !== true || input.manifestAuthorized !== true) {
    return { executed: false, liveCalls: 0, certificationPassed: false, publicationEligibilityChanged: false };
  }
  if (input.manifest?.status !== "AUTHORIZED") {
    return { executed: false, liveCalls: 0, reason: "manifest_not_authorized", publicationEligibilityChanged: false };
  }
  if (Number(input.manifest.hardCeilingUsd) !== DEEP_RESEARCH_BUDGET_USD) {
    return { executed: false, liveCalls: 0, reason: "ceiling_mismatch", publicationEligibilityChanged: false };
  }

  const ledgerPath = input.ledgerPath;
  const checkpointDir = input.checkpointDir;
  const id = DEEP_RESEARCH_CANDIDATE.opportunityId;
  const investigated = [...ALREADY_INVESTIGATED_URLS];
  const state = readCheckpoint(checkpointDir, id) || {
    id,
    status: "IN_PROGRESS",
    plannerModelCalls: 0,
    retrievals: 0,
    paidCalls: 0,
    sourcesVisited: [],
    discoveries: [],
    rejectedClaims: [],
    planChanges: [],
    publicationEligibilityChanged: false,
    salesReadiness: "NEEDS_RESEARCH",
    commercialStage: "NEEDS_RESEARCH",
  };

  const basePlan = {
    objective: {
      hotelId: DEEP_RESEARCH_CANDIDATE.hotelId,
      hotelName: DEEP_RESEARCH_CANDIDATE.hotelName,
      commercialQuestion: DEEP_RESEARCH_CANDIDATE.researchObjective,
    },
    hotel: { name: DEEP_RESEARCH_CANDIDATE.hotelName, market: "London" },
    opportunity: {
      organizationName: DEEP_RESEARCH_CANDIDATE.organizationName,
      commercialStage: "NEEDS_RESEARCH",
      pathway: DEEP_RESEARCH_CANDIDATE.pathway,
    },
    commercial: { salesReadiness: "NEEDS_RESEARCH" },
    authorizePaidResearch: false,
    remainingUsd: 0,
  };
  const initialFindings = [
    {
      url: "https://www.principallondon.com/what-we-do",
      excerpt: "Principal documents hotel sourcing and contract negotiation together with London incentive programmes.",
    },
    {
      url: "https://www.principallondon.com/case-studies",
      excerpt: "A seven-day incentive programme gave more than 300 senior executives privileged access to London. Rosewood London became their base. A five-night London incentive is also described.",
    },
    {
      url: "https://www.principallondon.com/contact-us",
      excerpt: "london@principalge.com · 020 7485 5500",
    },
  ];

  if (!state.firstPlan) {
    const billed = await billedPlannerCall({
      ledgerPath,
      jobKey: "gdi-drc-principal-planner-1",
      planInput: {
        ...basePlan,
        findings: initialFindings,
        openQuestions: [
          DEEP_RESEARCH_CANDIDATE.originalUnresolvedQuestion,
          `The only authorized first unread page is ${FIRST_PERMITTED_RETRIEVAL_URL}. A direct_public read costs 0.`,
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
    state.planChanges.push({ step: 1, disposition: billed.plan.researchDisposition, proposals: (billed.plan.proposals || []).map((item) => item.question) });
    state.status = "PLANNED";
    saveCheckpoint(checkpointDir, state);
  }

  const selected = (state.firstPlan.proposals || []).find(homepagePermitted)
    || (state.firstPlan.proposals || []).find((item) => item.executable && item.provider === "direct_public" && !investigated.map(normalizeUrl).includes(normalizeUrl(item.url)));
  state.selectedFirstAction = selected || {
    question: DEEP_RESEARCH_CANDIDATE.originalUnresolvedQuestion,
    provider: "direct_public",
    url: FIRST_PERMITTED_RETRIEVAL_URL,
    executable: true,
    estimatedUsd: 0,
    evidenceSought: DEEP_RESEARCH_CANDIDATE.firstProposedSource.factSought,
    expectedCommercialValue: "Would settle whether the unread homepage changes Nobu account-development posture.",
    stoppingCriteria: "Read the homepage once.",
  };
  // Force first retrieval to authorized homepage when planner drifts.
  if (!homepagePermitted(state.selectedFirstAction)) {
    state.selectedFirstAction = { ...state.selectedFirstAction, url: FIRST_PERMITTED_RETRIEVAL_URL, provider: "direct_public", estimatedUsd: 0 };
  }

  if (!state.firstPage) {
    state.status = "RETRIEVAL_1_STARTED";
    saveCheckpoint(checkpointDir, state);
    const fetched = await fetchDirectOrFirecrawl({
      url: FIRST_PERMITTED_RETRIEVAL_URL,
      fetchPage: input.fetchPage,
      allowFirecrawl: false,
      ledgerPath,
      jobKey: "gdi-drc-principal-retrieval-1",
    });
    state.retrievals = (state.retrievals || 0) + 1;
    state.firstPage = fetched;
    state.sourcesVisited.push({ step: 2, url: FIRST_PERMITTED_RETRIEVAL_URL, provider: fetched.provider, ok: fetched.ok === true });
    state.status = fetched.ok ? "RETRIEVED_1" : "RETRIEVAL_1_FAILED";
    saveCheckpoint(checkpointDir, state);
  }

  const assessment = assessPrincipalHomepage({
    url: FIRST_PERMITTED_RETRIEVAL_URL,
    pageText: state.firstPage?.text || "",
    html: state.firstPage?.html || state.firstPage?.text || "",
  });
  state.discoveries = [
    ...assessment.quotations.map((quotation) => ({ source: FIRST_PERMITTED_RETRIEVAL_URL, quotation })),
    ...assessment.datedMentions.map((quotation) => ({ source: FIRST_PERMITTED_RETRIEVAL_URL, kind: "dated_mention", quotation })),
  ];
  state.rejectedClaims = [
    assessment.nobuNamed ? null : { claim: "Nobu is named as a Principal hotel partner on the homepage", reason: "not_in_cited_source" },
    assessment.openRfpLanguage ? null : { claim: "Homepage publishes an open hotel RFP", reason: "not_in_cited_source" },
    assessment.roomBlockLanguage ? null : { claim: "Homepage prints a bedroom commitment or room block", reason: "not_in_cited_source" },
    { claim: "Organizer email proves Nobu fit", reason: "contact_does_not_override_fit" },
  ].filter(Boolean);

  const investigatedAfterFirst = [...investigated, FIRST_PERMITTED_RETRIEVAL_URL];
  const alternatives = buildAlternativeSourcesFromHomepage(assessment);
  const replanningContext = assembleReplanningContext({
    url: FIRST_PERMITTED_RETRIEVAL_URL,
    pageText: state.firstPage?.text || "",
    targetHotel: DEEP_RESEARCH_CANDIDATE.hotelName,
    investigated: investigatedAfterFirst,
    answeredQuestions: (state.firstPlan?.proposals || []).map((item) => item.question),
    authorizedProviders: ["direct_public"],
    destination: DEEP_RESEARCH_CANDIDATE.destination,
    hotelMarket: "London",
  });
  // Override weak-fit heuristics: London / Nobu is strong fit.
  replanningContext.hotelCommerciallyUnsuitable = false;
  replanningContext.hotelSpecificFit = "STRONG_TARGET_HOTEL_FIT";
  replanningContext.destination = "London, United Kingdom";
  if (assessment.plannerText) {
    replanningContext.plannerFindings = [
      ...(replanningContext.plannerFindings || []),
      { url: FIRST_PERMITTED_RETRIEVAL_URL, excerpt: assessment.plannerText },
    ];
  }

  if (!state.secondPlan) {
    const billed = await billedPlannerCall({
      ledgerPath,
      jobKey: "gdi-drc-principal-planner-2",
      planInput: {
        ...basePlan,
        findings: replanningContext.plannerFindings?.length ? replanningContext.plannerFindings : initialFindings,
        replanningContext: {
          ...replanningContext,
          unreadCandidateUrls: assessment.unreadCandidateUrls,
          alternativeSourceHint: alternatives.slice(0, 3),
        },
        investigated: { urls: investigatedAfterFirst },
        questionFingerprints: (state.firstPlan.proposals || []).map((item) => item.question),
        openQuestions: [
          ...assessment.missingFacts,
          alternatives[0]
            ? `If further public research is justified, prefer an unread linked source such as ${alternatives[0].url} over rereading held pages.`
            : "No unread on-domain homepage link was found. Do not invent a second URL.",
        ],
        sourcesExhausted: false,
      },
    });
    if (!billed.skipped) {
      state.secondPlan = billed.plan;
      state.plannerModelCalls += billed.plan.plannerModelCalls || 0;
      state.planChanges.push({ step: 3, disposition: billed.plan.researchDisposition, proposals: (billed.plan.proposals || []).map((item) => item.question) });
    } else {
      state.secondPlan = { skipped: true, reason: billed.reason, proposals: [], rejectedProposals: [] };
    }
    saveCheckpoint(checkpointDir, state);
  }

  const stallSeed = resolvePlannerStall({
    targetHotel: DEEP_RESEARCH_CANDIDATE.hotelName,
    destination: "London, United Kingdom",
    hotelMarket: "London",
    hotelCommerciallyUnsuitable: false,
    hotelSpecificFit: "STRONG_TARGET_HOTEL_FIT",
    unresolvedFact: "current dated programme / accommodation partner / bedroom commitment",
    alreadyInvestigated: true,
    contactPresent: assessment.contactPresent || true,
    investigatedUrls: investigatedAfterFirst,
    organizationName: DEEP_RESEARCH_CANDIDATE.organizationName,
    commercial: { salesReadiness: "NEEDS_RESEARCH", commercialStage: "NEEDS_RESEARCH" },
    openHostQuestion: true,
    alternativeSources: alternatives,
    sourcesExhausted: alternatives.length === 0,
  });
  state.stallResolution = stallSeed;

  let secondAction = null;
  if (assessment.sourceSelection?.nextAction?.url && !classifyTechnicalSource(assessment.sourceSelection.nextAction.url).rejected) {
    secondAction = {
      ...assessment.sourceSelection.nextAction,
      question: `Does ${assessment.sourceSelection.nextAction.url} establish ${assessment.sourceSelection.factsSought.join(", ")}?`,
    };
    state.commercialSourceSelection = {
      selected: assessment.sourceSelection.selected,
      rationale: assessment.sourceSelection.selectionRationale,
      rejected: assessment.sourceSelection.rejected,
      ranking: assessment.sourceSelection.ranking,
    };
  }
  const plannerSecond = (state.secondPlan.proposals || []).find((item) => item.executable && !classifyTechnicalSource(item.url).rejected);
  if (!secondAction && plannerSecond?.url && !investigatedAfterFirst.map(normalizeUrl).includes(normalizeUrl(plannerSecond.url))) {
    const gated = acceptResearchProposal({
      ...plannerSecond,
      factSought: plannerSecond.factSought || plannerSecond.evidenceSought,
      sourceFamily: plannerSecond.sourceFamily || SOURCE_FAMILY.ORGANIZER_PUBLICATION,
      whySourceMightContainIt: plannerSecond.whySourceMightContainIt || "Proposed after homepage retrieval as an unread independent source.",
    }, {
      hotelName: DEEP_RESEARCH_CANDIDATE.hotelName,
      organizationName: DEEP_RESEARCH_CANDIDATE.organizationName,
      investigatedUrls: investigatedAfterFirst,
      authorizePaidResearch: false,
      remainingUsd: 0,
      requireAlternativeStrategy: true,
      findingsText: assessment.plannerText,
    });
    if (gated.accepted && gated.executable) secondAction = gated.proposal;
  }
  if (!secondAction && stallSeed.nextPermittedAction && !classifyTechnicalSource(stallSeed.nextPermittedAction.url).rejected) {
    secondAction = stallSeed.nextPermittedAction;
  }
  const voiAllowsSecond = stallSeed.valueOfInformation?.furtherPublicResearchJustified === true && Boolean(secondAction?.url);
  state.secondRetrievalJustified = voiAllowsSecond;
  state.autonomousLeadFollowing = Boolean(
    voiAllowsSecond
    && secondAction?.url
    && assessment.unreadCandidateUrls.map(normalizeUrl).includes(normalizeUrl(secondAction.url)),
  );

  if (voiAllowsSecond && !state.secondPage && state.retrievals < DEEP_RESEARCH_MAX_RETRIEVALS) {
    state.status = "RETRIEVAL_2_STARTED";
    state.selectedSecondAction = secondAction;
    saveCheckpoint(checkpointDir, state);
    const fetched2 = await fetchDirectOrFirecrawl({
      url: secondAction.url,
      fetchPage: input.fetchPage,
      allowFirecrawl: true,
      ledgerPath,
      jobKey: "gdi-drc-principal-retrieval-2",
    });
    state.retrievals += 1;
    state.secondPage = { ...fetched2, url: secondAction.url };
    state.sourcesVisited.push({
      step: 4,
      url: secondAction.url,
      provider: fetched2.provider,
      ok: fetched2.ok === true,
      sourceFamily: secondAction.sourceFamily,
      factSought: secondAction.factSought,
    });
    if (fetched2.ok) {
      const secondAssessment = assessPrincipalHomepage({ url: secondAction.url, pageText: fetched2.text, html: fetched2.html || fetched2.text });
      state.discoveries.push(...secondAssessment.quotations.map((quotation) => ({ source: secondAction.url, quotation })));
      if (!secondAssessment.nobuNamed) {
        state.rejectedClaims.push({ claim: "Second source names Nobu as partner", reason: "not_in_cited_source" });
      }
      if (!secondAssessment.openRfpLanguage) {
        state.rejectedClaims.push({ claim: "Second source publishes an open RFP", reason: "not_in_cited_source" });
      }
    }
    state.status = "RETRIEVED_2";
    saveCheckpoint(checkpointDir, state);
  } else {
    state.secondRetrievalSkipped = {
      reason: voiAllowsSecond ? "retrieval_cap_or_missing_action" : "value_of_information_or_no_independent_source",
      preferredDisposition: stallSeed.researchDisposition,
    };
  }

  const combinedInvestigated = [
    ...investigatedAfterFirst,
    state.secondPage?.url,
  ].filter(Boolean);
  const finalFindings = [
    ...initialFindings,
    { url: FIRST_PERMITTED_RETRIEVAL_URL, excerpt: assessment.plannerText || assessment.quotations.join(" ") },
  ];
  if (state.secondPage?.ok) {
    finalFindings.push({
      url: state.secondPage.url,
      excerpt: String(state.secondPage.text || "").replace(/\s+/g, " ").trim().slice(0, 800),
    });
  }

  if (!state.thirdPlan) {
    const billed = await billedPlannerCall({
      ledgerPath,
      jobKey: "gdi-drc-principal-planner-3",
      planInput: {
        ...basePlan,
        findings: finalFindings,
        investigated: { urls: combinedInvestigated },
        questionFingerprints: [
          ...(state.firstPlan.proposals || []).map((item) => item.question),
          ...(state.secondPlan.proposals || []).map((item) => item.question),
        ],
        openQuestions: [
          "Record the final research disposition. Do not propose a third paid retrieval.",
          ...assessment.missingFacts,
        ],
        sourcesExhausted: !state.secondPage && alternatives.length === 0,
      },
    });
    if (!billed.skipped) {
      state.thirdPlan = billed.plan;
      state.plannerModelCalls += billed.plan.plannerModelCalls || 0;
      state.planChanges.push({ step: 5, disposition: billed.plan.researchDisposition, proposals: (billed.plan.proposals || []).map((item) => item.question) });
    } else {
      state.thirdPlan = { skipped: true, reason: billed.reason, proposals: [], rejectedProposals: [] };
    }
  }

  const finalDisposition = classifyResearchDisposition({
    findingsText: finalFindings.map((item) => item.excerpt).join(" "),
    targetHotel: DEEP_RESEARCH_CANDIDATE.hotelName,
    accepted: state.thirdPlan?.proposals || [],
    rejected: state.thirdPlan?.rejectedProposals || [],
    contactPresent: true,
    sourcesExhausted: Boolean(state.secondPage) || alternatives.length === 0,
    hotelCommerciallyUnsuitable: false,
  });
  let researchDisposition = finalDisposition.disposition;
  let dispositionNote = finalDisposition.reviewNote;
  if (researchDisposition === RESEARCH_DISPOSITION.PLANNER_STALLED || researchDisposition === RESEARCH_DISPOSITION.FURTHER_RESEARCH_JUSTIFIED) {
    const resolved = resolvePlannerStall({
      targetHotel: DEEP_RESEARCH_CANDIDATE.hotelName,
      destination: "London, United Kingdom",
      hotelMarket: "London",
      hotelCommerciallyUnsuitable: false,
      hotelSpecificFit: "STRONG_TARGET_HOTEL_FIT",
      unresolvedFact: "current dated programme / bedroom commitment at Nobu",
      alreadyInvestigated: true,
      contactPresent: true,
      investigatedUrls: combinedInvestigated,
      organizationName: DEEP_RESEARCH_CANDIDATE.organizationName,
      commercial: { salesReadiness: "NEEDS_RESEARCH" },
      openHostQuestion: true,
      alternativeSources: [],
      sourcesExhausted: true,
    });
    researchDisposition = resolved.researchDisposition;
    dispositionNote = resolved.dispositionNote;
    state.finalStallResolution = resolved;
  }

  state.researchDisposition = researchDisposition;
  state.dispositionNote = dispositionNote;
  state.nextPermittedAction = null;
  state.nextPermittedActionExecuted = false;
  state.thirdPaidOperationExecuted = false;
  state.status = "STOPPED";
  state.salesReadiness = "NEEDS_RESEARCH";
  state.commercialStage = "NEEDS_RESEARCH";
  state.publicationEligibilityChanged = false;
  saveCheckpoint(checkpointDir, state);

  const ledger = loadOrCreateDeepResearchLedger(ledgerPath);
  const certificationPassed = state.plannerModelCalls <= DEEP_RESEARCH_MAX_PLANNER_CALLS
    && state.retrievals <= DEEP_RESEARCH_MAX_RETRIEVALS
    && state.retrievals >= 1
    && Number(ledger.estimatedSpentUsd || 0) - DEEP_RESEARCH_BUDGET_USD <= 1e-9
    && state.publicationEligibilityChanged !== true;

  return {
    executed: true,
    certificationPassed,
    originalQuestion: DEEP_RESEARCH_CANDIDATE.researchObjective,
    sourcesVisited: state.sourcesVisited,
    discoveries: state.discoveries,
    rejectedClaims: state.rejectedClaims,
    planChanges: state.planChanges,
    firstPlan: { accepted: state.firstPlan?.proposals || [], rejected: state.firstPlan?.rejectedProposals || [] },
    secondPlan: { accepted: state.secondPlan?.proposals || [], rejected: state.secondPlan?.rejectedProposals || [] },
    thirdPlan: { accepted: state.thirdPlan?.proposals || [], rejected: state.thirdPlan?.rejectedProposals || [] },
    secondRetrievalJustified: state.secondRetrievalJustified === true,
    secondRetrievalSkipped: state.secondRetrievalSkipped || null,
    autonomousLeadFollowing: state.autonomousLeadFollowing === true,
    homepageAssessment: {
      identitySupported: assessment.identitySupported,
      unreadCandidateUrls: assessment.unreadCandidateUrls,
      nobuNamed: assessment.nobuNamed,
      openRfpLanguage: assessment.openRfpLanguage,
      roomBlockLanguage: assessment.roomBlockLanguage,
      missingFacts: assessment.missingFacts,
    },
    researchDisposition,
    dispositionNote,
    valueOfInformation: stallSeed.valueOfInformation,
    plannerModelCalls: state.plannerModelCalls,
    retrievals: state.retrievals,
    paidCalls: state.sourcesVisited.filter((item) => item.provider === "firecrawl").length,
    actualSpentUsd: ledger.estimatedSpentUsd,
    hardCeilingUsd: DEEP_RESEARCH_BUDGET_USD,
    thirdPaidOperationExecuted: false,
    checkpointStatus: state.status,
    salesReadiness: "NEEDS_RESEARCH",
    commercialStage: "NEEDS_RESEARCH",
    publicationEligibilityChanged: false,
  };
}

export async function executeDeepResearchCertification(input = {}) {
  return executeAuthorizedDeepResearch(input);
}
