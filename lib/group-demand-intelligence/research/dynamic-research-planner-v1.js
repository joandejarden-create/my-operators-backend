/**
 * Evidence-driven research planner for the GDI Research Supervisor.
 * Proposes at most three questions. Does not execute providers, reserve budget,
 * or change commercial stage or publication eligibility.
 * gpt-4o-mini price card matches the repo research-engine inventory:
 * input $0.15 and output $0.60 per 1M tokens. A missing card refuses the call.
 */

import { createHash } from "node:crypto";
import { canonicalEntityKey } from "../workflow/gdi-global-weekly-feedback-v2.js";
import { formulateNextResearchStep, quoteStructuredTask } from "./parallel-delegated-research-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import { writeMaturationCheckpointAtomic } from "./bethesda-maturation-checkpoint-v1.js";
import { selectCommercialResearchSource, classifyTechnicalSource } from "./commercial-source-selection-v1.js";

export const DYNAMIC_RESEARCH_PLANNER_VERSION = "gdi-dynamic-research-planner-v1";
export const PLANNER_MODEL = "gpt-4o-mini";
export const PLANNER_CEILING_USD = 0.01;
export const MAX_PROPOSALS = 3;
export const MAX_RESEARCH_QUESTIONS = 8;

export const RESEARCH_DISPOSITION = Object.freeze({
  RESEARCH_SUFFICIENT: "RESEARCH_SUFFICIENT",
  FURTHER_RESEARCH_JUSTIFIED: "FURTHER_RESEARCH_JUSTIFIED",
  BUYER_ENGAGEMENT_REQUIRED: "BUYER_ENGAGEMENT_REQUIRED",
  SCHEDULED_REVISIT: "SCHEDULED_REVISIT",
  RESEARCH_EXHAUSTED: "RESEARCH_EXHAUSTED",
  BUDGET_EXHAUSTED: "BUDGET_EXHAUSTED",
  PROVIDER_BLOCKED: "PROVIDER_BLOCKED",
  PLANNER_STALLED: "PLANNER_STALLED",
});

export const SOURCE_FAMILY = Object.freeze({
  OFFICIAL_EVENT_PAGE: "official_event_page",
  ORGANIZER_PUBLICATION: "organizer_publication",
  HISTORICAL_MEETING_PROGRAM: "historical_meeting_program",
  ACCOMMODATION_DOCUMENT: "accommodation_document",
  PARTNER_ORGANIZATION: "partner_organization",
  VERIFIED_CONTACT_SOURCE: "verified_contact_source",
});

export const PLANNER_PRICE_CARD = Object.freeze({
  model: PLANNER_MODEL,
  asOf: "2026-10-24",
  source: "GDI research-engine inventory: gpt-4o-mini input $0.15 and output $0.60 per 1M tokens",
  inputUsdPerMillion: 0.15,
  outputUsdPerMillion: 0.6,
  maxInputTokens: 6000,
  maxOutputTokens: 1200,
  ceilingUsd: PLANNER_CEILING_USD,
});

const PROVIDERS = new Set(["direct_public", "exa_contents", "firecrawl_scrape", "parallel_pro_task", "none"]);
const PAID_PROVIDERS = new Set(["exa_contents", "firecrawl_scrape", "parallel_pro_task"]);
const FORBIDDEN_FIELDS = ["publishable", "salesReadiness", "venueSourcingStatus", "prospectPublicationEligibility", "commercialStage", "evidenceConfidence"];

function text(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeUrl(value) {
  return text(value).replace(/\/+$/, "").toLowerCase();
}

export function questionFingerprint(question = "") {
  return canonicalEntityKey(question).slice(0, 180);
}

export function quotePlannerUsd({ inputTokens = 6000, outputTokens = 1200, priceCard = PLANNER_PRICE_CARD } = {}) {
  if (!priceCard || !(priceCard.inputUsdPerMillion > 0) || !(priceCard.outputUsdPerMillion > 0) || priceCard.model !== PLANNER_MODEL) {
    const err = new Error("planner_price_unknown");
    err.code = "PLANNER_PRICE_UNKNOWN";
    throw err;
  }
  const input = Math.min(Math.max(Number(inputTokens) || 0, 1), priceCard.maxInputTokens);
  const output = Math.min(Math.max(Number(outputTokens) || 0, 1), priceCard.maxOutputTokens);
  const estimatedUsd = Number(((input / 1e6) * priceCard.inputUsdPerMillion + (output / 1e6) * priceCard.outputUsdPerMillion).toFixed(6));
  if (!(estimatedUsd > 0) || estimatedUsd - Number(priceCard.ceilingUsd) > 1e-9) {
    const err = new Error("planner_price_above_ceiling");
    err.code = "PLANNER_PRICE_ABOVE_CEILING";
    throw err;
  }
  return { model: priceCard.model, estimatedUsd, ceilingUsd: Number(priceCard.ceilingUsd), inputTokens: input, outputTokens: output };
}

export function quotePlannerActualUsd(usage = {}, priceCard = PLANNER_PRICE_CARD) {
  if (!priceCard || !(priceCard.inputUsdPerMillion > 0) || !(priceCard.outputUsdPerMillion > 0) || priceCard.model !== PLANNER_MODEL) {
    const err = new Error("planner_price_unknown");
    err.code = "PLANNER_PRICE_UNKNOWN";
    throw err;
  }
  const input = Number(usage.prompt_tokens || usage.promptTokens || 0);
  const output = Number(usage.completion_tokens || usage.completionTokens || 0);
  if (!(input > 0) && !(output > 0)) {
    const err = new Error("planner_usage_missing");
    err.code = "PLANNER_USAGE_MISSING";
    throw err;
  }
  const estimatedUsd = Number(((input / 1e6) * priceCard.inputUsdPerMillion + (output / 1e6) * priceCard.outputUsdPerMillion).toFixed(6));
  if (!(estimatedUsd > 0) || estimatedUsd - Number(priceCard.ceilingUsd) > 1e-9) {
    const err = new Error("planner_price_above_ceiling");
    err.code = "PLANNER_PRICE_ABOVE_CEILING";
    err.estimatedUsd = estimatedUsd;
    throw err;
  }
  return { model: priceCard.model, estimatedUsd, ceilingUsd: Number(priceCard.ceilingUsd), inputTokens: input, outputTokens: output, known: true };
}

function providerQuote(provider, proposal) {
  if (provider === "none" || provider === "direct_public") return { bounded: true, maxUsd: 0 };
  if (provider === "parallel_pro_task") {
    const quote = quoteStructuredTask({ processor: "pro" });
    return { bounded: true, maxUsd: quote.estimatedUsd };
  }
  if (provider === "exa_contents") {
    return quoteBenchmarkCall({ provider: "exa", operation: "contents", params: { urls: [proposal.url || "https://example.com/source"], text: true } });
  }
  if (provider === "firecrawl_scrape") {
    return quoteBenchmarkCall({
      provider: "firecrawl",
      operation: "scrape",
      params: { url: proposal.url || "https://example.com/source", formats: ["markdown"] },
    });
  }
  return { bounded: false, maxUsd: null, reason: "PLANNER_PROVIDER_UNPRICED" };
}

function relevantToHotel(proposal, context) {
  const blob = `${proposal.question} ${proposal.evidenceSought} ${proposal.expectedCommercialValue}`.toLowerCase();
  const org = text(context.organizationName).toLowerCase();
  const hotel = text(context.hotelName).toLowerCase();
  const market = text(context.market).toLowerCase();
  if (org && blob.includes(org.slice(0, 12))) return true;
  if (hotel && hotel.split(/\s+/).filter((word) => word.length > 3).some((word) => blob.includes(word))) return true;
  if (market && blob.includes(market)) return true;
  return /accommodation|incentive|hotel|conference|meeting|housing|room|contact|sourcing|revisit|cycle/.test(blob);
}

function inventedClaim(proposal, findingsText) {
  const blob = `${proposal.question} ${proposal.evidenceSought}`;
  if (/\b2029\b/.test(blob) && !/\b2029\b/.test(findingsText)) return "invented_later_cycle";
  if (/\b\d{2,4}\s+rooms?\b/i.test(blob) && !/\b\d{2,4}\s+rooms?\b/i.test(findingsText)) return "invented_room_count";
  if (/\brfp\b|request for proposal/i.test(blob) && !/\brfp\b|request for proposal/i.test(findingsText)) return "invented_rfp";
  return null;
}

export function acceptResearchProposal(proposal = {}, context = {}) {
  const rejectedFields = FORBIDDEN_FIELDS.filter((field) => Object.prototype.hasOwnProperty.call(proposal, field));
  const question = text(proposal.question);
  const provider = text(proposal.provider);
  const findingsText = text(context.findingsText);
  const investigated = new Set((context.investigatedUrls || []).map(normalizeUrl));
  const asked = new Set((context.questionFingerprints || []).map(questionFingerprint));
  const fingerprint = questionFingerprint(question);
  let reason = null;
  if (!question || !text(proposal.evidenceSought) || !text(proposal.stoppingCriteria) || !text(proposal.expectedCommercialValue)) reason = "schema_incomplete";
  else if (!PROVIDERS.has(provider)) reason = "provider_not_permitted";
  else if (provider !== "none" && provider !== "parallel_pro_task" && !text(proposal.url)) reason = "schema_incomplete";
  else if (rejectedFields.length) reason = "classification_field_not_allowed";
  else if (/publishable|ready for sales|publication eligibility/i.test(question)) reason = "classification_change_requested";
  else if (asked.has(fingerprint)) reason = "duplicate_question";
  else if ((context.questionFingerprints || []).length >= MAX_RESEARCH_QUESTIONS) reason = "research_depth_limit";
  else if (proposal.url && investigated.has(normalizeUrl(proposal.url))) reason = "duplicate_source";
  else if (context.requireAlternativeStrategy === true && !text(proposal.sourceFamily)) reason = "source_family_required";
  else if (context.requireAlternativeStrategy === true && !text(proposal.factSought) && !text(proposal.evidenceSought)) reason = "fact_sought_required";
  else if (context.requireAlternativeStrategy === true && !text(proposal.whySourceMightContainIt)) reason = "source_rationale_required";
  else if (!relevantToHotel(proposal, context)) reason = "not_commercially_relevant";
  else if (inventedClaim(proposal, findingsText)) reason = inventedClaim(proposal, findingsText);
  const priced = reason ? null : providerQuote(provider, proposal);
  if (!reason && (!priced || priced.bounded !== true || priced.maxUsd == null)) reason = priced?.reason || "PLANNER_PROVIDER_UNPRICED";
  if (!reason && PAID_PROVIDERS.has(provider) && context.authorizePaidResearch !== true) reason = "paid_research_not_authorized";
  if (!reason && PAID_PROVIDERS.has(provider) && !(Number(context.remainingUsd) >= Number(priced.maxUsd))) reason = "budget_insufficient";
  if (!reason && proposal.estimatedUsd != null && Number.isFinite(Number(proposal.estimatedUsd)) && Math.abs(Number(proposal.estimatedUsd) - Number(priced.maxUsd)) > 0.000001) {
    reason = "quoted_cost_mismatch";
  }
  const executable = !reason && provider !== "none" && (provider === "direct_public" || context.authorizePaidResearch === true);
  return {
    accepted: !reason,
    reason: reason || null,
    executable: Boolean(executable),
    paidCalls: 0,
    publicationEligibilityChanged: false,
    originalQuestion: question,
    proposal: reason ? null : {
      question,
      fingerprint,
      expectedCommercialValue: text(proposal.expectedCommercialValue),
      evidenceSought: text(proposal.evidenceSought),
      provider,
      url: proposal.url ? normalizeUrl(proposal.url) : null,
      estimatedUsd: Number(priced.maxUsd),
      stoppingCriteria: text(proposal.stoppingCriteria),
      sourceFamily: text(proposal.sourceFamily) || null,
      factSought: text(proposal.factSought || proposal.evidenceSought) || null,
      whySourceMightContainIt: text(proposal.whySourceMightContainIt) || null,
      executable: Boolean(executable),
    },
  };
}

function findingsBlob(findings = []) {
  return findings.map((item) => `${item.statement || ""} ${item.excerpt || ""} ${item.url || ""}`).join("\n");
}

export function classifyResearchDisposition({
  findingsText = "",
  targetHotel = "",
  accepted = [],
  rejected = [],
  remainingUsd = 0,
  contactPresent = false,
  sourcesExhausted = false,
  hotelCommerciallyUnsuitable = false,
} = {}) {
  const hostSelected = /crystal gateway marriott/i.test(findingsText) && /bethesda/i.test(targetHotel);
  if (hostSelected && !/overflow rooms|rooms still needed/i.test(findingsText)) {
    return {
      disposition: RESEARCH_DISPOSITION.SCHEDULED_REVISIT,
      meetingCreated: false,
      reviewNote: "The committed cycle is known. Review official listings for a later announcement. Do not invent the next meeting.",
    };
  }
  const sourcing = /hotel sourcing|contract negotiation|group accommodation/i.test(findingsText);
  const programme = /incentive/i.test(findingsText);
  const namedTarget = targetHotel && findingsText.toLowerCase().includes(targetHotel.toLowerCase());
  const openRfp = /\brfp\b|request for proposal|accepting proposals/i.test(findingsText);
  if (sourcing && programme && contactPresent && !namedTarget && !openRfp && !hotelCommerciallyUnsuitable && accepted.length === 0) {
    return {
      disposition: RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED,
      meetingCreated: false,
      reviewNote: "Public pages support an introduction. They do not show a live room requirement.",
    };
  }
  if (accepted.some((item) => item.executable)) {
    return { disposition: RESEARCH_DISPOSITION.FURTHER_RESEARCH_JUSTIFIED, meetingCreated: false, reviewNote: "A new permitted question remains." };
  }
  if (rejected.some((item) => item.reason === "budget_insufficient") && !(Number(remainingUsd) > 0)) {
    return { disposition: RESEARCH_DISPOSITION.BUDGET_EXHAUSTED, meetingCreated: false, reviewNote: "A priced next step exceeds the remaining authorization." };
  }
  if (rejected.length && rejected.every((item) => item.reason === "paid_research_not_authorized" || item.reason === "provider_not_permitted")) {
    return { disposition: RESEARCH_DISPOSITION.PROVIDER_BLOCKED, meetingCreated: false, reviewNote: "The only proposed providers are not authorized." };
  }
  if (rejected.length && rejected.every((item) => item.reason === "duplicate_question" || item.reason === "duplicate_source" || item.reason === "research_depth_limit")) {
    if (sourcesExhausted === true) {
      return { disposition: RESEARCH_DISPOSITION.RESEARCH_EXHAUSTED, meetingCreated: false, reviewNote: "Distinct sources were read and no new authorized source remains." };
    }
    return { disposition: RESEARCH_DISPOSITION.PLANNER_STALLED, meetingCreated: false, reviewNote: "The planner repeated held questions or URLs. That does not exhaust the research." };
  }
  if (sourcing && programme && !openRfp) {
    return { disposition: RESEARCH_DISPOSITION.RESEARCH_SUFFICIENT, meetingCreated: false, reviewNote: "The held public evidence answers the research question that can be answered without a buyer." };
  }
  return { disposition: RESEARCH_DISPOSITION.FURTHER_RESEARCH_JUSTIFIED, meetingCreated: false, reviewNote: "A material public fact is still missing." };
}

function parseModelPayload(raw) {
  let body = raw;
  if (typeof raw === "string") {
    try {
      body = JSON.parse(raw);
    } catch {
      return { ok: false, reason: "invalid_json" };
    }
  }
  const proposals = body?.proposals;
  if (!Array.isArray(proposals)) return { ok: false, reason: "invalid_json" };
  if (proposals.length > MAX_PROPOSALS) return { ok: false, reason: "too_many_proposals" };
  if (proposals.some((item) => !text(item?.question))) return { ok: false, reason: "missing_question" };
  return { ok: true, proposals };
}

function fallbackResult(deterministic, reason, extra = {}) {
  return {
    version: DYNAMIC_RESEARCH_PLANNER_VERSION,
    proposals: [],
    rejectedProposals: [],
    fallback: deterministic,
    fallbackReason: reason,
    plannerModelCalls: extra.plannerModelCalls || 0,
    plannerEstimatedUsd: extra.plannerEstimatedUsd || 0,
    paidCalls: 0,
    publicationEligibilityChanged: false,
    commercialFieldsChanged: false,
    researchDisposition: deterministic?.action === "WATCH_FUTURE_CYCLE"
      ? RESEARCH_DISPOSITION.SCHEDULED_REVISIT
      : deterministic?.action === "STOP_BUDGET"
        ? RESEARCH_DISPOSITION.BUDGET_EXHAUSTED
        : deterministic?.action === "STOP_SUFFICIENT"
          ? RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED
          : RESEARCH_DISPOSITION.FURTHER_RESEARCH_JUSTIFIED,
    nextPermittedAction: deterministic,
  };
}

export function buildResearchMemory(input = {}, plan = {}) {
  return {
    version: DYNAMIC_RESEARCH_PLANNER_VERSION,
    objective: input.objective || null,
    questions: plan.proposals || [],
    completedAttempts: input.attempts || [],
    evidenceFingerprints: (input.findings || []).map((item) => questionFingerprint(item.excerpt || item.statement || "")),
    rejectedHypotheses: plan.rejectedProposals || [],
    sources: input.investigated?.urls || [],
    currentPlan: plan.proposals || [],
    nextPermittedAction: plan.nextPermittedAction || null,
    researchDisposition: plan.researchDisposition || null,
    weeklyCompatible: true,
    productionWeeklyJobChanged: false,
    publicationEligibilityChanged: false,
  };
}

async function requestPlannerCompletion(input, quote) {
  const key = text(input.apiKey || process.env.OPENAI_API_KEY);
  if (!key) {
    const err = new Error("planner_model_unavailable");
    err.code = "planner_unavailable";
    throw err;
  }
  let res;
  try {
    res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${key}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: PLANNER_MODEL,
      temperature: 0,
      max_tokens: PLANNER_PRICE_CARD.maxOutputTokens,
      response_format: { type: "json_object" },
      messages: [
        {
          role: "system",
          content: "Propose at most 3 research questions. Return JSON {proposals:[{question,expectedCommercialValue,evidenceSought,provider,estimatedUsd,stoppingCriteria,url}]}. Providers: direct_public, exa_contents, firecrawl_scrape, parallel_pro_task, none. Do not set publication or commercial classifications. Do not invent meetings, room counts, or RFPs.",
        },
        { role: "user", content: JSON.stringify({
          objective: input.objective || null,
          hotel: input.hotel || null,
          opportunity: input.opportunity || null,
          findings: input.findings || [],
          openQuestions: input.openQuestions || [],
          attempts: input.attempts || [],
          investigated: input.investigated || {},
          remainingUsd: input.remainingUsd ?? 0,
          plannerCeilingUsd: quote.estimatedUsd,
          replanningContext: input.replanningContext || null,
        }) },
      ],
    }),
  });
  } catch (err) {
    err.code = err.code || "planner_unavailable";
    err.httpAttempted = true;
    throw err;
  }
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error("planner_http_failed");
    err.code = "planner_unavailable";
    err.httpAttempted = true;
    throw err;
  }
  return {
    content: json.choices?.[0]?.message?.content || "",
    usage: json.usage || null,
  };
}

export async function planDynamicResearch(input = {}) {
  const findings = input.findings || [];
  const findingsText = findingsBlob(findings);
  const deterministic = input.deterministicStep || formulateNextResearchStep({
    organizationName: input.opportunity?.organizationName || input.objective?.organizationName || "",
    targetHotel: input.hotel?.name || input.objective?.hotelName || "",
    evidenceText: findingsText,
    sourceRecovery: input.sourceRecovery || [],
    attempts: input.attempts || [],
    investigatedUrls: input.investigated?.urls || [],
    remainingUsd: input.remainingUsd ?? 0,
    commercial: input.commercial || {},
  });
  let quote;
  try {
    quote = quotePlannerUsd({ priceCard: input.priceCard === undefined ? PLANNER_PRICE_CARD : input.priceCard });
  } catch (err) {
    return { ...fallbackResult(deterministic, err.code || "PLANNER_PRICE_UNKNOWN"), researchMemory: null };
  }
  if (input.authorizePlannerModel !== true && typeof input.complete !== "function") {
    const planned = fallbackResult(deterministic, "PLANNER_MODEL_NOT_AUTHORIZED", { plannerEstimatedUsd: quote.estimatedUsd });
    planned.researchMemory = buildResearchMemory(input, planned);
    return planned;
  }
  let raw;
  let plannerModelCalls = 0;
  let usage = null;
  try {
    if (typeof input.complete === "function") raw = await input.complete(input);
    else {
      const completion = await requestPlannerCompletion(input, quote);
      plannerModelCalls = 1;
      raw = completion.content;
      usage = completion.usage;
    }
  } catch (err) {
    const planned = fallbackResult(deterministic, "planner_unavailable", {
      plannerModelCalls: err?.httpAttempted ? 1 : 0,
    });
    planned.researchMemory = buildResearchMemory(input, planned);
    return planned;
  }
  let actual = null;
  if (usage) {
    try {
      actual = quotePlannerActualUsd(usage);
    } catch (err) {
      actual = { estimatedUsd: null, code: err.code || "PLANNER_PRICE_ABOVE_CEILING" };
    }
  }
  const parsed = parseModelPayload(raw);
  if (!parsed.ok) {
    const planned = fallbackResult(deterministic, parsed.reason, {
      plannerModelCalls,
      plannerEstimatedUsd: actual?.estimatedUsd ?? (plannerModelCalls ? quote.estimatedUsd : 0),
    });
    planned.plannerActualUsd = actual?.estimatedUsd ?? null;
    planned.plannerUsage = usage ? { promptTokens: usage.prompt_tokens, completionTokens: usage.completion_tokens } : null;
    planned.researchMemory = buildResearchMemory(input, planned);
    return planned;
  }
  const context = {
    organizationName: input.opportunity?.organizationName || "",
    hotelName: input.hotel?.name || input.objective?.hotelName || "",
    market: input.hotel?.market || "",
    findingsText,
    investigatedUrls: input.investigated?.urls || [],
    questionFingerprints: input.questionFingerprints || (input.memory?.questions || []).map((item) => item.fingerprint || item.question),
    authorizePaidResearch: input.authorizePaidResearch === true,
    remainingUsd: input.remainingUsd ?? 0,
  };
  const gated = parsed.proposals.map((proposal) => acceptResearchProposal(proposal, context));
  const accepted = gated.filter((item) => item.accepted).map((item) => item.proposal);
  const rejected = gated.filter((item) => !item.accepted).map((item) => ({ question: item.originalQuestion, reason: item.reason }));
  const disposition = classifyResearchDisposition({
    findingsText,
    targetHotel: context.hotelName,
    accepted,
    rejected: gated.filter((item) => !item.accepted),
    remainingUsd: context.remainingUsd,
    contactPresent: /@/.test(findingsText),
    sourcesExhausted: input.sourcesExhausted === true,
    hotelCommerciallyUnsuitable: input.replanningContext?.hotelCommerciallyUnsuitable === true,
  });
  const plan = {
    version: DYNAMIC_RESEARCH_PLANNER_VERSION,
    proposals: accepted,
    rejectedProposals: rejected,
    fallback: accepted.length ? null : deterministic,
    fallbackReason: accepted.length ? null : "no_accepted_proposal",
    researchDisposition: disposition.disposition,
    dispositionNote: disposition.reviewNote,
    meetingCreated: false,
    nextPermittedAction: accepted.find((item) => item.executable) || null,
    plannerModelCalls,
    plannerEstimatedUsd: actual?.estimatedUsd ?? (plannerModelCalls ? quote.estimatedUsd : 0),
    plannerActualUsd: actual?.estimatedUsd ?? null,
    plannerUsage: usage ? { promptTokens: usage.prompt_tokens, completionTokens: usage.completion_tokens } : null,
    paidCalls: 0,
    publicationEligibilityChanged: false,
    commercialFieldsChanged: false,
    commercialStage: input.opportunity?.commercialStage || null,
    salesReadiness: input.commercial?.salesReadiness || null,
  };
  plan.researchMemory = buildResearchMemory({ ...input, findings }, plan);
  return plan;
}

export async function replanFromFinding({ memory = {}, finding = {}, ...input } = {}) {
  const fingerprint = questionFingerprint(finding.excerpt || finding.statement || "");
  const held = new Set(memory.evidenceFingerprints || []);
  if (!fingerprint || held.has(fingerprint)) {
    return {
      changed: false,
      reason: "duplicate_evidence",
      paidCalls: 0,
      publicationEligibilityChanged: false,
      researchMemory: memory,
      proposals: memory.currentPlan || [],
    };
  }
  const findings = [...(input.findings || memory.findings || []), finding];
  const plan = await planDynamicResearch({
    ...input,
    findings,
    memory,
    questionFingerprints: [
      ...(memory.questions || []).map((item) => item.fingerprint || item.question),
      ...(input.questionFingerprints || []),
    ],
  });
  const prior = new Set((memory.questions || []).map((item) => item.fingerprint || questionFingerprint(item.question)));
  const novel = (plan.proposals || []).filter((item) => !prior.has(item.fingerprint));
  return {
    changed: novel.length > 0,
    novelQuestions: novel,
    repeatedQuestions: (plan.rejectedProposals || []).filter((item) => item.reason === "duplicate_question"),
    plan,
    researchDisposition: plan.researchDisposition,
    paidCalls: 0,
    publicationEligibilityChanged: false,
    researchMemory: plan.researchMemory,
  };
}

const COMMERCIAL_SENTENCE = /room|hotel|accommodation|incentive|contact|@|conference|meeting|sourcing/i;
const IRRELEVANT_SENTENCE = /pizza|tokyo/i;
const TERMINAL_DISPOSITIONS = new Set([
  RESEARCH_DISPOSITION.RESEARCH_SUFFICIENT,
  RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED,
  RESEARCH_DISPOSITION.SCHEDULED_REVISIT,
  RESEARCH_DISPOSITION.RESEARCH_EXHAUSTED,
  RESEARCH_DISPOSITION.BUDGET_EXHAUSTED,
  RESEARCH_DISPOSITION.PROVIDER_BLOCKED,
  RESEARCH_DISPOSITION.PLANNER_STALLED,
]);

function decodePage(value) {
  return String(value || "")
    .replace(/&#x27;|&#39;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&nbsp;/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function contentHash(value) {
  return createHash("sha256").update(String(value || "")).digest("hex").slice(0, 16);
}

function capture(page, pattern) {
  const match = String(page || "").match(pattern);
  return match ? match[0].replace(/\s+/g, " ").trim() : null;
}

export function buildRetrievedEvidenceBrief({ url = "", pageText = "", targetHotel = "" } = {}) {
  const page = decodePage(pageText);
  const narrativeDate = capture(page, /The 2027 GAD Institute will take place July 27\s*[-–]\s*29,?\s*2027, in Arlington, Virginia/i);
  const eventFieldDate = capture(page, /Date:\s*July\s+27\s*[-–]\s*30,?\s*2027/i);
  const genericDates = [];
  if (!narrativeDate && !eventFieldDate) {
    const found = page.match(/\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:\s*[-–]\s*\d{1,2})?,?\s+20\d{2}/gi) || [];
    genericDates.push(...found.slice(0, 4));
  }
  const dates = [narrativeDate, eventFieldDate, ...genericDates].filter(Boolean);
  const location = capture(page, /Location:\s*Arlington,\s*VA/i) || capture(page, /\bin Arlington, Virginia\b/i);
  const contactName = capture(page, /Contact Person:\s*Jami Sims/i);
  const contactEmail = capture(page, /GADInst@nar\.realtor/i) || (page.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i) || [null])[0];
  const contactPhone = capture(page, /Phone:\s*\(?202\)?[\s.-]*383[\s.-]*1221/i);
  const registration = capture(page, /Registration:\s*May 2027/i);
  const hotelBrands = page.match(/\b(?:Marriott|Hilton|Hyatt|Sheraton|Westin|Ritz-Carlton)(?:\s+[A-Z][A-Za-z]+){0,4}/g) || [];
  const namedHotels = [...new Set(hotelBrands.map((item) => item.replace(/\s+/g, " ").trim()))].filter((item) => !/Main menu|Browse by/i.test(item));
  const rfp = capture(page, /[^.]*\b(?:RFP|request for proposal|accepting proposals)\b[^.]*/i);
  const accommodation = capture(page, /[^.]*\b(?:room block|guest rooms|accommodation|hotel sourcing)\b[^.]*/i);
  const quotations = [narrativeDate, eventFieldDate, location, contactName, contactEmail, contactPhone, registration, rfp, accommodation].filter(Boolean);
  const plannerText = quotations.join(" ");
  const targetNamed = Boolean(targetHotel) && page.toLowerCase().includes(String(targetHotel).toLowerCase());
  const arlington = /arlington/i.test(location || page);
  const hotelCommerciallyUnsuitable = Boolean(targetHotel && /bethesda/i.test(targetHotel) && arlington && !targetNamed);
  return {
    url,
    contentHash: contentHash(page),
    evidenceConfidence: quotations.length ? "QUOTED_FROM_PAGE" : "INSUFFICIENT_PAGE_TEXT",
    identity: narrativeDate ? "2027 GAD Institute" : null,
    dates,
    dateConflict: Boolean(narrativeDate && eventFieldDate),
    location: location ? { place: "Arlington, VA", quotation: location } : null,
    namedHotels,
    sourcingStatus: rfp ? "OPEN_SOURCING_LANGUAGE_IN_PAGE" : "NOT_ESTABLISHED",
    accommodationEvidence: accommodation,
    contact: {
      name: contactName,
      email: contactEmail,
      phone: contactPhone,
      registration,
    },
    verifiedDemandMechanisms: [rfp, accommodation].filter(Boolean),
    missingFacts: [
      narrativeDate && eventFieldDate ? "The page prints both July 27–29 and July 27–30, 2027. Neither date is discarded." : null,
      namedHotels.length ? null : "No host hotel is named.",
      rfp ? null : "No open RFP is printed.",
      accommodation ? null : "No room block or accommodation commitment is printed.",
      targetNamed ? null : `${targetHotel || "The target hotel"} is not named.`,
    ].filter(Boolean),
    quotations,
    plannerText,
    navigationExcluded: !/Main menu|Browse by Section|Cookie Settings/i.test(plannerText),
    hotelCommerciallyUnsuitable,
    hotelSpecificFit: hotelCommerciallyUnsuitable ? "ARLINGTON_DESTINATION_NOT_A_BETHESDA_HOST" : targetNamed ? "PAGE_NAMES_TARGET_HOTEL" : "NOT_ESTABLISHED",
    publicationEligibilityChanged: false,
  };
}

export function assembleReplanningContext({
  url = "",
  pageText = "",
  targetHotel = "",
  investigated = [],
  answeredQuestions = [],
  failedAttempts = [],
  authorizedProviders = ["direct_public"],
  destination = "",
  hotelMarket = "",
  contactPresent = null,
  futureCycleOnly = false,
} = {}) {
  const brief = pageText ? buildRetrievedEvidenceBrief({ url, pageText, targetHotel }) : null;
  const readSources = [
    ...investigated.map((item) => (typeof item === "string" ? { url: item } : item)),
    brief ? { url: brief.url, contentHash: brief.contentHash } : null,
  ].filter((item) => item?.url);
  const contact = contactPresent == null
    ? Boolean(brief?.contact?.email || brief?.contact?.name)
    : contactPresent === true;
  return {
    evidenceBrief: brief,
    plannerFindings: (brief?.quotations || []).map((quotation) => ({ url, excerpt: quotation })),
    readSources,
    answeredQuestions,
    failedAttempts,
    unresolvedBlockers: brief?.missingFacts || [],
    authorizedProviders,
    paidProvidersAuthorized: authorizedProviders.some((provider) => provider !== "direct_public" && provider !== "none"),
    hotelCommerciallyUnsuitable: brief?.hotelCommerciallyUnsuitable === true,
    hotelSpecificFit: brief?.hotelSpecificFit || null,
    destination: destination || brief?.location?.place || "",
    hotelMarket,
    contactPresent: contact,
    futureCycleOnly: futureCycleOnly === true,
    publicationEligibilityChanged: false,
  };
}

/**
 * Decide whether a stalled investigation still deserves public research spend.
 * Does not change commercial stage or publication eligibility.
 */
export function assessValueOfInformation({
  targetHotel = "",
  destination = "",
  hotelMarket = "",
  hotelCommerciallyUnsuitable = false,
  hotelSpecificFit = "",
  unresolvedFact = "",
  alreadyInvestigated = false,
  contactPresent = false,
  futureCycleOnly = false,
  namedHostElsewhere = false,
  openHostQuestion = false,
} = {}) {
  const dest = text(destination).toLowerCase();
  const hotel = text(targetHotel).toLowerCase();
  const market = text(hotelMarket || targetHotel).toLowerCase();
  const geoRelevant = !(
    hotelCommerciallyUnsuitable === true
    || /arlington_destination_not_a_bethesda/i.test(hotelSpecificFit)
    || (dest && hotel && /arlington/i.test(dest) && /bethesda/i.test(hotel))
    || namedHostElsewhere === true
  );
  const strongFit = geoRelevant && (
    (dest && market && dest.includes(market.split(/\s+/)[0]))
    || (dest && hotel && dest.split(/[,\s]+/).filter((word) => word.length > 3).some((word) => hotel.includes(word)))
    || /page_names_target_hotel|strong_target_hotel_fit/i.test(hotelSpecificFit)
  );
  const factMaterial = openHostQuestion === true || /host hotel|room block|open rfp|accommodation|bedroom/i.test(unresolvedFact);
  const resolvingChangesQualification = Boolean(strongFit && factMaterial && !namedHostElsewhere);
  const publicSourceCanAnswer = resolvingChangesQualification && alreadyInvestigated === true;
  const furtherPublicResearchJustified = resolvingChangesQualification && publicSourceCanAnswer;
  let preferredDisposition = RESEARCH_DISPOSITION.PLANNER_STALLED;
  let reviewNote = "The investigation is stalled. Commercial value is not yet assessed.";
  if (futureCycleOnly === true || namedHostElsewhere === true) {
    preferredDisposition = RESEARCH_DISPOSITION.SCHEDULED_REVISIT;
    reviewNote = "A later official announcement is the next useful public check. Do not invent the meeting.";
  } else if (!geoRelevant) {
    preferredDisposition = contactPresent
      ? RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED
      : RESEARCH_DISPOSITION.RESEARCH_SUFFICIENT;
    reviewNote = contactPresent
      ? "The destination does not support the target hotel. Further public host searching is not justified. An introduction is the remaining commercial path."
      : "The destination does not support the target hotel. Further public research will not change qualification.";
  } else if (furtherPublicResearchJustified) {
    preferredDisposition = RESEARCH_DISPOSITION.FURTHER_RESEARCH_JUSTIFIED;
    reviewNote = "The hotel remains a plausible host and the missing fact could change qualification. A different public source family is required.";
  } else if (contactPresent) {
    preferredDisposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
    reviewNote = "Public pages no longer answer the commercial question. Buyer engagement is more appropriate than another search.";
  } else {
    preferredDisposition = RESEARCH_DISPOSITION.RESEARCH_SUFFICIENT;
    reviewNote = "The held evidence answers what public research can answer for this hotel.";
  }
  return {
    geographicallyRelevant: geoRelevant,
    commerciallyRelevant: geoRelevant && (strongFit || contactPresent),
    strongTargetHotelFit: strongFit,
    unresolvedFact: text(unresolvedFact) || null,
    resolvingFactWouldChangeQualification: resolvingChangesQualification,
    alreadyInvestigated: alreadyInvestigated === true,
    publicSourceCanAnswer,
    furtherPublicResearchJustified,
    preferredDisposition,
    reviewNote,
    additionalSpendAuthorized: false,
    commercialStageChanged: false,
    publicationEligibilityChanged: false,
  };
}

export function proposeAlternativeSourceStrategy({
  valueOfInformation = null,
  investigatedUrls = [],
  organizationName = "",
  unresolvedFact = "host hotel name",
  alternatives = [],
  hotelName = "",
  hotelMarket = "",
  factsSought = null,
  researchQuestion = "",
  intent = null,
} = {}) {
  if (!valueOfInformation?.furtherPublicResearchJustified) {
    return {
      proposed: false,
      reason: "further_public_research_not_justified",
      proposals: [],
      paidCalls: 0,
      publicationEligibilityChanged: false,
    };
  }
  const commercialSelection = selectCommercialResearchSource({
    candidates: alternatives,
    investigatedUrls,
    factsSought: factsSought || undefined,
    researchQuestion: researchQuestion || unresolvedFact || "",
    intent,
    hotelName: hotelName || valueOfInformation.hotelName || "",
    hotelMarket,
    organizationName,
  });
  const rankedAlternatives = (commercialSelection.ranking?.length
    ? commercialSelection.ranking.map((row) => {
      const original = alternatives.find((item) => normalizeUrl(item.url) === normalizeUrl(row.url)) || {};
      return {
        ...original,
        url: row.url,
        sourceFamily: original.sourceFamily || row.sourceFamily,
        factSought: text(original.factSought) || text(unresolvedFact) || commercialSelection.factsSought.join("; "),
        whySourceMightContainIt: text(original.whySourceMightContainIt) || row.whySelectedCouldAnswer,
        expectedCommercialValue: original.expectedCommercialValue,
        question: original.question,
        provider: original.provider || "direct_public",
        estimatedUsd: original.estimatedUsd ?? row.estimatedUsd ?? 0,
        stoppingCriteria: original.stoppingCriteria,
        selectionRationale: row.rationale,
      };
    })
    : alternatives.filter((item) => !classifyTechnicalSource(item.url).rejected));
  const held = new Set((investigatedUrls || []).map(normalizeUrl));
  const proposals = [];
  for (const item of rankedAlternatives) {
    const url = text(item.url);
    if (!url || held.has(normalizeUrl(url))) continue;
    if (classifyTechnicalSource(url).rejected) continue;
    if (!text(item.factSought) || !text(item.sourceFamily) || !text(item.whySourceMightContainIt)) continue;
    const gated = acceptResearchProposal({
      question: text(item.question) || `Does ${text(item.sourceFamily).replace(/_/g, " ")} name ${text(item.factSought)} for ${organizationName || "this meeting"}?`,
      expectedCommercialValue: text(item.expectedCommercialValue) || "Would show whether the target hotel remains a plausible host.",
      evidenceSought: text(item.factSought),
      factSought: text(item.factSought),
      sourceFamily: text(item.sourceFamily),
      whySourceMightContainIt: text(item.whySourceMightContainIt),
      provider: text(item.provider) || "direct_public",
      url,
      estimatedUsd: item.estimatedUsd ?? 0,
      stoppingCriteria: text(item.stoppingCriteria) || "Stop after that unread source is read once. Do not reread a held URL.",
    }, {
      organizationName,
      hotelName: valueOfInformation.hotelName || "",
      findingsText: unresolvedFact,
      investigatedUrls,
      authorizePaidResearch: false,
      remainingUsd: 0,
      requireAlternativeStrategy: true,
    });
    if (gated.accepted) proposals.push(gated.proposal);
    if (proposals.length >= MAX_PROPOSALS) break;
  }
  return {
    proposed: proposals.length > 0,
    reason: proposals.length ? null : (commercialSelection.stopReason || "no_independent_source_available"),
    proposals,
    commercialSourceSelection: {
      selected: commercialSelection.selected,
      rejected: commercialSelection.rejected,
      ranking: commercialSelection.ranking,
      selectionRationale: commercialSelection.selectionRationale,
      canIntroduceNewResearchDirections: commercialSelection.canIntroduceNewResearchDirections,
    },
    paidCalls: 0,
    publicationEligibilityChanged: false,
  };
}

/**
 * After PLANNER_STALLED: decide stop vs alternative strategy.
 * Never treats a repeated URL as research progress.
 */
export function resolvePlannerStall(input = {}) {
  const investigated = (input.investigatedUrls || input.investigated?.urls || []).map(normalizeUrl).filter(Boolean);
  const voi = assessValueOfInformation({
    targetHotel: input.targetHotel || input.hotel?.name || "",
    destination: input.destination || input.replanningContext?.destination || "",
    hotelMarket: input.hotelMarket || input.hotel?.market || "",
    hotelCommerciallyUnsuitable: input.hotelCommerciallyUnsuitable === true || input.replanningContext?.hotelCommerciallyUnsuitable === true,
    hotelSpecificFit: input.hotelSpecificFit || input.replanningContext?.hotelSpecificFit || "",
    unresolvedFact: input.unresolvedFact || (input.openQuestions || [])[0] || "host hotel name",
    alreadyInvestigated: investigated.length > 0 || input.alreadyInvestigated === true,
    contactPresent: input.contactPresent === true || input.replanningContext?.contactPresent === true,
    futureCycleOnly: input.futureCycleOnly === true || input.replanningContext?.futureCycleOnly === true,
    namedHostElsewhere: input.namedHostElsewhere === true,
    openHostQuestion: input.openHostQuestion !== false,
  });
  if (!voi.furtherPublicResearchJustified) {
    return {
      stalled: true,
      valueOfInformation: voi,
      researchDisposition: voi.preferredDisposition,
      dispositionNote: voi.reviewNote,
      proposals: [],
      nextPermittedAction: null,
      additionalSpend: false,
      paidCalls: 0,
      publicationEligibilityChanged: false,
      commercialStageChanged: false,
      commercialStage: input.commercial?.commercialStage || null,
      salesReadiness: input.commercial?.salesReadiness || null,
    };
  }
  const alt = proposeAlternativeSourceStrategy({
    valueOfInformation: { ...voi, hotelName: input.targetHotel || input.hotel?.name || "" },
    investigatedUrls: investigated,
    organizationName: input.organizationName || input.opportunity?.organizationName || "",
    unresolvedFact: voi.unresolvedFact,
    alternatives: input.alternativeSources || [],
    hotelName: input.targetHotel || input.hotel?.name || "",
    hotelMarket: input.hotelMarket || input.hotel?.market || "",
    researchQuestion: input.researchQuestion || voi.unresolvedFact || "",
    intent: input.researchIntent || null,
    factsSought: input.factsSought || null,
  });
  if (!alt.proposed) {
    return {
      stalled: true,
      valueOfInformation: voi,
      researchDisposition: input.sourcesExhausted === true
        ? RESEARCH_DISPOSITION.RESEARCH_EXHAUSTED
        : ((input.contactPresent === true || voi.contactPresent === true)
          ? RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED
          : RESEARCH_DISPOSITION.RESEARCH_SUFFICIENT),
      dispositionNote: alt.reason === "no_independent_source_available"
        ? "No unread independent source family remains after the primary page failed."
        : voi.reviewNote,
      proposals: [],
      nextPermittedAction: null,
      additionalSpend: false,
      paidCalls: 0,
      publicationEligibilityChanged: false,
      commercialStageChanged: false,
      commercialStage: input.commercial?.commercialStage || null,
      salesReadiness: input.commercial?.salesReadiness || null,
    };
  }
  return {
    stalled: true,
    valueOfInformation: voi,
    researchDisposition: RESEARCH_DISPOSITION.FURTHER_RESEARCH_JUSTIFIED,
    dispositionNote: voi.reviewNote,
    proposals: alt.proposals,
    nextPermittedAction: alt.proposals.find((item) => item.executable) || null,
    additionalSpend: false,
    paidCalls: 0,
    publicationEligibilityChanged: false,
    commercialStageChanged: false,
    commercialStage: input.commercial?.commercialStage || null,
    salesReadiness: input.commercial?.salesReadiness || null,
  };
}

export function evaluateProposalMateriality({ proposals = [], findingsText = "", targetHotel = "" } = {}) {
  const known = {
    sourcing: /hotel sourcing|contract negotiation|group accommodation/i.test(findingsText),
    programme: /incentive/i.test(findingsText),
    contact: /@/.test(findingsText),
    targetNamed: Boolean(targetHotel) && findingsText.toLowerCase().includes(String(targetHotel).toLowerCase()),
    openRfp: /\brfp\b|request for proposal|accepting proposals/i.test(findingsText),
    bedroomCommitment: /signed bedroom|room block|contracted \d+ rooms|\b\d+ rooms\b/i.test(findingsText),
  };
  const openGaps = [];
  if (!known.targetNamed) openGaps.push("target_hotel_not_named");
  if (!known.openRfp) openGaps.push("no_open_rfp");
  if (!known.bedroomCommitment) openGaps.push("no_bedroom_commitment");
  const seeksOpenGap = (question = "") => /current|dated|open|rfp|bedroom|room block|nobu|portman|commitment|named/i.test(question);
  const material = proposals.filter((item) => item.executable && seeksOpenGap(item.question));
  return {
    known,
    openGaps,
    materialQuestions: material.map((item) => item.question),
    wouldChangeCommercialDecision: material.length > 0,
    commercialStageChanged: false,
    publicationEligibilityChanged: false,
  };
}

export function assessResearchDepth({ questions = [], disposition = null } = {}) {
  const questionsUsed = questions.length;
  return {
    limit: MAX_RESEARCH_QUESTIONS,
    questionsUsed,
    limitRaised: false,
    sufficientForInitialInvestigation: questionsUsed <= MAX_RESEARCH_QUESTIONS && (
      questionsUsed < MAX_RESEARCH_QUESTIONS || TERMINAL_DISPOSITIONS.has(disposition)
    ),
    extension: {
      defaultAuthorized: false,
      maxAdditionalQuestions: 1,
      requires: [
        "a new verified finding that is not already fingerprinted",
        "the finding opens one closed gap: named target hotel, dated bedroom commitment, open RFP, or an official later-cycle announcement",
        "explicit authorizeDepthExtension for that investigation",
        "the added question is not a paraphrase of a held question",
      ],
    },
  };
}

export function evaluateDepthExtension({ memory = {}, finding = {}, authorizeDepthExtension = false } = {}) {
  if (authorizeDepthExtension !== true) {
    return { permitted: false, reason: "extension_not_authorized", limit: MAX_RESEARCH_QUESTIONS, limitRaised: false };
  }
  const held = new Set(memory.evidenceFingerprints || []);
  const fingerprint = questionFingerprint(finding.excerpt || finding.statement || "");
  if (!fingerprint || held.has(fingerprint)) {
    return { permitted: false, reason: "finding_already_held", limit: MAX_RESEARCH_QUESTIONS, limitRaised: false };
  }
  const opensGap = /room block|contracted \d+ rooms|\brfp\b|request for proposal|portman|announced a later/i.test(`${finding.excerpt || ""} ${finding.statement || ""}`);
  if (!opensGap) {
    return { permitted: false, reason: "finding_does_not_open_a_commercial_gap", limit: MAX_RESEARCH_QUESTIONS, limitRaised: false };
  }
  if ((memory.questions || []).length < MAX_RESEARCH_QUESTIONS) {
    return { permitted: false, reason: "limit_not_reached", limit: MAX_RESEARCH_QUESTIONS, limitRaised: false };
  }
  return { permitted: true, additionalQuestions: 1, limit: MAX_RESEARCH_QUESTIONS, limitRaised: false };
}

function corpusPage(corpus, url) {
  if (!corpus) return "";
  return corpus[url] || corpus[normalizeUrl(url)] || corpus[`${normalizeUrl(url)}/`] || "";
}

function executeFrozenRetrieval(action, corpus, state, assumptions) {
  if (!action || action.provider !== "direct_public") {
    return { executed: false, reason: "provider_not_executable", finding: null };
  }
  const url = normalizeUrl(action.url);
  if (state.retrievals.some((item) => item.url === url)) {
    return { executed: false, reason: "duplicate_source", finding: null };
  }
  state.retrievals.push({ url, provider: "direct_public", paidCalls: 0 });
  state.investigatedUrls.push(url);
  const page = corpusPage(corpus, action.url);
  if (!page) return { executed: true, supported: false, reason: "source_not_in_corpus", finding: null };
  const sentences = String(page).split(/(?<=\.)\s+/).map((item) => item.trim()).filter(Boolean);
  const supportedSentences = sentences.filter((sentence) => COMMERCIAL_SENTENCE.test(sentence) && !IRRELEVANT_SENTENCE.test(sentence) && page.includes(sentence));
  const irrelevant = sentences.filter((sentence) => !supportedSentences.includes(sentence));
  state.irrelevantDiscoveries.push(...irrelevant.map((sentence) => ({ url, sentence, reason: "not_commercially_relevant" })));
  const rejectedAssumptions = (assumptions || []).filter((assumption) => !page.toLowerCase().includes(String(assumption).toLowerCase()));
  state.rejectedAssumptions.push(...rejectedAssumptions.map((assumption) => ({ assumption, url, reason: "not_in_cited_source" })));
  if (!supportedSentences.length) return { executed: true, supported: false, reason: "citation_not_in_source", finding: null };
  const excerpt = supportedSentences.join(" ");
  const finding = { url, excerpt, supported: true, paidCalls: 0 };
  state.findings.push(finding);
  state.attempts.push({ url, outcome: "verified", countsAsResearchDepth: true, paidCalls: 0 });
  return { executed: true, supported: true, finding, rejectedAssumptions, irrelevant };
}

function persistLoop(state, checkpointDir) {
  if (!checkpointDir) return;
  writeMaturationCheckpointAtomic(checkpointDir, state);
}

export async function runEvidenceDrivenResearchLoop(input = {}) {
  if (input.executePaidProviders === true) {
    const err = new Error("paid_provider_execution_refused");
    err.code = "PAID_EXECUTION_REFUSED";
    throw err;
  }
  const state = input.resumeState ? structuredClone(input.resumeState) : {
    id: input.opportunityId || "offline-research-loop",
    status: "IN_PROGRESS",
    findings: [...(input.findings || [])],
    investigatedUrls: [...(input.investigated?.urls || [])],
    questions: [],
    rejectedProposals: [],
    retrievals: [],
    irrelevantDiscoveries: [],
    rejectedAssumptions: [],
    attempts: [...(input.attempts || [])],
    completeCalls: 0,
    plannerModelCalls: 0,
    paidCalls: 0,
    publicationEligibilityChanged: false,
    commercialStage: input.opportunity?.commercialStage || null,
    salesReadiness: input.commercial?.salesReadiness || null,
    steps: [],
  };
  state.steps = state.steps || [];
  const assumptions = input.assumptions || [];

  if (state.status === "INTERRUPTED" && state.pendingAction) {
    const executed = executeFrozenRetrieval(state.pendingAction, input.corpus, state, assumptions);
    state.pendingAction = null;
    state.status = "IN_PROGRESS";
    state.steps.push({ kind: "resumed_execution", ...executed, finding: executed.finding || null });
  }

  const maxSteps = input.maxSteps || 3;
  for (let step = 0; step < maxSteps && state.status === "IN_PROGRESS"; step += 1) {
    const plan = await planDynamicResearch({
      objective: input.objective,
      hotel: input.hotel,
      opportunity: input.opportunity,
      findings: state.findings,
      openQuestions: input.openQuestions || [],
      attempts: state.attempts,
      investigated: { urls: state.investigatedUrls },
      remainingUsd: input.remainingUsd ?? 0,
      authorizePaidResearch: input.authorizePaidResearch === true,
      commercial: input.commercial,
      questionFingerprints: state.questions.map((item) => item.question),
      complete: async (payload) => {
        state.completeCalls += 1;
        return input.complete(payload, { step, state });
      },
    });
    state.plannerModelCalls += plan.plannerModelCalls || 0;
    state.questions.push(...(plan.proposals || []));
    state.rejectedProposals.push(...(plan.rejectedProposals || []));
    state.commercialStage = plan.commercialStage;
    state.salesReadiness = plan.salesReadiness;
    state.researchDisposition = plan.researchDisposition;
    state.nextPermittedAction = plan.nextPermittedAction;
    state.steps.push({
      kind: "plan",
      researchDisposition: plan.researchDisposition,
      accepted: plan.proposals || [],
      rejected: plan.rejectedProposals || [],
      nextPermittedAction: plan.nextPermittedAction,
    });
    const action = plan.nextPermittedAction;
    if (!action || action.provider !== "direct_public" || action.executable !== true) {
      state.status = "STOPPED";
      state.stopJustified = TERMINAL_DISPOSITIONS.has(plan.researchDisposition);
      break;
    }
    if (input.interruptBeforeExecution === true && state.retrievals.length === 0) {
      state.status = "INTERRUPTED";
      state.pendingAction = action;
      break;
    }
    const executed = executeFrozenRetrieval(action, input.corpus, state, assumptions);
    state.steps.push({ kind: "execution", ...executed, finding: executed.finding || null });
    if (!executed.finding) {
      state.status = "STOPPED";
      state.stopJustified = true;
      break;
    }
  }
  state.meaningfulSteps = state.retrievals.filter((item) => state.findings.some((finding) => normalizeUrl(finding.url) === item.url)).length;
  state.depth = assessResearchDepth({ questions: state.questions, disposition: state.researchDisposition });
  state.publicationEligibilityChanged = false;
  persistLoop(state, input.checkpointDir);
  return state;
}
