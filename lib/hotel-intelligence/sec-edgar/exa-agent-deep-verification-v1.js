/**
 * Optional HIF stage: EXA_AGENT_DEEP_VERIFICATION
 * Runs only when a qualifying unresolved research question exists.
 * Agent conclusions are leads until independent verification gates pass.
 */

import { VERIFICATION_GATE } from "./contract-intelligence-identity-layers-v1.js";
import { createExaAgentClient } from "./exa-agent-client-v1.js";
import {
  createExaAgentPilotBudget,
  EXA_AGENT_PILOT_MAX_RETRIES,
} from "./exa-agent-pilot-budget-v1.js";

export const EXA_AGENT_DEEP_VERIFICATION_V1 = "exa-agent-deep-verification-v1";
export const HIF_STAGE_EXA_AGENT_DEEP_VERIFICATION = "EXA_AGENT_DEEP_VERIFICATION";

/**
 * Founder-approved default (2026-11-21): Agent is optional investigative tooling only.
 * Ordinary factory-wide discovery must NOT auto-invoke Agent.
 */
export const EXA_AGENT_ROUTING_MODE = Object.freeze({
  MANUAL_ESCALATION_ONLY: "MANUAL_ESCALATION_ONLY",
  CONDITIONAL_AUTO_ROUTE_AFTER_FOUNDER_APPROVAL: "CONDITIONAL_AUTO_ROUTE_AFTER_FOUNDER_APPROVAL",
  DO_NOT_AUTO_ROUTE: "DO_NOT_AUTO_ROUTE",
});

export const EXA_AGENT_DEFAULT_ROUTING = EXA_AGENT_ROUTING_MODE.MANUAL_ESCALATION_ONLY;

export const AGENT_FINDING_CLASS = Object.freeze({
  VERIFIED_DECISION_WINDOW: "VERIFIED_DECISION_WINDOW",
  VERIFIED_END_DATE_STATUS_UNCONFIRMED: "VERIFIED_END_DATE_STATUS_UNCONFIRMED",
  VERIFIED_COMMERCIAL_SIGNAL: "VERIFIED_COMMERCIAL_SIGNAL",
  RESEARCH_REQUIRED: "RESEARCH_REQUIRED",
  CLOSED_OR_SUPERSEDED: "CLOSED_OR_SUPERSEDED",
});

/**
 * Fail-closed authorization for Agent batches.
 * Requires explicit manual escalation + spend authorization; never auto-routes.
 */
export function assertManualAgentAuthorization(opts = {}) {
  const mode = opts.routingMode || EXA_AGENT_DEFAULT_ROUTING;
  if (mode !== EXA_AGENT_ROUTING_MODE.MANUAL_ESCALATION_ONLY && mode !== EXA_AGENT_DEFAULT_ROUTING) {
    // Only MANUAL_ESCALATION_ONLY is authorized in production factory defaults.
    if (mode !== EXA_AGENT_ROUTING_MODE.DO_NOT_AUTO_ROUTE) {
      return {
        ok: false,
        error: "EXA_AGENT_ROUTING_NOT_AUTHORIZED",
        reason: `Routing mode ${mode} is not approved; default is MANUAL_ESCALATION_ONLY`,
        defaultRouting: EXA_AGENT_DEFAULT_ROUTING,
      };
    }
  }
  if (opts.factoryWideAutoDiscovery === true) {
    return {
      ok: false,
      error: "EXA_AGENT_BLOCKED_ON_FACTORY_WIDE_RUN",
      reason: "Agent must not run during ordinary factory-wide discovery",
      defaultRouting: EXA_AGENT_DEFAULT_ROUTING,
    };
  }
  if (opts.explicitManualEscalation !== true) {
    return {
      ok: false,
      error: "EXA_AGENT_REQUIRES_MANUAL_ESCALATION",
      reason: "Set explicitManualEscalation=true for each authorized Agent batch",
      defaultRouting: EXA_AGENT_DEFAULT_ROUTING,
    };
  }
  if (opts.spendAuthorization !== true) {
    return {
      ok: false,
      error: "EXA_AGENT_REQUIRES_SPEND_AUTHORIZATION",
      reason: "Set spendAuthorization=true with an applicable Agent budget before each batch",
      defaultRouting: EXA_AGENT_DEFAULT_ROUTING,
    };
  }
  return { ok: true, routingMode: EXA_AGENT_DEFAULT_ROUTING };
}

export const AGENT_OUTPUT_SCHEMA = Object.freeze({
  type: "object",
  properties: {
    hotel_or_agreement_identity: { type: "string" },
    research_question: { type: "string" },
    proposed_answer: { type: "string" },
    source_url: { type: "string" },
    source_date: { type: "string" },
    supporting_excerpt: { type: "string" },
    source_attribution: { type: "string" },
    evidence_confidence: {
      type: "string",
      enum: ["HIGH", "MEDIUM", "LOW", "NONE"],
    },
    unresolved_limitations: { type: "string" },
    suggested_follow_up: { type: "string" },
    claimed_end_date: { type: "string" },
    claimed_notice_period_days: { type: "string" },
    claimed_renewal_or_termination_status: { type: "string" },
    claimed_owner: { type: "string" },
    claimed_management_company: { type: "string" },
  },
  required: [
    "hotel_or_agreement_identity",
    "research_question",
    "proposed_answer",
    "source_url",
    "evidence_confidence",
    "unresolved_limitations",
  ],
});

export const AGENT_SYSTEM_PROMPT = [
  "You investigate hotel franchise, management, and brand agreements for commercial deal timing.",
  "Prioritize: verified hotel-specific contract expiration/termination dates; notice periods;",
  "executed renewals/extensions/terminations; current owner and management company.",
  "Cite only publicly available primary sources (SEC EDGAR exhibits, company filings, press with clear facts).",
  "Never invent dates, renewals, or parties. If unknown, say so and set evidence_confidence to NONE or LOW.",
  "Do not treat hotel opening dates, loan maturities, or unrelated portfolio dates as contract expirations.",
  "Return only facts supported by the cited source_url excerpt.",
].join(" ");

export function buildResearchQuestion(caseRow = {}) {
  if (caseRow.researchQuestion) return String(caseRow.researchQuestion).trim();
  const hotel = caseRow.hotel || caseRow.hotelName || "this hotel";
  const cohort = caseRow.cohort || "";
  if (cohort === "A") {
    return `Is there authoritative public evidence that the ${caseRow.agreementType || "hotel"} agreement for ${hotel} was renewed, extended, terminated, superseded, or replaced since the historical filing? State the latest disclosed status and any new end date.`;
  }
  if (cohort === "B") {
    return `What is the latest disclosed expiration or termination date and any advance notice period for the specific ${caseRow.agreementType || "agreement"} covering ${hotel}? Prefer exhibit/amendment language over secondary summaries.`;
  }
  if (cohort === "C") {
    return `Who currently owns and who currently manages ${hotel}, and is there transition evidence relative to the parties named in the historical agreement?`;
  }
  return `What is the latest verified contractual end date, notice period, and current status for the hotel agreement covering ${hotel}?`;
}

export function caseQualifiesForAgent(caseRow = {}) {
  const q = buildResearchQuestion(caseRow);
  if (!q) return { ok: false, reason: "NO_RESEARCH_QUESTION" };
  const hotel = caseRow.hotel || caseRow.hotelName;
  if (!hotel) return { ok: false, reason: "NO_HOTEL_IDENTITY" };
  // Skip if Search baseline already fully resolved current status + terms
  if (
    caseRow.searchBaseline?.currentStatusVerified &&
    caseRow.searchBaseline?.contractTermsVerified &&
    !caseRow.forceAgent
  ) {
    return { ok: false, reason: "SEARCH_ALREADY_RESOLVED" };
  }
  return { ok: true, researchQuestion: q };
}

/**
 * Independent verification — Agent alone cannot satisfy gates.
 * Lightweight deterministic checks against structured Agent output + baseline facts.
 */
export function independentlyVerifyAgentFinding(caseRow, agentResult) {
  const structured = agentResult?.structured || {};
  const url = String(structured.source_url || "").trim();
  const excerpt = String(structured.supporting_excerpt || "").trim();
  const conf = String(structured.evidence_confidence || "NONE").toUpperCase();
  const answer = String(structured.proposed_answer || "").trim();
  const hotel = String(caseRow.hotel || caseRow.hotelName || "").toLowerCase();
  const identity = String(structured.hotel_or_agreement_identity || "").toLowerCase();

  const gates = {
    [VERIFICATION_GATE.AGREEMENT_EXISTENCE_VERIFIED]: false,
    [VERIFICATION_GATE.CONTRACT_TERMS_VERIFIED]: false,
    [VERIFICATION_GATE.CURRENT_STATUS_VERIFIED]: false,
  };

  const issues = [];
  let attributionOk = true;

  if (!url || !/^https?:\/\//i.test(url)) {
    issues.push("MISSING_OR_INVALID_SOURCE_URL");
  }
  if (!excerpt || excerpt.length < 20) {
    issues.push("MISSING_SUPPORTING_EXCERPT");
  }
  if (conf === "NONE" || !answer || /insufficient|unknown|not found|unable/i.test(answer)) {
    issues.push("UNGROUNDED_OR_EMPTY_ANSWER");
  }
  if (hotel && identity && !identity.includes(hotel.split(/\s+/)[0]) && !hotel.includes(identity.split(/\s+/)[0])) {
    // soft check — flag weak attribution
    if (!new RegExp(hotel.split(",")[0].replace(/[.*+?^${}()|[\]\\]/g, "\\$&").slice(0, 24), "i").test(identity + " " + answer + " " + excerpt)) {
      attributionOk = false;
      issues.push("HOTEL_ATTRIBUTION_WEAK");
    }
  }

  // Reject opening/loan false positives in claimed dates
  const claimedEnd = String(structured.claimed_end_date || "").trim();
  if (/open(ing)?\s+date|loan\s+matur|maturity\s+date/i.test(answer + " " + excerpt)) {
    issues.push("DATE_SEMANTIC_RISK_OPENING_OR_LOAN");
  }

  const hasUrlAndExcerpt = url && excerpt.length >= 20 && !issues.includes("UNGROUNDED_OR_EMPTY_ANSWER");
  // Carry forward Search/factory baseline only — Agent alone cannot satisfy any gate
  gates[VERIFICATION_GATE.AGREEMENT_EXISTENCE_VERIFIED] = Boolean(
    caseRow.searchBaseline?.agreementExistenceVerified,
  );
  gates[VERIFICATION_GATE.CONTRACT_TERMS_VERIFIED] = Boolean(caseRow.searchBaseline?.contractTermsVerified);
  gates[VERIFICATION_GATE.CURRENT_STATUS_VERIFIED] = Boolean(caseRow.searchBaseline?.currentStatusVerified);

  const noticeDays = structured.claimed_notice_period_days;

  const incremental = {
    newEndDate: null,
    newNoticeDays: null,
    statusResolution: null,
    renewalConfirmed: false,
    ownerOrOperator: null,
    falseOrUnsupported: issues.length > 0 && !hasUrlAndExcerpt,
    restatesBaselineOnly: false,
  };

  const baselineEnd = caseRow.searchBaseline?.contractEndDate || caseRow.verifiedEndDate || null;
  const baselineNotice = caseRow.searchBaseline?.noticePeriodDays ?? caseRow.noticePeriodDays ?? null;
  const isoEnd = (claimedEnd.match(/^(\d{4}-\d{2}-\d{2})/) || [])[1] || null;
  const claimedEndRestatesBaseline =
    Boolean(isoEnd) &&
    Boolean(baselineEnd) &&
    isoEnd === String(baselineEnd).slice(0, 10) &&
    /historical|no later|no newer|supplied by|requester|caller|already known|no new end date located/i.test(
      claimedEnd,
    );

  if (claimedEndRestatesBaseline) {
    incremental.restatesBaselineOnly = true;
  }

  // Only credit a NEW end date when ISO-clean, different from baseline, and not a baseline restatement
  if (
    isoEnd &&
    isoEnd !== String(baselineEnd || "").slice(0, 10) &&
    !claimedEndRestatesBaseline &&
    !issues.includes("DATE_SEMANTIC_RISK_OPENING_OR_LOAN") &&
    hasUrlAndExcerpt
  ) {
    incremental.newEndDate = isoEnd; // candidate — not gate-verified
  }

  if (noticeDays != null && String(noticeDays).match(/^\d+$/) && Number(noticeDays) !== Number(baselineNotice)) {
    if (hasUrlAndExcerpt && Number(noticeDays) > 0) incremental.newNoticeDays = Number(noticeDays);
  }

  const ren = String(structured.claimed_renewal_or_termination_status || "").toLowerCase();
  const positiveStatusClaim = /renewed|extended|terminated|superseded|replaced|conversion/.test(ren);
  const answerDeniesAllStatus =
    /no authoritative public evidence (?:was )?located that .{0,80}(?:renewed|extended|terminated)/i.test(
      answer,
    ) ||
    (/^no authoritative|^no evidence|^unable|^unknown/i.test(answer.trim()) && !positiveStatusClaim);

  if (positiveStatusClaim && hasUrlAndExcerpt && !answerDeniesAllStatus) {
    incremental.statusResolution = ren;
    if (/renewed|extended/.test(ren) && !/no renewal|not renewed|no extension/i.test(ren + " " + answer)) {
      incremental.renewalConfirmed = true; // candidate claim only
    }
  }

  const owner = String(structured.claimed_owner || "").trim();
  const mgmt = String(structured.claimed_management_company || "").trim();
  const weakParty =
    !owner && !mgmt
      ? true
      : /unknown|not (?:yet )?established|not disclosed|n\/a|none|not (?:publicly )?identified|insufficient|not independently confirmed|historically;|should be confirmed/i.test(
          `${owner} ${mgmt}`,
        );
  const baselineOwner = String(caseRow.searchBaseline?.owner || caseRow.owner || "").trim();
  const baselineMgmt = String(
    caseRow.searchBaseline?.managementCompany || caseRow.managementCompany || "",
  ).trim();
  const ownerNew = owner && owner.toLowerCase() !== baselineOwner.toLowerCase();
  const mgmtNew = mgmt && mgmt.toLowerCase() !== baselineMgmt.toLowerCase();
  if (
    (ownerNew || mgmtNew) &&
    !weakParty &&
    hasUrlAndExcerpt &&
    (conf === "HIGH" || conf === "MEDIUM") &&
    !answerDeniesAllStatus
  ) {
    incremental.ownerOrOperator = { owner: owner || null, managementCompany: mgmt || null };
  }

  if (
    !incremental.newEndDate &&
    !incremental.newNoticeDays &&
    !incremental.statusResolution &&
    !incremental.ownerOrOperator
  ) {
    issues.push("NO_INCREMENTAL_EVIDENCE_IN_ANSWER");
  }

  return {
    gates,
    issues,
    attributionOk,
    agentAloneSatisfiesAnyGate: false,
    incremental,
    structured,
    classificationHint: classifyFromIncremental(caseRow, incremental, issues, answer),
  };
}

function classifyFromIncremental(caseRow, incremental, issues, answer = "") {
  if (
    issues.includes("UNGROUNDED_OR_EMPTY_ANSWER") ||
    issues.includes("MISSING_OR_INVALID_SOURCE_URL") ||
    issues.includes("NO_INCREMENTAL_EVIDENCE_IN_ANSWER")
  ) {
    return AGENT_FINDING_CLASS.RESEARCH_REQUIRED;
  }
  if (/terminated|superseded|replaced|closed|conversion/.test(String(incremental.statusResolution || ""))) {
    return AGENT_FINDING_CLASS.CLOSED_OR_SUPERSEDED;
  }
  if (incremental.newEndDate && incremental.newNoticeDays) {
    return AGENT_FINDING_CLASS.VERIFIED_END_DATE_STATUS_UNCONFIRMED; // still needs CURRENT_STATUS
  }
  if (incremental.newEndDate || incremental.statusResolution) {
    return AGENT_FINDING_CLASS.VERIFIED_END_DATE_STATUS_UNCONFIRMED;
  }
  if (incremental.ownerOrOperator || incremental.newNoticeDays) {
    return AGENT_FINDING_CLASS.VERIFIED_COMMERCIAL_SIGNAL;
  }
  if (/no authoritative|no evidence|not located/i.test(answer)) {
    return AGENT_FINDING_CLASS.RESEARCH_REQUIRED;
  }
  return AGENT_FINDING_CLASS.RESEARCH_REQUIRED;
}

export async function runExaAgentDeepVerification(caseRow, opts = {}) {
  const auth = assertManualAgentAuthorization(opts);
  if (!auth.ok) {
    return {
      ok: false,
      skipped: true,
      reason: auth.error,
      authDetail: auth.reason,
      caseId: caseRow.caseId || caseRow.researchId || null,
      stage: HIF_STAGE_EXA_AGENT_DEEP_VERIFICATION,
      routingMode: EXA_AGENT_DEFAULT_ROUTING,
    };
  }

  const qualify = caseQualifiesForAgent(caseRow);
  if (!qualify.ok) {
    return {
      ok: false,
      skipped: true,
      reason: qualify.reason,
      caseId: caseRow.caseId,
      stage: HIF_STAGE_EXA_AGENT_DEEP_VERIFICATION,
    };
  }

  const budget = opts.budget || createExaAgentPilotBudget({ cwd: opts.cwd });
  const client = opts.client || createExaAgentClient({ budget, env: opts.env, skipCache: opts.skipCache });
  const effort = opts.effort || caseRow.effort || "low";
  const caseId = caseRow.caseId || caseRow.researchId || "unknown";
  const maxRetries = opts.maxRetries ?? EXA_AGENT_PILOT_MAX_RETRIES;

  const reserve = client.effortCostUsd[effort] ?? 0.025;
  const gate = budget.canSpend(reserve, caseId);
  if (!gate.ok) {
    return {
      ok: false,
      skipped: true,
      reason: gate.error,
      caseId,
      budget: budget.snapshot(),
      stage: HIF_STAGE_EXA_AGENT_DEEP_VERIFICATION,
    };
  }

  const query = [
    `Hotel: ${caseRow.hotel || caseRow.hotelName}`,
    caseRow.location ? `Location: ${caseRow.location}` : null,
    `Agreement type: ${caseRow.agreementType || "UNKNOWN"}`,
    caseRow.brand ? `Brand: ${caseRow.brand}` : null,
    caseRow.evidenceUrl || caseRow.sourceUrl
      ? `Prior evidence URL: ${caseRow.evidenceUrl || caseRow.sourceUrl}`
      : null,
    caseRow.verifiedEndDate ? `Known historical end date: ${caseRow.verifiedEndDate}` : null,
    caseRow.noticePeriodDays != null ? `Known notice days: ${caseRow.noticePeriodDays}` : null,
    `Research question: ${qualify.researchQuestion}`,
  ]
    .filter(Boolean)
    .join("\n");

  let last = null;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    last = await client.research({
      query,
      systemPrompt: AGENT_SYSTEM_PROMPT,
      effort,
      outputSchema: AGENT_OUTPUT_SCHEMA,
      caseId,
    });
    if (last.ok) break;
    if (
      [
        "EXA_AGENT_PILOT_CEILING",
        "AGENT_BUDGET_STOPPED",
        "EXA_AGENT_BUDGET_EXCEEDED",
        "EXA_AGENT_AUTH_FAILED",
        "EXA_AGENT_PER_CASE_CEILING",
      ].includes(last.error)
    ) {
      break;
    }
  }

  if (!last?.ok) {
    return {
      ok: false,
      caseId,
      researchQuestion: qualify.researchQuestion,
      error: last?.error || "AGENT_FAILED",
      runId: last?.runId || null,
      spentUsd: last?.spentUsd || 0,
      stage: HIF_STAGE_EXA_AGENT_DEEP_VERIFICATION,
      budget: budget.snapshot(),
    };
  }

  const verification = independentlyVerifyAgentFinding(caseRow, last);

  return {
    ok: true,
    caseId,
    researchQuestion: qualify.researchQuestion,
    runId: last.runId,
    effort,
    spentUsd: last.spentUsd || 0,
    structured: last.structured,
    text: last.text,
    citations: last.citations,
    verification,
    stage: HIF_STAGE_EXA_AGENT_DEEP_VERIFICATION,
    budget: budget.snapshot(),
  };
}

export default {
  runExaAgentDeepVerification,
  independentlyVerifyAgentFinding,
  caseQualifiesForAgent,
  buildResearchQuestion,
  assertManualAgentAuthorization,
  AGENT_OUTPUT_SCHEMA,
  AGENT_FINDING_CLASS,
  EXA_AGENT_ROUTING_MODE,
  EXA_AGENT_DEFAULT_ROUTING,
  HIF_STAGE_EXA_AGENT_DEEP_VERIFICATION,
};
