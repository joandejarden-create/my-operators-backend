/**
 * Isolated cross-organization follow-up: Principal ↔ The Savoy independent verification.
 * Does not spend prior/longform ledgers. Max 3 questions, 3 retrievals, $0.05.
 */

import fs from "node:fs";
import { createHash, randomBytes } from "node:crypto";
import {
  planDynamicResearch,
  quotePlannerUsd,
  PLANNER_CEILING_USD,
  RESEARCH_DISPOSITION,
} from "./dynamic-research-planner-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import {
  classifyTechnicalSource,
  extractCandidateUrlsFromHtml,
  resolvePublishingOrganization,
  isIndependentPublisherUrl,
  assessRelationshipVerification,
  RELATIONSHIP_VERIFICATION,
  ENTITY_RELATIONSHIP,
  STOPPING_TRIGGER,
  buildStopRecord,
  COMMERCIAL_RESEARCH_DIMENSION,
} from "./commercial-source-selection-v1.js";
import { writeMaturationCheckpointAtomic, maturationMemoryPath } from "./bethesda-maturation-checkpoint-v1.js";
import { buildCrossOrganizationFollowUpAssignment } from "./longform-research-certification-v1.js";
import {
  RETRIEVAL_OUTCOME,
  DEFAULT_FAILED_TECHNICAL_ATTEMPT_CAP,
  recordSourceAttempt,
  summarizeAttemptBudget,
  proposeAdaptiveRecovery,
  buildCrossOrgInvestigationReport,
  runOfflineSavoyRetrievalRecoveryCertification,
  classifyRetrievalOutcome,
} from "./cross-organization-retrieval-recovery-v1.js";

export const SAVOY_FOLLOWUP_VERSION = "gdi-cross-organization-followup-savoy-v1";
export const SAVOY_BUDGET_USD = 0.05;
export const SAVOY_CALL_CEILING_USD = 0.01;
export const SAVOY_MAX_PLANNER_CALLS = 3;
/** Successful substantive retrievals with readable independent content. */
export const SAVOY_MAX_RETRIEVALS = 3;
export const SAVOY_MAX_SUBSTANTIVE_RETRIEVALS = SAVOY_MAX_RETRIEVALS;
/** Failed technical attempts are bounded separately and do not exhaust substantive allowance. */
export const SAVOY_MAX_FAILED_TECHNICAL_ATTEMPTS = DEFAULT_FAILED_TECHNICAL_ATTEMPT_CAP;
export const SAVOY_MAX_SUBSTANTIVE_QUESTIONS = 3;
export const SAVOY_LEDGER_RELATIVE = "reports/gdi-cross-organization-followup-savoy-v1/budget-ledger.json";
export const ORGANIZER_HOST = "principallondon.com";
export const ORGANIZER_NAME = "Principal. London DMC";
export const RELATED_ENTITY = "The Savoy";

export {
  RETRIEVAL_OUTCOME,
  recordSourceAttempt,
  proposeAdaptiveRecovery,
  buildCrossOrgInvestigationReport,
  runOfflineSavoyRetrievalRecoveryCertification,
  classifyRetrievalOutcome,
};

const FORBIDDEN_LEDGER = /parallel-live-comparison|cohort-a-live|segment-e2e|paid-multisegment|parallel-budget|gdi-planner-evaluation|gdi-autonomous-research-certification|gdi-autonomous-deep-research|gdi-autonomous-longform-research-certification/i;

const SEED_INDEPENDENT_CANDIDATES = Object.freeze([
  "https://www.thesavoy.co.uk/",
  "https://www.thesavoy.co.uk/meetings-and-events",
  "https://www.fairmont.com/savoy-london/",
  "https://www.fairmont.com/en/hotels/london/fairmont-savoy.html",
  "https://www.thesavoy.co.uk/press-and-media",
  "https://www.hospitalitynet.org/",
  "https://www.meetingpack.com/",
]);

function normalizeUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "").toLowerCase();
}

function pathDir(filePath) {
  const parts = String(filePath || "").replace(/\\/g, "/").split("/");
  parts.pop();
  return parts.join("/") || ".";
}

function writeJsonAtomic(filePath, data) {
  fs.mkdirSync(pathDir(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`);
  fs.rmSync(filePath, { force: true });
  fs.renameSync(tmp, filePath);
}

export function assertSavoyFollowupLedger(ledgerPath) {
  const normalized = String(ledgerPath || "").replace(/\\/g, "/");
  if (FORBIDDEN_LEDGER.test(normalized)) {
    const err = new Error("savoy_followup_ledger_not_isolated");
    err.code = "SAVOY_FOLLOWUP_LEDGER_NOT_ISOLATED";
    throw err;
  }
  if (!normalized.endsWith(SAVOY_LEDGER_RELATIVE)) {
    const err = new Error("savoy_followup_ledger_path_refused");
    err.code = "SAVOY_FOLLOWUP_LEDGER_PATH_REFUSED";
    throw err;
  }
}

function emptyLedger() {
  return {
    version: `${SAVOY_FOLLOWUP_VERSION}-ledger`,
    currency: "USD",
    budgetUsd: SAVOY_BUDGET_USD,
    isolatedFromAllPriorResearchLedgers: true,
    usesLongformLedgerRemainingBalance: false,
    reservedUsd: 0,
    estimatedSpentUsd: 0,
    plannerCalls: 0,
    retrievals: 0,
    substantiveQuestions: 0,
    entries: [],
    updatedAt: new Date().toISOString(),
  };
}

export function loadOrCreateSavoyFollowupLedger(ledgerPath) {
  assertSavoyFollowupLedger(ledgerPath);
  if (!fs.existsSync(ledgerPath)) {
    const ledger = emptyLedger();
    writeJsonAtomic(ledgerPath, ledger);
    return ledger;
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  if (Number(ledger.budgetUsd) !== SAVOY_BUDGET_USD) {
    const err = new Error("savoy_followup_budget_refused");
    err.code = "SAVOY_FOLLOWUP_BUDGET_REFUSED";
    throw err;
  }
  return ledger;
}

export function reserveSavoyCall({ ledgerPath, jobKey, quotedUsd, kind = "planner" } = {}) {
  const ledger = loadOrCreateSavoyFollowupLedger(ledgerPath);
  const quoted = Number(quotedUsd);
  if (!(quoted >= 0) || (quoted > 0 && quoted - SAVOY_CALL_CEILING_USD > 1e-9)) {
    const err = new Error("savoy_quote_above_call_ceiling");
    err.code = "SAVOY_QUOTE_ABOVE_CALL_CEILING";
    throw err;
  }
  const existing = (ledger.entries || []).find((entry) => entry.jobKey === jobKey);
  if (existing && (existing.status === "spent" || existing.status === "failed")) {
    return { ok: false, reason: "job_already_used", entry: existing };
  }
  if (existing && existing.status === "reserved") {
    return { ok: true, reservationId: existing.reservationId, alreadyReserved: true };
  }
  if (kind === "planner" && Number(ledger.plannerCalls || 0) >= SAVOY_MAX_PLANNER_CALLS) {
    const err = new Error("savoy_planner_cap");
    err.code = "SAVOY_PLANNER_CAP";
    throw err;
  }
  if (kind === "retrieval" && Number(ledger.retrievals || 0) >= SAVOY_MAX_RETRIEVALS) {
    const err = new Error("savoy_retrieval_cap");
    err.code = "SAVOY_RETRIEVAL_CAP";
    throw err;
  }
  const committed = Number(ledger.estimatedSpentUsd || 0) + Number(ledger.reservedUsd || 0);
  if (quoted > 0 && committed + quoted - SAVOY_BUDGET_USD > 1e-9) {
    const err = new Error("savoy_budget_exhausted");
    err.code = "SAVOY_BUDGET_EXHAUSTED";
    throw err;
  }
  const reservationId = `rsv_sv_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
  if (quoted > 0) {
    ledger.reservedUsd = Number((Number(ledger.reservedUsd || 0) + quoted).toFixed(6));
    ledger.entries.push({
      reservationId,
      jobKey,
      kind,
      quotedUsd: quoted,
      actualUsd: null,
      status: "reserved",
      reservedAt: new Date().toISOString(),
    });
  } else {
    ledger.entries.push({
      reservationId,
      jobKey,
      kind,
      quotedUsd: 0,
      actualUsd: 0,
      status: "spent",
      spentAt: new Date().toISOString(),
    });
    if (kind === "retrieval") ledger.retrievals = Number(ledger.retrievals || 0) + 1;
    if (kind === "planner") ledger.plannerCalls = Number(ledger.plannerCalls || 0) + 1;
  }
  writeJsonAtomic(ledgerPath, ledger);
  return { ok: true, reservationId };
}

export function commitSavoyCall({ ledgerPath, reservationId, actualUsd, usage } = {}) {
  const ledger = loadOrCreateSavoyFollowupLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry) {
    const err = new Error("savoy_reservation_missing");
    err.code = "SAVOY_RESERVATION_MISSING";
    throw err;
  }
  if (entry.status === "spent") return { ok: true, idempotent: true };
  const actual = Number(actualUsd);
  const nextSpent = Number((Number(ledger.estimatedSpentUsd || 0) + actual).toFixed(6));
  if (!(actual >= 0) || actual - Number(entry.quotedUsd) > 1e-9 || nextSpent - SAVOY_BUDGET_USD > 1e-9) {
    const err = new Error("savoy_actual_above_reservation");
    err.code = "SAVOY_ACTUAL_ABOVE_RESERVATION";
    throw err;
  }
  entry.status = "spent";
  entry.actualUsd = actual;
  entry.usage = usage || null;
  entry.spentAt = new Date().toISOString();
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd)).toFixed(6));
  ledger.estimatedSpentUsd = nextSpent;
  if (entry.kind === "planner") ledger.plannerCalls = Number(ledger.plannerCalls || 0) + 1;
  if (entry.kind === "retrieval") ledger.retrievals = Number(ledger.retrievals || 0) + 1;
  writeJsonAtomic(ledgerPath, ledger);
  return { ok: true };
}

export function releaseSavoyCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateSavoyFollowupLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true };
  entry.status = "released";
  entry.releasedAt = new Date().toISOString();
  entry.releaseReason = reason || "not_billed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeJsonAtomic(ledgerPath, ledger);
  return { ok: true };
}

export function failSavoyCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateSavoyFollowupLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true };
  entry.status = "failed";
  entry.failedAt = new Date().toISOString();
  entry.failureReason = reason || "call_failed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeJsonAtomic(ledgerPath, ledger);
  return { ok: true };
}

export function buildSavoyFollowupAuthorizationManifest() {
  const assignment = buildCrossOrganizationFollowUpAssignment({ hardCeilingUsd: SAVOY_BUDGET_USD });
  const planner = quotePlannerUsd();
  return {
    version: SAVOY_FOLLOWUP_VERSION,
    status: "AWAITING_AUTHORIZATION",
    assignment,
    workflow: {
      orchestrator: "opportunity-research-supervisor-v1 (Node.js Research Supervisor sole orchestrator)",
      maxSubstantiveQuestions: SAVOY_MAX_SUBSTANTIVE_QUESTIONS,
      maxPlannerCalls: SAVOY_MAX_PLANNER_CALLS,
      maxRetrievals: SAVOY_MAX_RETRIEVALS,
      independentPublisherOnly: true,
      organizerHostForbidden: ORGANIZER_HOST,
    },
    operations: [
      {
        step: "planner_loop",
        operation: "planner_call",
        model: planner.model,
        maxCalls: SAVOY_MAX_PLANNER_CALLS,
        maxQuotedUsdEach: planner.estimatedUsd,
        hardCeilingUsdEach: PLANNER_CEILING_USD,
      },
      {
        step: "retrieval_loop",
        operation: "independent_publisher_retrieval",
        provider: "direct_public_preferred_firecrawl_fallback_if_priced",
        maxRetrievals: SAVOY_MAX_RETRIEVALS,
      },
    ],
    hardCeilingUsd: SAVOY_BUDGET_USD,
    perCallCeilingUsd: SAVOY_CALL_CEILING_USD,
    ledger: SAVOY_LEDGER_RELATIVE,
    usesPriorBenchmarkOrCertificationLedgers: false,
    usesLongformLedgerRemainingBalance: false,
    productionAllowlistsUnchanged: true,
    publicationEligibilityChanged: false,
    airtableWrites: 0,
    deploy: false,
    liveExecutionAuthorized: false,
  };
}

function pageMentionsPrincipal(text = "") {
  return /\bprincipal(?:\.?\s*london|\s+dmc|\s+ge)?\b/i.test(String(text || ""));
}

function pageMentionsSavoy(text = "") {
  return /\b(?:the\s+)?savoy\b/i.test(String(text || ""));
}

function pageCorroboratesPrincipalSavoyRelationship(text = "") {
  const body = String(text || "");
  if (!pageMentionsPrincipal(body) || !pageMentionsSavoy(body)) return false;
  // Require co-occurrence with a programme/commercial cue, not mere unrelated mentions far apart.
  const window = body.replace(/\s+/g, " ");
  return /principal[^.]{0,240}savoy|savoy[^.]{0,240}principal/i.test(window)
    || /principal[^.]{0,120}(?:evening|event|incentive|programme|program|partner|hosted|reception)[^.]{0,120}savoy/i.test(window)
    || /savoy[^.]{0,120}(?:evening|event|incentive|programme|program|partner|hosted|reception)[^.]{0,120}principal/i.test(window);
}

function rankIndependentCandidate(url, question = "") {
  const host = resolvePublishingOrganization({ url, organizationHost: ORGANIZER_HOST }).sourceOwnerHost;
  let score = 0;
  const q = String(question || "").toLowerCase();
  if (/thesavoy\.co\.uk/i.test(host)) score += 20;
  if (/fairmont\.com/i.test(host)) score += 16;
  if (/press|media|news|event|meeting/i.test(url)) score += 6;
  if (/hospitalitynet|meetingpack|conference|incentive/i.test(host)) score += 8;
  if (/savoy/.test(q) && /savoy/i.test(url + host)) score += 4;
  if (/principal/.test(q) && /principal/i.test(url)) score += 10;
  if (classifyTechnicalSource(url).rejected) score -= 100;
  if (!isIndependentPublisherUrl(url, ORGANIZER_HOST)) score -= 100;
  return score;
}

function selectNextIndependentSource({ candidates, investigatedUrls, question }) {
  const held = new Set((investigatedUrls || []).map(normalizeUrl));
  const ranked = [...new Set((candidates || []).map(String).filter(Boolean))]
    .filter((url) => !held.has(normalizeUrl(url)))
    .filter((url) => isIndependentPublisherUrl(url, ORGANIZER_HOST))
    .filter((url) => !classifyTechnicalSource(url).rejected)
    .map((url) => ({ url, score: rankIndependentCandidate(url, question) }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score || a.url.localeCompare(b.url));
  return {
    selected: ranked[0] || null,
    ranking: ranked.slice(0, 8),
    rejectedOrganizer: (candidates || []).filter((url) => !isIndependentPublisherUrl(url, ORGANIZER_HOST)),
  };
}

async function billedPlannerCall({ ledgerPath, jobKey, planInput }) {
  const reservation = reserveSavoyCall({
    ledgerPath,
    jobKey,
    quotedUsd: SAVOY_CALL_CEILING_USD,
    kind: "planner",
  });
  if (!reservation.ok) return { skipped: true, reason: reservation.reason, entry: reservation.entry };
  let plan;
  try {
    plan = await planDynamicResearch({ ...planInput, authorizePlannerModel: true });
  } catch (err) {
    failSavoyCall({ ledgerPath, reservationId: reservation.reservationId, reason: err.code || "planner_threw" });
    throw err;
  }
  if (plan.plannerModelCalls === 1 && plan.plannerActualUsd != null) {
    commitSavoyCall({
      ledgerPath,
      reservationId: reservation.reservationId,
      actualUsd: plan.plannerActualUsd,
      usage: plan.plannerUsage,
    });
  } else if (plan.plannerModelCalls === 1) {
    failSavoyCall({ ledgerPath, reservationId: reservation.reservationId, reason: "usage_missing" });
  } else {
    releaseSavoyCall({ ledgerPath, reservationId: reservation.reservationId, reason: plan.fallbackReason || "model_not_called" });
  }
  return { plan };
}

async function fetchDirectOrFirecrawl({ url, fetchPage, ledgerPath, jobKey, preferredMethod = null }) {
  if (!isIndependentPublisherUrl(url, ORGANIZER_HOST)) {
    return {
      ok: false,
      refused: true,
      reason: "organizer_publisher_forbidden",
      text: "",
      html: "",
      actualUsd: 0,
      provider: null,
      providerError: "organizer_publisher_forbidden",
    };
  }

  const tryDirect = preferredMethod !== "firecrawl";
  let direct = { ok: false, text: "", html: "" };
  if (tryDirect) {
    direct = await fetchPage(url);
    const body = String(direct?.text || "");
    if (direct?.ok && body.trim().length >= 200) {
      // Only reserve a spent retrieval slot when content is actually readable.
      reserveSavoyCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
      return {
        ok: true,
        text: body.slice(0, 25000),
        html: String(direct.html || ""),
        provider: "direct_public",
        method: "direct_public",
        actualUsd: 0,
      };
    }
    if (preferredMethod === "direct_public") {
      return {
        ok: false,
        text: body.slice(0, 25000),
        html: String(direct?.html || ""),
        provider: "direct_public",
        method: "direct_public",
        actualUsd: 0,
        thin: true,
        providerError: body.trim() ? "thin_or_unreadable_direct_public" : "empty_direct_public",
      };
    }
  }

  const quote = quoteBenchmarkCall({
    provider: "firecrawl",
    operation: "scrape",
    params: { url, formats: ["markdown"] },
  });
  const key = process.env.FIRECRAWL_API_KEY || process.env.FIRECRAWL_KEY;
  if (!quote.bounded || !key) {
    const body = String(direct?.text || "");
    return {
      ok: false,
      text: body.slice(0, 25000),
      html: String(direct?.html || ""),
      provider: "direct_public",
      method: "direct_public",
      firecrawlSkipped: !quote.bounded ? "unpriced" : "credential_absent",
      actualUsd: 0,
      thin: true,
      providerError: !quote.bounded ? "firecrawl_unpriced" : "firecrawl_credential_absent",
    };
  }
  const reservation = reserveSavoyCall({
    ledgerPath,
    jobKey: `${jobKey}-firecrawl`,
    quotedUsd: Number(quote.maxUsd),
    kind: "retrieval",
  });
  if (!reservation.ok) {
    return {
      ok: false,
      text: String(direct?.text || "").slice(0, 25000),
      provider: "direct_public",
      method: "direct_public",
      actualUsd: 0,
      providerError: reservation.reason || "firecrawl_reservation_refused",
      thin: true,
    };
  }
  try {
    const res = await fetch("https://api.firecrawl.dev/v2/scrape", {
      method: "POST",
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ url, formats: ["markdown"] }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      failSavoyCall({ ledgerPath, reservationId: reservation.reservationId, reason: `firecrawl_http_${res.status}` });
      return {
        ok: false,
        text: "",
        provider: "firecrawl",
        method: "firecrawl",
        actualUsd: 0,
        httpStatus: res.status,
        providerError: `firecrawl_http_${res.status}`,
      };
    }
    const md = String(json?.data?.markdown || json?.markdown || "");
    if (!md.trim()) {
      failSavoyCall({ ledgerPath, reservationId: reservation.reservationId, reason: "firecrawl_empty_body" });
      return {
        ok: false,
        text: "",
        provider: "firecrawl",
        method: "firecrawl",
        actualUsd: 0,
        providerError: "firecrawl_empty_body",
        thin: true,
      };
    }
    commitSavoyCall({ ledgerPath, reservationId: reservation.reservationId, actualUsd: Number(quote.maxUsd) });
    return {
      ok: true,
      text: md.slice(0, 25000),
      html: "",
      provider: "firecrawl",
      method: "firecrawl",
      actualUsd: Number(quote.maxUsd),
    };
  } catch (err) {
    failSavoyCall({ ledgerPath, reservationId: reservation.reservationId, reason: err.message || "firecrawl_failed" });
    return {
      ok: false,
      text: "",
      provider: "firecrawl",
      method: "firecrawl",
      actualUsd: 0,
      providerError: err.message || "firecrawl_failed",
    };
  }
}

/**
 * Evidence evaluation runs only after a successful readable retrieval.
 * Empty/failed responses must not create rejected factual claims.
 */
function evaluateRetrievedPage({ url, text, html, mayEvaluateEvidence = true }) {
  const publisher = resolvePublishingOrganization({
    url,
    organizationHost: ORGANIZER_HOST,
    organizationName: ORGANIZER_NAME,
  });
  if (!mayEvaluateEvidence) {
    return {
      publisher,
      corroborates: false,
      accepted: [],
      rejected: [],
      discoveredIndependentLinks: [],
      substantive: false,
      evidenceEvaluationSkipped: true,
    };
  }

  const accepted = [];
  const rejected = [];
  const corroborates = pageCorroboratesPrincipalSavoyRelationship(text);
  const mentionsSavoyOnly = pageMentionsSavoy(text) && !pageMentionsPrincipal(text);
  const mentionsPrincipalOnly = pageMentionsPrincipal(text) && !pageMentionsSavoy(text);

  if (corroborates) {
    const match = String(text).replace(/\s+/g, " ").match(/.{0,80}(?:principal.{0,160}savoy|savoy.{0,160}principal).{0,80}/i);
    accepted.push({
      evidenceId: `ev_${createHash("sha256").update(`${url}:corroboration`).digest("hex").slice(0, 12)}`,
      url,
      statement: (match?.[0] || "Independent page co-names Principal and The Savoy in a programme/commercial context.").replace(/\s+/g, " ").trim().slice(0, 400),
      topic: "independent_relationship_corroboration",
      accepted: true,
      publishingOrganization: publisher.publishingOrganization,
      sourceOwnerHost: publisher.sourceOwnerHost,
      independentOfOrganizer: true,
    });
  } else if (mentionsSavoyOnly) {
    rejected.push({
      claim: "Page independently corroborates Principal–Savoy relationship",
      reason: "savoy_described_without_principal",
      url,
      publishingOrganization: publisher.publishingOrganization,
    });
    accepted.push({
      evidenceId: `ev_${createHash("sha256").update(`${url}:savoy_only`).digest("hex").slice(0, 12)}`,
      url,
      statement: "Independent page describes The Savoy but does not name Principal; Savoy self-description is not Principal relationship confirmation.",
      topic: "negative_control",
      accepted: true,
      publishingOrganization: publisher.publishingOrganization,
      sourceOwnerHost: publisher.sourceOwnerHost,
      independentOfOrganizer: true,
    });
  } else if (mentionsPrincipalOnly) {
    rejected.push({
      claim: "Page independently corroborates Principal–Savoy relationship",
      reason: "principal_without_savoy_relationship",
      url,
      publishingOrganization: publisher.publishingOrganization,
    });
  } else {
    rejected.push({
      claim: "Page independently corroborates Principal–Savoy relationship",
      reason: "relationship_not_found",
      url,
      publishingOrganization: publisher.publishingOrganization,
    });
  }

  rejected.push({
    claim: "Page establishes open Savoy accommodation demand for Nobu",
    reason: "out_of_scope_nobu_demand_not_asserted",
    url,
  });
  rejected.push({
    claim: "Page establishes a Nobu–Principal relationship",
    reason: "nobu_relationship_not_in_scope",
    url,
  });

  const links = extractCandidateUrlsFromHtml(html || text, url)
    .filter((link) => isIndependentPublisherUrl(link, ORGANIZER_HOST))
    .filter((link) => !classifyTechnicalSource(link).rejected);

  return {
    publisher,
    corroborates,
    accepted,
    rejected,
    discoveredIndependentLinks: links,
    substantive: true,
    evidenceEvaluationSkipped: false,
  };
}

/**
 * Live Principal↔Savoy independent verification under the isolated $0.05 ledger.
 */
export async function executeAuthorizedSavoyFollowupResearch(input = {}) {
  if (input.authorizeLive !== true || input.manifestAuthorized !== true || input.manifest?.status !== "AUTHORIZED") {
    return { executed: false, liveCalls: 0, reason: "awaiting_explicit_authorization", publicationEligibilityChanged: false };
  }
  if (Number(input.manifest?.hardCeilingUsd) !== SAVOY_BUDGET_USD) {
    return { executed: false, reason: "ceiling_mismatch", publicationEligibilityChanged: false, spent: false };
  }
  if (!input.fetchPage || typeof input.fetchPage !== "function") {
    return { executed: false, reason: "fetch_page_required", publicationEligibilityChanged: false };
  }

  const ledgerPath = input.ledgerPath;
  const checkpointDir = input.checkpointDir;
  const id = "gdi_cross_org_principal_savoy_followup";
  const assignment = buildCrossOrganizationFollowUpAssignment({ hardCeilingUsd: SAVOY_BUDGET_USD });
  const objective = assignment.researchObjective;

  let state = null;
  const checkpointFile = maturationMemoryPath(checkpointDir, id);
  if (fs.existsSync(checkpointFile)) {
    state = JSON.parse(fs.readFileSync(checkpointFile, "utf8"));
  }
  if (!state || state.status !== "IN_PROGRESS") {
    state = {
      id,
      status: "IN_PROGRESS",
      investigatedUrls: [],
      candidateQueue: [...SEED_INDEPENDENT_CANDIDATES],
      retrievals: [],
      sourceAttempts: [],
      substantiveQuestions: [],
      acceptedClaims: [],
      rejectedClaims: [],
      relatedEntityFollowUps: [],
      followUpQuestions: [],
      planLog: [],
      plannerModelCalls: 0,
      disposition: null,
      stoppingTrigger: null,
      commercialNextAction: null,
      verificationStatus: RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
      independentlyVerified: false,
      publicationEligibilityChanged: false,
      cooledHosts: [],
      preferredRecoveryMethod: null,
    };
    writeMaturationCheckpointAtomic(checkpointDir, state);
  }
  if (!Array.isArray(state.sourceAttempts)) state.sourceAttempts = [];
  if (!Array.isArray(state.cooledHosts)) state.cooledHosts = [];

  let loops = 0;
  while (state.status === "IN_PROGRESS" && loops < 10) {
    loops += 1;
    const attemptBudget = summarizeAttemptBudget({
      attempts: state.sourceAttempts,
      substantiveRetrievalCap: SAVOY_MAX_SUBSTANTIVE_RETRIEVALS,
      failedTechnicalAttemptCap: SAVOY_MAX_FAILED_TECHNICAL_ATTEMPTS,
    });
    if (!attemptBudget.canAttemptMore) {
      state.stoppingTrigger = attemptBudget.failedTechnicalAttempts >= SAVOY_MAX_FAILED_TECHNICAL_ATTEMPTS
        ? "FAILED_TECHNICAL_ATTEMPT_CAP_REACHED"
        : STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED;
      state.disposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
      state.commercialNextAction = "Buyer engagement remains valid; independent corroboration incomplete after attempt limits";
      state.status = "STOPPED";
      state.planLog.push({ event: "attempt_budget_stop", attemptBudget });
      writeMaturationCheckpointAtomic(checkpointDir, state);
      break;
    }
    if (state.substantiveQuestions.length >= SAVOY_MAX_SUBSTANTIVE_QUESTIONS) {
      state.stoppingTrigger = "SUBSTANTIVE_QUESTION_CAP_REACHED";
      state.disposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
      state.status = "STOPPED";
      break;
    }

    const ledger = loadOrCreateSavoyFollowupLedger(ledgerPath);
    const remainingUsd = Number((SAVOY_BUDGET_USD - Number(ledger.estimatedSpentUsd || 0) - Number(ledger.reservedUsd || 0)).toFixed(6));
    if (!(remainingUsd > 0)) {
      state.disposition = RESEARCH_DISPOSITION.BUDGET_EXHAUSTED;
      state.stoppingTrigger = STOPPING_TRIGGER.BUDGET_EXHAUSTED;
      state.status = "STOPPED";
      state.planLog.push({ event: "budget_exhaustion", remainingUsd });
      writeMaturationCheckpointAtomic(checkpointDir, state);
      break;
    }

    const activeQuestion = state.followUpQuestions.slice(-1)[0]
      || "Does an independently published source (not a Principal webpage) corroborate a programme or commercial relationship between Principal. London DMC and The Savoy?";

    const plannerJob = `gdi-sv-planner-${state.plannerModelCalls + 1}`;
    const billed = await billedPlannerCall({
      ledgerPath,
      jobKey: plannerJob,
      planInput: {
        objective: {
          hotelId: "recC2kbCaS48jFjFF",
          hotelName: "Nobu Hotel London Portman Square",
          commercialQuestion: objective,
        },
        hotel: { name: "Nobu Hotel London Portman Square", market: "London" },
        opportunity: {
          organizationName: ORGANIZER_NAME,
          commercialStage: "NEEDS_RESEARCH",
          pathway: "ACCOUNT_LED",
        },
        commercial: { salesReadiness: "NEEDS_RESEARCH" },
        findings: state.acceptedClaims.map((item) => ({
          url: item.url,
          excerpt: item.statement,
          statement: item.statement,
        })),
        openQuestions: [
          activeQuestion,
          "Only independently owned non-Principal publishers are permitted.",
          "A Savoy page that does not name Principal is not confirmation.",
          "Do not invent Nobu demand or an open RFP.",
          state.candidateQueue[0]
            ? `Independent candidate pool includes ${state.candidateQueue.slice(0, 4).join(", ")}.`
            : "Independent candidate pool is empty; propose non-Principal URLs only.",
        ],
        investigated: { urls: state.investigatedUrls },
        authorizePaidResearch: false,
        remainingUsd: 0,
        questionFingerprints: state.substantiveQuestions.map((item) => item.question),
      },
    });

    if (!billed.skipped) {
      state.plannerModelCalls += billed.plan.plannerModelCalls || 0;
      state.planLog.push({
        event: "planner_call",
        disposition: billed.plan.researchDisposition,
        proposals: (billed.plan.proposals || []).map((item) => ({ question: item.question, url: item.url })),
      });
      for (const proposal of billed.plan.proposals || []) {
        if (!proposal.url) continue;
        if (!isIndependentPublisherUrl(proposal.url, ORGANIZER_HOST)) {
          state.planLog.push({ event: "organizer_url_rejected", url: proposal.url });
          continue;
        }
        if (classifyTechnicalSource(proposal.url).rejected) continue;
        const n = normalizeUrl(proposal.url);
        if (!state.investigatedUrls.map(normalizeUrl).includes(n)
          && !state.candidateQueue.map(normalizeUrl).includes(n)) {
          state.candidateQueue.push(proposal.url);
          state.planLog.push({ event: "independent_lead_queued", url: proposal.url, from: "planner" });
        }
      }
      if (billed.plan.proposals?.[0]?.question) {
        state.followUpQuestions.push(billed.plan.proposals[0].question);
      }
    } else if (billed.reason !== "job_already_used" && state.plannerModelCalls >= SAVOY_MAX_PLANNER_CALLS) {
      state.disposition = RESEARCH_DISPOSITION.BUDGET_EXHAUSTED;
      state.stoppingTrigger = STOPPING_TRIGGER.BUDGET_EXHAUSTED;
      state.status = "STOPPED";
      break;
    }

    const cooled = new Set(state.cooledHosts || []);
    const filteredQueue = state.candidateQueue.filter((url) => {
      try {
        const host = new URL(url).hostname.replace(/^www\./i, "").toLowerCase();
        return !cooled.has(host);
      } catch {
        return false;
      }
    });
    const selection = selectNextIndependentSource({
      candidates: filteredQueue,
      investigatedUrls: state.investigatedUrls,
      question: activeQuestion,
    });
    state.planLog.push({
      event: "source_selection",
      question: activeQuestion,
      selected: selection.selected?.url || null,
      score: selection.selected?.score || null,
      ranking: selection.ranking,
      rejectedOrganizerCount: selection.rejectedOrganizer.length,
      cooledHosts: [...cooled],
      whySelected: selection.selected
        ? `Independent publisher candidate scored ${selection.selected.score} for question: ${activeQuestion}`
        : "No remaining independent publisher candidate",
    });

    if (!selection.selected) {
      state.verificationStatus = RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE;
      state.independentlyVerified = false;
      state.disposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
      state.stoppingTrigger = STOPPING_TRIGGER.NO_UNREAD_COMMERCIAL_SOURCE;
      state.commercialNextAction = "Buyer engagement remains valid; do not claim independently verified Savoy partnership or Nobu demand";
      state.status = "STOPPED";
      state.planLog.push({ event: "no_independent_candidate_remaining" });
      writeMaturationCheckpointAtomic(checkpointDir, state);
      break;
    }

    const url = selection.selected.url;
    const attemptNumber = state.sourceAttempts.length + 1;
    const fetched = await fetchDirectOrFirecrawl({
      url,
      fetchPage: input.fetchPage,
      ledgerPath,
      jobKey: `gdi-sv-attempt-${attemptNumber}`,
      preferredMethod: state.preferredRecoveryMethod,
    });
    state.preferredRecoveryMethod = null;
    if (fetched.refused) {
      state.candidateQueue = state.candidateQueue.filter((item) => normalizeUrl(item) !== normalizeUrl(url));
      state.planLog.push({ event: "organizer_publisher_blocked", url });
      continue;
    }

    const attempt = recordSourceAttempt({
      url,
      organizationHost: ORGANIZER_HOST,
      organizationName: ORGANIZER_NAME,
      provider: fetched.provider,
      method: fetched.method || fetched.provider,
      researchQuestion: activeQuestion,
      ok: fetched.ok === true,
      text: fetched.text || "",
      html: fetched.html || "",
      providerError: fetched.providerError || null,
      httpStatus: fetched.httpStatus || null,
      accessRestricted: fetched.accessRestricted === true,
      actualUsd: fetched.actualUsd || 0,
      attemptIndex: attemptNumber,
      relevantContentPredicate: pageCorroboratesPrincipalSavoyRelationship,
    });
    state.sourceAttempts.push(attempt);
    state.planLog.push({
      event: attempt.isFailedTechnicalAttempt ? "retrieval_failure_recorded" : "retrieval_success_recorded",
      url: attempt.url,
      retrievalOutcome: attempt.retrievalOutcome,
      evidenceEvaluationSkipped: attempt.evidenceEvaluationSkipped,
      providerError: attempt.providerError,
    });

    // Failed/empty attempts: preserve history, recover, do NOT evaluate evidence claims.
    if (!attempt.mayEvaluateEvidence) {
      state.investigatedUrls.push(url);
      state.candidateQueue = state.candidateQueue.filter((item) => normalizeUrl(item) !== normalizeUrl(url));
      const recovery = proposeAdaptiveRecovery({
        failedAttempt: attempt,
        attempts: state.sourceAttempts,
        candidateQueue: state.candidateQueue,
        researchQuestion: activeQuestion,
        organizationHost: ORGANIZER_HOST,
      });
      state.cooledHosts = [...new Set([...(state.cooledHosts || []), ...(recovery.cooledHosts || [])])];
      for (const next of recovery.nextCandidates || []) {
        const n = normalizeUrl(next.url);
        if (state.investigatedUrls.map(normalizeUrl).includes(n)) continue;
        if (state.candidateQueue.map(normalizeUrl).includes(n)) continue;
        if (!isIndependentPublisherUrl(next.url, ORGANIZER_HOST)) continue;
        state.candidateQueue.unshift(next.url);
      }
      const methodAction = (recovery.actions || []).find((item) => item.type === "try_alternate_retrieval_method");
      if (methodAction?.method === "firecrawl_scrape") state.preferredRecoveryMethod = "firecrawl";
      if (methodAction?.method === "direct_public") state.preferredRecoveryMethod = "direct_public";
      state.planLog.push({
        event: "adaptive_recovery_proposed",
        preservedResearchQuestion: recovery.preservedResearchQuestion,
        proposedNext: recovery.proposedNextIndependentSource,
        cooledHosts: recovery.cooledHosts,
      });
      // Keep the original commercial question; do not invent negative relationship findings.
      state.followUpQuestions.push(activeQuestion);
      writeMaturationCheckpointAtomic(checkpointDir, state);
      continue;
    }

    const evaluated = evaluateRetrievedPage({
      url,
      text: fetched.text,
      html: fetched.html,
      mayEvaluateEvidence: true,
    });
    state.retrievals.push({
      url,
      ok: true,
      provider: fetched.provider,
      actualUsd: fetched.actualUsd || 0,
      publishingOrganization: evaluated.publisher.publishingOrganization,
      sourceOwnerHost: evaluated.publisher.sourceOwnerHost,
      independentOfOrganizer: true,
      sourceFamily: /press|news|media/i.test(url) ? "historical_meeting_program" : "partner_organization",
      question: activeQuestion,
      whySelected: `Selected independent publisher ${evaluated.publisher.publishingOrganization} (${evaluated.publisher.sourceOwnerHost}) to test Principal–Savoy corroboration.`,
      corroboratesRelationship: evaluated.corroborates,
      retrievalOutcome: attempt.retrievalOutcome,
    });
    state.investigatedUrls.push(url);
    state.candidateQueue = state.candidateQueue.filter((item) => normalizeUrl(item) !== normalizeUrl(url));
    state.acceptedClaims.push(...evaluated.accepted);
    state.rejectedClaims.push(...evaluated.rejected);
    state.substantiveQuestions.push({
      question: activeQuestion,
      url,
      substantive: true,
      dimension: COMMERCIAL_RESEARCH_DIMENSION.ACCOMMODATION_PARTNERS,
      corroborates: evaluated.corroborates,
      retrievalOutcome: attempt.retrievalOutcome,
    });

    for (const link of evaluated.discoveredIndependentLinks || []) {
      const n = normalizeUrl(link);
      if (state.investigatedUrls.map(normalizeUrl).includes(n)) continue;
      if (state.candidateQueue.map(normalizeUrl).includes(n)) continue;
      if (!/savoy|fairmont|principal|incentive|meeting|event|press/i.test(link)) continue;
      state.candidateQueue.push(link);
      state.relatedEntityFollowUps.push({
        entityName: link,
        relationship: ENTITY_RELATIONSHIP.RELATED_ENTITY_LEAD,
        suggestedUrl: link,
        verificationStatus: RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
      });
      state.planLog.push({ event: "independent_lead_queued", url: link, from: "page_links" });
    }

    if (evaluated.corroborates) {
      const verification = assessRelationshipVerification({
        provenanceUrl: assignment.relatedEntity.provenanceUrls[0],
        confirmingUrl: url,
        organizationHost: ORGANIZER_HOST,
        relationshipMentionedOnOrganizer: true,
        independentPublisherConfirms: true,
      });
      state.verificationStatus = verification.verificationStatus;
      state.independentlyVerified = verification.independentlyVerified === true;
      state.confirmingUrl = url;
      state.confirmingPublisher = evaluated.publisher.publishingOrganization;
      state.disposition = RESEARCH_DISPOSITION.RESEARCH_SUFFICIENT;
      state.stoppingTrigger = STOPPING_TRIGGER.RESEARCH_SUFFICIENT;
      state.commercialNextAction = "Organizer-claimed Savoy history now has independent publisher support for Principal–Savoy linkage; still no Nobu RFP/demand assertion";
      state.status = "STOPPED";
      state.planLog.push({
        event: "independent_verification_accepted",
        url,
        publishingOrganization: evaluated.publisher.publishingOrganization,
      });
      writeMaturationCheckpointAtomic(checkpointDir, state);
      break;
    }

    state.followUpQuestions.push(
      `After reading ${evaluated.publisher.publishingOrganization}, is there another independent publisher that names both Principal and The Savoy together?`,
    );
    writeMaturationCheckpointAtomic(checkpointDir, state);
  }

  if (state.status === "IN_PROGRESS") {
    state.verificationStatus = state.independentlyVerified
      ? RELATIONSHIP_VERIFICATION.INDEPENDENTLY_VERIFIED
      : RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE;
    state.disposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
    state.stoppingTrigger = state.stoppingTrigger || "RETRIEVAL_RECOVERY_REQUIRED";
    state.commercialNextAction = "Buyer engagement remains valid; Savoy independent confirmation incomplete";
    state.status = "STOPPED";
    state.planLog.push({
      event: "loop_complete_stop",
      stoppingTrigger: state.stoppingTrigger,
      verificationStatus: state.verificationStatus,
    });
    writeMaturationCheckpointAtomic(checkpointDir, state);
  }

  if (!state.independentlyVerified && state.verificationStatus !== RELATIONSHIP_VERIFICATION.INDEPENDENTLY_VERIFIED) {
    state.verificationStatus = RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE;
  }

  const ledger = loadOrCreateSavoyFollowupLedger(ledgerPath);
  const remainingAuthorizationUsd = Number((SAVOY_BUDGET_USD - Number(ledger.estimatedSpentUsd || 0)).toFixed(6));
  const investigationReport = buildCrossOrgInvestigationReport({
    attempts: state.sourceAttempts,
    independentlyVerified: state.independentlyVerified === true,
    verificationStatus: state.verificationStatus,
    commercialNextAction: state.commercialNextAction,
    remainingAuthorizedBudgetUsd: remainingAuthorizationUsd,
    substantiveRetrievalCap: SAVOY_MAX_SUBSTANTIVE_RETRIEVALS,
    failedTechnicalAttemptCap: SAVOY_MAX_FAILED_TECHNICAL_ATTEMPTS,
    buyerEngagementStillValid: true,
  });
  const stopRecord = investigationReport.stopRecord;
  state.stoppingTrigger = state.stoppingTrigger || stopRecord.stoppingTrigger;

  const newLeadsFollowed = state.planLog.filter((item) => item.event === "independent_lead_queued").length > 0
    && state.retrievals.some((item, index) => index > 0);

  const resumed = JSON.parse(fs.readFileSync(maturationMemoryPath(checkpointDir, id), "utf8"));
  const checkpointResumeOk = resumed.status === "STOPPED"
    && Array.isArray(resumed.investigatedUrls)
    && resumed.investigatedUrls.length === state.investigatedUrls.length;

  const substantiveSuccessful = state.sourceAttempts.filter((item) => item.countsTowardSubstantiveRetrievalCap).length;
  const failedTechnical = state.sourceAttempts.filter((item) => item.countsTowardFailedTechnicalAttemptCap).length;

  return {
    executed: true,
    certificationPassed: Number(ledger.estimatedSpentUsd || 0) - SAVOY_BUDGET_USD <= 1e-9
      && substantiveSuccessful <= SAVOY_MAX_SUBSTANTIVE_RETRIEVALS
      && failedTechnical <= SAVOY_MAX_FAILED_TECHNICAL_ATTEMPTS
      && state.substantiveQuestions.length <= SAVOY_MAX_SUBSTANTIVE_QUESTIONS
      && state.sourceAttempts.every((item) => item.independentOfOrganizer !== false || item.samePublisherAsOrganizer !== true)
      && state.publicationEligibilityChanged !== true,
    version: SAVOY_FOLLOWUP_VERSION,
    researchObjective: objective,
    sourceAttemptLedger: state.sourceAttempts,
    sourcesVisited: state.retrievals,
    substantiveQuestions: state.substantiveQuestions,
    acceptedClaims: state.acceptedClaims,
    rejectedClaims: state.rejectedClaims,
    contradictoryEvidence: state.acceptedClaims.filter((item) => item.topic === "contradiction"),
    verificationStatus: state.verificationStatus,
    independentlyVerified: state.independentlyVerified === true,
    confirmingUrl: state.confirmingUrl || null,
    confirmingPublisher: state.confirmingPublisher || null,
    newResearchLeadsDiscovered: state.planLog.filter((item) => item.event === "independent_lead_queued" || item.event === "adaptive_recovery_proposed").length,
    newResearchLeadsFollowed: newLeadsFollowed,
    plannerModelCalls: state.plannerModelCalls,
    retrievalCount: substantiveSuccessful,
    failedTechnicalAttemptCount: failedTechnical,
    substantiveQuestionCount: state.substantiveQuestions.length,
    actualSpentUsd: ledger.estimatedSpentUsd,
    remainingAuthorizationUsd,
    authorizedUsd: SAVOY_BUDGET_USD,
    researchDisposition: state.disposition,
    stoppingTrigger: state.stoppingTrigger,
    commercialNextAction: state.commercialNextAction,
    stopRecord,
    investigationReport,
    nobuCommercialImplications: {
      openSavoyDemandAsserted: false,
      nobuPrincipalRelationshipAsserted: false,
      principalAccountDevelopmentStillValid: true,
      savoyRelationshipIndependentlyVerified: state.independentlyVerified === true,
      note: state.independentlyVerified
        ? "Independent publisher support exists for Principal–Savoy historical/commercial linkage. This does not create Nobu demand or an open RFP."
        : "Independent confirmation of Principal–Savoy was unavailable or incomplete (including retrieval-recovery failures). Principal remains an account-development prospect for Nobu; buyer engagement remains valid. Do not treat Savoy history as independently verified or as Nobu demand.",
    },
    checkpointStatus: state.status,
    checkpointResumeOk,
    planLog: state.planLog,
    publicationEligibilityChanged: false,
    salesReadiness: "NEEDS_RESEARCH",
    commercialStage: "NEEDS_RESEARCH",
  };
}
