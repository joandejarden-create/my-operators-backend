/**
 * GDI_AUTONOMOUS_HOTEL_FULL_E2E_V2
 *
 * One hotel-agnostic supervisor. It resolves a hotel name, builds per-opportunity
 * research state, plans the next zero-cost repair, and records paid work as
 * blocked until a fresh authorization exists. It does not publish, spend, or
 * rebuild local opportunity files.
 */

import fs from "node:fs";
import path from "node:path";
import { GDI_REQUIRED_SEGMENTS } from "../../dealality-full-e2e-os/gdi-full-e2e-workflow-v1.js";
import { deriveProspectCommercialSection } from "../priority-action-window/assignment-contract-v1.js";
import { hasUsableContactRoute } from "../prospect-publication-eligibility-v1.js";
import { qualifyCanonicalOpportunity } from "../canonical-commercial-contract-v1.js";
import {
  enrichGdiOpportunityForCustomer,
  isGdiCustomerDrawerReady,
} from "../enrich-gdi-opportunity-for-customer.js";
import { classifyDemandPosture } from "./gdi-three-hotel-full-e2e-v1.js";
import {
  buildMarketProfile,
  EXECUTION_MODE,
  runWeeklyIncremental,
} from "./gdi-global-weekly-feedback-v2.js";
import { loadHotelEvidenceArchive, runHotelRecovery } from "./gdi-research-recovery-v2.js";
import { summarizeAcquisitionPlan } from "./gdi-evidence-acquisition-v2.js";
import { paidMultisegmentAuthorizationLedgerPath } from "../parallel/budget-ledger-v1.js";
import { resolvePortfolioAuthorization } from "./portfolio-us-marriott-five-hotel-auth-v1.js";
import { loadActiveResearchAuthorization } from "./gdi-five-hotel-research-authorization-v1.js";
import { verifiedProviderBilling } from "./gdi-provider-billing-guard-v1.js";
import { classifyWorkstreamResearchPosture } from "../research/segment-research-sufficiency-v1.js";

export const GDI_AUTONOMOUS_HOTEL_FULL_E2E_V2 = "GDI_AUTONOMOUS_HOTEL_FULL_E2E_V2";
export const COMMERCIAL_TARGET_MIN = 10;
export const COMMERCIAL_TARGET_MAX = 15;
export const MAX_SAME_ACTION_ATTEMPTS = 2;

export const RESEARCH_LIFECYCLE = Object.freeze({
  DISCOVERED: "DISCOVERED",
  INVESTIGATING: "INVESTIGATING",
  EVIDENCE_PARTIAL: "EVIDENCE_PARTIAL",
  QUALIFYING: "QUALIFYING",
  CONTACT_RESEARCH: "CONTACT_RESEARCH",
  READY_FOR_EVALUATION: "READY_FOR_EVALUATION",
  PUBLICATION_ELIGIBLE: "PUBLICATION_ELIGIBLE",
  NEEDS_MORE_RESEARCH: "NEEDS_MORE_RESEARCH",
  BLOCKED_AUTHORIZATION: "BLOCKED_AUTHORIZATION",
  BLOCKED_SOURCE_ACCESS: "BLOCKED_SOURCE_ACCESS",
  RESEARCH_EXHAUSTED: "RESEARCH_EXHAUSTED",
  REJECTED: "REJECTED",
  NOT_OPEN_BOOKING: "NOT_OPEN_BOOKING",
  SUPERSEDED: "SUPERSEDED",
});

const FORWARD = Object.freeze({
  DISCOVERED: ["INVESTIGATING", "REJECTED", "NOT_OPEN_BOOKING", "SUPERSEDED"],
  INVESTIGATING: ["EVIDENCE_PARTIAL", "QUALIFYING", "NEEDS_MORE_RESEARCH", "REJECTED", "NOT_OPEN_BOOKING"],
  EVIDENCE_PARTIAL: ["QUALIFYING", "CONTACT_RESEARCH", "NEEDS_MORE_RESEARCH", "RESEARCH_EXHAUSTED", "REJECTED"],
  QUALIFYING: ["CONTACT_RESEARCH", "READY_FOR_EVALUATION", "NEEDS_MORE_RESEARCH", "REJECTED"],
  CONTACT_RESEARCH: ["READY_FOR_EVALUATION", "NEEDS_MORE_RESEARCH", "BLOCKED_AUTHORIZATION", "RESEARCH_EXHAUSTED"],
  READY_FOR_EVALUATION: ["PUBLICATION_ELIGIBLE", "NEEDS_MORE_RESEARCH", "REJECTED"],
  NEEDS_MORE_RESEARCH: ["INVESTIGATING", "BLOCKED_AUTHORIZATION", "RESEARCH_EXHAUSTED", "REJECTED"],
  PUBLICATION_ELIGIBLE: ["SUPERSEDED", "NEEDS_MORE_RESEARCH"],
  BLOCKED_AUTHORIZATION: ["INVESTIGATING", "NEEDS_MORE_RESEARCH"],
  BLOCKED_SOURCE_ACCESS: ["INVESTIGATING", "RESEARCH_EXHAUSTED"],
  RESEARCH_EXHAUSTED: ["INVESTIGATING"],
  REJECTED: [],
  NOT_OPEN_BOOKING: ["INVESTIGATING"],
  SUPERSEDED: [],
});

export function evaluateExecutionGates({
  hotelId,
  authorizePublicFetch = false,
  authorizePaid = false,
  paidCapUsd = null,
  authorizePersistence = false,
  authorizePublication = false,
} = {}) {
  const portfolioGdi = resolvePortfolioAuthorization({ hotelId, product: "GDI" });
  const portfolioAdp = resolvePortfolioAuthorization({ hotelId, product: "ADP" });
  const active = loadActiveResearchAuthorization(hotelId);
  const billing = verifiedProviderBilling();
  const paidProviderEnabled = ["parallel", "serpapi", "surfe", "jev"].some((name) => billing[name].enabled === true);
  return {
    hotelId,
    research: {
      publicFetch: active.ok ? "AUTHORIZED" : "NOT_AUTHORIZED",
      publicFetchReason: active.ok ? "active_founder_ledger" : active.reason,
      paid: active.ok && paidProviderEnabled ? "AUTHORIZED" : "NOT_AUTHORIZED",
      paidReason: active.ok ? "NO_VERIFIED_PAID_PROVIDER_PRICE" : active.reason,
      paidCapUsd: active.ok ? active.hardCapUsd : null,
      callerFlagsIgnored: authorizePublicFetch === true || authorizePaid === true || paidCapUsd != null,
      portfolioGdiRecord: portfolioGdi.ok
        ? { authorizationId: portfolioGdi.authorizationId, budgetUsd: portfolioGdi.budgetUsd, consumedThisRun: false, reusedForThisCohort: false }
        : { ok: false, reason: portfolioGdi.reason },
      portfolioAdpRecord: portfolioAdp.ok
        ? { authorizationId: portfolioAdp.authorizationId, budgetUsd: portfolioAdp.budgetUsd, consumedThisRun: false, reusedForThisCohort: false }
        : { ok: false, reason: portfolioAdp.reason },
      billing,
    },
    persistence: authorizePersistence === true ? "AUTHORIZED_BY_CALLER" : "NOT_AUTHORIZED",
    publication: authorizePublication === true ? "AUTHORIZED_BY_CALLER" : "NOT_AUTHORIZED",
    historicalSharedBalanceAvailable: false,
    adpBudgetUsed: false,
    authorizationBlockerIsExhaustion: false,
  };
}

export function assertLifecycleTransition(from, to) {
  const allowed = FORWARD[from] || [];
  if (from === to) return { ok: true, unchanged: true };
  if (!allowed.includes(to)) {
    return { ok: false, reason: `illegal_transition_${from}_to_${to}` };
  }
  return { ok: true, unchanged: false };
}

function readJson(file, fallback = null) {
  if (!file || !fs.existsSync(file)) return fallback;
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function norm(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function listHotelConfigs(repoRoot) {
  const dir = path.join(repoRoot, "config/group-demand-intelligence/hotels");
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((name) => name.endsWith(".json"))
    .map((name) => readJson(path.join(dir, name), null))
    .filter((cfg) => cfg?.hotelId && cfg?.displayName);
}

function tokens(value) {
  const stop = new Set(["the", "hotel", "and", "of", "a"]);
  return norm(value)
    .split(" ")
    .filter((token) => token && !stop.has(token));
}

export function resolveHotelQuery(query, repoRoot) {
  const raw = String(query || "").trim();
  if (!raw) return { ok: false, error: "MISSING_HOTEL" };
  const configs = listHotelConfigs(repoRoot);
  const byId = configs.find((cfg) => cfg.hotelId === raw);
  if (byId) {
    return { ok: true, hotelId: byId.hotelId, displayName: byId.displayName, match: "canonical_id" };
  }
  const needle = norm(raw);
  const exact = configs.filter((cfg) => norm(cfg.displayName) === needle);
  if (exact.length === 1) {
    return { ok: true, hotelId: exact[0].hotelId, displayName: exact[0].displayName, match: "exact_name" };
  }
  const queryTokens = new Set(tokens(raw));
  const ranked = configs
    .map((cfg) => ({
      cfg,
      score: tokens(cfg.displayName).filter((token) => queryTokens.has(token)).length,
    }))
    .filter((row) => row.score > 0)
    .sort((a, b) => b.score - a.score);
  if (!ranked.length) return { ok: false, error: "UNRESOLVED_HOTEL", query: raw };
  const best = ranked[0].score;
  const winners = ranked.filter((row) => row.score === best);
  if (winners.length !== 1) {
    return {
      ok: false,
      error: "AMBIGUOUS_HOTEL",
      candidates: winners.map((row) => ({
        hotelId: row.cfg.hotelId,
        displayName: row.cfg.displayName,
      })),
    };
  }
  const winner = winners[0].cfg;
  const winnerTokens = new Set(tokens(winner.displayName));
  const aliasText = Object.entries(winner.aliases || {})
    .filter(([key]) => key !== "note")
    .map(([, value]) => (typeof value === "string" ? value : ""))
    .join(" ");
  const context = new Set(
    tokens([winner.city, winner.country, winner.region, winner.demandTerritory?.label, aliasText].join(" "))
  );
  const unmatched = [...queryTokens].filter(
    (token) => token.length >= 3 && !winnerTokens.has(token) && !context.has(token)
  );
  if (unmatched.length) {
    return {
      ok: false,
      error: "UNMATCHED_IDENTITY_TOKENS",
      unmatched,
      candidate: { hotelId: winner.hotelId, displayName: winner.displayName },
    };
  }
  return {
    ok: true,
    hotelId: winner.hotelId,
    displayName: winner.displayName,
    match: "unique_name",
  };
}

export function assessCompleteness(opp = {}) {
  const route = hasUsableContactRoute(opp);
  const posture = classifyDemandPosture(opp);
  const questions = {
    organization: Boolean(opp.organizationName || opp.company),
    demandNeed: Boolean(opp.demandSignalType || opp.opportunityType || opp.segment),
    documentedEvidence:
      (Array.isArray(opp.establishedFacts) && opp.establishedFacts.length > 0) ||
      (Array.isArray(opp.sources) && opp.sources.length > 0),
    destinationRelevance: Boolean(opp.localPresence || opp.destinationStatus || opp.eventLocationSummary),
    hotelFit: Boolean(opp.hotelOpportunityThesis || opp.whyThisHotel || opp.fitExplanation),
    timingKnown: Boolean(opp.timing || opp.eventStartDate || opp.eventDateDisplay),
    whyNow: String(opp.whyNow || "").trim().length > 24,
    contactRoute: route.ok === true,
    nextAction: String(opp.recommendedAction || opp.recommendedNextStep || "").trim().length > 12,
    confidenceLabeled: Boolean(posture.posture),
  };
  const unsafeMissing = [];
  if (!questions.organization) unsafeMissing.push("organization");
  if (!questions.documentedEvidence) unsafeMissing.push("documented_evidence");
  if (!questions.contactRoute) unsafeMissing.push("contact_route");
  if (!questions.hotelFit) unsafeMissing.push("hotel_fit");
  if (!questions.whyNow) unsafeMissing.push("why_now");
  if (!questions.nextAction) unsafeMissing.push("next_action");
  const weakMissing = [];
  if (!questions.timingKnown && opp.standingOpportunity !== true && opp.cateringOnly !== true) {
    weakMissing.push("timing");
  }
  if (!questions.destinationRelevance) weakMissing.push("destination_relevance");
  return {
    questions,
    unsafeMissing,
    weakMissing,
    publicationSafe: unsafeMissing.length === 0 && posture.posture !== "NOT_OPEN_BOOKING",
    posture: posture.posture,
  };
}

export function buildOpportunityResearchState(opp, hotelId, attempted = []) {
  const id = String(opp.id || opp.opportunityId || "").trim();
  if (opp.hotelId && String(opp.hotelId) !== String(hotelId)) {
    return {
      hotelId,
      opportunityId: id,
      organization: opp.organizationName || null,
      lifecycle: RESEARCH_LIFECYCLE.REJECTED,
      terminationReason: "cross_property_row",
      sourceHotelId: opp.hotelId,
      hotelFacing: false,
      storedCustomerVisible: false,
      publicationEligibility: "NOT_A_PROSPECT",
      publicationEligibilityMatchesStoredVisibility: true,
      evidenceGaps: ["cross_property_row"],
      attemptedActions: attempted,
      researchCostUsd: 0,
      commercialSection: null,
      commercialSectionSource: "rejected",
      demandMechanism: null,
      accountDevelopmentPublished: false,
    };
  }
  const completeness = assessCompleteness(opp);
  let eligibility = { hotelFacing: false, eligibility: "NOT_EVALUATED", reasons: [] };
  let demandMechanism = null;
  let accountDevelopmentPublished = false;
  try {
    const canonical = qualifyCanonicalOpportunity(opp);
    eligibility = canonical.eligibility;
    demandMechanism = canonical.demand.mechanism;
    accountDevelopmentPublished = canonical.publication.accountDevelopmentPublished;
  } catch (error) {
    eligibility = { hotelFacing: false, eligibility: "EVALUATION_ERROR", reasons: [error.message] };
  }
  const section = deriveProspectCommercialSection(opp, { eligibility });
  let lifecycle = RESEARCH_LIFECYCLE.DISCOVERED;
  if (completeness.posture === "NOT_OPEN_BOOKING") lifecycle = RESEARCH_LIFECYCLE.NOT_OPEN_BOOKING;
  else if (eligibility.eligibility === "NOT_A_PROSPECT") lifecycle = RESEARCH_LIFECYCLE.REJECTED;
  else if (eligibility.hotelFacing === true && completeness.publicationSafe) {
    lifecycle = RESEARCH_LIFECYCLE.PUBLICATION_ELIGIBLE;
  } else if (!completeness.questions.contactRoute) lifecycle = RESEARCH_LIFECYCLE.CONTACT_RESEARCH;
  else if (completeness.unsafeMissing.length) lifecycle = RESEARCH_LIFECYCLE.NEEDS_MORE_RESEARCH;
  else lifecycle = RESEARCH_LIFECYCLE.READY_FOR_EVALUATION;

  const gaps = [...completeness.unsafeMissing, ...completeness.weakMissing];
  return {
    hotelId,
    opportunityId: id,
    organization: opp.organizationName || opp.company || null,
    segment: opp.researchSegment || opp.segment || null,
    lifecycle,
    knownEvidenceCount: Array.isArray(opp.establishedFacts) ? opp.establishedFacts.length : 0,
    evidenceGaps: gaps,
    contactRouteComplete: completeness.questions.contactRoute === true,
    qualification: opp.opportunityQualification || null,
    whyNowComplete: completeness.questions.whyNow === true,
    confidence: completeness.posture,
    attemptedActions: attempted,
    publicationEligibility: eligibility.eligibility,
    hotelFacing: eligibility.hotelFacing === true,
    storedCustomerVisible: opp.customerVisible === true,
    publicationEligibilityMatchesStoredVisibility:
      (eligibility.hotelFacing === true) === (opp.customerVisible === true),
    commercialSection: section.section,
    commercialSectionSource: section.source,
    demandMechanism,
    accountDevelopmentPublished,
    terminationReason: null,
    researchCostUsd: 0,
  };
}

export function planNextResearchAction(state, context = {}) {
  const tried = new Set(state.attemptedActions || []);
  const sameActionCount = (action) =>
    (state.attemptedActions || []).filter((item) => item === action).length;
  const bounded = (action) => sameActionCount(action) >= MAX_SAME_ACTION_ATTEMPTS;

  if (!context.localRemoteAligned && context.localCount === 0 && context.remoteCount > 0) {
    return {
      action: "REBUILD_LOCAL_SNAPSHOT",
      justified: false,
      executableNow: false,
      blocker: "BLOCKED_AUTHORIZATION",
      reason: "Local file is empty while the canonical store has rows. Writing the snapshot needs authorization.",
    };
  }
  if (context.localCount === 0 && context.remoteCount === 0 && !bounded("PLAN_WORKSTREAM_RESEARCH")) {
    return {
      action: "PLAN_WORKSTREAM_RESEARCH",
      justified: true,
      executableNow: true,
      costUsd: 0,
      paidBlocked: true,
      reason: "No stored opportunities. Plan the canonical workstreams before any paid search.",
    };
  }
  if (!state.commercialSection && !bounded("DERIVE_COMMERCIAL_SECTION")) {
    return {
      action: "DERIVE_COMMERCIAL_SECTION",
      justified: true,
      executableNow: true,
      costUsd: 0,
      reason: "Stored packaging section is blank. Derive it from current eligibility and evidence.",
    };
  }
  if (state.evidenceGaps.includes("why_now") && context.sourceWhyNow && !bounded("RESTORE_SOURCE_WHY_NOW")) {
    return {
      action: "RESTORE_SOURCE_WHY_NOW",
      justified: true,
      executableNow: true,
      costUsd: 0,
      reason: "A substantive why-now already exists on the source record and should survive customer mapping.",
    };
  }
  if (state.evidenceGaps.includes("why_now") || state.evidenceGaps.includes("qualification")) {
    return {
      action: "PAID_EVIDENCE_RESEARCH",
      justified: false,
      executableNow: false,
      blocker: "BLOCKED_AUTHORIZATION",
      costUsd: null,
      reason: "The missing field is not present on the source record. New research is not authorized.",
    };
  }
  if (context.segmentStatus === "NOT_RESEARCHED" && !bounded("RECORD_SEGMENT_GAP")) {
    return {
      action: "RECORD_SEGMENT_GAP",
      justified: true,
      executableNow: true,
      costUsd: 0,
      reason: "Segment has no research-state file. Record NOT_RESEARCHED rather than exhausted.",
    };
  }
  if (state.hotelFacing && state.lifecycle === RESEARCH_LIFECYCLE.PUBLICATION_ELIGIBLE) {
    return {
      action: "NONE",
      justified: false,
      executableNow: false,
      reason: "Publication-eligible on current evidence. No further zero-cost repair is queued.",
    };
  }
  return {
    action: tried.has("NONE") ? "STOP_NO_YIELD" : "NONE",
    justified: false,
    executableNow: false,
    reason: "No authorized action improves this opportunity.",
  };
}

export function applyZeroCostAction(state, action) {
  const next = {
    ...state,
    attemptedActions: [...(state.attemptedActions || []), action.action],
    lastAction: action.action,
    lastResult: action.executableNow ? "applied_in_memory" : action.blocker || "not_executed",
  };
  if (action.action === "DERIVE_COMMERCIAL_SECTION" && action.derivedSection) {
    next.commercialSection = action.derivedSection;
    next.commercialSectionSource = "derived";
  }
  if (action.action === "RESTORE_SOURCE_WHY_NOW") {
    next.whyNowComplete = true;
    next.evidenceGaps = (next.evidenceGaps || []).filter((gap) => gap !== "why_now");
  }
  if ((next.attemptedActions.filter((item) => item === action.action).length >= MAX_SAME_ACTION_ATTEMPTS) && action.executableNow !== true) {
    next.terminationReason = "bounded_repeat_without_new_evidence";
  }
  return next;
}

export function runBoundedLoop(state, context = {}) {
  let current = { ...state, attemptedActions: [...(state.attemptedActions || [])] };
  const journal = [];
  for (let i = 0; i < 6; i += 1) {
    const action = planNextResearchAction(current, context);
    journal.push(action);
    if (!action.executableNow) {
      if (action.blocker) current.lifecycle = action.blocker;
      current = applyZeroCostAction(current, action);
      break;
    }
    const withPayload =
      action.action === "DERIVE_COMMERCIAL_SECTION"
        ? { ...action, derivedSection: current.commercialSection || "EXPLORATORY" }
        : action;
    current = applyZeroCostAction(current, withPayload);
    if (current.terminationReason) break;
  }
  return { state: current, journal };
}

export function classifySegmentCoverage(segmentId, doc, opportunitySegments, opportunities = []) {
  if (!doc && !(opportunitySegments || []).includes(segmentId)) {
    return { segmentId, status: "NOT_RESEARCHED", exhausted: false };
  }
  if (!doc) {
    return {
      segmentId,
      status: "RESEARCHED_WITH_GAPS",
      exhausted: false,
      reason: "Opportunities name this segment, but no segment research-state file exists.",
    };
  }
  const researched = Array.isArray(doc.sourceFamiliesResearched) ? doc.sourceFamiliesResearched : [];
  const status = String(doc.status || "");
  const posture = classifyWorkstreamResearchPosture(doc, opportunities);
  if (/EXHAUSTED/i.test(status) && researched.length === 0) {
    return { segmentId, status: "IN_PROGRESS", posture, exhausted: false, reason: "Exhausted label rejected without source families." };
  }
  if (/EXHAUSTED/i.test(status) && researched.length > 0 && posture !== "DEMAND_VERIFIED") {
    return { segmentId, status: posture, posture, exhausted: false, reason: "Exhausted label lacks verified buyer demand." };
  }
  if (status === "COMPLETE" && posture !== "DEMAND_VERIFIED") {
    return { segmentId, status: posture, posture, exhausted: false, reason: "Stored complete lacks verified buyer demand." };
  }
  if (status === "COMPLETE" && posture === "DEMAND_VERIFIED") {
    return { segmentId, status: "DEMAND_VERIFIED", posture, exhausted: false, reason: "Verified lodging demand. Not commercial exhaustion." };
  }
  if (researched.length > 0 || status === "IN_PROGRESS") {
    return { segmentId, status: posture === "PLANNED" ? "IN_PROGRESS" : posture, posture, exhausted: false };
  }
  return { segmentId, status: posture, posture, exhausted: false };
}

export function reconcileCounts(localCount, remoteCount) {
  if (remoteCount == null) {
    return { status: "REMOTE_NOT_READ", localCount, remoteCount: null, aligned: false };
  }
  if (localCount === remoteCount) {
    return { status: "ALIGNED", localCount, remoteCount, aligned: true };
  }
  if (localCount === 0 && remoteCount > 0) {
    return {
      status: "LOCAL_EMPTY_REMOTE_PRESENT",
      localCount,
      remoteCount,
      aligned: false,
      reconstruction: "REQUIRES_WRITE_AUTHORIZATION",
    };
  }
  return { status: "COUNT_DIVERGENCE", localCount, remoteCount, aligned: false };
}

function loadLocalOpportunities(repoRoot, hotelId) {
  const file = path.join(repoRoot, "data/group-demand-intelligence/hotels", hotelId, "opportunities.json");
  const doc = readJson(file, { opportunities: [] });
  const rows = Array.isArray(doc.opportunities) ? doc.opportunities : [];
  return {
    file,
    rows: rows.filter((row) => !row.hotelId || row.hotelId === hotelId),
    foreign: rows.filter((row) => row.hotelId && row.hotelId !== hotelId).length,
  };
}

function loadSegments(repoRoot, hotelId, opportunitySegments, opportunities = []) {
  const dir = path.join(
    repoRoot,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "segment-research-state"
  );
  return GDI_REQUIRED_SEGMENTS.map((segmentId) => {
    const doc = readJson(path.join(dir, `${segmentId}.json`), null);
    return classifySegmentCoverage(segmentId, doc, opportunitySegments, opportunities);
  });
}

async function readRemote(hotelId, allow) {
  if (!allow) return { attempted: false, rows: null };
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
  if (!token) return { attempted: false, blocker: "AIRTABLE_TOKEN_ABSENT", rows: null };
  try {
    const { listOpportunitiesForHotel } = await import("../airtable-opportunity-store.js");
    const rows = await Promise.race([
      listOpportunitiesForHotel(hotelId),
      new Promise((_, reject) => setTimeout(() => reject(new Error("AIRTABLE_READ_TIMEOUT")), 45000)),
    ]);
    const foreign = (rows || []).filter((row) => row.hotelId && row.hotelId !== hotelId).length;
    return { attempted: true, ok: foreign === 0, foreign, rows: rows || [], writes: 0 };
  } catch (error) {
    return { attempted: true, ok: false, blocker: error.message, rows: null, writes: 0 };
  }
}

export async function runHotelFullE2e(query, opts = {}) {
  const repoRoot = opts.repoRoot;
  const identity = resolveHotelQuery(query, repoRoot);
  if (!identity.ok) return { ok: false, identity };
  const config = listHotelConfigs(repoRoot).find((cfg) => cfg.hotelId === identity.hotelId) || {};
  const marketProfile = buildMarketProfile(config);
  const executionMode = opts.mode || EXECUTION_MODE.INITIAL_FULL_E2E;
  const weekly =
    executionMode === EXECUTION_MODE.WEEKLY_INCREMENTAL
      ? runWeeklyIncremental({
          hotelId: identity.hotelId,
          prior: opts.priorState,
          opportunities: opts.weeklyOpportunities || [],
          newSignals: opts.newSignals || [],
          feedback: opts.feedback || [],
          idempotencyKey: opts.idempotencyKey,
          authorizePublish: false,
        })
      : null;
  const local = loadLocalOpportunities(repoRoot, identity.hotelId);
  const remote = await readRemote(identity.hotelId, opts.allowAirtableRead === true);
  const remoteRows = Array.isArray(remote.rows) ? remote.rows : null;
  const reconciliation = reconcileCounts(local.rows.length, remoteRows ? remoteRows.length : null);
  const scoring = remoteRows && remoteRows.length ? remoteRows : local.rows;
  const corpusSource = remoteRows && remoteRows.length
    ? "airtable_canonical"
    : local.rows.length
      ? "filesystem_mirror"
      : "none";
  const segmentsOnOpps = [...new Set(scoring.map((row) => row.researchSegment).filter(Boolean))];
  const segments = loadSegments(repoRoot, identity.hotelId, segmentsOnOpps, scoring);
  const recoveryRun = runHotelRecovery({
    hotelId: identity.hotelId,
    opportunities: scoring,
    archive: loadHotelEvidenceArchive(repoRoot, identity.hotelId),
    today: new Date().toISOString().slice(0, 10),
  });
  const recovery = {
    version: recoveryRun.version,
    opportunities: recoveryRun.results.length,
    researchCostUsd: 0,
    airtableWrites: 0,
    localWrites: 0,
    publicationChanged: false,
    contactsRestored: recoveryRun.results.filter((item) =>
      item.opportunity.researchHistory?.some((entry) => entry.action === "RESTORE_CONTACT_FROM_ARCHIVE")
    ).length,
    qualificationsComputed: recoveryRun.results.filter((item) =>
      item.opportunity.researchHistory?.some((entry) => entry.action === "COMPUTE_QUALIFICATION_FROM_EVIDENCE")
    ).length,
    rejectedUnsupported: recoveryRun.results.filter((item) => item.opportunity.recovery?.reason === "organization_and_evidence_absent").length,
    superseded: recoveryRun.results.filter((item) => item.opportunity.recovery?.reason === "event_concluded").length,
  };
  const gates = evaluateExecutionGates({
    hotelId: identity.hotelId,
    authorizePublicFetch: opts.authorizePublicFetch === true,
    authorizePaid: opts.authorizePaid === true,
    paidCapUsd: opts.paidCapUsd,
    authorizePersistence: opts.authorizePersistence === true,
    authorizePublication: opts.authorizePublication === true,
  });
  const acquisition = summarizeAcquisitionPlan(scoring, {
    hotelId: identity.hotelId,
    allowPublicFetch: gates.research.publicFetch === "AUTHORIZED",
    mode: executionMode,
  });
  const states = scoring.map((opp) => buildOpportunityResearchState(opp, identity.hotelId));
  const loops = states.slice(0, opts.loopLimit || states.length).map((state) => {
    const source = scoring.find((row) => (row.id || row.opportunityId) === state.opportunityId);
    return runBoundedLoop(state, {
      localCount: local.rows.length,
      remoteCount: remoteRows ? remoteRows.length : null,
      localRemoteAligned: reconciliation.aligned,
      sourceWhyNow: source && String(source.whyNow || "").trim().length > 24,
      segmentStatus: segments.find((item) => item.segmentId === state.segment)?.status,
    });
  });
  const visible = scoring.filter((row) => row.customerVisible === true).slice(0, 8);
  const drawers = visible.map((opp) => {
    const enriched = enrichGdiOpportunityForCustomer(opp, { hotelId: identity.hotelId });
    const drawer = isGdiCustomerDrawerReady(enriched);
    return {
      opportunityId: opp.id || opp.opportunityId,
      ok: drawer.ok === true,
      failed: drawer.failed || [],
      hotelId: enriched.hotelId,
    };
  });
  const hotelFacing = states.filter((state) => state.hotelFacing).length;
  const storedVisible = states.filter((state) => state.storedCustomerVisible).length;
  const ledger = readJson(paidMultisegmentAuthorizationLedgerPath(), { entries: [] });
  const incurred = (ledger.entries || [])
    .filter((entry) => entry.hotelId === identity.hotelId && entry.status === "spent")
    .reduce((sum, entry) => sum + Number(entry.estimatedUsd || 0), 0);
  const coverageComplete = segments.every((item) => item.status === "EXHAUSTED");
  const commercialMet = hotelFacing >= COMMERCIAL_TARGET_MIN;
  const deliveryCertified = false;
  const technicalComplete = identity.ok === true && local.foreign === 0 && (remote.foreign || 0) === 0;
  let serpApiCanary = null;
  if (opts.executeSerpApiCanary === true) {
    const { runAuthorizedGoogleDiscovery } = await import("./gdi-serpapi-google-discovery-v1.js");
    serpApiCanary = await runAuthorizedGoogleDiscovery({
      hotelId: identity.hotelId,
      config,
      gates,
    });
  } else if (opts.executeDemandPathways === true && Array.isArray(opts.demandPathwayQueries)) {
    const { runAuthorizedGoogleDiscovery } = await import("./gdi-serpapi-google-discovery-v1.js");
    serpApiCanary = await runAuthorizedGoogleDiscovery({
      hotelId: identity.hotelId,
      config,
      gates,
      queries: opts.demandPathwayQueries,
      maxSearches: opts.maxSearches,
      includePriorPages: false,
    });
  }
  let retainedInvestigation = null;
  let organizationReview = null;
  if (Array.isArray(opts.organizationEvents)) {
    const { reviewOrganizations } = await import("./gdi-organization-demand-v1.js");
    organizationReview = reviewOrganizations({
      events: opts.organizationEvents,
      territoryTerms: opts.territoryTerms || [],
      market: opts.market || null,
      language: opts.language || "en",
      territoryLabels: opts.territoryLabels || [],
    });
  }
  if (Array.isArray(opts.retainedLeads) && typeof opts.fetchOfficialPage === "function") {
    const { investigateRetainedLeads } = await import("./gdi-organization-investigation-v1.js");
    retainedInvestigation = await investigateRetainedLeads({
      leads: opts.retainedLeads,
      territoryTerms: opts.territoryTerms || [],
      fetchPage: opts.fetchOfficialPage,
      mode: executionMode,
      completedFingerprints: opts.completedFingerprints || [],
    });
  }
  return {
    ok: true,
    contract: GDI_AUTONOMOUS_HOTEL_FULL_E2E_V2,
    identity,
    executionMode,
    marketProfile,
    weekly,
    recovery,
    acquisition,
    corpusSource,
    reconciliation,
    localForeign: local.foreign,
    remote,
    segments,
    opportunityCount: states.length,
    hotelFacing,
    storedCustomerVisible: storedVisible,
    visibilityMismatch: states.filter((state) => !state.publicationEligibilityMatchesStoredVisibility).length,
    derivedSections: states.filter((state) => state.commercialSectionSource === "derived").length,
    storedSections: states.filter((state) => state.commercialSectionSource === "stored").length,
    sectionCounts: states.reduce((acc, state) => {
      const key = state.commercialSection || "UNSCOPED";
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {}),
    drawers,
    loops: loops.length,
    blockedPaidActions: loops.filter((item) =>
      item.journal.some((action) => action.blocker === "BLOCKED_AUTHORIZATION")
    ).length,
    historicalIncurredUsd: Number(incurred.toFixed(2)),
    gates,
    freshAuthorization: gates.research.paid === "AUTHORIZED",
    airtableWrites: 0,
    localWrites: 0,
    completion: {
      TECHNICAL_E2E_COMPLETE: technicalComplete,
      RESEARCH_COVERAGE_COMPLETE: coverageComplete,
      COMMERCIAL_TARGET_MET: commercialMet,
      CUSTOMER_DELIVERY_CERTIFIED: deliveryCertified,
      FULL_E2E_CERTIFIED: technicalComplete && coverageComplete && commercialMet && deliveryCertified,
    },
    serpApiCanary,
    retainedInvestigation,
    organizationReview,
  };
}

export function acquireHotelLock(lockDir) {
  fs.mkdirSync(path.dirname(lockDir), { recursive: true });
  try {
    fs.mkdirSync(lockDir);
    fs.writeFileSync(path.join(lockDir, "lock.json"), JSON.stringify({ at: new Date().toISOString() }));
    return { ok: true };
  } catch (error) {
    if (error.code === "EEXIST") return { ok: false, error: "HOTEL_LOCK_HELD" };
    throw error;
  }
}

export function releaseHotelLock(lockDir) {
  fs.rmSync(lockDir, { recursive: true, force: true });
}

export function writeCheckpoint(file, doc) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const next = { ...doc, version: (doc.version || 0) + 1, savedAt: new Date().toISOString() };
  fs.writeFileSync(file, JSON.stringify(next, null, 2));
  return next;
}

export function resumeCheckpoint(file) {
  return readJson(file, null);
}
