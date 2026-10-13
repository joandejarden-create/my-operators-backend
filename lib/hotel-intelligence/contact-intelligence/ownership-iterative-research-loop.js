/**
 * Bounded iterative ownership research loop for Dealality native discovery.
 *
 * Adapted (not copied wholesale) from dzhng/deep-research patterns:
 * recursive depth/breadth narrowing, query+goal pairs, follow-up generation,
 * visited-URL / finding dedupe, and concurrency limits.
 *
 * Upstream: https://github.com/dzhng/deep-research
 * Pinned commit: 1f8f3e285bbc23e80b98a66a64effab9069f3ad4
 * License: MIT (see THIRD_PARTY_NOTICES.ownership-iterative-research.md)
 *
 * Capability map (Firecrawl → Dealality):
 * - firecrawl.search(+scrape markdown) → contextDevSearch + contextDevScrapeMarkdown
 * - generateObject learnings → evidence-backed claims via ownership extractors;
 *   unattributed model "learnings" are stored only as hypotheses
 * - writeFinalReport / writeFinalAnswer → NOT adapted (we return the handoff contract)
 * - Firecrawl / ai-sdk / lodash / zod deps → NOT installed
 *
 * Never authorizes enrichment, publication, or canonical ownership writes.
 */
import {
  resolveOwnershipResearchStrategy,
  extractLegalEntityBundle,
  assessPropertyLineage,
  computeOwnershipResearchProgress,
  concludeFromEntityEvidence,
  planScrapeFailureRecovery,
  shouldScrapeForOwnership,
  selectUrlsForPaidScrape,
  buildSerpLeadFollowUpGoals,
  stageLegalEntityFromEvidence,
  buildExactAddressContinuationQueries,
  OWNERSHIP_CONCLUSION_STATE,
} from "./ownership-research-strategy/index.js";
import {
  ownershipQueries,
  ownershipSearchResults,
  rankOwnershipSources,
  rankOwnershipSourcesDetailed,
  ownershipSerpNeedsAltProviderFallback,
  ownershipDocumentPassages,
  ownershipCandidates,
} from "./ownership-research-planning.js";
import {
  mergeOwnershipClaimCandidates,
  buildQuestionDependentFollowUpPlan,
  resolveAcquisitionDirection,
  resolveOwnsDirection,
  repairOwnershipCurrentness,
  normalizeUnnamedFamilyClaim,
} from "./ownership-follow-up-research.js";
import {
  proposeFollowUpQueries,
  modelOwnershipDocumentArm,
  computeConservativeMaxCostUsd,
  OWNERSHIP_DOC_LOCKED_MODEL,
} from "./ownership-document-structured-extract.js";
import {
  createOwnershipStageTrace,
  pushStageEvent,
  normalizeOwnershipClaimContract,
  historicalClaimFollowUpQuestions,
  inferEarliestOwnershipFailureStage,
  sanitizeOwnershipStageTraceForFixture,
} from "./ownership-research-stage-trace.js";
import { CONTEXT_DEV_CREDIT_COSTS } from "../../context-dev/credit-ledger.js";
import { serpGoogle } from "./live-native-discovery.js";
import {
  createJournaledCaller,
  buildOperationWorkKey,
  pendingUnknownFromJournal,
  resultsFromJournal,
  completedKeysFromJournal,
  restoreBudgetUsageFromJournal,
  OP_STATUS,
} from "./research-operation-journal.js";
import {
  hydrateAdaptiveState,
  planAdaptiveContinuation,
  finishCurrentPlaybook,
  shouldDeferCaseTerminalForPlaybook,
  serializeAdaptiveForResume,
  refreshCandidatesAndVerify,
  PLAYBOOK_OUTCOME,
  STRUCTURED_REJECTION_REASON,
} from "./adaptive-ownership-controller/index.js";

/** Reject anonymous purchaser groups / chrome mistaken for owner display names. */
export function isImplausibleOwnerDisplayName(name) {
  const n = String(name || "").trim();
  if (n.length < 4 || n.length > 90) return true;
  if (/^(of|the|a|an|and|for|to|in|on|by)\b/i.test(n)) return true;
  if (/ownership\s+database|who\s+owns|verified\s+company|biggest\s+brands|cookie|privacy\s+policy/i.test(n)) {
    return true;
  }
  // Anonymous purchaser/investor groups are leads — not resolved hotel-owner entities.
  if (/^(investors?|buyers?|purchasers?|owners?|a\s+group(?:\s+of\s+investors?)?|unnamed(?:\s+\w+)?)$/i.test(n)) {
    return true;
  }
  return false;
}

/** True when state has a substantive ownership/registry lead that needs follow-up spend. */
export function hasActionableOwnershipFollowUp(state = {}) {
  for (const c of state.claims || []) {
    if (c.requires_currency_research) return true;
    if (c.currentness === "HISTORICAL" || String(c.currentness || "").includes("HISTORICAL")) return true;
    if (c.party_role === OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER) return true;
    if (c.relationship === "ACQUIRED" && c.currentness !== "CURRENT_AS_OF_STATED_DATE") return true;
    if (c.classification === "REGISTERED_BUSINESS" || c.party_role === OWNERSHIP_PARTY_ROLE.REGISTERED_BUSINESS) {
      return true;
    }
    if (c.historical_vs_current === "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK") return true;
  }
  for (const p of state.candidate_parties || []) {
    if (p.party_role === OWNERSHIP_PARTY_ROLE.REGISTERED_BUSINESS) return true;
    if (p.party_role === OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER) return true;
  }
  return false;
}

/** Strong SERP lead for the unresolved ownership question — never defer solely for reserve. */
export function isStrongOwnershipSerpLead(row = {}) {
  const reasons = row.rank_reasons || [];
  const score = Number(row.score) || 0;
  if (reasons.includes("IDENTITY_ONLY_TRAVEL_LISTING")) return false;
  if (reasons.includes("OWNERSHIP_LANGUAGE")) return true;
  if (reasons.includes("INVESTOR_OR_REGISTRY")) return true;
  if (reasons.includes("GOVERNMENT_OR_REGISTRY_HOST")) return true;
  if (reasons.includes("PERSON_OR_PRESS_WITH_HOTEL")) return true;
  if (reasons.includes("BIO_INTERVIEW_OR_HISTORY_CONTEXT")) return true;
  if (reasons.includes("FIRST_PARTY_OR_SPONSOR_SIGNAL") && score >= 4) return true;
  return score >= 5;
}

export function computeFollowUpReserveCredits(avail, bounds = {}, searchUnit = 1, scrapeUnit = 1) {
  const fraction = Number(bounds.follow_up_reserve_fraction ?? DEFAULT_BOUNDS.follow_up_reserve_fraction);
  if (!(avail > 0) || !(fraction > 0)) return 0;
  const fractional = Math.floor(avail * fraction);
  // Cover at least one follow-up search + one potential fetch when anything is reserved.
  const floor = searchUnit + scrapeUnit;
  return Math.min(avail, Math.max(fractional, floor));
}

export const OWNERSHIP_ITERATIVE_LOOP_VERSION = "ownership-iterative-research-loop-v1.1";

const OWNERSHIP_RELEVANT_RE =
  /\b(?:acquir|purchas|bought|owned|owner|propriet|family[- ]owned|investor|sponsor|sold|sale|divest|adquir|propiedad|proprietár)/i;
export const DEEP_RESEARCH_PROVENANCE = Object.freeze({
  upstream_repo: "https://github.com/dzhng/deep-research",
  pinned_commit: "1f8f3e285bbc23e80b98a66a64effab9069f3ad4",
  license: "MIT",
  package_json_license_field: "ISC",
  license_file: "MIT",
  adapted_components: [
    "deepResearch depth/breadth recursion",
    "generateSerpQueries (query + researchGoal)",
    "processSerpResult followUpQuestions",
    "visitedUrls / learnings dedupe across branches",
    "concurrency limit before parallel SERP work",
  ],
  not_adapted: [
    "Firecrawl SDK",
    "writeFinalReport / writeFinalAnswer",
    "ai-sdk generateObject as primary claim extractor",
    "unbounded learnings-as-facts",
  ],
  capability_mismatches: [
    {
      upstream: "Firecrawl search with inline markdown scrape",
      dealality: "Separate Context.dev search then scrape_markdown",
      note: "Two billable steps; reserve scrape allowance before search when possible",
    },
    {
      upstream: "Model-generated learnings drive next queries",
      dealality: "Evidence-backed claims + question-dependent plans drive follow-ups; model text is hypothesis-only",
    },
  ],
});

/** Party role vocabulary retained across the iterative state. */
export const OWNERSHIP_PARTY_ROLE = Object.freeze({
  PROPERTY_OWNER: "PROPERTY_OWNER",
  ECONOMIC_SPONSOR: "ECONOMIC_SPONSOR",
  OPERATOR: "OPERATOR",
  BRAND: "BRAND",
  REGISTERED_BUSINESS: "REGISTERED_BUSINESS",
  HISTORICAL_OWNER: "HISTORICAL_OWNER",
  UNRESOLVED: "UNRESOLVED",
});

const OWNERSHIP_LIKE = new Set([
  OWNERSHIP_PARTY_ROLE.PROPERTY_OWNER,
  OWNERSHIP_PARTY_ROLE.ECONOMIC_SPONSOR,
]);

const DEFAULT_BOUNDS = Object.freeze({
  breadth: 3,
  depth: 2,
  concurrency: 2,
  docs_per_query: 2,
  /** Fraction of remaining credits reserved for actionable follow-up search+fetch after leads exist. Released when no actionable follow-up. */
  follow_up_reserve_fraction: 0.4,
  max_queries_total: 12,
  max_documents_total: 10,
  /** Bounded SerpAPI Google fallback when Context SERP ranks lack ownership signal. Default off. */
  serpapi_max: 0,
  serpapi_usd_max: 0,
  serpapi_per_search_usd: 0.01,
});

/**
 * Shared budget controller: reserve before concurrent dispatch so branches
 * cannot overspend the same ledger.
 */
export function createSharedBudgetController(ledger) {
  let reserved = 0;
  let inFlight = 0;
  const reservations = [];

  function remainingAvailable() {
    return Math.max(0, Number(ledger.remaining()) - reserved);
  }

  function tryReserve(cost, meta = {}) {
    const c = Number(cost);
    if (!Number.isFinite(c) || c <= 0) return { ok: false, reason: "INVALID_RESERVE_COST" };
    if (remainingAvailable() < c) {
      return { ok: false, reason: "BUDGET_RESERVE_DENIED", remaining_available: remainingAvailable() };
    }
    reserved += c;
    inFlight += 1;
    const token = { cost: c, meta, at: new Date().toISOString() };
    reservations.push(token);
    return { ok: true, token };
  }

  function settle(token, kind, meta = {}) {
    if (!token) return { ok: false, reason: "NO_RESERVATION_TOKEN" };
    reserved = Math.max(0, reserved - Number(token.cost));
    inFlight = Math.max(0, inFlight - 1);
    const idx = reservations.indexOf(token);
    if (idx >= 0) reservations.splice(idx, 1);
    return ledger.charge(kind, token.cost, { ...token.meta, ...meta });
  }

  function release(token) {
    if (!token) return;
    reserved = Math.max(0, reserved - Number(token.cost));
    inFlight = Math.max(0, inFlight - 1);
    const idx = reservations.indexOf(token);
    if (idx >= 0) reservations.splice(idx, 1);
  }

  /** Keep credits reserved after uncertain throw — do not return them to the pool. */
  function markUnknown(token) {
    if (!token) return { ok: false };
    token.unknown = true;
    token.unknown_at = new Date().toISOString();
    // reserved stays counted; remove from active list tracking only via settle later
    return { ok: true, outstanding_credits: Number(token.cost) };
  }

  return {
    ledger,
    remainingAvailable,
    tryReserve,
    settle,
    release,
    markUnknown,
    get reserved() {
      return reserved;
    },
    get inFlight() {
      return inFlight;
    },
    snapshot() {
      return {
        ...ledger.snapshot(),
        reserved_credits: reserved,
        open_reservations: reservations.length,
        in_flight_calls: inFlight,
        unknown_reservations: reservations.filter((r) => r.unknown).length,
      };
    },
  };
}

/**
 * Separate model-dollar ledger (OpenAI reader / planner). Never shares Context.dev credits.
 */
export function createModelUsdLedger({ budgetUsd = 0, alreadySpent = 0, label = "ownership-iterative-model" } = {}) {
  let spent = Number(alreadySpent) || 0;
  let reserved = 0;
  const entries = [];
  const budget = Math.max(0, Number(budgetUsd) || 0);

  function remaining() {
    return Math.max(0, budget - spent - reserved);
  }

  function canAfford(cost) {
    return remaining() >= Number(cost);
  }

  function tryReserve(cost, meta = {}) {
    const c = Number(cost);
    if (!Number.isFinite(c) || c <= 0) return { ok: false, reason: "INVALID_RESERVE" };
    if (!canAfford(c)) return { ok: false, reason: "MODEL_USD_BUDGET_EXCEEDED", remaining: remaining() };
    reserved += c;
    return { ok: true, token: { cost: c, meta } };
  }

  function release(token) {
    if (!token) return;
    reserved = Math.max(0, reserved - Number(token.cost));
  }

  function charge(cost, meta = {}) {
    const c = Number(cost);
    if (!Number.isFinite(c) || c < 0) throw new Error("invalid_model_usd_cost");
    if (c > 0 && spent + c > budget + 1e-9) {
      return { ok: false, blocked: true, reason: "MODEL_USD_BUDGET_EXCEEDED", spent, remaining: remaining() };
    }
    spent += c;
    const entry = { at: new Date().toISOString(), cost: c, spent, remaining: remaining(), ...meta };
    entries.push(entry);
    return { ok: true, entry, spent, remaining: remaining() };
  }

  function settle(token, actualCost, meta = {}) {
    release(token);
    return charge(actualCost != null ? actualCost : token?.cost || 0, meta);
  }

  return {
    label,
    budgetUsd: budget,
    get spent() {
      return spent;
    },
    get reserved() {
      return reserved;
    },
    remaining,
    canAfford,
    tryReserve,
    release,
    charge,
    settle,
    snapshot() {
      return {
        label,
        budget_usd: budget,
        spent_usd: Number(spent.toFixed(6)),
        reserved_usd: Number(reserved.toFixed(6)),
        remaining_usd: Number(remaining().toFixed(6)),
        model: OWNERSHIP_DOC_LOCKED_MODEL,
        entries: entries.slice(),
      };
    },
  };
}

/**
 * When deterministic extraction needs the structured model reader.
 */
export function shouldInvokeStructuredReader(deterministicCandidates = [], documentText = "", hotel = {}) {
  const text = String(documentText || "");
  const ownershipRelevant = OWNERSHIP_RELEVANT_RE.test(text);
  const accepted = (deterministicCandidates || []).filter(
    (c) => c.classification !== "REJECTED_NEGATIVE_CONTROL" && (c.name || c.subject)
  );
  const reasons = [];
  if (accepted.length === 0 && ownershipRelevant) reasons.push("NO_CLAIM_DESPITE_OWNERSHIP_RELEVANT_CONTENT");
  if (
    accepted.some(
      (c) =>
        !c.relationship ||
        c.relationship === "OTHER" ||
        c.scope === "GENERAL_PORTFOLIO" ||
        c.lead_reasons?.includes("HOTEL_NAME_NOT_BOUND_IN_PASSAGE")
    )
  ) {
    reasons.push("AMBIGUOUS_RELATIONSHIP");
  }
  if (
    accepted.some(
      (c) =>
        c.relationship === "ACQUIRED" ||
        c.lead_reasons?.includes("TRANSACTION_ACQUISITION_LANGUAGE") ||
        String(c.currentness || "").includes("HISTORICAL")
    )
  ) {
    reasons.push("HISTORICAL_TRANSACTION_NEEDS_INVESTIGATION");
  }
  if (accepted.some((c) => c.classification === "FAMILY_OWNED_UNNAMED_LEAD" || c.relationship === "FAMILY_OWNS_UNNAMED")) {
    reasons.push("FAMILY_OWNERSHIP_CLUE");
  }
  return {
    invoke: reasons.length > 0,
    reasons,
    hotel_id: hotel.hotel_id || null,
    ownership_relevant: ownershipRelevant,
    deterministic_accepted_count: accepted.length,
  };
}

export function deterministicFollowUpsInadequate(state, detGoals = []) {
  if (!detGoals.length) return { inadequate: true, reason: "exhausted" };
  // Miss-recovery alone must not block the model planner while Context remains —
  // live Arm B showed miss-recovery goals returned as "adequate" and planner never ran.
  const onlyMissRecovery = detGoals.every((g) => g.evidence_gap === "initial_search_miss");
  if (onlyMissRecovery) {
    return { inadequate: true, reason: "miss_recovery_only_allows_model_reformulation" };
  }
  const hasHist = (state.claims || []).some(
    (c) =>
      c.party_role === OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER ||
      c.currentness === "HISTORICAL" ||
      (c.relationship === "ACQUIRED" && c.currentness !== "CURRENT_AS_OF_STATED_DATE") ||
      ((c.relationship === "OWNS" || c.relationship === "OWNED_BY") &&
        c.currentness !== "CURRENT_AS_OF_STATED_DATE")
  );
  if (hasHist && !detGoals.some((g) => /sold|sale|current|divest|ownership|owner/i.test(`${g.query} ${g.researchGoal}`))) {
    return { inadequate: true, reason: "historical_without_currentness_followup" };
  }
  const hasFamily = (state.claims || []).some(
    (c) => c.relationship === "FAMILY_OWNS_UNNAMED" || c.classification === "FAMILY_OWNED_UNNAMED_LEAD"
  );
  if (hasFamily && !detGoals.some((g) => /family|owner|propriet|named/i.test(`${g.query} ${g.researchGoal}`))) {
    return { inadequate: true, reason: "family_without_named_party_followup" };
  }
  return { inadequate: false, reason: null };
}

/**
 * Strip / quarantine instruction-like payloads from retrieved text before
 * they can influence research controls. Retrieved text is evidence only.
 */
export function sanitizeRetrievedTextForControls(raw, controls = {}) {
  const text = String(raw || "");
  const locked = freezeResearchControls(controls);
  const injectionPatterns = [
    /ignore\s+(all\s+)?(previous|prior)\s+instructions/gi,
    /set\s+(?:the\s+)?(?:budget|depth|breadth|concurrency)\s*(?:to|=)\s*\d+/gi,
    /enable\s+(?:enrichment|publication|canonical\s+writes?)/gi,
    /disable\s+(?:gates?|evidence\s+gates?|budget)/gi,
    /you\s+are\s+now\s+(?:authorized|allowed)\s+to/gi,
    /tool\s+permissions?\s*[:=]/gi,
  ];
  let cleaned = text;
  const hits = [];
  for (const re of injectionPatterns) {
    if (re.test(cleaned)) {
      hits.push(re.source);
      cleaned = cleaned.replace(re, "[REDACTED_UNTRUSTED_INSTRUCTION]");
    }
  }
  return {
    text: cleaned,
    injection_hits: hits,
    controls_unchanged: locked,
    controls_after: freezeResearchControls(controls),
    overrides_applied: false,
  };
}

export function freezeResearchControls(controls = {}) {
  return Object.freeze({
    breadth: Number(controls.breadth ?? DEFAULT_BOUNDS.breadth),
    depth: Number(controls.depth ?? DEFAULT_BOUNDS.depth),
    concurrency: Number(controls.concurrency ?? DEFAULT_BOUNDS.concurrency),
    budget_credits: Number(controls.budget_credits ?? 0),
    enrichment_authorized: false,
    publication_authorized: false,
    canonical_writes: false,
    allow_retrieved_text_to_change_controls: false,
  });
}

export function mapClaimToPartyRole(claim = {}) {
  const rel = String(claim.relationship || "").toUpperCase();
  const currentness = String(claim.currentness || "").toUpperCase();
  const subject = String(claim.corrected_subject || claim.subject || claim.name || "").trim();
  if (/^(investors?|buyers?|purchasers?|owners?|a\s+group(?:\s+of\s+investors?)?|unnamed(?:\s+\w+)?)$/i.test(subject)) {
    // Anonymous purchaser-group lead — not a resolved hotel-owner entity.
    return OWNERSHIP_PARTY_ROLE.UNRESOLVED;
  }
  if (rel === "OPERATES" || rel === "MANAGES") return OWNERSHIP_PARTY_ROLE.OPERATOR;
  if (rel === "BRANDS" || rel === "BRAND") return OWNERSHIP_PARTY_ROLE.BRAND;
  if (rel === "FAMILY_OWNS_UNNAMED") return OWNERSHIP_PARTY_ROLE.UNRESOLVED;
  if (rel === "PARENT_ACQUIRED" || claim.scope === "COMPANY") {
    return OWNERSHIP_PARTY_ROLE.REGISTERED_BUSINESS;
  }
  if (rel === "ACQUIRED" || rel === "OWNS" || rel === "OWNED_BY") {
    if (currentness === "HISTORICAL" || currentness.includes("HISTORICAL")) {
      return OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER;
    }
    if (currentness === "CURRENT_AS_OF_STATED_DATE" || currentness === "CURRENT") {
      const hasDate = Boolean(claim.event_date || claim.source_date);
      if (!hasDate) return OWNERSHIP_PARTY_ROLE.UNRESOLVED;
      if (claim.supported_transaction_claim || claim.evidence_grounded) {
        return OWNERSHIP_PARTY_ROLE.PROPERTY_OWNER;
      }
    }
    if (claim.supported_transaction_claim || claim.evidence_grounded) {
      return OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER;
    }
    return OWNERSHIP_PARTY_ROLE.UNRESOLVED;
  }
  if (rel === "SPONSORS" || rel === "INVESTS") return OWNERSHIP_PARTY_ROLE.ECONOMIC_SPONSOR;
  if (claim.classification === "REGISTERED_BUSINESS") return OWNERSHIP_PARTY_ROLE.REGISTERED_BUSINESS;
  return OWNERSHIP_PARTY_ROLE.UNRESOLVED;
}

export function createOwnershipResearchState(hotel = {}, opts = {}) {
  return {
    version: OWNERSHIP_ITERATIVE_LOOP_VERSION,
    hotel_identity: {
      hotel_id: hotel.hotel_id || null,
      hotel_name: hotel.hotel_name || null,
      aliases: hotel.aliases || [],
      city: hotel.city || null,
      country: hotel.country || null,
      language: hotel.language || null,
      official_website: hotel.official_website || hotel.website || null,
      unresolved_identity_issues: opts.unresolved_identity_issues || [],
    },
    candidate_parties: [],
    claims: [],
    hypotheses: [],
    historical_transactions: [],
    conflicts: [],
    unanswered_questions: [],
    queries_attempted: [],
    urls_read: [],
    stop_reasons: [],
    warnings: [],
    budget: {
      consumed: 0,
      remaining: Number(opts.budget_remaining ?? 0),
      reserved_for_follow_up: 0,
      serpapi_used: 0,
      serpapi_usd: 0,
    },
    completed_work_keys: new Set(opts.completed_work_keys || []),
    stage_trace: createOwnershipStageTrace(hotel),
    ranked_empty_because_filtered: false,
    enrichment_authorized: false,
    publication_authorized: false,
    canonical_writes: false,
  };
}

function claimKey(c) {
  return [
    String(c.subject || "").toLowerCase(),
    String(c.relationship || ""),
    String(c.event_date || ""),
    String(c.url || c.source_url || ""),
    String(c.evidence_span || c.excerpt || "").slice(0, 80),
  ].join("|");
}

function queryKey(q) {
  return String(q || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeCompletedKey(kind, value) {
  return `${kind}:${queryKey(value)}`;
}

/**
 * True when research objective has supported ownership-like evidence
 * (not operator/brand-only, not historical-only without currentness).
 */
export function isOwnershipObjectiveSupported(state) {
  return (state.claims || []).some((c) => {
    const role = c.party_role || mapClaimToPartyRole(c);
    if (!OWNERSHIP_LIKE.has(role)) return false;
    const est = c.target_hotel_link?.establishes_relationship_to_target_validated;
    return Boolean(c.evidence_grounded || c.supported_transaction_claim) && est !== false;
  });
}

export function detectConflictingClaims(claims = []) {
  const byHotel = claims.filter(
    (c) =>
      (c.relationship === "ACQUIRED" || c.relationship === "OWNS") &&
      (c.subject || c.corrected_subject)
  );
  const conflicts = [];
  for (let i = 0; i < byHotel.length; i++) {
    for (let j = i + 1; j < byHotel.length; j++) {
      const a = byHotel[i];
      const b = byHotel[j];
      const sa = String(a.corrected_subject || a.subject || "").toLowerCase();
      const sb = String(b.corrected_subject || b.subject || "").toLowerCase();
      if (sa && sb && sa !== sb) {
        conflicts.push({
          kind: "CONFLICTING_OWNER_OR_TRANSACTION",
          parties: [a.corrected_subject || a.subject, b.corrected_subject || b.subject],
          dates: [a.event_date || null, b.event_date || null],
          evidence_spans: [a.evidence_span || a.excerpt, b.evidence_span || b.excerpt],
          urls: [a.url || a.source_url, b.url || b.source_url],
          retained: true,
        });
      }
    }
  }
  return conflicts;
}

/**
 * Build initial SERP-style query goals from hotel identity (language-aware).
 * Pattern adapted from deep-research generateSerpQueries — deterministic here.
 */
export function generateInitialOwnershipQueryGoals(hotel = {}, breadth = 3) {
  const strategy = resolveOwnershipResearchStrategy(hotel);
  if (strategy && typeof strategy.generateEntryGoals === "function") {
    // Brazil V1: legal-entity ladder first; allow wider breadth for address/CNPJ coverage.
    const goals = strategy.generateEntryGoals(hotel, Math.max(breadth, 6));
    if (goals.length) return goals;
  }
  const qs = ownershipQueries(hotel).slice(0, Math.max(1, breadth));
  return qs.map((query) => ({
    query,
    researchGoal: "Find hotel-specific ownership, acquisition, or sponsor evidence with dates and passages",
    evidence_gap: "missing_ownership_evidence",
  }));
}

/**
 * Follow-up query goals from evidence gaps (adapted from processSerpResult follow-ups).
 */
export function generateFollowUpQueryGoals(state, hotel, breadth = 2) {
  const merged = { candidates: state.claims, hotel_name: hotel.hotel_name };
  const plans = buildQuestionDependentFollowUpPlan(merged, hotel);
  const proposed = proposeFollowUpQueries(state.claims, hotel);
  const goals = [];
  const seen = new Set(state.queries_attempted.map(queryKey));

  // Prefer SERP-lead / address-continuation pivots ahead of generic follow-ups.
  for (const g of state.serp_lead_follow_ups || []) {
    const k = queryKey(g.query);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    goals.push({
      query: g.query,
      researchGoal: g.researchGoal,
      evidence_gap: g.evidence_gap,
      ladder_rung: g.ladder_rung || null,
      from: g.from || "serp_lead_follow_up",
    });
  }
  for (const q of state.unanswered_questions || []) {
    if (!q.query || !/CNPJ|raz[aã]o|FII|QSA|s[oó]cio/i.test(q.query)) continue;
    const k = queryKey(q.query);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    goals.push({
      query: q.query,
      researchGoal: q.question || "Address / entity continuation",
      evidence_gap: q.evidence_gap || "missing_legal_entity",
      from: q.from || "unanswered_pivot",
    });
  }

  for (const plan of plans) {
    for (const q of plan.proposed_queries || []) {
      const k = queryKey(q);
      if (!k || seen.has(k)) continue;
      if ((plan.avoid_queries || []).some((a) => queryKey(a) === k)) continue;
      seen.add(k);
      goals.push({
        query: q,
        researchGoal: plan.unresolved_question,
        evidence_gap: plan.unresolved_question,
        lead: plan.lead || null,
      });
    }
  }
  for (const p of proposed) {
    const k = queryKey(p.proposed_query);
    if (!k || seen.has(k)) continue;
    seen.add(k);
    goals.push({
      query: p.proposed_query,
      researchGoal: p.question,
      evidence_gap: p.kind,
      lead: p.based_on || null,
    });
  }

  // Historical claim → always emit bounded successor / role questions as research goals.
  for (const c of state.claims || []) {
    const hist =
      c.party_role === OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER ||
      c.currentness === "HISTORICAL" ||
      (c.relationship === "ACQUIRED" && c.currentness !== "CURRENT_AS_OF_STATED_DATE") ||
      ((c.relationship === "OWNS" || c.relationship === "OWNED_BY") &&
        c.currentness !== "CURRENT_AS_OF_STATED_DATE");
    if (!hist) continue;
    const party = c.corrected_subject || c.subject || c.name || "the named party";
    const core = String(hotel.hotel_name || "")
      .replace(/,?\s+(?:an? |by )?(?:autograph collection|tapestry collection|lxr hotels|all[- ]inclusive|adults only).*$/i, "")
      .trim();
    for (const qText of historicalClaimFollowUpQuestions(c, hotel)) {
      const searchQ = /sold after|sold|sale/i.test(qText)
        ? `"${core}" "${party}" (sold OR sale OR divested OR "no longer" OR owner)`
        : /operating company|asset or only/i.test(qText)
          ? `"${core}" "${party}" (asset OR "operating company" OR owner OR "real estate")`
          : /owner, operator, brand/i.test(qText)
            ? `"${core}" "${party}" (owner OR operator OR brand OR sponsor OR manager)`
            : `"${core}" (owner OR "owned by" OR proprietor) (2020 OR 2021 OR 2022 OR 2023 OR 2024 OR 2025 OR 2026)`;
      const k = queryKey(searchQ);
      if (!k || seen.has(k)) continue;
      seen.add(k);
      goals.push({
        query: searchQ,
        researchGoal: qText,
        evidence_gap: "historical_currentness",
        lead: { subject: party, relationship: c.relationship, currentness: c.currentness },
      });
    }
  }

  // Initial miss recovery — revise query without inventing owners (alias / tighter location).
  if (!goals.length && !(state.claims || []).length) {
    for (const g of generateMissRecoveryQueryGoals(state, hotel, breadth)) {
      const k = queryKey(g.query);
      if (seen.has(k)) continue;
      seen.add(k);
      goals.push(g);
    }
  }

  return goals.slice(0, Math.max(1, breadth));
}

/**
 * When the first pass yields no claims, try a distinct revised query (not a loop of the same SERP).
 */
export function generateMissRecoveryQueryGoals(state, hotel = {}, breadth = 1) {
  const tried = new Set((state.queries_attempted || []).map(queryKey));
  const alts = [];
  for (const a of hotel.aliases || []) {
    const q = `"${a}" (owner OR acquired OR acquisition OR investor)`;
    if (!tried.has(queryKey(q))) {
      alts.push({
        query: q,
        researchGoal: "Retry ownership search with hotel alias after initial miss",
        evidence_gap: "initial_search_miss",
      });
    }
  }
  const core = String(hotel.hotel_name || "")
    .replace(/,?\s+(?:an? |by )?(?:autograph collection|tapestry collection|lxr hotels|all[- ]inclusive|adults only).*$/i, "")
    .trim();
  const loc = [hotel.city, hotel.country].filter(Boolean).join(" ");
  const revised = `"${core}" ${loc} (owner OR proprietor OR investor OR "acquired by" OR sale) -tripadvisor -booking`.trim();
  if (core && !tried.has(queryKey(revised))) {
    alts.push({
      query: revised,
      researchGoal: "Revised ownership query after empty or unuseful first pass",
      evidence_gap: "initial_search_miss",
    });
  }
  return alts.slice(0, Math.max(1, breadth));
}

function assignEvidenceId(state, claim, url) {
  const id = `ev_${(state.claims?.length || 0) + 1}_${queryKey(url || "x").slice(0, 24) || "local"}`;
  return id;
}

function pushClaimIntoState(state, claim, url, hotel) {
  let working = { ...claim };
  if (working.relationship === "OWNS" || working.relationship === "OWNED_BY") {
    const ownsDir = resolveOwnsDirection(
      working.evidence_span || working.excerpt,
      working.subject || working.name,
      working.object,
      hotel.hotel_name
    );
    if (ownsDir.fields_inverted_vs_evidence) {
      working = {
        ...working,
        subject: ownsDir.corrected_subject,
        name: ownsDir.corrected_subject,
        object: ownsDir.corrected_object,
        relationship: "OWNS",
        direction_repaired: true,
        owns_direction: ownsDir,
        named_parties: ownsDir.corrected_subject ? [ownsDir.corrected_subject] : working.named_parties,
      };
    } else if (!working.owns_direction) {
      working.owns_direction = ownsDir;
    }
  }
  working = repairOwnershipCurrentness(working);
  if (working.evidence_grounded == null) {
    working.evidence_grounded = Boolean(working.evidence_span || working.excerpt);
  }

  const role = working.party_role || mapClaimToPartyRole(working);
  const contract = normalizeOwnershipClaimContract(
    {
      ...working,
      party_role: role,
      url: working.url || url,
      source_url: working.source_url || url,
    },
    hotel
  );
  const normalized = {
    ...working,
    ...contract,
    party_role: role,
    url: working.url || url,
    source_url: working.source_url || url,
    evidence_id: working.evidence_id || assignEvidenceId(state, working, url),
    // Quotation grounding ≠ relationship correctness
    grounded_quote_is_not_relationship_proof: true,
    promotes_to_current_ownership: false,
    implies_current_ownership: false,
  };
  if (role === OWNERSHIP_PARTY_ROLE.UNRESOLVED && normalized.relationship === "FAMILY_OWNS_UNNAMED") {
    normalized.subject = "";
    normalized.name = "";
    normalized.named_parties = [];
  }
  if (role === OWNERSHIP_PARTY_ROLE.OPERATOR || role === OWNERSHIP_PARTY_ROLE.BRAND) {
    normalized.classification = role === OWNERSHIP_PARTY_ROLE.OPERATOR ? "OPERATOR" : "BRAND";
  }
  // Historical acquisition never auto-promotes to current ownership
  if (
    normalized.relationship === "ACQUIRED" &&
    normalized.currentness !== "CURRENT_AS_OF_STATED_DATE"
  ) {
    normalized.party_role =
      normalized.party_role === OWNERSHIP_PARTY_ROLE.PROPERTY_OWNER
        ? OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER
        : normalized.party_role || OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER;
    normalized.historical_vs_current = "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK";
  }
  if (
    (normalized.relationship === "OWNS" || normalized.relationship === "OWNED_BY") &&
    normalized.currentness === "HISTORICAL"
  ) {
    normalized.party_role = OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER;
    normalized.historical_vs_current =
      normalized.historical_vs_current || "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK";
  }

  const k = claimKey(normalized);
  if ((state.claims || []).some((c) => claimKey(c) === k)) return false;
  state.claims.push(normalized);

  if (normalized.subject || normalized.name) {
    state.candidate_parties.push({
      name: normalized.subject || normalized.name,
      party_role: normalized.party_role,
      relationship: normalized.relationship,
      url,
      evidence_id: normalized.evidence_id,
      provenance_method: normalized.method || normalized.provenance?.method || null,
      staging_only: normalized.party_role === OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER,
    });
  }
  if (
    normalized.relationship === "ACQUIRED" ||
    normalized.event_date ||
    (normalized.relationship === "OWNS" && normalized.currentness === "HISTORICAL")
  ) {
    state.historical_transactions.push({
      subject: normalized.corrected_subject || normalized.subject,
      object: normalized.object || hotel.hotel_name,
      event_date: normalized.event_date || null,
      relationship: normalized.relationship,
      party_role: normalized.party_role,
      url,
      evidence_span: normalized.evidence_span,
      evidence_id: normalized.evidence_id,
      currentness: normalized.currentness || "UNRESOLVED",
      source_date: normalized.source_date || null,
    });
  }

  // Stage historical claims with explicit next research questions (never silent promote).
  const needsCurrentnessFollowUp =
    normalized.party_role === OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER ||
    normalized.currentness === "HISTORICAL" ||
    normalized.currentness === "UNRESOLVED" ||
    (normalized.relationship === "ACQUIRED" &&
      normalized.currentness !== "CURRENT_AS_OF_STATED_DATE");
  if (needsCurrentnessFollowUp && normalized.evidence_grounded) {
    const qs = historicalClaimFollowUpQuestions(normalized, hotel);
    normalized.next_research_question = qs[0];
    for (const q of qs) {
      if (!state.unanswered_questions.some((u) => u.question === q)) {
        state.unanswered_questions.push({
          question: q,
          evidence_gap: "historical_currentness",
          evidence_ids: normalized.evidence_id ? [normalized.evidence_id] : [],
          staging_only: true,
        });
      }
    }
  }

  if (state.stage_trace) {
    pushStageEvent(state.stage_trace, {
      kind: "claim",
      stage: "OWNERSHIP_RELATIONSHIP_CLASSIFICATION",
      claim: {
        subject: normalized.subject,
        relationship: normalized.relationship,
        object: normalized.object,
        currentness: normalized.currentness,
        party_kind: normalized.party_kind,
        evidence_grounded: normalized.evidence_grounded,
        target_hotel_link_validated: normalized.target_hotel_link_validated,
        source_url: normalized.source_url,
        source_date: normalized.source_date,
      },
      rejected: false,
    });
  }

  return true;
}

/**
 * Deterministic extract + optional structured model reader (separate provenance).
 */
export async function ingestDocumentClaims(state, md, url, hotel, opts = {}) {
  const { modelUsdLedger = null, deps = {}, applyStructuredReader = true, calls = [] } = opts;
  const sanitized = sanitizeRetrievedTextForControls(md, {
    breadth: state._controls?.breadth,
    depth: state._controls?.depth,
    concurrency: state._controls?.concurrency,
    budget_credits: state.budget.remaining,
  });
  if (sanitized.injection_hits.length) {
    state.hypotheses.push({
      kind: "RETRIEVED_INSTRUCTION_ATTEMPT",
      url,
      hits: sanitized.injection_hits,
      note: "Ignored — retrieved text cannot change research controls",
      evidence_backed: false,
    });
  }

  const document = ownershipDocumentPassages(sanitized.text, hotel);
  const rawCandidates = ownershipCandidates(document, hotel).map((c) => {
    const family = normalizeUnnamedFamilyClaim(c);
    const dir =
      family.relationship === "ACQUIRED" || c.relationship === "ACQUIRED"
        ? resolveAcquisitionDirection(
            family.evidence_span || c.excerpt || "",
            family.subject || c.name,
            family.object || hotel.hotel_name,
            hotel.hotel_name
          )
        : null;
    const claim = {
      ...c,
      ...family,
      method: "DETERMINISTIC",
      provenance: { method: "DETERMINISTIC", source_url: url },
      subject: family.subject !== undefined ? family.subject : c.name || c.subject,
      name: family.subject || c.name,
      acquisition_direction: dir,
      url,
      source_url: url,
      evidence_span: c.excerpt || c.evidence_span,
      evidence_grounded: Boolean(c.excerpt || c.evidence_span),
      supported_transaction_claim: c.supported_transaction_claim ?? c.supported_ownership ?? false,
    };
    claim.party_role = mapClaimToPartyRole(claim);
    return claim;
  });

  const readerGate = shouldInvokeStructuredReader(rawCandidates, sanitized.text, hotel);
  let modelGrounded = [];
  let modelRetained = [];
  let readerMeta = { invoked: false, gate: readerGate };

  if (applyStructuredReader && readerGate.invoke) {
    const readerFn = deps.modelOwnershipDocumentArm || modelOwnershipDocumentArm;
    const est = computeConservativeMaxCostUsd({
      documents: [{ id: url, text: sanitized.text, characters: sanitized.text.length }],
      hardCapUsd: modelUsdLedger ? modelUsdLedger.remaining() : 0,
    });
    const needUsd = Number(est.total_max_usd || 0);
    const canPay = modelUsdLedger && needUsd > 0 && modelUsdLedger.canAfford(needUsd);

    if (!canPay && !deps.modelOwnershipDocumentArm) {
      readerMeta = {
        invoked: false,
        gate: readerGate,
        skipped_reason: modelUsdLedger ? "MODEL_USD_BUDGET" : "NO_MODEL_LEDGER",
      };
      calls.push({
        provider: "openai",
        kind: "structured_reader_skipped",
        url,
        ok: true,
        reasons: readerGate.reasons,
        skipped_reason: readerMeta.skipped_reason,
      });
    } else {
      const reserve = canPay ? modelUsdLedger.tryReserve(needUsd, { url, kind: "structured_reader" }) : { ok: true, token: null };
      if (!reserve.ok && !deps.modelOwnershipDocumentArm) {
        readerMeta = { invoked: false, gate: readerGate, skipped_reason: "MODEL_USD_RESERVE_DENIED" };
      } else {
        try {
          const arm = await readerFn({
            sourceText: sanitized.text,
            hotel: { ...hotel, source_url: url },
            applyModel: true,
          });
          readerMeta = { invoked: true, gate: readerGate, status: arm.status, cost_usd: arm.cost_usd || 0 };
          calls.push({
            provider: "openai",
            kind: "structured_reader",
            url,
            ok: arm.status === "EXECUTED",
            status: arm.status,
            cost_usd: arm.cost_usd || 0,
            reasons: readerGate.reasons,
            iterative: true,
          });
          if (arm.paid_call && modelUsdLedger) {
            modelUsdLedger.settle(reserve.token, Number(arm.cost_usd || 0), {
              url,
              kind: "structured_reader",
              status: arm.status,
            });
          } else if (reserve.token && modelUsdLedger) {
            modelUsdLedger.release(reserve.token);
          }
          if (arm.status === "EXECUTED") {
            modelGrounded = arm.grounded_claims || [];
            modelRetained = arm.retained_for_review || [];
            // Unsupported model subject names → hypotheses only
            for (const r of arm.rejected_claims || []) {
              const subj = r.claim?.subject;
              if (subj) {
                state.hypotheses.push({
                  kind: "MODEL_SUBJECT_HYPOTHESIS",
                  subject: subj,
                  reason: r.reason,
                  url,
                  evidence_backed: false,
                  note: "Model suggestion without admitted grounded claim — not an ownership claim",
                });
              }
            }
          } else if (arm.status === "FAILED" || String(arm.status || "").startsWith("BLOCKED")) {
            state.warnings = state.warnings || [];
            state.warnings.push(`STRUCTURED_READER_${arm.status}`);
            // Preserve prior evidence — do not clear claims; continue deterministic path
          }
        } catch (err) {
          if (reserve.token && modelUsdLedger) modelUsdLedger.release(reserve.token);
          readerMeta = { invoked: true, gate: readerGate, failed: true, error: String(err?.message || err) };
          calls.push({
            provider: "openai",
            kind: "structured_reader",
            url,
            ok: false,
            error: String(err?.message || err),
          });
          state.warnings = state.warnings || [];
          state.warnings.push("STRUCTURED_READER_FAILED");
        }
      }
    }
  }

  const merged = mergeOwnershipClaimCandidates({
    hotelName: hotel.hotel_name,
    deterministicCandidates: rawCandidates,
    modelGrounded,
    modelRetained,
    sourceMeta: { source_url: url, url },
  });

  let newCount = 0;
  for (const c of merged.candidates || []) {
    if (pushClaimIntoState(state, c, url, hotel)) newCount++;
  }
  // Preserve unanswered currentness from historical txs
  for (const tx of state.historical_transactions) {
    if (tx.currentness !== "CURRENT_AS_OF_STATED_DATE") {
      const q = `Current ownership of ${hotel.hotel_name} after ${tx.event_date || "acquisition"} by ${tx.subject || "unknown"}?`;
      if (!state.unanswered_questions.some((u) => u.question === q)) {
        state.unanswered_questions.push({
          question: q,
          evidence_gap: "historical_currentness",
          evidence_ids: tx.evidence_id ? [tx.evidence_id] : [],
        });
      }
    }
  }

  const conflicts = detectConflictingClaims(state.claims);
  for (const conf of conflicts) {
    const ck = JSON.stringify(conf.parties);
    if (!state.conflicts.some((x) => JSON.stringify(x.parties) === ck)) {
      state.conflicts.push(conf);
    }
  }

  return { document, newCount, sanitized, readerMeta, merged };
}

/**
 * Model proposes searches only — code enforces budgets/dedupe/tools.
 * Inject deps.proposeModelFollowUpSearches for offline tests.
 */
export async function proposeModelAssistedFollowUpSearches({
  state,
  hotel = {},
  maxProposals = 2,
  modelUsdLedger = null,
  deps = {},
  calls = [],
} = {}) {
  const language = hotel.language || "en";
  if (deps.proposeModelFollowUpSearches) {
    const proposed = await deps.proposeModelFollowUpSearches({ state, hotel, maxProposals, language });
    return normalizeModelSearchProposals(proposed, state, hotel, language, maxProposals);
  }

  if (!modelUsdLedger || !modelUsdLedger.canAfford(0.01)) {
    return { source: "model_skipped", goals: [], model_used: false, reason: "MODEL_USD_BUDGET" };
  }
  if (!process.env.OPENAI_API_KEY && !deps.callModelJson) {
    return { source: "model_skipped", goals: [], model_used: false, reason: "NO_OPENAI_KEY" };
  }

  const evidenceDigest = (state.claims || []).slice(0, 12).map((c) => ({
    evidence_id: c.evidence_id,
    subject: c.subject || c.name || null,
    relationship: c.relationship,
    currentness: c.currentness,
    event_date: c.event_date,
    party_role: c.party_role,
    url: c.url || c.source_url,
    span: String(c.evidence_span || c.excerpt || "").slice(0, 180),
  }));
  const unanswered = (state.unanswered_questions || []).slice(0, 6);
  const tried = state.queries_attempted || [];

  const system = `You propose ownership research search queries only. Never assert ownership as fact. Never invent URLs or owner names as evidence. Today is ${new Date().toISOString()}.`;
  const user = JSON.stringify({
    hotel: {
      name: hotel.hotel_name,
      aliases: hotel.aliases || [],
      city: hotel.city,
      country: hotel.country,
      language,
    },
    evidence_digest: evidenceDigest,
    unanswered_questions: unanswered,
    queries_already_attempted: tried,
    max_proposals: maxProposals,
    required_fields_per_proposal: [
      "unresolved_question",
      "evidence_ids_or_initial_search_miss",
      "query",
      "language",
      "resolving_finding",
    ],
  });

  const reserveCost = 0.02;
  const reserve = modelUsdLedger.tryReserve(reserveCost, { kind: "follow_up_planner" });
  if (!reserve.ok) {
    return { source: "model_skipped", goals: [], model_used: false, reason: "MODEL_USD_RESERVE_DENIED" };
  }

  try {
    const callFn = deps.callModelJson;
    let parsed;
    let costUsd = 0;
    if (callFn) {
      const out = await callFn({ system, user });
      parsed = out.parsed;
      costUsd = Number(out.cost_usd || 0);
    } else {
      // Minimal inline call — same endpoint pattern as ownership-document-structured-extract
      const res = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          model: OWNERSHIP_DOC_LOCKED_MODEL,
          temperature: 0.1,
          max_tokens: 800,
          response_format: { type: "json_object" },
          messages: [
            { role: "system", content: system },
            {
              role: "user",
              content: `${user}\n\nReturn JSON: { "searches": [ { "unresolved_question": "", "evidence_ids": [], "initial_search_miss": false, "query": "", "language": "${language}", "resolving_finding": "" } ] }`,
            },
          ],
        }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error?.message || `openai_${res.status}`);
      parsed = JSON.parse(json.choices?.[0]?.message?.content || "{}");
      const usage = json.usage || {};
      costUsd =
        (Number(usage.prompt_tokens || 0) / 1e6) * 0.15 + (Number(usage.completion_tokens || 0) / 1e6) * 0.6;
    }
    modelUsdLedger.settle(reserve.token, costUsd, { kind: "follow_up_planner" });
    calls.push({
      provider: "openai",
      kind: "follow_up_planner",
      ok: true,
      cost_usd: costUsd,
      iterative: true,
    });
    const normalized = normalizeModelSearchProposals(parsed, state, hotel, language, maxProposals);
    // Model-only names in proposals are hypotheses
    for (const g of normalized.goals) {
      state.hypotheses.push({
        kind: "MODEL_FOLLOW_UP_PROPOSAL",
        query: g.query,
        unresolved_question: g.researchGoal,
        evidence_backed: false,
        note: "Search proposal only — not an ownership claim",
      });
    }
    return { ...normalized, model_used: true, cost_usd: costUsd };
  } catch (err) {
    modelUsdLedger.release(reserve.token);
    calls.push({
      provider: "openai",
      kind: "follow_up_planner",
      ok: false,
      error: String(err?.message || err),
    });
    return {
      source: "model_failed",
      goals: [],
      model_used: true,
      error: String(err?.message || err),
      // Caller keeps prior evidence and may stop/fallback
    };
  }
}

function normalizeModelSearchProposals(proposed, state, hotel, language, maxProposals) {
  const rows = Array.isArray(proposed?.searches)
    ? proposed.searches
    : Array.isArray(proposed?.goals)
      ? proposed.goals
      : Array.isArray(proposed)
        ? proposed
        : [];
  const tried = new Set((state.queries_attempted || []).map(queryKey));
  const goals = [];
  for (const row of rows) {
    const query = String(row.query || "").trim();
    if (!query || tried.has(queryKey(query))) continue;
    const miss = Boolean(row.initial_search_miss) || row.evidence_ids_or_initial_search_miss === "initial_search_miss";
    const evidenceIds = Array.isArray(row.evidence_ids)
      ? row.evidence_ids
      : Array.isArray(row.evidence_ids_or_initial_search_miss)
        ? row.evidence_ids_or_initial_search_miss
        : [];
    if (!miss && !evidenceIds.length && !row.evidence_ids_or_initial_search_miss) continue;
    goals.push({
      query,
      researchGoal: String(row.unresolved_question || row.researchGoal || "").trim() || "Unresolved ownership evidence gap",
      evidence_gap: miss ? "initial_search_miss" : "model_assisted_follow_up",
      evidence_ids: miss ? [] : evidenceIds.map(String),
      initial_search_miss: miss,
      language: row.language || language,
      resolving_finding: String(row.resolving_finding || "").trim() || null,
      source: "model_proposal",
    });
    if (goals.length >= maxProposals) break;
  }
  return { source: "model", goals, model_used: true };
}

/**
 * Prefer deterministic follow-ups; escalate to bounded model planner when exhausted/inadequate.
 */
export async function resolveNextQueryGoals(state, hotel, breadth, opts = {}) {
  const { modelUsdLedger = null, deps = {}, calls = [] } = opts;
  const det = generateFollowUpQueryGoals(state, hotel, breadth);
  const adequacy = deterministicFollowUpsInadequate(state, det);
  if (!adequacy.inadequate && det.length) {
    return { goals: det, source: "deterministic", adequacy };
  }
  const model = await proposeModelAssistedFollowUpSearches({
    state,
    hotel,
    maxProposals: breadth,
    modelUsdLedger,
    deps,
    calls,
  });
  if (model.goals?.length) {
    return { goals: model.goals, source: model.source || "model", adequacy, model };
  }
  // Fallback: keep any deterministic miss-recovery even if labeled inadequate
  if (det.length) return { goals: det, source: "deterministic_fallback", adequacy, model };
  return { goals: [], source: "none", adequacy, model };
}

function pickOwnerCandidateFromState(state) {
  for (const c of state.claims) {
    const role = c.party_role || mapClaimToPartyRole(c);
    if (!OWNERSHIP_LIKE.has(role) && role !== OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER) continue;
    if (role === OWNERSHIP_PARTY_ROLE.OPERATOR || role === OWNERSHIP_PARTY_ROLE.BRAND) continue;
    const name = c.corrected_subject || c.subject || c.name;
    if (!name || isImplausibleOwnerDisplayName(name)) continue;
    if (c.relationship === "FAMILY_OWNS_UNNAMED") continue;
    return {
      name,
      party_role: role,
      claim: c,
      historical_vs_current:
        role === OWNERSHIP_PARTY_ROLE.HISTORICAL_OWNER
          ? "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK"
          : role === OWNERSHIP_PARTY_ROLE.PROPERTY_OWNER
            ? "CURRENT_AS_OF_STATED_DATE"
            : "UNRESOLVED",
    };
  }
  return null;
}

async function mapPool(items, concurrency, fn) {
  const results = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
    while (next < items.length) {
      const i = next++;
      results[i] = await fn(items[i], i);
    }
  });
  await Promise.all(workers);
  return results;
}

/**
 * Core iterative loop. Inject search/scrape for offline tests.
 *
 * @param {object} args
 * @param {object} args.hotel
 * @param {object} args.budgetController - from createSharedBudgetController
 * @param {object} args.deps
 * @param {Function} args.deps.search - async ({query, numResults}) => {ok, data, error}
 * @param {Function} args.deps.scrape - async ({url}) => {ok, data, error}
 * @param {Function} [args.deps.isConfigured]
 * @param {Function} [args.deps.onOperationCheckpoint]
 * @param {object} [args.bounds]
 * @param {object} [args.resumeState] - prior state to resume without duplicate paid work
 */
export async function runOwnershipIterativeResearchLoop({
  hotel = {},
  budgetController,
  modelUsdLedger = null,
  deps = {},
  bounds = {},
  resumeState = null,
  chargedContext = null,
} = {}) {
  const b = {
    ...DEFAULT_BOUNDS,
    apply_structured_reader: true,
    apply_model_follow_up_planner: true,
    model_usd_max: 0,
    adaptive_ownership_controller: false,
    ...bounds,
  };
  const adaptiveEnabled =
    b.adaptive_ownership_controller === true ||
    String(process.env.ADAPTIVE_OWNERSHIP_CONTROLLER || "") === "1";
  const controls = freezeResearchControls({
    ...b,
    budget_credits: budgetController?.ledger?.budgetCredits ?? 0,
  });
  const modelLedger =
    modelUsdLedger ||
    createModelUsdLedger({
      budgetUsd: Number(b.model_usd_max || 0),
      label: `iterative-model:${hotel.hotel_id || "hotel"}`,
    });
  const state = (() => {
    if (!resumeState) {
      return createOwnershipResearchState(hotel, {
        budget_remaining: budgetController?.remainingAvailable?.() ?? 0,
      });
    }
    // Always merge onto a full base so partial resume payloads (missing urls_read,
    // historical_transactions, etc.) cannot crash mid-loop after soft-stop clear.
    const fresh = createOwnershipResearchState(hotel, {
      budget_remaining: budgetController?.remainingAvailable?.() ?? 0,
    });
    const base = {
      ...fresh,
      ...resumeState,
      budget: {
        ...fresh.budget,
        ...(resumeState.budget || {}),
        remaining:
          resumeState.budget?.remaining ??
          budgetController?.remainingAvailable?.() ??
          fresh.budget.remaining,
      },
      hotel_identity: resumeState.hotel_identity || fresh.hotel_identity,
      urls_read: Array.isArray(resumeState.urls_read) ? resumeState.urls_read : fresh.urls_read,
      queries_attempted: Array.isArray(resumeState.queries_attempted)
        ? resumeState.queries_attempted
        : fresh.queries_attempted,
      claims: Array.isArray(resumeState.claims) ? resumeState.claims : fresh.claims,
      unanswered_questions: Array.isArray(resumeState.unanswered_questions)
        ? resumeState.unanswered_questions
        : fresh.unanswered_questions,
      stop_reasons: Array.isArray(resumeState.stop_reasons)
        ? resumeState.stop_reasons
        : fresh.stop_reasons,
      warnings: Array.isArray(resumeState.warnings) ? resumeState.warnings : fresh.warnings,
      conflicts: Array.isArray(resumeState.conflicts) ? resumeState.conflicts : fresh.conflicts,
      candidate_parties: Array.isArray(resumeState.candidate_parties)
        ? resumeState.candidate_parties
        : fresh.candidate_parties,
      hypotheses: Array.isArray(resumeState.hypotheses) ? resumeState.hypotheses : fresh.hypotheses,
      historical_transactions: Array.isArray(resumeState.historical_transactions)
        ? resumeState.historical_transactions
        : fresh.historical_transactions,
      adaptive: hydrateAdaptiveState(resumeState.adaptive),
    };
    return {
      ...base,
      completed_work_keys:
        resumeState.completed_work_keys instanceof Set
          ? resumeState.completed_work_keys
          : new Set(resumeState.completed_work_keys || base.completed_work_keys || []),
      pending_unknown_keys:
        resumeState.pending_unknown_keys instanceof Set
          ? resumeState.pending_unknown_keys
          : new Set(resumeState.pending_unknown_keys || []),
    };
  })();
  if (adaptiveEnabled) {
    state.adaptive = hydrateAdaptiveState(state.adaptive || resumeState?.adaptive);
    state.adaptive.enabled = true;
    // Soft stops from prior playbook-scoped runs must not block adaptive resume.
    const softStops = new Set([
      "NO_USEFUL_NEW_EVIDENCE",
      "LOOP_COMPLETE",
      "CONTINUATION_EXHAUSTED",
      "BUDGET_EXHAUSTED",
    ]);
    state.stop_reasons = (state.stop_reasons || []).filter((r) => !softStops.has(r));
  }
  if (!state.pending_unknown_keys) state.pending_unknown_keys = new Set();
  if (!state.budget) {
    state.budget = {
      consumed: 0,
      remaining: budgetController?.remainingAvailable?.() ?? 0,
      reserved_for_follow_up: 0,
      serpapi_used: 0,
      serpapi_usd: 0,
    };
  }
  const journalResults = new Map();
  if (Array.isArray(deps.operation_journal)) {
    for (const k of pendingUnknownFromJournal(deps.operation_journal)) {
      state.pending_unknown_keys.add(k);
    }
    for (const [k, v] of resultsFromJournal(deps.operation_journal)) {
      journalResults.set(k, v);
    }
    for (const k of completedKeysFromJournal(deps.operation_journal)) {
      state.completed_work_keys.add(k);
    }
  }
  if (deps.result_by_work_key) {
    for (const [k, v] of Object.entries(deps.result_by_work_key)) {
      journalResults.set(k, v);
    }
  }
  state._controls = controls;
  state.budget.model = state.budget.model || { consumed: 0, remaining: modelLedger.remaining() };

  const journaler = createJournaledCaller({
    onOperationCheckpoint: deps.onOperationCheckpoint,
    completedWorkKeys: state.completed_work_keys,
    pendingUnknownKeys: state.pending_unknown_keys,
    resultByWorkKey: journalResults,
    forceRetryUnknown: deps.forceRetryUnknown === true,
    budgetUsage: restoreBudgetUsageFromJournal(
      deps.budget_usage || resumeState?.budget_usage || {},
      deps.operation_journal || []
    ),
    preDispatchGate: deps.preDispatchGate || null,
    nowFn: deps.nowFn || (() => Date.now()),
  });

  const rawSearchFn = deps.search;
  const rawScrapeFn = deps.scrape;
  const searchFn = rawSearchFn
    ? deps.providersJournaled
      ? rawSearchFn
      : async (args) => {
        const workKey = buildOperationWorkKey({
          provider: "context_dev",
          operation: "search",
          query: args.query,
        });
        const outcome = await journaler.run(
          workKey,
          {
            provider: "context_dev",
            op: "search",
            query: args.query,
            estimated_credits:
              budgetController.ledger.estimateSearchCost?.(10) ?? CONTEXT_DEV_CREDIT_COSTS.search_per_10_results,
          },
          () => rawSearchFn(args)
        );
        if (outcome.skip) {
          if (outcome.reason === "IN_FLIGHT_UNKNOWN_PENDING_RECONCILE") {
            return { ok: false, skipped: true, reason: outcome.reason, data: [] };
          }
          if (outcome.result) return outcome.result;
          return { ok: false, skipped: true, reason: outcome.reason, data: { results: [] } };
        }
        return outcome.result;
      }
    : null;
  const scrapeFn = rawScrapeFn
    ? deps.providersJournaled
      ? rawScrapeFn
      : async (args) => {
        const workKey = buildOperationWorkKey({
          provider: "context_dev",
          operation: "scrape",
          url: args.url,
        });
        const outcome = await journaler.run(
          workKey,
          {
            provider: "context_dev",
            op: "scrape",
            url: args.url,
            estimated_credits: CONTEXT_DEV_CREDIT_COSTS.scrape_markdown,
          },
          () => rawScrapeFn(args)
        );
        if (outcome.skip) {
          if (outcome.reason === "IN_FLIGHT_UNKNOWN_PENDING_RECONCILE") {
            return { ok: false, skipped: true, reason: outcome.reason, data: "" };
          }
          if (outcome.result) return outcome.result;
          return { ok: false, skipped: true, reason: outcome.reason, data: "" };
        }
        return outcome.result;
      }
    : null;
  const isConfigured = deps.isConfigured || (() => true);
  const calls = [];
  const sources = [];

  if (!searchFn || !scrapeFn) {
    state.stop_reasons.push("MISSING_SEARCH_OR_SCRAPE_DEPS");
    return finalizeLoopResult(state, calls, sources, controls, modelLedger, hotel);
  }
  if (!isConfigured()) {
    state.stop_reasons.push("PROVIDER_NOT_CONFIGURED");
    return finalizeLoopResult(state, calls, sources, controls, modelLedger, hotel);
  }

  const searchCost = () =>
    budgetController.ledger.estimateSearchCost?.(10) ?? CONTEXT_DEV_CREDIT_COSTS.search_per_10_results;
  const scrapeCost = CONTEXT_DEV_CREDIT_COSTS.scrape_markdown;

  // Recompute remaining from live controller — never trust a stale remaining=0 snapshot.
  state.budget.remaining = budgetController.remainingAvailable();
  state.budget.consumed = budgetController.ledger?.spent ?? state.budget.consumed ?? 0;

  /** Hard stops that must not be cleared for continuation. */
  const HARD_TERMINAL_STOPS = new Set([
    "DEPTH_LIMIT",
    "OBJECTIVE_SUPPORTED",
    "MAX_QUERIES_REACHED",
    "MISSING_SEARCH_OR_SCRAPE_DEPS",
    "PROVIDER_NOT_CONFIGURED",
  ]);

  /** Run-local stops that may be reassessed when budget + unattempted leads remain. */
  const SOFT_CONTINUATION_STOPS = new Set([
    "NO_USEFUL_NEW_EVIDENCE",
    "LOOP_COMPLETE",
    "CONTINUATION_EXHAUSTED",
    "BUDGET_EXHAUSTED",
  ]);

  function clearSoftBudgetStops() {
    state.stop_reasons = (state.stop_reasons || []).filter((r) => r !== "BUDGET_EXHAUSTED");
  }

  function hasHardTerminalStop() {
    return (state.stop_reasons || []).some((r) => HARD_TERMINAL_STOPS.has(r));
  }

  function filterUnattemptedGoals(goals) {
    return (goals || []).filter((g) => {
      const k = queryKey(g.query);
      if (!k) return false;
      if ((state.queries_attempted || []).includes(k)) return false;
      const ck = normalizeCompletedKey("search", k);
      if (state.completed_work_keys.has(ck)) return false;
      return true;
    });
  }

  /**
   * Playbook-scoped NO_USEFUL_NEW_EVIDENCE: finish current playbook and route to
   * next untried strategy when adaptive controller is enabled.
   * Does NOT mean the case is terminal.
   */
  function tryAdaptivePlaybookPivot(rejectionReason = STRUCTURED_REJECTION_REASON.NO_USEFUL_NEW_EVIDENCE) {
    if (!adaptiveEnabled || !state.adaptive) return { pivoted: false, goals: [] };
    if (!shouldDeferCaseTerminalForPlaybook(state.adaptive)) {
      state.adaptive = finishCurrentPlaybook(
        state.adaptive,
        PLAYBOOK_OUTCOME.NO_USEFUL_NEW_EVIDENCE
      );
      state.adaptive.research_path_exhausted = true;
      return { pivoted: false, goals: [], exhausted: true };
    }
    state.adaptive = finishCurrentPlaybook(
      state.adaptive,
      PLAYBOOK_OUTCOME.NO_USEFUL_NEW_EVIDENCE
    );
    const plan = planAdaptiveContinuation(state, hotel, {
      rejection_reason: rejectionReason,
    });
    state.adaptive = plan.adaptive;
    const goals = filterUnattemptedGoals(plan.goals || []);
    if (plan.allowed && goals.length) {
      state.stop_reasons = (state.stop_reasons || []).filter(
        (r) => r !== "NO_USEFUL_NEW_EVIDENCE" && r !== "CONTINUATION_EXHAUSTED"
      );
      return { pivoted: true, goals, playbook_id: plan.playbook_id };
    }
    return { pivoted: false, goals: [], exhausted: plan.adaptive?.research_path_exhausted };
  }

  /**
   * Reassess prior soft stops for authorized continuation.
   * Preserves stop history; clears soft stops only when budget remains and
   * at least one justified unattempted lead exists.
   */
  function reassessContinuationEligibility(candidateGoals) {
    const priorStops = [...(state.stop_reasons || [])];
    state.stop_history = state.stop_history || [];
    if (priorStops.length) {
      state.stop_history.push({
        at: new Date().toISOString(),
        stops: priorStops,
        phase: "pre_continuation_reassessment",
      });
    }

    const hard = priorStops.filter((r) => HARD_TERMINAL_STOPS.has(r));
    if (hard.length) {
      state.continuation_decision = {
        allowed: false,
        reason: "HARD_STOP_RETAINS",
        hard_stops: hard,
        remaining_budget: budgetController.remainingAvailable(),
      };
      return { allowed: false, goals: [] };
    }

    const remaining = budgetController.remainingAvailable();
    if (remaining < searchCost() + scrapeCost) {
      state.continuation_decision = {
        allowed: false,
        reason: "BUDGET_INSUFFICIENT_FOR_CONTINUATION",
        remaining_budget: remaining,
        required: searchCost() + scrapeCost,
      };
      if (!priorStops.includes("BUDGET_EXHAUSTED")) {
        state.stop_reasons = [...priorStops, "BUDGET_EXHAUSTED"];
      }
      return { allowed: false, goals: [] };
    }

    const goals = filterUnattemptedGoals(candidateGoals);
    // Country strategy: address-based legal-entity leads justify continuation past soft stops.
    const strategy = resolveOwnershipResearchStrategy(hotel);
    if (
      !goals.length &&
      strategy &&
      typeof strategy.hasUnattemptedAddressLegalEntityLead === "function" &&
      strategy.hasUnattemptedAddressLegalEntityLead(hotel, state.queries_attempted || [])
    ) {
      // Full legal-entity/address set — never truncate via generateEntryGoals(breadth).
      const entryPool =
        typeof strategy.buildAddressLegalEntityContinuationGoals === "function"
          ? strategy.buildAddressLegalEntityContinuationGoals(hotel, {
              queries_attempted: state.queries_attempted || [],
              completed_work_keys: state.completed_work_keys || new Set(),
              queryKeyFn: queryKey,
              normalizeCompletedKeyFn: normalizeCompletedKey,
            })
          : typeof strategy.buildLegalEntityQueries === "function"
            ? strategy.buildLegalEntityQueries(hotel).map((query, i) => ({
                query,
                researchGoal: "Resolve legal entity (CNPJ / razão social) for this exact property",
                evidence_gap: "missing_legal_entity",
                ladder_rung: "LEGAL_ENTITY_DISCOVERY",
                address_based:
                  Boolean(hotel.address || hotel.street_address) &&
                  /CNPJ/i.test(query) &&
                  String(query)
                    .toLowerCase()
                    .includes(
                      String(hotel.address || hotel.street_address || "")
                        .toLowerCase()
                        .slice(0, 12)
                    ),
                from: "address_legal_entity_continuation",
                strategy_id: strategy.id || null,
                _i: i,
              }))
            : typeof strategy.generateEntryGoals === "function"
              ? strategy.generateEntryGoals(hotel, 12)
              : [];
      const addrGoals =
        typeof strategy.buildAddressLegalEntityContinuationGoals === "function"
          ? entryPool
          : filterUnattemptedGoals(
              entryPool.filter(
                (g) =>
                  g.address_based === true ||
                  (hotel.address &&
                    /CNPJ/i.test(g.query || "") &&
                    String(g.query || "")
                      .toLowerCase()
                      .includes(String(hotel.address).toLowerCase().slice(0, 12)))
              )
            );
      if (addrGoals.length) {
        state.stop_reasons = priorStops.filter((r) => !SOFT_CONTINUATION_STOPS.has(r));
        state.continuation_decision = {
          allowed: true,
          reason: "ADDRESS_LEGAL_ENTITY_LEAD_REMAINS",
          remaining_budget: remaining,
          cleared_soft_stops: priorStops.filter((r) => SOFT_CONTINUATION_STOPS.has(r)),
          goal_count: addrGoals.length,
          first_goal_query: addrGoals[0]?.query || null,
        };
        return { allowed: true, goals: addrGoals };
      }
    }
    if (!goals.length) {
      state.continuation_decision = {
        allowed: false,
        reason: "NO_UNATTEMPTED_JUSTIFIED_LEADS",
        remaining_budget: remaining,
        exhausted_query_families: (state.queries_attempted || []).slice(0, 20),
        pending_questions: (state.unanswered_questions || []).map((q) => q.question).slice(0, 10),
      };
      // Keep soft stop truthfully if no leads remain
      const softKept = priorStops.filter((r) => SOFT_CONTINUATION_STOPS.has(r));
      state.stop_reasons = softKept.length
        ? softKept
        : ["CONTINUATION_EXHAUSTED"];
      return { allowed: false, goals: [] };
    }

    // Clear soft stops for this continuation; preserve history above.
    state.stop_reasons = (state.stop_reasons || []).filter(
      (r) => !SOFT_CONTINUATION_STOPS.has(r) && r !== "BUDGET_EXHAUSTED"
    );
    state.continuation_decision = {
      allowed: true,
      reason: "UNATTEMPTED_LEADS_WITH_BUDGET",
      remaining_budget: remaining,
      cleared_stops: priorStops.filter((r) => SOFT_CONTINUATION_STOPS.has(r)),
      goal_count: goals.length,
    };
    return { allowed: true, goals };
  }

  function hasTerminalStop() {
    // Soft stops abort the current depth path only when newly asserted mid-run;
    // pre-existing soft stops are cleared by reassessContinuationEligibility.
    return (state.stop_reasons || []).some(
      (r) => HARD_TERMINAL_STOPS.has(r) || SOFT_CONTINUATION_STOPS.has(r)
    );
  }

  async function runDepth(depth, breadth, queryGoals) {
    if (hasTerminalStop()) return;
    // Soft BUDGET_EXHAUSTED from a prior scrape race is cleared when enough remains for follow-up.
    if ((state.stop_reasons || []).includes("BUDGET_EXHAUSTED")) {
      if (budgetController.remainingAvailable() >= searchCost() + scrapeCost) {
        clearSoftBudgetStops();
      } else {
        return;
      }
    }
    if (depth <= 0) {
      state.stop_reasons.push("DEPTH_LIMIT");
      return;
    }
    if (isOwnershipObjectiveSupported(state)) {
      state.stop_reasons.push("OBJECTIVE_SUPPORTED");
      return;
    }
    // V2.2: evidence-gap may extend query cap by a small attributed budget
    if (state.evidence_gap_objective && state._queries_at_gap_inject == null) {
      state._queries_at_gap_inject = state.queries_attempted.length;
    }
    const gapBudget = Number(
      state.evidence_gap_query_budget || state.evidence_gap_objective?.max_expected_value || 0
    );
    const effectiveQueryCap = state.evidence_gap_objective
      ? Math.max(
          b.max_queries_total,
          Number(state._queries_at_gap_inject || 0) + Math.max(gapBudget, 6)
        )
      : b.max_queries_total;
    if (state.queries_attempted.length >= effectiveQueryCap) {
      state.stop_reasons.push("MAX_QUERIES_REACHED");
      return;
    }

    const avail = budgetController.remainingAvailable();
    // Reserve only when substantive ownership/registry leads need follow-up search+fetch.
    // Release (0) when no actionable follow-up exists — do not starve initial strong SERP hits.
    const actionableFollowUp = hasActionableOwnershipFollowUp(state);
    const followReserve = actionableFollowUp
      ? computeFollowUpReserveCredits(avail, b, searchCost(), scrapeCost)
      : 0;
    state.budget.reserved_for_follow_up = followReserve;
    state.budget.follow_up_reserve_active = Boolean(followReserve > 0);

    const goals = (queryGoals || []).filter((g) => {
      const k = queryKey(g.query);
      if (!k) return false;
      if (state.queries_attempted.includes(k)) return false;
      const ck = normalizeCompletedKey("search", k);
      if (state.completed_work_keys.has(ck)) return false;
      return true;
    });

    if (!goals.length) {
      const pivot = tryAdaptivePlaybookPivot();
      if (pivot.pivoted) {
        await runDepth(depth, breadth, pivot.goals);
        return;
      }
      state.stop_reasons.push(
        pivot.exhausted
          ? STRUCTURED_REJECTION_REASON.RESEARCH_PATH_EXHAUSTED
          : "NO_USEFUL_NEW_EVIDENCE"
      );
      return;
    }

    let newEvidenceThisRound = 0;

    await mapPool(goals.slice(0, breadth), b.concurrency, async (goal) => {
      if (state.queries_attempted.length >= effectiveQueryCap) return;
      if (isOwnershipObjectiveSupported(state)) return;

      const qk = queryKey(goal.query);
      const completedSearch = normalizeCompletedKey("search", qk);
      if (state.completed_work_keys.has(completedSearch)) {
        calls.push({
          provider: "context_dev",
          kind: "search_skipped_resume",
          query: goal.query,
          ok: true,
        });
        return;
      }

      const sc = searchCost();
      // Protect follow-up runway only after actionable leads exist. Before that, full remaining
      // is spendable so strong ownership SERP hits are not deferred for an empty reserve.
      const spendable =
        followReserve > 0
          ? Math.max(0, budgetController.remainingAvailable() - followReserve)
          : budgetController.remainingAvailable();
      if (spendable < sc) {
        // Round-local only — do not sticky-stop the loop while followReserve remains.
        if (budgetController.remainingAvailable() < sc) {
          state.stop_reasons.push("BUDGET_EXHAUSTED");
        }
        return;
      }

      let searchResult;
      try {
        if (chargedContext) {
          // chargedContext performs tryReserve → settle/release against budgetController.
          searchResult = await chargedContext(
            budgetController.ledger,
            "search",
            sc,
            { query: goal.query, phase: "iterative_loop", evidence_gap: goal.evidence_gap },
            () => searchFn({ query: goal.query, numResults: 10 })
          );
          if (searchResult?.reservation_denied || searchResult?.budget_blocked) {
            if (budgetController.remainingAvailable() < sc) {
              state.stop_reasons.push("BUDGET_EXHAUSTED");
            }
            return;
          }
        } else {
          const searchReserve = budgetController.tryReserve(sc, {
            query: goal.query,
            phase: "iterative_search",
          });
          if (!searchReserve.ok) {
            if (budgetController.remainingAvailable() < sc) {
              state.stop_reasons.push("BUDGET_EXHAUSTED");
            }
            return;
          }
          try {
            const raw = await searchFn({ query: goal.query, numResults: 10 });
            budgetController.settle(searchReserve.token, "search", {
              query: goal.query,
              evidence_gap: goal.evidence_gap,
            });
            searchResult = raw;
          } catch (err) {
            budgetController.release(searchReserve.token);
            throw err;
          }
        }
      } catch (err) {
        calls.push({
          provider: "context_dev",
          kind: "search",
          query: goal.query,
          ok: false,
          error: String(err?.message || err),
        });
        return;
      }

      state.queries_attempted.push(qk);
      state.completed_work_keys.add(completedSearch);
      state.unanswered_questions.push({
        question: goal.researchGoal,
        evidence_gap: goal.evidence_gap,
        query: goal.query,
        evidence_ids: goal.evidence_ids || [],
        resolving_finding: goal.resolving_finding || null,
      });

      const mapped = searchResult?.ok !== false ? ownershipSearchResults(searchResult.data ?? searchResult) : null;
      calls.push({
        provider: "context_dev",
        kind: "search",
        query: goal.query,
        ok: searchResult?.ok !== false && mapped !== null,
        result_count: mapped?.length ?? null,
        evidence_gap: goal.evidence_gap,
        iterative: true,
      });
      if (state.stage_trace) {
        pushStageEvent(state.stage_trace, {
          kind: "search",
          stage: "OWNERSHIP_SOURCE_RETRIEVAL",
          query: goal.query,
          language: hotel.language || "en",
          provider: "context_dev",
          request_parameters: { numResults: 10, evidence_gap: goal.evidence_gap },
          raw_result_count: mapped?.length ?? 0,
          ok: searchResult?.ok !== false && mapped !== null,
          remaining_budget: budgetController.remainingAvailable(),
        });
      }
      if (!mapped) return;

      sources.push({
        kind: "context_ownership_search_iterative",
        query: goal.query,
        research_goal: goal.researchGoal,
        evidence_gap: goal.evidence_gap,
        results: mapped,
      });

      let workingMapped = mapped;
      let rankDetail = rankOwnershipSourcesDetailed(workingMapped, hotel, {
        researchGoal: goal.researchGoal,
        evidence_gap: goal.evidence_gap,
      });
      // Brazil V1.1 hard scrape-selection gate: goal-aware; suppress OTA when high-value exists.
      let scrapeGate = selectUrlsForPaidScrape(rankDetail.ranked, {
        hotel,
        researchGoal: goal.researchGoal,
        evidence_gap: goal.evidence_gap,
      });
      let ranked = scrapeGate.selected
        .filter((r) => !(state.urls_read || []).includes(r.url))
        .map((r) => {
          const gate =
            r.ownership_scrape_gate ||
            shouldScrapeForOwnership(r, {
              hotel_address: hotel.address || hotel.street_address || "",
            });
          r.ownership_scrape_gate = gate;
          r.selection_reason =
            r.scrape_evaluation?.scrape_decision_reason || gate.reason;
          r.scrape_evaluation = r.scrape_evaluation || null;
          return r;
        });
      state.scrape_selection_trace = state.scrape_selection_trace || [];
      state.scrape_selection_trace.push({
        query: goal.query,
        research_goal_kind: scrapeGate.research_goal_kind,
        high_value_unsatisfied_exists: scrapeGate.high_value_unsatisfied_exists,
        selected_urls: ranked.map((r) => r.url),
        rejected: (scrapeGate.rejected || []).map((r) => ({
          url: r.url,
          reason: r.scrape_evaluation?.scrape_decision_reason,
          source_class: r.scrape_evaluation?.source_class,
        })),
      });
      // SERP snippet → exact CNPJ / FII / legal-entity pivots (no scrape required).
      const serpPivots = buildSerpLeadFollowUpGoals(workingMapped, hotel);
      if (serpPivots.length) {
        state.serp_lead_follow_ups = state.serp_lead_follow_ups || [];
        for (const g of serpPivots) {
          state.serp_lead_follow_ups.push(g);
          if (!(state.unanswered_questions || []).some((q) => q.query === g.query)) {
            state.unanswered_questions = state.unanswered_questions || [];
            state.unanswered_questions.push({
              question: g.researchGoal,
              evidence_gap: g.evidence_gap,
              query: g.query,
              from: g.from,
            });
          }
        }
      }
      // Parse SERP snippets for legal-entity leads even before scrape (Brazil ladder).
      for (const hit of workingMapped || []) {
        const blob = `${hit.title || ""} ${hit.snippet || ""}`;
        if (!/\bCNPJ\b|raz[aã]o\s+social|s[oó]cio|administradora|FII\b/i.test(blob)) continue;
        const bundle = extractLegalEntityBundle(blob, { source_url: hit.url });
        const staged = stageLegalEntityFromEvidence({
          hotel,
          text: blob,
          source_url: hit.url,
          bundle,
        });
        if (bundle.primary_cnpj || bundle.legal_name || bundle.principals.length || staged.fii_lead) {
          state.legal_entity_leads = state.legal_entity_leads || [];
          state.legal_entity_leads.push({
            ...bundle,
            ...staged,
            from: "serp_snippet",
            query: goal.query,
          });
          if (staged.staging_conclusion !== OWNERSHIP_CONCLUSION_STATE.UNRESOLVED) {
            newEvidenceThisRound += 1;
          }
          for (const q of staged.address_continuation_queries || []) {
            state.unanswered_questions = state.unanswered_questions || [];
            if (!state.unanswered_questions.some((x) => x.query === q)) {
              state.unanswered_questions.push({
                question: "Exact-address CNPJ after location conflict / ambiguity",
                evidence_gap: "missing_legal_entity",
                query: q,
                from: "identity_guard_address_pivot",
              });
            }
          }
        }
      }
      if (state.stage_trace) {
        pushStageEvent(state.stage_trace, {
          kind: "search",
          stage: "RESULT_RANKING_AND_FILTERING",
          query: goal.query,
          candidate_urls_before_filter: rankDetail.candidate_urls_before_filter,
          urls_removed: [
            ...(rankDetail.rejected || []).map((r) => ({
              url: r.url,
              reject_reason: r.reject_reason,
              score: r.score,
            })),
            ...(scrapeGate.rejected || []).map((r) => ({
              url: r.url,
              reject_reason: r.scrape_evaluation?.scrape_decision_reason,
              score: r.score,
              source_class: r.scrape_evaluation?.source_class,
            })),
          ],
          urls_kept: ranked.map((r) => ({
            url: r.url,
            score: r.score,
            rank_reasons: r.rank_reasons,
            scrape_priority: r.scrape_evaluation?.scrape_priority_score,
            source_class: r.scrape_evaluation?.source_class,
          })),
          raw_result_count: workingMapped.length,
          scrape_selection_version: scrapeGate.version,
        });
        if (workingMapped.length > 0 && ranked.length === 0) {
          state.ranked_empty_because_filtered = true;
        }
      }
      const altNeed = ownershipSerpNeedsAltProviderFallback(ranked);
      // Succession / named-owner queries are high-value for a single complementary SerpAPI pass
      // even when Context already returned a family-owned hotel page (Hughes-class pages often missing).
      const successionQuery = /family[- ]owned|proprietor|\bowners\b|fundador|heredero|herdeiro|shareholder/i.test(
        goal.query || ""
      );
      const serpMax = Number(b.serpapi_max || 0);
      const serpUsdMax = Number(b.serpapi_usd_max || 0);
      const serpUnit = Number(b.serpapi_per_search_usd || 0.01);
      const serpUsed = Number(state.budget.serpapi_used || 0);
      const serpUsd = Number(state.budget.serpapi_usd || 0);
      const serpKey = buildOperationWorkKey({
        provider: "serpapi",
        operation: "ownership_search_fallback",
        query: goal.query,
      });
      const canSerp =
        (altNeed.needed || successionQuery) &&
        serpMax > 0 &&
        serpUsed < serpMax &&
        (serpUsdMax <= 0 || serpUsd + serpUnit <= serpUsdMax + 1e-9) &&
        !state.completed_work_keys.has(serpKey);

      if (canSerp) {
        const serpFn = deps.serpGoogle || serpGoogle;
        const cost = { serpapi_searches: 0, serpapi_usd: 0 };
        try {
          // Reserve accounting before dispatch (same query string — no teacher URLs).
          const serpOutcome = await journaler.run(
            serpKey,
            { provider: "serpapi", op: "search", query: goal.query },
            async () => {
              state.budget.serpapi_used = serpUsed + 1;
              state.budget.serpapi_usd = Number((serpUsd + serpUnit).toFixed(4));
              return serpFn(goal.query, cost, {
                hl: hotel.language === "pt" ? "pt" : "en",
                gl: hotel.country === "Brazil" ? "br" : "us",
                num: 10,
              });
            }
          );
          if (serpOutcome.skip) {
            calls.push({
              provider: "serpapi",
              kind: "ownership_search_fallback_skipped_resume",
              query: goal.query,
              ok: true,
              skip_reason: serpOutcome.reason,
              replayed: Boolean(serpOutcome.result?.replayed),
            });
            if (serpOutcome.result?.organic?.length) {
              const organic = serpOutcome.result.organic.map((r) => ({
                title: r.title || "",
                url: r.link || r.url,
                snippet: r.snippet || "",
                provider: "serpapi",
                replayed: true,
              }));
              workingMapped = [...workingMapped, ...organic.filter((r) => /^https?:\/\//.test(r.url || ""))];
              rankDetail = rankOwnershipSourcesDetailed(workingMapped, hotel, {
                researchGoal: goal.researchGoal,
                evidence_gap: goal.evidence_gap,
              });
              scrapeGate = selectUrlsForPaidScrape(rankDetail.ranked, {
                hotel,
                researchGoal: goal.researchGoal,
                evidence_gap: goal.evidence_gap,
              });
              ranked = scrapeGate.selected.filter((r) => !state.urls_read.includes(r.url));
            }
          } else {
          const serp = serpOutcome.result;
          if (
            serpOutcome.restricted ||
            serp?.restricted ||
            String(serp?.usage_rights || "").toUpperCase() === "BLOCKED"
          ) {
            calls.push({
              provider: "serpapi",
              kind: "ownership_search_fallback",
              query: goal.query,
              ok: false,
              blocked: true,
              trigger: altNeed.needed ? altNeed.reason : "succession_query_complement",
              result_count: 0,
              iterative: true,
            });
          } else {
          const organic = (serp.organic || []).map((r) => ({
            title: r.title || "",
            url: r.link || r.url,
            snippet: r.snippet || r.description || "",
            provider: "serpapi",
          }));
          calls.push({
            provider: "serpapi",
            kind: "ownership_search_fallback",
            query: goal.query,
            ok: serp.ok !== false,
            trigger: altNeed.needed ? altNeed.reason : "succession_query_complement",
            result_count: organic.length,
            cost_usd: cost.serpapi_usd || serpUnit,
            iterative: true,
          });
          sources.push({
            kind: "serpapi_ownership_search_fallback",
            query: goal.query,
            trigger: altNeed.needed ? altNeed.reason : "succession_query_complement",
            results: organic,
          });
          // Merge Context + SerpAPI leads, then re-rank once.
          workingMapped = [...workingMapped, ...organic.filter((r) => /^https?:\/\//.test(r.url || ""))];
          rankDetail = rankOwnershipSourcesDetailed(workingMapped, hotel, {
            researchGoal: goal.researchGoal,
            evidence_gap: goal.evidence_gap,
          });
          scrapeGate = selectUrlsForPaidScrape(rankDetail.ranked, {
            hotel,
            researchGoal: goal.researchGoal,
            evidence_gap: goal.evidence_gap,
          });
          ranked = scrapeGate.selected.filter((r) => !state.urls_read.includes(r.url));
          if (state.stage_trace) {
            pushStageEvent(state.stage_trace, {
              kind: "search",
              stage: "RESULT_RANKING_AND_FILTERING",
              query: goal.query,
              provider: "context_dev+serpapi",
              candidate_urls_before_filter: rankDetail.candidate_urls_before_filter,
              urls_removed: (rankDetail.rejected || []).map((r) => ({
                url: r.url,
                reject_reason: r.reject_reason,
                score: r.score,
              })),
              urls_kept: ranked.map((r) => ({ url: r.url, score: r.score })),
              raw_result_count: workingMapped.length,
            });
            if (workingMapped.length > 0 && ranked.length === 0) {
              state.ranked_empty_because_filtered = true;
            }
          }
          }
          }
        } catch (err) {
          calls.push({
            provider: "serpapi",
            kind: "ownership_search_fallback",
            query: goal.query,
            ok: false,
            trigger: altNeed.needed ? altNeed.reason : "succession_query_complement",
            error: String(err?.message || err),
            in_flight_unknown: true,
          });
        }
      }

      for (const u of ranked.slice(0, b.docs_per_query)) {
        if (state.urls_read.length >= b.max_documents_total) break;
        const scrapeKey = normalizeCompletedKey("scrape", u.url);
        if (state.completed_work_keys.has(scrapeKey)) {
          calls.push({ provider: "context_dev", kind: "scrape_skipped_resume", url: u.url, ok: true });
          continue;
        }
        // Recompute reserve after prior ingest so new claims can protect follow-up runway.
        const reserveNow = hasActionableOwnershipFollowUp(state)
          ? computeFollowUpReserveCredits(
              budgetController.remainingAvailable(),
              b,
              searchCost(),
              scrapeCost
            )
          : 0;
        state.budget.reserved_for_follow_up = reserveNow;
        const strongLead = isStrongOwnershipSerpLead(u);
        // Never defer the strongest available ownership lead solely for reserve.
        // Weak/optional identity listings yield to reserve when actionable follow-ups exist.
        const scrapeSpendable = strongLead
          ? budgetController.remainingAvailable()
          : Math.max(0, budgetController.remainingAvailable() - reserveNow);
        if (scrapeSpendable < scrapeCost) {
          // Soft: stop scraping this goal; do not mark terminal BUDGET_EXHAUSTED if reserve remains.
          if (budgetController.remainingAvailable() < scrapeCost) {
            state.stop_reasons.push("BUDGET_EXHAUSTED");
          } else if (!strongLead && reserveNow > 0) {
            calls.push({
              provider: "context_dev",
              kind: "scrape_deferred_follow_up_reserve",
              url: u.url,
              ok: true,
              remaining_available: budgetController.remainingAvailable(),
              follow_up_reserve: reserveNow,
              deferred_because: "OPTIONAL_WEAK_FETCH_YIELDS_TO_ACTIONABLE_FOLLOW_UP",
              strong_ownership_lead: false,
            });
          } else {
            calls.push({
              provider: "context_dev",
              kind: "scrape_deferred_follow_up_reserve",
              url: u.url,
              ok: true,
              remaining_available: budgetController.remainingAvailable(),
              follow_up_reserve: reserveNow,
              deferred_because: "SPENDABLE_BELOW_SCRAPE_COST",
              strong_ownership_lead: strongLead,
            });
          }
          break;
        }
        let scraped;
        try {
          if (chargedContext) {
            scraped = await chargedContext(
              budgetController.ledger,
              "scrape_markdown",
              scrapeCost,
              { url: u.url, phase: "iterative_loop" },
              () => scrapeFn({ url: u.url })
            );
            if (scraped?.reservation_denied || scraped?.budget_blocked) {
              if (budgetController.remainingAvailable() < scrapeCost) {
                state.stop_reasons.push("BUDGET_EXHAUSTED");
              }
              break;
            }
          } else {
            const scrapeReserve = budgetController.tryReserve(scrapeCost, { url: u.url });
            if (!scrapeReserve.ok) {
              if (budgetController.remainingAvailable() < scrapeCost) {
                state.stop_reasons.push("BUDGET_EXHAUSTED");
              }
              break;
            }
            try {
              const raw = await scrapeFn({ url: u.url });
              budgetController.settle(scrapeReserve.token, "scrape_markdown", { url: u.url });
              scraped = raw;
            } catch (err) {
              budgetController.release(scrapeReserve.token);
              throw err;
            }
          }
        } catch (err) {
          calls.push({
            provider: "context_dev",
            kind: "scrape",
            url: u.url,
            ok: false,
            error: String(err?.message || err),
          });
          const recovery = planScrapeFailureRecovery(
            {
              url: u.url,
              title: u.title,
              snippet: u.snippet,
              tier: u.scrape_evaluation?.source_tier || u.ownership_scrape_gate?.tier || 99,
              fail_reason: String(err?.message || err),
            },
            {
              attempted_urls: state.urls_read,
              extracted_cnpj: state.legal_entity_leads?.find((e) => e.primary_cnpj)?.primary_cnpj?.cnpj_digits
                || state.legal_entity_leads?.find((e) => e.cnpj)?.cnpj
                || null,
              extracted_legal_name: state.legal_entity_leads?.find((e) => e.legal_name || e.entity_name)?.legal_name
                || state.legal_entity_leads?.find((e) => e.entity_name)?.entity_name
                || null,
              alternate_candidates: ranked.filter((r) => r.url !== u.url),
            }
          );
          state.scrape_failure_recoveries = state.scrape_failure_recoveries || [];
          state.scrape_failure_recoveries.push(recovery);
          for (const p of recovery.pivots || []) {
            if (p.query) {
              state.unanswered_questions = state.unanswered_questions || [];
              state.unanswered_questions.push({
                question: "High-value scrape failed — pivot without OTA substitute",
                evidence_gap: goal.evidence_gap || "missing_legal_entity",
                query: p.query,
                from: "scrape_failure_pivot",
              });
            }
          }
          if (state.stage_trace) {
            pushStageEvent(state.stage_trace, {
              kind: "fetch",
              stage: "PAGE_FETCHING_AND_DOCUMENT_PARSING",
              url: u.url,
              ok: false,
              attempted: true,
              characters: 0,
              fetch_outcome: "FETCH_FAILED",
              error: String(err?.message || err),
              recovery_reason: recovery.reason,
              ota_fallback_allowed: recovery.ota_fallback_allowed === true,
              remaining_budget: budgetController.remainingAvailable(),
            });
          }
          continue;
        }

        state.urls_read.push(u.url);
        state.completed_work_keys.add(scrapeKey);
        const md = String(
          typeof scraped?.data === "string"
            ? scraped.data
            : scraped?.data?.markdown || scraped?.data?.content || scraped?.markdown || ""
        );
        calls.push({
          provider: "context_dev",
          kind: "scrape",
          url: u.url,
          ok: scraped?.ok !== false && Boolean(md),
          characters: md.length,
          iterative: true,
        });
        if (state.stage_trace) {
          const docPass = md ? ownershipDocumentPassages(md, hotel) : { passages: [] };
          const passages = docPass.passages || [];
          const fetchOutcome = !md
            ? "EMPTY_DOCUMENT"
            : docPass.truncated || docPass.source_truncated
              ? "BLOCKED_TRUNCATION"
              : passages.length === 0
                ? "NO_OWNERSHIP_PASSAGE"
                : "FETCH_OK";
          pushStageEvent(state.stage_trace, {
            kind: "fetch",
            stage: "PAGE_FETCHING_AND_DOCUMENT_PARSING",
            url: u.url,
            ok: scraped?.ok !== false && Boolean(md),
            attempted: true,
            characters: md.length,
            fetch_outcome: fetchOutcome,
            extracted_passage_count: passages.length,
            extracted_passages: passages.map((p) => String(p.excerpt || "").slice(0, 240)),
            remaining_budget: budgetController.remainingAvailable(),
          });
          if (!md) {
            pushStageEvent(state.stage_trace, {
              kind: "fetch",
              stage: "EMPTY_DOCUMENT",
              url: u.url,
              ok: false,
              attempted: true,
              characters: 0,
              remaining_budget: budgetController.remainingAvailable(),
            });
          }
          pushStageEvent(state.stage_trace, {
            kind: "budget",
            stage: "BUDGET",
            remaining_budget: budgetController.remainingAvailable(),
            reserved_for_follow_up: state.budget.reserved_for_follow_up || 0,
          });
        }
        if (!md) continue;

        // Stage legal-entity / QSA from scraped document (research staging only).
        if (/\bCNPJ\b|raz[aã]o\s+social|s[oó]cio|administradora|FII\b|controladora/i.test(md)) {
          const stagedDoc = stageLegalEntityFromEvidence({
            hotel,
            text: md,
            source_url: u.url,
          });
          if (
            stagedDoc.entity_name ||
            stagedDoc.cnpj ||
            (stagedDoc.principals || []).length ||
            stagedDoc.fii_lead
          ) {
            state.legal_entity_leads = state.legal_entity_leads || [];
            state.legal_entity_leads.push({
              ...stagedDoc,
              from: "scrape_document",
              query: goal.query,
            });
            newEvidenceThisRound += 1;
          }
        }

        const ingest = await ingestDocumentClaims(state, md, u.url, hotel, {
          modelUsdLedger: modelLedger,
          deps,
          applyStructuredReader: b.apply_structured_reader !== false,
          calls,
        });
        newEvidenceThisRound += ingest.newCount;
        sources.push({
          kind: "context_scrape_ownership_iterative",
          url: u.url,
          ...ingest.document,
          structured_reader: ingest.readerMeta,
          observed_at: new Date().toISOString(),
        });
      }
    });

    state.budget.consumed = budgetController.ledger.spent;
    state.budget.remaining = budgetController.remainingAvailable();
    state.budget.model = {
      consumed: modelLedger.spent,
      remaining: modelLedger.remaining(),
    };

    // V2.2 adaptive loop: refresh candidates + verify after each evidence round
    if (adaptiveEnabled && state.adaptive && newEvidenceThisRound > 0) {
      const verified = refreshCandidatesAndVerify(
        state.adaptive,
        {
          ...state,
          ownership_conclusion: state.ownership_conclusion || null,
          legal_entity_leads: state.legal_entity_leads || [],
        },
        hotel
      );
      state.adaptive = verified.adaptive;
      state.adaptive_verification_routing = verified.routing || null;
    }

    if (isOwnershipObjectiveSupported(state)) {
      state.stop_reasons.push("OBJECTIVE_SUPPORTED");
      return;
    }

    if (newEvidenceThisRound === 0 && depth <= 1) {
      const pivot = tryAdaptivePlaybookPivot();
      if (pivot.pivoted) {
        await runDepth(Math.max(depth, 1), breadth, pivot.goals);
        return;
      }
      state.stop_reasons.push(
        pivot.exhausted
          ? STRUCTURED_REJECTION_REASON.RESEARCH_PATH_EXHAUSTED
          : "NO_USEFUL_NEW_EVIDENCE"
      );
      return;
    }

    // Allow planner + follow-up depth when soft scrape exhaustion left BUDGET_EXHAUSTED
    // but credits remain for at least one search+scrape.
    if (budgetController.remainingAvailable() >= searchCost() + scrapeCost) {
      clearSoftBudgetStops();
    }

    const nextBreadth = Math.max(1, Math.ceil(breadth / 2));
    let nextGoals = [];
    if (b.apply_model_follow_up_planner !== false) {
      const resolved = await resolveNextQueryGoals(state, hotel, nextBreadth, {
        modelUsdLedger: modelLedger,
        deps,
        calls,
      });
      nextGoals = resolved.goals || [];
      if (resolved.source?.startsWith("model")) {
        calls.push({
          provider: "planner",
          kind: "next_goals",
          source: resolved.source,
          count: nextGoals.length,
        });
      }
      if (resolved.model?.error) {
        state.warnings = state.warnings || [];
        state.warnings.push("MODEL_FOLLOW_UP_PLANNER_FAILED");
      }
    } else {
      nextGoals = generateFollowUpQueryGoals(state, hotel, nextBreadth);
    }

    if (!nextGoals.length) {
      const pivot = tryAdaptivePlaybookPivot();
      if (pivot.pivoted) {
        await runDepth(Math.max(depth - 1, 1), nextBreadth, pivot.goals);
        return;
      }
      state.stop_reasons.push(
        pivot.exhausted
          ? STRUCTURED_REJECTION_REASON.RESEARCH_PATH_EXHAUSTED
          : "NO_USEFUL_NEW_EVIDENCE"
      );
      return;
    }
    if (budgetController.remainingAvailable() < searchCost() + scrapeCost) {
      state.stop_reasons.push("BUDGET_EXHAUSTED");
      return;
    }

    await runDepth(depth - 1, nextBreadth, nextGoals);
  }

  // Entry: prefer unattempted initial goals; if exhausted, consult follow-up /
  // unanswered / miss-recovery planners before declaring no useful work.
  // Adaptive controller (optional): start with playbook-routed goals.
  let entryGoals = [];
  if (adaptiveEnabled) {
    const plan = planAdaptiveContinuation(state, hotel);
    state.adaptive = plan.adaptive;
    entryGoals = filterUnattemptedGoals(plan.goals || []);
  }
  if (!entryGoals.length) {
    entryGoals = filterUnattemptedGoals(generateInitialOwnershipQueryGoals(hotel, b.breadth));
  }
  if (!entryGoals.length) {
    entryGoals = filterUnattemptedGoals(generateFollowUpQueryGoals(state, hotel, b.breadth));
  }
  if (!entryGoals.length) {
    entryGoals = filterUnattemptedGoals(generateMissRecoveryQueryGoals(state, hotel, b.breadth));
  }

  const continuation = reassessContinuationEligibility(entryGoals);
  if (!continuation.allowed) {
    // Soft exhaustion: try next adaptive playbook before case-terminal stop.
    if (adaptiveEnabled) {
      const pivot = tryAdaptivePlaybookPivot(
        state.continuation_decision?.reason === "NO_UNATTEMPTED_JUSTIFIED_LEADS"
          ? STRUCTURED_REJECTION_REASON.NO_USEFUL_NEW_EVIDENCE
          : STRUCTURED_REJECTION_REASON.OWNER_EVIDENCE_INSUFFICIENT
      );
      if (pivot.pivoted) {
        await runDepth(b.depth, b.breadth, pivot.goals);
        const snapEarly = journaler.snapshot();
        state.result_by_work_key = snapEarly.result_by_work_key;
        state.pending_unknown_keys = new Set(snapEarly.pending_unknown_keys);
        for (const k of snapEarly.completed_work_keys) state.completed_work_keys.add(k);
        if (!state.stop_reasons.length) state.stop_reasons.push("LOOP_COMPLETE");
        return finalizeLoopResult(state, calls, sources, controls, modelLedger, hotel);
      }
    }
    if (!state.stop_reasons.length) {
      state.stop_reasons.push(
        adaptiveEnabled && state.adaptive?.research_path_exhausted
          ? STRUCTURED_REJECTION_REASON.RESEARCH_PATH_EXHAUSTED
          : "CONTINUATION_EXHAUSTED"
      );
    }
    return finalizeLoopResult(state, calls, sources, controls, modelLedger, hotel);
  }

  await runDepth(b.depth, b.breadth, continuation.goals);

  // Attach durable journal snapshot before finalize
  const snap = journaler.snapshot();
  state.result_by_work_key = snap.result_by_work_key;
  state.pending_unknown_keys = new Set(snap.pending_unknown_keys);
  for (const k of snap.completed_work_keys) state.completed_work_keys.add(k);

  if (!state.stop_reasons.length) {
    state.stop_reasons.push("LOOP_COMPLETE");
  }
  return finalizeLoopResult(state, calls, sources, controls, modelLedger, hotel);
}

function finalizeLoopResult(state, calls, sources, controls, modelLedger = null, hotel = {}) {
  const ownerPick = pickOwnerCandidateFromState(state);
  const completed = [...state.completed_work_keys];
  const stopReasons = [...new Set(state.stop_reasons)];
  const strategy = resolveOwnershipResearchStrategy(hotel);
  const lineage = assessPropertyLineage(hotel, {
    observed_current_name: state.observed_current_name || null,
    preserve_historical: state.historical_names || [],
  });
  const legalEntities = [...(state.legal_entity_leads || [])];
  // Also extract from scraped claim passages when present
  for (const c of state.claims || []) {
    const blob = `${c.passage || c.evidence_passage || c.text || ""} ${c.subject || ""}`;
    if (/\bCNPJ\b|raz[aã]o\s+social|s[oó]cio|administradora|FII\b/i.test(blob)) {
      const staged = stageLegalEntityFromEvidence({
        hotel,
        text: blob,
        source_url: c.source_url || null,
      });
      legalEntities.push({
        ...extractLegalEntityBundle(blob, { source_url: c.source_url || null }),
        ...staged,
        from: "claim_passage",
      });
    }
  }
  const principals = legalEntities.flatMap((e) => e.principals || []);
  const ambiguous = legalEntities.some(
    (e) =>
      e.staging_conclusion === OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS ||
      e.identity_match === "CONFLICTING_LOCATION"
  );
  let conclusion = concludeFromEntityEvidence({
    lineage,
    entities: legalEntities.filter(
      (e) => e.staging_conclusion !== OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS
    ),
    conflicting: (state.conflicts || []).length > 0 || ambiguous,
  });
  if (ambiguous && conclusion === OWNERSHIP_CONCLUSION_STATE.CONFLICTING_EVIDENCE) {
    conclusion = OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS;
  } else if (
    ambiguous &&
    !legalEntities.some(
      (e) =>
        e.legal_entity_resolved_for_target === true &&
        e.staging_conclusion !== OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS
    )
  ) {
    conclusion = OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS;
  }
  // Prefer strongest non-UNRESOLVED staged conclusion when present
  const stagedConclusions = legalEntities
    .map((e) => e.staging_conclusion)
    .filter((c) => c && c !== OWNERSHIP_CONCLUSION_STATE.UNRESOLVED);
  if (
    stagedConclusions.includes(OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OPERATING_ENTITY)
  ) {
    conclusion = OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OPERATING_ENTITY;
  } else if (
    stagedConclusions.includes(OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_IDENTIFIED_OWNER_UNCONFIRMED) &&
    conclusion === OWNERSHIP_CONCLUSION_STATE.UNRESOLVED
  ) {
    conclusion = OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_IDENTIFIED_OWNER_UNCONFIRMED;
  } else if (
    stagedConclusions.includes(OWNERSHIP_CONCLUSION_STATE.SUPPORTED_REAL_ESTATE_FUND_LEAD)
  ) {
    conclusion = OWNERSHIP_CONCLUSION_STATE.SUPPORTED_REAL_ESTATE_FUND_LEAD;
  } else if (stagedConclusions.includes(OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS)) {
    conclusion = OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS;
  }
  const progress = computeOwnershipResearchProgress({
    lineage,
    legal_entities: legalEntities,
    principals,
    conclusion,
  });
  // Exact-address continuation when identity ambiguous
  if (conclusion === OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS) {
    state.address_continuation_queries = buildExactAddressContinuationQueries(hotel);
  }
  // V2.1: evaluate all actionable candidates from existing evidence before serialize.
  // Does not call providers. Does not publish HPC ownership facts.
  if (state.adaptive?.enabled) {
    const verified = refreshCandidatesAndVerify(
      state.adaptive,
      {
        ...state,
        ownership_conclusion: conclusion,
        legal_entity_leads: legalEntities,
      },
      hotel
    );
    state.adaptive = verified.adaptive;
    state.adaptive_verification_routing = verified.routing || null;
  }

  if (state.stage_trace) {
    for (const g of state.unanswered_questions || []) {
      pushStageEvent(state.stage_trace, {
        kind: "follow_up",
        stage: "CURRENTNESS_AND_SUCCESSOR_RESEARCH",
        question: g.question,
        evidence_gap: g.evidence_gap,
        status: "queued_or_remaining",
      });
    }
    const earliest = inferEarliestOwnershipFailureStage({
      claims: state.claims,
      searches: state.stage_trace.searches,
      fetches: state.stage_trace.fetches,
      rankedEmptyBecauseFiltered: Boolean(state.ranked_empty_because_filtered),
      confirmedDomain: null,
      evidencedPeople: [],
      hasAttributableContact: false,
      stopReasons,
      objectiveSupported: isOwnershipObjectiveSupported(state),
    });
    state.stage_trace.earliest_failure_stage = earliest;
    state.stage_trace.final_stop_reasons = stopReasons;
    pushStageEvent(state.stage_trace, {
      kind: "budget",
      stage: "FINALIZE",
      remaining_budget: state.budget?.remaining ?? null,
      earliest_failure_stage: earliest,
      final_stop_reasons: stopReasons,
      ownership_conclusion: conclusion,
      research_progress: progress,
      adaptive_candidates_pending: state.adaptive?.telemetry?.candidates_pending ?? null,
      adaptive_verification_attempts: state.adaptive?.telemetry?.verification_attempts ?? null,
    });
  }
  return {
    version: OWNERSHIP_ITERATIVE_LOOP_VERSION,
    provenance: DEEP_RESEARCH_PROVENANCE,
    stop_reasons: stopReasons,
    earliest_failure_stage: state.stage_trace?.earliest_failure_stage || null,
    stage_trace: state.stage_trace
      ? sanitizeOwnershipStageTraceForFixture(state.stage_trace)
      : null,
    research_state: {
      ...state,
      adaptive: state.adaptive ? serializeAdaptiveForResume(state.adaptive) : state.adaptive,
      completed_work_keys: completed,
      pending_unknown_keys: [...(state.pending_unknown_keys || [])],
      result_by_work_key: state.result_by_work_key || {},
      _controls: undefined,
      controls,
      budget: {
        ...state.budget,
        model: modelLedger ? modelLedger.snapshot() : state.budget.model || null,
      },
      property_lineage: lineage,
      legal_entity_leads: legalEntities,
      ownership_conclusion: conclusion,
      research_progress: progress,
      strategy_id: strategy?.id || null,
    },
    ownership_candidate: ownerPick,
    property_lineage: lineage,
    legal_entities: legalEntities,
    principals,
    ownership_conclusion: conclusion,
    research_progress: progress,
    claims: state.claims,
    conflicts: state.conflicts,
    calls,
    sources,
    budgets: {
      context_dev: null,
      model_usd: modelLedger ? modelLedger.snapshot() : null,
    },
    enrichment_authorized: false,
    publication_authorized: false,
    canonical_writes: false,
  };
}

/**
 * Rollout gate — off by default.
 */
export function shouldUseIterativeOwnershipLoop(caseInput = {}, budgets = {}, env = process.env) {
  if (budgets.iterative_ownership_loop === true) return true;
  if (caseInput.iterative_ownership_loop === true) return true;
  if (String(env.CONTACT_INTELLIGENCE_ITERATIVE_OWNERSHIP_LOOP || "") === "1") return true;
  return false;
}
