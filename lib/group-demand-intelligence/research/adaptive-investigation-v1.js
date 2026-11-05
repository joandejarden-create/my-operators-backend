/**
 * Adaptive blocker investigation for the existing Research Supervisor.
 * Every paid step is a planner decision. Production provider order is untouched.
 */

import { createHash } from "node:crypto";
import { BLOCKER_KEY, INVESTIGATION_DISPOSITION } from "./research-constants-v1.js";
import {
  HeuristicResearchPlanner,
  classifyResearchPage,
  hypothesisFor,
  isDiminishingRepeat,
  equivalentEvidenceAlreadyHeld,
  recordSkippedPublicStrategy,
  strategyFingerprint,
} from "./research-planner-v1.js";
import { planBenchmarkAcquisition } from "../workflow/gdi-evidence-acquisition-v2.js";
import { extractEvidenceContacts } from "../evidence-contact-extraction-v1.js";

const RUNAWAY_STEP_GUARD = 12;

function hashText(value) {
  return createHash("sha256").update(String(value || "")).digest("hex");
}

function fold(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .toLowerCase();
}

function orgRelevant(organizationName, quote, sourceUrl) {
  const tokens = fold(organizationName)
    .split(/[^a-z0-9]+/)
    .filter((token) => token.length >= 4);
  const blob = fold(`${quote} ${sourceUrl}`);
  return tokens.some((token) => blob.includes(token));
}

export function interpretInvestigationEvidence({
  blockerKey,
  quote = "",
  sourceUrl = "",
  title = "",
  organizationName = "",
  countryHint = "",
} = {}) {
  const pageClass = classifyResearchPage({ url: sourceUrl, title, quote });
  const extracted = extractEvidenceContacts({ quote, sourceUrl, countryHint });
  const buyingConfirmed = false;
  if (pageClass === "coming_soon" || pageClass === "preview_shell") {
    return {
      effect: "refresh",
      pageClass,
      blockerClosed: false,
      buyingConfirmed,
      extracted,
      claim: "This source is unpublished or not yet available.",
    };
  }
  if (blockerKey === BLOCKER_KEY.CONTACT_ROUTE) {
    if (["terms_of_use", "homepage", "calendar", "ticket_commerce"].includes(pageClass)) {
      return {
        effect: "narrowed",
        pageClass,
        blockerClosed: false,
        buyingConfirmed,
        extracted,
        claim: `${pageClass} does not establish a sales route.`,
      };
    }
    const usable = extracted.email || extracted.phone;
    if (usable && orgRelevant(organizationName, quote, sourceUrl)) {
      return { effect: "closed", pageClass, blockerClosed: true, buyingConfirmed, extracted, claim: null };
    }
    return { effect: "open", pageClass, blockerClosed: false, buyingConfirmed, extracted, claim: null };
  }
  if (blockerKey === BLOCKER_KEY.FUTURE_DEMAND) {
    if (["ticket_commerce", "calendar", "terms_of_use", "preview_shell"].includes(pageClass)) {
      return {
        effect: "narrowed",
        pageClass,
        blockerClosed: false,
        buyingConfirmed,
        extracted,
        claim: `${pageClass} does not establish overnight lodging.`,
      };
    }
    if (/no (?:hotel )?room block|no overnight|does not arrange accommodation|local cast only/i.test(quote)) {
      return {
        effect: "negative",
        pageClass,
        blockerClosed: false,
        buyingConfirmed,
        extracted,
        claim: "This source states the lodging fact is not established.",
      };
    }
    if (/room block|housing bureau|official hotel|reserved a room block|accommodation has been arranged/i.test(quote)) {
      return { effect: "closed", pageClass, blockerClosed: true, buyingConfirmed: false, extracted, claim: null };
    }
    return { effect: "open", pageClass, blockerClosed: false, buyingConfirmed, extracted, claim: null };
  }
  return { effect: "open", pageClass, blockerClosed: false, buyingConfirmed, extracted, claim: null };
}

function attemptRecord({ decision, acquisition, reading, quote, sourceUrl, contentHash, costUsd, outcome, error }) {
  return {
    at: new Date().toISOString(),
    blocker: decision.blockerKey || null,
    missingFact: decision.missingFact || null,
    hypothesis: decision.hypothesis || null,
    sourceFamily: decision.sourceFamily || null,
    strategy: decision.strategy || null,
    operation: acquisition?.benchmarkOperation || decision.operation || null,
    language: decision.language || null,
    sourceUrl: sourceUrl || null,
    sourceIdentity: sourceUrl || decision.queries?.[0] || null,
    queries: decision.queries || [],
    fingerprint: decision.fingerprint || null,
    evidenceQuote: String(quote || "").slice(0, 1200),
    contentHash: contentHash || null,
    pageClass: reading?.pageClass || null,
    costUsd: Number(costUsd || 0),
    outcome: outcome || reading?.effect || "open",
    claim: reading?.claim || null,
    buyingConfirmed: false,
    provider: acquisition?.benchmarkProvider || null,
    newMaterialEvidence: reading?.effect === "closed" || reading?.effect === "negative",
    countsAsResearchDepth: outcome !== "skipped_duplicate" && outcome !== "page_class_rejected" && outcome !== "not_executable",
    email: reading?.extracted?.email?.value || null,
    phone: reading?.extracted?.phone?.normalized || null,
    error: error || null,
    plannerApproved: Boolean(decision.fingerprint),
  };
}

function finish(state, decisions, disposition, extra = {}) {
  state.stopDisposition = disposition;
  state.investigationStatus = disposition;
  state.stopReason = extra.stopReason || disposition;
  if (extra.remainingStrategies) state.remainingStrategies = extra.remainingStrategies;
  if (extra.negativeClaim) state.negativeClaim = extra.negativeClaim;
  return { state, decisions, disposition };
}

function terminalFromPlanner(state, decision) {
  if (decision.disposition === INVESTIGATION_DISPOSITION.RESEARCH_STRATEGIES_EXHAUSTED) {
    if (state.refreshCandidates?.length) return INVESTIGATION_DISPOSITION.SCHEDULED_REFRESH;
    const negative = (state.researchAttempts || []).find((attempt) => attempt.outcome === "negative" && attempt.claim);
    if (negative) return INVESTIGATION_DISPOSITION.EVIDENCE_BACKED_NEGATIVE;
  }
  return decision.disposition;
}

const CLOSED_DISPOSITIONS = new Set([
  INVESTIGATION_DISPOSITION.VERIFIED_RESOLVED,
  INVESTIGATION_DISPOSITION.EVIDENCE_BACKED_NEGATIVE,
  INVESTIGATION_DISPOSITION.RESEARCH_STRATEGIES_EXHAUSTED,
]);

function refreshIsDue(opportunity, options) {
  if (options.refreshDue === true) return true;
  if (!opportunity?.refreshAfter || !options.now) return false;
  return String(options.now) >= String(opportunity.refreshAfter);
}

export async function runAdaptiveInvestigation({
  opportunity,
  blocker,
  execute,
  planner = new HeuristicResearchPlanner(),
  remainingUsd = Number.POSITIVE_INFINITY,
  env = { GDI_BENCHMARK_PROVIDER_EVAL: "1" },
  interruptAfterSteps = null,
  countryHint = "",
  language = "",
  maxSteps = RUNAWAY_STEP_GUARD,
  refreshDue = false,
  now = "",
} = {}) {
  if (typeof execute !== "function") {
    throw new Error("ADAPTIVE_INVESTIGATION_REQUIRES_EXECUTE");
  }
  if (CLOSED_DISPOSITIONS.has(opportunity?.stopDisposition)) {
    return { state: opportunity, decisions: [], disposition: opportunity.stopDisposition, resumed: false };
  }
  if (
    opportunity?.stopDisposition === INVESTIGATION_DISPOSITION.SCHEDULED_REFRESH &&
    !refreshIsDue(opportunity, { refreshDue, now })
  ) {
    return { state: opportunity, decisions: [], disposition: opportunity.stopDisposition, resumed: false };
  }
  let state = {
    ...opportunity,
    researchAttempts: (opportunity.researchAttempts || []).map((attempt) =>
      refreshDue && attempt.outcome === "refresh" ? { ...attempt, disposition: "REOPENED" } : attempt
    ),
    refreshCandidates: [...(opportunity.refreshCandidates || [])],
    investigationStatus: INVESTIGATION_DISPOSITION.IN_PROGRESS,
    buyingConfirmed: false,
  };
  const decisions = [];
  let paidSteps = 0;
  let budget = remainingUsd;

  for (let step = 0; step < maxSteps; step += 1) {
    const decision = planner.planAdaptiveStep({
      opportunity: state,
      blocker,
      options: { language: language || state.researchLanguage, countryHint: countryHint || state.countryHint },
    });
    decision.blockerKey = blocker?.key;
    decisions.push({
      strategy: decision.strategy,
      fingerprint: decision.fingerprint,
      disposition: decision.disposition,
      sourceFamily: decision.sourceFamily,
      hypothesis: decision.hypothesis,
    });
    for (const skip of decision.skipped || []) {
      state = recordSkippedPublicStrategy(state, {
        blocker: blocker?.key,
        strategy: skip.strategy,
        reason: skip.reason,
      });
    }
    if (decision.disposition) {
      let disposition = terminalFromPlanner(state, decision);
      if (
        disposition === INVESTIGATION_DISPOSITION.RESEARCH_STRATEGIES_EXHAUSTED &&
        !decision.exhaustionAssessment?.noMateriallyDifferentStrategy
      ) {
        disposition = INVESTIGATION_DISPOSITION.NEEDS_HUMAN_REVIEW;
      }
      if (decision.exhaustionAssessment) state.exhaustionAssessment = decision.exhaustionAssessment;
      if (decision.refreshCandidate) state.refreshCandidates.push(decision.refreshCandidate);
      const negative = (state.researchAttempts || []).find((attempt) => attempt.outcome === "negative" && attempt.claim);
      return finish(state, decisions, disposition, {
        remainingStrategies: [],
        negativeClaim: disposition === INVESTIGATION_DISPOSITION.EVIDENCE_BACKED_NEGATIVE ? negative?.claim : null,
        stopReason: decision.exhaustionAssessment?.rationale || decision.negativeResultMeans || disposition,
      });
    }

    const acquisition = planBenchmarkAcquisition(state, {
      benchmarkEvaluation: true,
      env,
      plannerAction: decision,
      query: decision.queries?.[0],
    });
    const cost = Number(acquisition.benchmarkMaxUsd || 0);
    if (!(budget >= cost)) {
      return finish(state, decisions, INVESTIGATION_DISPOSITION.BUDGET_EXHAUSTED, {
        remainingStrategies: [decision.strategy, ...(decision.remainingStrategies || [])],
        stopReason: "Remaining budget is below the next bounded call.",
      });
    }
    const proposedUrl = acquisition.benchmarkParams?.urls?.[0] || acquisition.benchmarkParams?.url || "";
    const proposedClass = proposedUrl ? classifyResearchPage({ url: proposedUrl }) : "";
    if (["terms_of_use", "ticket_commerce", "preview_shell"].includes(proposedClass)) {
      state.researchAttempts.push(
        attemptRecord({
          decision,
          acquisition,
          reading: { pageClass: proposedClass, effect: "narrowed", claim: `${proposedClass} is not a target for this hypothesis.` },
          sourceUrl: proposedUrl,
          costUsd: 0,
          outcome: "page_class_rejected",
        })
      );
      continue;
    }
    if (!acquisition.benchmarkExecutable) {
      state.researchAttempts.push(
        attemptRecord({
          decision,
          acquisition,
          costUsd: 0,
          outcome: "not_executable",
          error: acquisition.benchmarkRefusal || "NOT_EXECUTABLE",
        })
      );
      continue;
    }
    const operation = acquisition.benchmarkOperation || decision.operation || "search";
    if (
      ["contents", "scrape", "parse"].includes(operation) &&
      equivalentEvidenceAlreadyHeld(state.researchAttempts, { sourceUrl: proposedUrl })
    ) {
      state.researchAttempts.push(
        attemptRecord({
          decision,
          acquisition,
          sourceUrl: proposedUrl,
          costUsd: 0,
          outcome: "skipped_duplicate",
        })
      );
      continue;
    }
    if (operation === "search" && state.suppressEquivalentSearches) {
      state.researchAttempts.push(
        attemptRecord({
          decision,
          acquisition,
          costUsd: 0,
          outcome: "skipped_duplicate",
        })
      );
      continue;
    }

    let result;
    try {
      result = await execute({ decision, acquisition, opportunity: state });
    } catch (error) {
      const code = error?.code || "PROVIDER_BLOCKED";
      state.researchAttempts.push(
        attemptRecord({
          decision,
          acquisition,
          costUsd: 0,
          outcome: "provider_failed",
          error: code,
        })
      );
      if (/BUDGET|CEILING/.test(code)) {
        return finish(state, decisions, INVESTIGATION_DISPOSITION.BUDGET_EXHAUSTED, {
          remainingStrategies: [decision.strategy, ...(decision.remainingStrategies || [])],
          stopReason: code,
        });
      }
      return finish(state, decisions, INVESTIGATION_DISPOSITION.PROVIDER_BLOCKED, {
        remainingStrategies: [decision.strategy, ...(decision.remainingStrategies || [])],
        stopReason: code,
      });
    }
    if (result?.duplicate) {
      state.researchAttempts.push(
        attemptRecord({
          decision,
          acquisition,
          sourceUrl: result.sourceUrl || "",
          costUsd: 0,
          outcome: "skipped_duplicate",
        })
      );
      continue;
    }
    if (Array.isArray(result?.candidates) && result.candidates.length) {
      const seen = new Set((state.discoveredCandidates || []).map((row) => row.url));
      state.discoveredCandidates = [
        ...(state.discoveredCandidates || []),
        ...result.candidates.filter((row) => row?.url && !seen.has(row.url)),
      ];
    }

    const quote = result?.quote || "";
    const sourceUrl = result?.sourceUrl || "";
    const contentHash = hashText(quote);
    const heldBefore = equivalentEvidenceAlreadyHeld(state.researchAttempts, { sourceUrl, contentHash });
    if (heldBefore) {
      if ((acquisition.benchmarkOperation || decision.operation) === "search") {
        state.suppressEquivalentSearches = true;
      }
      state.researchAttempts.push(
        attemptRecord({
          decision,
          acquisition,
          quote,
          sourceUrl,
          contentHash,
          costUsd: Number(result?.costUsd || 0),
          outcome: "skipped_duplicate",
        })
      );
      continue;
    }
    state.suppressEquivalentSearches = false;
    if (isDiminishingRepeat(state.researchAttempts, { contentHash, sourceUrl })) {
      state.researchAttempts.push(
        attemptRecord({
          decision,
          acquisition,
          quote,
          sourceUrl,
          contentHash,
          costUsd: 0,
          outcome: "skipped_duplicate",
        })
      );
      continue;
    }
    const reading = interpretInvestigationEvidence({
      blockerKey: blocker?.key,
      quote,
      sourceUrl,
      title: result?.title || "",
      organizationName: state.organizationName,
      countryHint: countryHint || decision.countryHint || state.countryHint,
    });
    const spent = Number(result?.costUsd ?? cost);
    budget -= spent;
    paidSteps += 1;
    state.researchAttempts.push(
      attemptRecord({
        decision,
        acquisition,
        reading,
        quote,
        sourceUrl,
        contentHash,
        costUsd: spent,
      })
    );
    if (reading.effect === "refresh") {
      state.refreshCandidates.push({
        sourceUrl,
        pageClass: reading.pageClass,
        reason: reading.claim,
      });
    }
    if (reading.effect === "closed") {
      state.buyingConfirmed = false;
      return finish(state, decisions, INVESTIGATION_DISPOSITION.VERIFIED_RESOLVED, {
        remainingStrategies: decision.remainingStrategies || [],
        stopReason: "The missing fact is present in retrieved evidence.",
      });
    }
    if (interruptAfterSteps != null && paidSteps >= interruptAfterSteps) {
      return finish(state, decisions, INVESTIGATION_DISPOSITION.INTERRUPTED, {
        remainingStrategies: decision.remainingStrategies || [],
        stopReason: "Execution interrupted after a persisted step.",
      });
    }
  }

  return finish(state, decisions, INVESTIGATION_DISPOSITION.NEEDS_HUMAN_REVIEW, {
    remainingStrategies: [],
    stopReason: "Runaway guard reached. This is not research exhaustion.",
  });
}

function strategyForFrozen(blockerKey, pageClass) {
  if (blockerKey === BLOCKER_KEY.CONTACT_ROUTE) {
    if (pageClass === "preview_shell" || pageClass === "coming_soon") return "official_business_email_phone";
    return "event_contact_page";
  }
  if (pageClass === "calendar") return "event_calendar";
  if (pageClass === "ticket_commerce" || pageClass === "coming_soon" || pageClass === "housing_page") {
    return "housing_block_or_booking_page";
  }
  return "official_future_events";
}

export function replayFrozenInvestigation({ opportunity, blocker, evidence = [], language = "en" } = {}) {
  const planner = new HeuristicResearchPlanner();
  let state = {
    ...opportunity,
    researchAttempts: [...(opportunity.researchAttempts || [])],
    refreshCandidates: [...(opportunity.refreshCandidates || [])],
    buyingConfirmed: false,
  };
  const interpretations = [];
  for (const item of evidence) {
    const reading = interpretInvestigationEvidence({
      blockerKey: blocker?.key,
      quote: item.quote || "",
      sourceUrl: item.sourceUrl || "",
      title: item.title || "",
      organizationName: state.organizationName,
      countryHint: item.countryHint || state.countryHint || "",
    });
    interpretations.push({ sourceUrl: item.sourceUrl || null, ...reading });
    const strategy = strategyForFrozen(blocker?.key, reading.pageClass);
    const described = hypothesisFor({
      opportunity: state,
      blockerKey: blocker?.key,
      strategy,
      language,
    });
    const queries = planner.queriesFor({
      strategy,
      org: state.organizationName,
      market: state.hotel?.market || "",
      yearWindow: "2026 2027 2028",
      language,
    });
    const fingerprint = strategyFingerprint({ blocker: blocker?.key, strategy, queries });
    state.researchAttempts.push({
      ...described,
      strategy,
      blocker: blocker?.key,
      fingerprint,
      sourceUrl: item.sourceUrl || null,
      sourceIdentity: item.sourceUrl || null,
      queries,
      operation: item.operation || "contents",
      language,
      evidenceQuote: String(item.quote || "").slice(0, 1200),
      contentHash: item.contentHash || hashText(item.quote || ""),
      pageClass: reading.pageClass,
      costUsd: 0,
      outcome: reading.effect,
      claim: reading.claim,
      buyingConfirmed: false,
      newMaterialEvidence: reading.effect === "closed",
      email: reading.extracted?.email?.value || null,
      phone: reading.extracted?.phone?.normalized || null,
      plannerApproved: true,
      historical: true,
    });
    if (reading.effect === "refresh") {
      state.refreshCandidates.push({
        sourceUrl: item.sourceUrl || null,
        pageClass: reading.pageClass,
        reason: reading.claim,
      });
    }
    if (reading.effect === "closed") {
      state.stopDisposition = INVESTIGATION_DISPOSITION.VERIFIED_RESOLVED;
      state.investigationStatus = INVESTIGATION_DISPOSITION.VERIFIED_RESOLVED;
      state.remainingStrategies = [];
      return { state, interpretations, next: null, disposition: state.stopDisposition, plannerInvoked: false };
    }
  }
  const next = planner.planAdaptiveStep({
    opportunity: state,
    blocker,
    options: { language, countryHint: state.countryHint },
  });
  state.remainingStrategies = next.strategy ? [next.strategy, ...(next.remainingStrategies || [])] : [];
  state.stopDisposition = next.disposition;
  state.investigationStatus = next.disposition || INVESTIGATION_DISPOSITION.IN_PROGRESS;
  return {
    state,
    interpretations,
    next,
    disposition: next.disposition,
    plannerInvoked: true,
  };
}
