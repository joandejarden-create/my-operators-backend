/**
 * Blocker-driven research planner — selects next strategy without repeating
 * exhausted fingerprints unless new evidence reopens the path.
 */

import { BLOCKER_KEY, INVESTIGATION_DISPOSITION } from "./research-constants-v1.js";
import { classifyEvidenceSourcePage } from "../evidence-contact-extraction-v1.js";

const STRATEGIES = Object.freeze({
  [BLOCKER_KEY.FUTURE_DEMAND]: [
    "official_future_events",
    "event_calendar",
    "exhibitor_or_sponsor_prospectus",
    "cvb_convention_calendar",
    "trade_press_future_announcement",
    "historical_recurrence_analysis",
    "parallel_targeted_followup",
  ],
  [BLOCKER_KEY.DEMAND_SCALE]: [
    "official_attendance_or_rooms",
    "housing_block_or_booking_page",
    "prospectus_attendance",
    "prior_event_scale",
    "venue_capacity_and_format",
    "parallel_targeted_followup",
  ],
  [BLOCKER_KEY.HOTEL_FIT]: [
    "distance_and_catchment",
    "event_venue_lodging_pattern",
    "room_block_pattern",
    "hotel_product_match",
  ],
  [BLOCKER_KEY.BUYER_IDENTITY]: [
    "official_staff_directory",
    "event_team_page",
    "association_management_company",
    "professional_profile_role_resolution",
    "speaker_sponsor_contact",
    "parallel_targeted_followup",
  ],
  [BLOCKER_KEY.CONTACT_ROUTE]: [
    "official_business_email_phone",
    "event_contact_page",
    "department_route",
    "named_buyer_business_contact",
    "intermediary_route",
  ],
  [BLOCKER_KEY.CORROBORATION]: [
    "independent_secondary_source",
    "official_pdf",
    "venue_or_partner_confirmation",
    "parallel_targeted_followup",
  ],
  [BLOCKER_KEY.SOURCE_CONFLICT]: [
    "primary_source_reconciliation",
    "official_correction_check",
  ],
  [BLOCKER_KEY.DRAWER_COMPLETENESS]: ["derive_drawer_fields_from_evidence"],
});

/** Paid providers — blocked when budget/auth exhausted; public strategies may continue. */
export const PAID_STRATEGIES = Object.freeze(
  new Set([
    "professional_profile_role_resolution",
    "named_buyer_business_contact",
    "surfe_named_person_enrichment",
    "parallel_targeted_followup",
  ])
);

export const PARALLEL_STRATEGY = "parallel_targeted_followup";

export function isParallelStrategy(strategy) {
  return String(strategy || "") === PARALLEL_STRATEGY;
}

export function publicStrategiesForBlocker(blockerKey) {
  return (STRATEGIES[blockerKey] || []).filter((s) => !PAID_STRATEGIES.has(s));
}

export function hasTriedParallelFollowup(opportunity, blockerKey) {
  return (opportunity.researchAttempts || []).some(
    (a) =>
      a.blocker === blockerKey &&
      (isParallelStrategy(a.strategy) || a.provider === "parallel")
  );
}

/**
 * True when Parallel was completed *after* public strategies for the blocker
 * were exhausted. A premature Parallel run (before CVB/trade-press/recurrence)
 * must not block a fresh paid follow-up or claim NO_FUTURE.
 */
export function hasCompletedParallelFollowup(opportunity, blockerKey) {
  const attempts = opportunity.researchAttempts || [];
  const matchBlocker = (a) =>
    !blockerKey || !a.blocker || a.blocker === blockerKey;
  // Ignore REUSED stubs — their timestamps are re-stamp of "now" and would
  // falsely look like Parallel-after-public when the original run was premature.
  const parallelAttempts = attempts.filter(
    (a) =>
      a.provider === "parallel" &&
      a.providerRunId &&
      !a.reusedProviderRun &&
      String(a.disposition || "").toUpperCase() !== "BLOCKED" &&
      String(a.disposition || "").toUpperCase() !== "REUSED_PROVIDER_RUN" &&
      !String(a.failureReason || "").includes("not_allowed")
  );
  if (!parallelAttempts.length) return false;

  const publics = publicStrategiesForBlocker(blockerKey || "future_demand");
  const publicAts = attempts
    .filter(
      (a) =>
        matchBlocker(a) &&
        publics.includes(a.strategy) &&
        !isParallelStrategy(a.strategy)
    )
    .map((a) => Date.parse(a.at || 0) || 0);
  const lastPublicAt = publicAts.length ? Math.max(...publicAts) : 0;

  // Prefer same-blocker Parallel that is not superseded by later public work
  const same = parallelAttempts.some((a) => {
    if (!matchBlocker(a)) return false;
    const at = Date.parse(a.at || 0) || 0;
    return !(lastPublicAt > 0 && at > 0 && lastPublicAt > at);
  });
  if (same) return true;

  return parallelAttempts.some((a) => {
    const at = Date.parse(a.at || 0) || 0;
    return !(lastPublicAt > 0 && at > 0 && lastPublicAt > at);
  });
}

/** True when a prior Parallel for this strategy may be reused (not superseded). */
export function canReuseParallelProviderRun(opportunity, blockerKey, priorAttempt) {
  if (!priorAttempt?.providerRunId) return false;
  if (hasCompletedParallelFollowup(opportunity, blockerKey)) return true;
  // Premature Parallel before later public strategies — force resubmit
  return false;
}

/**
 * Strategies explicitly skipped as inapplicable / not economically justified
 * for this opportunity+blocker. Skips count toward exhaustion so the planner
 * does not mechanically burn irrelevant public strategies.
 */
export function skippedInapplicablePublicStrategies(opportunity, blockerKey) {
  const fromAttempts = (opportunity.researchAttempts || [])
    .filter(
      (a) =>
        a.blocker === blockerKey &&
        (a.disposition === "SKIPPED_INAPPLICABLE" ||
          a.disposition === "SKIPPED_NOT_ECONOMICALLY_JUSTIFIED")
    )
    .map((a) => a.strategy)
    .filter(Boolean);
  const fromMeta = (opportunity.skippedPublicStrategies || [])
    .filter((s) => !blockerKey || !s.blocker || s.blocker === blockerKey)
    .map((s) => s.strategy)
    .filter(Boolean);
  return [...new Set([...fromAttempts, ...fromMeta])];
}

/**
 * Record why a public strategy was not executed for this blocker.
 * Persisted on the opportunity so exhaustion audits stay evidence-backed.
 */
export function recordSkippedPublicStrategy(
  opportunity,
  { blocker, strategy, reason, kind = "SKIPPED_INAPPLICABLE" } = {}
) {
  if (!strategy) return opportunity;
  const entry = {
    blocker: blocker || "future_demand",
    strategy,
    reason: reason || kind,
    kind,
    at: new Date().toISOString(),
  };
  const prior = opportunity.skippedPublicStrategies || [];
  const dup = prior.some(
    (s) => s.strategy === strategy && s.blocker === entry.blocker
  );
  return {
    ...opportunity,
    skippedPublicStrategies: dup ? prior : [...prior, entry],
    researchAttempts: [
      ...(opportunity.researchAttempts || []),
      {
        at: entry.at,
        blocker: entry.blocker,
        strategy,
        provider: "none",
        disposition: kind,
        reason: entry.reason,
        newMaterialEvidence: false,
        costUsd: 0,
      },
    ],
  };
}

export function publicPathExhaustedForBlocker(opportunity, blockerKey) {
  const publics = publicStrategiesForBlocker(blockerKey);
  if (!publics.length) return false;
  const tried = new Set(
    (opportunity.researchAttempts || [])
      .filter((a) => a.blocker === blockerKey && !a.newMaterialEvidence)
      .map((a) => a.strategy)
  );
  const skipped = new Set(
    skippedInapplicablePublicStrategies(opportunity, blockerKey)
  );
  // Exhaustion = every applicable public strategy attempted or explicitly skipped.
  // Previously min(3, n) let Parallel / NO_FUTURE stop while CVB, trade-press,
  // and recurrence strategies remained untried — that is not research exhaustion.
  // Mechanical "run all six" without applicability is also wrong: skipped strategies
  // must carry an auditable reason (see recordSkippedPublicStrategy).
  const covered = publics.filter((s) => tried.has(s) || skipped.has(s)).length;
  return covered >= publics.length;
}

/** Public strategies still untried and not marked inapplicable for a blocker. */
export function remainingPublicStrategiesForBlocker(opportunity, blockerKey) {
  const publics = publicStrategiesForBlocker(blockerKey);
  const tried = new Set(
    (opportunity.researchAttempts || [])
      .filter((a) => a.blocker === blockerKey)
      .map((a) => a.strategy)
      .filter(Boolean)
  );
  const skipped = new Set(
    skippedInapplicablePublicStrategies(opportunity, blockerKey)
  );
  return publics.filter((s) => !tried.has(s) && !skipped.has(s));
}

export const STRATEGY_SOURCE_FAMILY = Object.freeze({
  official_future_events: "official_event",
  event_calendar: "association_calendar",
  exhibitor_or_sponsor_prospectus: "event_prospectus",
  cvb_convention_calendar: "city_destination",
  trade_press_future_announcement: "trade_press",
  historical_recurrence_analysis: "historical_recurrence",
  housing_block_or_booking_page: "housing_provider",
  official_attendance_or_rooms: "official_event",
  prospectus_attendance: "event_prospectus",
  prior_event_scale: "historical_recurrence",
  venue_capacity_and_format: "venue_calendar",
  official_staff_directory: "official_contact",
  event_team_page: "official_contact",
  association_management_company: "intermediary",
  professional_profile_role_resolution: "named_person",
  speaker_sponsor_contact: "sponsor_exhibitor_list",
  parallel_targeted_followup: "paid_followup",
  official_business_email_phone: "official_contact",
  event_contact_page: "official_contact",
  department_route: "department_contact",
  named_buyer_business_contact: "named_person",
  intermediary_route: "intermediary",
  independent_secondary_source: "trade_press",
  official_pdf: "official_document",
  venue_or_partner_confirmation: "venue_calendar",
  distance_and_catchment: "geography",
  event_venue_lodging_pattern: "housing_provider",
  room_block_pattern: "housing_provider",
  hotel_product_match: "hotel_fit",
  primary_source_reconciliation: "official_event",
  official_correction_check: "official_event",
  derive_drawer_fields_from_evidence: "internal",
});

const LODGING_FACT = /lodg|accommodat|room block|housing|overnight/i;

export function classifyResearchPage({ url = "", title = "", quote = "" } = {}) {
  const blob = `${url} ${title} ${quote}`.toLowerCase();
  if (/coming soon|applications (?:will |to )?open|not yet (?:published|available)/.test(blob)) {
    return "coming_soon";
  }
  if (/preview mode|must be published to be live/.test(blob)) return "preview_shell";
  if (/\/terms(?:\.php)?(?:$|\?)|terms of use/.test(blob)) return "terms_of_use";
  if (/buy-tickets|\/tickets(?:\/|$|\?)|ticket fees|groups of \d+\+/.test(blob)) return "ticket_commerce";
  const listed = classifyEvidenceSourcePage(url);
  if (listed === "calendar" || listed === "homepage") return listed;
  if (/\.pdf(?:$|\?)/i.test(url)) return "pdf";
  if (/\/contact(?:-us)?(?:\/|$|\?)|secr[eé]tariat|secretariat/.test(blob)) return "contact_page";
  if (/accommodat|housing|room block|travel & accommodation|travel and accommodation/.test(blob)) {
    return "housing_page";
  }
  return "document";
}

export function missingFactFor(blockerKey, opportunity = {}) {
  if (opportunity.missingFact) return String(opportunity.missingFact);
  if (blockerKey === BLOCKER_KEY.CONTACT_ROUTE) {
    return "A commercially relevant organization contact route for this prospect.";
  }
  if (blockerKey === BLOCKER_KEY.FUTURE_DEMAND) {
    return "Whether this dated occasion has an accommodation or lodging mechanism.";
  }
  return `Unresolved ${blockerKey || "research"} fact.`;
}

function credibleStrategyOrder(blockerKey, missingFact) {
  if (blockerKey === BLOCKER_KEY.FUTURE_DEMAND && LODGING_FACT.test(missingFact)) {
    return [
      "housing_block_or_booking_page",
      "exhibitor_or_sponsor_prospectus",
      "official_pdf",
      "historical_recurrence_analysis",
      "trade_press_future_announcement",
      "official_future_events",
      "cvb_convention_calendar",
    ];
  }
  if (blockerKey === BLOCKER_KEY.CONTACT_ROUTE) {
    const publics = publicStrategiesForBlocker(blockerKey);
    const first = [
      "event_contact_page",
      "official_business_email_phone",
      "department_route",
      "association_management_company",
      "intermediary_route",
    ];
    return [...first.filter((item) => publics.includes(item)), ...publics.filter((item) => !first.includes(item))];
  }
  return publicStrategiesForBlocker(blockerKey);
}

function inapplicableReason(strategy, blockerKey, missingFact) {
  if (blockerKey === BLOCKER_KEY.FUTURE_DEMAND && LODGING_FACT.test(missingFact) && strategy === "event_calendar") {
    return "A calendar listing does not establish a lodging mechanism.";
  }
  return "";
}

export function hypothesisFor({ opportunity = {}, blockerKey, strategy, language = "en" } = {}) {
  const missingFact = missingFactFor(blockerKey, opportunity);
  const family = STRATEGY_SOURCE_FAMILY[strategy] || "unclassified";
  const org = opportunity.organizationName || opportunity.name || "the organization";
  return {
    missingFact,
    hypothesis: `${strategy} can establish whether ${org} satisfies: ${missingFact}`,
    sourceFamily: family,
    language,
    negativeResultMeans: `This ${family} source does not establish the missing fact. It does not close the blocker by itself.`,
  };
}

function foldText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .toLowerCase();
}

export function normalizeResearchUrl(value) {
  try {
    const url = new URL(String(value || ""));
    url.hash = "";
    const host = url.hostname.toLowerCase().replace(/^www\./, "");
    return `${url.protocol}//${host}${url.pathname.replace(/\/$/, "")}${url.search}`.toLowerCase();
  } catch {
    return "";
  }
}

function urlWasRead(attempt) {
  return Boolean(attempt?.historical || ["contents", "scrape", "parse"].includes(attempt?.operation));
}

export function equivalentEvidenceAlreadyHeld(attempts = [], { sourceUrl, contentHash } = {}) {
  const normalized = normalizeResearchUrl(sourceUrl);
  return attempts.some((attempt) => {
    if (!attempt?.evidenceQuote || attempt.outcome === "skipped_duplicate") return false;
    if (contentHash && attempt.contentHash && attempt.contentHash === contentHash) return true;
    return Boolean(normalized && normalizeResearchUrl(attempt.sourceUrl) === normalized);
  });
}

export function isDiminishingRepeat(attempts = [], { fingerprint, sourceUrl, contentHash } = {}) {
  const normalized = normalizeResearchUrl(sourceUrl);
  return attempts.some((attempt) => {
    if (attempt.disposition === "REOPENED") return false;
    if (attempt.newMaterialEvidence) return false;
    if (fingerprint && attempt.fingerprint === fingerprint) return true;
    if (contentHash && attempt.contentHash && attempt.contentHash === contentHash) return true;
    if (
      normalized &&
      urlWasRead(attempt) &&
      normalizeResearchUrl(attempt.sourceUrl) === normalized &&
      attempt.outcome !== "provider_failed"
    ) {
      return true;
    }
    return false;
  });
}

const REJECTED_PAGE_CLASSES = new Set([
  "terms_of_use",
  "ticket_commerce",
  "preview_shell",
  "homepage",
  "calendar",
  "coming_soon",
]);

export function selectUnreadRelevantSource(opportunity = {}, blockerKey, language = "en") {
  const read = new Set(
    (opportunity.researchAttempts || [])
      .filter((attempt) => urlWasRead(attempt))
      .map((attempt) => normalizeResearchUrl(attempt.sourceUrl))
      .filter(Boolean)
  );
  const tokens = foldText(opportunity.organizationName || opportunity.name)
    .split(/[^a-z0-9]+/)
    .filter((token) => token.length >= 4);
  let best = null;
  for (const row of opportunity.discoveredCandidates || []) {
    const url = normalizeResearchUrl(row.url);
    if (!url || read.has(url)) continue;
    const pageClass = classifyResearchPage({ url: row.url, title: row.title, quote: row.excerpt });
    if (REJECTED_PAGE_CLASSES.has(pageClass)) continue;
    const blob = foldText(`${row.url} ${row.title} ${row.excerpt}`);
    let score = tokens.some((token) => blob.includes(token)) ? 2 : 0;
    if (blockerKey === BLOCKER_KEY.CONTACT_ROUTE) {
      if (pageClass === "contact_page") score += 4;
      if (/contact|secretariat|equipe/.test(blob)) score += 2;
      if (language === "fr" && (/\/fr(?:\/|$)/.test(url) || /secretariat|telephone/.test(blob))) score += 3;
      if (pageClass === "pdf") score -= 3;
    }
    if (blockerKey === BLOCKER_KEY.FUTURE_DEMAND) {
      if (pageClass === "housing_page") score += 4;
      if (/room block|accommodation|housing|hebergement/.test(blob)) score += 3;
    }
    if (!best || score > best.score) best = { url: row.url, pageClass, score };
  }
  return best && best.score >= 4 ? best : null;
}

export function assessResearchExhaustion({ opportunity = {}, blockerKey, language = "en" } = {}) {
  const attempts = opportunity.researchAttempts || [];
  const familiesEvaluated = [
    ...new Set(
      attempts
        .filter((attempt) => attempt.outcome !== "skipped_duplicate" && attempt.countsAsResearchDepth !== false)
        .map((attempt) => attempt.sourceFamily)
        .filter(Boolean)
    ),
  ];
  const urlsRead = [
    ...new Set(
      attempts
        .filter((attempt) => urlWasRead(attempt))
        .map((attempt) => attempt.sourceUrl)
        .filter(Boolean)
    ),
  ];
  const languagesUsed = [...new Set(attempts.map((attempt) => attempt.language).filter(Boolean))];
  const unread = selectUnreadRelevantSource(opportunity, blockerKey, language);
  const thinPage = attempts.find((attempt) => {
    if (attempt.operation !== "contents") return false;
    if (String(attempt.evidenceQuote || "").length >= 200) return false;
    if (REJECTED_PAGE_CLASSES.has(attempt.pageClass)) return false;
    const url = normalizeResearchUrl(attempt.sourceUrl);
    if (!url) return false;
    return !attempts.some(
      (other) => other.operation === "scrape" && normalizeResearchUrl(other.sourceUrl) === url
    );
  });
  const frenchMissing =
    language === "fr" &&
    blockerKey === BLOCKER_KEY.CONTACT_ROUTE &&
    !attempts.some((attempt) => attempt.language === "fr" && attempt.strategy && attempt.outcome !== "skipped");
  const materiallyDifferent = Boolean(unread || thinPage || frenchMissing);
  const rationale = materiallyDifferent
    ? "A materially different unread source, thin official page, or required language query remains."
    : "No unread relevant URL remains, the required language was used, and no thin official page still needs another operation. Ending the fixed strategy list is recorded here and is not the proof of exhaustion.";
  return {
    noMateriallyDifferentStrategy: !materiallyDifferent,
    familiesEvaluated,
    urlsRead,
    languagesUsed,
    languagesRequired: [language || "en"],
    unreadRelevantUrl: unread?.url || null,
    thinPageUrl: thinPage?.sourceUrl || null,
    frenchQueryMissing: frenchMissing,
    fixedListEnded: true,
    rationale,
  };
}

export function strategyFingerprint({ blocker, strategy, queries = [] }) {
  const q = queries
    .map((x) =>
      String(x || "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim()
    )
    .sort()
    .join("|");
  return `${blocker}::${strategy}::${q}`;
}

function attemptedFingerprints(opportunity) {
  return new Set(
    (opportunity.researchAttempts || [])
      .filter((a) => a && a.fingerprint && a.disposition !== "REOPENED")
      .map((a) => a.fingerprint)
  );
}

function attemptedStrategies(opportunity, blocker) {
  return new Set(
    (opportunity.researchAttempts || [])
      .filter((a) => a && a.blocker === blocker && !a.newMaterialEvidence)
      .map((a) => a.strategy)
  );
}

function quote(s) {
  return `"${String(s || "").replaceAll('"', "")}"`;
}

export class HeuristicResearchPlanner {
  plan({ opportunity, blocker, options = {} }) {
    const paidBlocked = Boolean(options.paidBlocked);
    const tried = attemptedStrategies(opportunity, blocker.key);
    const fps = attemptedFingerprints(opportunity);
    const reopen = new Set(opportunity.reopenStrategies || []);
    const strategies = (STRATEGIES[blocker.key] || ["broad_evidence_search"]).filter(
      (s) => !(paidBlocked && PAID_STRATEGIES.has(s))
    );

    // 1) Untried strategies that were explicitly reopened (remaining public)
    // 2) Explicitly reopened strategies even if previously tried (new-evidence path)
    // 3) Any other untried strategy
    // Prefer (1) before (2) so a partially-complete reopen list (e.g. trade_press
    // already tried + historical still open) does not burn iterations redoing
    // trade_press.
    let strategy =
      strategies.find((s) => reopen.has(s) && !tried.has(s)) ||
      strategies.find((s) => reopen.has(s)) ||
      strategies.find((s) => !tried.has(s)) ||
      strategies.find((s) => {
        const org = opportunity.organizationName || opportunity.name;
        const q = this.queriesFor({
          strategy: s,
          org,
          market: opportunity.hotel?.market || opportunity.hotel?.city || "",
          yearWindow: opportunity.targetYears?.join(" ") || "2027 2028 2029",
        });
        return !fps.has(
          strategyFingerprint({ blocker: blocker.key, strategy: s, queries: q })
        );
      });

    if (!strategy) {
      return {
        strategy: null,
        queries: [],
        provider: "none",
        skipReason: "ALL_STRATEGIES_ATTEMPTED",
        paid: false,
      };
    }

    if (tried.has(strategy) && !reopen.has(strategy)) {
      const alt = strategies.find((s) => !tried.has(s) || reopen.has(s));
      if (!alt) {
        return {
          strategy: null,
          queries: [],
          provider: "none",
          skipReason: "DUPLICATE_STRATEGY_REJECTED",
          paid: false,
        };
      }
      strategy = alt;
    }

    const org = opportunity.organizationName || opportunity.name;
    const market = opportunity.hotel?.market || opportunity.hotel?.city || "";
    const yearWindow = opportunity.targetYears?.join(" ") || "2027 2028 2029";
    const queries = this.queriesFor({ strategy, org, market, yearWindow, opportunity, language: options.language });
    const fingerprint = strategyFingerprint({
      blocker: blocker.key,
      strategy,
      queries,
    });

    if (fps.has(fingerprint) && !reopen.has(strategy)) {
      return {
        strategy: null,
        queries: [],
        provider: "none",
        skipReason: "DUPLICATE_STRATEGY_REJECTED",
        fingerprint,
        paid: false,
      };
    }

    const described = hypothesisFor({
      opportunity,
      blockerKey: blocker.key,
      strategy,
      language: options.language || "en",
    });
    return {
      strategy,
      queries,
      provider: "router",
      fingerprint,
      paid: PAID_STRATEGIES.has(strategy),
      ...described,
      operation: "search",
    };
  }

  planAdaptiveStep({ opportunity, blocker, options = {} }) {
    const blockerKey = blocker?.key;
    const language = options.language || opportunity.researchLanguage || opportunity.language || "en";
    const missingFact = missingFactFor(blockerKey, opportunity);
    const attempts = opportunity.researchAttempts || [];
    const fps = attemptedFingerprints(opportunity);
    const skipped = new Set(skippedInapplicablePublicStrategies(opportunity, blockerKey));
    const order = credibleStrategyOrder(blockerKey, missingFact);
    const newlySkipped = [];
    if (blockerKey === BLOCKER_KEY.FUTURE_DEMAND && LODGING_FACT.test(missingFact) && !skipped.has("event_calendar")) {
      skipped.add("event_calendar");
      newlySkipped.push({ strategy: "event_calendar", reason: inapplicableReason("event_calendar", blockerKey, missingFact) });
    }
    const credible = order.filter((strategy) => !skipped.has(strategy) && !PAID_STRATEGIES.has(strategy));
    const unread = selectUnreadRelevantSource(opportunity, blockerKey, language);
    if (unread) {
      const fingerprint = `${blockerKey}::contents::${normalizeResearchUrl(unread.url)}`;
      if (!fps.has(fingerprint)) {
        const strategy = blockerKey === BLOCKER_KEY.CONTACT_ROUTE ? "event_contact_page" : "housing_block_or_booking_page";
        return {
          strategy,
          queries: [],
          urls: [unread.url],
          operation: "contents",
          fingerprint,
          paid: false,
          skipped: newlySkipped,
          remainingStrategies: credible,
          disposition: null,
          countryHint: options.countryHint || opportunity.countryHint || "",
          ...hypothesisFor({ opportunity, blockerKey, strategy, language }),
          hypothesis: `The unread ${unread.pageClass} page may contain the missing fact.`,
          sourceFamily: unread.pageClass,
        };
      }
    }
    let chosen = null;
    for (const strategy of credible) {
      const org = opportunity.organizationName || opportunity.name;
      const queries = this.queriesFor({
        strategy,
        org,
        market: opportunity.hotel?.market || opportunity.hotel?.city || "",
        yearWindow: opportunity.targetYears?.join(" ") || "2027 2028 2029",
        opportunity,
        language,
      });
      const fingerprint = strategyFingerprint({ blocker: blockerKey, strategy, queries });
      if (fps.has(fingerprint) || isDiminishingRepeat(attempts, { fingerprint })) continue;
      chosen = { strategy, queries, fingerprint };
      break;
    }
    const remaining = credible.filter((strategy) => {
      if (!chosen) return !attempts.some((attempt) => attempt.strategy === strategy && attempt.blocker === blockerKey);
      return strategy !== chosen.strategy && !attempts.some((attempt) => attempt.strategy === strategy && attempt.blocker === blockerKey);
    });
    if (!chosen) {
      const refresh = attempts.find((attempt) => attempt.outcome === "refresh" || attempt.pageClass === "coming_soon" || attempt.pageClass === "preview_shell");
      const exhaustionAssessment = assessResearchExhaustion({ opportunity, blockerKey, language });
      if (!exhaustionAssessment.noMateriallyDifferentStrategy && exhaustionAssessment.thinPageUrl) {
        const fingerprint = `${blockerKey}::scrape::${normalizeResearchUrl(exhaustionAssessment.thinPageUrl)}`;
        if (!fps.has(fingerprint)) {
          return {
            strategy: "official_pdf",
            queries: [],
            urls: [exhaustionAssessment.thinPageUrl],
            operation: "scrape",
            fingerprint,
            paid: false,
            skipped: newlySkipped,
            remainingStrategies: [],
            disposition: null,
            countryHint: options.countryHint || opportunity.countryHint || "",
            ...hypothesisFor({ opportunity, blockerKey, strategy: "official_pdf", language }),
            hypothesis: "The official page text was too thin to judge. A rendered read may still contain the missing fact.",
          };
        }
      }
      if (!exhaustionAssessment.noMateriallyDifferentStrategy && exhaustionAssessment.frenchQueryMissing) {
        const strategy = "event_contact_page";
        const queries = this.queriesFor({
          strategy,
          org: opportunity.organizationName || opportunity.name,
          market: opportunity.hotel?.market || opportunity.hotel?.city || "",
          yearWindow: opportunity.targetYears?.join(" ") || "2027 2028 2029",
          language: "fr",
        });
        const fingerprint = strategyFingerprint({ blocker: blockerKey, strategy, queries });
        if (!fps.has(fingerprint)) {
          return {
            strategy,
            queries,
            operation: "search",
            fingerprint,
            paid: false,
            skipped: newlySkipped,
            remainingStrategies: [],
            disposition: null,
            language: "fr",
            countryHint: options.countryHint || opportunity.countryHint || "",
            ...hypothesisFor({ opportunity, blockerKey, strategy, language: "fr" }),
          };
        }
      }
      if (!refresh && !exhaustionAssessment.noMateriallyDifferentStrategy) {
        return {
          strategy: null,
          queries: [],
          fingerprint: null,
          paid: false,
          skipped: newlySkipped,
          remainingStrategies: [],
          disposition: INVESTIGATION_DISPOSITION.NEEDS_HUMAN_REVIEW,
          exhaustionAssessment,
          ...hypothesisFor({ opportunity, blockerKey, strategy: "none", language }),
        };
      }
      return {
        strategy: null,
        queries: [],
        fingerprint: null,
        paid: false,
        skipped: newlySkipped,
        remainingStrategies: [],
        disposition: refresh
          ? INVESTIGATION_DISPOSITION.SCHEDULED_REFRESH
          : INVESTIGATION_DISPOSITION.RESEARCH_STRATEGIES_EXHAUSTED,
        exhaustionAssessment,
        refreshCandidate: refresh
          ? { sourceUrl: refresh.sourceUrl || null, pageClass: refresh.pageClass || null, reason: refresh.notes || refresh.outcome }
          : null,
        ...hypothesisFor({ opportunity, blockerKey, strategy: "none", language }),
      };
    }
    const described = hypothesisFor({ opportunity, blockerKey, strategy: chosen.strategy, language });
    return {
      ...chosen,
      ...described,
      provider: "benchmark",
      operation: "search",
      paid: false,
      skipped: newlySkipped,
      remainingStrategies: remaining,
      disposition: null,
      countryHint: options.countryHint || opportunity.countryHint || "",
    };
  }

  queriesFor({ strategy, org, market, yearWindow, language }) {
    const qOrg = quote(org);
    if (language === "fr" && /contact|official_business_email_phone|department_route|intermediary_route|event_team|staff_directory/.test(strategy)) {
      return [`${qOrg} secrétariat contact téléphone`, `${qOrg} contact officiel`];
    }
    if (language === "es" && /contact|official_business_email_phone|department_route|intermediary_route|event_team|staff_directory/.test(strategy)) {
      return [`${qOrg} contacto teléfono secretaría`, `${qOrg} contacto oficial`];
    }
    switch (strategy) {
      case "official_future_events":
        return [`${qOrg} annual meeting ${yearWindow}`, `${qOrg} future events ${yearWindow}`];
      case "event_calendar":
        return [
          `${qOrg} conference calendar ${yearWindow}`,
          `${qOrg} events ${market} ${yearWindow}`,
        ];
      case "exhibitor_or_sponsor_prospectus":
        return [
          `${qOrg} exhibitor prospectus ${yearWindow} pdf`,
          `${qOrg} sponsorship prospectus ${yearWindow} pdf`,
        ];
      case "cvb_convention_calendar":
        return [`${qOrg} ${market} convention`, `${qOrg} ${market} conference`];
      case "trade_press_future_announcement":
        return [
          `${qOrg} announces ${market} conference`,
          `${qOrg} annual meeting venue ${yearWindow}`,
        ];
      case "historical_recurrence_analysis":
        return [`${qOrg} annual conference history`, `${qOrg} previous venues hotels`];
      case "official_attendance_or_rooms":
        return [`${qOrg} attendees rooms hotel block`, `${qOrg} conference attendance`];
      case "housing_block_or_booking_page":
        return [
          `${qOrg} hotel booking housing ${yearWindow}`,
          `${qOrg} room block ${yearWindow}`,
        ];
      case "prospectus_attendance":
        return [`${qOrg} prospectus attendees delegates pdf`];
      case "prior_event_scale":
        return [
          `${qOrg} attendance delegates previous conference`,
          `${qOrg} hotel rooms conference`,
        ];
      case "venue_capacity_and_format":
        return [`${qOrg} ${market} venue delegates reception dinner`];
      case "official_staff_directory":
        return [`${qOrg} meetings director`, `${qOrg} events director`];
      case "event_team_page":
        return [`${qOrg} conference team contact`, `${qOrg} event contact`];
      case "association_management_company":
        return [`${qOrg} association management company`, `${qOrg} managed by`];
      case "professional_profile_role_resolution":
        return [`${qOrg} "Director of Events"`, `${qOrg} "Head of Events"`];
      case "speaker_sponsor_contact":
        return [`${qOrg} sponsorship contact`, `${qOrg} exhibitor contact`];
      case "parallel_targeted_followup":
        return [
          `${qOrg} ${market} future demand lodging ${yearWindow}`,
          `${qOrg} official sources hotel housing contact`,
        ];
      case "official_business_email_phone":
        return [`${qOrg} contact email phone`, `${qOrg} events contact email`];
      case "event_contact_page":
        return [`${qOrg} event enquiries`, `${qOrg} conference enquiries`];
      case "department_route":
        return [`${qOrg} events department`, `${qOrg} meetings team`];
      case "named_buyer_business_contact":
        return [`${qOrg} event manager email`, `${qOrg} meetings director email`];
      case "intermediary_route":
        return [`${qOrg} event agency`, `${qOrg} housing partner`];
      case "independent_secondary_source":
        return [`${qOrg} ${market} event`, `${qOrg} conference ${market}`];
      case "official_pdf":
        return [`${qOrg} ${yearWindow} filetype:pdf`];
      case "venue_or_partner_confirmation":
        return [`${qOrg} ${market} venue`, `${qOrg} ${market} hotel`];
      case "distance_and_catchment":
        return [`${qOrg} ${market} venue address`];
      case "event_venue_lodging_pattern":
        return [`${qOrg} ${market} official hotel`, `${qOrg} housing ${market}`];
      case "room_block_pattern":
        return [`${qOrg} room block ${market}`];
      case "hotel_product_match":
        return [`${qOrg} delegates luxury hotel ${market}`];
      case "primary_source_reconciliation":
        return [`${qOrg} official dates venue`, `${qOrg} cancelled postponed`];
      case "official_correction_check":
        return [`${qOrg} correction notice`, `${qOrg} venue change`];
      case "derive_drawer_fields_from_evidence":
        return [];
      default:
        return [`${qOrg} ${market}`];
    }
  }
}

export { STRATEGIES };
