/**
 * Research-action contract V1 (run-051).
 *
 * Guarantees agreement between commercial objective, research question,
 * target entity, investigation strategy, evidence sought, and executed query.
 *
 * Strategy identifiers are NEVER entity names.
 */

import { assessCommercialResearchState } from "./commercial-research-assessment-v1.js";
import { applyAssociationEventsGoldBrief } from "./association-events-gold-brief-v1.js";
import { applyScalingCommercialIntelligenceBrief } from "../scaling-commercial-intelligence-brief-v1.js";
import { maybeExaSearch } from "./deep-research-gdi-live-certification-v1.js";
import { fetchOfficialPage } from "../official-page-fetch.js";
import { extractPublicEvidenceFromText } from "./direct-public-executor-v1.js";
import {
  buildHotelWinThesisFromEvidence,
  applyHotelWinThesisToOpportunity,
} from "./hotel-win-thesis-from-evidence-v1.js";
import {
  filterPressPublisherOrgRoute,
  isPressPublisherNonCompanyRoute,
} from "../press-publisher-entity-v1.js";

export const RESEARCH_ACTION_CONTRACT_V1 = "gdi-research-action-contract-v1";

export const RESEARCH_STRATEGIES = Object.freeze({
  PROPERTY_SPECIFIC_FIT_FROM_GEO_AND_PROGRAMME:
    "PROPERTY_SPECIFIC_FIT_FROM_GEO_AND_PROGRAMME",
  OFFICIAL_CONTACT_ROUTE_DISCOVERY: "OFFICIAL_CONTACT_ROUTE_DISCOVERY",
  ORGANIZER_CONTACT_OR_FUNCTIONAL_ROUTE: "ORGANIZER_CONTACT_OR_FUNCTIONAL_ROUTE",
  SECRETARIAT_OR_PCO_ROUTE: "SECRETARIAT_OR_PCO_ROUTE",
  ACCOMMODATION_OR_PROGRAMME_EVIDENCE: "ACCOMMODATION_OR_PROGRAMME_EVIDENCE",
  HOUSING_BUREAU_OR_HOST_HOTEL_SEARCH: "HOUSING_BUREAU_OR_HOST_HOTEL_SEARCH",
  EDITION_CHRONOLOGY_AND_FUTURE_CYCLE: "EDITION_CHRONOLOGY_AND_FUTURE_CYCLE",
  ORGANIZER_OFFICIAL_EVENT_PAGE: "ORGANIZER_OFFICIAL_EVENT_PAGE",
  INDEPENDENT_PRESS_CORROBORATION: "INDEPENDENT_PRESS_CORROBORATION",
  ASSOCIATION_CALENDAR_OR_CALL_FOR_PAPERS: "ASSOCIATION_CALENDAR_OR_CALL_FOR_PAPERS",
  PRIMARY_SOURCE_RECOVERY: "PRIMARY_SOURCE_RECOVERY",
  GOLD_BRIEF_INTEGRITY_REPAIR: "GOLD_BRIEF_INTEGRITY_REPAIR",
  HEURISTIC_ORG_PATH: "HEURISTIC_ORG_PATH",
  SCHEDULE_FUTURE_REFRESH: "SCHEDULE_FUTURE_REFRESH",
  /** Exa Agent — Scaling discovery (authorized multi-step; not default). */
  EXA_AGENT_SCALING_DISCOVERY: "EXA_AGENT_SCALING_DISCOVERY",
  /** Exa Agent — Scaling deep commercial research dossier (authorized). */
  EXA_AGENT_SCALING_DEEP_RESEARCH: "EXA_AGENT_SCALING_DEEP_RESEARCH",
});

const STRATEGY_ID_SET = new Set(Object.values(RESEARCH_STRATEGIES));

const CONTACT_FAMILY =
  /CONTACT|ROUTE|SECRETARIAT|BUYER|OUTREACH|PCO/i;
const FIT_FAMILY =
  /PROPERTY_SPECIFIC_FIT|ACCOMMODATION_OR_PROGRAMME|HOUSING_BUREAU|FIT/i;
const CHRONO_FAMILY = /EDITION_CHRONOLOGY|FUTURE_CYCLE|CHRONOLOGY/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Decide which gold brief to regenerate after research-action evidence merge.
 * run-052 Issue 3: identity-based (brief / pathway / segment family), not a
 * narrow regex that skipped medical_academic Association rows.
 *
 * @returns {"association"|"scaling"|"none"}
 */
export function classifyResearchActionBriefKind(opportunity = {}) {
  if (opportunity.associationEventsGoldBrief) return "association";
  if (opportunity.commercialIntelligenceBrief && !opportunity.associationEventsGoldBrief) {
    // Scaling CIB alone → scaling, unless Association pathway markers dominate.
  }
  const blob = [
    opportunity.researchSegment,
    opportunity.segment,
    opportunity.segmentFamily,
    opportunity.pathway,
    opportunity.demandProductType,
    opportunity.opportunityType,
    opportunity.detailLayout,
    opportunity.gdi2ContinuationDispositionV1?.commercial?.segmentFamily,
  ]
    .map(text)
    .join(" ")
    .toLowerCase();

  const associationHit =
    /\b(assoc|association|event|events|government|medical|academic|congress|conference|meeting|symposium|congreso)\b/i.test(
      blob,
    ) ||
    /association_events|associations_events|medical_academic|ASSOCIATION_CONFERENCE/i.test(blob);

  const scalingHit =
    /\b(scaling|corporate_account|scaling_companies)\b/i.test(blob) ||
    /CORPORATE_ACCOUNT_RELATIONSHIP/i.test(blob);

  if (associationHit && !scalingHit) return "association";
  if (scalingHit && !associationHit) return "scaling";
  if (associationHit && scalingHit) {
    // Prefer existing brief identity when both markers present
    if (opportunity.associationEventsGoldBrief) return "association";
    if (opportunity.commercialIntelligenceBrief) return "scaling";
    return "association";
  }
  if (opportunity.commercialIntelligenceBrief) return "scaling";
  return "none";
}

export function isResearchStrategyIdentifier(value) {
  const t = text(value);
  return Boolean(t) && STRATEGY_ID_SET.has(t);
}

export function resolveResearchTargetEntity(opportunity = {}) {
  const candidates = [
    opportunity.organizationName,
    opportunity.organizer,
    opportunity.eventName,
    opportunity.canonicalEventDisplayTitle,
    opportunity.companyName,
  ]
    .map(text)
    .filter(Boolean);
  const rejectedStrategyIds = candidates.filter(isResearchStrategyIdentifier);
  if (rejectedStrategyIds.length && rejectedStrategyIds.length === candidates.length) {
    const err = new Error("strategy_identifier_misused_as_entity");
    err.code = "STRATEGY_AS_ENTITY";
    err.rejected = rejectedStrategyIds;
    throw err;
  }
  for (const c of candidates) {
    if (!isResearchStrategyIdentifier(c)) return c;
  }
  return "unknown_organization";
}

export function isContactPageUrl(url = "") {
  return /\/(contact|contacts|enquire|inquiry|enquiries|meetings|groups|group-sales|sales|book|booking)(\/|$|\?)/i.test(
    String(url || ""),
  );
}

export function isHomepageOnlyUrl(url = "") {
  try {
    const u = new URL(String(url));
    return u.pathname === "/" || u.pathname === "";
  } catch {
    return false;
  }
}

/**
 * Build a coherent research action for a strategy. Regenerates question +
 * evidenceSought + query whenever strategy is selected or changed.
 */
export function buildCoherentResearchAction({
  strategy,
  opportunity = {},
  hotel = {},
  priorQuestion = null,
} = {}) {
  const targetEntity = resolveResearchTargetEntity(opportunity);
  const hotelName = text(hotel.displayName || opportunity.hotelName) || "the target hotel";
  const city = text(hotel.city || opportunity.city) || "";
  const eventLabel =
    text(opportunity.eventName || opportunity.canonicalEventDisplayTitle) ||
    targetEntity;
  const strat = text(strategy) || RESEARCH_STRATEGIES.PRIMARY_SOURCE_RECOVERY;

  let question = null;
  let evidenceSought = null;
  let query = null;
  let objective = null;

  if (FIT_FAMILY.test(strat)) {
    objective = "Establish property-specific commercial fit beyond generic city co-location";
    question = `Why would ${hotelName} specifically compete for ${eventLabel} given programme, geography, and lodging needs?`;
    evidenceSought =
      "programme/venue geography, lodging or meeting mechanism, and property-specific advantages vs generic facilities";
    query = `${eventLabel} ${city} hotel lodging accommodation venue housing meetings ${hotelName}`.trim();
  } else if (CONTACT_FAMILY.test(strat)) {
    objective = "Recover an official organization outreach route for hotel sales";
    question = `What official contact, meetings, or secretariat route exists for ${targetEntity} hotel sales outreach?`;
    evidenceSought =
      "official /contact, meetings, groups, or secretariat page — not heuristic /about|/team guesses";
    query = `${targetEntity} official contact meetings groups hotel sales ${city}`.trim();
  } else if (CHRONO_FAMILY.test(strat)) {
    objective = "Confirm whether a future edition or cycle is announced";
    question = `Has ${eventLabel} announced a confirmed next edition or future cycle with dates/city?`;
    evidenceSought = "authoritative announcement of next edition dates, city, or call for papers";
    query = `${eventLabel} ${new Date().getFullYear() + 1} OR 2027 OR 2028 next edition dates city call for papers`.trim();
  } else if (/ACCOMMODATION|HOUSING|PROGRAMME/i.test(strat)) {
    objective = "Find lodging / housing / programme evidence for hotel demand";
    question = `What lodging, housing bureau, or host-hotel arrangements are published for ${eventLabel}?`;
    evidenceSought = "housing bureau, host hotel, room block, or official lodging page";
    query = `${eventLabel} housing hotel room block host hotel accommodation ${city}`.trim();
  } else if (/INDEPENDENT_PRESS|PRIMARY_SOURCE|ORGANIZER_OFFICIAL|CALENDAR|CALL_FOR/i.test(strat)) {
    objective = "Recover independent or official corroboration of the activity";
    question = `Which official or independent source establishes ${eventLabel} identity and commercial activity?`;
    evidenceSought = "official organizer page or independent press corroboration";
    query = `${eventLabel} ${targetEntity} ${city} official OR conference OR congress`.trim();
  } else if (strat === RESEARCH_STRATEGIES.SCHEDULE_FUTURE_REFRESH) {
    objective = "Schedule future refresh — public evidence exhausted for this wave";
    question =
      priorQuestion ||
      "Public evidence exhausted for this wave — schedule future refresh.";
    evidenceSought = null;
    query = null;
  } else {
    objective = `Execute research strategy ${strat}`;
    question =
      priorQuestion ||
      `What commercially material public fact remains for ${targetEntity}?`;
    evidenceSought = `Evidence relevant to ${strat}`;
    query = `${targetEntity} ${eventLabel} ${city}`.trim();
  }

  const action = {
    version: RESEARCH_ACTION_CONTRACT_V1,
    objective,
    question,
    strategy: strat,
    targetEntity,
    evidenceSought,
    query,
    hotelName,
    executable: strat !== RESEARCH_STRATEGIES.SCHEDULE_FUTURE_REFRESH,
    paidCalls: /INDEPENDENT_PRESS|PRIMARY_SOURCE|ACCOMMODATION|HOUSING|FIT|CONTACT|CHRONOLOGY/i.test(
      strat,
    )
      ? 1
      : 0,
  };
  assertResearchActionCoherent(action);
  return action;
}

/**
 * Coherence invariant: strategy, question, entity, evidence, query agree.
 */
export function assertResearchActionCoherent(action = {}) {
  const strategy = text(action.strategy);
  const question = text(action.question);
  const entity = text(action.targetEntity);
  if (!strategy) {
    const err = new Error("missing_strategy");
    err.code = "INCOHERENT_RESEARCH_ACTION";
    throw err;
  }
  if (isResearchStrategyIdentifier(entity)) {
    const err = new Error("strategy_identifier_misused_as_entity");
    err.code = "STRATEGY_AS_ENTITY";
    throw err;
  }
  if (action.executable !== false && !question) {
    const err = new Error("executable_action_missing_question");
    err.code = "INCOHERENT_RESEARCH_ACTION";
    throw err;
  }
  // Question family must match strategy family when both are contact/fit/chrono
  if (FIT_FAMILY.test(strategy) && CONTACT_FAMILY.test(question) && !/fit|compete|hotel|lodging|programme/i.test(question)) {
    const err = new Error("fit_strategy_with_contact_question");
    err.code = "INCOHERENT_RESEARCH_ACTION";
    throw err;
  }
  if (
    CONTACT_FAMILY.test(strategy) &&
    /why would .+ specifically compete/i.test(question)
  ) {
    const err = new Error("contact_strategy_with_fit_question");
    err.code = "INCOHERENT_RESEARCH_ACTION";
    throw err;
  }
  if (action.query && isResearchStrategyIdentifier(action.query)) {
    const err = new Error("strategy_identifier_used_as_query");
    err.code = "INCOHERENT_RESEARCH_ACTION";
    throw err;
  }
  return true;
}

/**
 * Prefer the top commercial gap. Heuristic /about|/team exhaustion may only
 * redirect contact-family strategies — never silently convert hotel-fit into
 * contact discovery while keeping a fit question.
 */
export function selectCoherentAdaptedStrategy({
  assessment,
  failedStrategies = [],
  heuristicOrgPathsExhausted = false,
} = {}) {
  const gaps = assessment?.gaps || [];
  let strategy = assessment?.nextStrategy || null;
  const failed = new Set(
    failedStrategies.map((s) => String(s || "").toUpperCase()).filter(Boolean),
  );

  if (heuristicOrgPathsExhausted) {
    failed.add("HEURISTIC_ORG_PATH");
    // Only redirect when the top commercial gap is contact-family.
    if (CONTACT_FAMILY.test(strategy || "") || CONTACT_FAMILY.test(gaps[0]?.code || "")) {
      strategy = RESEARCH_STRATEGIES.OFFICIAL_CONTACT_ROUTE_DISCOVERY;
      if (failed.has(strategy)) {
        strategy = RESEARCH_STRATEGIES.SECRETARIAT_OR_PCO_ROUTE;
      }
      if (failed.has(strategy)) {
        strategy = RESEARCH_STRATEGIES.INDEPENDENT_PRESS_CORROBORATION;
      }
    }
    // else keep hotel-fit / chronology / demand strategy from assessment
  }

  if (strategy && failed.has(String(strategy).toUpperCase())) {
    // Pick next gap whose strategy is not failed
    const alt = gaps.find(
      (g) => g.strategy && !failed.has(String(g.strategy).toUpperCase()),
    );
    strategy = alt?.strategy || null;
  }

  if (!strategy && gaps.length > 0) {
    strategy = RESEARCH_STRATEGIES.SCHEDULE_FUTURE_REFRESH;
  }
  return {
    strategy,
    failedStrategies: [...failed],
    topGapCode: gaps[0]?.code || null,
  };
}

function mergeSource(opportunity, hit = {}) {
  const url = text(hit.url || hit.id);
  if (!url || !/^https?:\/\//i.test(url)) return opportunity;
  const sources = [...(opportunity.sources || [])];
  if (sources.some((s) => text(s.url || s) === url)) return opportunity;
  sources.push({
    url,
    title: hit.title || null,
    excerpt: text(hit.text || hit.excerpt || "").slice(0, 500) || null,
    provider: hit.provider || "exa",
    retrievedAt: new Date().toISOString(),
  });
  return { ...opportunity, sources };
}

function applyStrategyEvidence(opportunity, action, { hits = [], pages = [] } = {}) {
  let next = { ...opportunity };
  const blob = [
    ...hits.map((h) => `${h.title || ""} ${h.text || h.excerpt || ""}`),
    ...pages.map((p) => p.text || ""),
  ]
    .join("\n")
    .slice(0, 20000);

  for (const h of hits) next = mergeSource(next, { ...h, provider: "exa" });
  for (const p of pages) {
    next = mergeSource(next, {
      url: p.url,
      title: p.title || action.strategy,
      excerpt: text(p.text).slice(0, 400),
      provider: p.provider || "direct_public",
    });
  }

  if (FIT_FAMILY.test(action.strategy)) {
    const thesis = buildHotelWinThesisFromEvidence({
      opportunity: next,
      action,
      evidenceText: blob,
      hotel: { displayName: action.hotelName, hotelId: next.hotelId },
    });
    if (thesis.rationale && (thesis.commerciallySpecific || blob.length > 80)) {
      next = applyHotelWinThesisToOpportunity(next, thesis);
      next.chapultepecFit = thesis.rationale;
      if (!next.localConnection && /mexico city|cdmx|chapultepec|reforma|polanco/i.test(blob)) {
        next.localConnection =
          "Mexico City geography may be relevant if this edition creates local lodging/meetings demand";
      }
    } else {
      next.notes = [
        ...(next.notes || []),
        `research_action_fit_insufficient_evidence:${action.strategy}`,
      ];
    }
  }

  if (CONTACT_FAMILY.test(action.strategy)) {
    const contactHit = [...hits, ...pages]
      .map((x) => text(x.url))
      .find(
        (u) =>
          isContactPageUrl(u) &&
          !isHomepageOnlyUrl(u) &&
          !isPressPublisherNonCompanyRoute(u) &&
          filterPressPublisherOrgRoute(u),
      );
    if (contactHit) {
      next.contactUrl = contactHit;
      next.organizationRouteUrl = contactHit;
      next.hasCredibleOutreachRoute = true;
      next.contactRouteQuality = "OFFICIAL_CONTACT_PAGE";
    } else {
      const rejectedPublisher = [...hits, ...pages]
        .map((x) => text(x.url))
        .find((u) => isPressPublisherNonCompanyRoute(u));
      if (rejectedPublisher) {
        next.notes = [
          ...(next.notes || []),
          `press_publisher_contact_rejected_as_org_route:${rejectedPublisher}`,
        ];
        if (isPressPublisherNonCompanyRoute(next.organizationRouteUrl || "")) {
          next.organizationRouteUrl = null;
        }
        if (isPressPublisherNonCompanyRoute(next.contactUrl || "")) {
          next.contactUrl = null;
        }
      }
    }
    // Extract emails from pages
    for (const p of pages) {
      const extracted = extractPublicEvidenceFromText(p.text || "", p.url);
      const email = extracted?.emails?.[0] || extracted?.preferredEmail || null;
      if (email && !next.functionalContactEmail) {
        next.functionalContactEmail = email;
        next.contactEmail = next.contactEmail || email;
      }
    }
  }

  if (CHRONO_FAMILY.test(action.strategy)) {
    const yearMatch = blob.match(/\b(202[6-9]|2030)\b/);
    if (yearMatch) {
      next.associationEventsChronology = {
        ...(next.associationEventsChronology || {}),
        status: "FUTURE_SIGNAL_PRESENT",
        notedYear: yearMatch[1],
        source: "research_action_contract",
      };
      const unresolved = (next.associationEventsUnresolvedQuestions || []).filter(
        (q) => !/NEXT_EDITION/i.test(String(q?.id || q || "")),
      );
      next.associationEventsUnresolvedQuestions = unresolved;
    }
  }

  next.researchActionTrace = [
    ...(Array.isArray(next.researchActionTrace) ? next.researchActionTrace : []),
    {
      at: new Date().toISOString(),
      strategy: action.strategy,
      question: action.question,
      targetEntity: action.targetEntity,
      query: action.query,
      hitCount: hits.length,
      pageCount: pages.length,
    },
  ];
  return next;
}

/**
 * Execute a coherent research action via Exa (when authorized) + direct public reads.
 * Does not invent answers. Returns updated opportunity + sanitized trace.
 */
export async function executeCoherentResearchAction({
  action,
  opportunity = {},
  hotel = {},
  ledgerPath = null,
  remainingBudgetUsd = 0,
  allowPaid = false,
  maxPages = 3,
} = {}) {
  assertResearchActionCoherent(action);
  const trace = {
    version: RESEARCH_ACTION_CONTRACT_V1,
    action: {
      objective: action.objective,
      question: action.question,
      strategy: action.strategy,
      targetEntity: action.targetEntity,
      evidenceSought: action.evidenceSought,
      query: action.query,
    },
    operations: [],
    newInvestigation: false,
    cachedOrSkipped: false,
  };

  if (!action.executable) {
    trace.cachedOrSkipped = true;
    trace.operations.push({ kind: "skip", reason: "not_executable" });
    return { opportunity, trace, changed: false };
  }

  const hits = [];
  if (allowPaid && ledgerPath && remainingBudgetUsd > 0.05 && action.query) {
    const exa = await maybeExaSearch(action.query, ledgerPath);
    trace.operations.push({
      kind: "exa_search",
      query: action.query,
      status: exa.status || (exa.hits?.length ? "ok" : "empty"),
      hitCount: (exa.hits || []).length,
      costUsd: exa.costUsd || 0,
      reservationId: exa.reservationId || null,
    });
    if ((exa.hits || []).length) {
      hits.push(...exa.hits.slice(0, 8));
      trace.newInvestigation = true;
    }
  } else {
    trace.operations.push({
      kind: "exa_search",
      skipped: true,
      reason: !allowPaid
        ? "paid_not_authorized"
        : remainingBudgetUsd <= 0.05
          ? "budget"
          : "no_query",
    });
  }

  const pageCandidates = [];
  for (const h of hits) {
    if (h.url && /^https?:\/\//i.test(h.url)) pageCandidates.push(h.url);
  }
  // Strategy-guided seeds from existing opportunity
  if (CONTACT_FAMILY.test(action.strategy)) {
    for (const u of [
      opportunity.contactUrl,
      opportunity.organizationWebsite,
      opportunity.website,
    ]) {
      if (u && isContactPageUrl(u)) pageCandidates.unshift(u);
    }
    // Prefer /contact under known host
    try {
      const base = opportunity.contactUrl || opportunity.organizationWebsite || hits[0]?.url;
      if (base) {
        const root = new URL(base);
        pageCandidates.unshift(`${root.origin}/contact`);
        pageCandidates.unshift(`${root.origin}/contact-us`);
      }
    } catch {
      /* ignore */
    }
  }
  if (FIT_FAMILY.test(action.strategy) || CHRONO_FAMILY.test(action.strategy)) {
    for (const s of opportunity.sources || []) {
      const u = s?.url || s;
      if (u && /^https?:\/\//i.test(u)) pageCandidates.push(u);
    }
  }

  const pages = [];
  const seen = new Set();
  for (const url of pageCandidates) {
    if (pages.length >= maxPages) break;
    const key = text(url).toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    try {
      const page = await fetchOfficialPage(url, { timeoutMs: 12000 });
      const body = text(page?.text || page?.body || page?.content || "");
      if (body.length >= 40) {
        pages.push({
          url,
          title: page?.title || null,
          text: body.slice(0, 12000),
          provider: page?.provider || "direct_public",
        });
        trace.operations.push({
          kind: "direct_public_fetch",
          url,
          ok: true,
          bytes: body.length,
        });
        trace.newInvestigation = true;
      } else {
        trace.operations.push({
          kind: "direct_public_fetch",
          url,
          ok: false,
          reason: "thin_or_empty",
        });
      }
    } catch (err) {
      trace.operations.push({
        kind: "direct_public_fetch",
        url,
        ok: false,
        reason: String(err?.message || err).slice(0, 120),
      });
    }
  }

  const beforeSources = (opportunity.sources || []).length;
  const beforeFit = text(
    opportunity.hotelFitRationale ||
      opportunity.associationEventsGoldBrief?.whyThisHotel ||
      opportunity.commercialIntelligenceBrief?.whyThisHotel ||
      "",
  );
  let next = applyStrategyEvidence(opportunity, action, { hits, pages });

  // Regenerate authoritative gold brief from updated fields.
  // run-052 Issue 3 (RC-10): do not rely on a brittle /assoc|event|government/
  // regex that misses medical_academic and other Association-family segments.
  // Prefer brief/pathway identity; never rebuild Scaling CIB for Association rows.
  const briefKind = classifyResearchActionBriefKind(next);
  if (briefKind === "association") {
    next = applyAssociationEventsGoldBrief(next, { hotel });
  } else if (briefKind === "scaling") {
    next = applyScalingCommercialIntelligenceBrief(next, hotel, {
      force: false,
    });
  }

  const assessment = assessCommercialResearchState(next, hotel);
  next.commercialResearchAssessment = assessment;

  const afterSources = (next.sources || []).length;
  const afterFit = text(
    next.hotelFitRationale ||
      next.associationEventsGoldBrief?.whyThisHotel ||
      next.commercialIntelligenceBrief?.whyThisHotel ||
      "",
  );
  const changed =
    afterSources > beforeSources ||
    afterFit !== beforeFit ||
    text(next.contactUrl) !== text(opportunity.contactUrl) ||
    text(next.functionalContactEmail) !== text(opportunity.functionalContactEmail) ||
    JSON.stringify(next.associationEventsChronology || {}) !==
      JSON.stringify(opportunity.associationEventsChronology || {});

  trace.changed = changed;
  trace.assessmentAfter = {
    nextStrategy: assessment.nextStrategy,
    nextResearchQuestion: assessment.nextResearchQuestion,
    gapCodes: assessment.gaps.map((g) => g.code),
    criticalCount: assessment.criticalCount,
    materialCount: assessment.materialCount,
  };

  return { opportunity: next, trace, changed };
}
