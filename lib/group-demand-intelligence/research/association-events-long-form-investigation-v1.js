/**
 * Association / Event long-form investigation loop V1.
 *
 * Hypothesis-driven free (+ optional paid) research BEFORE commercial qualification.
 * Reuses Scaling investigation patterns and official-page fetch — does not copy
 * the entire Scaling Companies system.
 *
 * Runs inside FullE2E even when paid research is not authorized (free public only).
 */

import { fetchOfficialPage } from "../official-page-fetch.js";
import {
  ASSOCIATION_EVENTS_BUYER_INTEGRITY_V1,
  scrubAssociationEventsContactFields,
  isAssociationEventsValidPersonName,
  scrubAssociationEventsPhone,
  assessAssociationEventsBuyerCandidate,
} from "./association-events-buyer-integrity-v1.js";
import {
  ASSOCIATION_EVENTS_CHRONOLOGY_V1,
  applyAssociationEventsChronology,
  classifyAssociationEventsChronology,
  EVENT_CHRONOLOGY_STATUS,
} from "./association-events-chronology-v1.js";
import {
  ASSOCIATION_EVENTS_UNRESOLVED_QUESTIONS_V1,
  deriveAssociationEventsUnresolvedQuestions,
  selectStrategyForUnresolvedQuestion,
  ASSOC_EVENT_QUESTION,
} from "./association-events-unresolved-questions-v1.js";
import { evaluateAssociationEventsPublicationQualification } from "./association-events-qualification-v1.js";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import {
  mayStopForPublicationQualification,
  applyCommercialResearchCompletion,
  RESEARCH_COMPLETION_STATE,
} from "./commercial-research-completion-contract-v1.js";

export const ASSOCIATION_EVENTS_LONG_FORM_INVESTIGATION_V1 =
  "gdi-association-events-long-form-investigation-v1";

export const RESEARCH_GAP_CLASS = Object.freeze({
  RESEARCHABLE_GAP: "RESEARCHABLE_GAP",
  ALTERNATIVE_HOTEL_DEMAND_PATH: "ALTERNATIVE_HOTEL_DEMAND_PATH",
  FUTURE_EDITION_CONTINUATION: "FUTURE_EDITION_CONTINUATION",
  CONFIRMED_NOT_COMMERCIALLY_RELEVANT: "CONFIRMED_NOT_COMMERCIALLY_RELEVANT",
  ENTITY_OR_SOURCE_INVALID: "ENTITY_OR_SOURCE_INVALID",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hostnameOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, "");
  } catch {
    return "";
  }
}

const NEWS_HOST =
  /magazzine|tyt\.com|poderyfinanzas|ejecentral|quintanaroohoy|24horas|linkedin|facebook|twitter|businesswire|prnewswire/i;

export function resolveAssociationEventBaseUrls(opportunity = {}) {
  const urls = [];
  const add = (u) => {
    const t = text(u);
    if (!t || !/^https?:\/\//i.test(t)) return;
    try {
      const parsed = new URL(t);
      const host = parsed.hostname.replace(/^www\./i, "");
      if (NEWS_HOST.test(host)) return;
      const root = `${parsed.protocol}//${parsed.hostname}`;
      if (!urls.includes(root)) urls.push(root);
      if (!urls.includes(t) && /event|congreso|congress|larim|icuh|andellac|amsac|longevity|vivi|medico|cooperativ/i.test(t)) {
        urls.push(t);
      }
    } catch {
      /* ignore */
    }
  };
  add(opportunity.organizationWebsite || opportunity.website);
  add(opportunity.contactUrl);
  add(opportunity.sourceUrl);
  add(opportunity.officialEventUrl);
  for (const s of opportunity.sources || []) add(s?.url || s);
  for (const u of opportunity.evidenceUrls || []) add(u);
  return [...new Set(urls)].slice(0, 8);
}

function extractPeopleFromText(blob = "") {
  const people = [];
  const patterns = [
    /([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ'’-]+){1,3})\s*[,–-]\s*([^.\n|]{0,80}?(?:Director|Manager|Presidente|President|Secretari[oa]|Coordinator|Organizer|Gerente|CEO|LOC|Chair)[^.\n|]{0,40})/gi,
    /(?:Dr\.?|Dra\.?|Prof\.?)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ'’-]+){1,3})/g,
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(blob))) {
      const name = text(m[1]);
      const title = text(m[2] || "organizing committee").slice(0, 90);
      if (isAssociationEventsValidPersonName(name)) {
        const a = assessAssociationEventsBuyerCandidate({ name, title });
        if (a.ok || /director|gerente|presidente|organizer|chair|loc|secretario/i.test(title)) {
          people.push({ name, title: a.ok ? title : title, integrityOk: a.ok });
        }
      }
    }
  }
  const seen = new Set();
  return people.filter((p) => {
    const k = p.name.toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  }).slice(0, 8);
}

function extractEmails(blob = "") {
  const out = [];
  const re = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
  let m;
  while ((m = re.exec(blob))) {
    const e = m[0].toLowerCase();
    if (/example\.|sentry\.|schema\.|wixpress|cloudflare|noreply/i.test(e)) continue;
    out.push(e);
  }
  return [...new Set(out)].slice(0, 6);
}

function detectHostContracted(blob = "") {
  const t = text(blob);
  if (/host hotel|hotel sede|sede oficial|official hotel|hospedaje oficial|paquete todo incluido.*hotel|all.?inclusive package.*(?:hotel|resort)/i.test(t)) {
    const hostMatch = t.match(
      /(?:host hotel|hotel sede|sede oficial|official hotel)[:\s]+([^.\n|]{5,80})/i,
    );
    return {
      contracted: true,
      hostName: hostMatch ? text(hostMatch[1]).slice(0, 80) : null,
      evidence: t.slice(0, 280),
    };
  }
  if (/dreams natura|four seasons m[eé]xico|club de industriales/i.test(t) &&
      /sede|host|oficial|paquete/i.test(t)) {
    return { contracted: true, hostName: null, evidence: t.slice(0, 280) };
  }
  return { contracted: false, hostName: null, evidence: null };
}

function detectOverflowOpen(blob = "") {
  const t = text(blob);
  if (/room block|bloque de habitaciones|alojamiento oficial|housing bureau|preferred hotel|hoteles recomendados|accommodation|hospedaje/i.test(t)) {
    if (/sold out|agotado|cerrado|fully booked|no longer accepting/i.test(t)) {
      return { open: false, note: "housing_closed_language" };
    }
    return { open: true, note: "housing_language_present" };
  }
  if (/canc[uú]n center|expo reforma|centro de convenciones|convention center/i.test(t)) {
    return { open: null, note: "convention_center_venue_overflow_hypothesis" };
  }
  return { open: null, note: null };
}

async function defaultFetchPage(url) {
  try {
    const page = await fetchOfficialPage(url, {
      allowRender: false,
      timeoutMs: 12000,
    });
    return {
      ok: page?.ok !== false && Boolean(page?.text || page?.bodyText),
      url,
      text: page?.text || page?.bodyText || page?.searchableText || "",
      method: page?.method || null,
    };
  } catch (err) {
    return { ok: false, url, text: "", error: err.message };
  }
}

function strategyUrls(strategyId, opportunity, baseUrls) {
  const pathsByStrategy = {
    OFFICIAL_EVENT_SITE: ["", "/eventos", "/events", "/congreso", "/congress"],
    OFFICIAL_CONTACT_PAGES: ["/contacto", "/contact", "/about", "/nosotros", "/equipo", "/team"],
    LOC_PCO_ORGANIZER_PAGES: [
      "/committee",
      "/organizacion",
      "/organizers",
      "/comite",
      "/about",
    ],
    HOUSING_ACCOMMODATION_PAGES: [
      "/accommodation",
      "/housing",
      "/hotels",
      "/hospedaje",
      "/alojamiento",
      "/travel",
      "/registration",
    ],
    HOST_VENUE_EVIDENCE: ["", "/venue", "/sede", "/location"],
    FUTURE_EDITION_ANNOUNCEMENT: ["", "/eventos", "/news", "/noticias", "/blog"],
    CHRONOLOGY_SOURCES: ["", "/eventos", "/archive", "/ediciones"],
    PRIOR_EDITION_HOTEL_ARRANGEMENTS: ["/accommodation", "/housing", "/hotels"],
    ANCILLARY_DEMAND_HYPOTHESIS: ["", "/program", "/programa", "/agenda"],
    ORGANIZER_IDENTITY: ["/about", "/nosotros", "/quienes-somos"],
    HOTEL_FIT_REASSESSMENT: [],
    GEOGRAPHY_PROXIMITY: [],
  };
  const paths = pathsByStrategy[strategyId] || [""];
  const urls = [];
  // Prefer full event URLs already known
  for (const u of baseUrls) {
    if (/\/(larim|icuh|longevity|andellac|amsac|medico)/i.test(u) || u.split("/").length > 3) {
      urls.push(u);
    }
  }
  for (const base of baseUrls.filter((u) => /^https?:\/\/[^/]+\/?$/i.test(u) || u.split("/").length <= 4)) {
    const root = base.replace(/\/$/, "");
    for (const p of paths) {
      urls.push(p ? `${root}${p}` : root);
    }
  }
  return [...new Set(urls)].slice(0, 6);
}

async function executeAssocStrategy({
  strategyId,
  opportunity,
  hotel,
  fetchPage,
  allowFreeFetch,
  pageFetchBudget,
}) {
  const baseUrls = resolveAssociationEventBaseUrls(opportunity);
  const urls = strategyUrls(strategyId, opportunity, baseUrls);
  const attempted = [];
  let pagesFetched = 0;
  let next = { ...opportunity };
  const found = {
    people: [],
    emails: [],
    host: null,
    overflow: null,
    chronologyHints: [],
  };
  const rejected = [];

  if (
    strategyId === "HOTEL_FIT_REASSESSMENT" ||
    strategyId === "GEOGRAPHY_PROXIMITY" ||
    strategyId === "ANCILLARY_DEMAND_HYPOTHESIS"
  ) {
    // Evidence-based reassessment without network when possible
    const venueBlob = `${next.venueName || ""} ${next.eventName || ""} ${next.whatIsHappening || ""}`;
    const overflow = detectOverflowOpen(venueBlob);
    if (overflow.open === null && /canc[uú]n center|expo reforma|centro de convenciones/i.test(venueBlob)) {
      next.hotelDemandThesis =
        next.hotelDemandThesis ||
        `OFFSITE_OVERFLOW_LODGING hypothesis vs convention/expo venue — verify room blocks still open for ${hotel?.displayName || "this hotel"}`;
      next.demandMechanism = next.demandMechanism || "OFFSITE_OVERFLOW_LODGING";
      found.overflow = overflow;
    }
    if (next.hostHotelContracted === true) {
      next.alternativeDemandPath =
        "Assess speaker/VIP, exhibitor overflow, or ancillary meetings only if evidence supports open demand — do not assume";
    }
    return {
      opportunity: next,
      informationFound: Boolean(found.overflow || next.hotelDemandThesis),
      informationRejected: rejected,
      urlsAttempted: [],
      pagesFetched: 0,
      strategyId,
      note: "local_hypothesis_reassessment",
    };
  }

  if (!allowFreeFetch || pageFetchBudget <= 0 || !urls.length) {
    return {
      opportunity: next,
      informationFound: false,
      informationRejected: ["no_fetch_budget_or_urls"],
      urlsAttempted: urls.slice(0, 3),
      pagesFetched: 0,
      strategyId,
      note: "skipped_fetch",
    };
  }

  for (const url of urls) {
    if (pagesFetched >= pageFetchBudget) break;
    attempted.push(url);
    const page = await fetchPage(url);
    pagesFetched += 1;
    if (!page.ok || !text(page.text)) {
      rejected.push({ url, reason: "empty_or_failed" });
      continue;
    }
    const blob = page.text.slice(0, 20000);
    found.people.push(...extractPeopleFromText(blob));
    found.emails.push(...extractEmails(blob));
    const host = detectHostContracted(blob);
    if (host.contracted) found.host = host;
    const overflow = detectOverflowOpen(blob);
    if (overflow.note) found.overflow = overflow;
    found.chronologyHints.push(blob.slice(0, 500));

    next.sources = [
      ...(next.sources || []),
      {
        url,
        sourceFamily: "long_form_investigation",
        strategyId,
        excerpt: text(blob).slice(0, 280),
        fetchedAt: new Date().toISOString(),
      },
    ];
  }

  // Apply extracted facts
  if (found.people.length) {
    const valid = found.people.filter((p) => isAssociationEventsValidPersonName(p.name));
    next.namedStakeholders = [
      ...(next.namedStakeholders || []).filter((s) =>
        isAssociationEventsValidPersonName(s?.name),
      ),
      ...valid,
    ];
    if (!isAssociationEventsValidPersonName(next.contactName) && valid.length) {
      const best =
        valid.find((p) => p.integrityOk) ||
        valid.find((p) =>
          /director|gerente|presidente|organizer|chair|secretario/i.test(p.title || ""),
        ) ||
        valid[0];
      next.contactName = best.name;
      next.contactRole = best.title || "event_organizing";
    }
  }
  if (found.emails.length) {
    // Prefer org/event emails; do not attach email to wrong person without evidence
    const preferred =
      found.emails.find((e) => /larim|congreso|congress|eventos|info@|contact|organiz/i.test(e)) ||
      found.emails[0];
    if (!next.contactEmail) next.contactEmail = preferred;
    if (!next.contactUrl && attempted[0]) next.contactUrl = attempted[0];
  }
  if (found.host?.contracted) {
    next.hostHotelContracted = true;
    next.hostHotelName = found.host.hostName || next.hostHotelName || null;
    next.buyingAndSourcingStatus =
      `Host/official hotel language detected${found.host.hostName ? `: ${found.host.hostName}` : ""} — investigate overflow/ancillary only if evidence supports open demand`;
  }
  if (found.overflow?.open === true) {
    next.externalRoomBlocksAvailable = true;
    next.hotelDemandThesis =
      next.hotelDemandThesis ||
      "Official accommodation / preferred-hotel language indicates external lodging path may be open";
    next.demandMechanism = next.demandMechanism || "OFFSITE_OVERFLOW_LODGING";
  } else if (found.overflow?.open === false) {
    next.externalRoomBlocksAvailable = false;
  }
  if (found.chronologyHints.length) {
    next.eventHistoryAndActivity = [
      text(next.eventHistoryAndActivity),
      ...found.chronologyHints.map((h) => text(h).slice(0, 200)),
    ]
      .filter(Boolean)
      .join(" | ")
      .slice(0, 2000);
  }

  next = scrubAssociationEventsContactFields(next);
  next = applyAssociationEventsChronology(next);

  return {
    opportunity: next,
    informationFound:
      found.people.length > 0 ||
      found.emails.length > 0 ||
      Boolean(found.host?.contracted) ||
      Boolean(found.overflow?.note),
    informationRejected: rejected,
    urlsAttempted: attempted,
    pagesFetched,
    strategyId,
    note: "fetch_strategy_complete",
  };
}

/**
 * Classify research gap type for a prior QA / research disposition.
 */
export function classifyAssociationEventsResearchGap(opportunity = {}, priorDisposition = null) {
  const chrono = opportunity.associationEventsChronology ||
    classifySafe(opportunity);
  if (
    opportunity.eventEntityIncorrect ||
    chrono.status === EVENT_CHRONOLOGY_STATUS.INCORRECT_LISTING
  ) {
    return RESEARCH_GAP_CLASS.ENTITY_OR_SOURCE_INVALID;
  }
  if (
    opportunity.hostHotelContracted === true &&
    opportunity.externalRoomBlocksAvailable === false
  ) {
    return RESEARCH_GAP_CLASS.CONFIRMED_NOT_COMMERCIALLY_RELEVANT;
  }
  if (
    opportunity.hostHotelContracted === true &&
    opportunity.externalRoomBlocksAvailable !== false
  ) {
    return RESEARCH_GAP_CLASS.ALTERNATIVE_HOTEL_DEMAND_PATH;
  }
  if (
    chrono.status === EVENT_CHRONOLOGY_STATUS.PAST_COMPLETED ||
    chrono.status === EVENT_CHRONOLOGY_STATUS.HISTORICALLY_RECURRING ||
    priorDisposition === "CONTINUE_RESEARCH"
  ) {
    if (
      chrono.status === EVENT_CHRONOLOGY_STATUS.PAST_COMPLETED ||
      chrono.status === EVENT_CHRONOLOGY_STATUS.HISTORICALLY_RECURRING
    ) {
      return RESEARCH_GAP_CLASS.FUTURE_EDITION_CONTINUATION;
    }
  }
  return RESEARCH_GAP_CLASS.RESEARCHABLE_GAP;
}

function classifySafe(opportunity) {
  try {
    return classifyAssociationEventsChronology(opportunity);
  } catch {
    return { status: EVENT_CHRONOLOGY_STATUS.AMBIGUOUS };
  }
}

/**
 * Run long-form investigation on one opportunity.
 */
export async function runAssociationEventsLongFormInvestigation({
  opportunity = {},
  hotel = {},
  allowFreeFetch = true,
  maxIterations = 5,
  maxPageFetches = 8,
  fetchPage = defaultFetchPage,
  paidSearch = null,
  asOf = null,
} = {}) {
  let current = scrubAssociationEventsContactFields({ ...opportunity });
  current = applyAssociationEventsChronology(current, { asOf });
  const attempts = Array.isArray(current.associationEventsInvestigationAttempts)
    ? [...current.associationEventsInvestigationAttempts]
    : [];
  let pagesUsed = 0;
  let stopReason = null;
  const unresolvedPack = deriveAssociationEventsUnresolvedQuestions(current, hotel);
  let questions = [...unresolvedPack.questions];

  // Early exit: entity invalid
  if (current.eventEntityIncorrect === true) {
    return finalize(current, attempts, "ENTITY_INVALID", questions, hotel, pagesUsed);
  }

  for (let i = 0; i < maxIterations; i += 1) {
    current = scrubAssociationEventsContactFields(current);
    current = applyAssociationEventsChronology(current, { asOf });
    const qual = evaluateAssociationEventsPublicationQualification(current, hotel);

    // Chronology gate: past edition without verified future → continuation, not qualify
    const chrono = current.associationEventsChronology || {};
    if (
      qual.qualified &&
      (chrono.status === EVENT_CHRONOLOGY_STATUS.PAST_COMPLETED ||
        chrono.status === EVENT_CHRONOLOGY_STATUS.HISTORICALLY_RECURRING) &&
      !chrono.futureEditionVerified
    ) {
      // Force continuation path — research next edition
      questions = [
        ASSOC_EVENT_QUESTION.IS_THERE_A_CONFIRMED_NEXT_EDITION,
        ...questions.filter((q) => q !== ASSOC_EVENT_QUESTION.IS_THERE_A_CONFIRMED_NEXT_EDITION),
      ];
    } else if (qual.qualified && mayStopForPublicationQualification(current, hotel)) {
      // Publication gates are separate from commercial completion — only stop when
      // material commercial gaps (fit/demand/sources/identity) are also addressed.
      stopReason = "PUBLICATION_QUALIFIED";
      break;
    } else if (qual.qualified && !mayStopForPublicationQualification(current, hotel)) {
      // Keep investigating commercially material questions despite publicationQualified.
      current = {
        ...current,
        associationEventsPublicationQualified: true,
        publicationQualifiedStopDeferred: true,
      };
    }

    if (
      current.hostHotelContracted === true &&
      current.externalRoomBlocksAvailable === false &&
      !current.forceAncillaryInvestigation
    ) {
      stopReason = "HOST_CONTRACTED_NO_OPEN_DEMAND";
      break;
    }

    const nextQuestion = questions[0] || ASSOC_EVENT_QUESTION.WHO_CONTROLS_HOTEL_SOURCING;
    const strategyId = selectStrategyForUnresolvedQuestion(nextQuestion, attempts);
    if (!strategyId) {
      // Try remaining questions
      questions = questions.slice(1);
      if (!questions.length) {
        stopReason = "STRATEGIES_EXHAUSTED";
        break;
      }
      continue;
    }

    const pageBudget = Math.max(0, maxPageFetches - pagesUsed);
    let executed;
    if (paidSearch && /LOC_PCO|FUTURE_EDITION|OFFICIAL_EVENT/i.test(strategyId)) {
      // Optional paid search enrichment (authorization handled by caller)
      try {
        const org = text(current.organizationName || current.eventName).slice(0, 80);
        const q = `"${org}" ${strategyId === "FUTURE_EDITION_ANNOUNCEMENT" ? "2027 OR 2028 OR próxima edición OR next edition" : "director OR organizador OR accommodation OR hospedaje OR contact"}`;
        const exa = await paidSearch(q);
        const hits = exa?.hits || [];
        for (const hit of hits.slice(0, 3)) {
          current.sources = [
            ...(current.sources || []),
            {
              url: hit.url,
              sourceFamily: "long_form_paid_search",
              excerpt: text(hit.text || hit.excerpt).slice(0, 280),
            },
          ];
          const people = extractPeopleFromText(`${hit.title || ""} ${hit.text || ""}`);
          if (people.length) {
            current.namedStakeholders = [
              ...(current.namedStakeholders || []),
              ...people,
            ];
          }
          const emails = extractEmails(`${hit.text || ""}`);
          if (emails[0] && !current.contactEmail) current.contactEmail = emails[0];
        }
      } catch {
        /* non-fatal */
      }
    }

    executed = await executeAssocStrategy({
      strategyId,
      opportunity: current,
      hotel,
      fetchPage,
      allowFreeFetch: allowFreeFetch && pageBudget > 0,
      pageFetchBudget: pageBudget,
    });

    pagesUsed += executed.pagesFetched || 0;
    current = executed.opportunity;
    attempts.push({
      at: new Date().toISOString(),
      strategyId,
      question: nextQuestion,
      informationFound: executed.informationFound === true,
      urlsAttempted: executed.urlsAttempted || [],
      pagesFetched: executed.pagesFetched || 0,
      note: executed.note || null,
      provider: "direct_public_or_saved",
    });

    const refreshed = deriveAssociationEventsUnresolvedQuestions(current, hotel);
    questions = refreshed.questions;

    if (!executed.informationFound) {
      // Advance question to avoid identical retries
      questions = questions.filter((q) => q !== nextQuestion).concat(
        questions.includes(nextQuestion) ? [] : [],
      );
      // Mark strategy tried; loop will pick next strategy via attempts
      if (!selectStrategyForUnresolvedQuestion(nextQuestion, attempts)) {
        questions = questions.filter((q) => q !== nextQuestion);
      }
    }
  }

  if (!stopReason) {
    const qual = evaluateAssociationEventsPublicationQualification(current, hotel);
    const chrono = current.associationEventsChronology || {};
    if (
      qual.qualified &&
      chrono.commerciallyActionableNow !== false &&
      chrono.status === EVENT_CHRONOLOGY_STATUS.CONFIRMED_FUTURE
    ) {
      stopReason = "PUBLICATION_QUALIFIED";
    } else if (qual.qualified && chrono.status === EVENT_CHRONOLOGY_STATUS.PAST_COMPLETED) {
      stopReason = "PAST_EDITION_NEEDS_FUTURE_CONFIRMATION";
    } else {
      stopReason = "ITERATION_CEILING";
    }
  }

  return finalize(current, attempts, stopReason, questions, hotel, pagesUsed);
}

function finalize(current, attempts, stopReason, questions, hotel, pagesUsed) {
  current = scrubAssociationEventsContactFields(current);
  current = applyAssociationEventsChronology(current);
  const gapClass = classifyAssociationEventsResearchGap(current);
  const qual = evaluateAssociationEventsPublicationQualification(current, hotel);
  const unresolved = deriveAssociationEventsUnresolvedQuestions(current, hotel);

  // Past editions cannot be publication-qualified even if buyer/route present
  let publicationQualified = qual.qualified === true;
  const chrono = current.associationEventsChronology || {};
  if (
    publicationQualified &&
    (chrono.status === EVENT_CHRONOLOGY_STATUS.PAST_COMPLETED ||
      chrono.status === EVENT_CHRONOLOGY_STATUS.HISTORICALLY_RECURRING) &&
    !chrono.futureEditionVerified
  ) {
    publicationQualified = false;
  }
  if (current.hostHotelContracted === true && current.externalRoomBlocksAvailable === false) {
    publicationQualified = false;
  }

  current = {
    ...current,
    associationEventsInvestigationAttempts: attempts,
    associationEventsLongFormVersion: ASSOCIATION_EVENTS_LONG_FORM_INVESTIGATION_V1,
    associationEventsInvestigationDisposition: stopReason,
    associationEventsUnresolvedQuestions: unresolved.questions,
    associationEventsResearchGapClass: gapClass,
    associationEventsBuyerIntegrityVersion: ASSOCIATION_EVENTS_BUYER_INTEGRITY_V1,
    associationEventsChronologyVersion: ASSOCIATION_EVENTS_CHRONOLOGY_V1,
    publicationQualified,
    researchContinuation:
      gapClass !== RESEARCH_GAP_CLASS.CONFIRMED_NOT_COMMERCIALLY_RELEVANT &&
      gapClass !== RESEARCH_GAP_CLASS.ENTITY_OR_SOURCE_INVALID &&
      !(
        publicationQualified &&
        mayStopForPublicationQualification(current, hotel)
      ),
    preserveOpportunity: true,
  };

  const strategiesExhausted =
    stopReason === "STRATEGIES_EXHAUSTED" || stopReason === "ENTITY_INVALID";
  const iterationCeiling = stopReason === "ITERATION_CEILING";
  current = applyCommercialResearchCompletion(current, hotel, {
    executionStopReason: stopReason,
    strategiesExhausted,
    iterationCeiling,
  });
  // Map publication-only stop to honest execution-finished when gaps remain
  if (
    stopReason === "PUBLICATION_QUALIFIED" &&
    current.commercialResearchCompletionState ===
      RESEARCH_COMPLETION_STATE.RESEARCH_CONTINUATION
  ) {
    current.associationEventsInvestigationDisposition = "PUBLICATION_QUALIFIED_COMMERCIAL_CONTINUATION";
    current.researchContinuation = true;
  }

  return {
    version: ASSOCIATION_EVENTS_LONG_FORM_INVESTIGATION_V1,
    opportunity: current,
    qualification: { ...qual, qualified: publicationQualified },
    attempts,
    disposition: current.associationEventsInvestigationDisposition || stopReason,
    researchGapClass: gapClass,
    unresolvedQuestions: unresolved.questions,
    commercialResearchCompletion: {
      state: current.commercialResearchCompletionState,
      acceptance: current.commercialDrawerAcceptance,
      gaps: current.commercialMaterialGaps,
    },
    metrics: {
      iterations: attempts.length,
      pagesFetched: pagesUsed,
      publicationQualified,
      commerciallyComplete: current.commerciallyResearchComplete === true,
      gapsRemaining: unresolved.questions,
      namedBuyerPresent: isAssociationEventsValidPersonName(current.contactName),
      chronologyStatus: chrono.status || null,
    },
  };
}

/**
 * Batch runner for matured association/event rows.
 */
export async function runAssociationEventsLongFormOnMaturedRows({
  maturedRows = [],
  hotel = {},
  allowFreeFetch = true,
  maxIterations = 5,
  maxPageFetches = 8,
  fetchPage = defaultFetchPage,
  paidSearch = null,
  asOf = null,
  prioritize = true,
} = {}) {
  let rows = maturedRows.map((r) =>
    r?.opportunityPayload || r?.opportunity
      ? r
      : { opportunityPayload: r, opportunity: r },
  );

  if (prioritize) {
    rows = [...rows].sort((a, b) => {
      const oa = a.opportunityPayload || a.opportunity || {};
      const ob = b.opportunityPayload || b.opportunity || {};
      const score = (o) => {
        let s = 0;
        if (/larim|longevity|icuh|andellac|m[eé]dicos|cooperativ/i.test(`${o.eventName || ""} ${o.organizationName || ""}`)) s += 5;
        if (o.associationEventsChronology?.status === EVENT_CHRONOLOGY_STATUS.CONFIRMED_FUTURE) s += 4;
        if (isAssociationEventsValidPersonName(o.contactName)) s += 2;
        if (o.hostHotelContracted) s -= 3;
        return s;
      };
      return score(ob) - score(oa);
    });
  }

  const results = [];
  const outRows = [];
  for (const row of rows) {
    const opp = row.opportunityPayload || row.opportunity || row;
    const result = await runAssociationEventsLongFormInvestigation({
      opportunity: opp,
      hotel,
      allowFreeFetch,
      maxIterations,
      maxPageFetches,
      fetchPage,
      paidSearch,
      asOf,
    });
    results.push({
      id: result.opportunity?.id,
      disposition: result.disposition,
      researchGapClass: result.researchGapClass,
      metrics: result.metrics,
    });
    outRows.push({
      ...row,
      opportunityPayload: result.opportunity,
      opportunity: result.opportunity,
      longFormInvestigation: result,
    });
  }

  return {
    version: ASSOCIATION_EVENTS_LONG_FORM_INVESTIGATION_V1,
    attempted: true,
    count: outRows.length,
    qualifiedCount: results.filter((r) => r.metrics?.publicationQualified).length,
    results,
    maturedRows: outRows,
  };
}

// Re-export helpers for tests
export {
  ASSOCIATION_EVENTS_UNRESOLVED_QUESTIONS_V1,
  ASSOC_EVENT_QUESTION,
  scrubAssociationEventsPhone,
  looksLikePersonName,
};
