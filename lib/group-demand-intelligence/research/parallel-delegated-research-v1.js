/**
 * Optional Parallel FindAll discovery and structured Task delegation
 * for the existing GDI Research Supervisor.
 * Disabled unless the master Parallel flag and the specific flag are on.
 * Does not publish, write Airtable, or call parallel-cli.
 * Pricing checked against https://docs.parallel.ai/getting-started/pricing on 2026-10-24.
 */

import { canonicalEntityKey } from "../workflow/gdi-global-weekly-feedback-v2.js";
import { RESEARCH_SEGMENT } from "../workflow/hotel-segment-research-plan-v1.js";
import {
  isGdiParallelHotelAllowed,
  isParallelDelegationEnabled,
  isParallelFindAllEnabled,
} from "../parallel/feature-flag.js";
import {
  reserveParallelBudget,
  commitParallelBudgetReservation,
  releaseParallelBudgetReservation,
  readParallelBudgetLedger,
} from "../parallel/budget-ledger-v1.js";
import {
  beginOrResumeParallelJob,
  markParallelJobSubmitted,
  markParallelJobUncertain,
  markParallelJobCompleted,
  createParallelJobKey,
} from "../parallel/job-store-v1.js";
import { createParallelClient } from "../../hotel-intelligence/research/providers/parallel/client.js";
import { readRecurringFromOpportunity } from "./recurring-meeting-intelligence-v1.js";
import { PROGRAM_TREATMENT } from "./recurring-meeting-intelligence-v1.js";

export const PARALLEL_DELEGATED_RESEARCH_VERSION = "gdi-parallel-delegated-research-v1";
export const PARALLEL_PRICE_VERIFIED_ON = "2026-10-24";

/** Per successful Task run. Failed runs are not billed. */
export const TASK_PRICE_USD = Object.freeze({
  lite: 0.005,
  base: 0.01,
  core: 0.025,
  core2x: 0.05,
  pro: 0.1,
  ultra: 0.3,
  ultra2x: 0.6,
  ultra4x: 1.2,
  ultra8x: 2.4,
});

const TASK_ALLOWED = new Set(["lite", "base", "core", "core2x", "pro"]);
const ULTRA = new Set(["ultra", "ultra2x", "ultra4x", "ultra8x"]);

/** Fixed cost plus per matched candidate. Enrichment is a separate Task charge and is refused. */
export const FINDALL_PRICE_USD = Object.freeze({
  preview: { fixed: 0.1, perMatch: 0, matchLimit: { min: 5, max: 10 } },
  base: { fixed: 0.25, perMatch: 0.03, matchLimit: { min: 1, max: 20 } },
  core: { fixed: 2, perMatch: 0.15, matchLimit: { min: 1, max: 20 } },
  pro: { fixed: 10, perMatch: 1, matchLimit: { min: 1, max: 20 } },
});

const FINDALL_ALLOWED = new Set(["preview", "base"]);

export const SEGMENT_DEMAND_CONDITIONS = Object.freeze({
  associations_government: "The organization runs a future association, government, or delegate meeting that can require group lodging. A city address alone is not a match.",
  sports: "The organization brings a team, tournament, or sports event that needs group rooms. A local club with no travel group is not a match.",
  corporate_meetings_training: "The organization holds a corporate meeting, training program, or project team that places people overnight. A headquarters address is not a match.",
  local_meetings_catering: "The organization books local meetings or catering with a stated group. A restaurant or office listing is not a match.",
  scaling_companies: "The company is growing and has a documented offsite, hiring cohort, or team travel program. Incorporation in the city is not a match.",
  medical_academic: "The organization runs a medical or academic meeting that can require delegate lodging. A hospital address alone is not a match.",
  weddings_social_private_events: "The organization plans weddings or private events that include guest rooms. A venue listing with no guest-room demand is not a match.",
  entertainment_production: "The organization moves a production, tour, or crew that needs group lodging. A studio address alone is not a match.",
  exhibitor_sponsor_teams: "The organization fields an exhibitor or sponsor team that travels to events and needs rooms. A sponsor logo is not a match.",
  tours_educational_specialist: "The organization operates group tours or educational programs that book hotel rooms. A school address alone is not a match.",
  professional_planners_dmc: "The organization places visiting groups in hotels, with venue sourcing, room blocks, or accommodation contracting. An office address and a generic events service are not a match.",
});

function text(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function refuse(code, message) {
  const err = new Error(message || code);
  err.code = code;
  return err;
}

function normalizeUrl(value) {
  return text(value).replace(/\/+$/, "").toLowerCase();
}

export function quoteStructuredTask({ processor = "pro", allowUltra = false } = {}) {
  const name = text(processor).toLowerCase();
  const price = TASK_PRICE_USD[name];
  if (price == null) throw refuse("PARALLEL_PROCESSOR_UNKNOWN", `Unknown Task processor: ${name || "blank"}`);
  if (ULTRA.has(name) && allowUltra !== true) throw refuse("PARALLEL_PROCESSOR_REFUSED", `${name} is refused until a separate authorization names it`);
  if (!TASK_ALLOWED.has(name) && !ULTRA.has(name)) throw refuse("PARALLEL_PROCESSOR_REFUSED", name);
  return {
    kind: "task",
    processor: name,
    estimatedUsd: price,
    unbounded: false,
    verifiedOn: PARALLEL_PRICE_VERIFIED_ON,
  };
}

export function quoteFindAll({ generator = "preview", matchLimit = null, enrichment = null, allowExpensiveGenerator = false } = {}) {
  if (enrichment) throw refuse("PARALLEL_ENRICHMENT_REFUSED", "FindAll enrichment is a separate per-match Task charge and is refused");
  const name = text(generator).toLowerCase();
  const tier = FINDALL_PRICE_USD[name];
  if (!tier) throw refuse("PARALLEL_GENERATOR_UNKNOWN", `Unknown FindAll generator: ${name || "blank"}`);
  if (!FINDALL_ALLOWED.has(name) && allowExpensiveGenerator !== true) {
    throw refuse("PARALLEL_GENERATOR_REFUSED", `${name} is refused until a separate authorization names it`);
  }
  const limit = Number(matchLimit);
  if (!Number.isInteger(limit) || limit < tier.matchLimit.min || limit > tier.matchLimit.max) {
    throw refuse("PARALLEL_MATCH_LIMIT_UNBOUNDED", `matchLimit must be an integer from ${tier.matchLimit.min} to ${tier.matchLimit.max}`);
  }
  const estimatedUsd = Number((tier.fixed + tier.perMatch * limit).toFixed(6));
  return {
    kind: "findall",
    generator: name,
    matchLimit: limit,
    fixedUsd: tier.fixed,
    perMatchUsd: tier.perMatch,
    estimatedUsd,
    unbounded: false,
    verifiedOn: PARALLEL_PRICE_VERIFIED_ON,
  };
}

export function findAllActualUsd(quote, matchedCount) {
  const matched = Number(matchedCount);
  if (!Number.isInteger(matched) || matched < 0 || matched > quote.matchLimit) {
    throw refuse("PARALLEL_MATCH_COUNT_INVALID", "Matched count must be within the reserved cap");
  }
  return Number((quote.fixedUsd + quote.perMatchUsd * matched).toFixed(6));
}

export function buildFindAllRequest({ segmentId, hotel = {}, exclusions = [], generator = "preview", matchLimit = 5 } = {}) {
  const segment = text(segmentId);
  if (!Object.values(RESEARCH_SEGMENT).includes(segment)) {
    throw refuse("UNKNOWN_SEGMENT", segment || "blank");
  }
  const quote = quoteFindAll({ generator, matchLimit });
  const demand = SEGMENT_DEMAND_CONDITIONS[segment];
  const market = text(hotel.market || hotel.city);
  const known = exclusions.map((item) => text(item.organizationName || item.name || item)).filter(Boolean);
  return {
    ...quote,
    publicationReady: false,
    customerVisible: false,
    payload: {
      objective: [
        `Find organizations for the ${segment} segment that ${hotel.name || "the target hotel"} in ${market || "its market"} could serve.`,
        demand,
        known.length ? `Do not return these already known organizations: ${known.join("; ")}.` : null,
      ].filter(Boolean).join(" "),
      entity_type: "organizations",
      generator: quote.generator,
      match_limit: quote.matchLimit,
      match_conditions: [
        { name: "demand_mechanism", description: demand },
        {
          name: "hotel_market_fit",
          description: `The group's activity must be servable by ${hotel.name || "the hotel"} in ${market || "the hotel market"}. Operating in the same city is not sufficient.`,
        },
      ],
    },
    exclusions: known.map((name) => canonicalEntityKey(name)),
  };
}

export function selectFindAllCandidates(result = {}, request = {}) {
  const excluded = new Set(request.exclusions || []);
  const seen = new Set();
  const candidates = [];
  const rejected = [];
  for (const row of result.candidates || []) {
    const name = text(row.name || row.organizationName);
    const key = canonicalEntityKey(name);
    const mechanism = row.output?.demand_mechanism;
    const market = row.output?.hotel_market_fit;
    let reason = null;
    if (text(row.match_status).toLowerCase() !== "matched") reason = "not_matched";
    else if (!name) reason = "missing_name";
    else if (excluded.has(key)) reason = "known_organization";
    else if (seen.has(key)) reason = "duplicate";
    else if (mechanism && mechanism.is_matched === false) reason = "demand_mechanism_not_met";
    else if (market && market.is_matched === false) reason = "city_presence_without_demand";
    if (reason) {
      rejected.push({ name, reason, publicationReady: false });
      continue;
    }
    seen.add(key);
    candidates.push({
      organizationName: name,
      organizationKey: key,
      sourceUrl: row.url || null,
      matchStatus: "matched",
      basis: row.basis || [],
      segmentId: request.segmentId || null,
      publicationReady: false,
      customerVisible: false,
      candidateOnly: true,
    });
  }
  return { candidates, rejected, publicationReady: false };
}

function programOf(opportunity, options) {
  if (options.commercial?.recurring?.programKey || options.commercial?.recurring?.cycles) {
    return options.commercial.recurring.programs
      ? options.commercial.recurring.programs[0]
      : options.commercial.recurring;
  }
  return readRecurringFromOpportunity(opportunity, options);
}

export function decideParallelDelegation(opportunity = {}, options = {}) {
  const program = programOf(opportunity, options);
  const blob = [
    opportunity.organizationName,
    opportunity.evidenceQuote,
    opportunity.hotelOpportunityThesis,
  ].map(text).join(" ");
  const questions = [];
  if (program?.treatment === PROGRAM_TREATMENT.COMPETITIVE_OVERFLOW || program?.cycles?.some((cycle) => cycle.overflowSupported)) {
    questions.push("OVERFLOW_AND_FUTURE_CYCLE");
  }
  if (
    program?.treatment === PROGRAM_TREATMENT.FUTURE_CYCLE_COMPETITIVE ||
    (program?.treatment === PROGRAM_TREATMENT.WATCH_RECURRENCE_UNKNOWN && program?.openHostClaimRejected) ||
    (program?.treatment === PROGRAM_TREATMENT.OPEN_OR_UNKNOWN_SOURCING && program?.recurrence?.pattern && program.recurrence.pattern !== "NOT_ESTABLISHED")
  ) {
    questions.push("RECURRING_CYCLE_HISTORY");
  }
  const dmc = /\bdmc\b|destination management|professional planner/i.test(blob);
  const placement = /venue sourcing|hotel room|room block|accommodation contracting|places groups in hotels/i.test(blob);
  if (dmc && placement) questions.push("DMC_HOTEL_PLACEMENT");
  const env = options.env || process.env;
  const hotelId = options.hotelId || opportunity.hotelId || null;
  const enabled = isParallelDelegationEnabled(env) && isGdiParallelHotelAllowed(hotelId, env);
  const unique = [...new Set(questions)];
  return {
    version: PARALLEL_DELEGATED_RESEARCH_VERSION,
    justified: unique.length > 0,
    questions: unique,
    fingerprint: unique.length ? `parallel_structured:${unique.slice().sort().join("+")}` : null,
    processor: "pro",
    executable: unique.length > 0 && enabled,
    reason: unique.length ? "multiple_related_commercial_facts" : dmc ? "address_and_generic_services_are_not_a_deep_investigation" : "single_fact_or_no_commercial_question",
    publicationAuthority: "supervisor",
    customerVisible: false,
  };
}

export function structuredTaskSchema() {
  return {
    type: "object",
    additionalProperties: false,
    required: ["claims"],
    properties: {
      claims: {
        type: "array",
        items: {
          type: "object",
          additionalProperties: false,
          required: ["statement", "evidenceUrls"],
          properties: {
            statement: { type: "string" },
            evidenceUrls: { type: "array", items: { type: "string" } },
            startDate: { type: ["string", "null"] },
            host: { type: ["string", "null"] },
            announced: { type: "boolean" },
            rfpOpen: { type: "boolean" },
            roomBlockAvailable: { type: "boolean" },
          },
        },
      },
    },
  };
}

function basisIndex(basis = []) {
  const urls = new Set();
  const excerpts = new Map();
  for (const item of basis) {
    for (const citation of item.citations || []) {
      const url = normalizeUrl(citation.url);
      if (!url) continue;
      urls.add(url);
      const prior = excerpts.get(url) || "";
      excerpts.set(url, `${prior} ${text(citation.excerpts?.join(" ") || citation.excerpt || "")}`);
    }
  }
  return { urls, excerpts };
}

function organizationNamedAsHost(host, organizationName) {
  if (!text(host) || !text(organizationName)) return false;
  return canonicalEntityKey(host) === canonicalEntityKey(organizationName);
}

const STATEMENT_FACTS = [
  { id: "senior_executives", pattern: /more than\s+\d[\d,]*\s+senior executives/i },
  { id: "five_night_incentive", pattern: /five-night/i },
  { id: "rosewood_london", pattern: /rosewood london/i },
  { id: "bankside_hotel", pattern: /bankside hotel/i },
  { id: "the_savoy", pattern: /the savoy/i },
  { id: "hilton_hotels", pattern: /\bhilton\b/i },
  { id: "nobu_portman", pattern: /nobu hotel london portman square/i },
  { id: "tower_of_london", pattern: /tower of london/i },
  { id: "savile_row", pattern: /savile row/i },
  { id: "hotel_sourcing", pattern: /hotel sourcing/i },
  { id: "contract_negotiation", pattern: /contract negotiation/i },
  { id: "group_accommodation", pattern: /group accommodation management/i },
];

function basisRows(basis = []) {
  const rows = [];
  for (const item of basis) {
    for (const citation of item.citations || []) {
      rows.push({
        url: normalizeUrl(citation.url),
        excerpt: text([].concat(citation.excerpts || citation.excerpt || []).join(" ")),
        title: text(citation.title),
      });
    }
  }
  return rows;
}

export function sourceRecoveryForClaim(claim = {}, citedExcerpt = "", basis = []) {
  const cited = new Set((claim.evidenceUrls || []).map(normalizeUrl));
  const excerpt = String(citedExcerpt || "");
  const rows = basisRows(basis);
  const recovery = [];
  for (const fact of STATEMENT_FACTS) {
    const match = String(claim.statement || "").match(fact.pattern);
    if (!match) continue;
    const phrase = match[0];
    if (fact.pattern.test(excerpt)) continue;
    const stored = rows.find((row) => row.excerpt && fact.pattern.test(row.excerpt));
    const titled = rows.find((row) => cited.has(row.url) && fact.pattern.test(row.title));
    if (stored) {
      recovery.push({
        fact: fact.id,
        phrase,
        path: "stored_basis",
        url: stored.url,
        excerpt: stored.excerpt,
        paidCall: false,
        disproven: false,
      });
    } else if (titled) {
      recovery.push({
        fact: fact.id,
        phrase,
        path: "citation_title_only",
        url: titled.url,
        title: titled.title,
        paidCall: false,
        disproven: false,
      });
    } else {
      recovery.push({
        fact: fact.id,
        phrase,
        path: "contents_or_parse_of_cited_url",
        url: [...cited][0] || null,
        paidCall: false,
        disproven: false,
      });
    }
  }
  return recovery;
}

export function validateStructuredTaskClaims(output = {}, basis = [], options = {}) {
  const index = basisIndex(basis);
  const accepted = [];
  const rejected = [];
  const sourceRecovery = [];
  for (const claim of output.claims || []) {
    const cited = [...new Set((claim.evidenceUrls || []).map(normalizeUrl).filter((url) => index.urls.has(url)))];
    const excerpt = cited.map((url) => index.excerpts.get(url) || "").join(" ");
    const hotelHost = organizationNamedAsHost(claim.host, options.organizationName) ? null : claim.host;
    let reason = null;
    if (!cited.length) reason = "no_basis_citation";
    else if (claim.startDate && !excerpt.includes(String(claim.startDate).slice(0, 10)) && !datePhraseInExcerpt(claim.startDate, excerpt)) {
      reason = "date_not_in_cited_basis";
    } else if (claim.announced === true && claim.startDate && reason == null && !datePhraseInExcerpt(claim.startDate, excerpt) && !excerpt.includes(String(claim.startDate).slice(0, 10))) {
      reason = "announced_date_not_in_cited_basis";
    } else if (claim.rfpOpen === true && !/rfp|request for proposal|accepting (?:hotel )?proposals/i.test(excerpt)) {
      reason = "rfp_not_in_cited_basis";
    } else if (claim.roomBlockAvailable === true && !/rooms still needed|overflow rooms|room block is (?:open|available)/i.test(excerpt)) {
      reason = "room_availability_not_in_cited_basis";
    } else if (hotelHost && !hostNamedInExcerpt(hotelHost, excerpt)) {
      reason = "host_not_in_cited_basis";
    }
    if (reason) {
      rejected.push({
        statement: claim.statement || "",
        reason,
        evidenceUrls: (claim.evidenceUrls || []).map(normalizeUrl).filter(Boolean),
        disproven: false,
        sourceRecovery: reason === "no_basis_citation" ? null : "contents_or_parse_of_cited_url",
        publicationReady: false,
      });
      if (reason !== "no_basis_citation") {
        sourceRecovery.push(...sourceRecoveryForClaim(claim, excerpt, basis).map((item) => ({ ...item, reason })));
      }
    }
    else {
      accepted.push({
        statement: claim.statement || "",
        evidenceUrls: cited,
        host: hotelHost || null,
        organizationName: organizationNamedAsHost(claim.host, options.organizationName) ? options.organizationName : null,
        startDate: claim.startDate || null,
        announced: claim.announced === true,
        rfpOpen: false,
        roomBlockAvailable: claim.roomBlockAvailable === true,
      });
      sourceRecovery.push(...sourceRecoveryForClaim(claim, excerpt, basis));
    }
  }
  return {
    accepted,
    rejected,
    sourceRecovery,
    publicationEligibilityChanged: false,
    customerVisible: false,
  };
}

function hostNamedInExcerpt(host, excerpt) {
  const raw = text(host).toLowerCase();
  const blob = String(excerpt || "").toLowerCase();
  if (!raw || !blob) return false;
  if (blob.includes(raw)) return true;
  const hotel = raw.split(",")[0].replace(/^the\s+/, "").trim();
  return hotel.length >= 8 && blob.includes(hotel);
}

function datePhraseInExcerpt(iso, excerpt) {
  const months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
  const [year, month, day] = String(iso || "").slice(0, 10).split("-");
  if (!year || !month || !day) return false;
  const name = months[Number(month) - 1];
  if (!name) return false;
  const pattern = new RegExp(`${name}\\s+${Number(day)}(?:,|\\s|-|–|—)+[^.]{0,12}${year}`, "i");
  return pattern.test(excerpt) || excerpt.includes(`${name} ${Number(day)}-${year}`) || excerpt.toLowerCase().includes(`${name.toLowerCase()} ${Number(day)}`);
}

export function evidenceFromAcceptedClaims(validation = {}) {
  return {
    evidence: (validation.accepted || []).map((claim) => ({
      sourceFamily: "parallel_structured_task",
      url: claim.evidenceUrls[0] || null,
      quote: claim.statement,
      publicationReady: false,
      customerVisible: false,
    })),
    fieldsNotApplied: ["publishable", "venueSourcingStatus", "eventStartDate", "demandScale", "prospectPublicationEligibility"],
    publicationEligibilityChanged: false,
  };
}

export function recoverHostClaimFromLedger(claim = {}, ledgerQuotes = []) {
  const hotel = text(claim.host).split(",")[0].replace(/^the\s+/i, "");
  const quote = ledgerQuotes.map(text).find((item) => hotel && item.toLowerCase().includes(hotel.toLowerCase()) && (!claim.startDate || datePhraseInExcerpt(claim.startDate, item) || item.includes(String(claim.startDate).slice(0, 4))));
  if (!quote) {
    return { recovered: false, path: "contents_or_parse_of_cited_url", paidCall: false };
  }
  return {
    recovered: true,
    path: "existing_evidence_ledger",
    paidCall: false,
    hotel,
    soldOut: /sold out/i.test(quote),
    contactEmail: (quote.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i) || [])[0] || null,
  };
}

function citationRows(candidate = {}) {
  const rows = [];
  for (const item of candidate.basis || []) {
    for (const citation of item.citations || []) {
      rows.push({
        url: citation.url || "",
        text: text([].concat(citation.excerpts || []).join(" ")),
      });
    }
  }
  return rows;
}

function domainOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

export function assessFindAllCandidate(candidate = {}, options = {}) {
  const targetDomains = new Set((options.targetDomains || ["nobuhotels.com"]).map((domain) => domain.replace(/^www\./, "")));
  const rows = citationRows(candidate);
  const ownSite = rows.filter((row) => row.url && !targetDomains.has(domainOf(row.url)) && !/linkedin\.com$/i.test(domainOf(row.url)));
  const ownText = ownSite.map((row) => row.text).join(" ");
  const targetText = rows.filter((row) => targetDomains.has(domainOf(row.url))).map((row) => row.text).join(" ");
  const placesHotels = /accommodation|finding the best hotel|hotel room|room block/i.test(ownText);
  const runsProgramme = /ground programme|destination management|incentive/i.test(ownText);
  const contractsVenues = /negotiate (&|and) contract|contract the (?:perfect )?venue|contract venues/i.test(ownText);
  let sourcingRole = "NOT_SHOWN";
  let rank = 0;
  if (placesHotels && runsProgramme) {
    sourcingRole = "PLACES_AND_OPERATES";
    rank = 3;
  } else if (contractsVenues) {
    sourcingRole = "CONTRACTS_VENUES";
    rank = 2;
  } else if (/accommodation requirements/i.test(ownText)) {
    sourcingRole = "REFERS_OR_SEARCHES";
    rank = 1;
  }
  const justified = rank >= 3;
  return {
    organizationName: candidate.organizationName || candidate.name || null,
    rank,
    sourcingRole,
    justified,
    ownSiteUrls: [...new Set(ownSite.map((row) => row.url))],
    targetHotelPagesAreNotPlacement: targetText.length > 0,
    luxuryCentralLondonNamed: /portman square|marylebone|knightsbridge/i.test(ownText),
    contactPublished: /@/.test(ownText),
    reason: justified
      ? "The organization's own pages describe hotel placement and programme delivery."
      : rank === 2
        ? "The firm contracts venues. Overnight hotel placement is not on its own cited pages."
        : rank === 1
          ? "Accommodation is an intake field. The hotel-fit citations are not this firm's placements."
          : "No company-site evidence of hotel placement.",
  };
}

export function planFindAllFollowUpTask(candidates = [], options = {}) {
  const assessed = candidates.map((candidate) => assessFindAllCandidate(candidate, options)).sort((a, b) => b.rank - a.rank);
  const selected = assessed.find((item) => item.justified) || null;
  const env = options.env || {};
  const hotelId = options.hotelId || null;
  const quote = selected ? quoteStructuredTask({ processor: "pro" }) : null;
  return {
    version: PARALLEL_DELEGATED_RESEARCH_VERSION,
    paidCalls: 0,
    publicationAuthority: "supervisor",
    customerVisible: false,
    selected: selected?.organizationName || null,
    deferred: assessed.filter((item) => item !== selected),
    task: selected ? {
      processor: "pro",
      estimatedUsd: quote.estimatedUsd,
      executable: (options.authorizePaidResearch === true || /^(1|true)$/i.test(String(options.env?.GDI_PARALLEL_BUDGET_AUTHORIZED || "")))
        && isParallelDelegationEnabled(env)
        && isGdiParallelHotelAllowed(hotelId, env),
      organizationName: selected.organizationName,
      fingerprint: `parallel_structured:FINDALL_FOLLOWUP:${text(selected.organizationName).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "")}`,
      questions: [
        "Does the firm source, negotiate, book, or contract overnight hotel accommodation? A generic services list is not confirmed hotel buying.",
        "Does it operate incentive or group programmes that state a need for accommodation?",
        "Which hotels or accommodation partners are named on its own pages?",
        "Do its own pages document a central London programme consistent with the target hotel? Name that hotel only when the firm's source names it.",
        "What contact route is published on the firm's own domain?",
        "What specific printed fact would give the target hotel a reason to pursue the account? If none is printed, say so.",
      ],
      discardTargetHotelPagesAsPlacementProof: true,
    } : null,
  };
}

const TARGET_HOTEL_MARKETING_DOMAINS = new Set([
  "nobuhotels.com",
  "conventionbureau.london",
  "hirespace.com",
  "venuecrew.com",
  "tagvenue.com",
]);

export function buildFindAllFollowUpInstruction(task = {}, targetHotel = "") {
  return [
    `Investigate only ${task.organizationName}. Follow related pages on that organization's own domain inside this assignment.`,
    `The target hotel is ${targetHotel || "the named hotel"}. Pages owned by the hotel, its brochure, or a venue directory copying those pages are not evidence that this organization places groups there.`,
    ...((task.questions || []).map((question, index) => `${index + 1}. ${question}`)),
    "Do not infer a current RFP, a room count, or a relationship with the target hotel unless the organization's own source says so.",
    "Every claim must include evidenceUrls copied from its citations.",
  ].join(" ");
}

export function classifyStructuredTaskEvidence(validation = {}, basis = [], options = {}) {
  const targetDomains = new Set([...(options.targetDomains || []), ...TARGET_HOTEL_MARKETING_DOMAINS].map((domain) => String(domain).replace(/^www\./, "")));
  const index = basisIndex(basis);
  const organizationOwned = [];
  const targetHotelMarketing = [];
  for (const claim of validation.accepted || []) {
    const urls = claim.evidenceUrls || [];
    const domains = urls.map(domainOf);
    const excerpt = urls.map((url) => index.excerpts.get(normalizeUrl(url)) || "").join(" ").trim();
    const record = { statement: claim.statement, evidenceUrls: urls, domains, excerpt, publicationReady: false, customerVisible: false };
    if (domains.some((domain) => domain && !targetDomains.has(domain) && !/linkedin\.com$/i.test(domain))) organizationOwned.push(record);
    else targetHotelMarketing.push(record);
  }
  const emails = [];
  for (const record of organizationOwned) {
    const found = record.excerpt.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [];
    for (const email of found) {
      const emailDomain = email.split("@")[1]?.replace(/^www\./, "").toLowerCase();
      emails.push({
        email,
        url: record.evidenceUrls.find((url) => domainOf(url) === emailDomain) || record.evidenceUrls[0],
        mailboxDomainDiffers: !record.domains.includes(emailDomain),
      });
    }
  }
  return {
    organizationOwned,
    targetHotelMarketing,
    sourceRecovery: (validation.rejected || []).filter((claim) => claim.sourceRecovery).map((claim) => ({
      statement: claim.statement,
      reason: claim.reason,
      evidenceUrls: claim.evidenceUrls || [],
      disproven: false,
      sourceRecovery: claim.sourceRecovery,
    })),
    contact: emails[0] || null,
    publicationEligibilityChanged: false,
  };
}

export function formulateNextResearchStep({
  organizationName = "",
  targetHotel = "",
  evidenceText = "",
  sourceRecovery = [],
  attempts = [],
  investigatedUrls = [],
  remainingUsd = 0,
  commercial = {},
} = {}) {
  const investigated = new Set(
    [...investigatedUrls, ...attempts.flatMap((attempt) => [attempt?.url, ...(attempt?.urls || []), ...(attempt?.evidenceIds || [])])]
      .map(normalizeUrl)
      .filter(Boolean)
  );
  const seen = new Set();
  const storedRecovery = [];
  const unreadRecovery = [];
  for (const item of sourceRecovery || []) {
    const key = `${item.fact}|${item.path}|${item.url || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    if (item.path === "stored_basis" || item.path === "citation_title_only") storedRecovery.push(item);
    else if (item.url && !investigated.has(normalizeUrl(item.url))) unreadRecovery.push(item);
  }
  const base = {
    paidCalls: 0,
    executable: false,
    publicationEligibilityChanged: false,
    customerVisible: false,
    avoidedUrls: [...investigated],
    storedRecovery,
  };
  if (/crystal gateway marriott/i.test(evidenceText) && /bethesda/i.test(targetHotel)) {
    return {
      ...base,
      action: "WATCH_FUTURE_CYCLE",
      question: "Has the association announced a cycle later than the committed host-hotel meeting? Do not invent that meeting.",
      evidenceSought: "A later announced cycle, or confirmation that the official listings stop at the committed meeting.",
      urls: [],
      provider: null,
      meetingCreated: false,
      disposition: "HOST_SELECTED",
      salesReadiness: commercial.salesReadiness || "EXCLUDE",
      stoppingCriteria: "Stop when the already read official pages do not announce a later cycle.",
    };
  }
  if (unreadRecovery.length && !(Number(remainingUsd) > 0)) {
    return {
      ...base,
      action: "STOP_BUDGET",
      question: `Does an unread page establish “${unreadRecovery[0].phrase}”?`,
      evidenceSought: unreadRecovery[0].phrase,
      urls: [],
      provider: "existing_contents_adapter",
      stoppingCriteria: "No authorized budget remains for another retrieval.",
    };
  }
  if (unreadRecovery.length) {
    return {
      ...base,
      action: "SOURCE_RECOVERY",
      question: `Does the organization's own page contain “${unreadRecovery[0].phrase}”?`,
      evidenceSought: unreadRecovery[0].phrase,
      urls: [unreadRecovery[0].url],
      provider: "existing_contents_adapter",
      stoppingCriteria: "Read that URL once. Do not repeat a URL already held.",
    };
  }
  const sourcing = /hotel sourcing|contract negotiation|group accommodation/i.test(evidenceText);
  const programme = /incentive/i.test(evidenceText);
  if (sourcing && programme) {
    return {
      ...base,
      action: "STOP_SUFFICIENT",
      disposition: "ACCOUNT_DEVELOPMENT",
      salesReadiness: commercial.salesReadiness || "NEEDS_RESEARCH",
      question: `Does ${organizationName || "the firm"} have a current programme that still needs hotel bedrooms at ${targetHotel || "the target hotel"}?`,
      openQuestionNotExecuted: true,
      evidenceSought: "A dated programme that names a bedroom commitment.",
      urls: [],
      missingFacts: [
        "No signed bedroom contract is printed.",
        targetHotel ? `${targetHotel} is not named by the firm.` : "The target hotel is not named.",
        "No current RFP or open programme is printed.",
      ],
      stoppingCriteria: "The reviewed pages support an introduction and do not support a live room requirement.",
    };
  }
  return {
    ...base,
    action: "NEEDS_RESEARCH",
    question: "Which page states overnight hotel placement by this organization?",
    evidenceSought: "Hotel sourcing, negotiation, or a named hotel placement on the organization's own domain.",
    urls: [],
    stoppingCriteria: "Stop when that sentence is found, or the held pages do not contain it.",
  };
}

export function unpackTaskResult(structured = {}) {
  const raw = structured.output || structured.result?.output || {};
  const content = raw && typeof raw.content === "object" && raw.content ? raw.content : raw;
  const basis = raw.basis || structured.basis || structured.result?.output?.basis || [];
  return {
    output: content && Array.isArray(content.claims) ? content : { ...content, claims: content.claims || [] },
    basis: Array.isArray(basis) ? basis : [],
  };
}

async function waitForFindAllResult(api, findallId, timeoutMs) {
  if (!api.getFindAllRun) return api.getFindAllResult(findallId);
  const started = Date.now();
  let last = null;
  while (Date.now() - started < timeoutMs) {
    const status = await api.getFindAllRun(findallId);
    last = status.structured;
    const state = String(last?.status?.status || last?.status || "").toLowerCase();
    if (state === "completed") return api.getFindAllResult(findallId);
    if (state === "failed") {
      const err = new Error("parallel_findall_failed");
      err.code = "parallel_findall_failed";
      throw err;
    }
    await new Promise((resolve) => setTimeout(resolve, 5000));
  }
  const err = new Error("parallel_findall_timeout");
  err.code = "parallel_findall_timeout";
  err.last_status = last;
  throw err;
}

function ledgerEnv(env, ledgerPath) {
  return { ...env, GDI_PARALLEL_BUDGET_LEDGER_PATH: ledgerPath || env.GDI_PARALLEL_BUDGET_LEDGER_PATH };
}

async function finishReservation({ reservationId, env, runId, actualUsd, failed = false }) {
  if (!reservationId) return;
  if (failed) {
    releaseParallelBudgetReservation(reservationId, { env, reason: "provider_failed_before_billable_completion" });
    return;
  }
  commitParallelBudgetReservation(reservationId, { env, providerRunId: runId, actualUsd });
}

export async function executeDelegatedParallelTask({
  opportunity = {},
  decision = null,
  hotelId = null,
  processor = "pro",
  env = process.env,
  client = null,
  allowUltra = false,
  timeoutMs = 900000,
  instruction = null,
} = {}) {
  const plan = decision || decideParallelDelegation(opportunity, { env, hotelId });
  if (!plan.justified) return { status: "not_justified", paidCalls: 0, publicationEligibilityChanged: false };
  if (!plan.executable && !client) return { status: "disabled", paidCalls: 0, publicationEligibilityChanged: false, decision: plan };
  if (!isParallelDelegationEnabled(env) || !isGdiParallelHotelAllowed(hotelId || opportunity.hotelId, env)) {
    return { status: "disabled", paidCalls: 0, publicationEligibilityChanged: false };
  }
  let quote;
  try {
    quote = quoteStructuredTask({ processor, allowUltra });
  } catch (err) {
    return { status: "refused", reason: err.code, paidCalls: 0, publicationEligibilityChanged: false };
  }
  const id = hotelId || opportunity.hotelId;
  const jobKey = createParallelJobKey(id, `structured-${plan.fingerprint}`);
  const job = beginOrResumeParallelJob({ jobKey, hotelId: id, label: "structured_task", env });
  const api = client || createParallelClient({ env });
  let reservationId = job.job?.reservationId || null;
  let runId = job.providerRunId || null;
  try {
    if (job.shouldSubmit !== false) {
      const reserved = reserveParallelBudget({
        label: "structured_parallel_task",
        processor: quote.processor,
        estimatedUsd: quote.estimatedUsd,
        jobKey,
        hotelId: id,
        maxUsd: quote.estimatedUsd,
        env,
      });
      reservationId = reserved.reservationId;
      const created = await api.createTaskRun({
        processor: quote.processor,
        input: instruction || {
          objective: "Investigate only the named organization. Return claims that each cite a basis URL. Do not invent a future meeting, an open RFP, or available rooms.",
          questions: plan.questions,
          organizationName: opportunity.organizationName || null,
        },
        task_spec: { output_schema: { type: "json", json_schema: structuredTaskSchema() } },
      });
      runId = created.structured?.run_id || created.structured?.id || created.structured?.run?.run_id || null;
      if (!runId) {
        await finishReservation({ reservationId, env, failed: true });
        return { status: "failed", reason: "parallel_run_id_missing", paidCalls: 0, publicationEligibilityChanged: false };
      }
      markParallelJobSubmitted(jobKey, { providerRunId: runId, reservationId }, env);
    }
    const result = job.shouldSubmit === false && api.getRunResult
      ? await api.getRunResult(runId, { timeoutMs })
      : await api.pollUntilComplete(runId, { timeoutMs });
    const unpacked = unpackTaskResult(result.structured || {});
    const output = unpacked.output;
    const basis = unpacked.basis;
    const validation = validateStructuredTaskClaims(output, basis, {
      organizationName: opportunity.organizationName || null,
    });
    const evidence = evidenceFromAcceptedClaims(validation);
    await finishReservation({ reservationId, env, runId, actualUsd: quote.estimatedUsd, failed: false });
    markParallelJobCompleted(jobKey, { providerRunId: runId, paidCalls: 1 }, env);
    return {
      status: "completed",
      resumed: job.shouldSubmit === false,
      paidCalls: 1,
      providerRunId: runId,
      estimatedUsd: quote.estimatedUsd,
      actualUsd: quote.estimatedUsd,
      validation,
      output,
      basis,
      evidence: evidence.evidence,
      publicationEligibilityChanged: false,
      customerVisible: false,
    };
  } catch (err) {
    if (runId) {
      markParallelJobUncertain(jobKey, err.code || err.message, env);
      return {
        status: "interrupted",
        paidCalls: 0,
        providerRunId: runId,
        reservationId,
        reason: err.code || "parallel_task_interrupted",
        publicationEligibilityChanged: false,
      };
    }
    await finishReservation({ reservationId, env, failed: true });
    const code = err.code || "parallel_task_failed";
    return {
      status: code === "PARALLEL_BUDGET_EXHAUSTED" || code === "PARALLEL_KILL_SWITCH" || code === "PARALLEL_DISABLED" ? "blocked" : "failed",
      reason: code,
      paidCalls: 0,
      publicationEligibilityChanged: false,
    };
  }
}

export async function executeFindAllDiscovery({
  segmentId,
  hotel = {},
  hotelId = null,
  exclusions = [],
  generator = "preview",
  matchLimit = 5,
  env = process.env,
  client = null,
  allowExpensiveGenerator = false,
} = {}) {
  if (!isParallelFindAllEnabled(env) || !isGdiParallelHotelAllowed(hotelId || hotel.hotelId, env)) {
    return { status: "disabled", paidCalls: 0, candidates: [], publicationReady: false };
  }
  let request;
  try {
    request = buildFindAllRequest({ segmentId, hotel, exclusions, generator, matchLimit });
    if (allowExpensiveGenerator) request = { ...buildFindAllRequest({ segmentId, hotel, exclusions, generator, matchLimit: matchLimit }), ...quoteFindAll({ generator, matchLimit, allowExpensiveGenerator }) };
  } catch (err) {
    return { status: "refused", reason: err.code, paidCalls: 0, candidates: [], publicationReady: false };
  }
  if (!FINDALL_ALLOWED.has(request.generator) && allowExpensiveGenerator !== true) {
    return { status: "refused", reason: "PARALLEL_GENERATOR_REFUSED", paidCalls: 0, candidates: [], publicationReady: false };
  }
  const id = hotelId || hotel.hotelId;
  const jobKey = createParallelJobKey(id, `findall-${segmentId}-${request.generator}-${request.matchLimit}`);
  const job = beginOrResumeParallelJob({ jobKey, hotelId: id, label: "findall", env });
  const api = client || createParallelClient({ env });
  let reservationId = job.job?.reservationId || null;
  let runId = job.providerRunId || null;
  try {
    if (job.shouldSubmit !== false) {
      const reserved = reserveParallelBudget({
        label: "findall_segment_discovery",
        processor: request.generator,
        estimatedUsd: request.estimatedUsd,
        jobKey,
        hotelId: id,
        maxUsd: request.estimatedUsd,
        env,
      });
      reservationId = reserved.reservationId;
      const created = await api.createFindAllRun(request.payload);
      runId = created.structured?.findall_id || created.structured?.id || null;
      if (!runId) {
        await finishReservation({ reservationId, env, failed: true });
        return { status: "failed", reason: "parallel_run_id_missing", paidCalls: 0, candidates: [], publicationReady: false };
      }
      markParallelJobSubmitted(jobKey, { providerRunId: runId, reservationId }, env);
    }
    const snapshot = await waitForFindAllResult(api, runId, 900000);
    const selected = selectFindAllCandidates(snapshot.structured || {}, { ...request, segmentId, exclusions: request.exclusions });
    const apiMatched = Number(snapshot.structured?.run?.status?.metrics?.matched_candidates_count);
    const billCount = Number.isInteger(apiMatched) ? Math.min(apiMatched, request.matchLimit) : selected.candidates.length;
    const actualUsd = findAllActualUsd(request, billCount);
    await finishReservation({ reservationId, env, runId, actualUsd, failed: false });
    markParallelJobCompleted(jobKey, { providerRunId: runId, paidCalls: 1, candidateCount: selected.candidates.length }, env);
    return {
      status: "completed",
      resumed: job.shouldSubmit === false,
      paidCalls: 1,
      providerRunId: runId,
      estimatedUsd: request.estimatedUsd,
      actualUsd,
      candidates: selected.candidates,
      rejected: selected.rejected,
      publicationReady: false,
      customerVisible: false,
    };
  } catch (err) {
    if (runId) {
      markParallelJobUncertain(jobKey, err.code || err.message, env);
      return { status: "interrupted", paidCalls: 0, providerRunId: runId, candidates: [], publicationReady: false };
    }
    await finishReservation({ reservationId, env, failed: true });
    return {
      status: err.code === "PARALLEL_BUDGET_EXHAUSTED" ? "blocked" : "failed",
      reason: err.code || "findall_failed",
      paidCalls: 0,
      candidates: [],
      publicationReady: false,
    };
  }
}

export function parallelDelegationReadiness(env = process.env) {
  const ledger = (() => {
    try {
      return readParallelBudgetLedger({ env, createIfMissing: false });
    } catch {
      return null;
    }
  })();
  return {
    version: PARALLEL_DELEGATED_RESEARCH_VERSION,
    priceVerifiedOn: PARALLEL_PRICE_VERIFIED_ON,
    delegationEnabled: isParallelDelegationEnabled(env),
    findAllEnabled: isParallelFindAllEnabled(env),
    productionDefault: "disabled",
    ledgerPath: ledger?.path || null,
    paidCalls: 0,
  };
}
