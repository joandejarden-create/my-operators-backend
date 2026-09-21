/**
 * GDI WHO Recall V12 — gap-directed recovery without loosening V11 precision.
 *
 * Ordering remains: RAW → structure → entity type (V11) → PERSON confirm
 * → role → event relation → ranking.
 *
 * No hotel/person/event/domain hardcodes in production logic.
 */

import { personTypeGateV11 } from "./person-boundary-v11.js";
import { aggregatorWhoGateV11 } from "./aggregator-who-gate-v11.js";
import { hostnameOf } from "./official-domain.js";

export const WHO_RECALL_GAP_V12 = Object.freeze({
  NO_STAFF_SOURCE_FOUND: "NO_STAFF_SOURCE_FOUND",
  EVENT_CONTACT_NOT_FOUND: "EVENT_CONTACT_NOT_FOUND",
  ROLE_NOT_ESTABLISHED: "ROLE_NOT_ESTABLISHED",
  FUNCTIONAL_ONLY: "FUNCTIONAL_ONLY",
  OPERATOR_UNRESOLVED: "OPERATOR_UNRESOLVED",
  HOUSING_CONTACT_UNRESOLVED: "HOUSING_CONTACT_UNRESOLVED",
  MULTIPLE_CANDIDATES_UNRESOLVED: "MULTIPLE_CANDIDATES_UNRESOLVED",
  SOURCE_ACCESS_FAILED: "SOURCE_ACCESS_FAILED",
  DYNAMIC_PAGE: "DYNAMIC_PAGE",
  PDF_CONTACT_MISSED: "PDF_CONTACT_MISSED",
  ALIAS_DOMAIN_FAILURE: "ALIAS_DOMAIN_FAILURE",
  QUERY_RECALL: "QUERY_RECALL",
  CURRENTNESS_NOT_ESTABLISHED: "CURRENTNESS_NOT_ESTABLISHED",
  GENUINE_NO_WHO: "GENUINE_NO_WHO",
  OTHER: "OTHER",
});

export const RECALL_STAGE_V12 = Object.freeze({
  STAGE_1_EVENT_CONTACT: "STAGE_1_EVENT_CONTACT",
  STAGE_2_ORG_STAFF: "STAGE_2_ORG_STAFF",
  STAGE_3_SERP_DEEP_LINK: "STAGE_3_SERP_DEEP_LINK",
  STAGE_4_ROLE_SPECIFIC: "STAGE_4_ROLE_SPECIFIC",
  STAGE_5_OFFICIAL_PDF: "STAGE_5_OFFICIAL_PDF",
  STAGE_6_OPERATOR_HOUSING: "STAGE_6_OPERATOR_HOUSING",
  STAGE_7_FUNCTIONAL_PIVOT: "STAGE_7_FUNCTIONAL_PIVOT",
});

const FUNCTIONAL_LOCAL =
  /^(info|events|meetings|conference|housing|registration|contact|hello|office|admin|support|sales|inquiries|customerservice)@/i;

const ROLE_FAMILY_QUERIES = Object.freeze([
  "conference director",
  "meetings manager",
  "event manager",
  "housing coordinator",
  "registration manager",
  "local arrangements",
  "program chair",
  "partnerships manager",
  "secretary general",
  "executive director events",
]);

/**
 * Classify primary WHO recall gap from prior research shape.
 */
export function classifyWhoRecallGapV12(row = {}) {
  const functional = row.functionalContacts || [];
  const hasFunctional = functional.some((f) => f?.email || f?.phone);
  const state = String(row.currentNoWhoReason || row.researchState || "");
  const priorPeople = [
    ...(row.priorV9?.people || []),
    ...(row.priorV10?.people || []),
  ];
  const secondary = [];

  // Prior confirmed people later invalidated by aggregator/domain → alias failure
  if (
    /CONFIRMED/i.test(String(row.priorV9?.researchState || "")) &&
    priorPeople.length &&
    !hasFunctional
  ) {
    return {
      primaryGap: WHO_RECALL_GAP_V12.ALIAS_DOMAIN_FAILURE,
      secondaryGaps: [WHO_RECALL_GAP_V12.EVENT_CONTACT_NOT_FOUND],
      rationale: "Prior WHO came from non-authoritative / aggregator path; need official domain",
    };
  }

  if (hasFunctional && /FUNCTIONAL/i.test(state)) {
    secondary.push(WHO_RECALL_GAP_V12.NO_STAFF_SOURCE_FOUND);
    return {
      primaryGap: WHO_RECALL_GAP_V12.FUNCTIONAL_ONLY,
      secondaryGaps: secondary,
      rationale: "Functional inbox present; named owner of function not resolved",
    };
  }

  if (/NEEDS_VALIDATION/i.test(state)) {
    return {
      primaryGap: WHO_RECALL_GAP_V12.ROLE_NOT_ESTABLISHED,
      secondaryGaps: [WHO_RECALL_GAP_V12.QUERY_RECALL],
      rationale: "Candidates seen but role/event relation not established",
    };
  }

  if (!hasFunctional) {
    return {
      primaryGap: WHO_RECALL_GAP_V12.EVENT_CONTACT_NOT_FOUND,
      secondaryGaps: [WHO_RECALL_GAP_V12.NO_STAFF_SOURCE_FOUND, WHO_RECALL_GAP_V12.QUERY_RECALL],
      rationale: "No named WHO and no usable functional path after prior research",
    };
  }

  return {
    primaryGap: WHO_RECALL_GAP_V12.GENUINE_NO_WHO,
    secondaryGaps: [],
    rationale: "Prior research complete without confirmable person",
  };
}

/**
 * Extract registrant/org domains from functional emails for site: deep-links.
 * Structural only — no domain denylist/allowlist of specific orgs.
 */
export function domainsFromFunctionalContacts(functionalContacts = []) {
  const hosts = new Set();
  for (const f of functionalContacts || []) {
    const email = String(f.email || "");
    const m = email.match(/@([a-z0-9.-]+\.[a-z]{2,})$/i);
    if (!m) continue;
    const host = m[1].toLowerCase().replace(/^www\./, "");
    // Skip freeweb / consumer mail hosts
    if (/^(gmail|yahoo|hotmail|outlook|icloud|aol|protonmail)\./i.test(host)) continue;
    hosts.add(host);
  }
  for (const f of functionalContacts || []) {
    const host = hostnameOf(f.sourceUrl || "");
    if (host && !/wikipedia|linkedin|facebook/i.test(host)) hosts.add(host);
  }
  return [...hosts];
}

/**
 * Build gap-directed SERP / site: queries (generic patterns).
 */
export function buildGapDirectedQueriesV12(opp = {}, gap = null) {
  const classified = gap || classifyWhoRecallGapV12(opp);
  const primary = classified.primaryGap;
  const org = String(opp.organization || "").trim();
  const event = String(opp.eventName || opp.opportunityName || "").trim();
  const domains = domainsFromFunctionalContacts(opp.functionalContacts || []);
  const queries = [];

  const push = (q) => {
    const s = String(q || "").replace(/\s+/g, " ").trim();
    if (s.length >= 8) queries.push(s);
  };

  // Stage 1–2 always useful
  if (event) {
    push(`"${event}" contact OR staff OR "conference director" OR "meeting planner"`);
    push(`"${event}" "local arrangements" OR housing OR registration`);
  }
  if (org) {
    push(`"${org}" staff OR team OR "meetings" OR "events" OR "conference"`);
    push(`"${org}" "executive director" OR "meetings manager" OR "conference director"`);
  }

  // Site deep-links from functional domains
  for (const host of domains.slice(0, 4)) {
    push(`site:${host} staff OR team OR meetings OR events OR conference OR contact`);
    push(`site:${host} "local arrangements" OR housing OR registration OR partnerships`);
  }

  if (primary === WHO_RECALL_GAP_V12.FUNCTIONAL_ONLY) {
    for (const host of domains.slice(0, 3)) {
      push(`site:${host} "director of" (events OR meetings OR conference OR development)`);
    }
    push(`${org || event} "director of events" OR "director of meetings" OR "special events"`);
  }

  if (
    primary === WHO_RECALL_GAP_V12.ALIAS_DOMAIN_FAILURE ||
    primary === WHO_RECALL_GAP_V12.EVENT_CONTACT_NOT_FOUND
  ) {
    push(`"${event || org}" official site OR about OR contact -conferences.com`);
    push(`"${org}" "${event.split(/\s+/).slice(0, 4).join(" ")}" chair OR director OR organizer`);
  }

  if (primary === WHO_RECALL_GAP_V12.OPERATOR_UNRESOLVED) {
    push(`"${event || org}" "event management" OR "association management" OR "conference producer"`);
    push(`"${event || org}" housing bureau OR "registration vendor" OR passkey OR onpeak`);
  }

  if (primary === WHO_RECALL_GAP_V12.PDF_CONTACT_MISSED || primary === WHO_RECALL_GAP_V12.EVENT_CONTACT_NOT_FOUND) {
    push(`"${event || org}" prospectus OR program OR "housing guide" OR "registration guide" filetype:pdf`);
  }

  // Role-first family queries
  for (const role of ROLE_FAMILY_QUERIES.slice(0, 5)) {
    push(`"${org || event}" ${role}`);
  }

  // Dedupe preserve order
  const seen = new Set();
  return queries.filter((q) => {
    const k = q.toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

/**
 * Ordered recall stages for a gap class.
 */
export function recallStagesForGapV12(primaryGap) {
  const g = String(primaryGap || "");
  switch (g) {
    case WHO_RECALL_GAP_V12.FUNCTIONAL_ONLY:
      return [
        RECALL_STAGE_V12.STAGE_7_FUNCTIONAL_PIVOT,
        RECALL_STAGE_V12.STAGE_2_ORG_STAFF,
        RECALL_STAGE_V12.STAGE_3_SERP_DEEP_LINK,
        RECALL_STAGE_V12.STAGE_4_ROLE_SPECIFIC,
        RECALL_STAGE_V12.STAGE_5_OFFICIAL_PDF,
      ];
    case WHO_RECALL_GAP_V12.ALIAS_DOMAIN_FAILURE:
      return [
        RECALL_STAGE_V12.STAGE_1_EVENT_CONTACT,
        RECALL_STAGE_V12.STAGE_3_SERP_DEEP_LINK,
        RECALL_STAGE_V12.STAGE_2_ORG_STAFF,
        RECALL_STAGE_V12.STAGE_6_OPERATOR_HOUSING,
      ];
    case WHO_RECALL_GAP_V12.OPERATOR_UNRESOLVED:
      return [
        RECALL_STAGE_V12.STAGE_6_OPERATOR_HOUSING,
        RECALL_STAGE_V12.STAGE_1_EVENT_CONTACT,
        RECALL_STAGE_V12.STAGE_4_ROLE_SPECIFIC,
      ];
    case WHO_RECALL_GAP_V12.PDF_CONTACT_MISSED:
      return [
        RECALL_STAGE_V12.STAGE_5_OFFICIAL_PDF,
        RECALL_STAGE_V12.STAGE_1_EVENT_CONTACT,
        RECALL_STAGE_V12.STAGE_2_ORG_STAFF,
      ];
    default:
      return [
        RECALL_STAGE_V12.STAGE_1_EVENT_CONTACT,
        RECALL_STAGE_V12.STAGE_2_ORG_STAFF,
        RECALL_STAGE_V12.STAGE_3_SERP_DEEP_LINK,
        RECALL_STAGE_V12.STAGE_4_ROLE_SPECIFIC,
        RECALL_STAGE_V12.STAGE_5_OFFICIAL_PDF,
        RECALL_STAGE_V12.STAGE_6_OPERATOR_HOUSING,
        RECALL_STAGE_V12.STAGE_7_FUNCTIONAL_PIVOT,
      ];
  }
}

/**
 * Confirm a recall candidate under full V11 precision law.
 * Does not loosen entity/aggregator/event gates.
 */
export function confirmWhoRecallCandidateV12(candidate = {}, ctx = {}) {
  const typeGate = personTypeGateV11({
    name: candidate.name,
    role: candidate.role,
    email: candidate.email,
    phone: candidate.phone,
    sectionKind: candidate.sectionKind || "STAFF",
    evidenceQuote: candidate.evidenceQuote || candidate.rationale || "",
    sourceUrl: candidate.sourceUrl,
    organization: candidate.organization || ctx.organization,
    onOfficialDomain: candidate.onOfficialDomain,
    domainClass: candidate.domainClass,
    fromStructuredData: candidate.fromStructuredData,
    schemaType: candidate.schemaType,
    profileUrl: candidate.profileUrl,
  });

  if (typeGate.reject) {
    return {
      accept: false,
      reason: typeGate.gate,
      entityType: typeGate.entityType,
      gate: typeGate,
    };
  }

  const name = typeGate.normalizedPersonName || candidate.name;
  const agg = aggregatorWhoGateV11(
    {
      name,
      sourceUrl: candidate.sourceUrl,
      domainClass: candidate.domainClass,
      onOfficialDomain: candidate.onOfficialDomain,
      officialCorroboration: Boolean(
        candidate.officialCorroboration ||
          candidate.onOfficialDomain ||
          /OFFICIAL/i.test(String(candidate.domainClass || ""))
      ),
      organization: candidate.organization || ctx.organization,
    },
    {
      organization: ctx.organization,
      opportunityName: ctx.opportunityName || ctx.eventName,
      eventSourceUrls: ctx.eventSourceUrls || [],
    }
  );

  if (agg.reject) {
    return {
      accept: false,
      reason: agg.gate,
      entityType: "PERSON",
      gate: agg,
    };
  }

  // Minimal event-relation / role relevance: require role or explicit event quote
  const role = String(candidate.role || "").trim();
  const quote = String(candidate.evidenceQuote || candidate.rationale || "");
  if (!role && !/director|manager|chair|coordinator|secretary|planner|housing|registration/i.test(quote)) {
    return {
      accept: false,
      reason: "ROLE_NOT_ESTABLISHED",
      entityType: "PERSON",
    };
  }

  return {
    accept: true,
    name,
    reason: null,
    entityType: "PERSON",
    typeGate,
    aggGate: agg,
  };
}

/**
 * Functional-local detector for pivot eligibility.
 */
export function isFunctionalEmailLocal(email) {
  return FUNCTIONAL_LOCAL.test(String(email || ""));
}

/**
 * Build discoverWhoV9 input enrichment from V12 gap plan.
 */
export function enrichDiscoverInputV12(baseInput = {}, opp = {}) {
  const gap = classifyWhoRecallGapV12(opp);
  const queries = buildGapDirectedQueriesV12(opp, gap);
  const domains = domainsFromFunctionalContacts(opp.functionalContacts || []);
  const seedUrls = [
    ...(baseInput.eventSourceUrls || []),
    ...(opp.officialSourceUrls || []),
    ...(opp.functionalContacts || []).map((f) => f.sourceUrl).filter(Boolean),
  ].filter(Boolean);

  return {
    input: {
      ...baseInput,
      opportunityName: baseInput.opportunityName || opp.eventName,
      organization: baseInput.organization || opp.organization,
      eventSourceUrls: [...new Set(seedUrls)].slice(0, 12),
      recallV12: {
        primaryGap: gap.primaryGap,
        secondaryGaps: gap.secondaryGaps,
        stages: recallStagesForGapV12(gap.primaryGap),
        directedQueries: queries.slice(0, 16),
        functionalDomains: domains,
      },
    },
    gap,
    queries,
  };
}
