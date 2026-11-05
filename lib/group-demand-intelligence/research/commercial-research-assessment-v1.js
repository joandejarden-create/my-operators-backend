/**
 * Commercial research assessment V1 (run-050).
 *
 * Broader than HOTEL_FIT_GENERIC / MISSING_CONTACT / IDENTITY string checks.
 * Produces prioritized research questions that must drive the next ORS strategy.
 */

import { materialCommercialGaps } from "./commercial-research-completion-contract-v1.js";
import { assessAssociationEventsBuyerCandidate } from "./association-events-buyer-integrity-v1.js";
import { countIndependentSources } from "./source-evidence-dedupe-v1.js";
import { scoreAssociationEventsGoldBrief } from "./association-events-gold-brief-v1.js";

export const COMMERCIAL_RESEARCH_ASSESSMENT_V1 =
  "gdi-commercial-research-assessment-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Assess the full commercial intelligence surface and return ordered gaps
 * with suggested existing ORS / FullE2E strategies.
 */
const MATERIAL_GAP_STRATEGY = Object.freeze({
  HOTEL_FIT_GENERIC_OR_MISSING: {
    question: (hotel) =>
      `Why would ${hotel.displayName || "this hotel"} specifically compete for this activity beyond generic city facilities?`,
    strategy: "PROPERTY_SPECIFIC_FIT_FROM_GEO_AND_PROGRAMME",
  },
  NO_CREDIBLE_CONTACT_ROUTE: {
    question: () =>
      "What official organization / secretariat / PCO outreach route exists for hotel sales?",
    strategy: "OFFICIAL_CONTACT_ROUTE_DISCOVERY",
  },
  NO_SOURCE_EVIDENCE: {
    question: () =>
      "Which primary sources establish the opportunity identity and activity?",
    strategy: "PRIMARY_SOURCE_RECOVERY",
  },
  EVENT_IDENTITY_UNCLEAR: {
    question: () =>
      "What is the authoritative event / edition name distinct from scraped headlines?",
    strategy: "ORGANIZER_OFFICIAL_EVENT_PAGE",
  },
  ORPHAN_CONTACT_ROLE: {
    question: () =>
      "Is there an evidence-backed named person for this role, or should the orphan role be cleared?",
    strategy: "ORGANIZER_CONTACT_OR_FUNCTIONAL_ROUTE",
  },
});

export function assessCommercialResearchState(opportunity = {}, hotel = {}) {
  const gaps = [];
  const { gaps: materialGaps } = materialCommercialGaps(opportunity, hotel);
  for (const g of materialGaps) {
    const mapped = MATERIAL_GAP_STRATEGY[g] || null;
    gaps.push({
      code: g,
      severity: "material",
      source: "completion_contract",
      ...(mapped
        ? {
            question: mapped.question(hotel),
            strategy: mapped.strategy,
          }
        : /UNRESOLVED:/i.test(g)
          ? {
              question: g.replace(/^UNRESOLVED:/i, "").replace(/_/g, " ").toLowerCase() + "?",
              strategy: "EDITION_CHRONOLOGY_AND_FUTURE_CYCLE",
            }
          : {}),
    });
  }

  const title = text(opportunity.eventName || opportunity.title);
  const sourceHeadline = text(opportunity.sourcePageHeadline || opportunity._priorEventNameRaw);
  if (
    /revista|poder y finanzas|speakers\s*[—–-]|llevará su|conmemoraci[oó]n del d[ií]a|el di[aá]logo serio|busca abrir nuevos mercados|- en -/i.test(
      title,
    ) ||
    (sourceHeadline && sourceHeadline === title && /[-–—|].{10,}/.test(title))
  ) {
    gaps.push({
      code: "EVENT_IDENTITY_HEADLINE_CONTAMINATION",
      severity: "critical",
      source: "identity",
      question: "What is the authoritative event / edition name distinct from the source-page headline?",
      strategy: "ORGANIZER_OFFICIAL_EVENT_PAGE",
    });
  }

  if (opportunity.contactName) {
    const a = assessAssociationEventsBuyerCandidate(
      { name: opportunity.contactName, title: opportunity.contactRole },
      {
        organizerName: opportunity.organizationName || "",
        eventBlob: title,
      },
    );
    if (!a.ok) {
      gaps.push({
        code: "CONTACT_RELATIONSHIP_UNSUPPORTED",
        severity: "critical",
        source: "contact",
        question: `Is ${opportunity.contactName} an evidence-backed hotel-sourcing contact for this opportunity, or should the contact be cleared?`,
        strategy: "ORGANIZER_CONTACT_OR_FUNCTIONAL_ROUTE",
      });
    }
  } else if (!text(opportunity.contactUrl || opportunity.functionalContactEmail || opportunity.organizationRouteUrl)) {
    gaps.push({
      code: "NO_CREDIBLE_OUTREACH_ROUTE",
      severity: "material",
      source: "contact",
      question: "What official organizer / secretariat / PCO outreach route exists for hotel sales?",
      strategy: "OFFICIAL_CONTACT_ROUTE_DISCOVERY",
    });
  }

  const chrono = opportunity.associationEventsChronology?.status || opportunity.eventDateStatus;
  if (/PAST|HISTORICAL/i.test(String(chrono || "")) && /upcoming|act now|urgent|próxim/i.test(
    `${opportunity.recommendedAction || ""} ${opportunity.associationEventsGoldBrief?.opportunityOverview || ""}`,
  )) {
    gaps.push({
      code: "CHRONOLOGY_CONTRADICTION",
      severity: "critical",
      source: "chronology",
      question: "Is this a past edition being presented as current demand, or is there a confirmed future edition?",
      strategy: "EDITION_CHRONOLOGY_AND_FUTURE_CYCLE",
    });
  }

  const cib = opportunity.commercialIntelligenceBrief || {};
  const demand = text(
    opportunity.proposedHotelUse ||
      opportunity.hotelDemandThesis ||
      opportunity.associationEventsGoldBrief?.potentialBusinessForHotel ||
      cib.potentialBusinessForHotel ||
      cib.demandMechanism ||
      cib.hotelDemandThesis ||
      "",
  );
  if (!demand || demand.length < 40) {
    gaps.push({
      code: "DEMAND_THESIS_MISSING",
      severity: "material",
      source: "demand",
      question: "What lodging / meeting / catering mechanism could create hotel demand?",
      strategy: "ACCOMMODATION_OR_PROGRAMME_EVIDENCE",
    });
  }

  const fit = text(
    opportunity.hotelFitRationale ||
      opportunity.whyThisHotel ||
      opportunity.hotelOpportunityThesis ||
      opportunity.associationEventsGoldBrief?.whyThisHotel ||
      cib.whyThisHotel ||
      cib.hotelFitRationale ||
      "",
  );
  // Ignore "rather than generic…" phrasing used in improved theses.
  const fitForGenericCheck = fit.replace(/rather than generic[^.?]*/gi, "");
  if (
    !fit ||
    fit.length < 50 ||
    /meeting rooms and|generic hotel|facilities alone are not|generic city co-location/i.test(
      fitForGenericCheck,
    )
  ) {
    gaps.push({
      code: "PROPERTY_FIT_UNSUPPORTED",
      severity: "material",
      source: "fit",
      question: `Why would ${hotel.displayName || "this hotel"} specifically compete for this activity?`,
      strategy: "PROPERTY_SPECIFIC_FIT_FROM_GEO_AND_PROGRAMME",
    });
  }

  const sources = [
    ...(opportunity.sources || []),
    ...(opportunity.evidence || []),
  ];
  const unique = countIndependentSources(sources);
  if (unique === 0) {
    gaps.push({
      code: "NO_INDEPENDENT_SOURCES",
      severity: "critical",
      source: "evidence",
      question: "Which primary sources establish the opportunity identity and activity?",
      strategy: "PRIMARY_SOURCE_RECOVERY",
    });
  }

  if (opportunity.associationEventsGoldBrief) {
    const score = scoreAssociationEventsGoldBrief(opportunity);
    for (const err of score.criticalErrors || []) {
      gaps.push({
        code: `GOLD_CRITICAL:${err}`,
        severity: "critical",
        source: "gold_brief",
        question: `Resolve gold-brief critical error: ${err}`,
        strategy: "GOLD_BRIEF_INTEGRITY_REPAIR",
      });
    }
  }

  // Deduplicate by code; critical first
  const byCode = new Map();
  for (const g of gaps) {
    if (!byCode.has(g.code)) byCode.set(g.code, g);
  }
  const ordered = [...byCode.values()].sort((a, b) => {
    const rank = { critical: 0, material: 1, advisory: 2 };
    return (rank[a.severity] ?? 9) - (rank[b.severity] ?? 9);
  });

  const nextQuestion = ordered.find((g) => g.question) || null;
  return {
    version: COMMERCIAL_RESEARCH_ASSESSMENT_V1,
    gaps: ordered,
    nextResearchQuestion: nextQuestion?.question || null,
    nextStrategy: nextQuestion?.strategy || null,
    criticalCount: ordered.filter((g) => g.severity === "critical").length,
    materialCount: ordered.filter((g) => g.severity === "material").length,
    uniqueSourceCount: unique,
  };
}
