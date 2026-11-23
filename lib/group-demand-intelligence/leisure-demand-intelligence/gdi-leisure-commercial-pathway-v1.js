/**
 * Leisure commercial pathways — independent evidence gates.
 * Campaign opportunities must NOT be forced into group-buyer contact rules
 * and must NOT become Sales Ready merely because an event exists.
 */

import { LEISURE_COMMERCIAL_PATHWAY } from "./gdi-leisure-module-registry-v1.js";

export const GDI_LEISURE_COMMERCIAL_PATHWAY_V1 =
  "gdi-leisure-commercial-pathway-v1";

export { LEISURE_COMMERCIAL_PATHWAY };

const PATHWAY_RULES = Object.freeze({
  [LEISURE_COMMERCIAL_PATHWAY.DIRECTLY_ADDRESSABLE]: Object.freeze({
    requiresNamedOrOrgBuyerPath: true,
    requiresPersonalBuyer: false,
    allowsSalesReadyCandidate: true,
    salesReadyRequires: Object.freeze([
      "commercial_decision_maker_or_entry_point",
      "room_night_demand_mechanism",
      "hotel_fit_evidence",
      "independent_of_email_alone",
    ]),
    contactMode: "named_buyer_or_org_entry",
  }),
  [LEISURE_COMMERCIAL_PATHWAY.PARTNERSHIP_DRIVEN]: Object.freeze({
    requiresNamedOrOrgBuyerPath: true,
    requiresPersonalBuyer: false,
    allowsSalesReadyCandidate: true,
    salesReadyRequires: Object.freeze([
      "partnership_or_referral_decision_maker",
      "referral_or_distribution_mechanism",
      "hotel_fit_evidence",
    ]),
    contactMode: "partnership_bd_or_distribution_owner",
  }),
  [LEISURE_COMMERCIAL_PATHWAY.CAMPAIGN_DRIVEN]: Object.freeze({
    requiresNamedOrOrgBuyerPath: false,
    requiresPersonalBuyer: false,
    allowsSalesReadyCandidate: false,
    salesReadyRequires: Object.freeze([]),
    contactMode: "org_or_channel_optional",
    note: "Campaign ≠ Sales Ready. Event existence alone is insufficient.",
  }),
});

export function getLeisurePathwayRules(pathway) {
  return PATHWAY_RULES[pathway] || null;
}

/**
 * Classify pathway from opportunity evidence (no invention).
 */
export function classifyLeisureCommercialPathway(opportunity = {}) {
  const explicit = String(
    opportunity.leisureCommercialPathway ||
      opportunity.commercialPathway ||
      opportunity.commercialOpportunityType ||
      "",
  )
    .trim()
    .toUpperCase();
  if (PATHWAY_RULES[explicit]) {
    return {
      pathway: explicit,
      basis: "explicit_field",
      rules: PATHWAY_RULES[explicit],
    };
  }

  const blob = JSON.stringify(opportunity).toLowerCase();
  const hasBuyer =
    Boolean(opportunity.primaryContact?.name) ||
    Boolean(opportunity.commercialDecisionMaker) ||
    Boolean(opportunity.entryPoint) ||
    /decision.?maker|housing.?bureau|tournament.?director|group.?sales/i.test(
      blob,
    );
  const partnership =
    /dmc|referral|partnership|distribution|preferred.?lodging|ota|tour.?operator|experience.?provider/i.test(
      blob,
    ) || opportunity.leisureSubtype === "leisure_distribution_referral";
  const campaignSignals =
    /campaign|marketing|revenue.?management|shoulder|vfr|destination.?demand|special.?occasion/i.test(
      blob,
    ) || opportunity.leisureSubtype === "destination_demand_special_occasions";

  if (hasBuyer && !campaignSignals) {
    return {
      pathway: LEISURE_COMMERCIAL_PATHWAY.DIRECTLY_ADDRESSABLE,
      basis: "buyer_or_entry_evidence",
      rules: PATHWAY_RULES[LEISURE_COMMERCIAL_PATHWAY.DIRECTLY_ADDRESSABLE],
    };
  }
  if (partnership) {
    return {
      pathway: LEISURE_COMMERCIAL_PATHWAY.PARTNERSHIP_DRIVEN,
      basis: "partnership_or_distribution_evidence",
      rules: PATHWAY_RULES[LEISURE_COMMERCIAL_PATHWAY.PARTNERSHIP_DRIVEN],
    };
  }
  if (campaignSignals) {
    return {
      pathway: LEISURE_COMMERCIAL_PATHWAY.CAMPAIGN_DRIVEN,
      basis: "campaign_or_destination_demand_evidence",
      rules: PATHWAY_RULES[LEISURE_COMMERCIAL_PATHWAY.CAMPAIGN_DRIVEN],
    };
  }

  return {
    pathway: LEISURE_COMMERCIAL_PATHWAY.CAMPAIGN_DRIVEN,
    basis: "default_conservative_campaign",
    rules: PATHWAY_RULES[LEISURE_COMMERCIAL_PATHWAY.CAMPAIGN_DRIVEN],
  };
}

/**
 * Sales Ready must remain independent — never promote from pathway or event alone.
 */
export function assertLeisureSalesReadyNotAutoPromoted(opportunity = {}) {
  const pathway = classifyLeisureCommercialPathway(opportunity);
  const salesReady =
    opportunity.salesReady === true ||
    opportunity.commercialDisposition === "SALES_READY" ||
    opportunity.eligibilityStatus === "SALES_READY";

  if (!salesReady) {
    return { ok: true, salesReady: false, pathway: pathway.pathway };
  }

  if (pathway.pathway === LEISURE_COMMERCIAL_PATHWAY.CAMPAIGN_DRIVEN) {
    return {
      ok: false,
      error: "CAMPAIGN_CANNOT_BE_SALES_READY",
      pathway: pathway.pathway,
      salesReady: true,
    };
  }

  if (opportunity.salesReadyReason === "event_exists") {
    return {
      ok: false,
      error: "EVENT_EXISTENCE_IS_NOT_SALES_READY",
      pathway: pathway.pathway,
      salesReady: true,
    };
  }

  return { ok: true, salesReady: true, pathway: pathway.pathway };
}
