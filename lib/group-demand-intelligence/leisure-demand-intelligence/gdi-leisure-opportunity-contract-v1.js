/**
 * Shared leisure opportunity contract — compatible with GDI canonical fields.
 * No Airtable schema mutation in V1; reuse GDO / research inventory / payload.
 */

import { createHash } from "node:crypto";
import {
  classifyLeisureCommercialPathway,
  assertLeisureSalesReadyNotAutoPromoted,
} from "./gdi-leisure-commercial-pathway-v1.js";
import { getLeisureModule } from "./gdi-leisure-module-registry-v1.js";
import { evaluateLeisureNeedPeriod } from "./gdi-leisure-need-period-v1.js";

export const GDI_LEISURE_OPPORTUNITY_CONTRACT_V1 =
  "gdi-leisure-opportunity-contract-v1";

export const LEISURE_CONTRACT_FIELDS = Object.freeze([
  "hotelId",
  "opportunityId",
  "demandGenerator",
  "organization",
  "demandFamily",
  "leisureSubtype",
  "leisureModuleId",
  "commercialOpportunityType",
  "leisureCommercialPathway",
  "eventOrRecurringDates",
  "geographicRelevance",
  "nonlocalVisitorEvidence",
  "roomNightDemandMechanism",
  "hotelFit",
  "needPeriodRelevance",
  "competitiveCommitments",
  "marketScopeClass",
  "commercialDecisionMakerOrEntryPoint",
  "sourceUrls",
  "verificationStatus",
  "recommendedSalesOrMarketingAction",
  "researchCompleteness",
  "nextResearchStep",
  "segmentAssociations",
  "adpLinkageHint",
]);

function text(v) {
  return String(v == null ? "" : v)
    .replace(/\s+/g, " ")
    .trim();
}

export function stableLeisureOpportunityId({
  hotelId,
  leisureModuleId,
  demandGenerator,
  organization,
  eventKey,
} = {}) {
  const raw = [
    text(hotelId),
    text(leisureModuleId),
    text(demandGenerator || organization),
    text(eventKey || ""),
  ].join("|");
  const hash = createHash("sha256").update(raw.toLowerCase()).digest("hex").slice(0, 12);
  return `gdi_ldi_${hash}`;
}

/**
 * Normalize a leisure candidate onto the shared contract without inventing facts.
 */
export function buildLeisureOpportunityContract(input = {}, opts = {}) {
  const mod = getLeisureModule(input.leisureModuleId || input.moduleId || opts.moduleId);
  const hotelId = text(input.hotelId || opts.hotelId);
  const demandGenerator = text(
    input.demandGenerator || input.programName || input.title || input.name,
  );
  const organization = text(input.organization || input.orgName || input.primaryOrganization);
  const opportunityId =
    text(input.opportunityId || input.id) ||
    stableLeisureOpportunityId({
      hotelId,
      leisureModuleId: mod?.id || "leisure_unknown",
      demandGenerator,
      organization,
      eventKey: input.eventKey || input.startDate || input.dates,
    });

  const pathway = classifyLeisureCommercialPathway({
    ...input,
    leisureSubtype: input.leisureSubtype || mod?.leisureSubtype,
  });

  const needPeriod = evaluateLeisureNeedPeriod(input, opts.needPeriodContext || null);

  const marketScopeClass = resolveMarketScopeClass(input);

  const contract = {
    schema: GDI_LEISURE_OPPORTUNITY_CONTRACT_V1,
    hotelId: hotelId || null,
    opportunityId,
    demandGenerator: demandGenerator || null,
    organization: organization || null,
    demandFamily: text(input.demandFamily) || "leisure",
    leisureSubtype: text(input.leisureSubtype) || mod?.leisureSubtype || null,
    leisureModuleId: mod?.id || text(input.leisureModuleId) || null,
    commercialOpportunityType: pathway.pathway,
    leisureCommercialPathway: pathway.pathway,
    pathwayBasis: pathway.basis,
    eventOrRecurringDates: input.eventOrRecurringDates || input.dates || input.startDate || null,
    geographicRelevance: input.geographicRelevance || input.geoRelevance || null,
    nonlocalVisitorEvidence: input.nonlocalVisitorEvidence || null,
    roomNightDemandMechanism:
      input.roomNightDemandMechanism || input.hotelRevenueMechanism || null,
    hotelFit: input.hotelFit || null,
    needPeriodRelevance: needPeriod,
    competitiveCommitments: input.competitiveCommitments || null,
    marketScopeClass,
    commercialDecisionMakerOrEntryPoint:
      input.commercialDecisionMakerOrEntryPoint ||
      input.primaryContact ||
      input.entryPoint ||
      null,
    sourceUrls: Array.isArray(input.sourceUrls)
      ? input.sourceUrls
      : input.sourceUrl
        ? [input.sourceUrl]
        : [],
    verificationStatus: text(input.verificationStatus) || "UNVERIFIED",
    recommendedSalesOrMarketingAction:
      input.recommendedSalesOrMarketingAction ||
      input.recommendedAction ||
      null,
    researchCompleteness: text(input.researchCompleteness) || "PARTIAL",
    nextResearchStep: input.nextResearchStep || null,
    segmentAssociations: uniqueStrings([
      ...(mod?.primarySegments || []),
      ...(Array.isArray(input.segmentAssociations) ? input.segmentAssociations : []),
      ...(Array.isArray(input.segmentIds) ? input.segmentIds : []),
    ]),
    adpLinkageHint: input.adpLinkageHint || null,
    year:
      text(input.year) ||
      (typeof input.eventOrRecurringDates === "object" &&
      input.eventOrRecurringDates &&
      String(input.eventOrRecurringDates.start || "").match(/\b(20\d{2})\b/)
        ? RegExp.$1
        : null),
    salesReady: false,
    publicationEligible: false,
    paidEnrichmentUsed: false,
  };

  const salesGate = assertLeisureSalesReadyNotAutoPromoted({
    ...contract,
    salesReady: input.salesReady === true,
    salesReadyReason: input.salesReadyReason,
  });
  if (!salesGate.ok) {
    contract.salesReadyBlockedReason = salesGate.error;
  }
  contract.salesReady = false;

  return contract;
}

function resolveMarketScopeClass(input = {}) {
  const explicit = text(input.marketScopeClass || input.marketScope || input.scope);
  if (/^competitive$/i.test(explicit)) return "Competitive";
  if (/^hotel$/i.test(explicit)) return "Hotel";
  if (/^all$/i.test(explicit)) return "All";

  const commitments = input.competitiveCommitments;
  if (
    commitments &&
    (commitments.verifiedCompetitorCommitment === true ||
      commitments.status === "VERIFIED_COMPETITOR")
  ) {
    return "Competitive";
  }
  // Unknown sourcing ≠ competitor-committed
  return "Hotel";
}

function uniqueStrings(arr) {
  const out = [];
  const seen = new Set();
  for (const v of arr) {
    const t = text(v);
    if (!t || seen.has(t)) continue;
    seen.add(t);
    out.push(t);
  }
  return out;
}

/** Schema gap analysis — propose only; do not apply. */
export function proposeLeisureAirtableSchemaGaps() {
  return {
    reuseExisting: [
      "GDO primaryContact* / opportunityPayloadJson",
      "demandFamily / opportunityType / hotelFit fields",
      "Market Scope Hotel|Competitive|All (UI + DTO)",
      "HI Contact Channels for durable emails (future Findymail)",
    ],
    gapsRequiringFounderSchemaGo: [
      {
        field: "leisureModuleId",
        rationale: "Typed select would aid filtering; currently payload-only OK",
        requiredForV1: false,
      },
      {
        field: "leisureCommercialPathway",
        rationale: "DIRECTLY_ADDRESSABLE | PARTNERSHIP_DRIVEN | CAMPAIGN_DRIVEN",
        requiredForV1: false,
      },
      {
        field: "needPeriodRelevanceStatus",
        rationale: "NEED_PERIOD_UNKNOWN | OVERLAP | NO_OVERLAP without inventing ADR",
        requiredForV1: false,
      },
    ],
    airtableWritesThisPhase: "NONE",
  };
}
