/**
 * Five discovery spines — all converge to the same canonical opportunity packet.
 * Participant-first remains the unchanged YOTEL-era path (orchestrated elsewhere).
 */

import { DISCOVERY_SPINE, SPINE_BASE_MAP, CONTROLLER_AUTHORITY } from "./constants.js";
import {
  buildDemandController,
  classifyControllerAuthorityFromEvidence,
} from "./demand-controller-model.js";
import {
  buildControllerDiscoveryQueries,
  inferControllerTypeFromText,
} from "./multilingual-controller-ontology.js";
import { evaluateEquivalentEvidence, CANONICAL_FACTS } from "./equivalent-evidence-policy.js";
import { EVIDENCE_ROUTE } from "./constants.js";

export function describeDiscoverySpines() {
  return Object.values(DISCOVERY_SPINE).map((spine) => ({
    spine,
    basesOfDemand: SPINE_BASE_MAP[spine] || [],
    convergesToSamePacket: true,
  }));
}

/**
 * PATH B — Demand Controller First (offline resolution from campaign + snippets).
 * Does not invent lodging authority from generic organizer status.
 */
export function runDemandControllerFirstSpine({
  campaign = {},
  snippets = [],
  marketProfile = {},
  languages = null,
} = {}) {
  const langs = languages || marketProfile.primaryLanguages || ["en"];
  const eventName = campaign.campaignName || campaign.eventName || campaign.title || "";
  const queries = buildControllerDiscoveryQueries({
    eventName,
    market: marketProfile.market || campaign.market,
    country: marketProfile.country || campaign.country,
    languages: langs,
  });

  const candidates = [];
  const blobSources = [
    ...(Array.isArray(snippets) ? snippets : []),
    {
      text: [campaign.organizerName, campaign.pcoName, campaign.housingProvider, campaign.secretariat]
        .filter(Boolean)
        .join(" | "),
      url: campaign.sourceUrl || campaign.officialUrl || null,
      evidenceType: campaign.housingProvider ? "OFFICIAL_HOUSING_PAGE" : "ORGANIZER_SHELL",
    },
  ];

  for (const snip of blobSources) {
    const text = String(snip.text || snip.snippet || snip.title || "");
    if (!text.trim()) continue;
    const type = inferControllerTypeFromText(text) || inferControllerTypeFromText(snip.evidenceType || "");
    const nameMatch =
      text.match(
        /(?:PCO|DMC|Secretar[ií]a(?:\s+T[eé]cnica)?|Kongressorganisation|organized by|organizado por|Veranstalter)[:\s]+([A-ZÁÉÍÓÚÜÑ][\wÁÉÍÓÚÜÑáéíóúüñ &.'-]{2,60})/i
      ) ||
      (campaign.organizerName ? [null, campaign.organizerName] : null) ||
      (campaign.pcoName ? [null, campaign.pcoName] : null);

    const controllerName = String(
      snip.controllerName || nameMatch?.[1] || campaign.housingProvider || ""
    ).trim();
    if (!controllerName || controllerName.length < 3) continue;

    // Reject pure venue / hotel as controller
    if (/hotel|marriott|westin|radisson|hilton|ac hotel|yotel/i.test(controllerName) && type === "UNKNOWN") {
      continue;
    }

    const authority = classifyControllerAuthorityFromEvidence({
      evidenceType: snip.evidenceType || "",
      evidenceText: text,
      controllerType: type,
    });

    // Generic organizer shell without lodging cues → CONTACT_ONLY max already in classifier
    if (
      authority === CONTROLLER_AUTHORITY.UNCONFIRMED &&
      !campaign.housingProvider &&
      !/hous|accom|hotel|aloj|unter|pco|dmc|secretar/i.test(text)
    ) {
      continue;
    }

    const built = buildDemandController({
      controllerName,
      controllerType: type,
      selectionAuthority: authority,
      lodgingAuthority: authority,
      campaignId: campaign.campaignId || campaign.id,
      market: marketProfile.market || campaign.market,
      country: marketProfile.country || campaign.country,
      evidenceSource: snip.url || campaign.sourceUrl,
      evidenceType: snip.evidenceType || "PUBLIC_SNIPPET",
      evidenceText: text.slice(0, 400),
      publicContactPath: snip.contactUrl || campaign.contactUrl || null,
      provenance: "PUBLIC_WEB",
    });
    if (!built.ok) continue;

    const eq = evaluateEquivalentEvidence({
      fact: CANONICAL_FACTS.BUYER_OR_CONTROLLER_PATH,
      route: EVIDENCE_ROUTE.PCO_DMC,
      form: "pco_contact",
      evidence: {
        campaignId: campaign.campaignId || campaign.id,
        campaignName: eventName,
        sourceUrl: built.controller.evidenceSource,
      },
    });

    candidates.push({
      discoverySpine: DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
      baseOfDemand: (SPINE_BASE_MAP[DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST] || [])[0],
      demandController: built.controller,
      equivalentEvidenceOk: eq.ok,
      equivalentEvidenceReason: eq.reason,
      historicalVsCurrent: "CURRENT",
      evidenceRoute: EVIDENCE_ROUTE.PCO_DMC,
    });
  }

  // Dedupe by controller id
  const byId = new Map();
  for (const c of candidates) {
    const id = c.demandController.demandControllerId;
    if (!byId.has(id)) byId.set(id, c);
  }

  return {
    spine: DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
    queries,
    controllers: [...byId.values()],
    packetHints: [...byId.values()].map((c) => ({
      discoverySpine: c.discoverySpine,
      demandControllerId: c.demandController.demandControllerId,
      buyerOrControllerResolved: c.equivalentEvidenceOk,
      lodgingAuthority: c.demandController.lodgingAuthority,
    })),
  };
}

/**
 * PATH C — Account First (trigger-based; requires evidence, no fabrication).
 */
export const ACCOUNT_FIRST_TRIGGERS = Object.freeze([
  "office_opening",
  "market_entry",
  "regional_expansion",
  "product_launch",
  "training_rollout",
  "sales_meeting",
  "annual_meeting",
  "board_meeting",
  "investor_meeting",
  "conference_participation",
  "trade_fair_participation",
  "project_deployment",
  "construction_infrastructure",
  "medical_trial_research",
  "university_exchange",
  "sports_competition",
  "production_shoot",
  "government_mission",
  "association_chapter_meeting",
]);

export function runAccountFirstSpine({ accounts = [], triggers = [], marketProfile = {} } = {}) {
  const out = [];
  for (const acc of accounts) {
    const name = String(acc.organizationName || acc.name || "").trim();
    if (!name) continue;
    const trigger = String(acc.trigger || acc.triggerFamily || "").toLowerCase();
    const triggerOk =
      ACCOUNT_FIRST_TRIGGERS.includes(trigger) ||
      triggers.some((t) => String(t).toLowerCase() === trigger);
    const evidenceUrl = acc.evidenceUrl || acc.sourceUrl;
    if (!triggerOk || !evidenceUrl) {
      out.push({
        discoverySpine: DISCOVERY_SPINE.ACCOUNT_FIRST,
        rejected: true,
        reason: !triggerOk ? "trigger_unsupported_or_missing" : "missing_evidence_url",
        organizationName: name,
      });
      continue;
    }
    // Do not fabricate hierarchy
    const graph = {
      parentCompany: acc.parentCompany || null,
      regionalOffice: acc.regionalOffice || null,
      subsidiary: acc.subsidiary || null,
      localBranch: acc.localBranch || null,
      businessUnit: acc.businessUnit || null,
      associationChapter: acc.associationChapter || null,
      agencyRelationship: acc.agencyRelationship || null,
      fabricated: false,
    };
    out.push({
      discoverySpine: DISCOVERY_SPINE.ACCOUNT_FIRST,
      baseOfDemand: (SPINE_BASE_MAP[DISCOVERY_SPINE.ACCOUNT_FIRST] || [])[0],
      rejected: false,
      organizationName: name,
      triggerFamily: trigger,
      market: marketProfile.market || acc.market,
      country: marketProfile.country || acc.country,
      evidenceUrl,
      relationshipGraph: graph,
      historicalVsCurrent: "CURRENT",
      evidenceRoute: EVIDENCE_ROUTE.OFFICIAL_PUBLIC,
      lookalikeConfirmedDemand: false,
    });
  }
  return { spine: DISCOVERY_SPINE.ACCOUNT_FIRST, accounts: out };
}

/**
 * PATH D — Historical Process First.
 * History establishes process pattern only — never current placement.
 */
export function runHistoricalProcessFirstSpine({ priorCycles = [], campaign = {} } = {}) {
  const patterns = [];
  for (const cycle of priorCycles) {
    patterns.push({
      discoverySpine: DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST,
      baseOfDemand: (SPINE_BASE_MAP[DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST] || [])[0],
      priorCycleId: cycle.cycleId || cycle.id || null,
      priorYear: cycle.year || null,
      historicalOrganizer: cycle.organizer || null,
      historicalPco: cycle.pco || null,
      historicalHousingProvider: cycle.housingProvider || null,
      historicalOfficialHotel: cycle.officialHotel || null,
      housingModel: cycle.housingModel || null,
      participantTypes: cycle.participantTypes || [],
      typicalDecisionTiming: cycle.decisionTiming || null,
      historicalVsCurrent: "HISTORICAL",
      canEstablishCurrentHotelSelection: false,
      canEstablishCurrentParticipation: false,
      canEstablishProcessPattern: true,
      canEstablishTypicalController: Boolean(cycle.pco || cycle.housingProvider),
      nextCycleHint: campaign.campaignId || campaign.eventName || null,
      evidenceRoute: EVIDENCE_ROUTE.HISTORICAL_PROCESS,
    });
  }
  return { spine: DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST, patterns };
}

/**
 * PATH E — Hotel History Lookalike (requires HOTEL_SUPPLIED provenance).
 */
export function runHotelHistoryFirstSpine({ hotelSupplied = [], hotelId } = {}) {
  const rows = [];
  for (const item of hotelSupplied) {
    if (String(item.provenance || "") !== "HOTEL_SUPPLIED_EVIDENCE" && item.provenance !== "HOTEL_SUPPLIED") {
      rows.push({
        discoverySpine: DISCOVERY_SPINE.HOTEL_HISTORY_FIRST,
        rejected: true,
        reason: "missing_hotel_supplied_provenance",
      });
      continue;
    }
    rows.push({
      discoverySpine: DISCOVERY_SPINE.HOTEL_HISTORY_FIRST,
      baseOfDemand: (SPINE_BASE_MAP[DISCOVERY_SPINE.HOTEL_HISTORY_FIRST] || [])[0],
      rejected: false,
      hotelId,
      knownAccount: item.accountName || item.organizationName || null,
      evidenceType: item.evidenceType || item.type || "OTHER",
      evidenceText: item.evidenceText || item.text || null,
      suppliedBy: item.suppliedBy || item.whoSupplied || null,
      timestamp: item.timestamp || item.at || null,
      lookalikeConfirmedDemand: false,
      historicalVsCurrent: "HOTEL_SUPPLIED_HISTORICAL_OR_CURRENT",
      evidenceRoute: EVIDENCE_ROUTE.HOTEL_SUPPLIED,
      provenance: "HOTEL_SUPPLIED_EVIDENCE",
      inferredFutureTrigger: item.inferredFutureTrigger || null,
      note: "Inferred lookalike is NOT confirmed demand",
    });
  }
  return { spine: DISCOVERY_SPINE.HOTEL_HISTORY_FIRST, rows };
}

/** PATH A — Participant First: delegate to existing YOTEL-era pipeline (marker only). */
export function participantFirstSpineContract() {
  return {
    spine: DISCOVERY_SPINE.PARTICIPANT_FIRST,
    implementedBy: "existing_yotel_era_decomposition",
    unchanged: true,
    flow: [
      "campaign",
      "official_list",
      "named_participant_account",
      "traveling_entity",
      "buyer",
      "lodging",
      "future_decision",
      "hotel_fit",
      "packet",
    ],
    basesOfDemand: SPINE_BASE_MAP[DISCOVERY_SPINE.PARTICIPANT_FIRST],
  };
}
