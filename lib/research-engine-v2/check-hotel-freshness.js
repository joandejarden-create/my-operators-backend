/**
 * Contradiction-first hotel freshness checker V1.1.
 * Match + geo + brand-contamination + corroboration gates.
 * Proposed corrections only — no Airtable writes.
 */

import { resolveBrandFamily, defaultParentForFamily, canonicalizeObservedBrand } from "./brand-family.js";
import {
  createClaim,
  createProposedCorrection,
  normalizeOperatingStatus,
} from "./claim-model.js";
import { generateResearchQueries } from "./query-generator.js";
import { assessEntityMatch, brandLabelsAlign, MATCH_GATE_CONFIG_V1_1 } from "./match-confidence.js";
import {
  assessStatusCorroboration,
  assessReflagCorroboration,
  applyTemporalStatusResolution,
  gateCorrection,
  CORROBORATION_CONFIG_V1_1,
} from "./corroboration.js";
import { fetchIhgHotelObservation, loadIhgDirectoryRows } from "./adapters/ihg.js";
import {
  fetchMarriottHotelObservation,
  loadMarriottTributeDirectoryRows,
  loadMarriottSoftBrandDirectoryRows,
} from "./adapters/marriott.js";
import { fetchChoiceHotelObservation } from "./adapters/choice.js";
import { fetchGenericOfficialObservation } from "./adapters/generic.js";
import {
  computeDirectoryGaps,
  computeChoiceIndividualsGaps,
  computeMarriottSoftBrandGaps,
} from "./directory-gaps.js";
import { isSourceUnsafeForProposals, classifySourceState } from "./source-state.js";

export const ENGINE_VERSION = "contradiction-first-v1.1";
export const OPS_ENGINE_VERSION = "shadow-operations-v1";

/**
 * @param {object} input
 * @param {object} [opts]
 */
export async function checkHotelFreshness(input, opts = {}) {
  const hotel = {
    hotelId: input.hotelId || input.recordId || input.id || "",
    name: input.name || input.hotelName || "",
    city: input.city || "",
    country: input.country || "",
    currentBrand: input.currentBrand || input.affiliation || input.brand || "",
    currentStatus: normalizeOperatingStatus(input.currentStatus || input.status),
    currentOperator: input.currentOperator || input.managementCompany || "",
    currentParent: input.currentParent || input.parentCompany || "",
    website: input.website || "",
    propertyId: input.propertyId || input.brandPropertyCode || "",
    marsha: input.marsha || "",
    brandFamily: input.brandFamily || resolveBrandFamily(input),
    geography: input.geography || [input.city, input.country].filter(Boolean).join(", "),
    dealalityLastVerified: input.dealalityLastVerified || null,
  };

  const brandFamily = hotel.brandFamily || resolveBrandFamily(hotel);
  const queries = generateResearchQueries(hotel, { brandFamily });

  /** @type {object} */
  let observation;
  if (brandFamily === "ihg") {
    observation = await fetchIhgHotelObservation(hotel, {
      directoryRows: opts.ihgDirectoryRows,
      fetchDelayMs: opts.fetchDelayMs,
      website: hotel.website,
    });
  } else if (brandFamily === "marriott") {
    observation = await fetchMarriottHotelObservation(hotel, {
      directoryRows: opts.marriottDirectoryRows,
      fetchDelayMs: opts.fetchDelayMs,
      website: hotel.website,
    });
  } else if (brandFamily === "choice") {
    observation = await fetchChoiceHotelObservation(hotel, {
      directoryRows: opts.choiceDirectoryRows,
      fetchDelayMs: opts.fetchDelayMs,
      website: hotel.website,
    });
  } else if (brandFamily === "hilton" || /hilton/i.test(`${hotel.currentParent} ${hotel.currentBrand}`)) {
    const { fetchHiltonHotelObservation } = await import("./adapters/hilton.js");
    observation = await fetchHiltonHotelObservation(hotel, {
      fetchDelayMs: opts.fetchDelayMs,
      ctyhocn: hotel.propertyId || hotel.ctyhocn,
    });
  } else {
    observation = await fetchGenericOfficialObservation(hotel, {
      fetchDelayMs: opts.fetchDelayMs,
      website: hotel.website,
    });
  }

  const entityMatch =
    observation.rawSignals?.entityMatch ||
    assessEntityMatch(hotel, {
      name: observation.officialHotelName,
      brand: observation.brand,
      country: observation.country,
      city: observation.city,
      officialUrl: observation.officialUrl,
      propertyId: observation.rawSignals?.propertyId || observation.rawSignals?.marsha,
      marsha: observation.rawSignals?.marsha,
    });

  const claims = buildClaimsFromObservation(hotel, observation, queries, entityMatch);
  let { proposedCorrections, reviewQueue, researchHistory } = buildGatedCorrections(
    hotel,
    claims,
    observation,
    entityMatch
  );

  // Source failure must NOT create change proposals
  const sourceState =
    observation.sourceState ||
    classifySourceState({
      status: observation.rawSignals?.httpStatus,
      hotelFound: observation.hotelFound,
      error: observation.notes,
    }).state;

  if (isSourceUnsafeForProposals(sourceState)) {
    const blockedNote = `Source ${sourceState} — no material proposal (not closed/reflagged/discontinued)`;
    researchHistory = [
      ...proposedCorrections.map((c) => ({
        ...c,
        recommended_action: "Insufficient Evidence",
        reason: blockedNote,
        sourceState,
      })),
      ...reviewQueue.map((c) => ({
        ...c,
        recommended_action: "Insufficient Evidence",
        reason: blockedNote,
        sourceState,
      })),
      ...researchHistory,
    ];
    proposedCorrections = [];
    reviewQueue = [];
  }

  return {
    engineVersion: ENGINE_VERSION,
    hotel,
    brandFamily,
    observation: { ...observation, sourceState },
    sourceState,
    entityMatch,
    queries,
    claims,
    proposedCorrections,
    reviewQueue,
    researchHistory,
    checkedAt: new Date().toISOString(),
  };
}

function buildClaimsFromObservation(hotel, observation, queries, entityMatch) {
  const now = observation.evidenceTimestamp || new Date().toISOString();
  /** @type {object[]} */
  const claims = [];

  claims.push(
    createClaim({
      claimType: "HOTEL_EXISTS",
      hotelId: hotel.hotelId,
      hotelName: hotel.name,
      currentDealalityValue: true,
      independentlyObservedValue: observation.hotelFound,
      claimStatus: observation.hotelFound
        ? "Confirmed"
        : entityMatch.level === "Medium"
          ? "Unverified"
          : "Unknown",
      evidenceSource: observation.officialUrl,
      sourceType: observation.sourceType,
      evidenceRetrievalDate: now,
      confidence: observation.confidence,
      contradictionFound: false,
      supportQueries: queries.supportQueries,
      contradictionQueries: queries.contradictionQueries,
      notes: `entityMatch=${entityMatch.level}; ${observation.notes || ""}`,
    })
  );

  const observedBrand = canonicalizeObservedBrand(observation.brand || "");
  const currentBrand = canonicalizeObservedBrand(hotel.currentBrand || "");
  const brandRel = brandLabelsAlign(currentBrand, observedBrand);
  const brandMismatch = brandRel === "conflict" && Boolean(observedBrand && currentBrand);

  claims.push(
    createClaim({
      claimType: "CURRENT_BRAND",
      hotelId: hotel.hotelId,
      hotelName: hotel.name,
      currentDealalityValue: currentBrand || null,
      independentlyObservedValue: observedBrand || null,
      claimStatus: !observedBrand ? "Unknown" : brandMismatch ? "Contradicted" : "Confirmed",
      evidenceSource: observation.officialUrl,
      sourceType: observation.sourceType,
      evidenceRetrievalDate: now,
      confidence: observation.confidence,
      contradictionFound: brandMismatch,
      proposedCorrection: brandMismatch ? observedBrand : null,
      notes: brandMismatch
        ? `Property-level brand conflict (entityMatch=${entityMatch.level})`
        : "Brand compatible or unobserved",
      supportQueries: queries.supportQueries,
      contradictionQueries: queries.contradictionQueries,
    })
  );

  const observedParent = observation.parent || defaultParentForFamily(hotel.brandFamily);
  const parentMismatch =
    Boolean(observedParent && hotel.currentParent) &&
    !parentsRoughlyAlign(hotel.currentParent, observedParent);

  claims.push(
    createClaim({
      claimType: "CURRENT_PARENT",
      hotelId: hotel.hotelId,
      hotelName: hotel.name,
      currentDealalityValue: hotel.currentParent || null,
      independentlyObservedValue: observedParent || null,
      claimStatus: !observedParent
        ? "Unknown"
        : parentMismatch
          ? "Contradicted"
          : hotel.currentParent
            ? "Confirmed"
            : "Unverified",
      evidenceSource: observation.officialUrl,
      sourceType: observation.sourceType,
      evidenceRetrievalDate: now,
      confidence: observation.confidence,
      contradictionFound: parentMismatch,
      proposedCorrection: parentMismatch ? observedParent : null,
      supportQueries: queries.supportQueries,
      contradictionQueries: queries.contradictionQueries,
    })
  );

  const currentStatus = normalizeOperatingStatus(hotel.currentStatus);
  const observedStatus = normalizeOperatingStatus(observation.operatingStatus);
  const temporal = applyTemporalStatusResolution(
    { ...hotel, currentStatus },
    { ...observation, operatingStatus: observedStatus }
  );

  let statusClaimStatus = "Unknown";
  let contradictionFound = false;
  let proposed = null;
  let notes = temporal.reason || "";

  if (!observedStatus || !observation.hotelFound) {
    statusClaimStatus = "Unverified";
  } else if (!currentStatus) {
    statusClaimStatus = "Unverified";
    proposed = observedStatus;
  } else if (currentStatus === observedStatus) {
    statusClaimStatus = "Confirmed";
  } else {
    statusClaimStatus = temporal.claimStatus || "Contradicted";
    contradictionFound = true;
    proposed = temporal.winningValue || observedStatus;
    notes = temporal.reason || notes;
  }

  claims.push(
    createClaim({
      claimType: "OPERATING_STATUS",
      hotelId: hotel.hotelId,
      hotelName: hotel.name,
      currentDealalityValue: currentStatus || null,
      independentlyObservedValue: observedStatus || null,
      claimStatus: statusClaimStatus,
      evidenceSource: observation.officialUrl,
      sourceType: observation.sourceType,
      sourceDate: observation.sourceDate || null,
      evidenceRetrievalDate: now,
      dealalityLastVerified: hotel.dealalityLastVerified || null,
      confidence: observation.confidence,
      contradictionFound,
      proposedCorrection: proposed,
      notes: `${notes}; entityMatch=${entityMatch.level}; dates=${JSON.stringify(temporal.dates || {})}`,
      supportQueries: queries.supportQueries,
      contradictionQueries: queries.contradictionQueries,
    })
  );

  claims.push(
    createClaim({
      claimType: "PIPELINE_STATUS",
      hotelId: hotel.hotelId,
      hotelName: hotel.name,
      currentDealalityValue:
        currentStatus === "Pipeline" ? "Pipeline" : currentStatus === "Open" ? "Not Pipeline" : null,
      independentlyObservedValue:
        observedStatus === "Pipeline" ? "Pipeline" : observedStatus === "Open" ? "Not Pipeline" : null,
      claimStatus: statusClaimStatus,
      evidenceSource: observation.officialUrl,
      sourceType: observation.sourceType,
      evidenceRetrievalDate: now,
      confidence: observation.confidence,
      contradictionFound,
      proposedCorrection: proposed,
      notes,
      supportQueries: queries.supportQueries,
      contradictionQueries: queries.contradictionQueries,
    })
  );

  claims.push(
    createClaim({
      claimType: "OPENING_STATUS",
      hotelId: hotel.hotelId,
      hotelName: hotel.name,
      currentDealalityValue: currentStatus || null,
      independentlyObservedValue: observation.rawSignals?.newHotelBanner
        ? "Recently opened / New Hotel banner"
        : observedStatus,
      claimStatus:
        observation.rawSignals?.newHotelBanner && currentStatus === "Pipeline"
          ? "Superseded"
          : statusClaimStatus,
      evidenceSource: observation.officialUrl,
      sourceType: observation.sourceType,
      evidenceRetrievalDate: now,
      confidence: observation.confidence,
      contradictionFound: Boolean(observation.rawSignals?.newHotelBanner && currentStatus === "Pipeline"),
      proposedCorrection:
        observation.rawSignals?.newHotelBanner && currentStatus === "Pipeline" ? "Open" : proposed,
      notes: observation.rawSignals?.newHotelBanner ? "IHG New Hotel banner observed" : "",
      supportQueries: queries.supportQueries,
      contradictionQueries: queries.contradictionQueries,
    })
  );

  claims.push(
    createClaim({
      claimType: "REFLAG_STATUS",
      hotelId: hotel.hotelId,
      hotelName: hotel.name,
      currentDealalityValue: currentBrand || null,
      independentlyObservedValue: observedBrand || null,
      claimStatus: brandMismatch ? "Contradicted" : observedBrand ? "Confirmed" : "Unknown",
      evidenceSource: observation.officialUrl,
      sourceType: observation.sourceType,
      evidenceRetrievalDate: now,
      confidence: observation.confidence,
      contradictionFound: brandMismatch,
      proposedCorrection: brandMismatch ? observedBrand : null,
      notes: brandMismatch
        ? `Possible reflag — requires property-level corroboration (entityMatch=${entityMatch.level})`
        : "No reflag signal",
      supportQueries: queries.supportQueries,
      contradictionQueries: queries.contradictionQueries,
    })
  );

  claims.push(
    createClaim({
      claimType: "CURRENT_OPERATOR",
      hotelId: hotel.hotelId,
      hotelName: hotel.name,
      currentDealalityValue: hotel.currentOperator || null,
      independentlyObservedValue: null,
      claimStatus: "Unverified",
      evidenceSource: null,
      sourceType: null,
      evidenceRetrievalDate: now,
      confidence: 0,
      contradictionFound: false,
      notes: "Operator not inferred from brand",
      supportQueries: queries.supportQueries.filter((q) => /operator|managed/i.test(q)),
      contradictionQueries: queries.contradictionQueries.filter((q) => /operator/i.test(q)),
    })
  );

  return claims;
}

function buildGatedCorrections(hotel, claims, observation, entityMatch) {
  /** @type {object[]} */
  const proposedCorrections = [];
  /** @type {object[]} */
  const reviewQueue = [];
  /** @type {object[]} */
  const researchHistory = [];

  const statusClaim = claims.find((c) => c.claimType === "OPERATING_STATUS");
  if (statusClaim?.contradictionFound && statusClaim.proposedCorrection) {
    const statusCorr = assessStatusCorroboration(observation, entityMatch, {
      currentStatus: String(statusClaim.currentDealalityValue || ""),
      observedStatus: String(statusClaim.independentlyObservedValue || ""),
    });
    const gated = gateCorrection({ match: entityMatch, corroboration: statusCorr, field: "status" });
    const payload = createProposedCorrection({
      hotel_id: hotel.hotelId,
      hotel_name: hotel.name,
      field: "status",
      current_value: statusClaim.currentDealalityValue,
      observed_value: statusClaim.proposedCorrection,
      classification: statusClaim.claimStatus,
      evidence: evidencePack(observation, entityMatch, statusCorr),
      confidence: observation.confidence,
      reason: `${statusCorr.reason}; match=${entityMatch.level}`,
      recommended_action: gated.recommended_action,
    });
    payload.confidenceBand = gated.confidenceBand;
    payload.queue = gated.queue;
    payload.entityMatchLevel = entityMatch.level;
    payload.corroboration = statusCorr.corroboration;
    payload.engineVersion = ENGINE_VERSION;

    if (gated.queue === "proposed_high" || gated.queue === "proposed_medium") {
      proposedCorrections.push(payload);
    } else if (gated.queue === "review") {
      reviewQueue.push(payload);
    } else {
      researchHistory.push(payload);
    }
  }

  const brandClaim = claims.find((c) => c.claimType === "CURRENT_BRAND");
  if (brandClaim?.contradictionFound && brandClaim.proposedCorrection) {
    const reflagCorr = assessReflagCorroboration(hotel, observation, entityMatch);
    const gated = gateCorrection({ match: entityMatch, corroboration: reflagCorr, field: "Affiliation" });
    const payload = createProposedCorrection({
      hotel_id: hotel.hotelId,
      hotel_name: hotel.name,
      field: "Affiliation",
      current_value: reflagCorr.currentBrand || brandClaim.currentDealalityValue,
      observed_value: reflagCorr.observedBrand || brandClaim.proposedCorrection,
      classification: brandClaim.claimStatus,
      evidence: evidencePack(observation, entityMatch, reflagCorr),
      confidence: observation.confidence,
      reason: `${reflagCorr.reason}; brandConfidence=${reflagCorr.brandConfidence}`,
      recommended_action: gated.recommended_action,
    });
    payload.confidenceBand = gated.confidenceBand;
    payload.queue = gated.queue;
    payload.entityMatchLevel = entityMatch.level;
    payload.brandConfidence = reflagCorr.brandConfidence;
    payload.engineVersion = ENGINE_VERSION;

    if (gated.queue === "proposed_high" || gated.queue === "proposed_medium") {
      proposedCorrections.push(payload);
    } else if (gated.queue === "review") {
      reviewQueue.push(payload);
    } else {
      researchHistory.push(payload);
    }
  }

  if (!proposedCorrections.length && !reviewQueue.length) {
    proposedCorrections.push(
      createProposedCorrection({
        hotel_id: hotel.hotelId,
        hotel_name: hotel.name,
        field: "status",
        current_value: hotel.currentStatus,
        observed_value: observation.operatingStatus,
        classification: statusClaim?.claimStatus || "Unknown",
        evidence: observation.officialUrl
          ? [
              {
                url: observation.officialUrl,
                sourceType: observation.sourceType,
                retrievedAt: observation.evidenceTimestamp,
                adapter: observation.adapter,
              },
            ]
          : [],
        confidence: observation.confidence,
        reason:
          observation.hotelFound && entityMatch.allowMaterialCorrection
            ? "Official evidence aligns or gates blocked material change"
            : "Insufficient evidence or match gate blocked material correction",
        recommended_action:
          observation.hotelFound && !statusClaim?.contradictionFound ? "No Change" : "Insufficient Evidence",
      })
    );
  }

  return { proposedCorrections, reviewQueue, researchHistory };
}

function evidencePack(observation, entityMatch, corr) {
  return [
    {
      url: observation.officialUrl,
      sourceType: observation.sourceType,
      retrievedAt: observation.evidenceTimestamp,
      sourceDate: observation.sourceDate || null,
      adapter: observation.adapter,
      entityMatchLevel: entityMatch.level,
      entityMatchReasons: entityMatch.reasons,
      corroborationTier: corr?.tier || null,
      brand: observation.brand,
      parent: observation.parent,
    },
  ];
}

function parentsRoughlyAlign(a, b) {
  const na = String(a || "").toLowerCase();
  const nb = String(b || "").toLowerCase();
  if (!na || !nb) return false;
  if (na.includes("ihg") && nb.includes("ihg")) return true;
  if (na.includes("marriott") && nb.includes("marriott")) return true;
  if (na.includes("choice") && nb.includes("choice")) return true;
  if (na.includes("minor") && nb.includes("minor")) return true;
  return na === nb || na.includes(nb) || nb.includes(na);
}

/** @deprecated use computeDirectoryGaps */
export function findDirectoryGaps(censusHotels, directoryRows, opts) {
  const out = computeDirectoryGaps(censusHotels, directoryRows, opts);
  return out.missingCensusCandidates.map((g) => ({
    directoryName: g.directoryName,
    country: g.country,
    city: g.city,
    brand: g.brand,
    officialUrl: g.officialUrl,
    bestCensusScore: g.bestCensusMatch?.score ?? 0,
    classification: g.classification,
    recommended_action: g.recommended_action,
  }));
}

export {
  loadIhgDirectoryRows,
  loadMarriottTributeDirectoryRows,
  loadMarriottSoftBrandDirectoryRows,
  computeDirectoryGaps,
  computeChoiceIndividualsGaps,
  computeMarriottSoftBrandGaps,
  MATCH_GATE_CONFIG_V1_1,
  CORROBORATION_CONFIG_V1_1,
};
