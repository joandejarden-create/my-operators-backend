/**
 * Segment discovery-coverage & commercial-yield reporting V2.
 * Four independent readings: discovery coverage, research completeness,
 * commercial qualification, client publication readiness.
 * Six investigated companies ≠ comprehensive market coverage.
 * Two useful prospects ≠ segment exhaustion.
 */

import { SUPERVISOR_EXPECTED_SEGMENTS } from "./research-constants-v1.js";
import { SEGMENT_SOURCE_FAMILIES_EXPECTED, SEGMENT_DEMAND_MECHANISMS_EXPECTED } from "./demand-segment-research-supervisor-v1.js";
import { STOPPING_TRIGGER } from "./commercial-source-selection-v1.js";

export const SEGMENT_DISCOVERY_COVERAGE_VERSION = "gdi-segment-discovery-coverage-v2";

export const COVERAGE_READING = Object.freeze({
  DISCOVERY_COVERAGE: "discovery_coverage",
  RESEARCH_COMPLETENESS: "research_completeness",
  COMMERCIAL_QUALIFICATION: "commercial_qualification",
  CLIENT_PUBLICATION_READINESS: "client_publication_readiness",
});

/**
 * Build segment coverage report from investigation state (offline-safe).
 */
export function buildSegmentDiscoveryCoverageReport({
  hotelId = null,
  hotelName = null,
  segmentId = null,
  knownOrganizations = [],
  discoveredOrganizations = [],
  investigatedCandidates = [],
  uninvestigatedQueue = [],
  incompleteCandidates = [],
  usefulInternalProspects = [],
  publicationEligibleCount = 0,
  independentPublisherReads = 0,
  relatedEntityLeadsFollowed = 0,
  stoppingTrigger = null,
  usefulProspectTargetHit = false,
  providerOrBudgetFinished = false,
  expectedSourceFamilies = null,
  expectedDemandMechanisms = null,
} = {}) {
  const known = uniqueNames(knownOrganizations);
  const discovered = uniqueNames(discoveredOrganizations);
  const investigated = uniqueNames(investigatedCandidates.map((c) => c.organizationName || c));
  const uninvestigated = uniqueNames(uninvestigatedQueue.map((c) => c.organizationName || c));
  const incomplete = uniqueNames(incompleteCandidates.map((c) => c.organizationName || c));
  const universeEstimate = uniqueNames([...known, ...discovered]);

  const discoveryCoverageRatio = universeEstimate.length
    ? Number((investigated.length / universeEstimate.length).toFixed(3))
    : 0;

  const discoveryReading = {
    reading: COVERAGE_READING.DISCOVERY_COVERAGE,
    status: discoveryCoverageRatio >= 0.7 && uninvestigated.length === 0
      ? "BROAD"
      : discoveryCoverageRatio >= 0.35
        ? "PARTIAL"
        : "NARROW",
    organizationsInUniverseEstimate: universeEstimate.length,
    knownOrganizations: known.length,
    discoveredOrganizations: discovered.length,
    investigatedCount: investigated.length,
    uninvestigatedQueueCount: uninvestigated.length,
    coverageRatio: discoveryCoverageRatio,
    note: "Investigating a shortlist (e.g. six firms) is not comprehensive market coverage.",
  };

  const researchCompleteness = {
    reading: COVERAGE_READING.RESEARCH_COMPLETENESS,
    status: incomplete.length === 0 && uninvestigated.length === 0 && investigated.length > 0
      ? "COMPLETE_FOR_KNOWN_QUEUE"
      : incomplete.length || uninvestigated.length
        ? "INCOMPLETE"
        : "NOT_STARTED",
    investigatedToDefensibleDisposition: investigated.length,
    incompleteCandidates: incomplete.length,
    independentPublisherReads,
    relatedEntityLeadsFollowed,
    stoppingTrigger: stoppingTrigger || null,
    retrievalCapIsNotExhaustion: stoppingTrigger === STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED,
    usefulProspectTargetIsNotSegmentExhaustion: usefulProspectTargetHit === true,
    providerOrBudgetFinishedIsNotExhaustion: providerOrBudgetFinished === true,
    segmentExhaustedDeclared: false,
    note: "Do not declare the segment exhausted because a search batch, provider, or budget finished, or because two useful prospects were found.",
  };

  const commercialQualification = {
    reading: COVERAGE_READING.COMMERCIAL_QUALIFICATION,
    status: usefulInternalProspects.length > 0 ? "HAS_INTERNAL_PROSPECTS" : "NO_QUALIFIED_INTERNAL_PROSPECTS_YET",
    usefulInternalProspectCount: usefulInternalProspects.length,
    commercialYieldPerInvestigated: investigated.length
      ? Number((usefulInternalProspects.length / investigated.length).toFixed(3))
      : 0,
    expectedSourceFamilies: expectedSourceFamilies
      || SEGMENT_SOURCE_FAMILIES_EXPECTED[segmentId]
      || null,
    expectedDemandMechanisms: expectedDemandMechanisms
      || SEGMENT_DEMAND_MECHANISMS_EXPECTED[segmentId]
      || null,
  };

  const publicationReadiness = {
    reading: COVERAGE_READING.CLIENT_PUBLICATION_READINESS,
    status: publicationEligibleCount > 0 ? "HAS_PUBLICATION_ELIGIBLE" : "NONE_PUBLICATION_ELIGIBLE",
    publicationEligibleCount,
    separateFromInternalResearchCompletion: true,
    separateFromDiscoveryCoverage: true,
  };

  return {
    version: SEGMENT_DISCOVERY_COVERAGE_VERSION,
    hotelId,
    hotelName,
    segmentId,
    elevenSegmentArchitecture: SUPERVISOR_EXPECTED_SEGMENTS.length === 11,
    readings: {
      discoveryCoverage: discoveryReading,
      researchCompleteness,
      commercialQualification,
      clientPublicationReadiness: publicationReadiness,
    },
    queues: {
      uninvestigated: uninvestigated.slice(0, 50),
      incomplete: incomplete.slice(0, 50),
    },
    preserveStateAcrossSessionsAndWeeklyRefreshes: true,
    doNotAutoStopAfterTwoUsefulProspects: true,
  };
}

function uniqueNames(list) {
  const out = [];
  const seen = new Set();
  for (const item of list || []) {
    const name = String(item || "").trim();
    if (!name) continue;
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(name);
  }
  return out;
}

/**
 * Decide whether segment research may stop for coverage reasons (not pilot targets).
 */
export function shouldContinueSegmentDiscovery({
  coverageReport = null,
  usefulProspectTargetHit = false,
  remainingBudgetUsd = 0,
  uninvestigatedRemaining = 0,
} = {}) {
  if (!(remainingBudgetUsd > 0.001)) {
    return {
      continue: false,
      reason: "BUDGET_EXHAUSTED",
      note: "Budget exhaustion is not research exhaustion of the segment universe.",
    };
  }
  if (usefulProspectTargetHit && uninvestigatedRemaining > 0) {
    return {
      continue: true,
      reason: "USEFUL_TARGET_HIT_BUT_QUEUE_REMAINS",
      note: "Two useful prospects is a commercial milestone, not segment coverage completion.",
    };
  }
  const discovery = coverageReport?.readings?.discoveryCoverage;
  if (discovery?.status === "NARROW" || discovery?.status === "PARTIAL") {
    return {
      continue: uninvestigatedRemaining > 0,
      reason: uninvestigatedRemaining > 0 ? "DISCOVERY_COVERAGE_INCOMPLETE" : "NO_UNINVESTIGATED_IN_KNOWN_UNIVERSE",
    };
  }
  return {
    continue: uninvestigatedRemaining > 0,
    reason: uninvestigatedRemaining > 0 ? "QUEUE_REMAINS" : "KNOWN_QUEUE_CLEARED",
  };
}
