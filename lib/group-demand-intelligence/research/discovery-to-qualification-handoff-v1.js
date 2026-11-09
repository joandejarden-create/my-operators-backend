/**
 * Discovery → commercial qualification handoff V1.
 *
 * After segment discovery extracts evidence-backed candidates, run Phase 2
 * disposition (zero-cost) and persist opportunity-research-state.
 * Does not invent buyers. Does not publish. Does not spend.
 */

import {
  PHASE2_DISPOSITION,
  classifyPhase2Disposition,
  detectDuplicateCommercialOpportunities,
  persistPhase2Dispositions,
  normalizeDemandKey,
  fuzzyDemandMatch,
} from "./live-pilot-commercial-qualification-phase2-v1.js";
import { persistDiscoveryTrace } from "./full-e2e-fourteen-segment-discovery-state-v1.js";
import { DISCOVERY_EXECUTION_STATUS } from "./segment-discovery-executor-v1.js";

export const DISCOVERY_TO_QUALIFICATION_HANDOFF_V1 =
  "gdi-discovery-to-qualification-handoff-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function urlsFromCandidate(c) {
  const fromEvidence = [
    ...(c.evidenceUrls || []),
    ...(c.supportingSources || []).map((s) => s?.url || s?.sourceUrl),
    c.sourceUrl,
    c.url,
  ].filter((u) => /^https?:\/\//i.test(String(u || "")));
  return [...new Set(fromEvidence.map(String))];
}

/**
 * Map a discovery-trace candidate into a Phase-2-like assessment row.
 */
export function mapDiscoveryCandidateToAssessment(candidate, meta = {}) {
  const organizationName =
    text(
      candidate.organizationName ||
        candidate.organizingEntity ||
        candidate.companyName ||
        ""
    ) || null;
  const activityTitle =
    text(
      candidate.title ||
        candidate.activityTitle ||
        candidate.programmeName ||
        candidate.eventName ||
        ""
    ) || null;
  const urls = urlsFromCandidate(candidate);
  const hasHttpEvidence =
    candidate.hasSourceEvidence === true || urls.length > 0;
  const dates = text(candidate.dates || candidate.needDate || "") || null;
  const venue = text(candidate.venue || "") || null;
  const city = text(candidate.city || candidate.geography || "") || null;

  const contacts = {
    namedContacts: [],
    functionalRoutes: urls
      .filter((u) => /hospedaje|housing|hotel-sede|alojamiento|lodging/i.test(u))
      .slice(0, 1)
      .map((route) => ({
        route,
        class: "FUNCTIONAL_CONTACT_ROUTE",
        note: "Organizational housing route from discovery evidence",
      })),
    verifiedDirectBuyers: 0,
    verifiedEntryPoints: 0,
    contactStatus: "NOT_VERIFIED",
  };
  if (contacts.functionalRoutes.length) {
    contacts.contactStatus = "FUNCTIONAL_CONTACT_ROUTE_ONLY";
  }

  return {
    candidateId: candidate.candidateId,
    hotelId: meta.hotelId || candidate.hotelId,
    hotelName: meta.hotelName || candidate.hotelName || null,
    segmentId: meta.segmentId || candidate.segmentId,
    providerRunId: candidate.providerRunId || meta.providerRunId || null,
    organizationName,
    activityTitle,
    demandIdentityKey: normalizeDemandKey(
      `${organizationName || ""}|${activityTitle || ""}|${meta.segmentId || ""}`
    ),
    urls,
    dates,
    venue,
    city,
    eventStatus: text(candidate.eventStatus || "") || null,
    sourcingStatus: text(candidate.sourcingStatus || "") || null,
    facts: [
      organizationName && `org:${organizationName}`,
      activityTitle && `activity:${activityTitle}`,
      dates && `dates:${dates}`,
      urls.length && `sources:${urls.length}`,
    ].filter(Boolean),
    inferences: [],
    unknowns: [
      "named_buyer_not_verified",
      "sourcing_accessibility_unconfirmed",
    ],
    hasHttpEvidence,
    overflowSpeculative: Boolean(candidate.overflowSpeculative),
    contacts,
  };
}

/**
 * Qualify discovery candidates without paid retrieval.
 * Returns dispositions + optional durable persistence.
 */
export function qualifyDiscoveryCandidates(candidates = [], meta = {}, opts = {}) {
  const assessments = (candidates || []).map((c) =>
    mapDiscoveryCandidateToAssessment(c, meta)
  );

  const unique = [];
  const rejected = [];
  const seen = new Map();
  const waterfall = {
    providerFindings: assessments.length,
    evidenceSupported: 0,
    uniqueDemandActivities: 0,
    newOpportunities: 0,
    rejectedSignals: 0,
    duplicateLiveCollapsed: 0,
  };

  for (const a of assessments) {
    if (a.hasHttpEvidence) waterfall.evidenceSupported += 1;
    else {
      waterfall.rejectedSignals += 1;
      rejected.push({ ...a, disposition: PHASE2_DISPOSITION.REJECTED });
      continue;
    }
    const key = `${a.hotelId}|${a.demandIdentityKey}`;
    if (seen.has(key)) {
      waterfall.duplicateLiveCollapsed += 1;
      const prior = seen.get(key);
      prior.urls = [...new Set([...(prior.urls || []), ...a.urls])];
      continue;
    }
    const disposition = classifyPhase2Disposition(a, a.contacts, null);
    const record = {
      ...a,
      match: null,
      disposition: disposition.disposition,
      dispositionReason: disposition.reason,
      customerPublicationReady: false,
      nextAction: disposition.nextAction || null,
      salesAction:
        disposition.disposition === PHASE2_DISPOSITION.ACCOUNT_DEVELOPMENT
          ? "Internal research continuation: identify named buyer/entry point"
          : disposition.disposition === PHASE2_DISPOSITION.NEEDS_MORE_RESEARCH
            ? "Resolve evidence gaps before commercial pursuit"
            : disposition.disposition === PHASE2_DISPOSITION.WATCH
              ? "Monitor; do not pitch as confirmed overflow"
              : "Do not pursue",
    };
    seen.set(key, record);
    unique.push(record);
    if (disposition.disposition === PHASE2_DISPOSITION.REJECTED) {
      waterfall.rejectedSignals += 1;
      rejected.push(record);
    } else waterfall.newOpportunities += 1;
  }
  waterfall.uniqueDemandActivities = unique.length;

  const duplicateScan = detectDuplicateCommercialOpportunities({
    liveFindings: unique,
    inventoryRows: opts.inventoryRows || [],
    historicalRows: opts.historicalRows || [],
  });

  const reconcileDoc = {
    version: DISCOVERY_TO_QUALIFICATION_HANDOFF_V1,
    at: new Date().toISOString(),
    hotelId: meta.hotelId,
    segmentId: meta.segmentId,
    waterfall,
    unique,
    rejected,
    duplicateScan,
    dispositionCounts: Object.fromEntries(
      Object.values(PHASE2_DISPOSITION).map((d) => [
        d,
        unique.filter((u) => u.disposition === d).length,
      ])
    ),
    paidSpendUsd: 0,
    discoveryComplete: false,
    qualificationComplete:
      unique.length > 0 &&
      unique.every((u) => Boolean(u.disposition)) &&
      unique.every(
        (u) =>
          u.disposition !== PHASE2_DISPOSITION.SALES_READY ||
          (u.contacts?.verifiedDirectBuyers || 0) +
            (u.contacts?.verifiedEntryPoints || 0) >
            0
      ),
  };

  let persisted = null;
  if (opts.persist !== false) {
    persisted = persistPhase2Dispositions(reconcileDoc, {
      outDir: opts.outDir,
    });
  }

  let discoveryState = null;
  if (opts.persistDiscoveryState !== false && meta.hotelId && meta.segmentId) {
    discoveryState = persistDiscoveryTrace(
      {
        hotelId: meta.hotelId,
        segmentId: meta.segmentId,
        researchRunId: opts.researchRunId || "qualification_handoff",
        executionStatus: DISCOVERY_EXECUTION_STATUS.QUALIFICATION_PARTIAL,
        retrievalMode: "PHASE2_ZERO_COST_QUALIFICATION",
        providerCallCount: 0,
        freshRetrievals: 0,
        researchCostUsd: 0,
        stopReason:
          "DISCOVERY_HANDED_OFF_TO_QUALIFICATION_CONTACT_GAPS_REMAIN",
        candidates: unique.map((r) => ({
          candidateId: r.candidateId,
          organizationName: r.organizationName,
          title: r.activityTitle,
          hasSourceEvidence: r.hasHttpEvidence,
          evidenceUrls: r.urls,
          stage: "B_COMMERCIAL_RESEARCH",
          disposition: r.disposition,
        })),
        rejections: rejected.map((r) => ({
          candidateId: r.candidateId,
          reason: r.dispositionReason || "rejected",
        })),
        discoveryCompleted: false,
      },
      { researchRunId: opts.researchRunId || "qualification_handoff" }
    );
  }

  return {
    reconcileDoc,
    persisted,
    discoveryState,
    fuzzyDemandMatch,
  };
}

/**
 * Attach qualification handoff onto a discovery cell trace.
 */
export function attachQualificationHandoffToTrace(trace, opts = {}) {
  if (!trace?.hotelId || !trace?.segmentId) {
    return { ...trace, qualificationHandoff: { skipped: true, reason: "missing_ids" } };
  }
  const candidates = trace.candidates || [];
  if (!candidates.length) {
    return {
      ...trace,
      qualificationHandoff: { skipped: true, reason: "no_candidates" },
      commercialResearchComplete: false,
    };
  }

  const result = qualifyDiscoveryCandidates(
    candidates,
    {
      hotelId: trace.hotelId,
      segmentId: trace.segmentId,
      hotelName: trace.hotelName,
      providerRunId: (trace.providerRunIds || [])[0] || null,
    },
    {
      persist: opts.persist !== false,
      persistDiscoveryState: opts.persistDiscoveryState !== false,
      researchRunId: trace.researchRunId,
      outDir: opts.outDir,
      inventoryRows: opts.inventoryRows,
      historicalRows: opts.historicalRows,
    }
  );

  return {
    ...trace,
    commercialResearchComplete: false,
    qualificationHandoff: {
      version: DISCOVERY_TO_QUALIFICATION_HANDOFF_V1,
      waterfall: result.reconcileDoc.waterfall,
      dispositionCounts: result.reconcileDoc.dispositionCounts,
      uniqueCount: result.reconcileDoc.unique.length,
      salesReady: result.reconcileDoc.dispositionCounts.SALES_READY || 0,
      paidSpendUsd: 0,
      durableStatePath: result.discoveryState?.path || null,
      note: "Discovery finished ≠ commercial research complete",
    },
    executionStatus:
      result.reconcileDoc.unique.length > 0
        ? DISCOVERY_EXECUTION_STATUS.QUALIFICATION_PARTIAL
        : trace.executionStatus,
  };
}

export default {
  DISCOVERY_TO_QUALIFICATION_HANDOFF_V1,
  mapDiscoveryCandidateToAssessment,
  qualifyDiscoveryCandidates,
  attachQualificationHandoffToTrace,
};
