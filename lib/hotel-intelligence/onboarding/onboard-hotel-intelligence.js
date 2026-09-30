/**
 * onboardHotelIntelligence(hpcHotelId)
 *
 * HPC → HI research → domain status → ADP attributes → completeness gate.
 * Idempotent. Does not invent seasonality/need periods.
 */

import { createHash } from "node:crypto";
import { resolveCanonicalHotelId, isAirtableRecordId } from "../../hotel-census/adp-gdi-canonical-identity.js";
import { researchHotelIntelligence } from "../research/research-hotel-intelligence.js";
import { buildHotelIntelligenceProfile } from "../adp-attributes/build-hotel-intelligence-profile.js";
import { buildAdpHotelAttributes } from "../adp-attributes/build-adp-hotel-attributes.js";
import { syncHotelAdpAttributesToAirtable } from "../adp-attributes/airtable-store.js";
import { countActiveAdpAttributes } from "./adp-attribute-counts.js";
import {
  upsertDomainStatus,
  loadDomainStatusLedger,
  evaluateHotelDomainStatuses,
} from "./domain-status-store.js";
import {
  HI_DOMAIN,
  HI_DOMAIN_STATUS,
  OVERALL_HI_STATUS,
  REQUIRED_HI_DOMAINS,
} from "./domain-status-v1.js";
import { isHotelIntelligenceComplete, assertNoNotResearchedForOnboard } from "./hi-completeness-gate.js";

function runIdFor(hpcHotelId) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const hash = createHash("sha1").update(`${hpcHotelId}|${stamp}`).digest("hex").slice(0, 8);
  return `hi_onboard_${stamp}_${hash}`;
}

function markDomain(hpcHotelId, domain, status, extra = {}) {
  return upsertDomainStatus(hpcHotelId, domain, {
    domainStatus: status,
    lastResearchedAt: new Date().toISOString(),
    researchRunId: extra.researchRunId || null,
    evidenceCount: extra.evidenceCount ?? 0,
    rowCount: extra.rowCount ?? 0,
    sourceCoverage: extra.sourceCoverage || [],
    notes: extra.notes || null,
    blocker:
      status === HI_DOMAIN_STATUS.NOT_RESEARCHED ||
      status === HI_DOMAIN_STATUS.ERROR ||
      status === HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING
        ? status
        : null,
  });
}

/**
 * Persist domain statuses from a research result + refreshed profile.
 */
export function persistDomainStatusesFromResearch(hpcHotelId, research, profile, opts = {}) {
  const runId = opts.researchRunId || research?.runId || runIdFor(hpcHotelId);
  const evidenceCount = (research?.evidence || []).length || profile?.evidenceSummary?.evidenceAirtable || 0;
  const sources = (research?.sourcesChecked || [])
    .filter((s) => s.ok)
    .map((s) => s.role);

  // Commercial
  const commercialPopulated =
    profile?.evidenceSummary?.commercialAirtable ||
    profile?.commercialProfile?.rooms != null ||
    research?.discovered?.commercial?.roomsKeys != null;
  markDomain(
    hpcHotelId,
    HI_DOMAIN.COMMERCIAL_PROFILE,
    commercialPopulated ? HI_DOMAIN_STATUS.POPULATED : HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING,
    {
      researchRunId: runId,
      rowCount: commercialPopulated ? 1 : 0,
      evidenceCount,
      sourceCoverage: sources,
      notes: commercialPopulated ? "commercial_profile_present" : "commercial_research_ceiling",
    }
  );

  // Event spaces
  const eventRows =
    (profile?.eventSpaces || []).filter((e) => e.fromHiAirtable).length ||
    (research?.discovered?.eventSpaces || []).length ||
    0;
  const eventsPageOk = (research?.sourcesChecked || []).some(
    (s) => s.role === "official_events" && s.ok
  );
  const meetingTotals =
    research?.discovered?.commercial?.totalMeetingSpaceSqFt != null ||
    profile?.commercialProfile?.meetingSpace?.totalSqFt != null;

  let eventStatus = HI_DOMAIN_STATUS.NOT_RESEARCHED;
  let eventNotes = null;
  if (eventRows > 0) {
    eventStatus = HI_DOMAIN_STATUS.POPULATED;
    eventNotes = "event_spaces_populated";
  } else if (eventsPageOk && !meetingTotals) {
    // Official events path checked; no supportable inventory extracted
    eventStatus = HI_DOMAIN_STATUS.RESEARCHED_EMPTY;
    eventNotes = "official_events_checked_no_supportable_space_rows";
  } else if (research) {
    eventStatus = HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING;
    eventNotes = "bounded_event_space_research_exhausted";
  }
  markDomain(hpcHotelId, HI_DOMAIN.EVENT_SPACES, eventStatus, {
    researchRunId: runId,
    rowCount: eventRows,
    evidenceCount,
    sourceCoverage: sources,
    notes: eventNotes,
  });

  // Demand nodes — HI airtable or proposed from research
  const demandRows =
    (profile?.demandNodes || []).filter((d) => d.fromHiAirtable).length ||
    (research?.discovered?.demandNodes || []).length ||
    0;
  // If GDI config seeds demand and research wrote them, count as populated
  const demandProposed = (research?.discovered?.demandNodes || []).length;
  const demandStatus =
    demandRows > 0 || demandProposed > 0
      ? HI_DOMAIN_STATUS.POPULATED
      : research
        ? HI_DOMAIN_STATUS.RESEARCHED_EMPTY
        : HI_DOMAIN_STATUS.NOT_RESEARCHED;
  markDomain(hpcHotelId, HI_DOMAIN.DEMAND_NODES, demandStatus, {
    researchRunId: runId,
    rowCount: Math.max(demandRows, demandProposed),
    evidenceCount,
    sourceCoverage: sources,
    notes:
      demandStatus === HI_DOMAIN_STATUS.RESEARCHED_EMPTY
        ? "no_supportable_demand_nodes"
        : "demand_nodes_present",
  });

  // Seasonality / need periods — policy: do not invent; after research → RESEARCHED_EMPTY
  const seasonRows =
    ((profile?.seasonality || []).length || 0) + ((profile?.needPeriods || []).length || 0);
  markDomain(
    hpcHotelId,
    HI_DOMAIN.SEASONALITY_NEED_PERIODS,
    seasonRows > 0
      ? HI_DOMAIN_STATUS.POPULATED
      : research
        ? HI_DOMAIN_STATUS.RESEARCHED_EMPTY
        : HI_DOMAIN_STATUS.NOT_RESEARCHED,
    {
      researchRunId: runId,
      rowCount: seasonRows,
      evidenceCount,
      sourceCoverage: sources,
      notes: seasonRows
        ? "seasonality_or_need_periods_present"
        : "no_hotel_specific_need_period_evidence_by_policy",
    }
  );

  // Evidence
  const evRows = profile?.evidenceSummary?.evidenceAirtable || evidenceCount;
  markDomain(
    hpcHotelId,
    HI_DOMAIN.HI_EVIDENCE,
    evRows > 0
      ? HI_DOMAIN_STATUS.POPULATED
      : research
        ? HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING
        : HI_DOMAIN_STATUS.NOT_RESEARCHED,
    {
      researchRunId: runId,
      rowCount: evRows,
      evidenceCount: evRows,
      sourceCoverage: sources,
      notes: evRows ? "evidence_rows_present" : "no_evidence_rows_after_research",
    }
  );

  // ADP attributes
  const adpCount = opts.adpAttributeActiveCount ?? 0;
  markDomain(
    hpcHotelId,
    HI_DOMAIN.ADP_ATTRIBUTES,
    adpCount > 0
      ? HI_DOMAIN_STATUS.POPULATED
      : research
        ? HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING
        : HI_DOMAIN_STATUS.NOT_RESEARCHED,
    {
      researchRunId: runId,
      rowCount: adpCount,
      evidenceCount,
      notes: adpCount ? "active_adp_attributes_present" : "adp_attributes_missing",
    }
  );

  return loadDomainStatusLedger(hpcHotelId);
}

/**
 * @param {string} hpcHotelId
 * @param {{
 *   mode?: "dry-run"|"apply",
 *   forceRefresh?: boolean,
 *   knownCventUrl?: string,
 *   skipResearch?: boolean,
 *   syncAdpAttributes?: boolean,
 * }} [opts]
 */
export async function onboardHotelIntelligence(hpcHotelId, opts = {}) {
  const mode = opts.mode || "dry-run";
  const researchRunId = runIdFor(hpcHotelId);
  const resolved =
    resolveCanonicalHotelId(hpcHotelId) ||
    (isAirtableRecordId(hpcHotelId) ? hpcHotelId : null);

  if (!resolved) {
    return {
      ok: false,
      error: "unresolved_hpc_hotel_id",
      hotelId: hpcHotelId,
      gdiEligible: false,
      adpGdiReady: false,
    };
  }

  // 1) validate HPC identity via profile
  const beforeProfile = await buildHotelIntelligenceProfile(resolved, {
    skipLiveHpc: opts.skipLiveHpc === true,
  });
  if (!beforeProfile.ok) {
    return {
      ok: false,
      error: beforeProfile.error,
      hotelId: resolved,
      gdiEligible: false,
      adpGdiReady: false,
    };
  }

  const beforeGate = await isHotelIntelligenceComplete(resolved, {
    profile: beforeProfile,
    skipAdpCount: opts.skipAdpCount === true,
  });

  // Short-circuit if already complete and not forcing refresh
  if (beforeGate.complete && !opts.forceRefresh && opts.skipResearch !== false) {
    return {
      ok: true,
      hotelId: resolved,
      hotelName: beforeProfile.identity.hotelName,
      adpPropertyId: beforeProfile.identity.adpPropertyId,
      mode,
      researchRunId,
      skippedResearch: true,
      reason: "already_hi_complete",
      before: beforeGate,
      after: beforeGate,
      gdiEligible: true,
      adpGdiReady: true,
      onboardAllowed: assertNoNotResearchedForOnboard(beforeGate),
    };
  }

  // 2–5) research commercial / event / demand / seasonality policy
  let research = null;
  if (opts.skipResearch !== true) {
    research = await researchHotelIntelligence(resolved, {
      mode,
      forceRefresh: opts.forceRefresh,
      knownCventUrl: opts.knownCventUrl,
      skipLiveHpc: opts.skipLiveHpc,
    });
    research.runId = researchRunId;
  }

  // 6–7) refresh profile + optional ADP sync if apply already did it inside research
  let afterProfile = await buildHotelIntelligenceProfile(resolved, {
    skipLiveHpc: opts.skipLiveHpc === true,
  });

  let adpSync = null;
  if (mode === "apply" && opts.syncAdpAttributes === true && research?.applied) {
    // research apply already syncs ADP attrs; optional re-derive
    const attrs = await buildAdpHotelAttributes(resolved, { profile: afterProfile });
    adpSync = await syncHotelAdpAttributesToAirtable(attrs, { dryRun: false });
    afterProfile = await buildHotelIntelligenceProfile(resolved, {
      skipLiveHpc: opts.skipLiveHpc === true,
    });
  }

  let adpCount = 0;
  try {
    adpCount = await countActiveAdpAttributes(resolved);
  } catch {
    adpCount = research?.adpAttributes?.total || 0;
  }

  // Persist domain statuses (even on dry-run for local ledger of research attempt)
  const ledger = persistDomainStatusesFromResearch(resolved, research, afterProfile, {
    researchRunId,
    adpAttributeActiveCount: adpCount,
  });

  const afterGate = await isHotelIntelligenceComplete(resolved, {
    profile: afterProfile,
    adpAttributeActiveCount: adpCount,
  });

  const onboardAllowed = assertNoNotResearchedForOnboard(afterGate);

  return {
    ok: true,
    hotelId: resolved,
    hotelName: afterProfile.identity?.hotelName || beforeProfile.identity.hotelName,
    adpPropertyId: afterProfile.identity?.adpPropertyId,
    mode,
    researchRunId,
    research: research
      ? {
          ok: research.ok,
          applied: research.applied,
          cost: research.cost,
          eventSpaces: research.discovered?.eventSpaces?.length || 0,
          demandNodes: research.discovered?.demandNodes?.length || 0,
          evidence: research.evidence?.length || 0,
          errors: research.errors || [],
          skipped: research.skipped || [],
        }
      : null,
    adpSync: adpSync
      ? {
          createCount: adpSync.createCount,
          updateCount: adpSync.updateCount,
          deactivateCount: adpSync.deactivateCount,
        }
      : null,
    domainLedger: {
      overallStatus: ledger.overallStatus,
      coverage: ledger.coverage,
      domains: Object.fromEntries(
        REQUIRED_HI_DOMAINS.map((d) => [d, ledger.domains?.[d]?.domainStatus || null])
      ),
    },
    before: {
      overallStatus: beforeGate.overallStatus,
      coverage: beforeGate.coverage,
      complete: beforeGate.complete,
    },
    after: afterGate,
    gdiEligible: onboardAllowed.allowed,
    adpGdiReady: onboardAllowed.allowed,
    onboardAllowed,
    flow: [
      "validate_hpc_identity",
      "commercial_profile",
      "event_spaces",
      "demand_nodes",
      "seasonality_need_periods",
      "evidence_audit",
      "adp_attribute_derivation",
      "hi_completeness_gate",
      "gdi_initialization_eligibility",
      "adp_gdi_ready_flag",
    ],
  };
}

export { evaluateHotelDomainStatuses, OVERALL_HI_STATUS, HI_DOMAIN, HI_DOMAIN_STATUS };
