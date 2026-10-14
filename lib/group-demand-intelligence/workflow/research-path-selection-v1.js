/**
 * Conditional research-path selection for the integrated GDI workflow.
 * Every path is recorded as executed / reused / not_applicable / blocked / incomplete.
 * A small existing database is not evidence that a path is irrelevant.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const RESEARCH_PATH_SELECTION_VERSION = "gdi-research-path-selection-v1";

export const PATH_STATUS = Object.freeze({
  EXECUTED: "executed",
  REUSED_EXISTING_EVIDENCE: "reused_existing_evidence",
  NOT_APPLICABLE: "not_applicable",
  BLOCKED: "blocked",
  INCOMPLETE: "incomplete",
});

/** Canonical research paths — inventory of historical GDI capabilities. */
export const RESEARCH_PATHS = Object.freeze({
  DEMAND_GENERATORS: "demand_generators",
  DEMAND_PROGRAMS: "demand_programs",
  DEMAND_GENERATOR_SIGNALS: "demand_generator_signals",
  HOTEL_GENERATOR_FIT: "hotel_generator_fit",
  PRIVATE_EVENT_VENUES: "private_event_venues",
  HOTEL_VENUE_FIT: "hotel_venue_fit",
  PRIVATE_EVENT_SIGNALS: "private_event_signals",
  GEOGRAPHIC_EXPANSION: "geographic_expansion",
  CONTACT_RESOLUTION: "contact_resolution",
  FEEDBACK_DRIVEN_RESEARCH: "feedback_driven_research",
  PARALLEL_DISCOVERY: "parallel_discovery",
  NATIVE_BLIND_DISCOVERY: "native_blind_discovery",
  OPPORTUNITY_STORE_REUSE: "opportunity_store_reuse",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadHotelConfig(hotelId) {
  const p = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${String(hotelId || "").trim()}.json`
  );
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function hasMeetingInventory(cfg) {
  const n =
    cfg?.capabilityProfile?.meetingRoomCount ??
    cfg?.capabilityProfile?.meetingRoomsIndoor ??
    cfg?.meetings?.meetingRoomCount;
  return Number(n) > 0;
}

function peRelevantFromConfig(cfg) {
  if (!cfg) return { relevant: true, reason: "no_config_fail_open_assess" };
  const blob = JSON.stringify(cfg).toLowerCase();
  const segments = (cfg.commercialPriorities?.targetSegments || []).map((s) =>
    String(s).toLowerCase()
  );
  const social =
    segments.some((s) => /social|wedding|weekend|private/.test(s)) ||
    /wedding|private.?event|social|weekend/.test(blob);
  const meetings = hasMeetingInventory(cfg);
  // Small DB ≠ irrelevant: PE path is relevant when hotel has meetings/social positioning
  // OR when territory/config does not explicitly exclude PE.
  const excluded = /exclude.?private.?events|pe.?not.?applicable/i.test(blob);
  if (excluded) {
    return { relevant: false, reason: "config_explicitly_excludes_private_events" };
  }
  if (social || meetings) {
    return {
      relevant: true,
      reason: social
        ? "social_weekend_or_wedding_positioning"
        : "meeting_inventory_present_pe_path_assessable",
    };
  }
  // Still assess — nearby venue demand may exist even without social segments
  return {
    relevant: true,
    reason: "fail_open_nearby_venue_demand_may_exist_small_db_not_disproof",
  };
}

function dgRelevantFromConfig(cfg) {
  if (!cfg) return { relevant: true, reason: "no_config_fail_open" };
  const anchors = cfg.commercialPriorities?.demandAnchorFocus || [];
  const segments = cfg.commercialPriorities?.targetSegments || [];
  if (anchors.length || segments.length) {
    return { relevant: true, reason: "configured_segments_or_demand_anchors" };
  }
  return {
    relevant: true,
    reason: "fail_open_empty_segments_do_not_prove_generators_irrelevant",
  };
}

/**
 * Select research paths for a hotel. Does not execute paid research.
 */
export function selectResearchPaths(hotelId, opts = {}) {
  const id = text(hotelId);
  const cfg = opts.hotelConfig || loadHotelConfig(id);
  const pe = peRelevantFromConfig(cfg);
  const dg = dgRelevantFromConfig(cfg);
  const evidence = opts.evidenceHints || {};
  const paths = [];

  function push(pathId, status, detail = {}) {
    paths.push({
      pathId,
      status,
      ...detail,
      recordedAt: new Date().toISOString(),
    });
  }

  // Opportunity store — always first
  push(RESEARCH_PATHS.OPPORTUNITY_STORE_REUSE, PATH_STATUS.REUSED_EXISTING_EVIDENCE, {
    selectionRule: "always_before_paid_discovery",
    note: "Existing opportunities including internal/enrichment/DQ",
    evidencePresent: evidence.opportunityCount != null,
    opportunityCount: evidence.opportunityCount ?? null,
  });

  // Demand generators / programs / signals / hotel fit
  if (!dg.relevant) {
    push(RESEARCH_PATHS.DEMAND_GENERATORS, PATH_STATUS.NOT_APPLICABLE, {
      selectionRule: "config_segments_anchors",
      reason: dg.reason,
    });
  } else if (evidence.airtableBlocked) {
    push(RESEARCH_PATHS.DEMAND_GENERATORS, PATH_STATUS.BLOCKED, {
      selectionRule: "config_segments_anchors",
      reason: evidence.airtableBlockReason || "airtable_not_configured",
      note: "Path remains relevant — small/zero local count is not irrelevance",
    });
    push(RESEARCH_PATHS.HOTEL_GENERATOR_FIT, PATH_STATUS.BLOCKED, {
      selectionRule: "hotel_id_linked_fits",
      reason: evidence.airtableBlockReason || "airtable_not_configured",
    });
    push(RESEARCH_PATHS.DEMAND_PROGRAMS, PATH_STATUS.BLOCKED, {
      selectionRule: "programs_under_generators",
      reason: evidence.airtableBlockReason || "airtable_not_configured",
    });
    push(RESEARCH_PATHS.DEMAND_GENERATOR_SIGNALS, PATH_STATUS.BLOCKED, {
      selectionRule: "signals_for_fitted_generators",
      reason: evidence.airtableBlockReason || "airtable_not_configured",
    });
  } else {
    const hasFits = (evidence.dgFitCount || 0) > 0;
    const hasPrograms = (evidence.programCount || 0) > 0;
    const hasSignals = (evidence.dgSignalCount || 0) > 0;
    const hasDgGraph = hasFits || hasPrograms || hasSignals || (evidence.dgCount || 0) > 0;

    push(
      RESEARCH_PATHS.DEMAND_GENERATORS,
      hasDgGraph
        ? PATH_STATUS.REUSED_EXISTING_EVIDENCE
        : PATH_STATUS.INCOMPLETE,
      {
        selectionRule: "config_segments_anchors",
        reason: hasDgGraph
          ? dg.reason
          : "relevant_but_no_stored_generators_loaded_yet",
        count: evidence.dgCount ?? evidence.dgFitCount ?? 0,
        note: hasDgGraph ? undefined : "Empty store ≠ path irrelevant",
      }
    );
    push(
      RESEARCH_PATHS.HOTEL_GENERATOR_FIT,
      hasFits
        ? PATH_STATUS.REUSED_EXISTING_EVIDENCE
        : PATH_STATUS.INCOMPLETE,
      {
        selectionRule: "hotel_id_linked_fits",
        reason: hasFits
          ? "hotel_scoped_fits_present"
          : "relevant_but_no_hotel_fits_empty_success_or_none",
        count: evidence.dgFitCount ?? 0,
        note: "Zero hotel fits is empty_success when AT read ok — not path irrelevance",
      }
    );
    push(
      RESEARCH_PATHS.DEMAND_PROGRAMS,
      hasPrograms
        ? PATH_STATUS.REUSED_EXISTING_EVIDENCE
        : PATH_STATUS.INCOMPLETE,
      {
        selectionRule: "programs_under_generators",
        reason: hasPrograms
          ? "programs_loaded_into_reuse_ledger"
          : "generators_relevant_but_programs_not_loaded_or_empty",
        count: evidence.programCount ?? 0,
      }
    );
    push(
      RESEARCH_PATHS.DEMAND_GENERATOR_SIGNALS,
      hasSignals
        ? PATH_STATUS.REUSED_EXISTING_EVIDENCE
        : PATH_STATUS.INCOMPLETE,
      {
        selectionRule: "signals_for_fitted_generators",
        reason: hasSignals
          ? "signals_loaded_into_reuse_ledger"
          : "path_relevant_signals_not_loaded_or_empty",
        count: evidence.dgSignalCount ?? 0,
      }
    );
  }

  // Private events triad
  if (!pe.relevant) {
    for (const p of [
      RESEARCH_PATHS.PRIVATE_EVENT_VENUES,
      RESEARCH_PATHS.HOTEL_VENUE_FIT,
      RESEARCH_PATHS.PRIVATE_EVENT_SIGNALS,
    ]) {
      push(p, PATH_STATUS.NOT_APPLICABLE, {
        selectionRule: "meeting_social_positioning_and_fail_open_nearby_venues",
        reason: pe.reason,
      });
    }
  } else {
    const peStatus = (countKey, emptyReason) => {
      if (evidence.airtableBlocked && !(evidence[countKey] > 0) && !evidence.peFixtureUsed) {
        return {
          status: PATH_STATUS.BLOCKED,
          reason: evidence.airtableBlockReason || "airtable_not_configured",
        };
      }
      if (evidence[countKey] > 0 || evidence.peFixtureUsed) {
        return {
          status: PATH_STATUS.REUSED_EXISTING_EVIDENCE,
          reason: pe.reason,
          count: evidence[countKey] ?? null,
        };
      }
      return {
        status: PATH_STATUS.INCOMPLETE,
        reason: emptyReason,
        note: "Small PE corpus elsewhere does not prove this hotel path is irrelevant",
      };
    };
    const v = peStatus(
      "peVenueCount",
      "pe_relevant_venues_not_loaded_or_none_yet_for_hotel"
    );
    push(RESEARCH_PATHS.PRIVATE_EVENT_VENUES, v.status, {
      selectionRule: "meeting_social_positioning_and_fail_open_nearby_venues",
      ...v,
    });
    const f = peStatus(
      "peFitCount",
      "pe_relevant_venue_fits_not_loaded_or_none_yet"
    );
    push(RESEARCH_PATHS.HOTEL_VENUE_FIT, f.status, {
      selectionRule: "hotel_specific_venue_fit_edges",
      ...f,
    });
    const s = peStatus(
      "peSignalCount",
      "pe_relevant_signals_not_loaded_or_none_yet"
    );
    push(RESEARCH_PATHS.PRIVATE_EVENT_SIGNALS, s.status, {
      selectionRule: "signals_linked_to_venues_in_catchment",
      ...s,
    });
  }

  // Geographic expansion
  const territory = cfg?.demandTerritory?.label || cfg?.demandTerritory?.includes;
  push(
    RESEARCH_PATHS.GEOGRAPHIC_EXPANSION,
    evidence.geographicPlanned
      ? PATH_STATUS.EXECUTED
      : PATH_STATUS.INCOMPLETE,
    {
      selectionRule: "configured_territory_corridor_expansion",
      reason: evidence.geographicPlanned
        ? "geographic_expansion_plan_executed_via_coordinator_no_paid"
        : territory
          ? "territory_configured_expansion_not_yet_planned_in_this_run"
          : "territory_missing_still_incomplete_not_na",
      territoryLabel: cfg?.demandTerritory?.label || null,
      note: "Do not claim no demand from unused expansion; plan≠paid search",
    }
  );

  // Contact resolution
  push(
    RESEARCH_PATHS.CONTACT_RESOLUTION,
    evidence.contactExecuted
      ? PATH_STATUS.EXECUTED
      : evidence.contactTargets > 0
        ? PATH_STATUS.INCOMPLETE
        : PATH_STATUS.INCOMPLETE,
    {
      selectionRule: "qualified_or_enrichment_queue_with_inadequate_routes_only",
      reason: evidence.contactExecuted
        ? "targeted_contact_resolution_executed"
        : evidence.contactTargets > 0
          ? "targets_planned_execution_pending"
          : "awaiting_targeted_contact_plan",
      targetCount: evidence.contactTargets ?? 0,
    }
  );

  // Feedback-driven
  push(
    RESEARCH_PATHS.FEEDBACK_DRIVEN_RESEARCH,
    evidence.feedbackCount > 0 || evidence.feedbackDrivenHintCount > 0
      ? PATH_STATUS.REUSED_EXISTING_EVIDENCE
      : PATH_STATUS.INCOMPLETE,
    {
      selectionRule: "operator_feedback_and_share_validation_inform_next_need",
      reason:
        evidence.feedbackDrivenHintCount > 0
          ? "feedback_mapped_to_next_research_and_discovery_hints"
          : evidence.feedbackCount > 0
            ? "feedback_present_in_reuse_ledger"
            : "no_feedback_yet_path_still_open",
      count: evidence.feedbackCount ?? 0,
      hintCount: evidence.feedbackDrivenHintCount ?? 0,
    }
  );

  // Discovery providers
  if (opts.runDiscovery) {
    push(RESEARCH_PATHS.PARALLEL_DISCOVERY, PATH_STATUS.EXECUTED, {
      selectionRule: "optional_gap_fill_after_reuse",
      reason: opts.discoveryMode || "replay_or_plan",
      note: "Webhound retired — not a path",
    });
  } else {
    push(RESEARCH_PATHS.PARALLEL_DISCOVERY, PATH_STATUS.INCOMPLETE, {
      selectionRule: "optional_gap_fill_after_reuse",
      reason: "run_discovery_not_requested",
    });
  }
  push(
    RESEARCH_PATHS.NATIVE_BLIND_DISCOVERY,
    evidence.nativePlannedOrExecuted
      ? evidence.nativeMode === "executed_live"
        ? PATH_STATUS.EXECUTED
        : PATH_STATUS.EXECUTED
      : PATH_STATUS.INCOMPLETE,
    {
      selectionRule: "native_before_paid_escalation_when_gaps",
      reason: evidence.nativePlannedOrExecuted
        ? evidence.nativeMode === "executed_live"
          ? "native_live_executed"
          : "native_plan_dry_run_executed_via_coordinator_no_paid"
        : "native_not_yet_planned_in_this_run",
      mode: evidence.nativeMode || null,
      note: "Plan/dry-run is coordinator execution of the path; paid live requires --authorize-native",
    }
  );

  const summary = {};
  for (const s of Object.values(PATH_STATUS)) summary[s] = 0;
  for (const p of paths) summary[p.status] = (summary[p.status] || 0) + 1;

  return {
    version: RESEARCH_PATH_SELECTION_VERSION,
    hotelId: id,
    displayName: cfg?.displayName || null,
    peRelevance: pe,
    dgRelevance: dg,
    summary,
    paths,
    evidence: { ...evidence },
    notes: [
      "Every path is recorded — silent skips are forbidden.",
      "Not every path must produce prospects.",
      "A small existing database is not evidence a path is irrelevant.",
      "Webhound is retired and is not listed as an executable path.",
    ],
  };
}

export function researchPathSelectionToCsv(doc) {
  const header = [
    "hotelId",
    "pathId",
    "status",
    "selectionRule",
    "reason",
    "count",
    "note",
  ];
  const lines = [header.join(",")];
  const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
  for (const p of doc.paths || []) {
    lines.push(
      [
        doc.hotelId,
        p.pathId,
        p.status,
        esc(p.selectionRule),
        esc(p.reason),
        p.count ?? "",
        esc(p.note),
      ].join(",")
    );
  }
  return lines.join("\n") + "\n";
}
