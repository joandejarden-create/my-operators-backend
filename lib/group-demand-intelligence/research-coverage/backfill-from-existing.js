/**
 * Build hotel research-target registry from existing DG / PE verified entities.
 * Does not fabricate historical runs.
 */

import {
  TARGET_TYPE,
  TARGET_STATUS,
  TARGET_PRIORITY,
} from "./constants.js";
import {
  buildResearchTarget,
  mapFitPriorityToTargetPriority,
  defaultCadenceForPriority,
} from "./entities.js";

/**
 * @param {object} opts
 * @param {string} opts.hotelId
 * @param {string} [opts.hotelName]
 * @param {Array} opts.generators — Demand Generator entities/rows
 * @param {Array} opts.programs
 * @param {Array} opts.fits — Hotel Demand Generator Fit for this hotel
 * @param {Array} opts.venues — Private Event Venues (optionally pre-filtered)
 * @param {Array} [opts.venueFits] — Hotel Venue Fit rows for this hotel
 */
export function buildTargetsFromExistingIntelligence({
  hotelId,
  hotelName,
  generators = [],
  programs = [],
  fits = [],
  venues = [],
  venueFits = [],
  now = new Date(),
} = {}) {
  const iso = new Date(now).toISOString();
  const fitByGen = new Map();
  for (const f of fits) {
    if (String(f.hotelId) !== String(hotelId)) continue;
    fitByGen.set(f.demandGeneratorId, f);
  }
  const venueFitIds = new Set(
    venueFits
      .filter((f) => String(f.hotelId) === String(hotelId))
      .map((f) => f.venueId || f.peVenueId)
      .filter(Boolean)
  );

  const targets = [];
  const seen = new Set();

  function push(raw) {
    const t = buildResearchTarget({ ...raw, hotelId, hotelName, createdAt: iso, firstSeenAt: raw.firstSeenAt || iso });
    if (seen.has(t.targetId)) return;
    seen.add(t.targetId);
    targets.push(t);
  }

  for (const g of generators) {
    const fit = fitByGen.get(g.demandGeneratorId);
    // Hotel-scoped registry: only generators with a Fit row for this hotel
    if (!fit) continue;
    const priority = mapFitPriorityToTargetPriority(
      fit?.generatorPriority || fit?.priority || TARGET_PRIORITY.MEDIUM
    );
    push({
      targetType: TARGET_TYPE.DEMAND_GENERATOR,
      entityType: g.organizationType || TARGET_TYPE.DEMAND_GENERATOR,
      entityKey: g.demandGeneratorId,
      entityId: g.demandGeneratorId,
      demandGeneratorId: g.demandGeneratorId,
      organizationId: g.demandGeneratorId,
      canonicalName: g.organizationName || g.demandGeneratorId,
      officialDomain: g.officialDomain || null,
      primarySourceUrl: g.website || (Array.isArray(g.sourceUrls) ? g.sourceUrls[0] : null),
      priority,
      status: TARGET_STATUS.ACTIVE,
      reasonMonitored: fit?.fitRationale || "Verified demand generator for hotel market",
      researchCadence: defaultCadenceForPriority(priority),
      // Force due on first coverage run after backfill (no fake last research)
      nextResearchAt: iso,
      lastResearchedAt: null,
      firstSeenAt: g.firstSeenAt || iso,
      confidence: g.confidence,
      sourceFamilies: ["demand_generator"],
      airtableRecordId: g.airtableRecordId || null,
      generatorRecordId: g.airtableRecordId || null,
    });
  }

  for (const p of programs) {
    const fit = fitByGen.get(p.demandGeneratorId);
    if (!fit) continue;
    const priority = mapFitPriorityToTargetPriority(fit?.generatorPriority);
    push({
      targetType: TARGET_TYPE.PROGRAM,
      entityType: p.programType || TARGET_TYPE.PROGRAM,
      entityKey: p.programId,
      entityId: p.programId,
      programId: p.programId,
      demandGeneratorId: p.demandGeneratorId,
      seriesId: p.seriesId || null,
      canonicalName: p.programName || p.programId,
      primarySourceUrl: Array.isArray(p.sourceUrls) ? p.sourceUrls[0] : null,
      priority,
      status: TARGET_STATUS.ACTIVE,
      reasonMonitored: "Recurring demand program under verified generator",
      researchCadence: defaultCadenceForPriority(priority),
      nextResearchAt: iso,
      firstSeenAt: p.firstSeenAt || iso,
      sourceFamilies: ["demand_program"],
      programRecordId: p.airtableRecordId || null,
    });
  }

  for (const v of venues) {
    const venueId = v.venueId || v.peVenueId;
    if (!venueId) continue;
    // Hotel-scoped: only venues with a Hotel Venue Fit row for this hotel
    if (!venueFitIds.has(venueId)) continue;
    push({
      targetType: TARGET_TYPE.PRIVATE_EVENT_VENUE,
      entityType: TARGET_TYPE.PRIVATE_EVENT_VENUE,
      entityKey: venueId,
      entityId: venueId,
      venueId,
      canonicalName: v.venueName || v.name || venueId,
      officialDomain: v.officialDomain || null,
      primarySourceUrl: v.website || v.primaryUrl || null,
      priority: TARGET_PRIORITY.MEDIUM,
      status: TARGET_STATUS.ACTIVE,
      reasonMonitored: "Private event venue in hotel partnership graph",
      researchCadence: defaultCadenceForPriority(TARGET_PRIORITY.MEDIUM),
      nextResearchAt: iso,
      firstSeenAt: v.firstSeenAt || iso,
      sourceFamilies: ["private_event_venue"],
      venueRecordId: v.airtableRecordId || null,
    });
  }

  const byType = {};
  const byPriority = { HIGH: 0, MEDIUM: 0, LOW: 0 };
  for (const t of targets) {
    byType[t.targetType] = (byType[t.targetType] || 0) + 1;
    byPriority[t.priority] = (byPriority[t.priority] || 0) + 1;
  }

  return {
    hotelId,
    hotelName,
    targets,
    totals: {
      total: targets.length,
      active: targets.filter((t) => t.status === TARGET_STATUS.ACTIVE).length,
      byType,
      byPriority,
    },
  };
}
