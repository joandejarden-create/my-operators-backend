/**
 * Generic hotel GDI onboarding — Fit + Research Target seed from canonical hotel inputs.
 *
 * Does NOT:
 * - run Bethesda pilot seeds / DMV deepen
 * - invent weekly NEW opportunities from seed alone
 * - use hotel-name switches (Renaissance / Bethesda / Times Square / New York)
 *
 * Deterministic: same hotel profile → same generator IDs / fit IDs / target IDs.
 */

import {
  buildHotelGroupDemandProfile,
  loadHotelDemandConfig,
  isHotelOnboardedForGdi,
} from "../hotel-profile.js";
import { resolveCanonicalHotelId } from "../../hotel-census/adp-gdi-canonical-identity.js";
import { isPilotReferenceLogicEnabled } from "../pilot-path-policy.js";
import {
  buildDemandGeneratorEntity,
  buildProgramEntity,
  buildHotelDemandGeneratorFit,
  upsertDemandGenerator,
  upsertDemandProgram,
  upsertHotelGeneratorFit,
} from "../demand-generators/index.js";
import { buildTargetsFromExistingIntelligence } from "./backfill-from-existing.js";
import { upsertResearchTarget } from "./airtable-stores.js";
import {
  selectPortableTemplatesForHotel,
  textHasPilotGeoBleed,
  PORTABLE_GENERATOR_SEED_VERSION,
} from "./portable-seed-templates.js";
import { TARGET_STATUS } from "./constants.js";
import { resolveGdiCanonicalBaseId } from "../canonical-airtable-base.js";

export const ONBOARD_SEED_VERSION = "gdi_hotel_onboard_seed_v1";

function clean(s) {
  return String(s || "").trim();
}

/**
 * Build generic seed input from canonical hotel record + GDI config.
 */
export function buildGenericHotelSeedInput(hotelId, opts = {}) {
  const requested = clean(hotelId);
  if (!requested) {
    const err = new Error("hotelId_required");
    err.code = "hotelId_required";
    throw err;
  }
  // Prefer durable HPC id when provisional/ADP aliases resolve — prevents duplicate Fit/Target IDs.
  const resolved =
    opts.forceHotelId ||
    resolveCanonicalHotelId(requested) ||
    requested;
  const id = clean(resolved);
  if (!isHotelOnboardedForGdi(id) && !isHotelOnboardedForGdi(requested) && !opts.allowMissingConfig) {
    const err = new Error("hotel_not_onboarded_for_gdi");
    err.code = "hotel_not_onboarded_for_gdi";
    throw err;
  }

  const config =
    loadHotelDemandConfig(id) || loadHotelDemandConfig(requested) || {};
  const profile =
    opts.profile ||
    buildHotelGroupDemandProfile(id) ||
    buildHotelGroupDemandProfile(requested);
  const capability = config.capabilityProfile || profile.capabilityProfile || {};
  const territory = config.demandTerritory || profile.demandTerritory || {};
  const commercial = config.commercialPriorities || {};

  return {
    hotelId: id,
    hotelName:
      clean(config.displayName) ||
      clean(profile.hotelName) ||
      clean(profile.displayName) ||
      id,
    brand: clean(capability.softBrand) || clean(profile.brand) || null,
    address: clean(profile.address) || null,
    city: clean(config.city) || clean(profile.city) || null,
    stateRegion: clean(profile.state) || clean(profile.stateRegion) || null,
    country:
      clean(config.country) ||
      clean(profile.country) ||
      clean(capability.country) ||
      clean(territory.country) ||
      null,
    latitude: profile.latitude ?? config.geo?.latitude ?? null,
    longitude: profile.longitude ?? config.geo?.longitude ?? null,
    rooms: Number(capability.totalGuestrooms || profile.rooms || 0) || null,
    meetingSpaceSqFt:
      Number(capability.totalMeetingSpaceSqFt || profile.meetingSpaceSqFt || 0) ||
      null,
    serviceLevel: clean(capability.serviceLevel) || null,
    hotelArchetype: clean(capability.classification) || null,
    officialWebsite: clean(profile.officialWebsite) || clean(profile.website) || null,
    brandPage: clean(profile.brandPage) || null,
    market: clean(territory.label) || clean(profile.market) || null,
    catchmentIncludes: Array.isArray(territory.includes) ? territory.includes : [],
    productCapabilities: {
      largestBallroomSqFt: capability.largestBallroomSqFt ?? null,
      largestTheaterCapacity: capability.largestTheaterCapacity ?? null,
      meetingRoomsIndoor: capability.meetingRoomsIndoor ?? null,
      outdoorEventSpace: capability.outdoorEventSpace ?? null,
      coreTargetPeakRoomsMin: commercial.coreTargetPeakRoomsMin ?? null,
      coreTargetPeakRoomsMax: commercial.coreTargetPeakRoomsMax ?? null,
    },
    gdiOnboarding: config.gdiOnboarding || {},
    config,
    profile,
  };
}

function rejectBleed(entity, hotelId) {
  const blob = [
    entity.organizationName,
    entity.headquartersLocation,
    ...(entity.primaryMarkets || []),
    ...(entity.sourceUrls || []),
    entity.website,
    entity.officialDomain,
    entity.fitRationale,
    entity.programName,
    entity.market,
  ]
    .filter(Boolean)
    .join(" | ");
  if (!isPilotReferenceLogicEnabled(hotelId) && textHasPilotGeoBleed(blob)) {
    return {
      rejected: true,
      reason: "pilot_geo_bleed",
      organizationName: entity.organizationName || entity.programName,
    };
  }
  return { rejected: false };
}

/**
 * Propose generators + fits + programs from portable templates + hotel input.
 * Pure / deterministic — no Airtable I/O.
 */
export function proposeHotelOnboardSeed(hotelId, opts = {}) {
  const input = opts.input || buildGenericHotelSeedInput(hotelId, opts);
  const now = opts.now ? new Date(opts.now) : new Date();
  const iso = now.toISOString();

  if (isPilotReferenceLogicEnabled(hotelId, opts) && opts.forbidPilotPath) {
    const err = new Error("pilot_path_forbidden_for_generic_seed");
    err.code = "pilot_path_forbidden_for_generic_seed";
    throw err;
  }

  const { archetype, templates, geoScopes } = selectPortableTemplatesForHotel({
    capabilityProfile: input.config?.capabilityProfile || {},
    config: input.config,
    country: input.country,
  });

  const marketLabel = input.market || "hotel market";
  const generators = [];
  const programs = [];
  const fits = [];
  const rejected = [];

  for (const tpl of templates) {
    const genRaw = {
      organizationName: tpl.organizationName,
      organizationAliases: tpl.organizationAliases,
      organizationType: tpl.organizationType,
      officialDomain: tpl.officialDomain,
      website: tpl.website,
      headquartersLocation: tpl.headquartersLocation,
      primaryMarkets: [marketLabel, ...(tpl.primaryMarkets || [])],
      industry: tpl.industry,
      generatorStatus: tpl.generatorStatus,
      sourceAuthority: tpl.sourceAuthority,
      sourceUrls: tpl.sourceUrls,
      firstSeenAt: iso,
      lastSeenAt: iso,
      researchReason: `generic_onboard_seed:${ONBOARD_SEED_VERSION}`,
      payload: {
        seedProvenance: {
          version: ONBOARD_SEED_VERSION,
          templateVersion: PORTABLE_GENERATOR_SEED_VERSION,
          hotelId: input.hotelId,
          marketLabel,
          archetype,
          generatedAt: iso,
          fitBasis: "portable_national_template_x_hotel_market",
        },
      },
    };
    const bleed = rejectBleed(genRaw, input.hotelId);
    if (bleed.rejected) {
      rejected.push(bleed);
      continue;
    }
    const generator = buildDemandGeneratorEntity(genRaw);
    generators.push(generator);

    for (const p of tpl.programs || []) {
      const progRaw = {
        demandGeneratorId: generator.demandGeneratorId,
        programName: p.programName,
        programType: p.programType,
        market: marketLabel,
        recurrenceStatus: p.recurrenceStatus,
        recurrenceFrequency: p.recurrenceFrequency,
        sourceUrls: p.sourceUrls,
        firstSeenAt: iso,
        lastSeenAt: iso,
      };
      const pBleed = rejectBleed(progRaw, input.hotelId);
      if (pBleed.rejected) {
        rejected.push(pBleed);
        continue;
      }
      programs.push(buildProgramEntity(progRaw));
    }

    const factors = {
      ...(tpl.fitFactors || {}),
      marketRelevance: tpl.fitFactors?.marketRelevance || "COMPETITIVE",
    };
    const fit = buildHotelDemandGeneratorFit({
      hotelId: input.hotelId,
      hotelName: input.hotelName,
      demandGeneratorId: generator.demandGeneratorId,
      organizationName: generator.organizationName,
      factors,
      productFit:
        input.meetingSpaceSqFt && input.meetingSpaceSqFt < 8000 ? "MEDIUM" : "HIGH",
      fitRationale: [
        `generic_seed market=${marketLabel}`,
        `archetype=${archetype}`,
        `rooms=${input.rooms || "unknown"}`,
        `meetingSqFt=${input.meetingSpaceSqFt || "unknown"}`,
        `org=${generator.organizationName}`,
        `basis=portable_national_template`,
        `version=${ONBOARD_SEED_VERSION}`,
      ].join("; "),
      firstEvaluatedAt: iso,
      lastEvaluatedAt: iso,
    });
    fits.push({
      ...fit,
      seedProvenance: {
        version: ONBOARD_SEED_VERSION,
        hotelId: input.hotelId,
        demandGeneratorId: generator.demandGeneratorId,
        fitBasis: "portable_national_template_x_hotel_capability",
        source: "generic_onboard_seed",
        generatedAt: iso,
      },
    });
  }

  const targetBundle = buildTargetsFromExistingIntelligence({
    hotelId: input.hotelId,
    hotelName: input.hotelName,
    generators,
    programs,
    fits,
    venues: [],
    venueFits: [],
    now,
  });

  // Baseline semantics: ACTIVE monitoring targets — not weekly NEW opportunities
  const targets = targetBundle.targets.map((t) => ({
    ...t,
    status: t.status || TARGET_STATUS.ACTIVE,
    seedProvenance: {
      version: ONBOARD_SEED_VERSION,
      generatedAt: iso,
      baseline: true,
      createsWeeklyNew: false,
      createsOpportunity: false,
    },
  }));

  const byType = {};
  const byPriority = { HIGH: 0, MEDIUM: 0, LOW: 0 };
  for (const t of targets) {
    byType[t.targetType] = (byType[t.targetType] || 0) + 1;
    byPriority[t.priority] = (byPriority[t.priority] || 0) + 1;
  }

  return {
    ok: true,
    version: ONBOARD_SEED_VERSION,
    hotelId: input.hotelId,
    hotelName: input.hotelName,
    input: {
      hotelId: input.hotelId,
      hotelName: input.hotelName,
      brand: input.brand,
      market: input.market,
      rooms: input.rooms,
      meetingSpaceSqFt: input.meetingSpaceSqFt,
      serviceLevel: input.serviceLevel,
      hotelArchetype: input.hotelArchetype,
      catchmentIncludes: input.catchmentIncludes,
    },
    archetype,
    geoScopes: geoScopes || null,
    pilotLogicEnabled: isPilotReferenceLogicEnabled(input.hotelId, opts),
    generators,
    programs,
    fits,
    targets,
    rejected,
    totals: {
      generators: generators.length,
      programs: programs.length,
      fits: fits.length,
      targets: targets.length,
      rejected: rejected.length,
      byType,
      byPriority,
    },
    quality: {
      baselineCreatesWeeklyNew: false,
      baselineCreatesOpportunity: false,
      hotelNameSwitches: false,
      pilotBleedRejected: rejected.filter((r) => r.reason === "pilot_geo_bleed").length,
    },
  };
}

/**
 * Compare two seed proposals for recreate proof (deterministic overlap).
 */
export function compareOnboardSeedProposals(first, second) {
  const fitIds1 = new Set((first.fits || []).map((f) => f.fitId));
  const fitIds2 = new Set((second.fits || []).map((f) => f.fitId));
  const targetIds1 = new Set((first.targets || []).map((t) => t.targetId));
  const targetIds2 = new Set((second.targets || []).map((t) => t.targetId));

  const fitOverlap = [...fitIds1].filter((id) => fitIds2.has(id)).length;
  const targetOverlap = [...targetIds1].filter((id) => targetIds2.has(id)).length;
  const fitDenom = Math.max(fitIds1.size, fitIds2.size, 1);
  const targetDenom = Math.max(targetIds1.size, targetIds2.size, 1);

  const materialFitDiffs = [
    ...[...fitIds1].filter((id) => !fitIds2.has(id)).map((id) => ({ side: "first_only", fitId: id })),
    ...[...fitIds2].filter((id) => !fitIds1.has(id)).map((id) => ({ side: "second_only", fitId: id })),
  ];
  const materialTargetDiffs = [
    ...[...targetIds1]
      .filter((id) => !targetIds2.has(id))
      .map((id) => ({ side: "first_only", targetId: id })),
    ...[...targetIds2]
      .filter((id) => !targetIds1.has(id))
      .map((id) => ({ side: "second_only", targetId: id })),
  ];

  return {
    fitFirst: fitIds1.size,
    fitRecreated: fitIds2.size,
    fitOverlapCount: fitOverlap,
    fitOverlapPct: Math.round((1000 * fitOverlap) / fitDenom) / 10,
    targetFirst: targetIds1.size,
    targetRecreated: targetIds2.size,
    targetOverlapCount: targetOverlap,
    targetOverlapPct: Math.round((1000 * targetOverlap) / targetDenom) / 10,
    materialFitDiffs,
    materialTargetDiffs,
    deterministic:
      fitIds1.size === fitIds2.size &&
      targetIds1.size === targetIds2.size &&
      materialFitDiffs.length === 0 &&
      materialTargetDiffs.length === 0,
    manualHotelSpecificInjection: false,
  };
}

/**
 * Apply proposed seed to Airtable (or dry-run).
 */
export async function applyHotelOnboardSeed(hotelId, opts = {}) {
  const dryRun = opts.dryRun !== false;
  const started = Date.now();
  let baseMeta = null;
  try {
    baseMeta = resolveGdiCanonicalBaseId();
  } catch (err) {
    if (!dryRun) throw err;
    baseMeta = { status: "MISSING", error: err.code || err.message };
  }

  const proposal = opts.proposal || proposeHotelOnboardSeed(hotelId, opts);
  const writeStats = {
    generators: { create: 0, update: 0, preview: 0 },
    programs: { create: 0, update: 0, preview: 0 },
    fits: { create: 0, update: 0, preview: 0 },
    targets: { create: 0, update: 0, preview: 0 },
  };
  const results = { generators: [], programs: [], fits: [], targets: [] };
  const genRecordById = new Map();

  for (const g of proposal.generators) {
    const res = await upsertDemandGenerator(g, { dryRun });
    results.generators.push(res);
    if (dryRun) writeStats.generators.preview += 1;
    else if (res.action === "CREATE") writeStats.generators.create += 1;
    else writeStats.generators.update += 1;
    if (res.recordId) genRecordById.set(g.demandGeneratorId, res.recordId);
  }

  for (const p of proposal.programs) {
    const res = await upsertDemandProgram(p, {
      dryRun,
      generatorRecordId: genRecordById.get(p.demandGeneratorId),
    });
    results.programs.push(res);
    if (dryRun) writeStats.programs.preview += 1;
    else if (res.action === "CREATE") writeStats.programs.create += 1;
    else writeStats.programs.update += 1;
  }

  for (const f of proposal.fits) {
    const res = await upsertHotelGeneratorFit(f, {
      dryRun,
      generatorRecordId: genRecordById.get(f.demandGeneratorId),
    });
    results.fits.push(res);
    if (dryRun) writeStats.fits.preview += 1;
    else if (res.action === "CREATE") writeStats.fits.create += 1;
    else writeStats.fits.update += 1;
  }

  for (const t of proposal.targets) {
    const res = await upsertResearchTarget(t, { dryRun });
    results.targets.push(res);
    if (dryRun) writeStats.targets.preview += 1;
    else if (res.action === "CREATE") writeStats.targets.create += 1;
    else writeStats.targets.update += 1;
  }

  return {
    ok: true,
    dryRun,
    hotelId: proposal.hotelId,
    hotelName: proposal.hotelName,
    base: baseMeta,
    proposal,
    writeStats,
    results,
    performance: {
      runtimeMs: Date.now() - started,
      airtableWrites: dryRun
        ? 0
        : writeStats.generators.create +
          writeStats.generators.update +
          writeStats.programs.create +
          writeStats.programs.update +
          writeStats.fits.create +
          writeStats.fits.update +
          writeStats.targets.create +
          writeStats.targets.update,
      airtableReadsEstimate: dryRun
        ? 0
        : proposal.generators.length +
          proposal.programs.length +
          proposal.fits.length +
          proposal.targets.length,
    },
  };
}

/**
 * Structural weekly readiness without running live discovery.
 */
export function assessWeeklyReadinessFromSeed(proposal) {
  const blockers = [];
  if (!proposal?.hotelId) blockers.push("missing_hotel_id");
  if (!proposal?.fits?.length) blockers.push("no_hotel_generator_fits");
  if (!proposal?.targets?.length) blockers.push("no_research_targets");
  if (proposal?.pilotLogicEnabled && proposal?.quality?.hotelNameSwitches) {
    blockers.push("pilot_logic_unexpected");
  }
  for (const t of proposal?.targets || []) {
    if (!t.targetId) blockers.push("target_missing_targetId");
    if (!t.targetType) blockers.push("target_missing_targetType");
    if (!t.hotelId || t.hotelId !== proposal.hotelId) blockers.push("target_hotel_scope_mismatch");
    if (!t.priority) blockers.push("target_missing_priority");
    if (!t.status) blockers.push("target_missing_status");
  }
  const uniqueBlockers = [...new Set(blockers)];
  return {
    status: uniqueBlockers.length ? "WEEKLY_BLOCKED" : "WEEKLY_READY",
    blockers: uniqueBlockers,
    targetCount: proposal?.targets?.length || 0,
    fitCount: proposal?.fits?.length || 0,
    note: uniqueBlockers.length
      ? "Seed incomplete for weekly orchestrator consumption"
      : "Seeded targets are structurally consumable by weekly discovery (no live run executed)",
  };
}
