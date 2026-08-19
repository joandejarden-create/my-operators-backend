/**
 * Adaptive overnight phase engine — Mode A structured + Mode B live research.
 * Structured plateau must NOT stop the worker.
 */
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { createLiveHotelPropertyCensusAdapter } from "./census-autopilot-batch-engine.js";
import { MAP_MASTER } from "./master-census-enrichment-v1.js";
import { MAP_BRAND } from "./master-brand-portfolio-validation-v1.js";
import { MAP_ROOMS } from "./production-census-rooms-keys-queue.js";
import { repairBrandMappingGaps } from "./brand-mapping-gap-repair-v1.js";
import { buildCanonicalBrandDictionary } from "./census-brand-canonical-dictionary.js";
import { runLiveOfficialDirectoryAcquisition } from "./live-official-directory-acquisition-v1.js";
import { runApifyFirstPartyAcquisition } from "./apify-first-party-acquisition-v1.js";
import { runRoomsCandidateCorroboration } from "./rooms-candidate-corroboration-v1.js";
import {
  runPropertyOutwardDomainIntelligence,
  runPropertyOutwardOfficialPages,
  runPropertyOutwardIndependent,
  runPropertyOutwardCandidateRouting,
  runPropertyOutwardWebsiteDiscovery,
  buildPropertyOutwardEnrichmentStatus,
} from "./property-outward-brand-enrichment-v1.js";
import { computeBrandResolutionMetrics } from "./brand-resolution-metrics-v1.js";
import {
  runResidualPropertyResearch,
  runOfficialFactSheetDiscovery,
} from "./residual-property-research-v1.js";
import { evaluateCoordinateCompletionEligibility } from "./census-coordinate-completion.js";
import { topYieldSources } from "./source-acquisition-registry-v1.js";
import { researchPropertyPage } from "./property-fundamentals-enrichment-v1.js";

export const ADAPTIVE_ENGINE_VERSION = "adaptive-overnight-engine-v1";

export const ADAPTIVE_PHASES = Object.freeze([
  { id: "phase_0_deterministic", mode: "A", lane: "Brand mapping + geography" },
  { id: "phase_1_structured", mode: "A", lane: "Known structured sources" },
  {
    id: "phase_1b_apify_first_party",
    mode: "B",
    lane: "Approved Apify first-party extractors (harvest only)",
  },
  {
    id: "phase_2_property_outward_domain",
    mode: "B",
    lane: "Website domain intelligence routing",
  },
  {
    id: "phase_2a_property_outward_pages",
    mode: "B",
    lane: "Official property page brand + fundamentals",
  },
  {
    id: "phase_2b_property_outward_independent",
    mode: "B",
    lane: "Validated independent classification",
  },
  {
    id: "phase_2c_candidate_brand_routing",
    mode: "B",
    lane: "Candidate brand routing (no self-validation)",
  },
  {
    id: "phase_2d_website_discovery",
    mode: "B",
    lane: "Website discovery for identity-anchored properties",
  },
  {
    id: "phase_2e_demand_adapters",
    mode: "B",
    lane: "Demand-ranked official company adapters",
  },
  { id: "phase_3_rooms_corroboration", mode: "B", lane: "Property-outward rooms corroboration" },
  { id: "phase_4_pdf_factsheet", mode: "B", lane: "Official fact sheets" },
  { id: "phase_5_residual", mode: "B", lane: "Residual multi-field research" },
  { id: "phase_6_newly_eligible_coords", mode: "B", lane: "Newly eligible coordinates" },
  { id: "phase_7_measure", mode: "B", lane: "Brand resolution metrics + re-rank" },
]);

/** Migrate legacy overnight checkpoints to property-outward phase IDs. */
export function migrateAdaptivePhaseStatus(phaseStatus = {}) {
  const next = { ...phaseStatus };
  if (next.phase_2_live_directories && !next.phase_2e_demand_adapters) {
    next.phase_2e_demand_adapters = next.phase_2_live_directories;
    delete next.phase_2_live_directories;
  }
  for (const p of ADAPTIVE_PHASES) {
    if (next[p.id] == null) next[p.id] = "READY";
  }
  return next;
}

export const RESEARCH_MODE_PHASE_IDS = Object.freeze([
  "phase_1_structured",
  "phase_1b_apify_first_party",
  "phase_2_property_outward_domain",
  "phase_2a_property_outward_pages",
  "phase_2b_property_outward_independent",
  "phase_2c_candidate_brand_routing",
  "phase_2d_website_discovery",
  "phase_2e_demand_adapters",
  "phase_3_rooms_corroboration",
  "phase_4_pdf_factsheet",
  "phase_5_residual",
  "phase_6_newly_eligible_coords",
]);

const READ_FIELDS = [
  MAP_MASTER.propertyName,
  MAP_MASTER.canonicalName,
  MAP_MASTER.country,
  MAP_MASTER.stateRegion,
  MAP_MASTER.city,
  MAP_MASTER.address,
  MAP_MASTER.postalCode,
  MAP_MASTER.latitude,
  MAP_MASTER.longitude,
  MAP_MASTER.currentBrand,
  MAP_MASTER.brandFamily,
  MAP_MASTER.familySourceFamily,
  MAP_MASTER.officialUrl,
  MAP_MASTER.phone,
  MAP_MASTER.roomsKeys,
  MAP_BRAND.candidateBrand,
  "Affiliation Status",
  MAP_ROOMS.confidenceExisting,
  MAP_ROOMS.sourceUrlExisting,
];

export function allAvailableResearchModesExhausted(phaseStatus = {}) {
  const migrated = migrateAdaptivePhaseStatus(phaseStatus);
  return RESEARCH_MODE_PHASE_IDS.every((id) => {
    const st = migrated[id];
    return st === "EXHAUSTED" || st === "PLATEAUED";
  });
}

async function listCensusRecords(baseId, token, fields) {
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    }
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return records;
}

function isBlank(v) {
  return v == null || String(v).trim() === "";
}

export function applyNullFillToRecords(records, proposals) {
  const byId = new Map(records.map((r) => [r.id, r]));
  const applied = [];
  for (const p of proposals || []) {
    const rec = byId.get(p.id);
    if (!rec) continue;
    const fields = { ...(p.fields || {}) };
    for (const [k, v] of Object.entries(fields)) {
      if (k === MAP_MASTER.lastReviewed || k === MAP_MASTER.enrichmentStatus) continue;
      if (!isBlank(rec.fields?.[k])) delete fields[k];
    }
    if (!Object.keys(fields).length) continue;
    applied.push({ id: p.id, fields });
    Object.assign(rec.fields, fields);
  }
  return applied;
}

export async function flushAdaptivePatches(proposals, opts = {}) {
  const enableWrites = opts.enableProductionWrites === true;
  if (!proposals?.length) return { updated: 0, WRONG_TABLE_WRITES: 0 };
  if (!enableWrites) return { updated: 0, dry_run: true, WRONG_TABLE_WRITES: 0 };
  const token = opts.token || resolvePat();
  const base = resolveTargetBase();
  const baseId = opts.baseId || base?.target_base_id || base?.baseId;
  const adapter = createLiveHotelPropertyCensusAdapter({
    token,
    baseId,
    tableId: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  });
  const res = await adapter.patchRecords(proposals);
  return {
    updated: res.updated || 0,
    WRONG_TABLE_WRITES: res.blocked_wrong_census_target ? 1 : 0,
    errors: res.errors?.length || 0,
  };
}

async function loadCensus(opts) {
  if (opts.loadCensusFn) return opts.loadCensusFn();
  if (opts.isolated) return [];
  const token = resolvePat();
  const base = resolveTargetBase();
  const baseId = base?.target_base_id || base?.baseId;
  return listCensusRecords(baseId, token, READ_FIELDS);
}

function emptyPhaseResult(id, extra = {}) {
  return {
    ok: true,
    phase: id,
    proposals: [],
    fields_written: 0,
    properties_patched: 0,
    exhausted: false,
    ...extra,
  };
}

/**
 * Run a single adaptive phase. Isolated tests inject fn overrides.
 */
export async function runAdaptivePhase(phase, ctx = {}) {
  const log = ctx.log || (() => {});
  const isolated = Boolean(ctx.isolated);
  const hasOverride =
    (phase.id === "phase_0_deterministic" && ctx.runMappingRepairFn) ||
    (phase.id === "phase_1_structured" && ctx.runMasterFn) ||
    (phase.id === "phase_1b_apify_first_party" && ctx.runApifyFirstPartyFn) ||
    (phase.id === "phase_2_property_outward_domain" && ctx.runPropertyOutwardDomainFn) ||
    (phase.id === "phase_2a_property_outward_pages" && ctx.runPropertyOutwardPagesFn) ||
    (phase.id === "phase_2b_property_outward_independent" && ctx.runPropertyOutwardIndependentFn) ||
    (phase.id === "phase_2c_candidate_brand_routing" && ctx.runPropertyOutwardCandidateFn) ||
    (phase.id === "phase_2d_website_discovery" && ctx.runPropertyOutwardDiscoveryFn) ||
    (phase.id === "phase_2e_demand_adapters" && ctx.runLiveDirectoryFn) ||
    (phase.id === "phase_3_rooms_corroboration" && ctx.runRoomsCorroborationFn) ||
    (phase.id === "phase_4_pdf_factsheet" && ctx.runFactSheetFn) ||
    (phase.id === "phase_5_residual" && ctx.runResidualFn) ||
    (phase.id === "phase_6_newly_eligible_coords" && ctx.runMasterFn) ||
    Boolean(ctx[`run_${phase.id}`]);
  if (isolated && !hasOverride) {
    return emptyPhaseResult(phase.id, {
      skipped: "isolated_test_stub",
      exhausted: false,
      mode: phase.mode,
    });
  }

  if (phase.id === "phase_0_deterministic") {
    const records = await loadCensus(ctx);
    const dictionary = buildCanonicalBrandDictionary({});
    const repair = (ctx.runMappingRepairFn || repairBrandMappingGaps)(records, {
      dictionary,
    });
    const applied = applyNullFillToRecords(records, repair.proposals);
    const flush = await flushAdaptivePatches(applied, ctx);
    return {
      ok: true,
      phase: phase.id,
      mode: "A",
      proposals: applied,
      properties_patched: applied.length,
      fields_written: applied.reduce((n, p) => n + Object.keys(p.fields).length, 0),
      BRAND_MAPPING_REPAIRS: repair.repairs,
      BRAND_MAPPING_GAPS_BEFORE: repair.remaining_gaps + repair.repairs,
      BRAND_MAPPING_GAPS_FIXED: repair.repairs,
      BRAND_MAPPING_GAPS_AFTER: repair.remaining_gaps,
      remaining_gap_samples: repair.remaining_gap_samples,
      WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
      exhausted: repair.repairs === 0,
    };
  }

  if (phase.id === "phase_1_structured") {
    const runMaster = ctx.runMasterFn;
    if (!runMaster) return emptyPhaseResult(phase.id, { exhausted: true, mode: "A" });
    const report = await runMaster(ctx.masterOpts || {});
    return {
      ok: report?.ok !== false,
      phase: phase.id,
      mode: "A",
      master_report: report,
      properties_patched: Number(report?.PROPERTIES_PATCHED_THIS_RUN || 0),
      fields_written: Number(report?.TOTAL_FIELDS_WRITTEN_THIS_RUN || 0),
      CURRENT_BRAND_WRITES: Number(report?.CURRENT_BRAND_WRITES || 0),
      ROOMS_WRITTEN: Number(report?.ROOMS_WRITTEN || 0),
      WRONG_TABLE_WRITES: Number(report?.WRONG_TABLE_WRITES || 0),
      DESTRUCTIVE_OVERWRITES: Number(report?.DESTRUCTIVE_OVERWRITES || 0),
      exhausted:
        Number(report?.CURRENT_BRAND_WRITES || 0) === 0 &&
        Number(report?.ROOMS_WRITTEN || 0) === 0 &&
        Number(report?.TOTAL_FIELDS_WRITTEN_THIS_RUN || 0) === 0,
    };
  }

  if (phase.id === "phase_1b_apify_first_party") {
    const records = await loadCensus(ctx);
    const apifyFn = ctx.runApifyFirstPartyFn || runApifyFirstPartyAcquisition;
    const apify = await apifyFn({
      censusRecords: records,
      log,
      allowSample: ctx.allowApifySample === true,
      packs: ctx.apifyPacks,
      dictionary: ctx.dictionary,
    });
    const applied = applyNullFillToRecords(records, apify.proposals);
    const flush = await flushAdaptivePatches(applied, ctx);
    return {
      ok: apify.ok !== false,
      phase: phase.id,
      mode: "B",
      proposals: applied,
      properties_patched: applied.length,
      fields_written: applied.reduce((n, p) => n + Object.keys(p.fields).length, 0),
      CURRENT_BRAND_WRITES: apify.CURRENT_BRAND_WRITES || 0,
      BRAND_FAMILY_DERIVATIONS: apify.BRAND_FAMILY_DERIVATIONS || 0,
      ROOMS_WRITTEN: apify.ROOMS_WRITES || 0,
      ROOMS_HIGH_FROM_FIRST_PARTY: apify.ROOMS_WRITES || 0,
      ROOM_CANDIDATES_CORROBORATED: apify.ROOM_CANDIDATES_CORROBORATED || 0,
      ADDRESS_PATCHES: apify.ADDRESS_PATCHES || 0,
      POSTAL_PATCHES: apify.POSTAL_PATCHES || 0,
      STATE_PATCHES: apify.STATE_PATCHES || 0,
      CITY_PATCHES: apify.CITY_PATCHES || 0,
      COORDINATES_WRITTEN: apify.COORDINATE_PATCHES || 0,
      PHONE_PATCHES: apify.PHONE_PATCHES || 0,
      WEBSITE_PATCHES: apify.WEBSITE_PATCHES || 0,
      CALA_PROPERTIES_MATCHED_HIGH: apify.CALA_PROPERTIES_MATCHED_HIGH || 0,
      APIFY_COST: apify.APIFY_COST || 0,
      WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
      exhausted: apify.exhausted === true,
    };
  }

  if (phase.id === "phase_2_property_outward_domain") {
    const records = await loadCensus(ctx);
    const domainFn = ctx.runPropertyOutwardDomainFn || runPropertyOutwardDomainIntelligence;
    const domain = domainFn({
      censusRecords: records,
      dictionary: ctx.dictionary,
      log,
    });
    const applied = applyNullFillToRecords(records, domain.proposals);
    const flush = await flushAdaptivePatches(applied, ctx);
    return {
      ok: domain.ok !== false,
      phase: phase.id,
      mode: "B",
      proposals: applied,
      properties_patched: applied.length,
      fields_written: applied.reduce((n, p) => n + Object.keys(p.fields).length, 0),
      WEBSITE_DOMAIN_PROPERTIES_ANALYZED: domain.WEBSITE_DOMAIN_PROPERTIES_ANALYZED || 0,
      OFFICIAL_GROUP_DOMAINS_IDENTIFIED: domain.OFFICIAL_GROUP_DOMAINS_IDENTIFIED || 0,
      TOP_20_UNRESOLVED_BRAND_COMPANY_DEMAND: domain.TOP_20_UNRESOLVED_BRAND_COMPANY_DEMAND || [],
      WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
      exhausted: domain.exhausted === true,
    };
  }

  if (phase.id === "phase_2a_property_outward_pages") {
    const records = await loadCensus(ctx);
    const pagesFn = ctx.runPropertyOutwardPagesFn || runPropertyOutwardOfficialPages;
    const pages = await pagesFn({
      censusRecords: records,
      log,
      maxProperties: ctx.maxPropertyOutwardPages || 24,
      fetchFn: ctx.fetchFn,
      researchFn: ctx.researchFn,
      dictionary: ctx.dictionary,
    });
    const applied = applyNullFillToRecords(records, pages.proposals);
    const flush = await flushAdaptivePatches(applied, ctx);
    return {
      ok: pages.ok !== false,
      phase: phase.id,
      mode: "B",
      proposals: applied,
      properties_patched: applied.length,
      fields_written: applied.reduce((n, p) => n + Object.keys(p.fields).length, 0),
      CURRENT_BRAND_WRITES: pages.CURRENT_BRAND_WRITES || 0,
      ROOMS_WRITTEN: pages.ROOMS_WRITES || 0,
      ADDRESS_PATCHES: pages.ADDRESS_WRITES || 0,
      POSTAL_PATCHES: pages.POSTAL_WRITES || 0,
      STATE_PATCHES: pages.STATE_WRITES || 0,
      CITY_PATCHES: pages.CITY_WRITES || 0,
      COORDINATES_WRITTEN: pages.COORDINATE_WRITES || 0,
      PHONE_PATCHES: pages.PHONE_WRITES || 0,
      WEBSITE_PATCHES: pages.WEBSITE_WRITES || 0,
      PROPERTY_WEBSITES_ANALYZED: pages.PROPERTY_WEBSITES_ANALYZED || 0,
      TOTAL_PROPERTIES_RESEARCHED: pages.TOTAL_PROPERTIES_RESEARCHED || 0,
      WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
      exhausted: pages.exhausted === true,
    };
  }

  if (phase.id === "phase_2b_property_outward_independent") {
    const records = await loadCensus(ctx);
    const indFn = ctx.runPropertyOutwardIndependentFn || runPropertyOutwardIndependent;
    const ind = await indFn({
      censusRecords: records,
      log,
      maxProperties: ctx.maxPropertyOutwardIndependent || 12,
      fetchFn: ctx.fetchFn,
    });
    const applied = applyNullFillToRecords(records, ind.proposals);
    const flush = await flushAdaptivePatches(applied, ctx);
    return {
      ok: ind.ok !== false,
      phase: phase.id,
      mode: "B",
      proposals: applied,
      properties_patched: applied.length,
      fields_written: applied.reduce((n, p) => n + Object.keys(p.fields).length, 0),
      INDEPENDENT_VALIDATED: ind.INDEPENDENT_VALIDATED || 0,
      WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
      exhausted: ind.exhausted === true,
    };
  }

  if (phase.id === "phase_2c_candidate_brand_routing") {
    const records = await loadCensus(ctx);
    const candFn = ctx.runPropertyOutwardCandidateFn || runPropertyOutwardCandidateRouting;
    const cand = await candFn({
      censusRecords: records,
      log,
      maxProperties: ctx.maxCandidateRouting || 20,
      fetchFn: ctx.fetchFn,
      researchFn: ctx.researchFn,
      dictionary: ctx.dictionary,
    });
    const applied = applyNullFillToRecords(records, cand.proposals);
    const flush = await flushAdaptivePatches(applied, ctx);
    return {
      ok: cand.ok !== false,
      phase: phase.id,
      mode: "B",
      proposals: applied,
      properties_patched: applied.length,
      fields_written: applied.reduce((n, p) => n + Object.keys(p.fields).length, 0),
      CURRENT_BRAND_WRITES: cand.CURRENT_BRAND_WRITES || 0,
      PROPERTY_WEBSITES_ANALYZED: cand.PROPERTY_WEBSITES_ANALYZED || 0,
      WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
      exhausted: cand.exhausted === true,
    };
  }

  if (phase.id === "phase_2d_website_discovery") {
    const records = await loadCensus(ctx);
    const discFn = ctx.runPropertyOutwardDiscoveryFn || runPropertyOutwardWebsiteDiscovery;
    const disc = discFn({
      censusRecords: records,
      log,
      maxProperties: ctx.maxWebsiteDiscovery || 30,
    });
    const applied = applyNullFillToRecords(records, disc.proposals);
    const flush = await flushAdaptivePatches(applied, ctx);
    return {
      ok: disc.ok !== false,
      phase: phase.id,
      mode: "B",
      proposals: applied,
      properties_patched: applied.length,
      fields_written: applied.reduce((n, p) => n + Object.keys(p.fields).length, 0),
      WEBSITE_PATCHES: disc.WEBSITE_WRITES || 0,
      WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
      exhausted: disc.exhausted === true,
    };
  }

  if (phase.id === "phase_2e_demand_adapters") {
    const records = await loadCensus(ctx);
    const liveFn = ctx.runLiveDirectoryFn || runLiveOfficialDirectoryAcquisition;
    const live = await liveFn({
      censusRecords: records,
      log,
      maxCompanies: ctx.maxCompanies || 2,
      delayMs: ctx.delayMs,
      fetchFn: ctx.fetchFn,
      sleepFn: ctx.sleepFn,
      demandRanked: ctx.demandRanked !== false,
    });
    const applied = applyNullFillToRecords(records, live.proposals);
    const flush = await flushAdaptivePatches(applied, ctx);
    return {
      ok: live.ok !== false,
      phase: phase.id,
      mode: "B",
      proposals: applied,
      properties_patched: applied.length,
      fields_written: applied.reduce((n, p) => n + Object.keys(p.fields).length, 0),
      LIVE_OFFICIAL_DOMAINS_DISCOVERED: live.LIVE_OFFICIAL_DOMAINS_DISCOVERED || 0,
      LIVE_OFFICIAL_DOMAINS_CRAWLED: live.LIVE_OFFICIAL_DOMAINS_CRAWLED || 0,
      NEW_SOURCE_REGISTRY_ENTRIES: live.NEW_SOURCE_REGISTRY_ENTRIES || 0,
      requests: live.requests || 0,
      WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
      exhausted: live.exhausted === true,
      registry: live.registry,
    };
  }

  if (phase.id === "phase_3_rooms_corroboration") {
    const records = await loadCensus(ctx);
    const roomsFn = ctx.runRoomsCorroborationFn || runRoomsCandidateCorroboration;
    const rooms = await roomsFn({
      censusRecords: records,
      log,
      maxProperties: ctx.maxRoomsCorr || 40,
      fetchPageFn: ctx.fetchPageFn || (async (url, rec) => {
        const r = await researchPropertyPage(rec);
        return { html: r.extract?.html || "", url };
      }),
    });
    const applied = applyNullFillToRecords(records, rooms.proposals);
    const flush = await flushAdaptivePatches(applied, ctx);
    return {
      ok: rooms.ok !== false,
      phase: phase.id,
      mode: "B",
      proposals: applied,
      properties_patched: applied.length,
      fields_written: applied.reduce((n, p) => n + Object.keys(p.fields).length, 0),
      ROOM_CANDIDATES_BEFORE: rooms.ROOM_CANDIDATES_BEFORE,
      ROOM_CANDIDATES_CORROBORATED: rooms.ROOM_CANDIDATES_CORROBORATED,
      ROOM_CANDIDATES_REMAINING: rooms.ROOM_CANDIDATES_REMAINING,
      ROOMS_HIGH_FROM_FIRST_PARTY: rooms.ROOMS_HIGH_FROM_FIRST_PARTY,
      ROOMS_HIGH_FROM_TWO_SOURCE_CORROBORATION:
        rooms.ROOMS_HIGH_FROM_TWO_SOURCE_CORROBORATION,
      WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
      exhausted: rooms.exhausted === true,
    };
  }

  if (phase.id === "phase_4_pdf_factsheet") {
    const records = await loadCensus(ctx);
    const pdfFn = ctx.runFactSheetFn || runOfficialFactSheetDiscovery;
    const pdf = await pdfFn({
      censusRecords: records,
      log,
      maxProperties: ctx.maxFactSheets || 16,
      fetchFn: ctx.fetchFn,
    });
    const applied = applyNullFillToRecords(records, pdf.proposals);
    const flush = await flushAdaptivePatches(applied, ctx);
    return {
      ok: true,
      phase: phase.id,
      mode: "B",
      proposals: applied,
      properties_patched: applied.length,
      fields_written: applied.reduce((n, p) => n + Object.keys(p.fields).length, 0),
      WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
      exhausted: pdf.exhausted === true,
    };
  }

  if (phase.id === "phase_5_residual") {
    const records = await loadCensus(ctx);
    const residualFn = ctx.runResidualFn || runResidualPropertyResearch;
    const residual = await residualFn({
      censusRecords: records,
      log,
      maxProperties: ctx.maxResidual || 24,
      researchFn: ctx.researchFn,
    });
    const applied = applyNullFillToRecords(records, residual.proposals);
    const flush = await flushAdaptivePatches(applied, ctx);
    return {
      ok: true,
      phase: phase.id,
      mode: "B",
      proposals: applied,
      properties_patched: applied.length,
      fields_written: applied.reduce((n, p) => n + Object.keys(p.fields).length, 0),
      RESIDUAL_PROPERTIES_RESEARCHED: residual.RESIDUAL_PROPERTIES_RESEARCHED,
      RESIDUAL_PROPERTIES_WITH_2_PLUS_FIELDS_FILLED:
        residual.RESIDUAL_PROPERTIES_WITH_2_PLUS_FIELDS_FILLED,
      NEWLY_ELIGIBLE_COORDINATES: residual.NEWLY_ELIGIBLE_COORDINATES,
      newly_eligible_coordinate_ids: residual.newly_eligible_coordinate_ids,
      WRONG_TABLE_WRITES: flush.WRONG_TABLE_WRITES || 0,
      exhausted: residual.exhausted === true,
    };
  }

  if (phase.id === "phase_6_newly_eligible_coords") {
    const runMaster = ctx.runMasterFn;
    if (!runMaster) {
      return emptyPhaseResult(phase.id, { mode: "B", exhausted: true });
    }
    const report = await runMaster({
      ...(ctx.masterOpts || {}),
      forceBrandRoomsWave: true,
      skipBrandPortfolio: true,
      skipRoomsRegistry: true,
      skipPropertyFundamentals: false,
      skipCoordinates: false,
      continueMapboxWave: false,
      maxOpportunisticCoordinateRequests: 200,
      maxPfResearch: 20,
    });
    return {
      ok: report?.ok !== false,
      phase: phase.id,
      mode: "B",
      master_report: report,
      properties_patched: Number(report?.PROPERTIES_PATCHED_THIS_RUN || 0),
      fields_written: Number(report?.TOTAL_FIELDS_WRITTEN_THIS_RUN || 0),
      COORDINATES_WRITTEN: Number(
        report?.ADDITIONAL_COORDINATES_WRITTEN || report?.COORDINATES_WRITTEN || 0
      ),
      MAPBOX_REQUESTS: Number(report?.MAPBOX_REQUESTS || 0),
      ESTIMATED_MAPBOX_COST: Number(report?.ESTIMATED_MAPBOX_COST || 0),
      exhausted:
        Number(report?.ADDITIONAL_COORDINATES_WRITTEN || report?.COORDINATES_WRITTEN || 0) === 0,
    };
  }

  // phase_7 measure — brand resolution rate is the primary metric
  const records = ctx.isolated ? [] : await loadCensus(ctx);
  const resolution = computeBrandResolutionMetrics(records);
  let newlyEligible = 0;
  for (const rec of records) {
    const elig = evaluateCoordinateCompletionEligibility(rec, {
      masterFounderApprovedPathway: true,
    });
    if (elig.eligible) newlyEligible += 1;
  }
  const propertyOutward = buildPropertyOutwardEnrichmentStatus(
    ctx.aggregates || {},
    records,
    {
      TOP_20_SOURCE_YIELDS: topYieldSources(ctx.registry || { sources: {} }, 20),
      BRAND_MAPPING_GAPS_AFTER: ctx.brandMappingGapsAfter ?? null,
    }
  );
  return {
    ok: true,
    phase: phase.id,
    mode: "B",
    ...resolution,
    ...propertyOutward,
    VALIDATED_BRANDED_COUNT: resolution.BRANDED_VALIDATED,
    BRAND_UNRESOLVED_COUNT: resolution.BRAND_UNRESOLVED,
    NEWLY_ELIGIBLE_COORDINATES: newlyEligible,
    exhausted: false,
    top_yield_sources: topYieldSources(ctx.registry || { sources: {} }, 20),
  };
}

void researchPropertyPage;
