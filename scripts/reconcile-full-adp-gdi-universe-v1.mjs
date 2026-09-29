/**
 * FULL ADP / GDI Hotel Universe Reconciliation V1
 *
 * Discovers universe dynamically from published ADP manifests (+ census links / alias map).
 * Audits HPC → HI → ADP Attributes → ADP → GDI fits/targets/runs/opps.
 * Repairs missing HI / ADP attrs / GDI seed / init Research Run (no expensive ADP re-run,
 * no broad discovery).
 *
 *   node scripts/reconcile-full-adp-gdi-universe-v1.mjs
 *   node scripts/reconcile-full-adp-gdi-universe-v1.mjs --apply
 *   node scripts/reconcile-full-adp-gdi-universe-v1.mjs --apply --hotel=adp_w_rome
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  resolveCanonicalHotelId,
  loadAdpGdiAliasMap,
  listAdpGdiHotelUniverse,
} from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { getCensusLinkEntry } from "../lib/ai-demand-positioning/census-link-registry.js";
import {
  loadHotelDemandConfig,
  isHotelOnboardedForGdi,
  buildHotelGroupDemandProfile,
} from "../lib/group-demand-intelligence/hotel-profile.js";
import { applyHotelOnboardSeed } from "../lib/group-demand-intelligence/research-coverage/onboard-hotel-research-graph.js";
import { upsertResearchRun } from "../lib/group-demand-intelligence/research-coverage/airtable-stores.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { buildGdiOpportunitySummary } from "../lib/group-demand-intelligence/opportunity-summary-v1.js";
import { applyGdiWhoHowResolution } from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";
import {
  commercialProfileDedupeKey,
  eventSpaceDedupeKey,
  demandNodeDedupeKey,
  evidenceDedupeKey,
  HOTEL_INTELLIGENCE_SCHEMA_VERSION,
} from "../lib/hotel-intelligence/schema/hotel-intelligence-schema-v1.js";
import {
  applyHotelIntelligencePacket,
  loadHotelIntelligenceFromAirtable,
} from "../lib/hotel-intelligence/schema/hi-airtable-store.js";
import { buildHotelIntelligenceProfile } from "../lib/hotel-intelligence/adp-attributes/build-hotel-intelligence-profile.js";
import { buildAdpHotelAttributes } from "../lib/hotel-intelligence/adp-attributes/build-adp-hotel-attributes.js";
import { syncHotelAdpAttributesToAirtable } from "../lib/hotel-intelligence/adp-attributes/airtable-store.js";
import { MAP_HOTEL_ADP_ATTRIBUTE as ATTR } from "../lib/hotel-intelligence/adp-attributes/field-map.js";
import { MAP_HOTEL_GENERATOR_FIT as FIT } from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";
import { MAP_RESEARCH_TARGET as TGT } from "../lib/group-demand-intelligence/research-coverage/airtable-field-map.js";
import { MAP_RESEARCH_RUN as RUN } from "../lib/group-demand-intelligence/research-coverage/airtable-field-map.js";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
  getGdiOpportunitiesAirtableBaseId,
} from "../lib/decision-outcomes/airtable-base.js";
import {
  assessSubstantiveResearchEvidence,
  classifyGdiResearchMaturity,
  isInitFootprintRun,
} from "../lib/group-demand-intelligence/research-maturity-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "adp-gdi-universe-reconciliation-v1");
const APPLY = process.argv.includes("--apply");
const hotelFilter = process.argv.find((a) => a.startsWith("--hotel="))?.slice(8) || null;

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const ALT = process.env.AIRTABLE_BASE_ID_ALT;
const intel =
  getGdiOpportunitiesAirtableBaseId() ||
  process.env.ADP_AIRTABLE_BASE_ID ||
  CANONICAL_INTELLIGENCE_BASE_ID;
assertNotLegacyMvpCanonicalBase(intel, { surface: "universe-reconciliation" });
if (intel !== CANONICAL_INTELLIGENCE_BASE_ID) throw new Error(`wrong_intel_base:${intel}`);
if (!token) throw new Error("missing_token");

const intelBase = new Airtable({ apiKey: token }).base(intel);
const hpcBase = new Airtable({ apiKey: token }).base(ALT);

function esc(s) {
  return String(s || "").replace(/"/g, '\\"');
}

function readJson(p) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/** Dynamic universe: published ADP manifests are authoritative for "current ADP hotels". */
function discoverUniverse() {
  const pubRoot = path.join(ROOT, "data/ai-demand-positioning/published");
  const dirs = fs.readdirSync(pubRoot).filter((d) => fs.existsSync(path.join(pubRoot, d, "manifest.json")));
  const aliasMap = loadAdpGdiAliasMap();
  const aliasUniverse = listAdpGdiHotelUniverse().filter((r) => r.adp);
  const byAdp = new Map();

  for (const adp of dirs) {
    const manifest = readJson(path.join(pubRoot, adp, "manifest.json"));
    const link = getCensusLinkEntry(adp);
    const hpc =
      resolveCanonicalHotelId(adp) ||
      link?.censusRecordId ||
      aliasMap.aliases?.[adp]?.canonicalHotelId ||
      null;
    const profileFile = fs
      .readdirSync(path.join(ROOT, "fixtures/ai-demand-positioning"))
      .find((f) => {
        if (!f.endsWith("-property-profile.json")) return false;
        const j = readJson(path.join(ROOT, "fixtures/ai-demand-positioning", f));
        return j?.propertyId === adp;
      });
    const profile = profileFile
      ? readJson(path.join(ROOT, "fixtures/ai-demand-positioning", profileFile))
      : null;
    byAdp.set(adp, {
      adpPropertyId: adp,
      hotelName: manifest?.propertyName || profile?.name || adp,
      hpcId: hpc,
      period: manifest?.latestPeriodId || null,
      publishStatus: manifest?.publishStatus || null,
      demandCaptureRate: manifest?.demandCaptureRate ?? null,
      providerCount: manifest?.providerCount ?? null,
      sources: ["published_manifest", link ? "census_link" : null, profile ? "property_profile" : null].filter(Boolean),
      active: String(manifest?.publishStatus || "").toLowerCase() === "live",
      profilePath: profileFile || null,
      gdiConfigPresent: hpc ? isHotelOnboardedForGdi(hpc) : false,
    });
  }

  // Flag alias ADP entries not in published set
  const orphanAliases = [];
  for (const row of aliasUniverse) {
    if (row.adpPropertyId && !byAdp.has(row.adpPropertyId)) {
      orphanAliases.push(row);
    }
  }

  let hotels = [...byAdp.values()].sort((a, b) => a.hotelName.localeCompare(b.hotelName));
  if (hotelFilter) {
    hotels = hotels.filter(
      (h) => h.adpPropertyId === hotelFilter || h.hpcId === hotelFilter || h.hotelName.toLowerCase().includes(hotelFilter.toLowerCase())
    );
  }
  return { hotels, orphanAliases, publishedCount: dirs.length, aliasAdpCount: aliasUniverse.length };
}

async function listByField(table, field, value, fields = null) {
  const rows = [];
  const opts = { pageSize: 100, filterByFormula: `{${field}} = "${esc(value)}"` };
  if (fields) opts.fields = fields;
  await intelBase(table)
    .select(opts)
    .eachPage((recs, next) => {
      for (const r of recs) rows.push({ id: r.id, fields: r.fields || {}, createdTime: r._rawJson?.createdTime });
      next();
    });
  return rows;
}

async function loadHpc(hpcId) {
  if (!hpcId) return null;
  try {
    const r = await hpcBase("Hotel Property Census").find(hpcId);
    const f = r.fields || {};
    return {
      id: r.id,
      name: f["Property Name"] || f["Canonical Property Name"],
      brand: f["Current Brand"] || f["Brand Family"],
      city: f.City,
      country: f.Country,
      rooms: f["Rooms / Keys"],
      website: f["Official Property URL"],
      owner: f["Owner Name"] || null,
      operator: f["Operator / Management Company"] || null,
      address: f.Address || null,
    };
  } catch (e) {
    return { id: hpcId, error: String(e.message || e) };
  }
}

function inferDemandNodeType(name = "") {
  const n = String(name).toLowerCase();
  if (/airport|aeropuerto/.test(n)) return "Airport";
  if (/hospital|medical|nih|health|clinic/.test(n)) return "Medical / Healthcare";
  if (/university|college|edu/.test(n)) return "University";
  if (/convention|expo|conference|palacio/.test(n)) return "Convention / Conference";
  if (/government|federal|capitol|congress/.test(n)) return "Government";
  if (/military|walter reed/.test(n)) return "Military";
  if (/port|marina|yacht/.test(n)) return "Other";
  if (/corporate|employer|hq|business/.test(n)) return "Corporate";
  if (/sport|stadium|arena|coliseum/.test(n)) return "Sports";
  if (/tourism|beach|resort destination/.test(n)) return "Tourism";
  return "Other";
}

async function ensureHiAndAttrs(hotel, hiLoaded) {
  const repairs = [];
  const needsHi = !hiLoaded.commercial?.airtableRecordId;
  const needsAttrs = (await listByField("Hotel ADP Attributes", ATTR.hpcHotelId, hotel.hpcId, [ATTR.active])).filter(
    (r) => r.fields[ATTR.active] === true
  ).length === 0;

  if (!needsHi && !needsAttrs) {
    return { repairs, hiApply: null, attrSync: null };
  }

  const profile = await buildHotelIntelligenceProfile(hotel.hpcId);
  const ms = profile.commercialProfile?.meetingSpace || {};
  const commercial = {
    profileKey: commercialProfileDedupeKey(hotel.hpcId),
    hpcHotelId: hotel.hpcId,
    dealalityHotelId: profile.identity?.dealalityHotelId || null,
    adpPropertyId: hotel.adpPropertyId,
    hotelName: hotel.hotelName,
    brand: profile.identity?.brand || null,
    roomsKeys: profile.identity?.roomsKeys ?? profile.commercialProfile?.rooms ?? null,
    officialPropertyUrl: profile.identity?.officialPropertyUrl || null,
    officialEventsUrl: profile.commercialProfile?.eventsUrl || null,
    meetingSpaceFlag: Boolean(ms.totalSqFt || ms.meetingRooms),
    totalMeetingSpaceSqFt: ms.totalSqFt ?? null,
    meetingRoomCount: ms.meetingRooms ?? null,
    largestMeetingSpaceSqFt: ms.largestRoom?.sqFt ?? null,
    largestEventCapacity: ms.largestRoom?.capacity ?? null,
    researchStatus: "Partial",
    researchVersion: "universe_reconciliation_v1",
    confidence: ms.totalSqFt != null ? "HIGH" : "MEDIUM",
    lastResearchedAt: new Date().toISOString(),
    schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
    notes: "Universe reconciliation HI upsert from HPC + HI profile sources",
  };

  const eventSpaces = [];
  if (ms.largestRoom?.name) {
    eventSpaces.push({
      spaceKey: eventSpaceDedupeKey(hotel.hpcId, ms.largestRoom.name),
      hpcHotelId: hotel.hpcId,
      spaceName: ms.largestRoom.name,
      spaceType: "Ballroom",
      sqFt: ms.largestRoom.sqFt ?? null,
      theaterCapacity: ms.largestRoom.capacity ?? null,
      sourceUrl: ms.source || commercial.officialEventsUrl,
      confidence: ms.confidence || "MEDIUM",
      active: true,
      lastVerifiedAt: new Date().toISOString(),
      schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
    });
  }

  const demandNodes = (profile.demandNodes || []).map((n) => ({
    nodeKey: demandNodeDedupeKey(hotel.hpcId, n.name, n.type || inferDemandNodeType(n.name)),
    hpcHotelId: hotel.hpcId,
    demandNodeName: n.name,
    demandNodeType: n.type || inferDemandNodeType(n.name),
    sourceType: n.sourceType || "Hotel Supplied",
    sourceName: n.sourceName || "GDI hotel demand config / prior profile",
    confidence: n.confidence || "MEDIUM",
    active: true,
    lastVerifiedAt: new Date().toISOString(),
    schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
  }));

  const evidence = [];
  if (commercial.officialPropertyUrl) {
    evidence.push({
      evidenceId: evidenceDedupeKey(hotel.hpcId, "Commercial Profile", "Official Property URL", commercial.officialPropertyUrl, commercial.officialPropertyUrl),
      hpcHotelId: hotel.hpcId,
      entityType: "Commercial Profile",
      fieldName: "Official Property URL",
      valueObserved: commercial.officialPropertyUrl,
      sourceName: "Official property URL",
      sourceType: "Public Research",
      sourceUrl: commercial.officialPropertyUrl,
      confidence: "HIGH",
      current: true,
      schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
    });
  }
  if (commercial.roomsKeys != null) {
    evidence.push({
      evidenceId: evidenceDedupeKey(hotel.hpcId, "Commercial Profile", "Rooms / Keys", "HPC", commercial.roomsKeys),
      hpcHotelId: hotel.hpcId,
      entityType: "Commercial Profile",
      fieldName: "Rooms / Keys",
      valueObserved: String(commercial.roomsKeys),
      sourceName: "HPC",
      sourceType: "HPC",
      confidence: "HIGH",
      current: true,
      schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
    });
  }

  let hiApply = null;
  if (needsHi) {
    repairs.push("MISSING_HI");
    if (APPLY) {
      hiApply = await applyHotelIntelligencePacket(
        { commercial, eventSpaces, demandNodes, seasonality: [], evidence },
        { dryRun: false }
      );
    } else {
      hiApply = { dryRun: true, wouldWrite: true };
    }
  }

  const rebuiltProfile = await buildHotelIntelligenceProfile(hotel.hpcId);
  const attrs = await buildAdpHotelAttributes(hotel.hpcId, { profile: rebuiltProfile });
  let attrSync = null;
  if (needsAttrs || needsHi) {
    if (needsAttrs) repairs.push("MISSING_ADP_ATTR");
    if (APPLY) {
      attrSync = await syncHotelAdpAttributesToAirtable(attrs, { dryRun: false });
    } else {
      attrSync = { dryRun: true, wouldCreate: attrs.counts?.total };
    }
  }

  return { repairs, hiApply, attrSync, attrCounts: attrs.counts };
}

async function ensureGdi(hotel, gdiState) {
  const repairs = [];
  let seedResult = null;
  let runResult = null;

  if (!hotel.gdiConfigPresent) {
    repairs.push("MISSING_GDI_CONFIG");
    return { repairs, seedResult, runResult, blocked: "no_gdi_config" };
  }

  if (gdiState.fits === 0 || gdiState.targets === 0) {
    if (gdiState.fits === 0) repairs.push("MISSING_GDI_FITS");
    if (gdiState.targets === 0) repairs.push("MISSING_GDI_TARGETS");
    if (APPLY) {
      seedResult = await applyHotelOnboardSeed(hotel.hpcId, { dryRun: false });
    } else {
      seedResult = await applyHotelOnboardSeed(hotel.hpcId, { dryRun: true });
    }
  }

  const runsAfter = APPLY && (gdiState.fits === 0 || gdiState.targets === 0)
    ? (await listByField("GDI Research Runs", RUN.hotelId, hotel.hpcId, [RUN.runId])).length
    : gdiState.runs;

  if ((gdiState.runs === 0 && !(APPLY && seedResult)) || (APPLY && runsAfter === 0 && gdiState.runs === 0)) {
    // After seed, check runs again
  }

  let runCount = gdiState.runs;
  if (APPLY && (gdiState.fits === 0 || gdiState.targets === 0)) {
    runCount = (await listByField("GDI Research Runs", RUN.hotelId, hotel.hpcId, [RUN.runId])).length;
  }

  if (runCount === 0) {
    repairs.push("MISSING_GDI_RUN");
    const runId = `grun_universe_init_${hotel.hpcId}_${new Date().toISOString().replace(/[^0-9]/g, "").slice(0, 14)}`;
    if (APPLY) {
      runResult = await upsertResearchRun(
        {
          runId,
          hotelId: hotel.hpcId,
          hotelName: hotel.hotelName,
          runType: "MANUAL",
          status: "COMPLETED",
          startedAt: new Date().toISOString(),
          completedAt: new Date().toISOString(),
          notes: "Universe reconciliation — initialization footprint (seed/init; not a live discovery cycle)",
          payload: { source: "universe_reconciliation_v1", kind: "INIT_FOOTPRINT" },
        },
        { dryRun: false }
      );
    } else {
      runResult = { dryRun: true, runId };
    }
  }

  return { repairs, seedResult, runResult };
}

async function auditHotel(hotel) {
  const hpc = await loadHpc(hotel.hpcId);
  let hi = { commercial: null, eventSpaces: [], demandNodes: [], seasonality: [], needPeriods: [], evidence: [] };
  try {
    hi = await loadHotelIntelligenceFromAirtable(hotel.hpcId);
  } catch (e) {
    hi = { error: String(e.message || e) };
  }

  const attrs = hotel.hpcId
    ? await listByField("Hotel ADP Attributes", ATTR.hpcHotelId, hotel.hpcId, [ATTR.dedupeKey, ATTR.active, ATTR.attributeName])
    : [];
  const activeAttrs = attrs.filter((r) => r.fields[ATTR.active] === true);
  const byKey = new Map();
  for (const r of activeAttrs) {
    const k = r.fields[ATTR.dedupeKey] || "";
    if (!k) continue;
    byKey.set(k, (byKey.get(k) || 0) + 1);
  }
  const multiActive = [...byKey.values()].filter((c) => c > 1).length;

  const fits = hotel.hpcId
    ? await listByField("Hotel Demand Generator Fit", FIT.hotelId, hotel.hpcId, [FIT.fitId, FIT.demandGeneratorId])
    : [];
  const targets = hotel.hpcId
    ? await listByField("GDI Research Targets", TGT.hotelId, hotel.hpcId, [TGT.targetId])
    : [];
  const runs = hotel.hpcId
    ? await listByField("GDI Research Runs", RUN.hotelId, hotel.hpcId, [
        RUN.runId,
        RUN.notes,
        RUN.queries,
        RUN.fetches,
        RUN.targetsAttempted,
        RUN.targetsCompleted,
        RUN.newSignals,
        RUN.newOpportunities,
        RUN.payloadJson,
        RUN.status,
        RUN.runType,
      ])
    : [];
  let targetRuns = [];
  try {
    targetRuns = hotel.hpcId
      ? await listByField("GDI Research Target Runs", "Hotel ID", hotel.hpcId, [
          "Target Run ID",
          "Target ID",
          "Execution Status",
          "Queries Used",
          "Fetches Used",
          "Source Count",
        ])
      : [];
  } catch {
    targetRuns = [];
  }

  let oppsDoc = { opportunities: [], persistence: null };
  try {
    oppsDoc = await loadOpportunitiesCanonical(hotel.hpcId);
  } catch {
    oppsDoc = { opportunities: [], persistence: "error" };
  }
  const customer = filterCustomerFacingOpportunities(oppsDoc.opportunities || []);
  const summaryQuality = { STRONG: 0, ADEQUATE: 0, THIN: 0, INVALID: 0, other: 0 };
  const whoStates = {};
  let notResearchedPromoted = 0;
  for (const o of customer) {
    const s = buildGdiOpportunitySummary(o);
    const q = String(s.quality || s.summaryQuality || "other").toUpperCase();
    if (summaryQuality[q] != null) summaryQuality[q] += 1;
    else summaryQuality.other += 1;
    const who = applyGdiWhoHowResolution(o);
    const st = who.whoState || who.contactGrade || who.resolution || "UNKNOWN";
    whoStates[st] = (whoStates[st] || 0) + 1;
    if (/NOT_RESEARCHED/i.test(String(st))) notResearchedPromoted += 1;
  }

  const completeness = {
    identity: Boolean(hotel.hpcId && !hpc?.error),
    commercial: Boolean(hi.commercial?.airtableRecordId),
    meeting: Boolean(hi.commercial?.totalMeetingSpaceSqFt || hi.commercial?.meetingRoomCount || (hi.eventSpaces || []).length),
    demandNodes: (hi.demandNodes || []).length > 0,
    eventSpaces: (hi.eventSpaces || []).length > 0,
    seasonality: (hi.seasonality || []).length > 0,
    needPeriods: (hi.needPeriods || []).length > 0,
  };
  // evidence counted separately for founder visibility; score uses 7 checks aligned prior reports
  const scoreParts = {
    identity: completeness.identity,
    commercial: completeness.commercial,
    meeting: completeness.meeting,
    demandNodes: completeness.demandNodes,
    eventSpaces: completeness.eventSpaces,
    seasonality: completeness.seasonality,
    needPeriods: completeness.needPeriods,
  };
  const score = Object.values(scoreParts).filter(Boolean).length;

  let adpClass = "MISSING_BASELINE";
  if (hotel.period && hotel.publishStatus === "Live") adpClass = "CERTIFIED_CURRENT";
  else if (hotel.period) adpClass = "CERTIFIED_STALE";
  else if (hotel.profilePath) adpClass = "PROFILE_ONLY";

  // Research maturity: INIT_FOOTPRINT / zero-activity runs ≠ researched.
  const normRuns = runs.map((r) => {
    const f = r.fields || {};
    let payload = null;
    try {
      payload = f[RUN.payloadJson] ? JSON.parse(f[RUN.payloadJson]) : null;
    } catch {
      payload = null;
    }
    return {
      runId: f[RUN.runId],
      notes: f[RUN.notes],
      queries: f[RUN.queries] || 0,
      fetches: f[RUN.fetches] || 0,
      targetsAttempted: f[RUN.targetsAttempted] || 0,
      targetsCompleted: f[RUN.targetsCompleted] || 0,
      newSignals: f[RUN.newSignals] || 0,
      newOpportunities: f[RUN.newOpportunities] || 0,
      payload,
      payloadJson: f[RUN.payloadJson],
    };
  });
  const evidence = assessSubstantiveResearchEvidence({
    runs: normRuns,
    targetRuns: targetRuns.map((r) => ({
      targetId: r.fields?.["Target ID"],
      executionStatus: r.fields?.["Execution Status"],
      queriesUsed: r.fields?.["Queries Used"] || 0,
      fetchesUsed: r.fields?.["Fetches Used"] || 0,
      sourceCount: r.fields?.["Source Count"] || 0,
    })),
    customerReady: customer.length,
  });
  const maturityClass = classifyGdiResearchMaturity({
    totalTargets: targets.length,
    targetsResearched: evidence.researchedTargetIds.length,
    customerReady: customer.length,
    evidence,
  });

  let gdiClass = "GDI_MISSING";
  if (customer.length > 0) gdiClass = "GDI_READY";
  else if (maturityClass.maturity === "RESEARCHED_NO_READY") gdiClass = "GDI_RESEARCHED_NO_READY";
  else if (maturityClass.maturity === "PARTIALLY_RESEARCHED") gdiClass = "GDI_PARTIAL";
  else if (maturityClass.maturity === "INITIALIZED_ONLY") gdiClass = "GDI_INITIALIZED";
  else if (fits.length > 0 || targets.length > 0) gdiClass = "GDI_PARTIAL";
  else if (runs.length > 0 && normRuns.every((r) => isInitFootprintRun(r))) gdiClass = "GDI_INITIALIZED";

  let zeroOppReason = null;
  if (customer.length === 0) {
    if (maturityClass.maturity === "INITIALIZED_ONLY" || (fits.length === 0 && targets.length === 0)) {
      zeroOppReason = "INITIALIZED_NOT_RESEARCHED";
    } else if (maturityClass.maturity === "PARTIALLY_RESEARCHED") {
      zeroOppReason = "INSUFFICIENT_RESEARCH_COVERAGE";
    } else if (maturityClass.maturity === "RESEARCHED_NO_READY") {
      zeroOppReason = "NO_READY_OPPORTUNITIES";
    } else if (runs.length === 0) {
      zeroOppReason = "INITIALIZED_NOT_RESEARCHED";
    } else {
      zeroOppReason = "NO_READY_OPPORTUNITIES";
    }
  }

  const before = {
    hpc: hpc,
    hi: {
      commercialId: hi.commercial?.airtableRecordId || null,
      eventSpaces: (hi.eventSpaces || []).length,
      demandNodes: (hi.demandNodes || []).length,
      seasonality: (hi.seasonality || []).length,
      needPeriods: (hi.needPeriods || []).length,
      evidence: (hi.evidence || []).length,
      completeness: `${score}/7`,
      score,
    },
    adpAttrs: {
      total: attrs.length,
      active: activeAttrs.length,
      multiActiveSameKey: multiActive,
    },
    adp: { class: adpClass, period: hotel.period, publishStatus: hotel.publishStatus },
    gdi: {
      class: gdiClass,
      maturity: maturityClass.maturity,
      systemState: maturityClass.systemState,
      maturityRationale: maturityClass.rationale,
      evidenceStrength: evidence.strength,
      initRuns: normRuns.filter((r) => isInitFootprintRun(r)).length,
      fits: fits.length,
      uniqueFits: new Set(fits.map((r) => r.fields[FIT.fitId] || r.id)).size,
      targets: targets.length,
      uniqueTargets: new Set(targets.map((r) => r.fields[TGT.targetId] || r.id)).size,
      runs: runs.length,
      targetRuns: targetRuns.length,
      opportunities: (oppsDoc.opportunities || []).length,
      customerReady: customer.length,
      persistence: oppsDoc.persistence,
      zeroOppReason,
      summaryQuality,
      whoStates,
      notResearchedPromoted,
    },
  };

  const missing = [];
  if (!hotel.hpcId || hpc?.error) missing.push("MISSING_HPC");
  if (!before.hi.commercialId) missing.push("MISSING_HI");
  if (before.adpAttrs.active === 0) missing.push("MISSING_ADP_ATTR");
  if (!hotel.period) missing.push("MISSING_ADP_BINDING");
  if (before.gdi.fits === 0) missing.push("MISSING_GDI_FITS");
  if (before.gdi.targets === 0) missing.push("MISSING_GDI_TARGETS");
  if (before.gdi.runs === 0) missing.push("MISSING_GDI_RUN");
  if (multiActive > 0) missing.push("DUPLICATE_ACTIVE_DATA");

  return { before, missing, fits, targets, runs, hi };
}

async function processHotel(hotel) {
  console.error(`[universe] ${hotel.hotelName} (${hotel.adpPropertyId})`);
  const audit = await auditHotel(hotel);
  const fixed = [];
  let hiRepair = null;
  let gdiRepair = null;

  if (hotel.hpcId && !audit.before.hpc?.error) {
    hiRepair = await ensureHiAndAttrs(hotel, audit.hi);
    fixed.push(...(hiRepair.repairs || []));
    gdiRepair = await ensureGdi(hotel, {
      fits: audit.before.gdi.fits,
      targets: audit.before.gdi.targets,
      runs: audit.before.gdi.runs,
    });
    fixed.push(...(gdiRepair.repairs || []));
  }

  // Re-audit after apply
  const after = APPLY ? (await auditHotel(hotel)).before : audit.before;
  const stillMissing = [];
  if (!after.hpc || after.hpc.error) stillMissing.push("MISSING_HPC");
  if (!after.hi.commercialId) stillMissing.push("MISSING_HI");
  if (after.adpAttrs.active === 0) stillMissing.push("MISSING_ADP_ATTR");
  if (!hotel.period) stillMissing.push("MISSING_ADP_BINDING");
  if (after.gdi.fits === 0) stillMissing.push("MISSING_GDI_FITS");
  if (after.gdi.targets === 0) stillMissing.push("MISSING_GDI_TARGETS");
  if (after.gdi.runs === 0) stillMissing.push("MISSING_GDI_RUN");

  const gdiInfraPass = after.gdi.fits > 0 && after.gdi.targets > 0 && after.gdi.runs > 0;
  const finalStatus =
    after.hpc &&
    !after.hpc.error &&
    after.hi.commercialId &&
    after.adpAttrs.active > 0 &&
    hotel.period &&
    gdiInfraPass
      ? after.gdi.customerReady > 0
        ? "COMPLETE_WITH_READY_OPPS"
        : "COMPLETE_ZERO_READY_OPPS"
      : "INCOMPLETE";

  return {
    hotel,
    before: audit.before,
    missingBefore: audit.missing,
    fixed: [...new Set(fixed)],
    stillMissing,
    after,
    hiRepair: hiRepair
      ? { repairs: hiRepair.repairs, attrSync: hiRepair.attrSync, commercialId: hiRepair.hiApply?.commercial?.id }
      : null,
    gdiRepair: gdiRepair
      ? {
          repairs: gdiRepair.repairs,
          seedFits: gdiRepair.seedResult?.writeStats?.fits || null,
          seedTargets: gdiRepair.seedResult?.writeStats?.targets || null,
          runId: gdiRepair.runResult?.recordId || gdiRepair.runResult?.entity?.airtableRecordId || gdiRepair.runResult?.runId || null,
        }
      : null,
    finalStatus,
    gdiInfraPass,
  };
}

const discovered = discoverUniverse();
fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(
  path.join(OUT_DIR, "FULL_ADP_HOTEL_UNIVERSE.json"),
  JSON.stringify({ generatedAt: new Date().toISOString(), apply: APPLY, bases: { intel, hpc: ALT, forbidden: LEGACY_DEAL_CAPTURE_MVP_BASE_ID }, ...discovered }, null, 2) + "\n"
);

const results = [];
for (const hotel of discovered.hotels) {
  results.push(await processHotel(hotel));
}

const summary = {
  generatedAt: new Date().toISOString(),
  apply: APPLY,
  totalActiveAdp: discovered.hotels.filter((h) => h.active).length,
  totalPublished: discovered.hotels.length,
  hpcLinked: results.filter((r) => r.after.hpc && !r.after.hpc.error).length,
  hiWithCommercial: results.filter((r) => r.after.hi.commercialId).length,
  adpAttrActive: results.filter((r) => r.after.adpAttrs.active > 0).length,
  adpCertified: results.filter((r) => r.after.adp.class === "CERTIFIED_CURRENT").length,
  gdiInitialized: results.filter((r) => r.gdiInfraPass).length,
  gdiMissingBefore: results.filter((r) => r.missingBefore.some((m) => /GDI/.test(m))).length,
  gdiMissingAfter: results.filter((r) => r.stillMissing.some((m) => /GDI/.test(m))).length,
  customerReadyAtLeast1: results.filter((r) => r.after.gdi.customerReady > 0).length,
  zeroReadyValid: results.filter((r) => r.gdiInfraPass && r.after.gdi.customerReady === 0).length,
  incomplete: results.filter((r) => r.finalStatus === "INCOMPLETE").map((r) => ({
    hotel: r.hotel.hotelName,
    adp: r.hotel.adpPropertyId,
    stillMissing: r.stillMissing,
  })),
};

fs.writeFileSync(path.join(OUT_DIR, `RESULTS-${APPLY ? "apply" : "dry"}.json`), JSON.stringify({ summary, results }, null, 2) + "\n");
fs.writeFileSync(path.join(OUT_DIR, `SUMMARY-${APPLY ? "apply" : "dry"}.json`), JSON.stringify(summary, null, 2) + "\n");

console.log(JSON.stringify(summary, null, 2));
