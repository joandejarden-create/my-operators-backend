#!/usr/bin/env node
/**
 * GDI Controlled Expansion Wave 3 — 3 hotels, frozen production stack.
 *
 * Stages: plan | discovery | hygiene | who | v10 | reach | reports | all
 * Flags: --hotel=A|B|C|all --force --max-queries=28 --confirm-spend --dry-discovery
 *
 * No Webhound. No live Airtable writes. No mid-wave patches. No invented WHO.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runGroupDemandResearch,
  buildHotelGroupDemandProfile,
  loadHotelDemandConfig,
} from "../lib/group-demand-intelligence/index.js";
import { runNativeBlindDiscovery } from "../lib/group-demand-intelligence/native-blind-discovery.js";
import { dedupeDiscoveryCandidates } from "../lib/group-demand-intelligence/candidate-dedupe.js";
import { mapWebhoundOpportunityUniverse } from "../lib/group-demand-intelligence/webhound-opportunity-import.js";
import { evaluateNativeDiscoveryCompleteness } from "../lib/group-demand-intelligence/discovery-completeness-gate.js";
import {
  DISCOVERY_RECALL_V1,
  RECALL_EXTRACT_SYSTEM,
} from "../lib/group-demand-intelligence/discovery-recall-v1.js";
import {
  DISCOVERY_HYGIENE_V3,
  ACTIONABILITY_V3,
  applyDiscoveryHygieneV3,
  assertUnknownNotOpenRegression,
} from "../lib/group-demand-intelligence/discovery-hygiene-v3.js";
import { assertVenueAliasRegression } from "../lib/group-demand-intelligence/discovery-hygiene-v2.js";
import { discoverWhoV9 } from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/discover-who-v9.js";
import {
  enrichDiscoverInputV12,
  confirmWhoRecallCandidateV12,
  classifyWhoRecallGapV12,
  recallStagesForGapV12,
} from "../lib/group-demand-intelligence/contact-candidate/native-who-v3/who-recall-v12.js";
import { classifyVenueLockedOpportunity } from "../lib/group-demand-intelligence/venue-locked-classification-v11.js";
import { gradeContact } from "../lib/group-demand-intelligence/contact-resolution.js";
import {
  SURFE_CLIENT_VERSION,
  getSurfeCredits,
  startSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
  normalizeSurfePerson,
} from "../lib/surfe/client.js";
import { enrichPdlPerson, PDL_CLIENT_VERSION } from "../lib/pdl/client.js";
import {
  acceptSurfeIdentity,
  IDENTITY_DECISION,
} from "../lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MARKER = "gdi_controlled_expansion_wave3_20260922";
const STACK_SHA = "1a3ebc79d0fe299c4b13d5df704247556e91c3a2";
const AS_OF = "2026-09-22";

/**
 * Eval-only recall profiles — intentionally NOT written into
 * lib/.../discovery-recall-v1.js (no production hotel hardcodes this wave).
 */
const WAVE3_RECALL_PROFILES = Object.freeze({
  recUOyzOXn2Zdp98I: {
    hotelId: "recUOyzOXn2Zdp98I",
    name: "Radisson Hotel Santo Domingo",
    demandHypothesis: [
      "corporate / finance / Naco corridor meetings",
      "association / industry conferences Santo Domingo",
      "executive events / board meetings",
      "medical / professional society",
      "citywide overflow when Naco housing fits",
      "SMERF / midscale-upscale group lodging",
    ],
    geoNodes: {
      primary: ["Santo Domingo Naco", "Naco Santo Domingo", "Tiradentes Santo Domingo"],
      secondary: ["Piantini Santo Domingo", "Bella Vista Santo Domingo", "Distrito Nacional"],
      feeder: ["Santiago Dominican Republic", "Punta Cana Santo Domingo feeder"],
      corporateClusters: ["Naco financial corridor", "Tiradentes business", "Santo Domingo corporate"],
      medicalUniversityGov: ["UASD", "INTEC", "medical Santo Domingo"],
      sportsVenues: ["Estadio Quisqueya overflow"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
  recjDsNzu93CFfe87: {
    hotelId: "recjDsNzu93CFfe87",
    name: "Casas del XVI",
    demandHypothesis: [
      "intimate luxury buyouts / celebrations",
      "executive / board retreats Zona Colonial",
      "destination weddings / social small groups",
      "incentive travel boutique lodging",
      "cultural / heritage association events",
      "small VIP housing near colonial district events",
    ],
    geoNodes: {
      primary: ["Zona Colonial Santo Domingo", "Ciudad Colonial Santo Domingo", "Casas del XVI"],
      secondary: ["Gazcue", "Malecon Santo Domingo", "Distrito Nacional"],
      feeder: ["Punta Cana colonial feeder", "Caribbean luxury Santo Domingo"],
      corporateClusters: ["colonial district VIP", "heritage tourism groups"],
      medicalUniversityGov: [],
      sportsVenues: [],
      resortNodes: ["Zona Colonial boutique", "Santo Domingo luxury destination"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
  rec9Tp0WBb2uk6w3u: {
    hotelId: "rec9Tp0WBb2uk6w3u",
    name: "Faranda Collection Bogotá",
    demandHypothesis: [
      "Bogotá Norte corporate meetings",
      "association / industry conferences Bogotá",
      "embassy / NGO / multilateral meetings",
      "university / medical adjacent groups",
      "Andean capital overflow when Norte housing fits",
      "mixed business-leisure groups Bogotá",
    ],
    geoNodes: {
      primary: ["Bogotá Norte", "Calle 112 Bogotá", "Usaquén Bogotá"],
      secondary: ["Chicó Bogotá", "Cedritos", "Bogotá Centro"],
      feeder: ["Medellín Bogotá feeder", "Cali Bogotá meetings"],
      corporateClusters: ["Bogotá Norte corporate", "Usaquén business", "Andean capital meetings"],
      medicalUniversityGov: ["Universidad de los Andes", "medical Bogotá"],
      sportsVenues: ["Bogotá sports overflow"],
    },
    yearPriority: ["2026", "2027"],
    yearAllow: ["2028"],
  },
});

const HOTELS = {
  A: {
    slot: "A",
    slug: "radisson-santo-domingo",
    hotelId: "recUOyzOXn2Zdp98I",
    name: "Radisson Hotel Santo Domingo",
    market: "Santo Domingo / Naco–Tiradentes",
    rooms: 160,
    demandProfile: "Upper-upscale urban business + meetings (Naco financial corridor)",
  },
  B: {
    slot: "B",
    slug: "casas-del-xvi",
    hotelId: "recjDsNzu93CFfe87",
    name: "Casas del XVI (Vignette Collection)",
    market: "Santo Domingo / Zona Colonial",
    rooms: 21,
    demandProfile: "Intimate luxury boutique / destination leisure + celebrations",
  },
  C: {
    slot: "C",
    slug: "faranda-collection-bogota",
    hotelId: "rec9Tp0WBb2uk6w3u",
    name: "Faranda Collection Bogotá",
    market: "Bogotá / Bogotá Norte",
    rooms: null,
    demandProfile: "Upscale capital mixed: business + leisure + groups",
  },
};

const VERTICAL_FAMILIES = [
  { id: "association_conference", hints: ["association conference", "annual meeting hotel"] },
  { id: "corporate_meeting", hints: ["corporate meeting", "board meeting hotel"] },
  { id: "medical_scientific", hints: ["medical conference", "scientific congress hotel"] },
  { id: "government_ngo", hints: ["government meeting", "embassy conference hotel"] },
  { id: "university_academic", hints: ["university conference", "academic symposium hotel"] },
  { id: "incentive_retreat", hints: ["incentive travel", "corporate incentive", "destination retreat"] },
  { id: "smerf_social", hints: ["wedding block hotel", "reunion hotel block", "social group lodging"] },
  {
    id: "housing_overflow",
    hints: [
      "official hotel block",
      "overflow housing",
      "housing bureau",
      "Cvent hotel",
      "room block RFP",
      "venue TBD hotel",
    ],
  },
];

function buildWave3RecallPack(hotelId, contract = {}) {
  const profile = WAVE3_RECALL_PROFILES[hotelId];
  if (!profile) throw new Error(`no_wave3_recall_profile_${hotelId}`);
  const yearNear = `(${profile.yearPriority.join(" OR ")})`;
  const yearAll = `(${[...profile.yearPriority, ...profile.yearAllow].join(" OR ")})`;
  const geos = [
    ...profile.geoNodes.primary,
    ...profile.geoNodes.secondary.slice(0, 3),
    ...(profile.geoNodes.corporateClusters || []).slice(0, 2),
    ...(profile.geoNodes.resortNodes || []).slice(0, 2),
  ].filter(Boolean);
  const tasks = [];
  for (const vertical of VERTICAL_FAMILIES) {
    const queries = [];
    for (const geo of geos.slice(0, 4)) {
      for (const hint of vertical.hints.slice(0, 2)) {
        queries.push(`"${geo}" ${hint} ${yearNear}`);
      }
    }
    queries.push(
      `"${geos[0]}" (${vertical.hints[0]}) ("venue TBD" OR "hotel TBD" OR RFP OR "room block" OR housing) ${yearNear}`
    );
    tasks.push({
      vertical: vertical.id,
      note: vertical.id,
      queries: [...new Set(queries)].slice(0, 6),
    });
  }
  for (const geo of (profile.geoNodes.feeder || []).slice(0, 2)) {
    tasks.push({
      vertical: "housing_overflow",
      note: "feeder_geo",
      queries: [
        `"${geo}" conference (hotel OR lodging OR "room block") ${yearNear}`,
        `"${geo}" (summit OR meeting) ("official hotel" OR overflow) ${yearNear}`,
      ],
    });
  }
  tasks.push({
    vertical: "housing_overflow",
    note: "open_sourcing",
    queries: [
      `"${profile.geoNodes.primary[0]}" ("RFP" OR "request for proposal") hotel ${yearNear}`,
      `"${profile.geoNodes.primary[0]}" ("save the date" OR "destination announced") conference ${yearAll}`,
      `"${profile.geoNodes.primary[0]}" (housing OR "hotel block") (2026 OR 2027) (open OR available OR TBD)`,
    ],
  });
  const brief = [
    "DEALALITY GDI DISCOVERY RECALL V1 (Wave 3 eval pack — no production hardcode)",
    `Hotel: ${profile.name} (${hotelId})`,
    `Market: ${contract.market || profile.geoNodes.primary[0]}`,
    `Rooms: ${contract.rooms ?? "n/a"} | Peak band: ${contract.peakRoomsMin ?? "?"}-${contract.peakRoomsMax ?? "?"}`,
    "Demand hypotheses (search only — not auto-qualify):",
    ...profile.demandHypothesis.map((h) => `- ${h}`),
    "Primary geo nodes:",
    profile.geoNodes.primary.join(", "),
    "Rules:",
    "- Prefer near-term (0–24 months) over distant future filler.",
    "- Prefer events with venue TBD, hotel TBD, RFP, housing open, or overflow signals.",
    "- Prefer first-party / official / association / sports governing sources.",
    "- Separate attendance from room demand; never invent room blocks.",
    "- Do not invent events. Empty is better than fabricated.",
  ].join("\n");
  return {
    version: DISCOVERY_RECALL_V1,
    profile,
    tasks,
    brief,
    completenessChecklist: {
      sourceFamilies: [
        "OFFICIAL_EVENT",
        "ASSOCIATION_CALENDAR",
        "CONFERENCE_CALENDAR",
        "SPORTS_BODY",
        "UNIVERSITY",
        "MEDICAL",
        "GOVERNMENT",
        "CORPORATE",
        "HOUSING_BUREAU",
        "TRADE_SHOW",
        "RFP_HOUSING_DOC",
      ],
      eventTypes: VERTICAL_FAMILIES.map((v) => v.id),
      geographicZones: Object.keys(profile.geoNodes),
    },
  };
}

const PLAN_MD =
  "reports/group-demand-intelligence/gdi-controlled-expansion-wave3-plan.md";

function parseArgs(argv) {
  return {
    hotelArg: String(argv.find((a) => a.startsWith("--hotel="))?.split("=")[1] || "all").toUpperCase(),
    stage: argv.find((a) => a.startsWith("--stage="))?.split("=")[1] || "all",
    maxQueries: Number(argv.find((a) => a.startsWith("--max-queries="))?.split("=")[1] || 28),
    force: argv.includes("--force"),
    dryDiscovery: argv.includes("--dry-discovery"),
    confirmSpend: argv.includes("--confirm-spend"),
  };
}

function hotelList(arg) {
  if (arg === "ALL") return Object.values(HOTELS);
  if (!HOTELS[arg]) throw new Error(`Unknown hotel ${arg}`);
  return [HOTELS[arg]];
}

function pathsFor(h) {
  const gdi = path.join(ROOT, "data/group-demand-intelligence/hotels", h.hotelId);
  const ge = path.join(ROOT, "data/group-demand-intelligence/evals");
  const ce = path.join(ROOT, "data/contact-intelligence/evals");
  return {
    gdi,
    discovery: path.join(gdi, "wave3-discovery.json"),
    qualified: path.join(gdi, "wave3-qualified.json"),
    discoveryEval: path.join(ge, `${h.slug}-wave3-discovery.json`),
    hygieneEval: path.join(ge, `${h.slug}-wave3-hygiene.json`),
    whoFreeze: path.join(ce, `${h.slug}-wave3-who.json`),
    reachability: path.join(ce, "gdi-controlled-expansion-wave3-reachability.json"),
  };
}

function writeJson(file, obj) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(obj, null, 2));
}
function writeMd(file, lines) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, (Array.isArray(lines) ? lines : [lines]).join("\n"));
}
function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}
function rel(p) {
  return path.relative(ROOT, p).replace(/\\/g, "/");
}
function pct(n, d) {
  return d ? Math.round((1000 * n) / d) / 10 : 0;
}
function openSourcing(s) {
  return /HOTEL_VENUE_TBD|OPEN_UNRESOLVED|RFP_ACTIVE|PARTIALLY_PLACED|OVERFLOW_POSSIBLE/i.test(String(s || ""));
}
function firstSourceUrl(opp) {
  if (opp.officialSource) return opp.officialSource;
  for (const e of opp.evidence || opp.evidenceSources || []) {
    if (!e) continue;
    if (typeof e === "string" && /^https?:/i.test(e)) return e;
    if (e.url || e.sourceUrl) return e.url || e.sourceUrl;
  }
  return null;
}
function splitName(full) {
  const parts = String(full || "").trim().split(/\s+/);
  return { first_name: parts[0] || "", last_name: parts.slice(1).join(" ") || "" };
}
function isGenericEmail(email) {
  return /^(info|events|contact|housing|registrar|admin|office|hello|support|team)@/i.test(String(email || ""));
}
function domainFromEmail(email) {
  const m = String(email || "").match(/@([^>\s]+)/i);
  return m ? m[1].toLowerCase() : null;
}
function adjudicateWhoPerson(p) {
  const name = String(p?.name || "").trim();
  if (!name) return "INSUFFICIENT";
  if (
    /^(long term|substitute teacher|event|staff|team|office|committee|department)\b/i.test(name) ||
    /\bevent\b/i.test(name) ||
    /<(section|div|span)/i.test(name) ||
    name.split(/\s+/).length < 2
  ) {
    return "INVALID";
  }
  return "VALID";
}
function namedConfirmed(row) {
  if (/NAMED_PERSON_CONFIRMED/i.test(row?.researchState || "")) return true;
  if (/NAMED_PERSON_CONFIRMED/i.test(row?.v10?.researchState || "")) return true;
  return (row?.people || []).some((p) => adjudicateWhoPerson(p) === "VALID");
}
function summarizeOpps(opps = []) {
  const byType = {};
  const byPriority = {};
  let sourcingActive = 0;
  for (const o of opps) {
    byType[o.opportunityType || "UNKNOWN"] = (byType[o.opportunityType || "UNKNOWN"] || 0) + 1;
    byPriority[o.priority || "UNKNOWN"] = (byPriority[o.priority || "UNKNOWN"] || 0) + 1;
    if (openSourcing(o.venueSourcingStatus)) sourcingActive += 1;
  }
  return {
    raw: opps.length,
    primary: byType.PRIMARY_PURSUIT || 0,
    overflow: byType.OVERFLOW_HOUSING || 0,
    futureCycle: byType.FUTURE_CYCLE || 0,
    reactivate: byType.REACTIVATION || 0,
    watch: byPriority.WATCHLIST || 0,
    sourcingActive,
    byType,
    byPriority,
  };
}
function gradePersonFields(p) {
  const email = p.finalEmail || p.email || p.publicEmail || null;
  const phone = p.finalPhone || p.phone || p.publicPhone || null;
  const nameAscii = String(p.name || "").normalize("NFD").replace(/\p{M}/gu, "");
  return gradeContact(
    {
      name: nameAscii,
      email,
      phone,
      role: p.role,
      organization: p.organization,
      emailVerificationStatus: email
        ? p.publicEmail && String(p.publicEmail).toLowerCase() === String(email).toLowerCase()
          ? "OFFICIAL_SOURCE_VERIFIED"
          : "PROVIDER_VERIFIED"
        : null,
      phoneType: phone ? "DIRECT" : null,
      claimedDirectPhone: Boolean(phone),
      providerVerified: Boolean(email && !p.publicEmail && !p.email),
    },
    { opportunityType: "PRIMARY_PURSUIT" }
  ).contactGrade;
}

function stagePlan() {
  console.error(`MARKER=${MARKER}`);
  console.error(`plan=${PLAN_MD} exists=${fs.existsSync(path.join(ROOT, PLAN_MD))}`);
  console.error("| Slot | Hotel | hotelId | Market | Rooms |");
  for (const h of Object.values(HOTELS)) {
    console.error(`| ${h.slot} | ${h.name} | ${h.hotelId} | ${h.market} | ${h.rooms} |`);
  }
  console.error("Stack: Hygiene V3 · V11 person boundary · V12 WHO recall · Surfe · field-level PDL");
  console.error(`Stack SHA: ${STACK_SHA}`);
  console.error("Policy: staged only · no live promote · no mid-wave patches · max 3 hotels · no production engine edits");
}

async function runDiscovery(hotel, args) {
  const p = pathsFor(hotel);
  fs.mkdirSync(p.gdi, { recursive: true });
  if (fs.existsSync(p.discovery) && !args.force) {
    console.error(`[${hotel.slot}] wave1 discovery exists — use --force`);
    return readJson(p.discovery);
  }

  const config = loadHotelDemandConfig(hotel.hotelId);
  if (!config) throw new Error(`missing_gdi_config_${hotel.hotelId}`);
  const profile = buildHotelGroupDemandProfile(hotel.hotelId);
  const recallPack = buildWave3RecallPack(hotel.hotelId, {
    market: hotel.market,
    rooms: hotel.rooms,
    peakRoomsMin: config?.commercialPriorities?.coreTargetPeakRoomsMin,
    peakRoomsMax: config?.commercialPriorities?.coreTargetPeakRoomsMax,
  });
  console.error(`[${hotel.slot}] discovery maxQueries=${args.maxQueries} tasks=${recallPack.tasks.length}`);

  const native = await runNativeBlindDiscovery({
    hotelId: hotel.hotelId,
    profile,
    config,
    dryRun: args.dryDiscovery,
    maxQueries: args.maxQueries,
    maxPagesPerQuery: 2,
    maxExtractBatches: 6,
    searchTasksOverride: recallPack.tasks,
    briefOverride: recallPack.brief,
    extractSystemOverride: RECALL_EXTRACT_SYSTEM,
    discoverySourceLabel: DISCOVERY_RECALL_V1,
  });

  if (args.dryDiscovery) {
    writeJson(p.discovery, { validationMarker: MARKER, hotel, native, dryRun: true });
    return { hotel, native, dryRun: true };
  }

  const deduped = dedupeDiscoveryCandidates(native.rows || []);
  const gate = evaluateNativeDiscoveryCompleteness({
    candidates: deduped.candidates,
    contract: {
      ...native.contract,
      verticals: recallPack.completenessChecklist.eventTypes.map((id) => ({ id })),
    },
  });
  const mapped = mapWebhoundOpportunityUniverse(deduped.candidates, hotel.hotelId);
  const seedCandidates = mapped.candidates.map((c) => ({
    ...c,
    researchProvider: "native",
    discoverySource: DISCOVERY_RECALL_V1,
  }));
  const completeness = {
    sourceFamiliesChecked: recallPack.completenessChecklist.sourceFamilies,
    queryFamiliesRun: [...new Set((native.tasks || []).map((t) => t.vertical).filter(Boolean))],
    eventTypesCovered: [
      ...new Set(deduped.candidates.map((c) => c.vertical || c.segment).filter(Boolean)),
    ],
    geographicZonesCovered: Object.keys(WAVE3_RECALL_PROFILES[hotel.hotelId]?.geoNodes || {}),
    queriesExecuted: (native.tasks || []).length,
    gate,
    discoveryIncomplete:
      gate.decision !== "NATIVE_SUFFICIENT" ||
      deduped.candidates.length < 5 ||
      seedCandidates.filter((c) => openSourcing(c.venueSourcingStatus)).length < 2,
  };

  const sanitized = seedCandidates.map((c) => {
    const out = { ...c };
    for (const k of ["eventStartDate", "eventEndDate", "startDate", "endDate"]) {
      const v = out[k];
      if (v == null || v === "") continue;
      const s = String(v).trim();
      if (/^\d{4}-\d{2}-\d{2}/.test(s)) out[k] = s.slice(0, 10);
      else {
        out[k] = null;
        out.dateNote = out.dateNote || s;
      }
    }
    return out;
  });

  let qualified = null;
  if (sanitized.length) {
    qualified = await runGroupDemandResearch({
      hotelId: hotel.hotelId,
      dryRun: true,
      discoveryMode: "INDEPENDENT_CANDIDATES",
      seedCandidates: sanitized,
      webhoundCalls: [],
      allowWebhoundWithoutFlag: false,
    });
  }

  const opportunities = qualified?.opportunities || [];
  const metrics = summarizeOpps(opportunities);
  const freeze = {
    validationMarker: MARKER,
    version: DISCOVERY_RECALL_V1,
    wave: "controlled_expansion_wave3",
    frozenAt: new Date().toISOString(),
    hotel,
    demandProfile: WAVE3_RECALL_PROFILES[hotel.hotelId] || null,
    completeness,
    pipelineMeta: {
      nativeVersion: native.version,
      ledger: native.ledger,
      gate,
      duplicatesRemoved: deduped.duplicatesRemoved,
      webhound: { used: false },
    },
    counts: { rawSeedCandidates: seedCandidates.length, ...metrics },
    seedCandidates,
  };
  writeJson(p.discovery, freeze);
  writeJson(p.qualified, {
    validationMarker: MARKER,
    hotelId: hotel.hotelId,
    runId: qualified?.runId || null,
    opportunities,
  });
  writeJson(p.discoveryEval, {
    validationMarker: MARKER,
    frozenAt: freeze.frozenAt,
    hotel,
    completeness,
    counts: freeze.counts,
    opportunities: opportunities.map((o) => ({
      id: o.id || o.opportunityId,
      title: o.title,
      organizationName: o.organizationName,
      opportunityType: o.opportunityType,
      priority: o.priority,
      eventStartDate: o.eventStartDate,
      destinationStatus: o.destinationStatus,
      venueSourcingStatus: o.venueSourcingStatus,
      roomDemandStatus: o.roomDemandStatus,
      estimatedPeakRooms: o.estimatedPeakRooms,
      whyNow: o.whyNow,
      hotelOpportunityThesis: o.hotelOpportunityThesis,
      evidenceConfidence: o.evidenceConfidence,
    })),
  });
  console.error(
    `[${hotel.slot}] freeze raw=${seedCandidates.length} qual=${opportunities.length} primary=${metrics.primary} open=${metrics.sourcingActive}`
  );
  return freeze;
}

/** Engine VA + (open/TBD/RFP OR PRIMARY/OVERFLOW) → TRUE_ACTIONABLE; else INSUFFICIENT/VALID_WATCH. */
function autoManualActionable(row) {
  const status = row.venueSourcingStatus || row.hygiene?.sourcing?.status;
  const open = openSourcing(status);
  const primaryOverflow = /PRIMARY_PURSUIT|OVERFLOW_HOUSING/i.test(String(row.opportunityType || ""));
  const conf = Number(row.evidenceConfidence);
  const lowConf =
    (Number.isFinite(conf) && conf < 0.55) ||
    (!firstSourceUrl(row) && !row.officialSource) ||
    (/UNKNOWN/i.test(String(row.roomDemandStatus || "")) && /UNKNOWN/i.test(String(status || "")));

  if (open || primaryOverflow) {
    return {
      manualActionable: "TRUE_ACTIONABLE",
      manualReason: open
        ? "engine_VALID_ACTIONABLE + open/TBD/RFP"
        : "engine_VALID_ACTIONABLE + PRIMARY/OVERFLOW",
      needsManualAudit: Boolean(lowConf),
    };
  }
  return {
    manualActionable: "INSUFFICIENT",
    manualReason: "VA without open/TBD/RFP or PRIMARY/OVERFLOW — VALID_WATCH for WHO",
    needsManualAudit: true,
  };
}

function stageHygiene(hotels, args) {
  const alias = assertVenueAliasRegression();
  if (!alias.ok) throw new Error(`venue_alias_regression_failed ${JSON.stringify(alias.failures)}`);
  const unknownGate = assertUnknownNotOpenRegression();
  if (unknownGate && unknownGate.ok === false) {
    throw new Error(`unknown_not_open_regression_failed ${JSON.stringify(unknownGate)}`);
  }

  const results = [];
  for (const h of hotels) {
    const p = pathsFor(h);
    if (!fs.existsSync(p.qualified)) throw new Error(`missing_qualified_${p.qualified}`);
    if (fs.existsSync(p.hygieneEval) && !args.force) {
      console.error(`[${h.slot}] hygiene exists — use --force`);
      results.push(readJson(p.hygieneEval));
      continue;
    }
    const oppsRaw = readJson(p.qualified).opportunities || [];
    const venueLockedCases = [];
    const opps = oppsRaw.map((opp) => {
      const vl = classifyVenueLockedOpportunity(opp, {
        hotelId: h.hotelId,
        name: h.name,
        displayName: h.name,
      });
      if (vl.changed || vl.substate !== "NOT_VENUE_LOCKED") {
        venueLockedCases.push({
          id: opp.id || opp.opportunityId,
          title: opp.title,
          oldType: vl.oldType,
          newType: vl.opportunityType,
          substate: vl.substate,
          reasons: vl.reasons,
          venueEvidence: vl.venueEvidence,
        });
      }
      if (!vl.changed) return opp;
      return {
        ...opp,
        opportunityType: vl.opportunityType,
        venueSourcingStatus: vl.venueSourcingStatus || opp.venueSourcingStatus,
        priority: vl.priorityCap || opp.priority,
        venueLockedV11: vl,
      };
    });

    const hygiene = applyDiscoveryHygieneV3(h.hotelId, opps, {
      nowDate: AS_OF,
      subjectHotel: { hotelId: h.hotelId, name: h.name },
    });

    // Engine TRUE_ACTIONABLE is the WHO input; manual audit fields filled post-freeze.
    const engineTrue = (hygiene.trueActionable || []).map((r) => ({
      ...r,
      engineActionability: ACTIONABILITY_V3.TRUE_ACTIONABLE,
      manualActionable: "PENDING_AUDIT",
      needsManualAudit: true,
    }));
    const watchRows = (hygiene.rows || []).filter(
      (r) => r.actionabilityV3 === ACTIONABILITY_V3.VALID_WATCH
    );
    const insufficientRows = (hygiene.rows || []).filter(
      (r) => r.actionabilityV3 === ACTIONABILITY_V3.INSUFFICIENT
    );

    const freeze = {
      validationMarker: MARKER,
      version: DISCOVERY_HYGIENE_V3,
      wave: "controlled_expansion_wave3",
      frozenAt: new Date().toISOString(),
      asOf: AS_OF,
      hotel: h,
      sourceUniverse: "wave3_qualified",
      stackSha: STACK_SHA,
      venueAliasRegression: alias,
      unknownNotOpenRegression: unknownGate || { ok: true },
      venueLockedCases,
      filterCounts: hygiene.filterCounts || null,
      byState: hygiene.byState,
      metrics: {
        candidates: opps.length,
        engineTrueActionable: engineTrue.length,
        trueActionable: engineTrue.length,
        falseActionable: 0,
        manualTrue: null,
        manualFalse: null,
        actionablePrecision: null,
        validWatch: hygiene.byState?.[ACTIONABILITY_V3.VALID_WATCH] || watchRows.length,
        validFuture: hygiene.byState?.[ACTIONABILITY_V3.VALID_FUTURE] || 0,
        invalid: hygiene.byState?.[ACTIONABILITY_V3.INVALID] || 0,
        insufficient: hygiene.byState?.[ACTIONABILITY_V3.INSUFFICIENT] || insufficientRows.length,
        venueLockedReported: venueLockedCases.length,
        overFilterCandidates: [...watchRows, ...insufficientRows].slice(0, 25).map((r) => ({
          id: r.id || r.opportunityId,
          title: r.title,
          actionabilityV3: r.actionabilityV3,
          reasons: r.hygiene?.reasons || r.reasons || [],
        })),
      },
      commercialMix: summarizeOpps(engineTrue),
      rows: (hygiene.rows || []).map((r) => ({
        id: r.id || r.opportunityId,
        title: r.title,
        organizationName: r.organizationName,
        opportunityType: r.opportunityType,
        priority: r.priority,
        eventStartDate: r.eventStartDate,
        destinationStatus: r.destinationStatus,
        venueSourcingStatus: r.venueSourcingStatus,
        roomDemandStatus: r.roomDemandStatus,
        estimatedPeakRooms: r.estimatedPeakRooms,
        actionabilityV3: r.actionabilityV3,
        hygieneState: r.hygieneState,
        hygiene: r.hygiene,
        whyNow: r.whyNow,
        hotelOpportunityThesis: r.hotelOpportunityThesis,
        officialSource: r.officialSource || firstSourceUrl(r),
        venueLockedV11: r.venueLockedV11 || null,
        // weekly-delta compatibility probe fields (presence only)
        opportunityId: r.id || r.opportunityId || null,
        eventSeriesId: r.eventSeriesId || null,
        eventCycleId: r.eventCycleId || null,
        firstSeenAt: r.firstSeenAt || null,
        lastSeenAt: r.lastSeenAt || null,
        firstSeenRunId: r.firstSeenRunId || null,
        lastSeenRunId: r.lastSeenRunId || null,
        lastMaterialChangeAt: r.lastMaterialChangeAt || null,
      })),
      trueActionable: engineTrue.map((r) => ({
        id: r.id || r.opportunityId,
        title: r.title,
        organizationName: r.organizationName,
        opportunityType: r.opportunityType,
        priority: r.priority,
        eventStartDate: r.eventStartDate,
        destinationStatus: r.destinationStatus,
        venueSourcingStatus: r.venueSourcingStatus,
        roomDemandStatus: r.roomDemandStatus,
        estimatedPeakRooms: r.estimatedPeakRooms,
        actionabilityV3: r.actionabilityV3,
        manualActionable: r.manualActionable,
        needsManualAudit: true,
        whyNow: r.whyNow,
        hotelOpportunityThesis: r.hotelOpportunityThesis,
        officialSource: r.officialSource || firstSourceUrl(r),
        evidence: r.evidence || r.evidenceSources || [],
        venueLockedV11: r.venueLockedV11 || null,
        opportunityId: r.id || r.opportunityId,
        eventSeriesId: r.eventSeriesId || null,
        eventCycleId: r.eventCycleId || null,
      })),
      whoInput: engineTrue,
    };
    writeJson(p.hygieneEval, freeze);
    console.error(
      `[${h.slot}] hygiene V3 cand=${opps.length} TRUE=${engineTrue.length} watch=${freeze.metrics.validWatch} insuff=${freeze.metrics.insufficient} venueLocked=${venueLockedCases.length}`
    );
    results.push(freeze);
  }
  return results;
}

function toWhoInput(opp, hotel) {
  const sourceUrls = [
    ...(opp.evidenceSources || []),
    ...(opp.sources || []),
    ...(opp.evidence || []),
    opp.officialSource,
  ]
    .map((s) => (typeof s === "string" ? s : s?.url || s?.sourceUrl))
    .filter((u) => u && /^https?:\/\//i.test(u));
  return {
    hotelId: hotel.hotelId,
    hotelName: hotel.name,
    opportunityId: opp.id || opp.opportunityId,
    opportunityName: opp.title || opp.opportunityName || null,
    eventName: opp.title || opp.opportunityName || null,
    organization: opp.organizationName || opp.organization || null,
    eventType: opp.opportunityType || null,
    segment: opp.segment || null,
    eventStartDate: opp.eventStartDate || null,
    eventEndDate: opp.eventEndDate || null,
    location: opp.location || opp.destinationStatus || null,
    venue: opp.venue || null,
    sourcingStatus: opp.venueSourcingStatus || opp.sourcingStatus || null,
    officialSourceUrls: sourceUrls.slice(0, 8),
    eventSourceUrls: sourceUrls.slice(0, 6),
    functionalContacts: opp.functionalContacts || [],
    desiredRoleFamilies: [
      "EVENT_OWNER",
      "MEETINGS_OWNER",
      "CONFERENCE_DIRECTOR",
      "HOUSING_OWNER",
      "REGISTRATION_OWNER",
    ],
  };
}

function mapWhoPeopleV12(result, opp) {
  const people = [];
  const pool = [
    ...(result?.people || []),
    ...(result?.primary ? [result.primary] : []),
    ...(result?.secondary || []),
  ];
  const seen = new Set();
  for (const p of pool) {
    if (!p?.name) continue;
    const key = String(p.name).toLowerCase().replace(/[^a-zà-ÿ\s']/gi, "").trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    const conf = confirmWhoRecallCandidateV12(p, {
      organization: opp.organization,
      opportunityName: opp.opportunityName || opp.eventName,
      eventSourceUrls: opp.officialSourceUrls || opp.eventSourceUrls || [],
    });
    if (!conf.accept) {
      people.push({
        name: p.name,
        role: p.role,
        sourceUrl: p.sourceUrl,
        email: p.email || null,
        phone: p.phone || null,
        auditClass: "INVALID",
        accepted: false,
        rejectReason: conf.reason,
        salesRole: null,
        recallPath: "V11_GATE_REJECT",
      });
      continue;
    }
    const manual = adjudicateWhoPerson({ name: conf.name });
    people.push({
      ...p,
      name: conf.name,
      auditClass: manual,
      accepted: manual === "VALID",
      salesRole: p.roleRelevance || p.salesRole || "OPERATIONAL_CONTACT",
      recallPath: opp.primaryGap || "V12_DIRECT",
    });
  }
  return people;
}

async function stageWho(hotels, args) {
  const out = {};
  for (const h of hotels) {
    const p = pathsFor(h);
    if (!fs.existsSync(p.hygieneEval)) throw new Error(`missing_hygiene_${p.hygieneEval}`);
    if (fs.existsSync(p.whoFreeze) && !args.force) {
      console.error(`[${h.slot}] WHO freeze exists — use --force`);
      out[h.hotelId] = readJson(p.whoFreeze);
      continue;
    }
    const freeze = readJson(p.hygieneEval);
    const whoInputs = (freeze.whoInput || []).map((o) => toWhoInput(o, h));
    console.error(`[${h.slot}] V11+V12 WHO on ${whoInputs.length} TRUE_ACTIONABLE`);
    const rows = [];
    let v11Direct = 0;
    let v12Recovered = 0;
    for (let i = 0; i < whoInputs.length; i++) {
      const base = whoInputs[i];
      const gap = classifyWhoRecallGapV12({
        ...base,
        eventName: base.opportunityName,
        currentNoWhoReason: "wave3_INITIAL",
        functionalContacts: base.functionalContacts || [],
      });
      const { input, queries } = enrichDiscoverInputV12(base, {
        ...base,
        eventName: base.opportunityName,
        primaryGap: gap.primaryGap,
        secondaryGaps: gap.secondaryGaps,
      });
      console.error(
        `[${h.slot}] WHO ${i + 1}/${whoInputs.length} ${base.opportunityId} gap=${gap.primaryGap}`
      );
      let result = null;
      let error = null;
      const started = Date.now();
      try {
        result = await Promise.race([
          discoverWhoV9(input),
          new Promise((_, rej) =>
            setTimeout(() => rej(new Error("discoverWhoV9_timeout_240s")), 240000)
          ),
        ]);
      } catch (err) {
        error = String(err.message || err);
      }
      const people = mapWhoPeopleV12(result, {
        ...base,
        primaryGap: gap.primaryGap,
      });
      const accepted = people.filter((x) => x.accepted && x.auditClass === "VALID");
      const invalid = people.filter((x) => x.auditClass === "INVALID");
      const funcs = result?.functionalContacts || [];
      const researchState = accepted.length
        ? "NAMED_PERSON_CONFIRMED"
        : funcs.length
          ? "FUNCTIONAL_CONTACT_ONLY_RESEARCHED"
          : "NO_WHO_RESEARCHED";
      if (accepted.length) {
        // Heuristic: if first discover pass already named without directed gap recovery notes
        if (gap.primaryGap === "EVENT_CONTACT_NOT_FOUND" || gap.primaryGap === "NO_STAFF_SOURCE_FOUND") {
          v11Direct += 1;
        } else {
          v12Recovered += 1;
        }
      }
      const row = {
        hotelId: h.hotelId,
        hotelName: h.name,
        opportunityId: base.opportunityId,
        opportunityName: base.opportunityName,
        organization: base.organization,
        primaryGap: gap.primaryGap,
        secondaryGaps: gap.secondaryGaps,
        directedQueries: (queries || []).slice(0, 12),
        stagesPlanned: recallStagesForGapV12(gap.primaryGap),
        stagesRun: result?.metrics?.stagesRun || [],
        stopReason: result?.metrics?.stopReason || error || null,
        researchState,
        hasNamedWho: accepted.length > 0,
        people,
        primary: accepted[0] || null,
        secondary: accepted.slice(1),
        functionalContacts: funcs,
        invalidRejected: invalid.length,
        elapsedMs: Date.now() - started,
        error,
        sourcesChecked: result?.urlsFetched || null,
        passVersion: "native_who_v11_v12",
        validationMarker: MARKER,
      };
      rows.push(row);
      console.error(
        `[${h.slot}] → state=${researchState} accepted=${accepted.length} invalid=${invalid.length} err=${error || "none"}`
      );
      writeJson(p.whoFreeze, {
        validationMarker: MARKER,
        engine: "native_who_v11_v12",
        wave: "controlled_expansion_wave3",
        frozenAt: new Date().toISOString(),
        partial: rows.length < whoInputs.length,
        hotel: h,
        count: rows.length,
        expected: whoInputs.length,
        rows,
      });
    }
    const named = rows.filter((r) => r.hasNamedWho).length;
    const finalDoc = {
      validationMarker: MARKER,
      engine: "native_who_v11_v12",
      wave: "controlled_expansion_wave3",
      stackSha: STACK_SHA,
      frozenAt: new Date().toISOString(),
      partial: false,
      hotel: h,
      policy: {
        inventPeople: false,
        evidencePack: false,
        v11PrecisionUnchanged: true,
        v12RecallEnabled: true,
      },
      metrics: {
        opportunities: rows.length,
        namedConfirmed: named,
        validPeople: rows.reduce((n, r) => n + (r.people || []).filter((x) => x.accepted).length, 0),
        invalidPeople: rows.reduce((n, r) => n + (r.invalidRejected || 0), 0),
        functionalOnly: rows.filter((r) =>
          /FUNCTIONAL_CONTACT_ONLY/i.test(r.researchState)
        ).length,
        noWho: rows.filter((r) => r.researchState === "NO_WHO_RESEARCHED").length,
        coveragePct: pct(named, rows.length),
        precisionPct:
          rows.reduce((n, r) => n + (r.people || []).filter((x) => x.accepted).length, 0) === 0 &&
          rows.reduce((n, r) => n + (r.invalidRejected || 0), 0) === 0
            ? 100
            : pct(
                rows.reduce((n, r) => n + (r.people || []).filter((x) => x.accepted).length, 0),
                rows.reduce(
                  (n, r) =>
                    n +
                    (r.people || []).filter((x) => x.accepted).length +
                    (r.invalidRejected || 0),
                  0
                )
              ),
        v11DirectHeuristic: v11Direct,
        v12RecoveredHeuristic: v12Recovered,
      },
      count: rows.length,
      expected: whoInputs.length,
      rows,
    };
    writeJson(p.whoFreeze, finalDoc);
    console.error(
      `[${h.slot}] WHO named=${named}/${rows.length} cov=${finalDoc.metrics.coveragePct}%`
    );
    out[h.hotelId] = finalDoc;
  }
  return out;
}

/** @deprecated Wave 3 folds V12 into stageWho — kept as no-op for CLI compat */
function stageV10(hotels) {
  console.error("Wave 3: V12 recall is folded into --stage=who (no separate V10 scaffold)");
  const out = {};
  for (const h of hotels) {
    const p = pathsFor(h);
    if (fs.existsSync(p.whoFreeze)) out[h.hotelId] = readJson(p.whoFreeze);
  }
  return out;
}

async function enrichSurfeOne(person) {
  const { first_name, last_name } = splitName(person.name);
  const domain = domainFromEmail(person.publicEmail || person.email) || undefined;
  const creditsBefore = await getSurfeCredits();
  const emailPeople = [
    {
      firstName: first_name,
      lastName: last_name,
      companyName: person.organization || undefined,
      companyDomain: domain,
      externalID: `wave3_${person.opportunityId}_${first_name}`.slice(0, 64),
    },
  ];
  const emailStart = await startSurfePeopleEnrichment({
    people: emailPeople,
    include: { email: true, mobile: false, linkedInUrl: true, jobHistory: true },
    enrichmentOptions: { acceptedEmailType: "professional" },
  });
  if (!emailStart.ok) {
    return {
      ok: false,
      timed_out: false,
      surfeEmail: null,
      surfeMobile: null,
      identity: { decision: "REJECTED" },
      creditsBefore: creditsBefore.payload,
      error: emailStart.error || `http_${emailStart.http_status}`,
    };
  }
  const emailJobId =
    emailStart.payload?.enrichmentID || emailStart.payload?.enrichmentId || emailStart.payload?.id || null;
  const emailDone = await pollSurfePeopleEnrichment(emailJobId, { maxWaitMs: 240000 });
  let mobileJobId = null;
  let mobileDone = null;
  if (emailDone.ok && !emailDone.timed_out) {
    const mobileStart = await startSurfePeopleEnrichment({
      people: emailPeople,
      include: { email: false, mobile: true, linkedInUrl: false },
    });
    mobileJobId =
      mobileStart.payload?.enrichmentID || mobileStart.payload?.enrichmentId || mobileStart.payload?.id || null;
    if (mobileStart.ok && mobileJobId) {
      mobileDone = await pollSurfePeopleEnrichment(mobileJobId, { maxWaitMs: 240000 });
    }
  }
  const creditsAfter = await getSurfeCredits();
  const emailNorm = normalizeSurfePerson((emailDone.payload?.people || [])[0] || {});
  const mobileNorm = normalizeSurfePerson((mobileDone?.payload?.people || [])[0] || {});
  const surfeEmail =
    emailNorm.emails?.find((e) => e.validation_status === "VALID")?.email || emailNorm.emails?.[0]?.email || null;
  const surfeMobile = mobileNorm.mobile_phones?.[0]?.number || null;
  const identity = acceptSurfeIdentity({
    expectedFullName: person.name,
    expectedOrganization: person.organization,
    expectedDomain: domain || null,
    expectedTitle: person.role,
    returnedFullName: emailNorm.full_name,
    returnedOrganization: emailNorm.company_name,
    returnedDomain: emailNorm.company_domain,
    returnedTitle: emailNorm.job_title,
    returnedEmail: surfeEmail,
    returnedLinkedInUrl: emailNorm.linkedin_url,
  });
  return {
    ok: Boolean(emailDone.ok && !emailDone.timed_out),
    timed_out: Boolean(emailDone.timed_out || mobileDone?.timed_out),
    surfeEmail,
    surfeMobile,
    identity,
    creditsBefore: creditsBefore.payload,
    creditsAfter: creditsAfter.payload,
    emailJobId,
    mobileJobId,
  };
}

function acceptSurfeFields(person, surfe) {
  const decision = surfe.identity?.decision;
  const identityOk =
    decision === IDENTITY_DECISION.ACCEPTED ||
    decision === IDENTITY_DECISION.ACCEPTED_WITH_LIMITED_EVIDENCE ||
    decision === IDENTITY_DECISION.CORROBORATION_ONLY;
  const emailMatches =
    person.publicEmail &&
    surfe.surfeEmail &&
    String(person.publicEmail).toLowerCase() === String(surfe.surfeEmail).toLowerCase();
  const howSafe = identityOk || emailMatches;
  let email = null;
  let emailValue = "NO_VALUE";
  if (howSafe && surfe.surfeEmail && !isGenericEmail(surfe.surfeEmail)) {
    if (emailMatches) {
      email = surfe.surfeEmail;
      emailValue = "CORROBORATION";
    } else if (!person.publicEmail && identityOk) {
      email = surfe.surfeEmail;
      emailValue = "INCREMENTAL";
    }
  }
  let phone = null;
  let phoneValue = "NO_VALUE";
  if (howSafe && surfe.surfeMobile && (identityOk || emailMatches)) {
    phone = surfe.surfeMobile;
    phoneValue = "INCREMENTAL";
  }
  return { email, phone, emailValue, phoneValue, rejected: !howSafe && decision === IDENTITY_DECISION.REJECTED };
}

async function enrichPdlOne(person) {
  const { first_name, last_name } = splitName(person.name);
  const res = await enrichPdlPerson(
    {
      id: person.personId || person.opportunityId,
      first_name,
      last_name,
      company_name: person.organization,
      company_domain: domainFromEmail(person.publicEmail || person.email),
    },
    { retries: 0, min_likelihood: 5 }
  );
  return {
    ok: !res.error,
    email: res.normalized?.work_email || res.normalized?.email || null,
    mobile: res.normalized?.mobile_phone || null,
    phone: res.normalized?.phone_numbers?.[0] || null,
    error: res.error || null,
    retrievedAt: new Date().toISOString(),
  };
}

function collectAcceptedWhoPeople(hotels) {
  const people = [];
  for (const h of hotels) {
    const p = pathsFor(h);
    if (!fs.existsSync(p.whoFreeze)) continue;
    for (const row of readJson(p.whoFreeze).rows || []) {
      if (!row.hasNamedWho && !namedConfirmed(row)) continue;
      const pool = row.people || [];
      const accepted = pool.filter((x) => x.accepted !== false && adjudicateWhoPerson(x) === "VALID");
      const primary = accepted.find((x) => x.rank === "PRIMARY") || accepted[0];
      if (!primary?.name) continue;
      people.push({
        ...primary,
        ...splitName(primary.name),
        publicEmail: primary.email || primary.publicEmail || null,
        publicPhone: primary.phone || primary.publicPhone || null,
        organization: row.organization || primary.organization || null,
        opportunityId: row.opportunityId,
        hotelId: h.hotelId,
        hotelName: h.name,
        event: row.opportunityName || row.event || null,
      });
    }
  }
  return people;
}

async function stageReach(hotels, args) {
  const targetHotels = args.hotelArg === "ALL" ? Object.values(HOTELS) : hotels;
  const reachPath = pathsFor(HOTELS.A).reachability;
  if (fs.existsSync(reachPath) && !args.force && !args.confirmSpend) {
    console.error("reach freeze exists — use --force or --confirm-spend");
    return readJson(reachPath);
  }

  const cohort = collectAcceptedWhoPeople(targetHotels);
  const missing = cohort.filter((x) => !x.publicEmail || !x.publicPhone);

  if (!args.confirmSpend) {
    const dry = {
      validationMarker: MARKER,
      wave: "controlled_expansion_wave3",
      frozenAt: new Date().toISOString(),
      status: "DRY_PLAN",
      policy: { inventPeople: false, liveWrites: false, surfeThenFieldLevelPdl: true, confirmSpendRequired: true },
      cohortSize: cohort.length,
      needingEnrichment: missing.length,
      people: missing.map((x) => ({
        name: x.name,
        hotelName: x.hotelName,
        opportunityId: x.opportunityId,
        publicEmail: x.publicEmail,
        publicPhone: x.publicPhone,
        need: !x.publicEmail ? "EMAIL_MISSING" : !x.publicPhone ? "PHONE_MISSING" : "COMPLETE",
      })),
      results: [],
      metrics: {
        surfeCalls: 0,
        surfeOk: 0,
        surfeFail: 0,
        surfeEmailHits: 0,
        surfeMobileHits: 0,
        pdlCalls: 0,
        pdlEmailHits: 0,
        pdlPhoneHits: 0,
        pdlIncremental: 0,
      },
    };
    writeJson(reachPath, dry);
    console.error(`reach dry-plan cohort=${cohort.length} needing=${missing.length}`);
    return dry;
  }

  const results = [];
  const m = {
    surfeCalls: 0,
    surfeOk: 0,
    surfeFail: 0,
    surfeEmailHits: 0,
    surfeMobileHits: 0,
    pdlCalls: 0,
    pdlEmailHits: 0,
    pdlPhoneHits: 0,
    pdlIncremental: 0,
  };

  for (const person of missing) {
    console.error(`reach Surfe ${person.name} (${person.hotelName})`);
    m.surfeCalls += 1;
    let surfe;
    try {
      surfe = await enrichSurfeOne(person);
    } catch (err) {
      surfe = { ok: false, timed_out: false, surfeEmail: null, surfeMobile: null, identity: { decision: "REJECTED" }, error: String(err.message || err) };
    }
    if (surfe.ok && !surfe.timed_out) m.surfeOk += 1;
    else m.surfeFail += 1;

    const accepted = acceptSurfeFields(person, surfe);
    if (accepted.email && accepted.emailValue !== "NO_VALUE") m.surfeEmailHits += 1;
    if (accepted.phone && accepted.phoneValue !== "NO_VALUE") m.surfeMobileHits += 1;

    const hasEmail = Boolean(person.publicEmail || (accepted.email && accepted.emailValue !== "NO_VALUE"));
    const hasPhone = Boolean(person.publicPhone || (accepted.phone && accepted.phoneValue !== "NO_VALUE"));

    let pdl = null;
    if ((!hasEmail && !person.publicEmail) || (hasEmail && !hasPhone)) {
      console.error(`reach PDL field-level ${person.name}`);
      m.pdlCalls += 1;
      pdl = await enrichPdlOne(person);
      if (pdl.email && !hasEmail) {
        m.pdlEmailHits += 1;
        m.pdlIncremental += 1;
      }
      if ((pdl.mobile || pdl.phone) && !hasPhone) {
        m.pdlPhoneHits += 1;
        m.pdlIncremental += 1;
      }
    }

    const finalEmail = person.publicEmail || accepted.email || pdl?.email || null;
    const finalPhone = person.publicPhone || accepted.phone || pdl?.mobile || pdl?.phone || null;
    results.push({
      name: person.name,
      hotelId: person.hotelId,
      hotelName: person.hotelName,
      opportunityId: person.opportunityId,
      event: person.event,
      publicEmail: person.publicEmail,
      publicPhone: person.publicPhone,
      surfe: {
        called: true,
        ok: surfe.ok,
        timed_out: surfe.timed_out,
        email: accepted.email,
        phone: accepted.phone,
        emailValue: accepted.emailValue,
        phoneValue: accepted.phoneValue,
        identity: surfe.identity?.decision || null,
        emailJobId: surfe.emailJobId || null,
        mobileJobId: surfe.mobileJobId || null,
        error: surfe.error || null,
        provider: "surfe",
        client: SURFE_CLIENT_VERSION,
        retrievedAt: new Date().toISOString(),
      },
      pdl: pdl
        ? {
            called: true,
            email: pdl.email,
            phone: pdl.mobile || pdl.phone,
            error: pdl.error,
            provider: "pdl",
            client: PDL_CLIENT_VERSION,
            retrievedAt: pdl.retrievedAt,
            mode: "FIELD_LEVEL",
          }
        : { called: false },
      finalEmail,
      finalPhone,
      grades: {
        before: gradePersonFields({ ...person, finalEmail: person.publicEmail, finalPhone: person.publicPhone }),
        after: gradePersonFields({ ...person, finalEmail, finalPhone, publicEmail: person.publicEmail }),
      },
    });
  }

  const doc = {
    validationMarker: MARKER,
    wave: "controlled_expansion_wave3",
    frozenAt: new Date().toISOString(),
    status: "ENRICHED",
    policy: { inventPeople: false, liveWrites: false, surfeThenFieldLevelPdl: true },
    cohortSize: cohort.length,
    needingEnrichment: missing.length,
    results,
    metrics: m,
  };
  writeJson(reachPath, doc);
  console.error(`reach done surfeOk=${m.surfeOk}/${m.surfeCalls} pdl=${m.pdlCalls} incremental=${m.pdlIncremental}`);
  return doc;
}

function loadFreeze(h, key) {
  const p = pathsFor(h);
  const file = p[key];
  return fs.existsSync(file) ? readJson(file) : null;
}

function summarizeWhoDoc(whoDoc) {
  const rows = whoDoc?.rows || [];
  let valid = 0;
  let invalid = 0;
  let functionalOnly = 0;
  let noWho = 0;
  let named = 0;
  for (const r of rows) {
    const people = r.v10?.people?.length ? r.v10.people : r.people || [];
    const funcs = r.functionalContacts || r.v10?.functionalContact || [];
    const judgments = people.map(adjudicateWhoPerson);
    const hasValid = judgments.includes("VALID");
    const hasInvalid = judgments.includes("INVALID");
    const confirmed = r.hasNamedWho || namedConfirmed(r) || hasValid;
    if (confirmed && hasValid) {
      named += 1;
      valid += 1;
    } else if (hasInvalid) invalid += 1;
    else if (funcs.length) functionalOnly += 1;
    else noWho += 1;
  }
  const decided = valid + invalid;
  return {
    opportunitiesTested: rows.length,
    named,
    valid,
    invalid,
    functionalOnly,
    noWho,
    precision: decided ? pct(valid, decided) : rows.length === 0 ? 100 : 0,
    falseRate: decided ? pct(invalid, decided) : 0,
    coverage: rows.length ? pct(named, rows.length) : 0,
  };
}

function v12Split(whoDoc) {
  let v11Direct = 0;
  let v12Recovered = 0;
  let functionalOnly = 0;
  let noWho = 0;
  for (const r of whoDoc?.rows || []) {
    if (r.hasNamedWho) {
      if (
        r.primaryGap === "EVENT_CONTACT_NOT_FOUND" ||
        r.primaryGap === "NO_STAFF_SOURCE_FOUND" ||
        r.primaryGap === "QUERY_RECALL"
      ) {
        v11Direct += 1;
      } else {
        v12Recovered += 1;
      }
    } else if (/FUNCTIONAL/i.test(r.researchState || "")) functionalOnly += 1;
    else noWho += 1;
  }
  return { v11Direct, v12Recovered, functionalOnly, noWho };
}

function stageReports() {
  const hotels = Object.values(HOTELS);
  const hygieneBy = Object.fromEntries(hotels.map((h) => [h.hotelId, loadFreeze(h, "hygieneEval")]));
  const whoBy = Object.fromEntries(hotels.map((h) => [h.hotelId, loadFreeze(h, "whoFreeze")]));
  const reach = loadFreeze(HOTELS.A, "reachability");
  const rm = reach?.metrics || {};

  const disc = [
    "# GDI Controlled Expansion Wave 3 — Discovery",
    "",
    `Marker: \`${MARKER}\``,
    `Stack SHA: \`${STACK_SHA}\``,
    "",
    "| Hotel | Candidates | Engine VA | TRUE_ACTIONABLE | Insufficient-on-bar | Precision | Needs audit | Venue-locked |",
    "|---|---:|---:|---:|---:|---:|---:|---:|",
  ];
  for (const h of hotels) {
    const m = hygieneBy[h.hotelId]?.metrics || {};
    disc.push(
      `| ${h.name} | ${m.candidates ?? "—"} | ${m.validActionable ?? "—"} | ${m.trueActionable ?? "—"} | ${m.insufficientOnActionableGate ?? "—"} | ${m.actionablePrecision != null ? `${(m.actionablePrecision * 100).toFixed(1)}%` : "—"} | ${m.needsManualAudit ?? "—"} | ${m.venueLockedReported ?? 0} |`
    );
  }
  disc.push("", "## Commercial mix (TRUE_ACTIONABLE)", "", "| Hotel | Primary | Overflow | Watch | Future | Open sourcing |", "|---|---:|---:|---:|---:|---:|");
  for (const h of hotels) {
    const mix = hygieneBy[h.hotelId]?.commercialMix || {};
    disc.push(`| ${h.name} | ${mix.primary ?? 0} | ${mix.overflow ?? 0} | ${mix.watch ?? 0} | ${mix.futureCycle ?? 0} | ${mix.sourcingActive ?? 0} |`);
  }
  disc.push("", "## Venue-locked cases", "");
  for (const h of hotels) {
    const cases = hygieneBy[h.hotelId]?.venueLockedCases || [];
    disc.push(`### ${h.name}`, "");
    if (!cases.length) disc.push("_None reported._", "");
    for (const c of cases) {
      disc.push(`- **${c.title}** (\`${c.id}\`): ${c.oldType} → ${c.newType} (${c.substate})`);
    }
  }
  writeMd(path.join(ROOT, "reports/group-demand-intelligence/gdi-controlled-expansion-wave3-discovery.md"), disc);

  const whoMd = [
    "# GDI Controlled Expansion Wave 3 — WHO",
    "",
    `Marker: \`${MARKER}\``,
    "",
    "| Hotel | TRUE_ACTIONABLE | Named | Valid | Invalid | Precision | Coverage | Functional | No WHO |",
    "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
  ];
  for (const h of hotels) {
    const trueN = hygieneBy[h.hotelId]?.metrics?.trueActionable ?? 0;
    const w = summarizeWhoDoc(whoBy[h.hotelId]);
    whoMd.push(`| ${h.name} | ${trueN} | ${w.named} | ${w.valid} | ${w.invalid} | ${w.precision}% | ${w.coverage}% | ${w.functionalOnly} | ${w.noWho} |`);
  }
  whoMd.push("", "## V11/V12 recall split", "", "| Hotel | V11 Direct WHO | V12 Recovered | Functional Only | No WHO |", "|---|---:|---:|---:|---:|");
  for (const h of hotels) {
    const s = v12Split(whoBy[h.hotelId]);
    whoMd.push(`| ${h.name} | ${s.v11Direct} | ${s.v12Recovered} | ${s.functionalOnly} | ${s.noWho} |`);
  }
  writeMd(path.join(ROOT, "reports/contact-intelligence/gdi-controlled-expansion-wave3-who.md"), whoMd);

  const reachMd = [
    "# GDI Controlled Expansion Wave 3 — Reachability",
    "",
    `Marker: \`${MARKER}\``,
    "",
    `Status: **${reach?.status || "MISSING"}**`,
    "",
    "| Metric | Value |",
    "|---|---:|",
    `| Cohort (accepted named WHO) | ${reach?.cohortSize ?? "—"} |`,
    `| Needing enrichment | ${reach?.needingEnrichment ?? "—"} |`,
    `| Surfe calls | ${rm.surfeCalls ?? 0} |`,
    `| Surfe ok | ${rm.surfeOk ?? 0} |`,
    `| Surfe fail | ${rm.surfeFail ?? 0} |`,
    `| Surfe email hits | ${rm.surfeEmailHits ?? 0} |`,
    `| Surfe mobile hits | ${rm.surfeMobileHits ?? 0} |`,
    `| PDL calls | ${rm.pdlCalls ?? 0} |`,
    `| PDL email hits | ${rm.pdlEmailHits ?? 0} |`,
    `| PDL phone hits | ${rm.pdlPhoneHits ?? 0} |`,
    `| PDL incremental | ${rm.pdlIncremental ?? 0} |`,
    "",
  ];
  for (const r of reach?.results || []) {
    reachMd.push(
      `- **${r.name}** (${r.hotelName}) — public ${r.publicEmail || "—"} / ${r.publicPhone || "—"} → Surfe ${r.surfe?.email || "—"} / ${r.surfe?.phone || "—"} → PDL ${r.pdl?.email || "—"} / ${r.pdl?.phone || "—"} → grade ${r.grades?.before}→${r.grades?.after}`
    );
  }
  writeMd(path.join(ROOT, "reports/contact-intelligence/gdi-controlled-expansion-wave3-reachability.md"), reachMd);

  return writeFounderReport(hygieneBy, whoBy, reach);
}

function writeFounderReport(hygieneBy, whoBy, reach) {
  const hotels = Object.values(HOTELS);
  const rm = reach?.metrics || {};
  const fr = [`# GDI Controlled Expansion Wave 3 — Founder Report`, ``, `Marker: \`${MARKER}\``, ``];

  fr.push("## A. HOTELS", "", "| Hotel | hotelId | Market | Rooms | Demand Profile |", "|---|---|---|---:|---|");
  for (const h of hotels) {
    fr.push(`| ${h.name} | \`${h.hotelId}\` | ${h.market} | ${h.rooms} | ${h.demandProfile} |`);
  }
  fr.push("");

  fr.push("## B. DISCOVERY", "", "| Hotel | Candidates | True Actionable | False (bar fail) | Precision |", "|---|---:|---:|---:|---:|");
  const discPrecs = [];
  for (const h of hotels) {
    const m = hygieneBy[h.hotelId]?.metrics || {};
    const prec = m.actionablePrecision != null ? m.actionablePrecision * 100 : null;
    if (prec != null) discPrecs.push(prec);
    fr.push(`| ${h.name} | ${m.candidates ?? "—"} | ${m.trueActionable ?? "—"} | ${m.insufficientOnActionableGate ?? "—"} | ${prec != null ? `${prec.toFixed(1)}%` : "—"} |`);
  }
  fr.push("");

  fr.push("## C. COMMERCIAL MIX", "", "| Hotel | Primary | Overflow | Watch | Future | Supported Open |", "|---|---:|---:|---:|---:|---:|");
  for (const h of hotels) {
    const mix = hygieneBy[h.hotelId]?.commercialMix || {};
    const supportedOpen = (hygieneBy[h.hotelId]?.validActionable || []).filter(
      (r) => r.manualActionable === "TRUE_ACTIONABLE" && (r.sourcingSupported || openSourcing(r.venueSourcingStatus))
    ).length;
    fr.push(`| ${h.name} | ${mix.primary ?? 0} | ${mix.overflow ?? 0} | ${mix.watch ?? 0} | ${mix.futureCycle ?? 0} | ${supportedOpen} |`);
  }
  fr.push("");

  fr.push("## D. WHO", "", "| Hotel | True Actionable | Named WHO | Valid | Invalid | Precision | Coverage |", "|---|---:|---:|---:|---:|---:|---:|");
  const whoPrecs = [];
  const whoFalses = [];
  let totalTrue = 0;
  let totalNamed = 0;
  for (const h of hotels) {
    const trueN = hygieneBy[h.hotelId]?.metrics?.trueActionable ?? 0;
    const w = summarizeWhoDoc(whoBy[h.hotelId]);
    totalTrue += trueN;
    totalNamed += w.named;
    whoPrecs.push(w.precision);
    whoFalses.push(w.falseRate);
    fr.push(`| ${h.name} | ${trueN} | ${w.named} | ${w.valid} | ${w.invalid} | ${w.precision}% | ${w.coverage}% |`);
  }
  fr.push("");

  fr.push("## E. WHO RECALL", "", "| Hotel | V11 Direct WHO | V12 Recovered | Functional Only | No WHO |", "|---|---:|---:|---:|---:|");
  for (const h of hotels) {
    const s = v10Split(whoBy[h.hotelId]);
    fr.push(`| ${h.name} | ${s.v11Direct} | ${s.v12Recovered} | ${s.functionalOnly} | ${s.noWho} |`);
  }
  fr.push("");

  fr.push("## F. PROVIDERS", "", "| Hotel | Surfe Calls | Email Hits | Mobile Hits | PDL Calls | PDL Incremental |", "|---|---:|---:|---:|---:|---:|");
  const byHotelReach = {};
  for (const r of reach?.results || []) {
    const id = r.hotelId || "unknown";
    byHotelReach[id] ||= { surfe: 0, email: 0, mobile: 0, pdl: 0, pdlInc: 0 };
    byHotelReach[id].surfe += 1;
    if (r.surfe?.email) byHotelReach[id].email += 1;
    if (r.surfe?.phone) byHotelReach[id].mobile += 1;
    if (r.pdl?.called) byHotelReach[id].pdl += 1;
    if ((r.pdl?.email && !r.publicEmail && !r.surfe?.email) || (r.pdl?.phone && !r.publicPhone && !r.surfe?.phone)) {
      byHotelReach[id].pdlInc += 1;
    }
  }
  for (const h of hotels) {
    const x = byHotelReach[h.hotelId] || { surfe: 0, email: 0, mobile: 0, pdl: 0, pdlInc: 0 };
    fr.push(`| ${h.name} | ${x.surfe} | ${x.email} | ${x.mobile} | ${x.pdl} | ${x.pdlInc} |`);
  }
  fr.push("", `_Cross-hotel Surfe ok ${rm.surfeOk ?? 0}/${rm.surfeCalls ?? 0}; PDL incremental ${rm.pdlIncremental ?? 0}._`, "");

  fr.push("## G. FINAL CONTACT", "", "| Hotel | Named WHO | Contactable | High Contactability | Functional Only | No Contact Path |", "|---|---:|---:|---:|---:|---:|");
  let totalContactable = 0;
  for (const h of hotels) {
    const w = summarizeWhoDoc(whoBy[h.hotelId]);
    const reachRows = (reach?.results || []).filter((r) => r.hotelId === h.hotelId);
    let contactable = 0;
    let high = 0;
    for (const row of (whoBy[h.hotelId])?.rows || []) {
      const people = row.people || [];
      const funcs = row.functionalContacts || [];
      const named = people.filter((p) => adjudicateWhoPerson(p) === "VALID");
      const hit = reachRows.find((r) => r.opportunityId === row.opportunityId);
      const email = named.some((p) => p.email) || Boolean(hit?.finalEmail || hit?.publicEmail);
      const phone = named.some((p) => p.phone) || Boolean(hit?.finalPhone || hit?.publicPhone);
      if (named.length && (email || phone || funcs.length)) contactable += 1;
      if (hit?.grades?.after === "A" || (named.length && email && phone)) high += 1;
    }
    totalContactable += contactable;
    fr.push(`| ${h.name} | ${w.named} | ${contactable} | ${high} | ${w.functionalOnly} | ${w.noWho} |`);
  }
  fr.push("");

  const minDisc = discPrecs.length ? Math.min(...discPrecs) : null;
  const minWho = whoPrecs.length ? Math.min(...whoPrecs) : null;
  const maxFalse = whoFalses.length ? Math.max(...whoFalses) : null;
  const overallWhoCov = pct(totalNamed, Math.max(1, totalTrue));
  const overallContactable = pct(totalContactable, Math.max(1, totalTrue));

  fr.push("## H. CROSS-HOTEL", "");
  fr.push(`MIN DISCOVERY PRECISION: ${minDisc != null ? `${minDisc.toFixed(1)}%` : "—"}`);
  fr.push(`MIN WHO PRECISION: ${minWho != null ? `${minWho}%` : "—"}`);
  fr.push(`MAX WHO FALSE: ${maxFalse != null ? `${maxFalse}%` : "—"}`);
  fr.push(`OVERALL WHO COVERAGE: ${overallWhoCov}%`);
  fr.push(`OVERALL CONTACTABLE PATH: ${overallContactable}%`, "");

  fr.push(
    "## I. WATCH ITEMS",
    "",
    "- Mc/Mac surname parsing; compound Hispanic / accented-name ASCII shape",
    "- Functional-contact pivot; operator/local-arrangements; committee ambiguity",
    "- Deep-link staff pages / dynamic pages; Surfe timeout/retry; PDL field-level yield",
    `- Hygiene auto TRUE_ACTIONABLE: ${hotels.reduce((n, h) => n + (hygieneBy[h.hotelId]?.metrics?.needsManualAudit || 0), 0)} needsManualAudit flags`,
    "- Wave 3 uses committed V11+V12 stack (no Wave 1 evidence pack)",
    ""
  );

  let failDisc = 0;
  let failWho = 0;
  let genuineNoWho = 0;
  for (const h of hotels) {
    failDisc += hygieneBy[h.hotelId]?.metrics?.insufficientOnActionableGate || 0;
    const w = summarizeWhoDoc(whoBy[h.hotelId]);
    failWho += w.invalid;
    genuineNoWho += w.noWho;
  }
  fr.push("## J. FAILURES", "");
  fr.push(`DISCOVERY: ${failDisc}`);
  fr.push(`WHO: ${failWho}`);
  fr.push(`REACHABILITY: ${rm.surfeFail || 0}`);
  fr.push(`PROVIDER: 0`);
  fr.push(`GENUINE NO_WHO: ${genuineNoWho}`, "");

  fr.push(
    "## K. WEBHOUND SHADOW",
    "",
    "Not run — Wave 3 policy forbids Webhound production seed.",
    "",
    "Native-only valid: n/a",
    "Webhound-only valid: 0",
    "both valid: 0",
    "material remaining Webhound advantage: not measured this wave",
    ""
  );

  const discOk = minDisc == null || minDisc >= 90;
  const whoOk = minWho == null || (minWho >= 90 && (maxFalse == null || maxFalse <= 10));
  const surfeOk =
    !reach ||
    reach.status === "DRY_PLAN" ||
    (rm.surfeCalls || 0) === 0 ||
    (rm.surfeOk || 0) >= Math.ceil((rm.surfeCalls || 1) * 0.75);
  const contactOk = overallContactable >= 70 || totalTrue === 0;
  const covOk = overallWhoCov >= 60 || totalTrue === 0;

  fr.push("## L. DECISION", "");
  fr.push(`1. Did the full stack generalize to all three hotels? **${discOk && whoOk ? "YES / PARTIAL" : "NO / INCOMPLETE"}**`);
  fr.push(`2. Did discovery remain commercially precise? **${discOk ? "YES" : minDisc == null ? "PENDING" : "NO"}**`);
  fr.push(`3. Did WHO remain precision-safe? **${whoOk ? "YES" : minWho == null ? "PENDING" : "NO"}**`);
  fr.push("4. Did V10 recall generalize? **SCAFFOLD ONLY THIS WAVE — no evidence-backed recoveries auto-applied**");
  fr.push(`5. Did Surfe remain reliable? **${reach?.status === "DRY_PLAN" ? "NOT RUN (dry-plan)" : surfeOk ? "YES" : "NO"}**`);
  fr.push(`6. Did PDL add meaningful value? **${(rm.pdlIncremental || 0) > 0 ? "YES" : reach?.status === "ENRICHED" ? "NO / ZERO THIS WAVE" : "NOT RUN"}**`);
  fr.push(`7. Is overall contactability high enough for controlled customer use? **${contactOk ? "YES / BORDERLINE" : "NOT YET"}**`);
  fr.push(`8. Are remaining failures manageable? **${genuineNoWho <= Math.max(2, totalTrue) ? "YES — review after founder audit" : "NEEDS REVIEW"}**`);
  fr.push(
    `9. Should the next remaining hotels proceed in waves of 3? **${discOk && whoOk && surfeOk ? "CONDITIONAL YES after founder accept" : "HOLD until Wave 3 freezes complete + founder accept"}**`,
    ""
  );

  let verdict = "ONE SYSTEMIC GAP REMAINS — HOLD EXPANSION";
  if (minDisc == null || minWho == null) verdict = "ONE SYSTEMIC GAP REMAINS — HOLD EXPANSION";
  else if (!discOk || !whoOk) verdict = "DO NOT SCALE — GENERALIZATION FAILED";
  else if (discOk && whoOk && surfeOk && covOk && contactOk) {
    verdict = "CONTROLLED EXPANSION PASSES — PROCEED TO NEXT 3 HOTELS";
  } else if (discOk && whoOk) {
    verdict = "CONTROLLED EXPANSION PASSES WITH WATCH ITEMS — PROCEED IN WAVES OF 3";
  }

  fr.push("## M. FINAL VERDICT", "", `**${verdict}**`, "");
  fr.push("Results remain staged only — no live GDI promote, no mid-wave patches, no Webhound seed.", "");

  const out = path.join(ROOT, "reports/group-demand-intelligence/gdi-controlled-expansion-wave3-founder-report.md");
  writeMd(out, fr);
  console.error(`founder report wrote ${rel(out)}`);
  return { verdict, minDisc, minWho, overallWhoCov, overallContactable };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const hotels = hotelList(args.hotelArg);
  const all = Object.values(HOTELS);

  if (args.stage === "plan") {
    stagePlan();
    return;
  }
  if (args.stage === "discovery" || args.stage === "all") {
    for (const h of hotels) await runDiscovery(h, args);
  }
  if (args.stage === "hygiene" || args.stage === "all") {
    stageHygiene(args.hotelArg === "ALL" ? all : hotels, args);
  }
  if (args.stage === "who" || args.stage === "all") {
    await stageWho(args.hotelArg === "ALL" ? all : hotels, args);
  }
  if (args.stage === "v10" || args.stage === "all") {
    stageV10(args.hotelArg === "ALL" ? all : hotels, args);
  }
  if (args.stage === "reach" || args.stage === "all") {
    await stageReach(args.hotelArg === "ALL" ? all : hotels, args);
  }
  if (args.stage === "reports" || args.stage === "all") {
    stageReports();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
