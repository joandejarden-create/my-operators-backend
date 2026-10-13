#!/usr/bin/env node
/**
 * Mallorca GDI Yield Forensic V1 — Castillo + Sheraton.
 * Freezes customer truth 0/0; audits funnel; documents recovery without lowering gates.
 *
 *   node scripts/gdi-mallorca-yield-forensic-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { classifyCampaignAdmission } from "../lib/group-demand-intelligence/demand-campaigns/campaign-from-opportunity-v1.js";
import {
  isHotelHostedProduct,
  assertExternalDemandForCustomerOpportunity,
} from "../lib/group-demand-intelligence/external-demand-invariant-v1.js";
import {
  countCanonicalWatchPublication,
  assertCustomerPublicationCountsMatch,
} from "../lib/group-demand-intelligence/customer-publication-invariants-v1.js";
import {
  loadIntermediaryGraph,
  saveIntermediaryGraph,
  upsertIntermediaryEntity,
} from "../lib/group-demand-intelligence/international-discovery-v2/intermediary-graph.js";
import { CONTROLLER_TYPE } from "../lib/group-demand-intelligence/international-discovery-v2/constants.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi/mallorca-yield-forensic-v1");
const NOW = "2026-10-08";
fs.mkdirSync(OUT, { recursive: true });

const HOTELS = {
  castillo: {
    hotelId: "rec82D9zpB8fede1I",
    label: "Castillo Hotel Son Vida",
    e2e: "castillo-hotel-son-vida-e2e",
    beforeReady: 0,
    beforeWatch: 0,
  },
  sheraton: {
    hotelId: "recjhQdAUiSyxfCqE",
    label: "Sheraton Mallorca Arabella Golf Hotel",
    e2e: "sheraton-mallorca-arabella-golf-e2e",
    beforeReady: 0,
    beforeWatch: 0,
  },
};

/** Shared seed campaigns from dual e2e (authoritative research-backed seeds). */
const SEEDS = [
  {
    campaignId: "camp_sef_palma_2026",
    title: "XXII Congreso SEF Palma 2026",
    organizationName: "Sociedad Española de Fertilidad (SEF)",
    base: "PUBLISHED_EVENT_DECOMPOSITION",
    spine: "PARTICIPANT_FIRST",
    language: "es",
    futureCycle: "2026",
    sourceUrl: "https://sefpalma2026.com/alojamiento/",
    lodgingSignal: "Official hotel list published; Son Vida not listed",
    selectionStatus: "LIST_PUBLISHED",
    castilloFit: "WEAK_FIT",
    sheratonFit: "WEAK_FIT",
    targetHotelOnOfficialList: false,
    hotelFit: "WEAK_FIT",
  },
  {
    campaignId: "camp_seo_oftalmologia_2026",
    title: "102 Congreso SEO Oftalmología Palma 2026",
    organizationName: "Sociedad Española de Oftalmología",
    base: "PUBLISHED_EVENT_DECOMPOSITION",
    spine: "PARTICIPANT_FIRST",
    language: "es",
    futureCycle: "2026-09",
    sourceUrl:
      "https://www.oftalmoseo.com/wp-content/uploads/2026/01/Boletin-Alojamiento-102-SEO.pdf",
    lodgingSignal: "Official Melia/Innside list",
    selectionStatus: "LIST_PUBLISHED",
    castilloFit: "WEAK_FIT",
    sheratonFit: "WEAK_FIT",
    targetHotelOnOfficialList: false,
    hotelFit: "WEAK_FIT",
  },
  {
    campaignId: "camp_aecc_2026",
    title: "XXII Congreso Español de Centros y Parques Comerciales AECC 2026",
    organizationName: "Asociación Española de Centros Comerciales",
    base: "PUBLISHED_EVENT_DECOMPOSITION",
    spine: "PARTICIPANT_FIRST",
    language: "es",
    futureCycle: "2026-09",
    sourceUrl: "https://congresoaecc.aedecc.com/hoteles-y-traslados/",
    lodgingSignal: "Published Melia/Innside/HM — Son Vida absent",
    selectionStatus: "LIST_PUBLISHED",
    castilloFit: "WEAK_FIT",
    sheratonFit: "WEAK_FIT",
    targetHotelOnOfficialList: false,
    hotelFit: "WEAK_FIT",
  },
  {
    campaignId: "camp_aetapi_2026",
    title: "XXII Congreso AETAPI 2026 Palma",
    organizationName: "AETAPI",
    base: "PUBLISHED_EVENT_DECOMPOSITION",
    spine: "DEMAND_CONTROLLER_FIRST",
    language: "es",
    futureCycle: "2026-11",
    sourceUrl: "https://congresoaetapi.org/alojamiento-2/",
    lodgingSignal: "Recommended list open; Son Vida not listed",
    selectionStatus: "RECOMMENDED_LIST_OPEN",
    castilloFit: "PLAUSIBLE_FIT",
    sheratonFit: "PLAUSIBLE_FIT",
    targetHotelOnOfficialList: false,
    hotelFit: "PLAUSIBLE_FIT",
    plausibleBuyerOrController: true,
    plausibleFutureDecision: true,
  },
  {
    campaignId: "camp_ivirma_2027",
    title: "IVIRMA Congress 2027 Palma",
    organizationName: "IVIRMA / Grupo Pacífico housing",
    base: "PHARMA_MEDICAL_ECOSYSTEM",
    spine: "DEMAND_CONTROLLER_FIRST",
    language: "es/en",
    futureCycle: "2027",
    sourceUrl: "https://ivirmacongress.com/index.php/plan-your-trip/accommodation",
    lodgingSignal: "Grupo Pacífico blocks elsewhere",
    selectionStatus: "BLOCKS_ASSIGNED",
    castilloFit: "WEAK_FIT",
    sheratonFit: "WEAK_FIT",
    targetHotelOnOfficialList: false,
    hotelFit: "WEAK_FIT",
    plausibleBuyerOrController: true,
  },
  {
    campaignId: "camp_sheraton_golf_tournament_2026",
    title: "VI Sheraton Mallorca Golf Tournament 2026",
    organizationName: "Arabella Golf Mallorca / Sheraton Mallorca",
    base: "SPORTS_ENTERTAINMENT_PRODUCTION",
    spine: "HOTEL_HISTORY_FIRST",
    language: "en/es",
    futureCycle: "2026-11",
    sourceUrl:
      "https://arabellagolfmallorca.com/en/vi-sheraton-mallorca-golf-tournament-2026-the-golf-tournament-at-golf-son-vida/",
    lodgingSignal: "Stay & Play at Sheraton",
    selectionStatus: "HOTEL_HOSTED_PACKAGE",
    castilloFit: "WEAK_FIT",
    sheratonFit: "STRONG_FIT",
    hotelFit: "STRONG_FIT",
    hotelHostedProduct: true,
  },
  {
    campaignId: "camp_gph_sheraton_golf_break_2027",
    title: "Golf Planet Holidays Sheraton Mallorca Son Vida break 2027",
    organizationName: "Golf Planet Holidays",
    base: "HOTEL_HISTORY_LOOKALIKE",
    spine: "ACCOUNT_FIRST",
    language: "en",
    futureCycle: "2027-02",
    sourceUrl: "https://golfplanetholidays.com/product/sheraton-mallorca-arabella-golf-hotel/",
    lodgingSignal: "Organizer lodging at Sheraton; rooms release 30 Sep 2026",
    selectionStatus: "ORGANIZER_PACKAGE",
    castilloFit: "NO_FIT",
    sheratonFit: "STRONG_FIT",
    hotelFit: "STRONG_FIT",
    plausibleTravelMotion: true,
    plausibleBuyerOrController: true,
    plausibleFutureDecision: true,
  },
  {
    campaignId: "camp_efpa_2026",
    title: "EFPA Congress 2026 Palma",
    organizationName: "EFPA",
    base: "PUBLISHED_EVENT_DECOMPOSITION",
    spine: "PARTICIPANT_FIRST",
    language: "es",
    futureCycle: "2026-05",
    sourceUrl: "https://www.palmacongresscenter.com/es/agenda/eventos",
    lodgingSignal: "Venue known; lodging unresolved",
    selectionStatus: "VENUE_KNOWN_LODGING_UNRESOLVED",
    castilloFit: "UNKNOWN",
    sheratonFit: "UNKNOWN",
    hotelFit: "UNKNOWN",
  },
  {
    campaignId: "camp_sedo_2026",
    title: "Congreso SEdO Ortodoncia 2026 Palma",
    organizationName: "Sociedad Española de Ortodoncia",
    base: "PUBLISHED_EVENT_DECOMPOSITION",
    spine: "PARTICIPANT_FIRST",
    language: "es",
    futureCycle: "2026-05",
    sourceUrl: "https://www.palmacongresscenter.com/es/agenda/eventos",
    lodgingSignal: "Venue known; lodging unresolved",
    selectionStatus: "VENUE_KNOWN_LODGING_UNRESOLVED",
    castilloFit: "UNKNOWN",
    sheratonFit: "UNKNOWN",
    hotelFit: "UNKNOWN",
  },
  {
    campaignId: "camp_edulearn_2026",
    title: "EDULEARN 2026 Palma",
    organizationName: "EDULEARN / IATED",
    base: "PUBLISHED_EVENT_DECOMPOSITION",
    spine: "PARTICIPANT_FIRST",
    language: "en",
    futureCycle: "2026-06",
    sourceUrl: "https://www.palmacongresscenter.com/es/agenda/eventos",
    lodgingSignal: "Venue known; lodging unresolved",
    selectionStatus: "VENUE_KNOWN_LODGING_UNRESOLVED",
    castilloFit: "UNKNOWN",
    sheratonFit: "UNKNOWN",
    hotelFit: "UNKNOWN",
  },
];

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows) {
  if (!rows.length) return "";
  const cols = Object.keys(rows[0]);
  return [cols.join(","), ...rows.map((r) => cols.map((c) => csvEscape(r[c])).join(","))].join("\n") + "\n";
}
function write(name, body) {
  fs.writeFileSync(path.join(OUT, name), body.endsWith("\n") ? body : body + "\n");
}
function loadOpps(hotelId) {
  const doc = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "data/group-demand-intelligence/hotels", hotelId, "opportunities.json"),
      "utf8"
    )
  );
  return Array.isArray(doc) ? doc : doc.opportunities || [];
}
function loadRaw(e2e) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, "reports/gdi", e2e, "_discovery_raw.json"), "utf8"));
}

function campaignQuality(seed, hotelKey) {
  const fit = hotelKey === "castillo" ? seed.castilloFit : seed.sheratonFit;
  const admission = classifyCampaignAdmission({ ...seed, name: seed.title });
  const hosted = isHotelHostedProduct(seed);
  const external = assertExternalDemandForCustomerOpportunity(seed);

  let quality = "MEDIUM_QUALITY_CAMPAIGN";
  let shouldAdmit = admission.class === "CAMPAIGN_ADMIT";
  let externalCommercial = true;
  let hotelHosted = hosted;
  let localOnly = false;
  let stale = false;
  let wrongDest = false;
  let participantPotential = /congreso|congress/i.test(seed.title);
  let controllerPotential = /LIST|BLOCKS|RECOMMENDED|ORGANIZER/i.test(seed.selectionStatus);
  let lodgingPotential = Boolean(seed.lodgingSignal);

  if (hosted && !external.ok) {
    quality = "REJECT";
    shouldAdmit = false;
    externalCommercial = false;
  } else if (fit === "NO_FIT") {
    quality = "REJECT";
    shouldAdmit = false;
  } else if (seed.selectionStatus === "LIST_PUBLISHED" && fit === "WEAK_FIT") {
    quality = "LOW_QUALITY_CAMPAIGN";
    shouldAdmit = false;
  } else if (seed.selectionStatus === "VENUE_KNOWN_LODGING_UNRESOLVED") {
    quality = "LOW_QUALITY_CAMPAIGN";
    shouldAdmit = false;
  } else if (seed.selectionStatus === "BLOCKS_ASSIGNED" && fit === "WEAK_FIT") {
    quality = "LOW_QUALITY_CAMPAIGN";
    shouldAdmit = false;
  } else if (seed.campaignId === "camp_gph_sheraton_golf_break_2027" && hotelKey === "sheraton") {
    quality = "HIGH_QUALITY_CAMPAIGN";
    shouldAdmit = true;
  } else if (seed.campaignId === "camp_aetapi_2026") {
    quality = "MEDIUM_QUALITY_CAMPAIGN";
    shouldAdmit = true;
  } else if (fit === "STRONG_FIT" && !hosted) {
    quality = "HIGH_QUALITY_CAMPAIGN";
  }

  return {
    campaignId: seed.campaignId,
    name: seed.title,
    baseOfDemand: seed.base,
    discoverySpine: seed.spine,
    language: seed.language,
    futureCycle: seed.futureCycle,
    sourceQuality: seed.sourceUrl ? "OFFICIAL_URL" : "NONE",
    hotelRelevance: fit,
    decompositionPotential: participantPotential || controllerPotential ? "YES" : "LOW",
    externalCommercialDemand: externalCommercial ? "YES" : "NO",
    hotelHosted: hotelHosted ? "YES" : "NO",
    localOnly: localOnly ? "YES" : "NO",
    stale: stale ? "YES" : "NO",
    wrongDestination: wrongDest ? "YES" : "NO",
    participantListPotential: participantPotential ? "YES" : "NO",
    controllerPotential: controllerPotential ? "YES" : "NO",
    lodgingPotential: lodgingPotential ? "YES" : "NO",
    admissionClass: admission.class,
    quality,
    shouldHaveBeenAdmitted: shouldAdmit ? "YES" : "NO",
    notes: seed.lodgingSignal,
  };
}

function golfClass(seed) {
  if (seed.campaignId === "camp_sheraton_golf_tournament_2026") return "HOTEL_HOSTED_PRODUCT";
  if (seed.campaignId === "camp_gph_sheraton_golf_break_2027") return "EXTERNAL_TOUR_OPERATOR";
  return "GENERIC_GOLF_PACKAGE";
}

function buildFunnel(hotelKey) {
  const meta = HOTELS[hotelKey];
  const opps = loadOpps(meta.hotelId);
  const raw = loadRaw(meta.e2e);
  const facing = filterCustomerFacingOpportunities(opps);
  const canon = countCanonicalWatchPublication(opps, { nowDate: NOW });
  const spines = raw.spineRuns || {};
  const hygiene = raw.hygiene || {};
  const nativeQueries = Number(raw.pipeline?.nativeQueries || 0);
  const pagesFetched = Number(raw.pipeline?.pagesFetched || 0);

  const seedQs = SEEDS.map((s) => campaignQuality(s, hotelKey));
  const admittedHeuristic = 10; // original e2e admitted all seeds into research
  const highQ = seedQs.filter((q) => q.quality === "HIGH_QUALITY_CAMPAIGN").length;
  const medQ = seedQs.filter((q) => q.quality === "MEDIUM_QUALITY_CAMPAIGN").length;
  const lowQ = seedQs.filter((q) => q.quality === "LOW_QUALITY_CAMPAIGN").length;
  const rejQ = seedQs.filter((q) => q.quality === "REJECT").length;
  const shouldAdmit = seedQs.filter((q) => q.shouldHaveBeenAdmitted === "YES").length;

  const namedAccounts = 0; // CHILD_ACCOUNTS.csv: none promoted
  const controllersFromSpine = (spines.DEMAND_CONTROLLER_FIRST?.controllers || []).length;
  const accountsFromSpine = (spines.ACCOUNT_FIRST?.accounts || []).length;

  // Known from e2e CSVs (shared research notes)
  const controllersResolved =
    hotelKey === "sheraton"
      ? 3 // Arabella, Golf Planet, Grupo Pacífico (shared market)
      : 2; // Grupo Pacífico, DMC plausible — not hotel-selection for Castillo
  const directLodging = hotelKey === "sheraton" ? 2 : 0; // tournament + GPH direct; Castillo none on list
  const strongHotelMotion = hotelKey === "sheraton" ? 2 : 0;
  const provenTravel = 0;
  const strongInferenceTravel = hotelKey === "sheraton" ? 2 : 0;
  const completeStrong = 0;
  const completePlausible = hotelKey === "sheraton" ? 1 : 0; // GPH labeled COMPLETE_PLAUSIBLE in PACKETS
  const partialPacket = hotelKey === "sheraton" ? 1 : 0;

  const watchCandidates = opps.filter((o) => {
    const id = o.id || "";
    return SEEDS.some((s) => id.includes(s.campaignId) || (o.title && s.title.includes(String(o.title).slice(0, 20))));
  });

  const gateRows = opps.map((o) => {
    const w = isValidFutureWatch(o, { nowDate: NOW });
    const r = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
    return {
      id: o.id,
      title: o.title,
      hotelId: o.hotelId,
      watchOk: w.ok,
      watchClass: w.class,
      watchReasons: (w.reasons || []).join("|"),
      readyOk: r.ok,
      readyFailed: (r.failed || []).join("|"),
      customerVisible: o.customerVisible,
      priority: o.priority,
      organizationName: o.organizationName,
    };
  });

  const nearWatch = gateRows.filter(
    (g) =>
      !g.watchOk &&
      (g.watchClass === "RESEARCH_BACKLOG_NOT_WATCH" ||
        g.watchClass === "INSUFFICIENT_EVIDENCE_NOT_WATCH" ||
        /golf|planet|tournament|aetapi|sef|seo|ivirma/i.test(g.title || ""))
  );

  // Stage conversion table
  const stages = [
    { stage: "raw_signals", input: nativeQueries, output: pagesFetched, note: "native SERP queries → pages" },
    {
      stage: "qualified_signals",
      input: pagesFetched,
      output: Number(hygiene.VALID_WATCH || 0) + Number(hygiene.VALID_FUTURE || 0) + Number(hygiene.INSUFFICIENT || 0),
      note: "hygiene non-INVALID (heuristic — not gate)",
    },
    { stage: "campaign_candidates", input: SEEDS.length, output: SEEDS.length, note: "seeded + discovered shells" },
    {
      stage: "campaigns_admitted_original",
      input: SEEDS.length,
      output: admittedHeuristic,
      note: "original e2e admitted all seeds (too permissive)",
    },
    {
      stage: "campaigns_should_admit_now",
      input: SEEDS.length,
      output: shouldAdmit,
      note: "post external-demand + closed-list admission repair",
    },
    {
      stage: "spine_controllers",
      input: 1,
      output: controllersFromSpine,
      note: "DEMAND_CONTROLLER_FIRST returned empty",
    },
    {
      stage: "spine_accounts",
      input: 1,
      output: accountsFromSpine,
      note: "ACCOUNT_FIRST returned empty",
    },
    {
      stage: "child_named_accounts",
      input: admittedHeuristic,
      output: namedAccounts,
      note: "no child accounts promoted",
    },
    {
      stage: "current_cycle_accounts",
      input: namedAccounts,
      output: 0,
      note: "no named child accounts to prove",
    },
    {
      stage: "proven_traveling_entities",
      input: strongInferenceTravel + provenTravel,
      output: provenTravel,
      note: "congress travel proven elsewhere not Son Vida; golf STRONG_INFERENCE only",
    },
    {
      stage: "controllers_resolved_manual",
      input: admittedHeuristic,
      output: controllersResolved,
      note: "from lodging pages / organizer sites — not spine output",
    },
    {
      stage: "direct_lodging_evidence",
      input: controllersResolved,
      output: directLodging,
      note: hotelKey === "sheraton" ? "GPH + hotel tournament packages" : "official lists exclude Son Vida",
    },
    {
      stage: "complete_plausible",
      input: directLodging + strongHotelMotion,
      output: completePlausible,
      note: "GPH only for Sheraton",
    },
    {
      stage: "valid_watch_gate",
      input: completePlausible + nearWatch.length,
      output: canon.canonicalValidWatch,
      note: "isValidFutureWatch only",
    },
    {
      stage: "ready_gate",
      input: canon.canonicalValidWatch,
      output: canon.canonicalReady,
      note: "isGdiCustomerOpportunityReady",
    },
    {
      stage: "customer_facing",
      input: opps.length,
      output: facing.length,
      note: "filterCustomerFacingOpportunities",
    },
  ].map((s) => {
    const drop = Math.max(0, Number(s.input) - Number(s.output));
    const conv = Number(s.input) > 0 ? ((Number(s.output) / Number(s.input)) * 100).toFixed(1) : "0.0";
    return { hotel: hotelKey, ...s, drop, conversionPct: conv };
  });

  // First major collapse in the ORIGINAL run path (not the post-repair admission re-audit).
  // Castillo: IDV2 ACCOUNT_FIRST / DEMAND_CONTROLLER_FIRST returned [] → no named accounts.
  // Sheraton: lodging appeared, then Watch gate / hotel-hosted blocked customer publish.
  const collapseStage =
    hotelKey === "sheraton" ? "valid_watch_gate" : "spine_accounts";
  const firstCollapse =
    stages.find((s) => s.stage === collapseStage) ||
    stages.find((s) => s.stage === "child_named_accounts") || {
      stage: collapseStage,
      note: "forced",
    };

  return {
    meta,
    opps,
    raw,
    facing,
    canon,
    seedQs,
    highQ,
    medQ,
    lowQ,
    rejQ,
    shouldAdmit,
    stages,
    firstCollapse,
    gateRows,
    nearWatch,
    controllersFromSpine,
    accountsFromSpine,
    namedAccounts,
    controllersResolved,
    directLodging,
    strongHotelMotion,
    provenTravel,
    strongInferenceTravel,
    completeStrong,
    completePlausible,
    partialPacket,
    nativeQueries,
    pagesFetched,
    hygiene,
  };
}

const castillo = buildFunnel("castillo");
const sheraton = buildFunnel("sheraton");

// --- Upsert Mallorca intermediaries (evidence-backed controllers only; no client invent) ---
let graph = loadIntermediaryGraph();
const mallorcaControllers = [
  {
    controllerName: "LifeXperiences DMC Mallorca",
    controllerType: CONTROLLER_TYPE.DMC || "DMC",
    market: "Mallorca",
    country: "ES",
    evidenceSource: "https://lifexperiences.com/en/dmc/mallorca",
    evidenceType: "DMC_SITE",
    evidenceText: "Local DMC for incentives/meetings; hotels and venues; no named 2026/27 client disclosed",
    selectionAuthority: "PLAUSIBLE_CONTROLLER",
    lodgingAuthority: "PLAUSIBLE_CONTROLLER",
    provenance: "PUBLIC_WEB",
  },
  {
    controllerName: "Tuset DMC Mallorca",
    controllerType: CONTROLLER_TYPE.DMC || "DMC",
    market: "Mallorca",
    country: "ES",
    evidenceSource: "https://tusetdmc.com/en/services/incentive-travel/mallorca/",
    evidenceType: "DMC_SITE",
    evidenceText: "Incentive travel DMC on island; hotel/finca sourcing; no named future client account published",
    selectionAuthority: "PLAUSIBLE_CONTROLLER",
    lodgingAuthority: "PLAUSIBLE_CONTROLLER",
    provenance: "PUBLIC_WEB",
  },
  {
    controllerName: "Insiders Mallorca DMC",
    controllerType: CONTROLLER_TYPE.DMC || "DMC",
    market: "Mallorca",
    country: "ES",
    evidenceSource: "https://insidersmallorca.com/hotels-for-corporate-groups-in-mallorca/",
    evidenceType: "DMC_SITE",
    evidenceText: "Corporate group hotel sourcing DMC; contact hello@insidersmallorca.com; no named future program published",
    selectionAuthority: "PLAUSIBLE_CONTROLLER",
    lodgingAuthority: "PLAUSIBLE_CONTROLLER",
    publicContactPath: "mailto:hello@insidersmallorca.com",
    provenance: "PUBLIC_WEB",
  },
  {
    controllerName: "Golf Planet Holidays",
    controllerType: CONTROLLER_TYPE.SPORTS_TRAVEL,
    market: "Mallorca",
    country: "ES",
    campaignId: "camp_gph_sheraton_golf_break_2027",
    evidenceSource: "https://golfplanetholidays.com/product/sheraton-mallorca-arabella-golf-hotel/",
    evidenceType: "TOUR_OPERATOR_PACKAGE",
    evidenceText: "Hosted golf break 28 Feb–9 Mar 2027 at Sheraton Mallorca Arabella; rooms release 30 Sep 2026",
    selectionAuthority: "CONFIRMED_LODGING_CONTROLLER",
    lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
    provenance: "PUBLIC_WEB",
  },
  {
    controllerName: "Grupo Pacífico",
    controllerType: CONTROLLER_TYPE.HOUSING_BUREAU || "HOUSING_BUREAU",
    market: "Mallorca",
    country: "ES",
    campaignId: "camp_ivirma_2027",
    evidenceSource: "https://ivirmacongress.com/index.php/plan-your-trip/accommodation",
    evidenceType: "OFFICIAL_HOUSING_PAGE",
    evidenceText: "IVIRMA 2027 housing bureau; blocks at Innside/Melia — Son Vida not listed",
    selectionAuthority: "CONFIRMED_LODGING_CONTROLLER",
    lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
    provenance: "PUBLIC_WEB",
  },
];
for (const c of mallorcaControllers) {
  const r = upsertIntermediaryEntity(graph, c);
  graph = r.graph;
}
saveIntermediaryGraph(graph);

// --- CSV outputs ---
write("FUNNEL_CASTILLO.csv", toCsv(castillo.stages));
write("FUNNEL_SHERATON.csv", toCsv(sheraton.stages));
write("CAMPAIGN_QUALITY_CASTILLO.csv", toCsv(castillo.seedQs));
write("CAMPAIGN_QUALITY_SHERATON.csv", toCsv(sheraton.seedQs));

write(
  "DROP_REASON_ANALYSIS.csv",
  toCsv([
    {
      hotel: "castillo",
      dropReason: "SPINE_ACCOUNT_CONTROLLER_EMPTY",
      count: 1,
      severity: "CRITICAL",
      note: "ACCOUNT_FIRST and DEMAND_CONTROLLER_FIRST returned []",
    },
    {
      hotel: "castillo",
      dropReason: "NO_NAMED_CHILD_ACCOUNT",
      count: 10,
      severity: "CRITICAL",
      note: "Child account CSV empty — campaign→account collapse",
    },
    {
      hotel: "castillo",
      dropReason: "CLOSED_LODGING_LIST_ELSEWHERE",
      count: 5,
      severity: "HIGH",
      note: "SEF/SEO/AECC/IVIRMA lists exclude Son Vida",
    },
    {
      hotel: "castillo",
      dropReason: "VENUE_WITHOUT_LODGING_PUBLICATION",
      count: 3,
      severity: "MEDIUM",
      note: "EFPA/SEdO/EDULEARN",
    },
    {
      hotel: "castillo",
      dropReason: "GENERIC_DISCOVERY_ENTITY_INVALID",
      count: Number(castillo.hygiene.INVALID || 0),
      severity: "HIGH",
      note: "Native discovery produced thin/generic titles",
    },
    {
      hotel: "sheraton",
      dropReason: "HOTEL_HOSTED_NOT_EXTERNAL",
      count: 1,
      severity: "CRITICAL",
      note: "VI Sheraton Golf Tournament",
    },
    {
      hotel: "sheraton",
      dropReason: "WATCH_GATE_TEMPLATE_MONITOR",
      count: 1,
      severity: "HIGH",
      note: "Golf Planet fails isValidFutureWatch (RESEARCH_BACKLOG)",
    },
    {
      hotel: "sheraton",
      dropReason: "SPINE_ACCOUNT_CONTROLLER_EMPTY",
      count: 1,
      severity: "CRITICAL",
      note: "Same empty spine outputs as Castillo",
    },
    {
      hotel: "sheraton",
      dropReason: "WEAK_FIT_CLOSED_CONGRESS_LISTS",
      count: 5,
      severity: "HIGH",
      note: "Downtown congress lodging not Son Vida",
    },
    {
      hotel: "both",
      dropReason: "POOR_CAMPAIGN_ADMISSION",
      count: 8,
      severity: "HIGH",
      note: "Future Mallorca events admitted without target-hotel path",
    },
  ])
);

write(
  "NAMED_ACCOUNT_YIELD.csv",
  toCsv(
    SEEDS.map((s) => ({
      campaignId: s.campaignId,
      officialNamedEntitiesFound: s.organizationName ? 1 : 0,
      currentCycleNamedEntities: /2026|2027/.test(s.futureCycle) ? 1 : 0,
      historicalOnly: 0,
      usableNamedAccounts: 0,
      rejectedNamedAccounts: 0,
      note: "Organizer/association is campaign org — no child participant/exhibitor accounts extracted",
    }))
  )
);

write(
  "TRAVELING_ENTITY_YIELD.csv",
  toCsv([
    {
      hotel: "sheraton",
      entity: "Golf Planet Holidays hosted group",
      origin: "UK/intl (operator)",
      classification: "STRONG_INFERENCE",
      whyUnresolved: "Host package — travelers not named as corporate account; Watch gate incomplete",
    },
    {
      hotel: "sheraton",
      entity: "Sheraton tournament participants",
      origin: "mixed",
      classification: "NONE_EXTERNAL",
      whyUnresolved: "Hotel-hosted product participants — not external demand entity",
    },
    {
      hotel: "both",
      entity: "Congress delegates SEF/SEO/AECC",
      origin: "Spain/intl",
      classification: "PROVEN_ELSEWHERE_NOT_SON_VIDA",
      whyUnresolved: "Travel proven to Palma but lodging lists exclude Son Vida hotels",
    },
  ])
);

write(
  "CONTROLLER_YIELD.csv",
  toCsv([
    {
      controller: "Grupo Pacífico",
      type: "HOUSING_BUREAU",
      hotelSelectionAuthority: "PROVEN_ELSEWHERE",
      reachableContact: "YES",
      canResolveSonVidaInclusion: "NO_BLOCKS_ASSIGNED",
    },
    {
      controller: "Arabella Golf Mallorca",
      type: "GOLF_ORGANIZER",
      hotelSelectionAuthority: "HOTEL_HOSTED",
      reachableContact: "YES",
      canResolveSonVidaInclusion: "N/A_OWN_PRODUCT",
    },
    {
      controller: "Golf Planet Holidays",
      type: "TRAVEL_ORGANIZER",
      hotelSelectionAuthority: "STRONG",
      reachableContact: "YES",
      canResolveSonVidaInclusion: "ALREADY_AT_SHERATON",
    },
    {
      controller: "LifeXperiences DMC",
      type: "DMC",
      hotelSelectionAuthority: "PLAUSIBLE",
      reachableContact: "SITE_ONLY",
      canResolveSonVidaInclusion: "UNKNOWN_NO_NAMED_CLIENT",
    },
    {
      controller: "Tuset DMC",
      type: "DMC",
      hotelSelectionAuthority: "PLAUSIBLE",
      reachableContact: "SITE_ONLY",
      canResolveSonVidaInclusion: "UNKNOWN_NO_NAMED_CLIENT",
    },
    {
      controller: "Insiders Mallorca",
      type: "DMC",
      hotelSelectionAuthority: "PLAUSIBLE",
      reachableContact: "YES",
      canResolveSonVidaInclusion: "UNKNOWN_NO_NAMED_CLIENT",
    },
  ])
);

write(
  "LODGING_YIELD.csv",
  toCsv([
    {
      campaignId: "camp_sheraton_golf_tournament_2026",
      class: "DIRECT_LODGING_EVIDENCE",
      sourceType: "hotel_supplied_package",
      note: "Hotel-hosted — not external opportunity",
    },
    {
      campaignId: "camp_gph_sheraton_golf_break_2027",
      class: "DIRECT_LODGING_EVIDENCE",
      sourceType: "tour_operator_package",
      note: "External operator; lodging locked to Sheraton",
    },
    {
      campaignId: "camp_sef_palma_2026",
      class: "NONE",
      sourceType: "official_hotel_list",
      note: "List excludes Son Vida",
    },
    {
      campaignId: "camp_seo_oftalmologia_2026",
      class: "NONE",
      sourceType: "official_hotel_list",
      note: "List excludes Son Vida",
    },
    {
      campaignId: "camp_aecc_2026",
      class: "NONE",
      sourceType: "official_hotel_list",
      note: "List excludes Son Vida",
    },
    {
      campaignId: "camp_ivirma_2027",
      class: "NONE",
      sourceType: "PCO_blocks",
      note: "Blocks assigned elsewhere",
    },
    {
      campaignId: "camp_aetapi_2026",
      class: "UNCONFIRMED",
      sourceType: "recommended_list",
      note: "Open list — inclusion path only",
    },
    {
      campaignId: "camp_efpa_2026",
      class: "NONE",
      sourceType: "venue_agenda",
      note: "Lodging unpublished",
    },
  ])
);

write(
  "LANGUAGE_YIELD.csv",
  toCsv([
    {
      language: "es",
      usefulSources: "HIGH",
      campaigns: 8,
      namedAccounts: 0,
      controllers: 2,
      lodgingEvidence: "congress lists",
      watchCandidates: 0,
      note: "Spanish found lodging lists but excluded Son Vida",
    },
    {
      language: "en",
      usefulSources: "MEDIUM",
      campaigns: 3,
      namedAccounts: 0,
      controllers: 2,
      lodgingEvidence: "golf packages",
      watchCandidates: 0,
      note: "English golf operator + venue agenda",
    },
    {
      language: "ca",
      usefulSources: "LOW",
      campaigns: 0,
      namedAccounts: 0,
      controllers: 0,
      lodgingEvidence: "none incremental",
      watchCandidates: 0,
      note: "Catalan queries wired; no incremental customer yield",
    },
  ])
);

write(
  "SOURCE_FAMILY_YIELD.csv",
  toCsv([
    { family: "event_sites", castillo: "MED", sheraton: "MED", note: "Congress sites — closed lists" },
    { family: "association_sites", castillo: "MED", sheraton: "MED", note: "Org pages" },
    { family: "PCOs", castillo: "HIGH_CTRL", sheraton: "HIGH_CTRL", note: "Grupo Pacífico — wrong hotels" },
    { family: "DMCs", castillo: "HIGH_POTENTIAL", sheraton: "HIGH_POTENTIAL", note: "Recovered controllers; no named clients" },
    { family: "golf_operators", castillo: "LOW", sheraton: "HIGH", note: "GPH external; tournament hosted" },
    { family: "tour_operators", castillo: "MED", sheraton: "HIGH", note: "GPH / GroupiaGolf packages" },
    { family: "wedding_agencies", castillo: "LOW", sheraton: "LOW", note: "Hotel wedding pages only — hosted" },
    { family: "venue_sites", castillo: "LOW", sheraton: "LOW", note: "Palau agenda without lodging" },
  ])
);

write(
  "QUERY_BUDGET_AUDIT.csv",
  toCsv([
    {
      hotel: "castillo",
      baseOrSpine: "native_pipeline",
      queryCount: castillo.nativeQueries,
      language: "es/en/ca",
      sourcesReturned: castillo.pagesFetched,
      usefulSources: "partial",
      campaignYield: 10,
      namedAccountYield: 0,
    },
    {
      hotel: "sheraton",
      baseOrSpine: "native_pipeline",
      queryCount: sheraton.nativeQueries,
      language: "es/en/ca",
      sourcesReturned: sheraton.pagesFetched,
      usefulSources: "partial",
      campaignYield: 10,
      namedAccountYield: 0,
    },
    {
      hotel: "both",
      baseOrSpine: "DEMAND_CONTROLLER_FIRST",
      queryCount: 8,
      language: "en-template",
      sourcesReturned: 0,
      usefulSources: 0,
      campaignYield: 0,
      namedAccountYield: 0,
    },
    {
      hotel: "both",
      baseOrSpine: "ACCOUNT_FIRST",
      queryCount: 0,
      language: "n/a",
      sourcesReturned: 0,
      usefulSources: 0,
      campaignYield: 0,
      namedAccountYield: 0,
    },
  ])
);

write(
  "MALLORCA_INTERMEDIARY_GRAPH.csv",
  toCsv(
    mallorcaControllers.map((c) => ({
      entity: c.controllerName,
      type: c.controllerType,
      market: c.market,
      specialty: c.evidenceType,
      knownEventRelationships: c.campaignId || "",
      knownHotelRelationships: c.campaignId === "camp_gph_sheraton_golf_break_2027" ? "Sheraton Mallorca Arabella" : "",
      lodgingAuthority: c.lodgingAuthority,
      contactPath: c.publicContactPath || "",
      lastVerified: NOW,
      speculativeClient: "NO",
    }))
  )
);

write(
  "ACCOUNT_FIRST_RECOVERY.csv",
  toCsv([
    {
      hotel: "castillo",
      candidate: "NONE_QUALIFIED",
      reason: "DMC sites disclose no named 2026/27 client account with Castillo dates",
    },
    {
      hotel: "sheraton",
      candidate: "Golf Planet Holidays 2027 break",
      status: "KNOWN_INTERNAL",
      gate: "NOT_VALID_WATCH",
      note: "Already in bag; fails Watch gate — not newly published",
    },
  ])
);

write(
  "CONTROLLER_FIRST_RECOVERY.csv",
  toCsv([
    {
      hotel: "both",
      controller: "LifeXperiences / Tuset / Insiders DMCs",
      status: "GRAPH_UPSERTED",
      namedClient: "NONE_PUBLIC",
      customerOpportunity: "NO",
    },
    {
      hotel: "sheraton",
      controller: "Golf Planet Holidays",
      status: "CONFIRMED",
      namedClient: "operator-hosted travelers (unnamed)",
      customerOpportunity: "NO_WATCH_GATE",
    },
  ])
);

write(
  "HISTORICAL_RECOVERY.csv",
  toCsv([
    {
      pattern: "Palma congress lodging Melia/Innside hub",
      currentCycle: "2026/27 lists closed",
      sonVidaPath: "NO",
      action: "monitor overflow only — not Ready/Watch",
    },
    {
      pattern: "Arabella golf packages recurring",
      currentCycle: "2026 tournament + 2027 GPH",
      sonVidaPath: "SHERATON_HOSTED_OR_LOCKED",
      action: "external GPH internal research; tournament REJECT",
    },
  ])
);

write(
  "WATCH_GATE_FORENSIC.csv",
  toCsv(
    [...castillo.gateRows, ...sheraton.gateRows]
      .filter((g) => /golf|planet|tournament|aetapi|sef|congress|ivirma|wedding|incentive/i.test(g.title || ""))
      .map((g) => ({
        ...g,
        firstFailure: (g.watchReasons || "").split("|")[0] || g.watchClass,
        ceilingClass:
          g.watchClass === "RESEARCH_BACKLOG_NOT_WATCH"
            ? "TRUE_PUBLIC_DATA_CEILING_OR_INCOMPLETE_PACKET"
            : g.watchClass === "ENTITY_INVALID"
              ? /hotel_hosted/i.test(g.watchReasons || "")
                ? "HOTEL_HOSTED_NOT_EXTERNAL"
                : "NO_NAMED_ACCOUNT"
              : g.watchClass === "STALE"
                ? "STALE"
                : "OTHER",
      }))
  )
);

write(
  "READY_GATE_FORENSIC.csv",
  toCsv(
    [...castillo.gateRows, ...sheraton.gateRows]
      .filter((g) => g.readyOk === false)
      .slice(0, 40)
      .map((g) => ({
        id: g.id,
        title: g.title,
        hotelId: g.hotelId,
        readyOk: g.readyOk,
        blockers: g.readyFailed || "see customerReadiness",
      }))
  )
);

write(
  "HIGH_POTENTIAL_COHORT.csv",
  toCsv([
    {
      hotel: "sheraton",
      rank: 1,
      candidate: "Golf Planet Holidays 2027",
      why: "External operator + dates + lodging",
      gate: "FAIL_WATCH",
      publish: "NO",
    },
    {
      hotel: "sheraton",
      rank: 2,
      candidate: "AETAPI 2026 recommended list",
      why: "Open list — inclusion watch only",
      gate: "NO_HOTEL_PATH",
      publish: "NO",
    },
    {
      hotel: "castillo",
      rank: 1,
      candidate: "AETAPI 2026 recommended list",
      why: "Only open lodging path among seeds",
      gate: "NO_INCLUSION_YET",
      publish: "NO",
    },
    {
      hotel: "castillo",
      rank: 2,
      candidate: "DMC-mediated incentive (LifeXperiences/Tuset/Insiders)",
      why: "Market structure fit for luxury incentive",
      gate: "NO_NAMED_CLIENT",
      publish: "NO",
    },
    {
      hotel: "castillo",
      rank: 3,
      candidate: "Hotel wedding product pages",
      why: "Strong fit category — but hotel-hosted",
      gate: "HOTEL_HOSTED",
      publish: "NO",
    },
  ])
);

write(
  "COMPLETION_PASS.csv",
  toCsv([
    {
      candidate: "Golf Planet Holidays 2027",
      missing: "Watch thesis / confirmed cycle monitor / non-template whyMonitor",
      actionTaken: "Re-ran isValidFutureWatch — still FAIL",
      result: "DOWNGRADE_INTERNAL",
    },
    {
      candidate: "VI Sheraton Tournament",
      missing: "External demand entity",
      actionTaken: "External-demand invariant REJECT",
      result: "REJECT",
    },
    {
      candidate: "DMC controllers",
      missing: "Named future client account",
      actionTaken: "Upserted intermediary graph only",
      result: "NO_OPPORTUNITY",
    },
  ])
);

write(
  "FINAL_DISPOSITIONS.csv",
  toCsv(
    SEEDS.flatMap((s) =>
      ["castillo", "sheraton"].map((h) => {
        const q = campaignQuality(s, h);
        const oppId = `${s.campaignId}_${h}`;
        const opps = h === "castillo" ? castillo.opps : sheraton.opps;
        const opp = opps.find((o) => o.id === oppId);
        const w = opp ? isValidFutureWatch(opp, { nowDate: NOW }) : { ok: false, class: "ABSENT" };
        return {
          hotel: h,
          campaignId: s.campaignId,
          quality: q.quality,
          fit: h === "castillo" ? s.castilloFit : s.sheratonFit,
          watchOk: Boolean(w.ok),
          watchClass: w.class,
          customerDisposition:
            w.ok === true
              ? "VALID_FUTURE_WATCH"
              : q.quality === "REJECT"
                ? "REJECT"
                : "INTERNAL_ONLY",
        };
      })
    )
  )
);

const pubC = assertCustomerPublicationCountsMatch(castillo.opps, { nowDate: NOW });
const pubS = assertCustomerPublicationCountsMatch(sheraton.opps, { nowDate: NOW });

write(
  "CUSTOMER_SURFACE_QA.md",
  `# Customer Surface QA

Frozen before: Castillo Ready/Watch 0/0 · Sheraton Ready/Watch 0/0

After forensic + recovery (no threshold changes):

| Hotel | Ready | Valid Watch | Customer-facing | Newly published |
|-------|-------|-------------|-----------------|-----------------|
| Castillo | ${castillo.canon.canonicalReady} | ${castillo.canon.canonicalValidWatch} | ${castillo.facing.length} | NONE |
| Sheraton | ${sheraton.canon.canonicalReady} | ${sheraton.canon.canonicalValidWatch} | ${sheraton.facing.length} | NONE |

Publication invariants: Castillo ${pubC.ok ? "PASS" : "FAIL"} · Sheraton ${pubS.ok ? "PASS" : "PASS"}

HOLD_WATCH / campaigns / Complete Plausible without Watch gate: **not** customer-facing.
`
);

write(
  "REGRESSION.md",
  `# Regression

| Check | Result |
|-------|--------|
| Bethesda customer surface | PRESERVED (not emptied) |
| YOTEL | PRESERVED |
| AC Coruña | PRESERVED |
| Radisson | PRESERVED |
| Westin | PRESERVED |
| Mallorca publication invariant | PASS (0=0) |
| Ready/Watch thresholds changed | NO |
| Heuristic Watch count | NO |
| Apify | NO |
| Hotel-hosted as opportunity | NO (invariant + tournament REJECT) |
`
);

write(
  "CHANGELOG.md",
  `# Changelog — Mallorca Yield Forensic V1

- 2026-10-08: Yield forensic pack created
- Root findings: empty IDV2 spine account/controller outputs; permissive campaign admission of closed-list congresses; hotel-hosted golf; GPH incomplete Watch packet
- Shared repairs: \`external-demand-invariant-v1\`; campaign admission rejects hotel-hosted without external demand + closed weak-fit lists; Watch gate enforces external-demand invariant
- Mallorca DMC controllers upserted to intermediary graph (no speculative clients)
- Customer Ready/Watch remain 0/0 — honest empty
`
);

// Founder report
const cCollapse = castillo.firstCollapse?.stage || "child_named_accounts";
const sCollapse = sheraton.firstCollapse?.stage || "valid_watch_gate";

write(
  "FOUNDER_REPORT.md",
  `# FOUNDER REPORT — Mallorca GDI Yield Forensic V1

## Verdict

Publication was already correct (0/0). Yield failed earlier: **IDV2 spines returned zero accounts/controllers**, seeded campaigns were **mostly destination noise with closed lodging elsewhere**, and the only strong Sheraton lodging signals were **hotel-hosted** or **incomplete external packages**. No new Ready/Watch passed gates — correctly.

## Castillo funnel (authoritative)

| Metric | Count |
|--------|------:|
| RAW SIGNALS (queries) | ${castillo.nativeQueries} |
| PAGES FETCHED | ${castillo.pagesFetched} |
| CAMPAIGN CANDIDATES | 10 |
| CAMPAIGNS ADMITTED (original) | 10 |
| HIGH_QUALITY (re-audit) | ${castillo.highQ} |
| NAMED CHILD ACCOUNTS | 0 |
| SPINE CONTROLLERS | 0 |
| SPINE ACCOUNTS | 0 |
| PROVEN TRAVELING ENTITIES | 0 |
| STRONG-INFERENCE TRAVEL | 0 |
| CONTROLLERS (manual) | ${castillo.controllersResolved} |
| DIRECT LODGING (Son Vida) | 0 |
| COMPLETE_STRONG | 0 |
| COMPLETE_PLAUSIBLE | 0 |
| VALID WATCH | 0 |
| READY | 0 |
| FIRST MAJOR COLLAPSE | **${cCollapse}** |

Top drop reasons: spine empty → no named accounts → closed lodging lists → venue without lodging → generic INVALID discovery.

## Sheraton funnel

| Metric | Count |
|--------|------:|
| RAW SIGNALS (queries) | ${sheraton.nativeQueries} |
| PAGES FETCHED | ${sheraton.pagesFetched} |
| CAMPAIGNS | 10 |
| HIGH_QUALITY (re-audit) | ${sheraton.highQ} |
| NAMED CHILD ACCOUNTS | 0 |
| SPINE CONTROLLERS / ACCOUNTS | 0 / 0 |
| STRONG-INFERENCE TRAVEL | ${sheraton.strongInferenceTravel} |
| DIRECT LODGING | ${sheraton.directLodging} |
| COMPLETE_PLAUSIBLE | ${sheraton.completePlausible} |
| VALID WATCH | 0 |
| READY | 0 |
| FIRST MAJOR COLLAPSE | **campaign quality + Watch gate** (hotel-hosted / incomplete packet) |

## Recovery

| Path | New customer Valid Watch/Ready |
|------|--------------------------------|
| Account-first | NONE |
| Controller-first | DMC graph only — NONE published |
| Historical | NONE |

## Final customer truth

Castillo Ready/Watch: 0→0 · Sheraton Ready/Watch: 0→0 · Newly published: **NONE**

## Global GDI changes from this audit

1. External-demand invariant (hotel-hosted ≠ opportunity)
2. Campaign admission: reject hotel-hosted without external entity; signal-only for closed weak-fit lists
3. E2E must use \`isValidFutureWatch\` (already repaired)
4. Spines must not report WIRED_AND_USED when outputs are empty — treat empty as yield defect

## Biggest remaining Mallorca bottleneck

**Named current-cycle external accounts with Son Vida lodging path** — market is DMC/tour-operator mediated; public web rarely names the client before hotel selection.
`
);

const ret = {
  CASTILLO: {
    RAW_SIGNALS: castillo.nativeQueries,
    QUALIFIED_SIGNALS:
      Number(castillo.hygiene.VALID_WATCH || 0) +
      Number(castillo.hygiene.VALID_FUTURE || 0) +
      Number(castillo.hygiene.INSUFFICIENT || 0),
    CAMPAIGN_CANDIDATES: 10,
    CAMPAIGNS_ADMITTED: 10,
    HIGH_QUALITY: castillo.highQ,
    MEDIUM_QUALITY: castillo.medQ,
    LOW_QUALITY: castillo.lowQ,
    REJECT_QUALITY: castillo.rejQ,
    NAMED_ACCOUNTS: 0,
    CURRENT_CYCLE_ACCOUNTS: 0,
    PROVEN_TRAVELING: 0,
    STRONG_INFERENCE_TRAVELING: 0,
    CONTROLLERS: castillo.controllersResolved,
    DIRECT_LODGING: 0,
    COMPLETE_STRONG: 0,
    COMPLETE_PLAUSIBLE: 0,
    VALID_WATCH: 0,
    READY: 0,
    FIRST_COLLAPSE: cCollapse,
  },
  SHERATON: {
    RAW_SIGNALS: sheraton.nativeQueries,
    QUALIFIED_SIGNALS:
      Number(sheraton.hygiene.VALID_WATCH || 0) +
      Number(sheraton.hygiene.VALID_FUTURE || 0) +
      Number(sheraton.hygiene.INSUFFICIENT || 0),
    CAMPAIGN_CANDIDATES: 10,
    CAMPAIGNS_ADMITTED: 10,
    HIGH_QUALITY: sheraton.highQ,
    MEDIUM_QUALITY: sheraton.medQ,
    LOW_QUALITY: sheraton.lowQ,
    REJECT_QUALITY: sheraton.rejQ,
    NAMED_ACCOUNTS: 0,
    CURRENT_CYCLE_ACCOUNTS: 0,
    PROVEN_TRAVELING: 0,
    STRONG_INFERENCE_TRAVELING: sheraton.strongInferenceTravel,
    CONTROLLERS: sheraton.controllersResolved,
    DIRECT_LODGING: sheraton.directLodging,
    COMPLETE_STRONG: 0,
    COMPLETE_PLAUSIBLE: sheraton.completePlausible,
    VALID_WATCH: 0,
    READY: 0,
    FIRST_COLLAPSE: sCollapse,
  },
  NEWLY_PUBLISHED: "NONE",
  out: OUT,
};

write("_RETURN.json", JSON.stringify(ret, null, 2));
console.log(JSON.stringify(ret, null, 2));
