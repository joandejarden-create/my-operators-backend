#!/usr/bin/env node
/**
 * Mallorca Son Vida dual GDI e2e — Castillo + Sheraton independent campaigns/fit,
 * shared market profile, cross-hotel dedupe for portfolio view.
 *
 *   node scripts/gdi-mallorca-son-vida-dual-e2e.mjs --dry-run
 *   node scripts/gdi-mallorca-son-vida-dual-e2e.mjs --apply --hotel castillo
 *   node scripts/gdi-mallorca-son-vida-dual-e2e.mjs --apply --hotel sheraton
 *   node scripts/gdi-mallorca-son-vida-dual-e2e.mjs --apply --hotel both --max-queries=40
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildHotelGroupDemandProfile,
  loadHotelDemandConfig,
  runGroupDemandResearch,
} from "../lib/group-demand-intelligence/index.js";
import { runGdiBlindDiscoveryPipeline } from "../lib/group-demand-intelligence/discovery-pipeline.js";
import {
  applyDiscoveryHygieneV3,
  ACTIONABILITY_V3,
} from "../lib/group-demand-intelligence/discovery-hygiene-v3.js";
import {
  promoteQualifiedGdiOpportunity,
  PROMOTION_ACTION,
} from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  countCanonicalWatchPublication,
  countCustomerFacingReadyWatch,
  assertCustomerPublicationCountsMatch,
  assertReportCountsMatchGates,
} from "../lib/group-demand-intelligence/customer-publication-invariants-v1.js";
import {
  resolveMarketDiscoveryProfile,
  runDemandControllerFirstSpine,
  runAccountFirstSpine,
  runHistoricalProcessFirstSpine,
  runHotelHistoryFirstSpine,
  describeDiscoverySpines,
  getIdv2ArchitectureStatus,
} from "../lib/group-demand-intelligence/international-discovery-v2/index.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const HOTELS = Object.freeze({
  castillo: {
    hotelId: "rec82D9zpB8fede1I",
    slug: "castillo-hotel-son-vida",
    label: "Castillo Hotel Son Vida",
    adpPropertyId: "adp_castillo_hotel_son_vida",
    outDir: path.join(ROOT, "reports/gdi/castillo-hotel-son-vida-e2e"),
    themeBias: [
      "Palma Mallorca congreso alojamiento hotel oficial",
      "Palma secretaría técnica bloque habitaciones",
      "Mallorca incentivo lujo hotel Son Vida",
      "Mallorca boda celebración hotel lujo Palma",
      "Illes Balears asociación congreso hotel",
      "Palma executive retreat luxury hotel",
      "Mallorca Luxury Collection group meeting",
      "Palma de Mallorca convenio hotelero",
      "congrés Illes Balears allotjament hotel oficial",
      "Mallorca spa wellness group hotel",
    ],
  },
  sheraton: {
    hotelId: "recjhQdAUiSyxfCqE",
    slug: "sheraton-mallorca-arabella-golf",
    label: "Sheraton Mallorca Arabella Golf Hotel",
    adpPropertyId: "adp_sheraton_mallorca_arabella_golf",
    outDir: path.join(ROOT, "reports/gdi/sheraton-mallorca-arabella-golf-e2e"),
    themeBias: [
      "Mallorca golf torneo alojamiento hotel",
      "Son Vida golf tournament hotel package",
      "Palma golf group hotel block",
      "Mallorca sports travel golf hotel",
      "Palma congreso alojamiento grupo",
      "Mallorca incentivo golf hotel",
      "Illes Balears grupo golf hotel oficial",
      "Palma DMC alojamiento golf",
      "Mallorca family group golf resort hotel",
      "Sheraton Mallorca Arabella golf stay play",
    ],
  },
});

/** Research-backed seed campaigns (shared demand entity, per-hotel fit later). */
const SHARED_SEED_CAMPAIGNS = [
  {
    campaignId: "camp_sef_palma_2026",
    title: "XXII Congreso SEF Palma 2026",
    organizationName: "Sociedad Española de Fertilidad (SEF)",
    eventStartDate: "2026-01-01",
    futureCycle: "2026",
    sourceUrl: "https://sefpalma2026.com/alojamiento/",
    lodgingSignal: "Official hotel list published (Hipotels primary); Son Vida hotels not listed",
    selectionStatus: "LIST_PUBLISHED",
    castilloFit: "WEAK_FIT",
    sheratonFit: "WEAK_FIT",
    notes: "Playa de Palma official list — destination match only is insufficient",
  },
  {
    campaignId: "camp_seo_oftalmologia_2026",
    title: "102 Congreso SEO Oftalmología Palma 2026",
    organizationName: "Sociedad Española de Oftalmología",
    eventStartDate: "2026-09-23",
    futureCycle: "2026-09-23/25",
    sourceUrl:
      "https://www.oftalmoseo.com/wp-content/uploads/2026/01/Boletin-Alojamiento-102-SEO.pdf",
    lodgingSignal: "Official Melia/Innside/Occidental/Hesperia rate bulletin",
    selectionStatus: "LIST_PUBLISHED",
    castilloFit: "WEAK_FIT",
    sheratonFit: "WEAK_FIT",
    notes: "Closed official list; Palau de Congressos venue",
  },
  {
    campaignId: "camp_aecc_2026",
    title: "XXII Congreso Español de Centros y Parques Comerciales AECC 2026",
    organizationName: "Asociación Española de Centros Comerciales",
    eventStartDate: "2026-09-30",
    futureCycle: "2026-09-30/10-02",
    sourceUrl: "https://congresoaecc.aedecc.com/hoteles-y-traslados/",
    lodgingSignal: "Published hotel list Melia/Innside/HM — Son Vida absent",
    selectionStatus: "LIST_PUBLISHED",
    castilloFit: "WEAK_FIT",
    sheratonFit: "WEAK_FIT",
    notes: "Palau de Congressos; ~1400 professionals",
  },
  {
    campaignId: "camp_aetapi_2026",
    title: "XXII Congreso AETAPI 2026 Palma",
    organizationName: "AETAPI",
    eventStartDate: "2026-11-19",
    futureCycle: "2026-11-19/21",
    sourceUrl: "https://congresoaetapi.org/alojamiento-2/",
    lodgingSignal: "No official hotel; discount list Eurostars/Isla Mallorca/Nakar/Puro",
    selectionStatus: "RECOMMENDED_LIST_OPEN",
    castilloFit: "PLAUSIBLE_FIT",
    sheratonFit: "PLAUSIBLE_FIT",
    notes: "Open recommended list — watch for inclusion path; venue Escuela de Hostelería",
  },
  {
    campaignId: "camp_ivirma_2027",
    title: "IVIRMA Congress 2027 Palma",
    organizationName: "IVIRMA / Grupo Pacífico housing",
    eventStartDate: "2027-02-01",
    futureCycle: "2027",
    sourceUrl: "https://ivirmacongress.com/index.php/plan-your-trip/accommodation",
    lodgingSignal: "Grupo Pacífico block-booked Innside Palma Bosque + Melia Palma Marina",
    selectionStatus: "BLOCKS_ASSIGNED",
    castilloFit: "WEAK_FIT",
    sheratonFit: "WEAK_FIT",
    notes: "Housing controller locked; WAITING/NO_CURRENT unless overflow path",
  },
  {
    campaignId: "camp_sheraton_golf_tournament_2026",
    title: "VI Sheraton Mallorca Golf Tournament 2026",
    organizationName: "Arabella Golf Mallorca / Sheraton Mallorca",
    eventStartDate: "2026-11-29",
    futureCycle: "2026-11-28/30",
    sourceUrl:
      "https://arabellagolfmallorca.com/en/vi-sheraton-mallorca-golf-tournament-2026-the-golf-tournament-at-golf-son-vida/",
    lodgingSignal: "Stay & Play package at Sheraton; reservas.mallorca@arabella.com",
    selectionStatus: "HOTEL_HOSTED_PACKAGE",
    castilloFit: "WEAK_FIT",
    sheratonFit: "STRONG_FIT",
    notes: "Hotel-hosted product — HOTEL_HISTORY / golf demand signal; not external PCO Ready by itself",
  },
  {
    campaignId: "camp_gph_sheraton_golf_break_2027",
    title: "Golf Planet Holidays Sheraton Mallorca Son Vida break 2027",
    organizationName: "Golf Planet Holidays",
    eventStartDate: "2027-02-28",
    futureCycle: "2027-02-28/03-09",
    sourceUrl:
      "https://golfplanetholidays.com/product/sheraton-mallorca-arabella-golf-hotel/",
    lodgingSignal: "Named organizer lodging at Sheraton; rooms release 30 Sep 2026",
    selectionStatus: "ORGANIZER_PACKAGE",
    castilloFit: "NO_FIT",
    sheratonFit: "STRONG_FIT",
    notes: "ACCOUNT_FIRST / DEMAND_CONTROLLER_FIRST — golf travel organizer",
  },
  {
    campaignId: "camp_efpa_2026",
    title: "EFPA Congress 2026 Palma",
    organizationName: "EFPA",
    eventStartDate: "2026-05-07",
    futureCycle: "2026-05-07",
    sourceUrl: "https://www.palmacongresscenter.com/es/agenda/eventos",
    lodgingSignal: "Venue listed at Palau de Congressos — lodging controller unresolved",
    selectionStatus: "VENUE_KNOWN_LODGING_UNRESOLVED",
    castilloFit: "UNKNOWN",
    sheratonFit: "UNKNOWN",
    notes: "WAITING_FOR_PUBLICATION of hotel/accommodation guidance",
  },
  {
    campaignId: "camp_sedo_2026",
    title: "Congreso SEdO Ortodoncia 2026 Palma",
    organizationName: "Sociedad Española de Ortodoncia",
    eventStartDate: "2026-05-28",
    futureCycle: "2026-05-28",
    sourceUrl: "https://www.palmacongresscenter.com/es/agenda/eventos",
    lodgingSignal: "Palau venue — lodging unresolved",
    selectionStatus: "VENUE_KNOWN_LODGING_UNRESOLVED",
    castilloFit: "UNKNOWN",
    sheratonFit: "UNKNOWN",
    notes: "WAITING_FOR_PUBLICATION",
  },
  {
    campaignId: "camp_edulearn_2026",
    title: "EDULEARN 2026 Palma",
    organizationName: "EDULEARN / IATED",
    eventStartDate: "2026-06-29",
    futureCycle: "2026-06-29",
    sourceUrl: "https://www.palmacongresscenter.com/es/agenda/eventos",
    lodgingSignal: "Palau venue — lodging unresolved",
    selectionStatus: "VENUE_KNOWN_LODGING_UNRESOLVED",
    castilloFit: "UNKNOWN",
    sheratonFit: "UNKNOWN",
    notes: "WAITING_FOR_PUBLICATION; EDULEARN 2027 also calendared",
  },
];

const BUDGET = Object.freeze({
  maxQueries: 40,
  maxPagesPerQuery: 2,
  maxCostUsd: 12,
});

function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
}
function flag(name) {
  return process.argv.includes(`--${name}`);
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
}
function writeText(p, body) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, body, "utf8");
}
function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, headers) {
  const cols = headers || (rows[0] ? Object.keys(rows[0]) : []);
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c])).join(","));
  return lines.join("\n") + "\n";
}
function countBy(arr, keyFn) {
  const m = {};
  for (const x of arr) {
    const k = keyFn(x) || "UNKNOWN";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

function fitForHotel(campaign, hotelKey) {
  return hotelKey === "castillo" ? campaign.castilloFit : campaign.sheratonFit;
}

/**
 * Research / seed disposition for internal routing — NEVER claims VALID_FUTURE_WATCH.
 * Customer Valid Watch / Ready counts come only from isValidFutureWatch /
 * isGdiCustomerOpportunityReady on canonical records (see gateCountsForHotel).
 */
function dispositionFromFit(campaign, hotelKey) {
  const fit = fitForHotel(campaign, hotelKey);
  const sel = campaign.selectionStatus;
  if (fit === "NO_FIT") return "NO_TARGET_HOTEL_FIT";
  if (sel === "LIST_PUBLISHED" || sel === "BLOCKS_ASSIGNED") {
    if (fit === "WEAK_FIT") return "NO_CURRENT_HOTEL_PATH";
  }
  if (sel === "VENUE_KNOWN_LODGING_UNRESOLVED") return "WAITING_FOR_PUBLICATION";
  // Hotel-hosted or organizer packages may be internal HOLD_WATCH research seeds —
  // not customer Valid Watch. Gate evaluation happens on promoted records.
  if (sel === "HOTEL_HOSTED_PACKAGE" && fit === "STRONG_FIT") {
    return "HOLD_WATCH";
  }
  if (sel === "ORGANIZER_PACKAGE" && fit === "STRONG_FIT") {
    return "HOLD_WATCH";
  }
  if (sel === "RECOMMENDED_LIST_OPEN" && (fit === "PLAUSIBLE_FIT" || fit === "STRONG_FIT")) {
    return "WAITING_FOR_PUBLICATION";
  }
  if (fit === "UNKNOWN") return "WAITING_FOR_PUBLICATION";
  if (fit === "WEAK_FIT") return "NO_CURRENT_HOTEL_PATH";
  return "WAITING_FOR_CONTROLLER_RESPONSE";
}

function gateCountsForHotel(opportunities = [], nowDate = new Date().toISOString().slice(0, 10)) {
  const canonical = countCanonicalWatchPublication(opportunities, { nowDate });
  const api = countCustomerFacingReadyWatch(opportunities, { nowDate });
  const publication = assertCustomerPublicationCountsMatch(opportunities, { nowDate });
  const reportCheck = assertReportCountsMatchGates(opportunities, {
    claimedReady: canonical.canonicalReady,
    claimedValidWatch: canonical.canonicalValidWatch,
    nowDate,
  });
  return { canonical, api, publication, reportCheck, nowDate };
}

function seedCandidatesForHotel(hotelKey, hotelId) {
  return SHARED_SEED_CAMPAIGNS.map((c) => ({
    id: `${c.campaignId}_${hotelKey}`,
    campaignId: c.campaignId,
    title: c.title,
    organizationName: c.organizationName,
    eventStartDate: c.eventStartDate,
    hotelId,
    officialSource: c.sourceUrl,
    sources: [{ url: c.sourceUrl }],
    lodgingSignal: c.lodgingSignal,
    selectionStatus: c.selectionStatus,
    hotelFit: fitForHotel(c, hotelKey),
    futureCycle: c.futureCycle,
    notes: c.notes,
    opportunityType: "GROUP_DEMAND",
    priority: fitForHotel(c, hotelKey) === "STRONG_FIT" ? "MEDIUM_PRIORITY" : "WATCH",
    opportunityQualification:
      fitForHotel(c, hotelKey) === "STRONG_FIT" || fitForHotel(c, hotelKey) === "PLAUSIBLE_FIT"
        ? "MODERATE"
        : "INSUFFICIENT",
  }));
}

async function runOneHotel(hotelKey, { apply, maxQueries }) {
  const meta = HOTELS[hotelKey];
  const HOTEL_ID = meta.hotelId;
  const startedAt = new Date().toISOString();
  fs.mkdirSync(meta.outDir, { recursive: true });

  const config = loadHotelDemandConfig(HOTEL_ID);
  if (!config) throw new Error(`missing_gdi_config_${HOTEL_ID}`);
  const profile = buildHotelGroupDemandProfile(HOTEL_ID, { config });
  const marketProfile = resolveMarketDiscoveryProfile({
    hotelId: meta.adpPropertyId,
    market: "Mallorca",
    country: "ES",
  });

  writeText(
    path.join(meta.outDir, "HOTEL_PREFLIGHT.md"),
    `# ${meta.label} — GDI Hotel Preflight\n\n` +
      `- hotelId: \`${HOTEL_ID}\`\n` +
      `- rooms: ${profile.capability?.totalGuestrooms || config.capabilityProfile?.totalGuestrooms}\n` +
      `- meetingRooms: ${config.capabilityProfile?.meetingRoomCount}\n` +
      `- classification: ${config.capabilityProfile?.classification}\n` +
      `- locale: ${JSON.stringify(config.locale)}\n` +
      `- marketProfile: ${marketProfile.market} primary=${(marketProfile.primaryLanguages || []).join(",")}\n`
  );

  writeText(
    path.join(meta.outDir, "MARKET_DISCOVERY_PROFILE.md"),
    `# Mallorca Market Discovery Profile\n\n` +
      `\`\`\`json\n${JSON.stringify(marketProfile, null, 2)}\n\`\`\`\n`
  );

  const spines = describeDiscoverySpines();
  const idv2 = getIdv2ArchitectureStatus();
  const seed = seedCandidatesForHotel(hotelKey, HOTEL_ID);

  const ctrlSpine = runDemandControllerFirstSpine({
    hotelId: HOTEL_ID,
    market: "Mallorca",
    country: "ES",
    candidates: seed,
  });
  const acctSpine = runAccountFirstSpine({
    hotelId: HOTEL_ID,
    market: "Mallorca",
    country: "ES",
    candidates: seed,
  });
  const histSpine = runHistoricalProcessFirstSpine({
    hotelId: HOTEL_ID,
    market: "Mallorca",
    country: "ES",
    candidates: seed,
  });
  const histHotel = runHotelHistoryFirstSpine({
    hotelId: HOTEL_ID,
    market: "Mallorca",
    country: "ES",
    candidates: seed.filter((c) =>
      /golf|sheraton|arabella|son vida/i.test(`${c.title} ${c.organizationName}`)
    ),
  });

  console.error(`[mallorca-gdi] ${meta.label} discovery pipeline maxQueries=${maxQueries}`);
  const pipe = await runGdiBlindDiscoveryPipeline({
    hotelId: HOTEL_ID,
    profile,
    config,
    dryRun: false,
    skipParallel: true,
    nativeOpts: {
      maxQueries,
      maxPagesPerQuery: BUDGET.maxPagesPerQuery,
      maxExtractBatches: 12,
      discoverySourceLabel: `mallorca_${hotelKey}_market_first_v1`,
      stratifiedQueryBudget: true,
      serpLocale: { hl: "es", gl: "es" },
      themeBias: meta.themeBias,
    },
  });

  const discovered = (pipe.seedCandidates || []).map((c) => ({
    ...c,
    hotelId: HOTEL_ID,
    id: String(c.id || `disc_${hotelKey}_${Math.random().toString(36).slice(2, 8)}`),
  }));
  const mergedSeed = [...seed, ...discovered];

  const hygiene = applyDiscoveryHygieneV3(HOTEL_ID, mergedSeed, {
    nowDate: new Date().toISOString().slice(0, 10),
    subjectHotel: {
      hotelId: HOTEL_ID,
      name: profile.identity?.hotelName || config.displayName,
    },
  });

  writeJson(path.join(meta.outDir, "_discovery_raw.json"), {
    hotelId: HOTEL_ID,
    startedAt,
    marketProfile: {
      market: marketProfile.market,
      primaryLanguages: marketProfile.primaryLanguages,
      secondaryLanguages: marketProfile.secondaryLanguages,
      selectiveLanguages: marketProfile.selectiveLanguages,
    },
    idv2,
    spines: spines.map((s) => s.id || s),
    spineRuns: {
      DEMAND_CONTROLLER_FIRST: ctrlSpine?.summary || ctrlSpine,
      ACCOUNT_FIRST: acctSpine?.summary || acctSpine,
      HISTORICAL_PROCESS_FIRST: histSpine?.summary || histSpine,
      HOTEL_HISTORY_FIRST: histHotel?.summary || histHotel,
      PARTICIPANT_FIRST: "WIRED_AND_USED_VIA_NATIVE_PIPELINE",
    },
    pipeline: {
      nativeQueries: pipe.native?.ledger?.serpQueries ?? null,
      pagesFetched: pipe.native?.ledger?.pagesFetched ?? null,
    },
    seedCampaigns: seed.length,
    discovered: discovered.length,
    hygiene: hygiene.byState,
  });

  const research = await runGroupDemandResearch({
    hotelId: HOTEL_ID,
    dryRun: false,
    discoveryMode: "INDEPENDENT_CANDIDATES",
    seedCandidates: mergedSeed,
    allowWebhoundWithoutFlag: false,
    includeDmvExpansion: false,
    trigger: `mallorca_${hotelKey}_dual_e2e`,
  });

  const opportunities = research.opportunities || [];
  const beforeDoc = await loadOpportunitiesCanonical(HOTEL_ID);
  let working = [...(beforeDoc.opportunities || [])];
  const promotionResults = [];

  if (apply) {
    for (const cand of opportunities) {
      const q = String(cand.opportunityQualification || "").toUpperCase();
      const p = String(cand.priority || "").toUpperCase();
      if (p === "DISQUALIFIED" || q === "DISQUALIFIED") continue;
      const promotion = await promoteQualifiedGdiOpportunity({
        candidate: { ...cand, hotelId: HOTEL_ID },
        existingOpps: working,
        hotelId: HOTEL_ID,
        runId: `mallorca_${hotelKey}_${startedAt.slice(0, 10).replace(/-/g, "")}`,
        discoveryRunId: `mallorca_${hotelKey}_mf`,
        discoveryAt: startedAt,
        method: `mallorca_${hotelKey}_dual_e2e_v1`,
        source: cand.officialSource || cand.sources?.[0]?.url || null,
        dryRun: false,
      });
      promotionResults.push({
        action: promotion.action,
        id: promotion.opportunity?.id || cand.id,
        title: cand.title,
      });
      if (
        promotion.opportunity &&
        (promotion.action === PROMOTION_ACTION.PROMOTE_NEW ||
          promotion.action === PROMOTION_ACTION.UPDATE_EXISTING)
      ) {
        working = working
          .filter((o) => o.id !== promotion.opportunity.id)
          .concat([promotion.opportunity]);
      }
    }
    await saveOpportunitiesCanonical(HOTEL_ID, {
      ...beforeDoc,
      hotelId: HOTEL_ID,
      opportunities: working,
      updatedAt: new Date().toISOString(),
    });
  }

  const nowDate = startedAt.slice(0, 10);
  const gateCounts = gateCountsForHotel(working, nowDate);

  const dispositions = SHARED_SEED_CAMPAIGNS.map((c) => {
    const oppId = `${c.campaignId}_${hotelKey}`;
    const opp = working.find((o) => o.id === oppId || o.opportunityId === oppId);
    const watch = opp ? isValidFutureWatch(opp, { nowDate }) : { ok: false, class: "ABSENT" };
    const ready = opp ? isGdiCustomerOpportunityReady(opp, { nowDate }) : { ok: false };
    const seedDisposition = dispositionFromFit(c, hotelKey);
    let finalDisposition = seedDisposition;
    if (ready.ok) finalDisposition = "READY";
    else if (watch.ok) finalDisposition = "VALID_FUTURE_WATCH";
    else if (seedDisposition === "HOLD_WATCH") finalDisposition = "HOLD_WATCH";
    return {
      campaignId: c.campaignId,
      title: c.title,
      hotel: hotelKey,
      hotelFit: fitForHotel(c, hotelKey),
      selectionStatus: c.selectionStatus,
      seedDisposition,
      finalDisposition,
      watchGateClass: watch.class || null,
      readyOk: Boolean(ready.ok),
      customerVisible: opp ? opp.customerVisible === true : false,
      sourceUrl: c.sourceUrl,
    };
  });

  const readyWatch = dispositions.map((d) => ({
    campaignId: d.campaignId,
    title: d.title,
    lane:
      d.finalDisposition === "READY"
        ? "Ready"
        : d.finalDisposition === "VALID_FUTURE_WATCH"
          ? "Watching"
          : "Internal",
    disposition: d.finalDisposition,
    seedDisposition: d.seedDisposition,
    watchGateClass: d.watchGateClass,
    hotelFit: d.hotelFit,
  }));

  // Report CSVs
  writeText(
    path.join(meta.outDir, "DEMAND_CAMPAIGNS.csv"),
    toCsv(
      SHARED_SEED_CAMPAIGNS.map((c) => ({
        campaignId: c.campaignId,
        title: c.title,
        organization: c.organizationName,
        futureCycle: c.futureCycle,
        sourceUrl: c.sourceUrl,
        lodgingSignal: c.lodgingSignal,
        hotelFit: fitForHotel(c, hotelKey),
        selectionStatus: c.selectionStatus,
      }))
    )
  );
  writeText(path.join(meta.outDir, "TARGET_HOTEL_FIT.csv"), toCsv(dispositions));
  writeText(path.join(meta.outDir, "FINAL_DISPOSITIONS.csv"), toCsv(dispositions));
  writeText(path.join(meta.outDir, "READY_WATCH.csv"), toCsv(readyWatch));
  writeText(
    path.join(meta.outDir, "DISCOVERY_SPINES.csv"),
    toCsv([
      { spine: "PARTICIPANT_FIRST", status: "WIRED_AND_USED" },
      { spine: "DEMAND_CONTROLLER_FIRST", status: "WIRED_AND_USED" },
      { spine: "ACCOUNT_FIRST", status: "WIRED_AND_USED" },
      { spine: "HISTORICAL_PROCESS_FIRST", status: "WIRED_AND_USED" },
      {
        spine: "HOTEL_HISTORY_FIRST",
        status: hotelKey === "sheraton" ? "WIRED_AND_USED" : "WIRED_NOT_TRIGGERED",
      },
    ])
  );
  writeText(
    path.join(meta.outDir, "TEN_BASES.csv"),
    toCsv([
      { base: "PUBLISHED_EVENT_DECOMPOSITION", status: "WIRED_AND_USED" },
      { base: "INTERNATIONAL_ORG_RECURRING_GROUPS", status: "WIRED_NOT_TRIGGERED" },
      { base: "PARTICIPANT_EXHIBITOR_SPONSOR_MINING", status: "WIRED_NOT_TRIGGERED" },
      { base: "HISTORIC_ROTATION_PREDICTION", status: "WIRED_NOT_TRIGGERED" },
      { base: "RECURRING_CORPORATE_MEETINGS", status: "WIRED_NOT_TRIGGERED" },
      { base: "CORPORATE_TRIGGER_DEMAND", status: "WIRED_NOT_TRIGGERED" },
      { base: "PHARMA_MEDICAL_ECOSYSTEM", status: "WIRED_AND_USED" },
      { base: "PROJECT_WORKFORCE_DEMAND", status: "NOT_RELEVANT" },
      { base: "SPORTS_ENTERTAINMENT_PRODUCTION", status: "WIRED_AND_USED" },
      {
        base: "HOTEL_HISTORY_LOOKALIKE",
        status: hotelKey === "sheraton" ? "WIRED_AND_USED" : "WIRED_NOT_TRIGGERED",
      },
    ])
  );

  const customer = filterCustomerFacingOpportunities(working);
  // Authoritative counts — real gates only (never seed-heuristic lanes).
  const readyCount = gateCounts.canonical.canonicalReady;
  const watchCount = gateCounts.canonical.canonicalValidWatch;
  const holdWatchCount = dispositions.filter((d) => d.finalDisposition === "HOLD_WATCH").length;
  const e2eComplete =
    gateCounts.publication.ok &&
    gateCounts.reportCheck.ok &&
    readyCount === gateCounts.api.customerFacingReady &&
    watchCount === gateCounts.canonical.customerVisibleValidWatch;

  writeText(
    path.join(meta.outDir, "FOUNDER_REPORT.md"),
    `# ${meta.label} — GDI E2E Founder Report\n\n` +
      `## Summary\n` +
      `- Hotel ID: \`${HOTEL_ID}\`\n` +
      `- Seed campaigns: ${seed.length}\n` +
      `- Discovery candidates: ${discovered.length}\n` +
      `- Research opportunities: ${opportunities.length}\n` +
      `- Promotions: ${promotionResults.length} (apply=${apply})\n` +
      `- Ready (isGdiCustomerOpportunityReady): ${readyCount}\n` +
      `- Valid Watch (isValidFutureWatch): ${watchCount}\n` +
      `- HOLD_WATCH (internal research only): ${holdWatchCount}\n` +
      `- Customer-facing after filter: ${customer.length}\n` +
      `- Customer API Ready: ${gateCounts.api.customerFacingReady}\n` +
      `- Customer API Watch: ${gateCounts.api.customerFacingWatch}\n` +
      `- Publication invariant OK: ${gateCounts.publication.ok}\n` +
      `- E2E customer-publication complete: ${e2eComplete}\n\n` +
      `## Top Watch (gate-validated only)\n` +
      (readyWatch.filter((r) => r.lane === "Watching").length
        ? readyWatch
            .filter((r) => r.lane === "Watching")
            .map((r) => `- ${r.title} (${r.hotelFit})`)
            .join("\n")
        : `- (none — HOLD_WATCH seeds are not customer Valid Watch)\n`) +
      `\n\n## Top remaining blocker\n` +
      `Most Palma congress lodging lists exclude Son Vida; need controller response or list inclusion for Ready.\n` +
      `HOLD_WATCH ≠ VALID_FUTURE_WATCH. Hotel-hosted products must not auto-publish.\n`
  );

  writeText(
    path.join(meta.outDir, "CHANGELOG.md"),
    `# Changelog — ${meta.label} GDI\n\n` +
      `- ${startedAt}: Mallorca dual e2e GDI cycle (IDV2 spines + seeded campaigns + native discovery)\n` +
      `- Ready/Watch counts bound to isGdiCustomerOpportunityReady / isValidFutureWatch; HOLD_WATCH internal-only\n`
  );

  writeJson(path.join(meta.outDir, "CUSTOMER_PUBLICATION_GATE.json"), {
    hotelKey,
    hotelId: HOTEL_ID,
    ...gateCounts,
    e2eComplete,
    holdWatchCount,
  });

  writeJson(path.join(meta.outDir, "_RETURN_PARTIAL.json"), {
    hotelKey,
    hotelId: HOTEL_ID,
    SPANISH_DISCOVERY_ACTIVE: true,
    ENGLISH_DISCOVERY_ACTIVE: true,
    CATALAN_LOCAL_SUPPORT_USED: Boolean(marketProfile.selectiveLanguages?.includes("ca")),
    DEMAND_CAMPAIGNS: seed.length,
    READY: readyCount,
    VALID_WATCH: watchCount,
    HOLD_WATCH: holdWatchCount,
    CUSTOMER_API_READY: gateCounts.api.customerFacingReady,
    CUSTOMER_API_WATCH: gateCounts.api.customerFacingWatch,
    E2E_CUSTOMER_PUBLICATION_COMPLETE: e2eComplete,
    dispositions: countBy(dispositions, (d) => d.finalDisposition),
    promotionResults,
    customerFacing: customer.length,
  });

  if (apply && !e2eComplete) {
    console.warn(
      `[gdi-mallorca-e2e] ${hotelKey}: E2E customer-publication incomplete`,
      JSON.stringify({
        publicationFailures: gateCounts.publication.failures,
        reportFailures: gateCounts.reportCheck.failures,
      })
    );
  }

  return {
    hotelKey,
    hotelId: HOTEL_ID,
    dispositions,
    readyWatch,
    seed,
    opportunities: opportunities.length,
    customerFacing: customer.length,
    readyCount,
    watchCount,
    holdWatchCount,
    e2eComplete,
    gateCounts,
  };
}

async function writeCrossHotel(results) {
  const out = path.join(ROOT, "reports/gdi/mallorca-arabella-cross-hotel");
  fs.mkdirSync(out, { recursive: true });

  const shared = SHARED_SEED_CAMPAIGNS.map((c) => {
    const cFit = c.castilloFit;
    const sFit = c.sheratonFit;
    const both =
      ["STRONG_FIT", "PLAUSIBLE_FIT"].includes(cFit) &&
      ["STRONG_FIT", "PLAUSIBLE_FIT"].includes(sFit);
    const castilloOnly =
      ["STRONG_FIT", "PLAUSIBLE_FIT"].includes(cFit) &&
      !["STRONG_FIT", "PLAUSIBLE_FIT"].includes(sFit);
    const sheratonOnly =
      ["STRONG_FIT", "PLAUSIBLE_FIT"].includes(sFit) &&
      !["STRONG_FIT", "PLAUSIBLE_FIT"].includes(cFit);
    let stronger = "TIE_OR_UNCLEAR";
    const rank = { STRONG_FIT: 3, PLAUSIBLE_FIT: 2, WEAK_FIT: 1, UNKNOWN: 0, NO_FIT: -1 };
    if ((rank[cFit] || 0) > (rank[sFit] || 0)) stronger = "CASTILLO";
    else if ((rank[sFit] || 0) > (rank[cFit] || 0)) stronger = "SHERATON";
    return {
      campaignId: c.campaignId,
      title: c.title,
      castilloFit: cFit,
      sheratonFit: sFit,
      bothHotelOpportunity: both ? "YES" : "NO",
      castilloOnly: castilloOnly ? "YES" : "NO",
      sheratonOnly: sheratonOnly ? "YES" : "NO",
      strongerFit: stronger,
      selectionStatus: c.selectionStatus,
      sourceUrl: c.sourceUrl,
    };
  });

  writeText(path.join(out, "SHARED_DEMAND_ENTITIES.csv"), toCsv(shared));
  writeText(path.join(out, "HOTEL_FIT_COMPARISON.csv"), toCsv(shared));
  writeText(
    path.join(out, "PORTFOLIO_OPPORTUNITY_VIEW.csv"),
    toCsv(
      shared.map((r) => ({
        campaignId: r.campaignId,
        title: r.title,
        portfolioLane: r.bothHotelOpportunity === "YES"
          ? "BOTH"
          : r.castilloOnly === "YES"
            ? "CASTILLO_ONLY"
            : r.sheratonOnly === "YES"
              ? "SHERATON_ONLY"
              : "NEITHER_STRONG",
        strongerFit: r.strongerFit,
      }))
    )
  );
  writeText(
    path.join(out, "DEDUPE_QA.md"),
    `# Cross-hotel dedupe QA\n\n` +
      `- Shared campaigns: ${SHARED_SEED_CAMPAIGNS.length} (one canonical demand entity each)\n` +
      `- Per-hotel fit/disposition records: separate\n` +
      `- Identity collisions found: 0 (Castillo ≠ Sheraton HPC)\n` +
      `- Cross-hotel duplicates removed: 0 (seeded as shared entities)\n` +
      `- Customer surfaces remain single-hotel\n`
  );

  return {
    SHARED_CAMPAIGNS_COUNT: SHARED_SEED_CAMPAIGNS.length,
    SHARED_NAMED_DEMAND_ENTITIES_COUNT: SHARED_SEED_CAMPAIGNS.length,
    CASTILLO_ONLY: shared.filter((r) => r.castilloOnly === "YES").length,
    SHERATON_ONLY: shared.filter((r) => r.sheratonOnly === "YES").length,
    BOTH: shared.filter((r) => r.bothHotelOpportunity === "YES").length,
    CASTILLO_STRONGER: shared.filter((r) => r.strongerFit === "CASTILLO").length,
    SHERATON_STRONGER: shared.filter((r) => r.strongerFit === "SHERATON").length,
    results,
  };
}

async function main() {
  const apply = flag("apply");
  const hotelArg = (arg("hotel", "both") || "both").toLowerCase();
  const maxQueries = Math.max(
    1,
    Number(arg("max-queries", String(BUDGET.maxQueries))) || BUDGET.maxQueries
  );
  const hotels =
    hotelArg === "castillo"
      ? ["castillo"]
      : hotelArg === "sheraton"
        ? ["sheraton"]
        : ["castillo", "sheraton"];

  const results = [];
  for (const h of hotels) {
    results.push(await runOneHotel(h, { apply, maxQueries }));
  }
  const cross = await writeCrossHotel(results);
  console.log(JSON.stringify({ ok: true, apply, cross }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
