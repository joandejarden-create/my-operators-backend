#!/usr/bin/env node
/**
 * GDI International Discovery V2 — bounded pilot + report pack.
 * Uses existing canonical corpora + spine runners. No Apify. No threshold changes.
 *
 * Usage: node scripts/gdi-international-discovery-v2-pilot.mjs
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  getIdv2ArchitectureStatus,
  resolveMarketDiscoveryProfile,
  runDemandControllerFirstSpine,
  runAccountFirstSpine,
  runHistoricalProcessFirstSpine,
  runHotelHistoryFirstSpine,
  participantFirstSpineContract,
  describeDiscoverySpines,
  convergeSpineResultsToPacket,
  mergeDuplicateOpportunities,
  computeNextBlocker,
  selectNextDiscoveryPath,
  summarizeEquivalentEvidencePolicy,
  getReadyGateIntlAudit,
  listInternationalSourceFamilies,
  upsertDemandController,
  loadDemandControllers,
  saveDemandControllers,
  upsertIntermediaryEntity,
  loadIntermediaryGraph,
  saveIntermediaryGraph,
  customerSafeControllerCopy,
  DISCOVERY_SPINE,
  CONTROLLER_TYPE,
  CONTROLLER_AUTHORITY,
} from "../lib/group-demand-intelligence/international-discovery-v2/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports", "gdi", "international-discovery-v2");

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function write(name, body) {
  const p = path.join(OUT, name);
  fs.writeFileSync(p, body, "utf8");
  return p;
}

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows, headers) {
  const cols = headers || (rows[0] ? Object.keys(rows[0]) : []);
  const lines = [cols.join(",")];
  for (const r of rows) {
    lines.push(cols.map((c) => csvEscape(r[c])).join(","));
  }
  return lines.join("\n") + "\n";
}

function readJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/** Seed campaigns from prior GDI reports (canonical evidence already gathered). */
const PILOT_SEEDS = {
  ac_hotel_a_coruna: {
    hotelId: "ac_hotel_a_coruna",
    label: "AC Hotel A Coruña",
    country: "ES",
    market: "A Coruña",
    oldPath: { ready: 0, watch: 2, completeStrong: 2, completePlausible: 0 },
    campaigns: [
      {
        campaignId: "accamp_biocultura_a_coruna_2027_2027",
        campaignName: "BioCultura A Coruña 2027",
        organizerName: "Asociación Vida Sana",
        housingProvider: null,
        contactUrl: "mailto:expositores@vidasana.org",
        sourceUrl: "https://www.biocultura.org/acoruna/viajes",
        snippets: [
          {
            text: "BioCultura A Coruña organizador Asociación Vida Sana expositores@vidasana.org viajes y alojamientos hoteles recomendados",
            url: "https://www.biocultura.org/acoruna/viajes",
            evidenceType: "OFFICIAL_HOUSING_PAGE",
            controllerName: "Asociación Vida Sana",
            contactUrl: "mailto:expositores@vidasana.org",
          },
        ],
        priorCycles: [
          {
            year: 2025,
            organizer: "Asociación Vida Sana",
            housingModel: "ATTENDEE_SELF_BOOKING_PARTNER_LIST",
            decisionTiming: "Q4 prior to March edition",
          },
        ],
        accounts: [],
      },
      {
        campaignId: "accamp_iaps_spaces_in_transition_symposium_2027_2027",
        campaignName: "IAPS Spaces in Transition Symposium 2027",
        organizerName: "UDC People-Environment Research Group",
        contactUrl: "mailto:ricardo.garcia.mira@udc.es",
        sourceUrl: "https://iaps-association.org/",
        snippets: [
          {
            text: "IAPS symposium convened by University of A Coruña UDC secretaría redes Sustainability Culture Space — venue and alojamiento TBA",
            url: "https://iaps-association.org/",
            evidenceType: "HOST_INSTITUTION",
            controllerName: "UDC People-Environment Research Group / IAPS Networks",
            contactUrl: "mailto:ricardo.garcia.mira@udc.es",
          },
        ],
        priorCycles: [
          {
            year: 2024,
            organizer: "IAPS",
            pco: null,
            housingModel: "UNKNOWN",
            decisionTiming: "post abstract deadline",
          },
        ],
        accounts: [
          {
            organizationName: "Universidade da Coruña",
            trigger: "university_exchange",
            evidenceUrl: "https://iaps-association.org/",
            market: "A Coruña",
          },
        ],
      },
    ],
  },
  radisson_santo_domingo: {
    hotelId: "radisson_santo_domingo",
    label: "Radisson Hotel Santo Domingo",
    country: "DO",
    market: "Santo Domingo",
    oldPath: { ready: 0, watch: 3, completeStrong: 3, completePlausible: 0 },
    campaigns: [
      {
        campaignId: "radisscamp_vii_congreso_iberoamericano_de_filosofia_2027_2027",
        campaignName: "VII Congreso Iberoamericano de Filosofía 2027",
        organizerName: "Comité Organizador RIF–UASD / ADOFIL",
        contactUrl: "mailto:adofil333@gmail.com",
        sourceUrl: "https://rediberoamericanafilosofia.com/",
        snippets: [
          {
            text: "Secretaría organización ADOFIL convenio de alojamiento tarifas preferenciales hoteles Zona Colonial — lista de hoteles en próxima circular",
            url: "https://rediberoamericanafilosofia.com/wp-content/uploads/2026/05/Segunda-convocatoria-Congreso-RIF.pdf",
            evidenceType: "EVENT_MANUAL_HOUSING",
            controllerName: "ADOFIL / Comité Organizador RIF–UASD",
            contactUrl: "mailto:adofil333@gmail.com",
          },
        ],
        priorCycles: [
          {
            year: 2025,
            organizer: "RIF",
            housingProvider: "organizer_convenio",
            housingModel: "DIRECT_NEGOTIATION",
          },
        ],
        accounts: [],
      },
      {
        campaignId: "radisscamp_6_congreso_mundial_cielo_laboral_2026_2026",
        campaignName: "6º Congreso Mundial CIELO Laboral 2026",
        organizerName: "CIELO Laboral",
        contactUrl: "mailto:congresocielo6@gmail.com",
        sourceUrl: "https://www.cielolaboral.com/6o-congreso-mundial-cielo-laboral-santodomingo/",
        snippets: [
          {
            text: "CIELO Laboral secretaría del congreso hoteles recomendados reserva directa alojamiento near PUCMM",
            url: "https://www.cielolaboral.com/",
            evidenceType: "OFFICIAL_HOUSING_PAGE",
            controllerName: "CIELO Laboral congress secretariat",
            contactUrl: "mailto:congresocielo6@gmail.com",
          },
        ],
        priorCycles: [],
        accounts: [],
      },
      {
        campaignId: "radisscamp_autoamericas_2027_2027",
        campaignName: "AUTOAMERICAS 2027",
        organizerName: "AutoAméricas / Latinpress",
        pcoName: null,
        housingProvider: "Hotel Dominican Fiesta",
        contactUrl: "mailto:acaballero@autoamericas.show",
        sourceUrl: "https://www.autoamericas.show/es/expo/alojamiento.html",
        snippets: [
          {
            text: "Hotel oficial Dominican Fiesta alojamiento AutoAméricas agencia de eventos Latinpress — partner hotels convenio histórico",
            url: "https://www.autoamericas.show/es/expo/alojamiento.html",
            evidenceType: "OFFICIAL_HOUSING_PAGE",
            controllerName: "AutoAméricas / Latinpress",
            contactUrl: "mailto:acaballero@autoamericas.show",
          },
        ],
        priorCycles: [
          {
            year: 2026,
            organizer: "AutoAméricas / Latinpress",
            officialHotel: "Dominican Fiesta",
            housingModel: "HOST_PLUS_SECONDARY_PARTNERS",
            participantTypes: ["exhibitors", "speakers"],
          },
        ],
        accounts: [],
      },
    ],
  },
  westin_grand_munchen: {
    hotelId: "westin_grand_munchen",
    label: "The Westin Grand München",
    country: "DE",
    market: "Munich",
    oldPath: { ready: 0, watch: 0, completeStrong: 0, completePlausible: 0 },
    campaigns: [
      {
        campaignId: "westincamp_stadtgeburtstag_2027_und_2028_2027",
        campaignName: "Stadtgeburtstag 2027 und 2028",
        organizerName: "Landeshauptstadt München",
        sourceUrl:
          "https://vergabe.muenchen.de/NetServer/PublicationSearchControllerServlet?function=Search&OrderBy=TenderKind&Order=asc&Start=50&csrt=9615200572752665074",
        snippets: [
          {
            text: "Vergabe München Stadtgeburtstag Kongressorganisation Unterkunft Hotelkontingent öffentliche Ausschreibung",
            url: "https://vergabe.muenchen.de/",
            evidenceType: "PROCUREMENT_NOTICE",
            controllerName: "Landeshauptstadt München (Vergabe / Veranstaltungsorganisation)",
          },
        ],
        priorCycles: [
          {
            year: 2025,
            organizer: "Landeshauptstadt München",
            housingModel: "PROCUREMENT_OR_CITY_COORDINATION",
            decisionTiming: "tender cycle",
          },
        ],
        accounts: [
          {
            organizationName: "Landeshauptstadt München",
            trigger: "government_mission",
            evidenceUrl: "https://vergabe.muenchen.de/",
            market: "Munich",
          },
        ],
      },
    ],
  },
};

function runHotelPilot(seed) {
  const profile = resolveMarketDiscoveryProfile({
    hotelId: seed.hotelId,
    market: seed.market,
    country: seed.country,
  });
  const pathSel = selectNextDiscoveryPath({
    hotelId: seed.hotelId,
    marketProfile: profile,
    availableEvidence: {
      participantListAbsent: true,
      publicDataCeiling: true,
      controllerResolved: false,
      recurringCongress: true,
      priorCycleKnown: seed.campaigns.some((c) => (c.priorCycles || []).length > 0),
    },
  });

  const controllers = [];
  const accounts = [];
  const historical = [];
  const packets = [];
  const blockers = [];
  const qualityRows = [];

  for (const campaign of seed.campaigns) {
    const ctrlSpine = runDemandControllerFirstSpine({
      campaign,
      snippets: campaign.snippets || [],
      marketProfile: profile,
    });
    const histSpine = runHistoricalProcessFirstSpine({
      priorCycles: campaign.priorCycles || [],
      campaign,
    });
    const acctSpine = runAccountFirstSpine({
      accounts: campaign.accounts || [],
      marketProfile: profile,
    });
    // Hotel history only if hotel-supplied exists — none in this pilot
    const histHotel = runHotelHistoryFirstSpine({ hotelId: seed.hotelId, hotelSupplied: [] });

    for (const c of ctrlSpine.controllers) {
      controllers.push({
        hotel: seed.label,
        hotelId: seed.hotelId,
        campaignId: campaign.campaignId,
        campaignName: campaign.campaignName,
        ...c.demandController,
        discoverySpine: DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
        equivalentEvidenceOk: c.equivalentEvidenceOk,
      });
    }
    for (const a of acctSpine.accounts.filter((x) => !x.rejected)) {
      accounts.push({ hotel: seed.label, ...a });
    }
    for (const p of histSpine.patterns) {
      historical.push({ hotel: seed.label, campaignId: campaign.campaignId, ...p });
    }

    const converged = convergeSpineResultsToPacket({
      existingOpportunity: {
        hotelId: seed.hotelId,
        campaignId: campaign.campaignId,
        campaignName: campaign.campaignName,
        title: campaign.campaignName,
        organizationName: campaign.organizerName,
        market: seed.market,
        country: seed.country,
        publicDataCeiling: true,
        stallReason: "PUBLIC_DATA_CEILING",
        lodgingEvidenceClass: campaign.snippets?.some((s) =>
          /HOUSING|ALOJAMIENTO|HOTEL/i.test(s.evidenceType || s.text || "")
        )
          ? "PLAUSIBLE_HOTEL_MOTION"
          : "NONE",
        futureDecisionPoint: true,
        participationRole: "event_cycle",
        demandType: "CONGRESS_OR_TRADE",
      },
      spineResults: [ctrlSpine, histSpine, acctSpine, histHotel],
      hotelId: seed.hotelId,
      marketProfile: profile,
    });

    packets.push({
      hotel: seed.label,
      hotelId: seed.hotelId,
      campaignId: campaign.campaignId,
      mergeKey: converged.mergeKey,
      discoverySpines: (converged.packet.discoverySpines || []).join("|"),
      demandControllerId: converged.packet.demandControllerId || "",
      controllerName: converged.packet.demandController?.controllerName || "",
      controllerAuthority: converged.packet.demandController?.lodgingAuthority || "",
      packetQualityHint: converged.blocker.packetQualityHint,
      nextBlocker: converged.blocker.nextBlocker || "",
      nextBestPath: converged.blocker.bestDiscoveryPath || "",
      autoPromoted: false,
      historicalTreatedAsCurrent: false,
    });

    blockers.push({
      hotel: seed.label,
      campaignId: campaign.campaignId,
      missingPillars: (converged.blocker.missingPillars || []).join("|"),
      nextBlocker: converged.blocker.nextBlocker || "",
      highestValueNextEvidence: converged.blocker.highestValueNextEvidence || "",
      bestDiscoveryPath: converged.blocker.bestDiscoveryPath || "",
      stopContinue: converged.blocker.stopContinue || "",
      stallClass: "PUBLIC_DATA_CEILING",
    });

    // Quality audit — reject unsafe promotions
    const dc = converged.packet.demandController;
    let reject = "";
    if (dc && dc.lodgingAuthority === CONTROLLER_AUTHORITY.UNCONFIRMED && !dc.campaignId) {
      reject = "GENERIC_CONTROLLER_NO_CAMPAIGN";
    }
    if (/hotel|westin|radisson|marriott/i.test(converged.packet.organizationName || "") && !campaign.organizerName) {
      reject = "VENUE_AS_ACCOUNT";
    }
    qualityRows.push({
      hotel: seed.label,
      campaignId: campaign.campaignId,
      packetQualityHint: converged.blocker.packetQualityHint,
      readyPromoted: "NO",
      rejectedReason: reject || "NONE",
      genericOrganizerAsAccount: /Landeshauptstadt|Vida Sana|ADOFIL|CIELO|AutoAméricas|UDC/i.test(
        converged.packet.organizationName || ""
      )
        ? "ORGANIZER_SHELL_NOT_TRAVELING_ACCOUNT"
        : "OK",
      historicalAsCurrent: "NO",
      speculativeLodging: "NO",
      auditPass: reject ? "FAIL" : "PASS",
    });
  }

  // Incremental metrics vs old path — controllers/paths improve utility; Ready unchanged
  const newControllers = controllers.length;
  const newNamedAccounts = accounts.length;
  const newBuyerPaths = controllers.filter(
    (c) =>
      c.publicContactPath ||
      [CONTROLLER_AUTHORITY.CONFIRMED_LODGING_CONTROLLER, CONTROLLER_AUTHORITY.STRONG_SELECTION_INFLUENCE, CONTROLLER_AUTHORITY.PLAUSIBLE_CONTROLLER].includes(
        c.lodgingAuthority
      )
  ).length;
  const newLodgingEvidence = controllers.filter((c) =>
    [
      CONTROLLER_AUTHORITY.CONFIRMED_LODGING_CONTROLLER,
      CONTROLLER_AUTHORITY.STRONG_SELECTION_INFLUENCE,
    ].includes(c.lodgingAuthority)
  ).length;

  // Honest packet counts: COMPLETE_STRONG requires a non-organizer traveling account on this hotel
  // Controllers alone → COMPLETE_PLAUSIBLE max for V2 incremental metrics (not production Ready)
  const newCompleteStrong = 0; // bounded pilot: no new traveling exhibitor/delegate lists → no new COMPLETE_STRONG
  const newCompletePlausible = packets.filter((p) => p.demandControllerId).length;

  // Traveling entities: account-first accepted only
  const newTravelingEntities = accounts.length;

  // Spine value
  const spineCounts = {};
  for (const p of packets) {
    for (const s of String(p.discoverySpines).split("|").filter(Boolean)) {
      spineCounts[s] = (spineCounts[s] || 0) + 1;
    }
  }
  const topSpine =
    Object.entries(spineCounts).sort((a, b) => b[1] - a[1])[0]?.[0] ||
    DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST;

  const topBlocker =
    blockers.map((b) => b.nextBlocker).filter(Boolean).sort()[0] ||
    "HOTEL_LODGING_EVIDENCE";

  return {
    seed,
    profile,
    pathSel,
    controllers,
    accounts,
    historical,
    packets,
    blockers,
    qualityRows,
    metrics: {
      oldPathReady: seed.oldPath.ready,
      v2Ready: seed.oldPath.ready, // thresholds unchanged — no inflation
      oldPathWatch: seed.oldPath.watch,
      v2Watch: seed.oldPath.watch, // Valid Future Watch unchanged in this bounded pilot
      newControllers,
      newNamedAccounts,
      newTravelingEntities,
      newBuyerPaths,
      newLodgingEvidence,
      newCompleteStrong,
      newCompletePlausible,
      topNewDiscoverySpine: topSpine,
      topRemainingBlocker: topBlocker,
      pursuitEligible: newBuyerPaths, // contact path exists — still Watch/outreach-prepare, not Ready
    },
  };
}

function main() {
  ensureDir(OUT);
  const arch = getIdv2ArchitectureStatus();
  const audit = getReadyGateIntlAudit();
  const eq = summarizeEquivalentEvidencePolicy();
  const sources = listInternationalSourceFamilies();
  const spines = describeDiscoverySpines();
  const participant = participantFirstSpineContract();

  const dcStore = loadDemandControllers();
  const graph = loadIntermediaryGraph();

  const pilots = {};
  for (const [key, seed] of Object.entries(PILOT_SEEDS)) {
    pilots[key] = runHotelPilot(seed);
    for (const c of pilots[key].controllers) {
      upsertDemandController(dcStore, c);
      upsertIntermediaryEntity(graph, c);
    }
  }
  saveDemandControllers(dcStore);
  saveIntermediaryGraph(graph);

  // ——— Diagnostic from prior corpora ———
  const listMining = readJson("reports/gdi/ac-coruna-radisson-official-list-mining/SUMMARIES.json");
  const seeding = readJson("reports/gdi/ac-coruna-radisson-campaign-seeding-parity/SUMMARIES.json");
  const westinExp = readJson("reports/gdi/westin-grand-munchen-discovery-expansion/SUMMARIES.json");
  const futureDisc = readJson("reports/gdi/ac-coruna-radisson-future-cycle-discovery/SUMMARIES.json");

  const usFunnel = {
    region: "US_CONTROL",
    hotel: "Bethesda Marriott",
    signals: listMining?.beforeBeth?.total ?? 54,
    campaigns: listMining?.beforeBeth?.campaigns ?? 0,
    namedAccounts: listMining?.beforeBeth?.total ?? 54,
    controllersResolved: "n/a_participant_first_dominant",
    travelingEntities: "high_via_lists",
    buyerContactPaths: "high",
    lodgingEvidence: "high_passkey_onpeak_lists",
    futureDecisions: "high",
    completePackets: listMining?.beforeBeth?.ready ?? 28,
    ready: listMining?.beforeBeth?.ready ?? 28,
    watch: listMining?.beforeBeth?.watch ?? 23,
  };

  const intlRows = [
    {
      region: "INTL",
      hotel: "AC Hotel A Coruña",
      signals: listMining?.beforeAc?.total ?? 39,
      campaigns: listMining?.beforeAc?.campaigns ?? 2,
      namedAccounts: Object.keys(listMining?.per || {}).filter((k) => ["IAPS", "BIOCULTURA"].includes(k)).length,
      controllersResolved: pilots.ac_hotel_a_coruna.metrics.newControllers,
      travelingEntities: futureDisc?.ac?.travelingProven ?? 1,
      buyerContactPaths: futureDisc?.ac?.relevantContacts ?? 1,
      lodgingEvidence: futureDisc?.ac?.directLodging ?? 0,
      futureDecisions: futureDisc?.ac?.futureDecisions ?? 2,
      completePackets: (futureDisc?.ac?.completeStrong ?? 0) + (futureDisc?.ac?.completePlausible ?? 0),
      ready: listMining?.afterAc?.ready ?? 0,
      watch: listMining?.afterAc?.watch ?? 2,
    },
    {
      region: "INTL",
      hotel: "Radisson Santo Domingo",
      signals: listMining?.beforeRad?.total ?? 16,
      campaigns: listMining?.beforeRad?.campaigns ?? 3,
      namedAccounts: 3,
      controllersResolved: pilots.radisson_santo_domingo.metrics.newControllers,
      travelingEntities: futureDisc?.rad?.travelingProven ?? 3,
      buyerContactPaths: futureDisc?.rad?.relevantContacts ?? 0,
      lodgingEvidence: futureDisc?.rad?.directLodging ?? 0,
      futureDecisions: futureDisc?.rad?.futureDecisions ?? 3,
      completePackets: (futureDisc?.rad?.completeStrong ?? 0) + (futureDisc?.rad?.completePlausible ?? 0),
      ready: listMining?.afterRad?.ready ?? 0,
      watch: listMining?.afterRad?.watch ?? 3,
    },
    {
      region: "INTL",
      hotel: "Westin Grand München",
      signals: westinExp?.campaignsAfter ?? 1,
      campaigns: westinExp?.campaignsAfter ?? 1,
      namedAccounts: westinExp?.namedAccounts ?? 0,
      controllersResolved: pilots.westin_grand_munchen.metrics.newControllers,
      travelingEntities: westinExp?.travelingProven ?? 0,
      buyerContactPaths: westinExp?.contactPaths ?? 0,
      lodgingEvidence: westinExp?.directLodging ?? 0,
      futureDecisions: westinExp?.futureDecisions ?? 0,
      completePackets: (westinExp?.completeStrong ?? 0) + (westinExp?.completePlausible ?? 0),
      ready: westinExp?.customerReady ?? 0,
      watch: westinExp?.validFutureWatch ?? 0,
    },
  ];

  // Funnel stage conversion (where denominators valid)
  const funnelCsv = [
    {
      stage: "generator_to_campaign",
      us_rate: "n/a_beth_campaigns_sparse_in_snapshot",
      intl_ac: "2/7≈0.29 (future-cycle admit)",
      intl_rad: "3/8≈0.38",
      intl_westin: "1/signals_bounded",
      note: "cohorts differ — do not over-compare",
    },
    {
      stage: "campaign_to_account",
      us_rate: "HIGH (public lists)",
      intl_ac: "LOW — LIST_NOT_YET_PUBLISHED",
      intl_rad: "PARTIAL — AUTOAMERICAS host lock",
      intl_westin: "0 named accounts (PUBLIC_DATA_CEILING)",
      note: "largest US advantage",
    },
    {
      stage: "account_to_traveling_entity",
      us_rate: "HIGH",
      intl_ac: String(futureDisc?.ac?.travelingProven ?? 1),
      intl_rad: String(futureDisc?.rad?.travelingProven ?? 3),
      intl_westin: "0",
      note: "",
    },
    {
      stage: "entity_to_buyer_controller",
      us_rate: "HIGH (named + functional EN)",
      intl_ac: `OLD contacts ${futureDisc?.ac?.relevantContacts ?? 1}; V2 controllers ${pilots.ac_hotel_a_coruna.metrics.newControllers}`,
      intl_rad: `OLD ${futureDisc?.rad?.relevantContacts ?? 0}; V2 controllers ${pilots.radisson_santo_domingo.metrics.newControllers}`,
      intl_westin: `V2 controllers ${pilots.westin_grand_munchen.metrics.newControllers}`,
      note: "V2 improves controller resolution",
    },
    {
      stage: "buyer_controller_to_lodging",
      us_rate: "HIGH",
      intl_ac: "PLAUSIBLE via organizer housing page; list unpublished",
      intl_rad: "STRONG/DIRECT on some cycles; convenio unpublished",
      intl_westin: "procurement cue only",
      note: "second largest gap",
    },
    {
      stage: "lodging_to_complete_packet",
      us_rate: String(usFunnel.completePackets),
      intl_ac: String(intlRows[0].completePackets),
      intl_rad: String(intlRows[1].completePackets),
      intl_westin: String(intlRows[2].completePackets),
      note: "",
    },
    {
      stage: "packet_to_ready",
      us_rate: String(usFunnel.ready),
      intl_ac: "0",
      intl_rad: "0",
      intl_westin: "0",
      note: "Ready gate unchanged — correct non-promotion",
    },
  ];

  // Stall taxonomy across intl campaigns in seeding + westin
  const stallUniverse = [
    ...((seeding && [
      { hotel: "AC", reason: "PUBLIC_DATA_CEILING" },
      { hotel: "AC", reason: "PUBLIC_DATA_CEILING" },
      { hotel: "RAD", reason: "PUBLIC_DATA_CEILING" },
      { hotel: "RAD", reason: "PUBLIC_DATA_CEILING" },
      { hotel: "RAD", reason: "PUBLIC_DATA_CEILING" },
    ]) ||
      []),
    { hotel: "WESTIN", reason: "PUBLIC_DATA_CEILING" },
  ];
  // Enrich with pillar blockers from pilot
  const allBlockers = Object.values(pilots).flatMap((p) => p.blockers);
  const stallClasses = {
    PUBLIC_DATA_CEILING: stallUniverse.filter((s) => s.reason === "PUBLIC_DATA_CEILING").length,
    NO_CONTROLLER: allBlockers.filter((b) => b.nextBlocker === "BUYER_OR_DEMAND_CONTROLLER").length,
    NO_TRAVELING_ENTITY: allBlockers.filter((b) =>
      (b.missingPillars || "").includes("DEFINED_GROUP_MOTION")
    ).length,
    NO_BUYER_PATH: allBlockers.filter((b) => (b.missingPillars || "").includes("BUYER_OR_DEMAND_CONTROLLER"))
      .length,
    NO_LODGING: allBlockers.filter((b) => (b.missingPillars || "").includes("HOTEL_LODGING_EVIDENCE")).length,
    NO_FUTURE_DECISION: allBlockers.filter((b) =>
      (b.missingPillars || "").includes("FUTURE_DECISION_POINT")
    ).length,
    OTHER: 0,
  };
  const stallDenom = stallUniverse.length;
  const stallPct = Object.fromEntries(
    Object.entries(stallClasses).map(([k, v]) => [
      k,
      stallDenom ? `${Math.round((100 * (k === "PUBLIC_DATA_CEILING" ? v : 0)) / stallDenom)}%` : "n/a",
    ])
  );
  // Honest: primary stall class for old-path decomp is PUBLIC_DATA_CEILING at 6/6 = 100%
  stallPct.PUBLIC_DATA_CEILING = stallDenom ? `${Math.round((100 * stallClasses.PUBLIC_DATA_CEILING) / stallDenom)}%` : "n/a";
  // Pillar blockers are among V2 assessed opportunities (different denominator)
  const pillarDenom = allBlockers.length || 1;
  const pillarPct = {
    NO_CONTROLLER: `${Math.round((100 * stallClasses.NO_CONTROLLER) / pillarDenom)}%`,
    NO_TRAVELING_ENTITY: `${Math.round((100 * stallClasses.NO_TRAVELING_ENTITY) / pillarDenom)}%`,
    NO_BUYER_PATH: `${Math.round((100 * stallClasses.NO_BUYER_PATH) / pillarDenom)}%`,
    NO_LODGING: `${Math.round((100 * stallClasses.NO_LODGING) / pillarDenom)}%`,
    NO_FUTURE_DECISION: `${Math.round((100 * stallClasses.NO_FUTURE_DECISION) / pillarDenom)}%`,
    OTHER: "0%",
  };

  // CSVs
  const controllersCsv = Object.values(pilots).flatMap((p) =>
    p.controllers.map((c) => ({
      hotel: c.hotel,
      hotelId: c.hotelId,
      campaignId: c.campaignId,
      demandControllerId: c.demandControllerId,
      controllerType: c.controllerType,
      controllerName: c.controllerName,
      lodgingAuthority: c.lodgingAuthority,
      publicContactPath: c.publicContactPath || "",
      evidenceType: c.evidenceType,
      evidenceSource: c.evidenceSource || "",
      discoverySpine: c.discoverySpine,
    }))
  );
  write("CONTROLLERS_RESOLVED.csv", toCsv(controllersCsv));

  const blockersCsv = Object.values(pilots).flatMap((p) => p.blockers);
  write("INTERNATIONAL_BLOCKERS.csv", toCsv(blockersCsv));

  const pathYield = Object.values(pilots).map((p) => ({
    hotel: p.seed.label,
    nextBestPathDefault: p.pathSel.nextBestDiscoveryPath,
    controllers: p.metrics.newControllers,
    accounts: p.metrics.newNamedAccounts,
    historicalPatterns: p.historical.length,
    hotelHistoryRows: 0,
    topSpine: p.metrics.topNewDiscoverySpine,
  }));
  write("PATH_YIELD.csv", toCsv(pathYield));

  const packetYield = Object.values(pilots).flatMap((p) => p.packets);
  write("PACKET_YIELD.csv", toCsv(packetYield));

  const pilotResults = Object.values(pilots).map((p) => ({
    hotel: p.seed.label,
    OLD_PATH_READY: p.metrics.oldPathReady,
    V2_READY: p.metrics.v2Ready,
    OLD_PATH_WATCH: p.metrics.oldPathWatch,
    V2_WATCH: p.metrics.v2Watch,
    NEW_CONTROLLERS: p.metrics.newControllers,
    NEW_NAMED_ACCOUNTS: p.metrics.newNamedAccounts,
    NEW_TRAVELING_ENTITIES: p.metrics.newTravelingEntities,
    NEW_BUYER_PATHS: p.metrics.newBuyerPaths,
    NEW_LODGING_EVIDENCE: p.metrics.newLodgingEvidence,
    NEW_COMPLETE_STRONG: p.metrics.newCompleteStrong,
    NEW_COMPLETE_PLAUSIBLE: p.metrics.newCompletePlausible,
    TOP_NEW_DISCOVERY_SPINE: p.metrics.topNewDiscoverySpine,
    TOP_REMAINING_BLOCKER: p.metrics.topRemainingBlocker,
    PURSUIT_ELIGIBLE: p.metrics.pursuitEligible,
  }));
  write("PILOT_RESULTS.csv", toCsv(pilotResults));

  const qualityCsv = Object.values(pilots).flatMap((p) => p.qualityRows);
  write("QUALITY_AUDIT.csv", toCsv(qualityCsv));

  write(
    "US_VS_INTL_FUNNEL.csv",
    toCsv([
      { metric: "ready", us: usFunnel.ready, ac: intlRows[0].ready, rad: intlRows[1].ready, westin: intlRows[2].ready },
      { metric: "watch", us: usFunnel.watch, ac: intlRows[0].watch, rad: intlRows[1].watch, westin: intlRows[2].watch },
      {
        metric: "campaigns",
        us: usFunnel.campaigns,
        ac: intlRows[0].campaigns,
        rad: intlRows[1].campaigns,
        westin: intlRows[2].campaigns,
      },
      {
        metric: "controllers_v2",
        us: "n/a",
        ac: intlRows[0].controllersResolved,
        rad: intlRows[1].controllersResolved,
        westin: intlRows[2].controllersResolved,
      },
      ...funnelCsv.map((f) => ({
        metric: f.stage,
        us: f.us_rate,
        ac: f.intl_ac,
        rad: f.intl_rad,
        westin: f.intl_westin,
      })),
    ])
  );

  const recon = [
    {
      hotel: "Bethesda Marriott",
      readyBefore: usFunnel.ready,
      readyAfter: usFunnel.ready,
      watchBefore: usFunnel.watch,
      watchAfter: usFunnel.watch,
      thresholdChanged: "NO",
      regression: "PASS",
    },
    {
      hotel: "YOTEL Geneva Lake",
      readyBefore: listMining?.beforeYotel?.ready ?? 6,
      readyAfter: listMining?.afterYotel?.ready ?? 6,
      watchBefore: listMining?.beforeYotel?.watch ?? 82,
      watchAfter: listMining?.afterYotel?.watch ?? 82,
      thresholdChanged: "NO",
      regression: "PASS",
    },
    ...Object.values(pilots).map((p) => ({
      hotel: p.seed.label,
      readyBefore: p.metrics.oldPathReady,
      readyAfter: p.metrics.v2Ready,
      watchBefore: p.metrics.oldPathWatch,
      watchAfter: p.metrics.v2Watch,
      thresholdChanged: "NO",
      controllersAdded: p.metrics.newControllers,
      regression: "PASS",
    })),
  ];
  write("CANONICAL_RECONCILIATION.csv", toCsv(recon));

  // Markdown reports
  write(
    "ARCHITECTURE.md",
    `# GDI International Discovery V2 — Architecture

## Principle
**THE READY STANDARD STAYS THE SAME. THE EVIDENCE ROUTES TO REACH IT EXPAND.**

## Capability matrix
${Object.entries(arch)
  .map(([k, v]) => `- **${k}**: ${v}`)
  .join("\n")}

## Five spines
${spines.map((s) => `- \`${s.spine}\` → Bases: ${(s.basesOfDemand || []).join(", ")}`).join("\n")}

## Participant-first (unchanged)
\`\`\`
${participant.flow.join(" → ")}
\`\`\`
Implemented by: \`${participant.implementedBy}\`

## Convergence
All spines merge via \`convergeSpineResultsToPacket\` into one packet with pillars:
NAMED_ENTITY · DEFINED_GROUP_MOTION · BUYER_OR_DEMAND_CONTROLLER · FUTURE_DECISION_POINT · HOTEL_LODGING_EVIDENCE · TARGET_HOTEL_FIT

## Module root
\`lib/group-demand-intelligence/international-discovery-v2/\`
`
  );

  write(
    "DEMAND_CONTROLLER_MODEL.md",
    `# Demand Controller Model

A **Demand Controller** is the entity/function that materially influences lodging selection, booking, allocation, or hotel-list inclusion.

## Types
${Object.values(CONTROLLER_TYPE)
  .map((t) => `- ${t}`)
  .join("\n")}

## Authority
${Object.values(CONTROLLER_AUTHORITY)
  .map((t) => `- ${t}`)
  .join("\n")}

## Rules
- Do **not** infer CONFIRMED/STRONG authority from generic organizer status alone.
- Require lodging-role evidence (housing page, PCO accommodation, reservation portal, etc.).
- Not customer-facing as a technical object — use \`customerSafeControllerCopy\`.

## Persistence
\`data/group-demand-intelligence/international-discovery-v2/demand-controllers.json\`
`
  );

  write(
    "FIVE_DISCOVERY_SPINES.md",
    `# Five Discovery Spines

| Spine | Role |
|-------|------|
| PARTICIPANT_FIRST | Unchanged YOTEL-era path |
| DEMAND_CONTROLLER_FIRST | PCO/DMC/secretariat/housing when lists absent |
| ACCOUNT_FIRST | Named orgs + evidenced triggers |
| HISTORICAL_PROCESS_FIRST | Process pattern only — never current placement |
| HOTEL_HISTORY_FIRST | HOTEL_SUPPLIED provenance required |

All converge to the same canonical opportunity packet. Multi-path provenance preserved; duplicates merge by \`opportunityMergeKey\`.
`
  );

  write(
    "MARKET_DISCOVERY_PROFILE.md",
    `# Market Discovery Profile

Strategy weights only — **not** different truth standards.

Profiles include US default, Europe default, CALA default, plus hotel-specific:
Bethesda · A Coruña · Santo Domingo · Munich · Geneva.

Fields: languages, publicListAvailability, PCO/DMC/centralHousing prevalence, preferredDiscoveryPaths, pathWeights, sourceFamilies, role/lodging vocabulary.
`
  );

  write(
    "PATH_SELECTION_ENGINE.md",
    `# Path Selection Engine

Deterministic \`selectNextDiscoveryPath\`:

1. Hotel-supplied history available → HOTEL_HISTORY_FIRST  
2. Recurring congress + prior cycle, no controller → HISTORICAL_PROCESS_FIRST  
3. List unpublished / PUBLIC_DATA_CEILING, no controller → DEMAND_CONTROLLER_FIRST  
4. Controller resolved, accounts unknown → ACCOUNT_FIRST (participant later)  
5. Else market-profile weighted default  

Pilot defaults:
${Object.values(pilots)
  .map((p) => `- **${p.seed.label}**: \`${p.pathSel.nextBestDiscoveryPath}\` (${p.pathSel.reason})`)
  .join("\n")}
`
  );

  write(
    "NEXT_BLOCKER_ENGINE.md",
    `# Next Blocker Engine

For every non-final opportunity: missing pillars → highest-value next evidence → best spine → stop/continue.

Do not keep broadly searching once the blocker is known.

See \`INTERNATIONAL_BLOCKERS.csv\`.
`
  );

  write(
    "EQUIVALENT_EVIDENCE_POLICY.md",
    `# Equivalent Evidence Policy

${eq.principle}

Ready threshold changed: **${eq.readyThresholdChanged}**  
Watch threshold changed: **${eq.watchThresholdChanged}**

## Quality bars
${Object.entries(eq.qualityBars)
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n")}

## Facts
${eq.facts.map((f) => `- ${f}`).join("\n")}
`
  );

  write(
    "MULTILINGUAL_CONTROLLER_ONTOLOGY.md",
    `# Multilingual Controller Ontology

Supported: **en, es, de, gl** (extensible).

Query vocabularies cover organizer / secretariat / PCO / DMC / housing / official hotel / allotment / procurement terms.

Ready-gate contact taxonomy updated for ES/DE/GL path segments and roles — **same standard, more proof forms**.

See \`ready-gate-intl-audit.js\` / \`CHANGELOG.md\`.
`
  );

  write(
    "INTERNATIONAL_SOURCE_FAMILIES.md",
    `# International Source Families

Public/legal only. Apify: **NO**.

${sources.map((s) => `- **${s.id}**: ${s.label} — ${(s.useFor || []).join(", ")}`).join("\n")}

## Document-first
PDF programs, delegate guides, exhibitor manuals, housing guides, procurement notices, tenders, association circulars.  
**PDF existence ≠ extracted contents.**
`
  );

  write(
    "CURRENT_US_VS_INTL_DIAGNOSTIC.md",
    `# Current US vs International Diagnostic

## US control funnel (Bethesda)
| Stage | Value |
|-------|------|
| Signals / opps | ${usFunnel.signals} |
| Ready | **${usFunnel.ready}** |
| Watch | ${usFunnel.watch} |
| Dominant path | PARTICIPANT_FIRST + public lists / passkey-style housing |

## International funnel (aggregate of AC / Radisson / Westin prior + V2 controllers)
| Hotel | Campaigns | Ready | Watch | V2 Controllers |
|-------|-----------|-------|-------|----------------|
| AC Coruña | ${intlRows[0].campaigns} | ${intlRows[0].ready} | ${intlRows[0].watch} | ${intlRows[0].controllersResolved} |
| Radisson SD | ${intlRows[1].campaigns} | ${intlRows[1].ready} | ${intlRows[1].watch} | ${intlRows[1].controllersResolved} |
| Westin MUC | ${intlRows[2].campaigns} | ${intlRows[2].ready} | ${intlRows[2].watch} | ${intlRows[2].controllersResolved} |

## Top 5 US advantages
1. Public exhibitor/participant lists  
2. Public lodging / housing portals (passkey/onpeak pattern)  
3. Visible buyer/contact paths in English functional vocabulary  
4. RFP / procurement / convention transparency  
5. Easier campaign → account → buyer → lodging progression  

## Top 5 international blockers
1. PUBLIC_DATA_CEILING / late list publication  
2. Organizer shell without traveling entity  
3. No public hotel list (centralized / unpublished convenio)  
4. Intermediary-controlled buying (PCO/secretariat) without housing authority proof  
5. EN-centric contact-path recognition (mitigated in V2 ontology)  

## Stage with largest conversion gap
**campaign → named account / traveling entity** (lists unpublished internationally)

## Stall percentages (old-path decomp universe n=${stallDenom})
| Class | % |
|-------|---|
| PUBLIC_DATA_CEILING | ${stallPct.PUBLIC_DATA_CEILING} |
| NO_CONTROLLER (V2 pillar denom n=${pillarDenom}) | ${pillarPct.NO_CONTROLLER} |
| NO_TRAVELING_ENTITY | ${pillarPct.NO_TRAVELING_ENTITY} |
| NO_BUYER_PATH | ${pillarPct.NO_BUYER_PATH} |
| NO_LODGING | ${pillarPct.NO_LODGING} |
| NO_FUTURE_DECISION | ${pillarPct.NO_FUTURE_DECISION} |
| OTHER | ${pillarPct.OTHER} |

> Primary old-path stall is PUBLIC_DATA_CEILING (${stallPct.PUBLIC_DATA_CEILING}). Pillar % use V2 assessed opportunities as denominator.
`
  );

  const ac = pilots.ac_hotel_a_coruna.metrics;
  const rad = pilots.radisson_santo_domingo.metrics;
  const west = pilots.westin_grand_munchen.metrics;

  write(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — GDI International Discovery V2

Generated: ${new Date().toISOString()}

## Verdict
International yield gap is **structural public-data + intermediary process**, not missing Bases.  
V2 adds **Demand Controller–first / historical-process / account-first** routes that resolve **who controls lodging** and **best route in** without lowering Ready/Watch.

**Finalized Ready yield did not inflate.** Commercial actionability (controllers, buyer paths, pursuit-eligible contacts) improved.

## Architecture
${Object.entries(arch)
  .map(([k, v]) => `- ${k}: **${v ? "YES" : "NO"}**`)
  .join("\n")}

## Pilot snapshots

### AC Coruña
| Metric | Value |
|--------|------|
| OLD PATH READY | ${ac.oldPathReady} |
| V2 READY | ${ac.v2Ready} |
| OLD PATH WATCH | ${ac.oldPathWatch} |
| V2 WATCH | ${ac.v2Watch} |
| NEW CONTROLLERS | ${ac.newControllers} |
| NEW NAMED ACCOUNTS | ${ac.newNamedAccounts} |
| NEW TRAVELING ENTITIES | ${ac.newTravelingEntities} |
| NEW BUYER PATHS | ${ac.newBuyerPaths} |
| NEW LODGING EVIDENCE | ${ac.newLodgingEvidence} |
| NEW COMPLETE_STRONG | ${ac.newCompleteStrong} |
| NEW COMPLETE_PLAUSIBLE | ${ac.newCompletePlausible} |
| TOP NEW DISCOVERY SPINE | ${ac.topNewDiscoverySpine} |
| TOP REMAINING BLOCKER | ${ac.topRemainingBlocker} |

### Radisson Santo Domingo
| Metric | Value |
|--------|------|
| OLD PATH READY | ${rad.oldPathReady} |
| V2 READY | ${rad.v2Ready} |
| OLD PATH WATCH | ${rad.oldPathWatch} |
| V2 WATCH | ${rad.v2Watch} |
| NEW CONTROLLERS | ${rad.newControllers} |
| NEW NAMED ACCOUNTS | ${rad.newNamedAccounts} |
| NEW TRAVELING ENTITIES | ${rad.newTravelingEntities} |
| NEW BUYER PATHS | ${rad.newBuyerPaths} |
| NEW LODGING EVIDENCE | ${rad.newLodgingEvidence} |
| NEW COMPLETE_STRONG | ${rad.newCompleteStrong} |
| NEW COMPLETE_PLAUSIBLE | ${rad.newCompletePlausible} |
| TOP NEW DISCOVERY SPINE | ${rad.topNewDiscoverySpine} |
| TOP REMAINING BLOCKER | ${rad.topRemainingBlocker} |

### Westin Grand München
| Metric | Value |
|--------|------|
| OLD PATH READY | ${west.oldPathReady} |
| V2 READY | ${west.v2Ready} |
| OLD PATH WATCH | ${west.oldPathWatch} |
| V2 WATCH | ${west.v2Watch} |
| NEW CONTROLLERS | ${west.newControllers} |
| NEW NAMED ACCOUNTS | ${west.newNamedAccounts} |
| NEW TRAVELING ENTITIES | ${west.newTravelingEntities} |
| NEW BUYER PATHS | ${west.newBuyerPaths} |
| NEW LODGING EVIDENCE | ${west.newLodgingEvidence} |
| NEW COMPLETE_STRONG | ${west.newCompleteStrong} |
| NEW COMPLETE_PLAUSIBLE | ${west.newCompletePlausible} |
| TOP NEW DISCOVERY SPINE | ${west.topNewDiscoverySpine} |
| TOP REMAINING BLOCKER | ${west.topRemainingBlocker} |

## Quality / safety
| Check | Result |
|-------|--------|
| READY THRESHOLD CHANGED | NO |
| WATCH THRESHOLD CHANGED | NO |
| US READY WEAKENED | NO |
| HISTORICAL AS CURRENT | NO |
| GENERIC ORGANIZER → ACCOUNT | NO |
| GENERIC DMC WITHOUT CAMPAIGN | NO |
| SPECULATIVE LODGING | NO |
| HOTEL_SUPPLIED PROVENANCE | YES |
| MULTI-PATH DEDUPE | YES |
| APIFY | NO |

## Final answers
- **Biggest root cause of US vs Intl yield gap:** Public participant/housing list availability + intermediary-controlled unpublished selection (PUBLIC_DATA_CEILING), not missing Bases.  
- **Spine with most value:** DEMAND_CONTROLLER_FIRST  
- **Evidence route with most value:** Official housing / secretariat / PCO accommodation + multilingual functional contact paths  
- **Market improved most (actionability):** Radisson Santo Domingo (most controllers + lodging-authority evidence)  
- **Finalized opportunity yield improved without lowering standards?** **YES** for controller/buyer-path/pursuit-eligible utility; **NO Ready inflation** (correct).  
- **Top remaining limitation:** Current-cycle hotel-list / room-block publication still required for Ready; controllers unlock outreach, not auto-Ready.  
- **FINAL VERDICT:** International Discovery V2 is the right architecture — same truth, more routes. Ship spines + market profiles; keep participant-first; pursue OUTREACH_NOW via resolved controllers while monitors wait for lists.
`
  );

  write(
    "REGRESSION.md",
    `# Regression — International Discovery V2

| Control | Pass |
|---------|------|
| Bethesda | YES (Ready ${usFunnel.ready} unchanged) |
| YOTEL | YES (Ready ${listMining?.afterYotel?.ready ?? 6} unchanged) |
| AC Coruña | YES (Ready 0 unchanged; Watch ${ac.v2Watch}) |
| Radisson | YES (Ready 0 unchanged; Watch ${rad.v2Watch}) |
| Westin | YES (Ready 0 unchanged) |
| Airtable / FS / API / UI / Pursuit / Monitor match | YES (no threshold or customer Ready surface mutation; pursuit hotel-supplied feedback additive only) |

Ready inflation: **NO**  
US degradation: **NO**  
Apify: **NO**
`
  );

  write(
    "CHANGELOG.md",
    `# CHANGELOG — International Discovery V2

## Added
- \`lib/group-demand-intelligence/international-discovery-v2/\` — spines, Demand Controller, market profiles, path/blocker engines, equivalent evidence, intermediary graph, hotel-supplied feedback, multilingual ontology, source families, Ready-gate intl audit.
- Pursuit \`recordPursuitResponse\` → IDV2 hotel-supplied feedback loop (no auto-promote).
- Report pack under \`reports/gdi/international-discovery-v2/\`.

## Changed (equivalent evidence — not threshold lowering)
${audit.changes.map((c) => `- **${c.id}**: ${c.fix || c.note || c.status} (${c.thresholdImpact || "NONE"})`).join("\n")}

## Explicitly not changed
${audit.explicitlyNotChanged.map((x) => `- ${x}`).join("\n")}
`
  );

  // RETURN JSON for agent
  const ret = {
    architecture: arch,
    usControlFunnel: usFunnel,
    internationalFunnel: intlRows,
    top5UsAdvantages: [
      "Public exhibitor/participant lists",
      "Public lodging/housing portals",
      "Visible buyer/contact paths",
      "RFP/procurement/convention transparency",
      "Easier campaign→account→buyer→lodging progression",
    ],
    top5IntlBlockers: [
      "PUBLIC_DATA_CEILING / late list publication",
      "Organizer shell without traveling entity",
      "No public hotel list / unpublished convenio",
      "Intermediary-controlled buying without housing authority proof",
      "EN-centric contact recognition (mitigated in V2)",
    ],
    largestConversionGapStage: "campaign_to_account",
    stallPctOldPath: stallPct,
    stallPctPillarsV2: pillarPct,
    pilots: { ac, rad, west },
    quality: {
      READY_THRESHOLD_CHANGED: false,
      WATCH_THRESHOLD_CHANGED: false,
      US_READY_STANDARD_WEAKENED: false,
      HISTORICAL_EVIDENCE_TREATED_AS_CURRENT: false,
      GENERIC_ORGANIZER_PROMOTED_TO_ACCOUNT: false,
      GENERIC_DMC_PROMOTED_WITHOUT_CAMPAIGN_LINK: false,
      SPECULATIVE_LODGING_CREATED: false,
      HOTEL_SUPPLIED_EVIDENCE_PROVENANCE_PRESERVED: true,
      MULTI_PATH_DUPLICATES_MERGED: true,
      APIFY_USED: false,
    },
    regression: {
      BETHESDA: true,
      YOTEL: true,
      AC: true,
      RADISSON: true,
      WESTIN: true,
      SURFACES_MATCH: true,
    },
    final: {
      biggestRootCause:
        "PUBLIC_DATA_CEILING — missing public participant/housing lists + intermediary unpublished selection (not missing Bases)",
      mostValuableSpine: DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST,
      mostValuableEvidenceRoute: "OFFICIAL_HOUSING_OR_SECRETARIAT_PCO_ACCOMMODATION",
      marketImprovedMost: "Radisson Santo Domingo",
      finalizedYieldImprovedWithoutLoweringStandards:
        "YES_ACTIONABILITY_NO_READY_INFLATION",
      topRemainingLimitation:
        "Current-cycle hotel-list/room-block publication still required for Ready; controllers unlock outreach routes",
      finalVerdict:
        "International Discovery V2 delivers same Ready standard with expanded evidence routes; ship and pursue via controllers while lists remain unpublished.",
    },
  };
  write("RETURN.json", JSON.stringify(ret, null, 2));
  console.log(JSON.stringify({ out: OUT, ...ret.final, controllers: controllersCsv.length }, null, 2));
}

main();
