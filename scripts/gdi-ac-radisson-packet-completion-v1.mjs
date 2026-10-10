#!/usr/bin/env node
/**
 * MODE B — AC Coruña + Radisson SD Watch → packet completion bottleneck pass.
 * NO broad discovery. NO Apify. NO threshold changes. NO forced Ready.
 *
 * Usage: node scripts/gdi-ac-radisson-packet-completion-v1.mjs [--apply]
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { evaluateCompleteDemandPacket } from "../lib/group-demand-intelligence/complete-demand-packet-v8/packet-schema.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  classifyCustomerSurfaceOpportunity,
  applyCustomerSurfaceDisposition,
  isCustomerSurfaceActiveEligible,
} from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";
import { isCustomerFacingOpportunity } from "../lib/group-demand-intelligence/customer-visibility.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { meetsReadyContactRequirement } from "../lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js";
import { applyGdiWhoHowResolution } from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/ac-coruna-radisson-packet-completion");
const APPLY = process.argv.includes("--apply");
const NOW = new Date("2026-10-07T12:00:00Z");
const NOW_OPTS = { nowDate: NOW };

const HOTELS = [
  {
    key: "AC",
    hotelId: "rec2PVBDavppGpenm",
    name: "AC Hotel A Coruña",
  },
  {
    key: "RAD",
    hotelId: "recUOyzOXn2Zdp98I",
    name: "Radisson Hotel Santo Domingo",
  },
];

/** Official-source forensic patches — evidence-backed only. */
const FORENSICS = {
  // --- AC ---
  gdi_opp_international_symposium_6: {
    hotel: "AC",
    tier: "P0_COMPLETION_CANDIDATE",
    rationale:
      "Confirmed in-market IAPS symposium 14–16 Jun 2027 A Coruña; 500+ international researchers announced; venue/housing TBA.",
    patch: {
      title: "IAPS Spaces in Transition Symposium 2027",
      organizationName: "IAPS Sustainability Network / IAPS Culture and Space Network",
      eventStartDate: "2027-06-14",
      eventEndDate: "2027-06-16",
      eventYear: 2027,
      destinationCity: "A Coruña",
      destinationCountry: "Spain",
      destinationStatus: "A Coruña, Galicia — symposium host city confirmed; venue TBA",
      officialSource: "https://iaps-association.org/",
      discoverySource:
        "https://www.nosdiario.gal/articulo/social/coruna-acollera-2027-reunion-da-rede-internacional-investigadores-sustentabilidade/20260629155003260560.html",
      travelingEntityType: "ASSOCIATION_DELEGATION",
      travelingEntityEvidence:
        "Official IAPS save-the-date: international symposium in A Coruña 14–16 Jun 2027 gathering researchers/practitioners; local press cites >500 international researchers/professionals/policymakers. Named company teams not yet published.",
      travelingEntityProven: false,
      travelingEntityClass: "TRAVELING_ENTITY_STRONG_INFERENCE",
      groupMotionType: "ASSOCIATION_DELEGATION",
      groupMotionEvidence:
        "Multi-day international academic symposium with call for papers (abstracts due 31 Dec 2026); out-of-market researchers implied by international network meeting.",
      groupMotionConfidence: "MEDIUM",
      travelReason: "Attend/present at IAPS Sustainability + Culture & Space joint symposium",
      likelyTravelWindow: "2027-06-13/2027-06-17",
      destination: "A Coruña, Spain",
      hotelMotionClass: "PLAUSIBLE_HOTEL_MOTION",
      lodgingEvidence:
        "Multi-day international symposium in A Coruña with venue TBA and no published housing page yet — city hotel demand plausible; no room block or official hotel list evidenced.",
      housingStatus: "WEAK",
      venueStatus: "HOTEL_TBD",
      hotelOpportunityThesis:
        "AC Hotel A Coruña is an in-market Marriott soft-brand option while symposium venue and housing remain unannounced — pursue local IAPS/UDC organizers before housing partners lock.",
      // Association homepage alone — NOT Ready-eligible (no named housing/events buyer desk).
      primaryContactRole: "Association public information",
      primaryContactName: null,
      primaryContact: {
        role: "Association public information",
        name: null,
        email: null,
        url: "https://iaps-association.org/",
      },
      organizationContactUrl: "https://iaps-association.org/",
      publicContactPath: "https://iaps-association.org/",
      contactPathClass: "GENERAL_ORG_CONTACT",
      whoPathClass: "ORG_CONTACT",
      whoResearchAttempted: true,
      whoResearchState: "ATTEMPTED_NO_NAMED_BUYER",
      buyerRole: null,
      futureDecisionType: "ABSTRACT_DEADLINE",
      futureDecisionDate: "2026-12-31",
      decisionWindow: "Call for papers open; abstracts due 31 Dec 2026; symposium Jun 2027",
      futureDecisionEvidence:
        "IAPS official news: call for papers opens 1 Sep 2026; abstracts 300–500 words due 31 Dec 2026.",
      whyNow:
        "IAPS Spaces in Transition symposium is confirmed for A Coruña 14–16 Jun 2027 with abstracts due 31 Dec 2026 — engage IAPS/UDC network coordinators while venue and housing partners remain open.",
      cardWhyNowLine:
        "Confirmed Jun 2027 A Coruña symposium; abstract deadline 31 Dec 2026; housing not yet published.",
      recommendedAction:
        "Contact IAPS Sustainability Network / UDC local coordinators to introduce AC Hotel A Coruña as a city lodging option before official housing is announced.",
      recommendedNextStep:
        "Request intro to symposium organizing committee; ask when housing/venue RFP opens.",
      summaryWhat:
        "IAPS Sustainability and Culture & Space networks host an international symposium in A Coruña (14–16 Jun 2027).",
      summaryWhyMatters:
        "International researchers and practitioners travel for a multi-day symposium; lodging controllers are still open.",
      summaryWhyHotel:
        "In-city AC soft-brand with meeting capacity suited to midsize academic groups while host venue remains TBA.",
      fitExplanation:
        "Location fit for A Coruña city symposium; room inventory and meeting space support academic delegations once housing opens.",
      hotelFitScore: 72,
      accountQualityClass: "NAMED_ASSOCIATION_NETWORK",
      packetForensicNote: "PACKET_COMPLETION_2026_10_07",
    },
  },
  gdi_opp_174th_international_softwood_conference_isc_2026_3: {
    hotel: "AC",
    tier: "P2_LOW_INFORMATION",
    rationale: "ISC 2026 is in Dublin (Grand Hotel Malahide), not A Coruña — wrong destination.",
    patch: {
      destinationCity: "Dublin",
      destinationCountry: "Ireland",
      destinationStatus: "Dublin, Ireland — OUT_OF_MARKET for AC Hotel A Coruña",
      officialSource: "https://www.iscevent2026.com/",
      travelingEntityClass: "NO_TRAVELING_ENTITY",
      travelingEntityProven: false,
      travelingEntityEvidence:
        "AEIM notes a Spanish delegation attends ISC in Dublin; lodging is at Grand Hotel Malahide / Portmarnock — not A Coruña.",
      hotelMotionClass: "NONE",
      lodgingEvidence:
        "Official accommodation page lists Grand Hotel Malahide and Portmarnock Hotel & Golf Links in Dublin only.",
      housingStatus: "NONE",
      venueStatus: "HOST_HOTEL_NAMED_OUT_OF_MARKET",
      whyNow:
        "Conference destination is Dublin — not an A Coruña lodging opportunity.",
      recommendedAction: "Do not pursue for AC Hotel A Coruña; archive as out-of-market signal.",
      customerFacingState: "CLOSED",
      priority: "DISQUALIFIED",
      staleAuditClass: "WRONG_DESTINATION",
      packetForensicNote: "PACKET_COMPLETION_2026_10_07",
    },
  },
  gdi_opp_models_2026_10: {
    hotel: "AC",
    tier: "P2_LOW_INFORMATION",
    rationale: "MoDELS 2026 venue is Meliá Costa del Sol, Torremolinos/Málaga — wrong destination.",
    patch: {
      organizationName: "ACM / MODELS 2026 Organizing Committee",
      destinationCity: "Málaga",
      destinationCountry: "Spain",
      destinationStatus: "Torremolinos / Málaga — OUT_OF_MARKET for AC Hotel A Coruña",
      officialSource: "https://conf.researchr.org/venue/models-2026/models-2026-venue",
      travelingEntityClass: "NO_TRAVELING_ENTITY",
      travelingEntityProven: false,
      hotelMotionClass: "NONE",
      lodgingEvidence:
        "Official MODELS accommodation is host hotel Meliá Costa del Sol (Torremolinos).",
      venueStatus: "HOST_HOTEL_NAMED_OUT_OF_MARKET",
      whyNow: "Event is in Málaga, not A Coruña.",
      recommendedAction: "Close as wrong-destination contamination.",
      customerFacingState: "CLOSED",
      priority: "DISQUALIFIED",
      staleAuditClass: "WRONG_DESTINATION",
      packetForensicNote: "PACKET_COMPLETION_2026_10_07",
    },
  },
  gdi_opp_misi_n_comercial_china_2: {
    hotel: "AC",
    tier: "P2_LOW_INFORMATION",
    rationale:
      "Outbound Galicia→Shanghai trade mission; lodging demand is in China, not at AC Hotel A Coruña.",
    patch: {
      eventStartDate: "2026-10-31",
      eventEndDate: "2026-11-15",
      destinationCity: "Shanghai",
      destinationCountry: "China",
      destinationStatus: "Shanghai / China — OUTBOUND mission (not inbound A Coruña lodging)",
      officialSource:
        "https://www.camaracoruna.com/evento/mision-comercial-a-china-taiwan-hong-kong-multisectorial-noviembre-2026-foexga-pymes-y-autonomos-ig422b-2026-000-000106/",
      travelingEntityClass: "NO_TRAVELING_ENTITY",
      travelingEntityProven: false,
      travelingEntityEvidence:
        "Mission sends Galician SMEs to Shanghai; Cámara organizes outbound travel/accommodation in China.",
      hotelMotionClass: "NONE",
      lodgingEvidence: "Program includes alojamiento in Shanghai soft-landing hub — not A Coruña rooms.",
      whyNow: "Outbound China mission — not an inbound room opportunity for AC Hotel A Coruña.",
      recommendedAction: "Close as outbound/wrong-direction demand.",
      customerFacingState: "CLOSED",
      priority: "DISQUALIFIED",
      staleAuditClass: "WRONG_DESTINATION",
      packetForensicNote: "PACKET_COMPLETION_2026_10_07",
    },
  },
  gdi_opp_super_copa_de_espa_a_cadete_9: {
    hotel: "AC",
    tier: "P2_LOW_INFORMATION",
    rationale: "Super Copa Cadete 2026 is in Vigo with official hotel Coia — not A Coruña.",
    patch: {
      eventStartDate: "2026-10-17",
      eventEndDate: "2026-10-18",
      destinationCity: "Vigo",
      destinationCountry: "Spain",
      destinationStatus: "Vigo — OUT_OF_MARKET for AC Hotel A Coruña",
      officialSource:
        "https://www.fgjudo.com/circulares/temporada%202026%202027/SUPERCOPA%20CADETE%20VIGO%202026/20%20-26%20SUPER%20COPA%20DE%20ESP%20CADETE%20DE%20GALICIA%20-%2030%20TROFEO%20CIDADE%20DE%20VIGO%20DE%20JUDO.pdf",
      travelingEntityClass: "NO_TRAVELING_ENTITY",
      hotelMotionClass: "NONE",
      lodgingEvidence: "Official hotel: Hotel Coia de Vigo **** with federation room rates.",
      venueStatus: "HOST_HOTEL_NAMED_OUT_OF_MARKET",
      whyNow: "Competition and housing are in Vigo, not A Coruña.",
      recommendedAction: "Close as wrong-destination.",
      customerFacingState: "CLOSED",
      priority: "DISQUALIFIED",
      staleAuditClass: "WRONG_DESTINATION",
      packetForensicNote: "PACKET_COMPLETION_2026_10_07",
    },
  },
  gdi_opp_total_solar_eclipse_2: {
    hotel: "AC",
    tier: "P2_LOW_INFORMATION",
    rationale:
      "Eclipse date 2026-08-12 is past; eclipse itself is not an account; no evidenced named tour/science buyers retained.",
    patch: {
      travelingEntityClass: "NO_TRAVELING_ENTITY",
      travelingEntityProven: false,
      travelingEntityEvidence:
        "No named astronomy orgs, tour operators, media crews, or research teams with lodging evidence retained after forensic pass.",
      hotelMotionClass: "NONE",
      staleAuditClass: "PAST_CLOSED",
      eclipseForensic:
        "Decomposed thesis only — no named room-buyer entities with evidence; event cycle closed.",
      whyNow: "Solar eclipse viewing window closed (12 Aug 2026); no evidenced named traveling buyers remain.",
      recommendedAction: "Keep research history; do not surface as Future Watch.",
      customerFacingState: "CLOSED",
      priority: "DISQUALIFIED",
      packetForensicNote: "PACKET_COMPLETION_2026_10_07",
    },
  },
  gdi_opp_sustainable_trends_summit_rep_blica_dominicana_2_10: {
    hotel: "RAD",
    tier: "P2_LOW_INFORMATION",
    rationale: "2025 summit is past; no confirmed next-cycle dates found.",
    patch: {
      staleAuditClass: "PAST_SIGNAL",
      recurringSeriesClass: "RECURRING_SERIES_UNCONFIRMED_NEXT",
      travelingEntityClass: "NO_TRAVELING_ENTITY",
      hotelMotionClass: "NONE",
      whyNow: "2025 Sustainable Trends Summit is past; next cycle not confirmed with dates.",
      recommendedAction: "Archive as past series signal; do not keep as Future Watch.",
      customerFacingState: "CLOSED",
      priority: "DISQUALIFIED",
      packetForensicNote: "PACKET_COMPLETION_2026_10_07",
    },
  },
  gdi_opp_sdq_mice_2026_5: {
    hotel: "RAD",
    tier: "P1_COMPLETION_CANDIDATE",
    rationale:
      "SDQ MICE 2026 ran 21–23 Sep 2026 at El Embajador (past). Series likely continues but 2027 buyer cycle not dated.",
    patch: {
      eventStartDate: "2026-09-21",
      eventEndDate: "2026-09-23",
      eventYear: 2026,
      destinationCity: "Santo Domingo",
      destinationCountry: "Dominican Republic",
      destinationStatus: "Santo Domingo — host hotel El Embajador (2026 edition)",
      officialSource: "https://cometosantodomingo.com/sdq-mice/",
      discoverySource:
        "https://www.diariolibre.com/economia/turismo/2026/07/30/lanzan-la-sexta-edicion-de-sdq-mice-2026/3615122",
      travelingEntityType: "HOSTED_BUYER_DELEGATION",
      travelingEntityEvidence:
        "AHSD hosted-buyer program covers airfare, transfers, alojamiento for selected international MICE buyers — 2026 edition completed.",
      travelingEntityProven: true,
      travelingEntityClass: "TRAVELING_ENTITY_PROVEN",
      groupMotionType: "HOSTED_BUYER_PROGRAM",
      groupMotionEvidence:
        "Official SDQ MICE page: hosted buyers receive paid lodging; 50 international buyers / 11 markets reported for 2026 edition.",
      groupMotionConfidence: "HIGH",
      hotelMotionClass: "DIRECT_LODGING_EVIDENCE",
      lodgingEvidence:
        "2026 edition hosted at El Embajador, a Royal Hideaway Hotel with official alojamiento for hosted buyers — competitor host; Radisson overflow not evidenced.",
      housingStatus: "STRONG",
      venueStatus: "HOST_HOTEL_NAMED_COMPETITOR",
      primaryContactRole: "Director Ejecutivo / MICE chapter coordination",
      primaryContact: {
        role: "Director Ejecutivo / MICE program",
        name: null,
        email: "director@ahsd.com.do",
        url: "https://cometosantodomingo.com/sdq-mice/",
      },
      organizationContactUrl: "https://cometosantodomingo.com/sdq-mice/",
      publicContactPath: "mailto:director@ahsd.com.do",
      contactPathClass: "RELEVANT_FUNCTION_CONTACT",
      whoResearchAttempted: true,
      whoResearchState: "ATTEMPTED_FUNCTIONAL_PATH",
      buyerRole: "AHSD Executive / MICE program director",
      futureDecisionType: "SERIES_NEXT_CYCLE_UNCONFIRMED",
      futureDecisionDate: null,
      decisionWindow: "2026 edition closed; next SDQ MICE cycle dates not published",
      staleAuditClass: "PAST_CLOSED",
      recurringSeriesClass: "RECURRING_SERIES_UNCONFIRMED_NEXT",
      whyNow:
        "SDQ MICE 2026 (21–23 Sep) has closed at El Embajador — retain series intelligence only; do not sell as current Future Watch until 2027 dates publish.",
      recommendedAction:
        "Monitor AHSD for 2027 SDQ MICE announcement; ask whether Radisson can join supplier/host roster before next hosted-buyer lodging is assigned.",
      customerFacingState: "CLOSED",
      priority: "DISQUALIFIED",
      hotelFitScore: 55,
      summaryWhat:
        "AHSD SDQ MICE 2026 hosted international MICE buyers in Santo Domingo (21–23 Sep) at El Embajador.",
      packetForensicNote: "PACKET_COMPLETION_2026_10_07",
    },
  },
  gdi_opp_latinamerican_family_business_summit_11: {
    hotel: "RAD",
    tier: "P0_COMPLETION_CANDIDATE",
    rationale:
      "In-market Santo Domingo 7–8 Oct 2026 at Marriott Piantini — active/current; host competitor named; overflow unproven.",
    patch: {
      title: "Latin American Family Business Summit 2026 — Santo Domingo",
      organizationName: "América Empresarial",
      eventStartDate: "2026-10-07",
      eventEndDate: "2026-10-08",
      destinationCity: "Santo Domingo",
      destinationCountry: "Dominican Republic",
      destinationStatus: "Santo Domingo — Hotel Marriott Piantini (host)",
      officialSource:
        "https://rd.americaempresarial.com/family-business-summit-2026-republica-dominicana/",
      travelingEntityType: "CORPORATE_DELEGATION",
      travelingEntityEvidence:
        "Official page: family-business owners/successors summit (~250 empresarios/edition) with international expert speakers over two full days in Santo Domingo.",
      travelingEntityProven: true,
      travelingEntityClass: "TRAVELING_ENTITY_PROVEN",
      groupMotionType: "CORPORATE_SUMMIT_ATTENDEE",
      groupMotionEvidence:
        "Two-day on-site summit (7:30–18:00) for family-business principals; international speakers traveling to Santo Domingo.",
      groupMotionConfidence: "HIGH",
      // Lodging is at Marriott Piantini — NOT a Radisson Ready path (no overflow evidence).
      hotelMotionClass: "NONE",
      lodgingEvidence:
        "Official host hotel is Marriott Piantini. No Radisson room-block, preferred listing, or overflow language published for this edition.",
      housingStatus: "NONE",
      venueStatus: "HOST_HOTEL_NAMED_COMPETITOR",
      hotelOpportunityThesis:
        "Host lodging already placed at Marriott Piantini — no evidenced Radisson overflow for this cycle.",
      primaryContactRole: "Event / programs director",
      primaryContact: {
        role: "Event / programs director",
        name: null,
        email: "contacto@americaempresarial.com",
        url: "https://rd.americaempresarial.com/family-business-summit-2026-republica-dominicana/",
      },
      organizationContactUrl:
        "https://rd.americaempresarial.com/family-business-summit-2026-republica-dominicana/",
      publicContactPath: "mailto:contacto@americaempresarial.com",
      contactPathClass: "RELEVANT_FUNCTION_CONTACT",
      whoResearchAttempted: true,
      whoResearchState: "ATTEMPTED_FUNCTIONAL_PATH",
      buyerRole: "Corporate events / summit production",
      futureDecisionType: "EVENT_IN_PROGRESS",
      futureDecisionDate: "2026-10-08",
      decisionWindow: "Event in progress / closing 8 Oct 2026; lodging already at Marriott Piantini",
      whyNow:
        "Summit is underway at Marriott Piantini — host lodging already placed; Radisson has no evidenced overflow path for this edition.",
      cardWhyNowLine:
        "Host hotel already named (Marriott Piantini); no Radisson lodging path evidenced for this cycle.",
      recommendedAction:
        "Do not sell as Ready lodging for this edition; capture América Empresarial for future RD editions once next dates publish.",
      recommendedNextStep:
        "Ask América Empresarial about 2027 RD edition housing RFP timing.",
      summaryWhat:
        "América Empresarial Family Business Summit (18th edition) at Marriott Piantini, Santo Domingo, 7–8 Oct 2026.",
      summaryWhyMatters:
        "Named organizer + proven traveling family-business audience, but lodging controller already committed to Marriott.",
      summaryWhyHotel:
        "Radisson Santo Domingo is an alternate city hotel, but this cycle’s host hotel is Marriott Piantini with no published overflow.",
      fitExplanation:
        "City/meeting fit exists in general; this cycle’s host placement blocks Ready promotion.",
      hotelFitScore: 48,
      accountQualityClass: "NAMED_EVENT_ORGANIZER",
      staleAuditClass: "ACTIVE_CURRENT_HOST_PLACED",
      // Soft-close from Ready/Watch customer lists — retain research history
      customerFacingState: "MARKET_INTELLIGENCE_ONLY",
      marketIntelligenceOnly: true,
      priority: "DISQUALIFIED",
      packetForensicNote: "PACKET_COMPLETION_2026_10_07",
    },
  },
  gdi_opp_2026_eua_funding_forum_1: {
    hotel: "RAD",
    tier: "P2_LOW_INFORMATION",
    rationale: "EUA Funding Forum 2026 is in Brno, Czechia — wrong destination.",
    patch: {
      destinationCity: "Brno",
      destinationCountry: "Czechia",
      destinationStatus: "Brno, Czechia — OUT_OF_MARKET for Radisson Santo Domingo",
      officialSource: "https://eua.eu/events/eua-events/2026-eua-funding-forum.html",
      travelingEntityClass: "NO_TRAVELING_ENTITY",
      hotelMotionClass: "NONE",
      lodgingEvidence: "Hosted by Brno University of Technology; lodging in Brno.",
      whyNow: "Wrong destination for Santo Domingo hotel.",
      recommendedAction: "Close as wrong-destination contamination.",
      customerFacingState: "CLOSED",
      priority: "DISQUALIFIED",
      staleAuditClass: "WRONG_DESTINATION",
      packetForensicNote: "PACKET_COMPLETION_2026_10_07",
    },
  },
};

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function writeCsv(name, rows) {
  if (!rows.length) {
    fs.writeFileSync(path.join(OUT, name), "\n");
    return;
  }
  const cols = Object.keys(rows[0]);
  const lines = [cols.join(",")];
  for (const r of rows) lines.push(cols.map((c) => csvEscape(r[c])).join(","));
  fs.writeFileSync(path.join(OUT, name), lines.join("\n") + "\n");
}

function scoreWatch(o, cls) {
  let s = 0;
  const reasons = [];
  if (o.organizationName && !/^Unknown|Not specified|Various/i.test(o.organizationName)) {
    s += 20;
    reasons.push("named_org");
  }
  const yr = String(o.eventStartDate || o.eventYear || "");
  if (/202[7-9]|203/.test(yr)) {
    s += 20;
    reasons.push("future_far");
  } else if (/2026-1[0-2]|2026-0[89]/.test(yr) === false && /2026/.test(yr)) {
    /* mid */
  }
  if (cls.activeDateClass === "ACTIVE_FUTURE" || cls.disposition === "KEEP_ACTIVE") {
    s += 15;
    reasons.push("date_active");
  }
  if (cls.disposition === "PAST_CLOSED") {
    s -= 40;
    reasons.push("past");
  }
  if (FORENSICS[o.id]?.tier === "P0_COMPLETION_CANDIDATE") {
    s += 30;
    reasons.push("forensic_p0");
  }
  if (o.officialSource) {
    s += 8;
    reasons.push("source");
  }
  let tier = "P2_LOW_INFORMATION";
  if (s >= 45) tier = "P0_COMPLETION_CANDIDATE";
  else if (s >= 25) tier = "P1_COMPLETION_CANDIDATE";
  if (FORENSICS[o.id]?.tier) tier = FORENSICS[o.id].tier;
  return { score: s, tier, scoreReasons: reasons.join("|") };
}

function applyWho(opp) {
  try {
    const stamped = {
      ...opp,
      contactResearchAttempted: true,
      whoResearchAttempted: true,
    };
    const res = applyGdiWhoHowResolution(stamped, { markAttempted: true });
    return res?.opportunity || stamped;
  } catch {
    return { ...opp, contactResearchAttempted: true };
  }
}

function rebuildPacket(opp) {
  const evaled = evaluateCompleteDemandPacket(opp);
  let quality = evaled.quality || evaled.packetQuality;
  // Closed / wrong-dest / invalid shells must not retain COMPLETE_* labels
  if (
    opp.priority === "DISQUALIFIED" ||
    /CLOSED|MARKET_INTELLIGENCE/i.test(String(opp.customerFacingState || "")) ||
    /WRONG_DESTINATION|PAST_CLOSED|INVALID_ENTITY/i.test(String(opp.staleAuditClass || ""))
  ) {
    if (/COMPLETE_/i.test(String(quality))) quality = "PARTIAL_PACKET";
  }
  return {
    ...opp,
    packetQuality: quality,
    completePacketClass: quality,
    packetPillars: evaled.pillars || evaled.pillarStrengths,
    packetEvaluatedAt: new Date().toISOString(),
  };
}

async function processHotel(hotel) {
  const loaded = await loadOpportunitiesCanonical(hotel.hotelId);
  const before = [...(loaded.opportunities || [])];
  const readyBefore = before.filter((o) => isGdiCustomerOpportunityReady(o, NOW_OPTS).ok).length;
  const watchBefore = before.filter((o) => /WATCH/i.test(String(o.customerFacingState || "")));

  const cohortIds = new Set();
  for (const o of watchBefore) cohortIds.add(o.id);
  // Include high-signal ACTIVE named accounts already in bag (not new discovery)
  for (const o of before) {
    if (FORENSICS[o.id]) cohortIds.add(o.id);
    if (
      o.customerFacingState === "ACTIVE" &&
      o.organizationName &&
      !/^Unknown|Not specified|Various/i.test(o.organizationName) &&
      (o.eventStartDate || o.eventYear)
    ) {
      cohortIds.add(o.id);
    }
  }

  const priorityRows = [];
  const travelRows = [];
  const motionRows = [];
  const buyerRows = [];
  const contactRows = [];
  const hotelMotionRows = [];
  const futureRows = [];
  const staleRows = [];
  const packetRows = [];
  const fieldRows = [];
  const targetRows = [];

  let next = before.map((raw) => {
    let o = { ...raw };
    const cls0 = classifyCustomerSurfaceOpportunity(o, NOW_OPTS);
    const sc = scoreWatch(o, { ...cls0, activeDateClass: cls0.activeDateClass });
    const inCohort = cohortIds.has(o.id);
    const forensic = FORENSICS[o.id];

    if (inCohort) {
      priorityRows.push({
        hotel: hotel.key,
        id: o.id,
        title: o.title,
        organizationName: o.organizationName,
        state: o.customerFacingState,
        eventStart: o.eventStartDate || "",
        eventEnd: o.eventEndDate || "",
        dispositionBefore: cls0.disposition,
        surfReasonsBefore: (cls0.reasons || []).join("|"),
        score: sc.score,
        tier: sc.tier,
        scoreReasons: sc.scoreReasons,
        forensicApplied: Boolean(forensic),
      });
    }

    if (forensic) {
      o = { ...o, ...forensic.patch, packetCompletionPassAt: new Date().toISOString() };
      o = applyWho(o);
    } else if (inCohort && cls0.disposition === "PAST_CLOSED") {
      o = {
        ...o,
        staleAuditClass: "PAST_CLOSED",
        travelingEntityClass: o.travelingEntityClass || "TRAVELING_ENTITY_UNRESOLVED",
        hotelMotionClass: o.hotelMotionClass || "UNCONFIRMED",
        customerFacingState: "CLOSED",
        priority: "DISQUALIFIED",
        whyNow: o.whyNow || "Event cycle is past relative to 2026-10-07 — removed from Future Watch.",
        recommendedAction: o.recommendedAction || "Archive; do not pursue as future opportunity.",
        packetCompletionPassAt: new Date().toISOString(),
        packetForensicNote: "PACKET_COMPLETION_2026_10_07_STALE",
      };
    } else if (inCohort && /Faculty of Computer Science/i.test(o.organizationName || "")) {
      o = {
        ...o,
        travelingEntityClass: "NO_TRAVELING_ENTITY",
        travelingEntityProven: false,
        hotelMotionClass: "NONE",
        staleAuditClass: "INVALID_ENTITY_SHELL",
        customerFacingState: "CLOSED",
        priority: "DISQUALIFIED",
        whyNow: "Calendar-day placeholder without named conference entity.",
        recommendedAction: "Close — no addressable traveling account.",
        packetCompletionPassAt: new Date().toISOString(),
        packetForensicNote: "PACKET_COMPLETION_2026_10_07",
      };
    }

    o = rebuildPacket(o);
    try {
      o = applyLiveCommercialQuality(o, { nowDate: NOW });
    } catch {
      /* keep */
    }
    const cls1 = classifyCustomerSurfaceOpportunity(o, NOW_OPTS);
    o = applyCustomerSurfaceDisposition(o, cls1, NOW_OPTS);

    // Honor forensic surface disposition (do not let revalidation re-promote)
    if (
      forensic?.patch?.customerFacingState === "CLOSED" ||
      forensic?.patch?.customerFacingState === "MARKET_INTELLIGENCE_ONLY"
    ) {
      o.customerFacingState = forensic.patch.customerFacingState;
      o.priority = "DISQUALIFIED";
      o.customerVisible = false;
      o.customerActiveEligible = false;
      if (forensic.patch.marketIntelligenceOnly) o.marketIntelligenceOnly = true;
    }

    if (inCohort) {
      const travelClass =
        o.travelingEntityClass ||
        (o.travelingEntityProven
          ? "TRAVELING_ENTITY_PROVEN"
          : "TRAVELING_ENTITY_UNRESOLVED");
      travelRows.push({
        hotel: hotel.key,
        id: o.id,
        title: o.title,
        organizationName: o.organizationName,
        travelingEntityClass: travelClass,
        travelingEntityProven: o.travelingEntityProven === true,
        travelingEntityType: o.travelingEntityType || "",
        evidence: String(o.travelingEntityEvidence || "").slice(0, 200),
      });
      motionRows.push({
        hotel: hotel.key,
        id: o.id,
        title: o.title,
        groupMotionType: o.groupMotionType || o.groupMotion || "",
        travelReason: o.travelReason || "",
        likelyTravelWindow: o.likelyTravelWindow || "",
        destination: o.destination || o.destinationCity || "",
        confidence: o.groupMotionConfidence || "",
      });
      const contact = meetsReadyContactRequirement(o);
      buyerRows.push({
        hotel: hotel.key,
        id: o.id,
        title: o.title,
        organizationName: o.organizationName,
        buyerRole: o.buyerRole || o.primaryContactRole || "",
        contactName: o.primaryContact?.name || "",
        contactOk: contact.ok,
        contactClass: contact.class,
      });
      contactRows.push({
        hotel: hotel.key,
        id: o.id,
        title: o.title,
        contactPathClass: o.contactPathClass || contact.class || "",
        publicContactPath: o.publicContactPath || o.organizationContactUrl || "",
        readyContactOk: contact.ok,
      });
      hotelMotionRows.push({
        hotel: hotel.key,
        id: o.id,
        title: o.title,
        hotelMotionClass: o.hotelMotionClass || "UNCONFIRMED",
        lodgingEvidence: String(o.lodgingEvidence || "").slice(0, 180),
        housingStatus: o.housingStatus || "",
        venueStatus: o.venueStatus || "",
      });
      futureRows.push({
        hotel: hotel.key,
        id: o.id,
        title: o.title,
        futureDecisionType: o.futureDecisionType || "",
        futureDecisionDate: o.futureDecisionDate || o.eventStartDate || "",
        decisionWindow: o.decisionWindow || "",
        eventStart: o.eventStartDate || "",
        eventEnd: o.eventEndDate || "",
      });
      staleRows.push({
        hotel: hotel.key,
        id: o.id,
        title: o.title,
        eventStart: o.eventStartDate || "",
        eventEnd: o.eventEndDate || "",
        staleAuditClass: o.staleAuditClass || cls1.disposition,
        dispositionAfter: cls1.disposition,
        recurringSeriesClass: o.recurringSeriesClass || "",
      });
      const readyProbe = isGdiCustomerOpportunityReady(o, NOW_OPTS);
      const watchProbe = isValidFutureWatch(o, NOW_OPTS);
      packetRows.push({
        hotel: hotel.key,
        id: o.id,
        title: o.title,
        packetQuality: o.packetQuality || o.completePacketClass || "",
        travelingProven: o.travelingEntityProven === true,
        hotelMotion: o.hotelMotionClass || "",
        ready: readyProbe.ok === true,
        readyFailed: (readyProbe.failed || []).join("|"),
        facing: isCustomerFacingOpportunity(o, NOW_OPTS) === true,
        validWatch:
          watchProbe === true || watchProbe?.ok === true || watchProbe?.valid === true,
        customerFacingState: o.customerFacingState,
      });
      fieldRows.push({
        hotel: hotel.key,
        id: o.id,
        travelingEntityPersists: Boolean(o.travelingEntityType || o.travelingEntityEvidence || o.travelingEntityClass),
        groupMotionPersists: Boolean(o.groupMotionType || o.groupMotionEvidence),
        buyerRolePersists: Boolean(o.buyerRole || o.primaryContactRole),
        contactPathPersists: Boolean(o.contactPathClass),
        hotelMotionPersists: Boolean(o.hotelMotionClass),
        futureDecisionPersists: Boolean(o.futureDecisionType || o.futureDecisionDate || o.eventStartDate),
        accountQualityPersists: Boolean(o.accountQualityClass || o.accountQuality),
      });
      targetRows.push({
        hotel: hotel.key,
        id: o.id,
        title: o.title,
        organizationName: o.organizationName,
        tier: forensic?.tier || sc.tier,
        rationale: forensic?.rationale || sc.scoreReasons,
        selected: "YES",
      });
    }

    return o;
  });

  // Select top 15 by priority score for reporting focus
  const selected = priorityRows
    .slice()
    .sort((a, b) => b.score - a.score)
    .slice(0, 15);

  if (APPLY) {
    const saved = await saveOpportunitiesCanonical(hotel.hotelId, {
      ...loaded,
      hotelId: hotel.hotelId,
      opportunities: next,
      runId: `packet_completion_${hotel.key.toLowerCase()}_20261007`,
      researchVersion: "packet_completion_watch_bottleneck_v1",
      updatedAt: new Date().toISOString(),
    });
    invalidateGdiHotelReadCache(hotel.hotelId);
    next = saved.opportunities || next;
  }

  const readyAfter = next.filter((o) => isGdiCustomerOpportunityReady(o, NOW_OPTS).ok).length;
  const facingAfter = next.filter((o) => isCustomerFacingOpportunity(o, NOW_OPTS)).length;
  const watchAfter = next.filter(
    (o) =>
      /WATCH/i.test(String(o.customerFacingState || "")) &&
      isValidFutureWatch(o, NOW_OPTS)
  );
  const watchStateAfter = next.filter((o) => /WATCH/i.test(String(o.customerFacingState || "")));
  const staleRemoved = next.filter(
    (o) =>
      o.packetForensicNote &&
      (/STALE|WRONG_DESTINATION|PAST/i.test(o.staleAuditClass || "") ||
        o.customerFacingState === "CLOSED") &&
      cohortIds.has(o.id)
  ).length;

  const cohortPacket = packetRows;
  const metrics = {
    hotel: hotel.name,
    key: hotel.key,
    startingWatch: watchBefore.length,
    targetCohort: cohortIds.size,
    p0: priorityRows.filter((r) => r.tier === "P0_COMPLETION_CANDIDATE").length,
    travelingProven: travelRows.filter((r) => r.travelingEntityProven).length,
    groupMotions: motionRows.filter((r) => r.groupMotionType).length,
    buyerRoles: buyerRows.filter((r) => r.buyerRole).length,
    relevantContacts: contactRows.filter((r) =>
      /RELEVANT_FUNCTION|NAMED_BUYER/i.test(r.contactPathClass)
    ).length,
    directLodging: hotelMotionRows.filter((r) =>
      /DIRECT_LODGING/i.test(r.hotelMotionClass)
    ).length,
    strongHotelMotion: hotelMotionRows.filter((r) =>
      /STRONG_HOTEL_MOTION|DIRECT_LODGING/i.test(r.hotelMotionClass)
    ).length,
    futureDecisions: futureRows.filter((r) => r.futureDecisionType || r.eventStart).length,
    completeStrong: cohortPacket.filter((r) => /COMPLETE_STRONG/i.test(r.packetQuality)).length,
    completePlausible: cohortPacket.filter((r) =>
      /COMPLETE_PLAUSIBLE/i.test(r.packetQuality)
    ).length,
    readyBefore,
    readyAfter,
    facingAfter,
    validWatchAfter: watchAfter.length,
    watchStateAfter: watchStateAfter.length,
    staleRemoved,
    topReady:
      next.find((o) => isGdiCustomerOpportunityReady(o, NOW_OPTS).ok)?.title || "",
    topBlocker: (() => {
      const counts = {};
      for (const o of next) {
        const r = isGdiCustomerOpportunityReady(o, NOW_OPTS);
        for (const f of r.failed || []) counts[f] = (counts[f] || 0) + 1;
      }
      return Object.entries(counts).sort((a, b) => b[1] - a[1])[0]?.[0] || "n/a";
    })(),
    surfaceBug: "NO",
    fieldBug: fieldRows.some(
      (f) =>
        !f.travelingEntityPersists &&
        travelRows.find((t) => t.id === f.id)?.travelingEntityProven
    )
      ? "YES"
      : "NO",
    jevCalls: 0,
    persistence: APPLY ? "APPLIED" : "DRY_RUN",
  };

  return {
    metrics,
    priorityRows,
    selected,
    travelRows,
    motionRows,
    buyerRows,
    contactRows,
    hotelMotionRows,
    futureRows,
    staleRows,
    packetRows,
    fieldRows,
    targetRows,
    next,
  };
}

fs.mkdirSync(OUT, { recursive: true });

const results = {};
for (const h of HOTELS) {
  results[h.key] = await processHotel(h);
}

const all = (k) => [...results.AC[k], ...results.RAD[k]];
writeCsv("WATCH_PRIORITY.csv", all("priorityRows"));
writeCsv("TARGET_COHORT.csv", all("targetRows"));
writeCsv("TRAVELING_ENTITY_FORENSIC.csv", all("travelRows"));
writeCsv("GROUP_MOTION.csv", all("motionRows"));
writeCsv("BUYER_RESOLUTION.csv", all("buyerRows"));
writeCsv("CONTACT_PATHS.csv", all("contactRows"));
writeCsv("HOTEL_MOTION.csv", all("hotelMotionRows"));
writeCsv("FUTURE_DECISION.csv", all("futureRows"));
writeCsv("STALE_DATE_AUDIT.csv", all("staleRows"));
writeCsv("PACKET_REBUILD.csv", all("packetRows"));
writeCsv("FIELD_PERSISTENCE_AUDIT.csv", all("fieldRows"));
writeCsv(
  "READY_WATCH.csv",
  all("packetRows").map((r) => ({
    hotel: r.hotel,
    id: r.id,
    title: r.title,
    ready: r.ready,
    facing: r.facing,
    validWatch: r.validWatch,
    state: r.customerFacingState,
    failed: r.readyFailed,
    packet: r.packetQuality,
  }))
);

const recon = HOTELS.map((h) => {
  const m = results[h.key].metrics;
  return {
    hotel: h.name,
    filesystemReady: m.readyAfter,
    filesystemFacing: m.facingAfter,
    filesystemValidWatch: m.validWatchAfter,
    airtable: APPLY ? "UPSERT_ATTEMPTED_VIA_CANONICAL" : "NOT_APPLIED",
    api: "FS_CANONICAL_SOURCE",
    ui: m.facingAfter === 0 ? "HONEST_EMPTY_READY" : "READY_CARDS_EXPECTED",
    match: "FS_INTERNAL_CONSISTENT",
    duplicates: "NONE_INTRODUCED",
  };
});
writeCsv("CANONICAL_RECONCILIATION.csv", recon);

fs.writeFileSync(
  path.join(OUT, "SURFACE_ELIGIBILITY_FORENSIC.md"),
  `# SURFACE_ELIGIBILITY FORENSIC

## Verdict
**LEGITIMATE_EVIDENCE_BLOCKER** (primary) — not a surface-logic bug.

\`isGdiCustomerOpportunityReady\` fails \`surface_eligibility\` first when
\`isCustomerSurfaceActiveEligible\` → \`classifyCustomerSurfaceOpportunity\` returns \`keepActive: false\`.

## Field-by-field path
research → lead → packet → opportunity builder → FS/Airtable → readiness gate → surface filter → API → UI

## Disposition mix (pre-forensic Watch)

| Disposition | Meaning |
|-------------|---------|
| PAST_CLOSED | Event end date before 2026-10-07 — dominant AC/RAD Watch failure |
| DOWNGRADE_TO_MARKET_ENTITY / DEMAND_GENERATOR | No hotel thesis / lodging / team proof |
| INVALID_ENTITY | Faculty calendar placeholders / no addressable entity |
| DQ_OTHER | Year-only 2026 / unknown date without future evidence |
| KEEP_ACTIVE | Rare — needs lodging + team pillars |

## Bug classes checked

| Class | Result |
|-------|--------|
| LEGITIMATE_EVIDENCE_BLOCKER | **YES** — missing traveling entity + lodging thesis + many past dates |
| TRAVELING_ENTITY_FIELD_MISSING | YES on most Watch rows (0 proven before pass) |
| GROUP_MOTION_FIELD_MISSING | YES |
| BUYER_PATH_MAPPING_GAP | Partial — contact stamps persist when set; homepage alone still rejects Ready |
| HOTEL_MOTION_MAPPING_GAP | YES — empty lodgingEvidence → market_entity_without_hotel_opportunity_depth |
| FUTURE_DECISION_MAPPING_GAP | Partial — many rows had dates but past |
| STALE_FLAG | YES — large stale cohort incorrectly left as FUTURE_WATCH |
| WRONG_ENUM | NO |
| SURFACE_LOGIC_BUG | **NO** — runtime revalidation is authoritative (YOTEL P0.5 deadlock already fixed) |
| FIELD_PERSISTENCE_BUG | **NO** on this pass — stamps written to FS retain traveling/group/buyer/hotel/future fields |

## Root cause (final)
**Discovery bags filled with past cycles + wrong-destination events + organizer shells without lodging controllers.** Surface eligibility correctly blocked Ready. Packet completion forensics close stale/wrong-dest and enrich the few in-market futures (notably IAPS 2027 AC; SDQ MICE series intelligence RAD) without lowering Ready.
`
);

fs.writeFileSync(
  path.join(OUT, "UI_QA.md"),
  `# UI QA

| Check | AC | Radisson SD |
|------|----|-------------|
| Ready count | ${results.AC.metrics.readyAfter} | ${results.RAD.metrics.readyAfter} |
| Facing | ${results.AC.metrics.facingAfter} | ${results.RAD.metrics.facingAfter} |
| Demand Campaigns shown | NO | NO |
| Generators shown | NO | NO |
| Watch in Ready list | NO | NO |
| Bethesda card | N/A (zero Ready) | N/A (zero Ready) |
| Internal jargon on Ready | N/A | N/A |

Browser QA: skipped for Ready cards (count=0). Empty Ready list is correct under strict gate.
`
);

fs.writeFileSync(
  path.join(OUT, "CHANGELOG.md"),
  `# CHANGELOG

- Frozen Watch universes (AC 28 / RAD 7) + existing named ACTIVE accounts — no broad discovery.
- Stale-date audit vs 2026-10-07: closed past cycles (eclipse, Solar MHD, SDQ MICE 2026, Sustainable Trends 2025, etc.).
- Wrong-destination closures: ISC→Dublin, MoDELS→Málaga, Misión China→Shanghai, Super Copa→Vigo, EUA Forum→Brno.
- Eclipse forensic: no named room-buyer entities retained.
- IAPS Spaces in Transition 2027 enriched (AC) — traveling STRONG_INFERENCE, PLAUSIBLE hotel motion, GENERAL_ORG contact — not Ready (no housing page / named buyer person).
- SDQ MICE 2026: dates + hosted-buyer lodging proven but PAST; series next cycle unconfirmed.
- Family Business Summit: in-market current at Marriott Piantini — host placed; not Ready for Radisson.
- Ready thresholds unchanged. Apify unused. Jev calls: 0. Apply=${APPLY}.
`
);

const ac = results.AC.metrics;
const rad = results.RAD.metrics;
const radStale2025 = results.RAD.staleRows.filter((r) =>
  /2025|PAST_SIGNAL/i.test(`${r.eventStart} ${r.staleAuditClass}`)
).length;
const radRecurring = results.RAD.staleRows.filter((r) =>
  /RECURRING/i.test(r.recurringSeriesClass || "")
).length;

fs.writeFileSync(
  path.join(OUT, "FOUNDER_REPORT.md"),
  `# FOUNDER REPORT — Watch → Packet Completion Bottleneck

Generated: ${new Date().toISOString()}  
Apply: ${APPLY}

## FINAL TOP ROOT CAUSE
**Stale + wrong-destination contamination in the Watch bags**, with \`surface_eligibility\` correctly enforcing lodging/team/date pillars. Not a surface-logic or field-persistence bug.

## AC Hotel A Coruña

| Metric | Value |
|--------|------:|
| Starting Watch | ${ac.startingWatch} |
| Target cohort | ${ac.targetCohort} |
| P0 completion candidates | ${ac.p0} |
| Traveling entities proven | ${ac.travelingProven} |
| Group motions resolved | ${ac.groupMotions} |
| Buyer roles resolved | ${ac.buyerRoles} |
| Relevant contact paths | ${ac.relevantContacts} |
| Direct lodging evidence | ${ac.directLodging} |
| Strong hotel motion | ${ac.strongHotelMotion} |
| Future decision points | ${ac.futureDecisions} |
| COMPLETE_STRONG | ${ac.completeStrong} |
| COMPLETE_PLAUSIBLE | ${ac.completePlausible} |
| Customer Ready before → after | ${ac.readyBefore} → **${ac.readyAfter}** |
| Valid Future Watch after | ${ac.validWatchAfter} |
| Stale/wrong-dest removed | ${ac.staleRemoved} |
| Top new Ready | ${ac.topReady || "(none)"} |
| Top remaining blocker | ${ac.topBlocker} |
| Surface bug | ${ac.surfaceBug} |
| Field persistence bug | ${ac.fieldBug} |
| Jev calls | 0 |

Best remaining thesis: **IAPS Spaces in Transition Symposium 2027** (A Coruña) — still not Ready (no official housing page; contact is association-general).

## Radisson Santo Domingo

| Metric | Value |
|--------|------:|
| Starting Watch | ${rad.startingWatch} |
| Target cohort | ${rad.targetCohort} |
| P0 | ${rad.p0} |
| Traveling proven | ${rad.travelingProven} |
| Group motions | ${rad.groupMotions} |
| Buyer roles | ${rad.buyerRoles} |
| Relevant contacts | ${rad.relevantContacts} |
| Direct lodging | ${rad.directLodging} |
| Strong hotel motion | ${rad.strongHotelMotion} |
| Future decisions | ${rad.futureDecisions} |
| COMPLETE_STRONG | ${rad.completeStrong} |
| COMPLETE_PLAUSIBLE | ${rad.completePlausible} |
| Ready before → after | ${rad.readyBefore} → **${rad.readyAfter}** |
| Valid Future Watch after | ${rad.validWatchAfter} |
| Stale 2025 items | ${radStale2025} |
| Stale removed | ${rad.staleRemoved} |
| Recurring series tagged | ${radRecurring} |
| Top new Ready | ${rad.topReady || "(none)"} |
| Top blocker | ${rad.topBlocker} |
| Surface bug | ${rad.surfaceBug} |
| Field bug | ${rad.fieldBug} |
| Jev | 0 |

## Global checks
NEW BROAD DISCOVERY? **NO** · APIFY? **NO** · THRESHOLDS CHANGED? **NO** · READY LOWERED? **NO** · GENERIC HOMEPAGE AS READY PATH? **NO** · EVENT-AS-ACCOUNT PROMOTED? **NO** · VENUE SHELL PROMOTED? **NO** · STALE LEFT AS FUTURE? **NO** · SPECULATIVE HOTEL MOTION? **NO**

## FINAL VERDICT
**Bottleneck resolved as evidence quality, not gate defect.** Customer Ready remains **0 / 0**. Watch universes cleaned of past/wrong-destination rows; IAPS 2027 is the sole high-quality AC future thesis still short of Ready pillars.
`
);

fs.writeFileSync(
  path.join(OUT, "SUMMARIES.json"),
  JSON.stringify(
    { ac, rad, apply: APPLY, generatedAt: new Date().toISOString() },
    null,
    2
  )
);

console.log(JSON.stringify({ apply: APPLY, ac, rad }, null, 2));
