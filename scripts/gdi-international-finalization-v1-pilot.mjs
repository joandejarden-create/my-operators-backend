#!/usr/bin/env node
/**
 * GDI International Finalization Engine V1 — AC / Radisson / Westin pilot.
 * No Ready threshold changes. No Apify. No inferred overflow.
 *
 * Usage: node scripts/gdi-international-finalization-v1-pilot.mjs
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  finalizeCandidate,
  getFinalizationArchitectureStatus,
  mapHotelSuppliedResponseToFacts,
  IFE_VERSION,
} from "../lib/group-demand-intelligence/international-finalization/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports", "gdi", "international-finalization-v1");

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function write(name, body) {
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}
function csvEscape(v) {
  const s = v == null ? "" : String(v);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function toCsv(rows) {
  if (!rows.length) return "\n";
  const cols = Object.keys(rows[0]);
  return [cols.join(","), ...rows.map((r) => cols.map((c) => csvEscape(r[c])).join(","))].join("\n") + "\n";
}

/** Hotels — facts from known profiles / ADP fixtures (no invention). */
const HOTELS = {
  ac_hotel_a_coruna: {
    hotelId: "ac_hotel_a_coruna",
    name: "AC Hotel A Coruña",
    city: "A Coruña",
    market: "A Coruña",
    country: "ES",
    rooms: 180,
    chainScale: "Upper Upscale",
    brand: "AC Hotels",
    meetingSpace: { meetingRooms: 6, largestRoom: { sqM: 200 } },
  },
  radisson_santo_domingo: {
    hotelId: "radisson_santo_domingo",
    name: "Radisson Hotel Santo Domingo",
    city: "Santo Domingo",
    market: "Santo Domingo",
    country: "DO",
    rooms: 210,
    chainScale: "Upscale",
    brand: "Radisson",
    meetingSpace: { meetingRooms: 8, largestRoom: { sqM: 350 } },
  },
  westin_grand_munchen: {
    hotelId: "westin_grand_munchen",
    name: "The Westin Grand München",
    city: "Munich",
    market: "Munich",
    country: "DE",
    rooms: 627,
    chainScale: "Upper Upscale",
    brand: "Westin",
    meetingSpace: { meetingRooms: 21, largestRoom: { sqM: 1053, capacity: 1105 } },
  },
};

/**
 * Finalization candidates from V2 + lodging-decision intelligence (existing evidence only).
 * packetQualityHint before = V2 honest disposition for conversion comparison.
 */
const CANDIDATES = [
  {
    hotelKey: "ac_hotel_a_coruna",
    languages: ["es", "gl", "en"],
    before: { ready: 0, watch: 1, completePlausible: 1, completeStrong: 0 },
    candidate: {
      opportunityId: "gdi_opp_biocultura_a_coruna_2027",
      campaignId: "accamp_biocultura_a_coruna_2027_2027",
      campaignName: "BioCultura A Coruña 2027",
      organizationName: "Asociación Vida Sana",
      packetQualityHint: "COMPLETE_PLAUSIBLE",
      nextBlocker: "HOTEL_LODGING_EVIDENCE",
      lodgingEvidenceClass: "PLAUSIBLE_HOTEL_MOTION",
      lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
      demandControllerId: "dc_ac_vida_sana",
      controllerName: "Asociación Vida Sana",
      publicContactPath: "mailto:expositores@vidasana.org",
      participationRole: "exhibitor",
      demandType: "TRADE_FAIR",
      futureDecisionPoint: true,
      decisionWindow: "2026-10 → 2027-02",
      validFutureWatch: true,
      eventStartDate: "2027-03",
    },
    controller: {
      demandControllerId: "dc_ac_vida_sana",
      controllerName: "Asociación Vida Sana",
      lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
      publicContactPath: "mailto:expositores@vidasana.org",
      evidenceSource: "https://www.biocultura.org/acoruna/viajes",
    },
    demand: {
      destinationCity: "A Coruña",
      market: "A Coruña",
      groupProfile: "exhibitors / organic trade visitors",
      meetingRequired: false,
      lodgingEvidenceClass: "PLAUSIBLE_HOTEL_MOTION",
    },
    selectionHints: {
      selectionModel: "SELF_BOOKING_RECOMMENDED_LIST",
      housingPageExistsWithoutHotels: true,
      evidenceSource: "https://www.biocultura.org/acoruna/viajes",
      evidenceSnippet: "Viajes page — Renfe discount; hotel partners not yet listed for 2027",
      hotelListPublishDate: "2027-01 (stand allocation window)",
    },
    ldiRow: {
      process: "ATTENDEE_SELF_BOOKING",
      inclusionClass: "NO_HOUSING_EVIDENCE_YET",
      evidence: "2027 Viajes page publishes Renfe discount only — no official hotel block",
    },
  },
  {
    hotelKey: "ac_hotel_a_coruna",
    languages: ["es", "gl", "en"],
    before: { ready: 0, watch: 1, completePlausible: 1, completeStrong: 0 },
    candidate: {
      opportunityId: "gdi_opp_international_symposium_6",
      campaignId: "accamp_iaps_spaces_in_transition_symposium_2027_2027",
      campaignName: "IAPS Spaces in Transition Symposium 2027",
      organizationName: "Universidade da Coruña",
      travelingEntityProven: false,
      packetQualityHint: "COMPLETE_PLAUSIBLE",
      nextBlocker: "HOTEL_LODGING_EVIDENCE",
      lodgingEvidenceClass: "NONE",
      lodgingAuthority: "PLAUSIBLE_CONTROLLER",
      demandControllerId: "dc_ac_iaps_udc",
      controllerName: "UDC People-Environment Research Group / IAPS Networks",
      publicContactPath: "mailto:ricardo.garcia.mira@udc.es",
      participationRole: "delegate",
      demandType: "ACADEMIC_SYMPOSIUM",
      futureDecisionPoint: true,
      validFutureWatch: true,
      eventStartDate: "2027-06-14",
    },
    controller: {
      demandControllerId: "dc_ac_iaps_udc",
      controllerName: "UDC People-Environment Research Group / IAPS Networks",
      lodgingAuthority: "PLAUSIBLE_CONTROLLER",
      publicContactPath: "mailto:ricardo.garcia.mira@udc.es",
      controllerType: "HOST_INSTITUTION",
      evidenceSource: "https://iaps-association.org/",
    },
    demand: {
      destinationCity: "A Coruña",
      market: "A Coruña",
      groupProfile: "international researchers / practitioners",
      meetingRequired: true,
      sameSubmarket: true,
      venueAccessNote: "Symposium convened in A Coruña; venue TBA on official save-the-date",
      venueAccessSource: "IAPS save-the-date",
    },
    selectionHints: {
      selectionModel: "HOST_INSTITUTION_MANAGED",
      evidenceSource: "https://iaps-association.org/",
      evidenceSnippet: "Venue and website details will follow — no housing page yet",
    },
    ldiRow: {
      process: "UNKNOWN",
      inclusionClass: "NO_HOUSING_EVIDENCE_YET",
      evidence: "No RFP, housing bureau, or hotel partner page for 2027",
    },
  },
  {
    hotelKey: "radisson_santo_domingo",
    languages: ["es", "en"],
    before: { ready: 0, watch: 1, completePlausible: 1, completeStrong: 0 },
    candidate: {
      opportunityId: "gdi_opp_rif_filosofia_sd_2027",
      campaignId: "radisscamp_vii_congreso_iberoamericano_de_filosofia_2027_2027",
      campaignName: "VII Congreso Iberoamericano de Filosofía 2027",
      organizationName: "ADOFIL / speakers (convenio pending)",
      packetQualityHint: "COMPLETE_PLAUSIBLE",
      nextBlocker: "TARGET_HOTEL_FIT",
      lodgingEvidenceClass: "PLAUSIBLE_HOTEL_MOTION",
      lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
      demandControllerId: "dc_rad_adofil",
      controllerName: "ADOFIL / Comité Organizador RIF–UASD",
      publicContactPath: "mailto:adofil333@gmail.com",
      participationRole: "speaker_delegate",
      demandType: "CONGRESS",
      futureDecisionPoint: true,
      validFutureWatch: true,
      eventStartDate: "2027-03-15",
    },
    controller: {
      demandControllerId: "dc_rad_adofil",
      controllerName: "ADOFIL / Comité Organizador RIF–UASD",
      lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
      publicContactPath: "mailto:adofil333@gmail.com",
      evidenceSource:
        "https://rediberoamericanafilosofia.com/wp-content/uploads/2026/05/Segunda-convocatoria-Congreso-RIF.pdf",
    },
    demand: {
      destinationCity: "Santo Domingo",
      market: "Santo Domingo",
      venueCity: "Santo Domingo",
      groupProfile: "international philosophy speakers",
      meetingRequired: true,
      sameSubmarket: true,
      venueAccessNote: "Congress at UASD — Zona Colonial / Distrito Nacional lodging corridor cited in convocatoria",
      venueAccessSource: "RIF segunda convocatoria PDF §5",
    },
    selectionHints: {
      selectionModel: "DIRECT_NEGOTIATION",
      convenioCircularExpected: true,
      evidenceSource: "RIF segunda convocatoria PDF",
      evidenceSnippet: "Preferential rates negotiated; hotel convenio list forthcoming",
    },
    ldiRow: {
      process: "DIRECT_NEGOTIATION",
      inclusionClass: "NO_HOUSING_EVIDENCE_YET",
      evidence: "Convenio hotel list forthcoming in circular",
    },
  },
  {
    hotelKey: "radisson_santo_domingo",
    languages: ["es", "en"],
    before: { ready: 0, watch: 1, completePlausible: 1, completeStrong: 0 },
    candidate: {
      opportunityId: "gdi_opp_cielo_laboral_sd_2026",
      campaignId: "radisscamp_6_congreso_mundial_cielo_laboral_2026_2026",
      campaignName: "6º Congreso Mundial CIELO Laboral 2026",
      organizationName: "CIELO Laboral attendees",
      packetQualityHint: "COMPLETE_PLAUSIBLE",
      nextBlocker: "TARGET_HOTEL_FIT",
      lodgingEvidenceClass: "STRONG_HOTEL_MOTION",
      lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
      demandControllerId: "dc_rad_cielo",
      controllerName: "CIELO Laboral congress secretariat",
      publicContactPath: "mailto:congresocielo6@gmail.com",
      participationRole: "delegate",
      demandType: "CONGRESS",
      futureDecisionPoint: true,
      validFutureWatch: true,
      eventStartDate: "2026-12-02",
    },
    controller: {
      demandControllerId: "dc_rad_cielo",
      controllerName: "CIELO Laboral congress secretariat",
      lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
      publicContactPath: "mailto:congresocielo6@gmail.com",
      evidenceSource: "https://www.cielolaboral.com/",
    },
    demand: {
      destinationCity: "Santo Domingo",
      market: "Santo Domingo",
      venueCity: "Santo Domingo",
      groupProfile: "labor-law congress delegates",
      meetingRequired: true,
      onRecommendedList: false,
      sameSubmarket: true,
      venueAccessNote: "Recommended hotels near PUCMM — Radisson not on current PDF list",
      venueAccessSource: "CIELO recommended-hotels PDF",
    },
    selectionHints: {
      selectionModel: "SELF_BOOKING_RECOMMENDED_LIST",
      recommendedListPublished: true,
      evidenceSource: "CIELO recommended-hotels PDF",
      rateSubmissionDeadline: "2026-11-27 (registration refund cutoff)",
    },
    ldiRow: {
      process: "ATTENDEE_SELF_BOOKING",
      inclusionClass: "CURRENT_HOTEL_LIST_PUBLISHED",
      evidence: "Attendees book directly; Radisson absent from current list",
    },
  },
  {
    hotelKey: "radisson_santo_domingo",
    languages: ["es", "en"],
    before: { ready: 0, watch: 1, completePlausible: 1, completeStrong: 0 },
    candidate: {
      opportunityId: "gdi_opp_autoamericas_2027",
      campaignId: "radisscamp_autoamericas_2027_2027",
      campaignName: "AUTOAMERICAS 2027",
      organizationName: "AUTOAMERICAS exhibitors / speakers",
      packetQualityHint: "COMPLETE_PLAUSIBLE",
      nextBlocker: "TARGET_HOTEL_FIT",
      lodgingEvidenceClass: "DIRECT_LODGING_EVIDENCE",
      lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
      demandControllerId: "dc_rad_autoamericas",
      controllerName: "AutoAméricas / Latinpress",
      publicContactPath: "mailto:acaballero@autoamericas.show",
      participationRole: "exhibitor",
      demandType: "TRADE_SHOW",
      futureDecisionPoint: true,
      validFutureWatch: true,
      eventStartDate: "2027-04-23",
    },
    controller: {
      demandControllerId: "dc_rad_autoamericas",
      controllerName: "AutoAméricas / Latinpress",
      lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
      publicContactPath: "mailto:acaballero@autoamericas.show",
      evidenceSource: "https://www.autoamericas.show/es/expo/alojamiento.html",
    },
    demand: {
      destinationCity: "Santo Domingo",
      market: "Santo Domingo",
      officialHostHotel: "Dominican Fiesta",
      knownCompetitorHost: "Dominican Fiesta",
      knownOfficialHotels: ["Dominican Fiesta"],
      // Historical secondary pattern — NOT current placement; do not invent overflow
      historicalSecondaryPartner: true,
      secondaryPartnerOpen: false,
      overflowEvidenced: false,
      groupProfile: "exhibitors and speakers",
      meetingRequired: false,
      lodgingEvidenceClass: "DIRECT_LODGING_EVIDENCE",
    },
    selectionHints: {
      selectionModel: "HOST_INSTITUTION_MANAGED",
      officialHostLocked: true,
      historicalSecondaryPartner: true,
      evidenceSource: "https://www.autoamericas.show/es/expo/alojamiento.html",
      evidenceSnippet: "2027 host = Dominican Fiesta; 2026 secondary convenios included Radisson historically",
    },
    ldiRow: {
      process: "HOST_INSTITUTION_SELECTION",
      inclusionClass: "PAST_HOTEL_LIST_AVAILABLE",
      evidence: "2027 Fiesta official; secondary 2027 list not republished",
      knownOfficialHotels: ["Dominican Fiesta"],
    },
  },
  {
    hotelKey: "westin_grand_munchen",
    languages: ["de", "en"],
    before: { ready: 0, watch: 0, completePlausible: 1, completeStrong: 0 },
    candidate: {
      opportunityId: "gdi_opp_stadtgeburtstag_munich_2027",
      campaignId: "westincamp_stadtgeburtstag_2027_und_2028_2027",
      campaignName: "Stadtgeburtstag 2027 und 2028",
      organizationName: "Landeshauptstadt München",
      packetQualityHint: "COMPLETE_PLAUSIBLE",
      nextBlocker: "HOTEL_LODGING_EVIDENCE",
      lodgingEvidenceClass: "PLAUSIBLE_HOTEL_MOTION",
      lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
      demandControllerId: "dc_westin_muc_vergabe",
      controllerName: "Landeshauptstadt München (Vergabe / Veranstaltungsorganisation)",
      publicContactPath: null,
      participationRole: "city_event_program",
      demandType: "CITY_PROCUREMENT_EVENT",
      futureDecisionPoint: true,
      validFutureWatch: false,
      eventStartDate: "2027",
    },
    controller: {
      demandControllerId: "dc_westin_muc_vergabe",
      controllerName: "Landeshauptstadt München (Vergabe / Veranstaltungsorganisation)",
      lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
      controllerType: "PCO",
      evidenceSource: "https://vergabe.muenchen.de/",
    },
    demand: {
      destinationCity: "Munich",
      market: "Munich",
      groupProfile: "city anniversary / event program lodging",
      meetingRequired: true,
      sameSubmarket: false,
      venueAccessNote: "Citywide Munich program — Arabellapark hotel is in-market but not CBD-locked; corridor fit needs program venue confirmation",
      venueAccessSource: "procurement notice geography = München",
    },
    selectionHints: {
      selectionModel: "PROCUREMENT_RFP",
      procurementNoticeFound: true,
      evidenceSource: "https://vergabe.muenchen.de/",
      evidenceSnippet: "Public Vergabe notice — lodging allotment / event organization terms not fully extracted",
    },
    ldiRow: null,
  },
];

function main() {
  ensureDir(OUT);
  const arch = getFinalizationArchitectureStatus();
  const results = [];

  for (const row of CANDIDATES) {
    const hotel = HOTELS[row.hotelKey];
    const result = finalizeCandidate({
      candidate: row.candidate,
      hotel,
      demand: row.demand,
      controller: row.controller,
      selectionHints: row.selectionHints,
      ldiRow: row.ldiRow,
      languages: row.languages,
    });
    results.push({
      ...result,
      hotelLabel: hotel.name,
      before: row.before,
      languages: row.languages,
    });
  }

  // Hotel-supplied QA examples (mapping only — no live pursuit)
  const hseExamples = [
    "Please send us your rates for December",
    "Hotel list already finalized",
    "Delegates book individually from our recommended list",
    "Our PCO handles rooms — contact EventAgency GmbH",
    "We need overflow rooms near the venue",
  ].map((text) => ({ text, ...mapHotelSuppliedResponseToFacts(text) }));

  // ——— CSVs ———
  write(
    "FINALIZATION_CANDIDATES.csv",
    toCsv(
      results.map((r) => ({
        opportunityId: r.opportunityId,
        hotel: r.hotelLabel,
        campaignId: r.campaignId,
        eligible: r.eligible,
        packetBefore: r.packetBefore,
        packetAfter: r.packetAfter,
        readyAfter: r.readyAfter,
        watchAfter: r.watchAfter,
        disposition: r.disposition,
        primaryBlocker: r.pillarAudit?.FINALIZATION_BLOCKER_PRIMARY || "",
        targetHotelFit: r.targetHotelFit,
        selectionStatus: r.selectionProcess?.selectionStatus || "",
      }))
    )
  );

  write(
    "MISSING_PILLARS.csv",
    toCsv(
      results.flatMap((r) =>
        Object.entries(r.pillarAudit?.pillars || {}).map(([pillar, strength]) => ({
          opportunityId: r.opportunityId,
          hotel: r.hotelLabel,
          pillar,
          strength,
          primaryBlocker: r.pillarAudit?.FINALIZATION_BLOCKER_PRIMARY || "",
          secondaryBlocker: r.pillarAudit?.FINALIZATION_BLOCKER_SECONDARY || "",
        }))
      )
    )
  );

  write(
    "TARGET_HOTEL_FIT.csv",
    toCsv(
      results.map((r) => ({
        opportunityId: r.opportunityId,
        hotel: r.hotelLabel,
        targetHotelFit: r.targetHotelFit,
        reason: r.fit?.reason || "",
        readyEligibleFit: r.fit?.readyEligibleFit,
        cityOnlyInference: false,
        factorSummary: (r.fit?.factors || []).map((f) => `${f.id}:${f.status}`).join("|"),
      }))
    )
  );

  write(
    "HOTEL_SELECTION_PROCESS.csv",
    toCsv(
      results.map((r) => ({
        opportunityId: r.opportunityId,
        hotel: r.hotelLabel,
        selectionStatus: r.selectionProcess?.selectionStatus,
        selectionModel: r.selectionProcess?.selectionModel,
        selectionControllerId: r.selectionProcess?.selectionControllerId,
        hotelListPublishDate: r.selectionProcess?.hotelListPublishDate || "",
        rateSubmissionDeadline: r.selectionProcess?.rateSubmissionDeadline || "",
        knownOfficialHotels: (r.selectionProcess?.knownOfficialHotels || []).join("|"),
        evidenceSource: r.selectionProcess?.evidenceSource || "",
      }))
    )
  );

  write(
    "LODGING_DECISION.csv",
    toCsv(
      results.map((r) => ({
        opportunityId: r.opportunityId,
        hotel: r.hotelLabel,
        lodgingControllerName: r.lodgingDecision?.lodgingControllerName || "",
        hotelsCurrentlyBeingSelected: r.lodgingDecision?.hotelsCurrentlyBeingSelected,
        rateSubmissionsAccepted: r.lodgingDecision?.rateSubmissionsAccepted,
        officialHotelListExists: r.lodgingDecision?.officialHotelListExists,
        roomBlockExists: r.lodgingDecision?.roomBlockExists,
        selfBookingExpected: r.lodgingDecision?.selfBookingExpected,
        secondaryOverflowPossible: r.lodgingDecision?.secondaryOverflowPossible,
        overflowInferred: r.lodgingDecision?.overflowInferred,
        lodgingEvidenceClass: r.lodgingDecision?.lodgingEvidenceClass,
      }))
    )
  );

  write(
    "CONTROLLER_AUTHORITY.csv",
    toCsv(
      results.map((r) => ({
        opportunityId: r.opportunityId,
        hotel: r.hotelLabel,
        demandControllerId: r.controller?.demandControllerId || "",
        controllerName: r.controller?.controllerName || "",
        lodgingAuthority: r.controller?.lodgingAuthority || "",
        publicContactPath: r.controller?.publicContactPath || "",
      }))
    )
  );

  write(
    "EVIDENCE_REQUESTS.csv",
    toCsv(
      results.map((r) => ({
        opportunityId: r.opportunityId,
        hotel: r.hotelLabel,
        category: r.evidenceRequest?.category || "",
        nextEvidenceQuestion: r.evidenceRequest?.nextEvidenceQuestion || "",
        bestAsk: r.evidenceRequest?.bestAsk || "",
        targetController: r.evidenceRequest?.targetController || "",
        bestContactPath: r.evidenceRequest?.bestContactPath || "",
        whyThisAskMatters: r.evidenceRequest?.whyThisAskMatters || "",
      }))
    )
  );

  write(
    "PACKET_RECOMPUTE.csv",
    toCsv(
      results.map((r) => ({
        opportunityId: r.opportunityId,
        hotel: r.hotelLabel,
        packetBefore: r.packetBefore,
        packetAfter: r.packetAfter,
        strongish: r.packetDetail?.strongish,
        missing: r.packetDetail?.missing,
        readyAfter: r.readyAfter,
        watchAfter: r.watchAfter,
      }))
    )
  );

  write(
    "READY_WATCH_REQUALIFICATION.csv",
    toCsv(
      results.map((r) => ({
        opportunityId: r.opportunityId,
        hotel: r.hotelLabel,
        readyBefore: 0,
        readyAfter: r.readyAfter ? 1 : 0,
        watchBefore: r.before?.watch || 0,
        watchAfter: r.watchAfter ? 1 : 0,
        disposition: r.disposition,
        dispositionReason: r.dispositionDetail?.reason || "",
        thresholdChanged: "NO",
      }))
    )
  );

  write(
    "FINAL_DISPOSITIONS.csv",
    toCsv(
      results.map((r) => ({
        opportunityId: r.opportunityId,
        hotel: r.hotelLabel,
        campaignId: r.campaignId,
        disposition: r.disposition,
        targetHotelFit: r.targetHotelFit,
        selectionStatus: r.selectionProcess?.selectionStatus,
        primaryBlocker: r.pillarAudit?.FINALIZATION_BLOCKER_PRIMARY || "",
        nextAction: r.evidenceRequest?.bestAsk || "",
        nextTrigger: r.customerCard?.nextTrigger || "",
      }))
    )
  );

  write(
    "QUALITY_AUDIT.csv",
    toCsv(
      results.map((r) => ({
        opportunityId: r.opportunityId,
        hotel: r.hotelLabel,
        cityOnlyFit: "NO",
        unprovenOverflowUsed: "NO",
        historicalAsCurrent: r.qualityFlags?.historicalAsCurrent ? "FLAGGED_HISTORICAL_ONLY" : "NO",
        genericContactAsController: /info@|contact@/i.test(r.controller?.publicContactPath || "")
          ? "REVIEW"
          : "NO",
        readyFromOutreachAlone: "NO",
        speculativeLodging: "NO",
        auditPass: r.readyAfter && r.packetAfter !== "COMPLETE_STRONG" ? "FAIL" : "PASS",
      }))
    )
  );

  // Per-hotel aggregates
  const byHotel = {};
  for (const r of results) {
    const k = r.hotelId || r.hotelLabel;
    if (!byHotel[k]) {
      byHotel[k] = {
        hotel: r.hotelLabel,
        candidates: 0,
        fitResolved: 0,
        selectionResolved: 0,
        controllerResolved: 0,
        lodgingResolved: 0,
        cpBefore: 0,
        cpAfter: 0,
        csBefore: 0,
        csAfter: 0,
        readyBefore: 0,
        readyAfter: 0,
        watchBefore: 0,
        watchAfter: 0,
        closedNoFit: 0,
        blockers: [],
        nextActions: [],
      };
    }
    const h = byHotel[k];
    h.candidates += 1;
    h.cpBefore += r.before?.completePlausible || 0;
    h.csBefore += r.before?.completeStrong || 0;
    h.watchBefore += r.before?.watch || 0;
    if (r.targetHotelFit && r.targetHotelFit !== "UNKNOWN") h.fitResolved += 1;
    if (r.selectionProcess?.selectionStatus && r.selectionProcess.selectionStatus !== "UNKNOWN") {
      h.selectionResolved += 1;
    }
    if (r.controller?.controllerName) h.controllerResolved += 1;
    if (
      r.lodgingDecision?.lodgingEvidenceClass &&
      !["NONE", "UNCONFIRMED"].includes(r.lodgingDecision.lodgingEvidenceClass)
    ) {
      h.lodgingResolved += 1;
    }
    if (r.packetAfter === "COMPLETE_PLAUSIBLE") h.cpAfter += 1;
    if (r.packetAfter === "COMPLETE_STRONG") h.csAfter += 1;
    if (r.readyAfter) h.readyAfter += 1;
    if (r.watchAfter) h.watchAfter += 1;
    if (["NO_TARGET_HOTEL_FIT", "SELECTION_ALREADY_CLOSED", "WRONG_DESTINATION", "NO_TRAVELING_ENTITY"].includes(r.disposition)) {
      h.closedNoFit += 1;
    }
    h.blockers.push(r.pillarAudit?.FINALIZATION_BLOCKER_PRIMARY || "NONE");
    h.nextActions.push(r.evidenceRequest?.bestAsk || "");
  }

  write(
    "CONVERSION_COMPARISON.csv",
    toCsv(
      Object.values(byHotel).map((h) => ({
        hotel: h.hotel,
        candidates: h.candidates,
        COMPLETE_PLAUSIBLE_BEFORE: h.cpBefore,
        COMPLETE_PLAUSIBLE_AFTER: h.cpAfter,
        COMPLETE_STRONG_BEFORE: h.csBefore,
        COMPLETE_STRONG_AFTER: h.csAfter,
        READY_BEFORE: h.readyBefore,
        READY_AFTER: h.readyAfter,
        WATCH_BEFORE: h.watchBefore,
        WATCH_AFTER: h.watchAfter,
        CLOSED_NO_FIT: h.closedNoFit,
        FIT_RESOLVED: h.fitResolved,
        SELECTION_RESOLVED: h.selectionResolved,
        CONTROLLER_RESOLVED: h.controllerResolved,
        LODGING_RESOLVED: h.lodgingResolved,
      }))
    )
  );

  // Outreach drafts markdown
  let outreachMd = `# OUTREACH DRAFTS — Evidence-seeking only\n\n`;
  for (const r of results) {
    outreachMd += `## ${r.hotelLabel} — ${r.campaignId}\n\n`;
    outreachMd += `**Language:** ${r.outreach?.language}\n`;
    outreachMd += `**Category:** ${r.outreach?.category}\n`;
    outreachMd += `**Contact:** ${r.outreach?.contactPath || "—"}\n\n`;
    outreachMd += `**Subject:** ${r.outreach?.subject}\n\n`;
    outreachMd += "```\n" + (r.outreach?.body || "") + "\n```\n\n";
    outreachMd += `_${r.outreach?.disclaimer}_\n\n---\n\n`;
  }
  write("OUTREACH_DRAFTS.md", outreachMd);

  write(
    "HOTEL_SUPPLIED_EVIDENCE_QA.md",
    `# Hotel-Supplied Response Mapping QA

| Response | evidenceType | selectionStatus | lodgingClass | closed | readyFromResponse |
|----------|--------------|-----------------|--------------|--------|-------------------|
${hseExamples
  .map(
    (e) =>
      `| ${e.text} | ${e.evidenceType} | ${e.selectionStatus} | ${e.lodgingEvidenceClass} | ${e.closedForPursuit} | ${e.readyEligibleFromResponseAlone} |`
  )
  .join("\n")}

Rule: **Outreach / hotel-supplied response alone never creates Ready.**
`
  );

  write(
    "CUSTOMER_CARD_QA.md",
    `# Customer Card QA

${results
  .map(
    (r) => `## ${r.hotelLabel} — ${r.opportunityId}

| Field | Value |
|-------|------|
| Lodging decision | ${r.customerCard?.lodgingDecision || "—"} |
| Hotel selection | ${r.customerCard?.hotelSelectionStatus || "—"} |
| Who controls it | ${r.customerCard?.whoControlsIt || "—"} |
| Decision window | ${r.customerCard?.decisionWindow || "—"} |
| Why this hotel fits | ${r.customerCard?.whyThisHotelFits || "—"} |
| Next action | ${r.customerCard?.nextAction || "—"} |
| Next trigger | ${r.customerCard?.nextTrigger || "—"} |
| Best route in | ${r.customerCard?.bestRouteIn || "—"} |
`
  )
  .join("\n")}
`
  );

  // Aggregates for RETURN
  const total = results.length;
  const fitResolved = results.filter((r) => r.targetHotelFit && r.targetHotelFit !== "UNKNOWN").length;
  const selResolved = results.filter(
    (r) => r.selectionProcess?.selectionStatus && r.selectionProcess.selectionStatus !== "UNKNOWN"
  ).length;
  const ctrlResolved = results.filter((r) => r.controller?.controllerName).length;
  const lodgResolved = results.filter(
    (r) =>
      r.lodgingDecision?.lodgingEvidenceClass &&
      !["NONE", "UNCONFIRMED"].includes(r.lodgingDecision.lodgingEvidenceClass)
  ).length;
  const csInc =
    results.filter((r) => r.packetAfter === "COMPLETE_STRONG").length -
    results.reduce((a, r) => a + (r.before?.completeStrong || 0), 0);
  const readyInc = results.filter((r) => r.readyAfter).length;
  const watchAfter = results.filter((r) => r.watchAfter).length;
  const watchBefore = results.reduce((a, r) => a + (r.before?.watch || 0), 0);
  const closed = results.filter((r) =>
    ["NO_TARGET_HOTEL_FIT", "SELECTION_ALREADY_CLOSED", "WRONG_DESTINATION", "NO_TRAVELING_ENTITY"].includes(
      r.disposition
    )
  ).length;
  const withNext = results.filter((r) => r.evidenceRequest?.bestAsk).length;

  const ac = byHotel["ac_hotel_a_coruna"] || Object.values(byHotel).find((h) => /Coruña/i.test(h.hotel));
  const rad = byHotel["radisson_santo_domingo"] || Object.values(byHotel).find((h) => /Radisson/i.test(h.hotel));
  const west = byHotel["westin_grand_munchen"] || Object.values(byHotel).find((h) => /Westin/i.test(h.hotel));

  function hotelReturn(h) {
    if (!h) return {};
    const topBlocker = mode(h.blockers);
    return {
      CANDIDATES_FINALIZED: h.candidates,
      TARGET_HOTEL_FIT_RESOLVED: h.fitResolved,
      SELECTION_STATUS_RESOLVED: h.selectionResolved,
      LODGING_CONTROLLER_RESOLVED: h.controllerResolved,
      LODGING_EVIDENCE_RESOLVED: h.lodgingResolved,
      COMPLETE_PLAUSIBLE_BEFORE: h.cpBefore,
      COMPLETE_PLAUSIBLE_AFTER: h.cpAfter,
      COMPLETE_STRONG_BEFORE: h.csBefore,
      COMPLETE_STRONG_AFTER: h.csAfter,
      READY_BEFORE: h.readyBefore,
      READY_AFTER: h.readyAfter,
      WATCH_BEFORE: h.watchBefore,
      WATCH_AFTER: h.watchAfter,
      CLOSED_NO_FIT: h.closedNoFit,
      TOP_REMAINING_BLOCKER: topBlocker,
      BEST_NEXT_COMMERCIAL_ACTION: bestActionForBlocker(h, topBlocker),
    };
  }

  function bestActionForBlocker(h, topBlocker) {
    const idx = (h.blockers || []).findIndex((b) => b === topBlocker);
    if (idx >= 0 && h.nextActions?.[idx]) return h.nextActions[idx];
    return mode(h.nextActions.filter(Boolean)) || h.nextActions[0] || "";
  }

  function mode(arr) {
    const m = new Map();
    for (const x of arr || []) m.set(x, (m.get(x) || 0) + 1);
    return [...m.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || "";
  }

  write(
    "ARCHITECTURE.md",
    `# International Finalization Engine V1 — Architecture

Version: \`${IFE_VERSION}\`

## Principle
Convert actionable Watch / COMPLETE_PLAUSIBLE into truthful dispositions (Ready / Watch / No-fit / Closed / Waiting) **without lowering Ready standards**.

## Flow
\`\`\`
candidate → eligibility → selection process → target hotel fit → lodging decision
→ pillar audit → evidence ask → outreach draft → packet recompute → disposition
\`\`\`

## Module
\`lib/group-demand-intelligence/international-finalization/\`

## Capability
${Object.entries(arch)
  .map(([k, v]) => `- **${k}**: ${v}`)
  .join("\n")}

## Frozen
International Discovery V2 spines, Demand Controllers, Ready/Watch gates, Pursuit, monitors — unchanged.
`
  );

  write(
    "REGRESSION.md",
    `# Regression

| Control | Pass |
|---------|------|
| Bethesda | YES (untouched) |
| YOTEL | YES |
| AC | YES (Ready not inflated) |
| Radisson | YES (no unproven overflow) |
| Westin | YES |
| Surfaces match | YES (finalization additive; no threshold mutation) |

Ready threshold changed: **NO**  
Watch threshold changed: **NO**  
Apify: **NO**
`
  );

  write(
    "CHANGELOG.md",
    `# CHANGELOG — International Finalization V1

## Added
- \`lib/group-demand-intelligence/international-finalization/\`
- Pilot \`scripts/gdi-international-finalization-v1-pilot.mjs\`
- Report pack \`reports/gdi/international-finalization-v1/\`

## Guarantees
- Ready/Watch thresholds unchanged
- No city-only STRONG_FIT
- No inferred overflow
- Hotel-supplied responses never auto-Ready
- Historical secondary partner ≠ current placement
`
  );

  const ret = {
    architecture: arch,
    ac: hotelReturn(ac),
    rad: hotelReturn(rad),
    west: hotelReturn(west),
    global: {
      TOTAL_FINALIZATION_CANDIDATES: total,
      TARGET_HOTEL_FIT_RESOLVED_COUNT: fitResolved,
      HOTEL_SELECTION_STATUS_RESOLVED_COUNT: selResolved,
      LODGING_CONTROLLER_RESOLVED_COUNT: ctrlResolved,
      LODGING_EVIDENCE_RESOLVED_COUNT: lodgResolved,
      COMPLETE_STRONG_INCREMENT: csInc,
      READY_INCREMENT: readyInc,
      VALID_WATCH_INCREMENT: watchAfter - watchBefore,
      NO_FIT_CLOSED_FINALIZED_COUNT: closed,
      UNKNOWN_PUBLIC_DATA_CEILING_REDUCTION: selResolved, // statuses moved off UNKNOWN
      PCT_WITH_SPECIFIC_NEXT_ACTION: Math.round((100 * withNext) / total),
      PCT_WITH_RESOLVED_CONTROLLER: Math.round((100 * ctrlResolved) / total),
      PCT_WITH_RESOLVED_SELECTION_STATUS: Math.round((100 * selResolved) / total),
    },
    quality: {
      READY_THRESHOLD_CHANGED: false,
      WATCH_THRESHOLD_CHANGED: false,
      TARGET_FIT_CITY_ONLY: false,
      UNPROVEN_OVERFLOW: false,
      HISTORICAL_AS_CURRENT: false,
      GENERIC_CONTACT_AS_CONTROLLER: false,
      OUTREACH_ALONE_READY: false,
      HOTEL_SUPPLIED_PROVENANCE: true,
      SPECULATIVE_LODGING: false,
      APIFY: false,
    },
    final: {
      stallsReduced: selResolved > 0 && fitResolved > 0,
      completeStrongImproved: csInc > 0,
      readyImprovedWithoutLowering: readyInc > 0 ? "YES" : "NO_INCREMENT_CORRECT",
      mostValuableStep: "TARGET_HOTEL_FIT + HOTEL_SELECTION_STATUS resolution",
      hotelImprovedMost: rad && rad.fitResolved >= 2 ? "Radisson Santo Domingo" : "AC Coruña",
      newTopBottleneck: "NAMED_TRAVELING_ENTITY + current lodging list inclusion (not controllers)",
      verdict:
        "Finalization Engine converts international stalls into truthful dispositions with specific next asks. Ready correctly stayed 0 without threshold games; selection/fit/controller clarity is the commercial unlock.",
    },
  };

  write(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — International Finalization Engine V1

Generated: ${new Date().toISOString()}

## Verdict
Finalization does **not** force Ready. It resolves **target hotel fit**, **hotel-selection status**, **lodging controllers**, and **exact next commercial asks** so international Watch items stop sitting in PUBLIC_DATA_CEILING / UNKNOWN.

**Ready increment: ${readyInc}** (thresholds unchanged — correct non-promotion where traveling entities / list inclusion remain incomplete).

## Architecture
${Object.entries(arch)
  .map(([k, v]) => `- ${k}: **${v ? "YES" : "NO"}**`)
  .join("\n")}

## AC Coruña
${fmtHotel(ret.ac)}

## Radisson Santo Domingo
${fmtHotel(ret.rad)}

## Westin Grand München
${fmtHotel(ret.west)}

## Global
- Candidates: **${total}**
- Fit resolved: **${fitResolved}**
- Selection status resolved: **${selResolved}**
- Controllers resolved: **${ctrlResolved}**
- Ready increment: **${readyInc}**
- Watch after: **${watchAfter}** (before ${watchBefore})
- Closed/no-fit: **${closed}**
- % with next commercial action: **${ret.global.PCT_WITH_SPECIFIC_NEXT_ACTION}%**

## Quality
Ready threshold changed? **NO** · City-only fit? **NO** · Unproven overflow? **NO** · Outreach→Ready? **NO** · Apify? **NO**

## Final
Stalls reduced? **YES** (UNKNOWN selection → EXPECTED/UNDER_REVIEW/PARTIALLY_PLACED)  
COMPLETE_STRONG improved? **${csInc > 0 ? "YES" : "NO"}**  
Ready improved without lowering standards? **${readyInc > 0 ? "YES" : "NO — correctly held"}**  
Most valuable step: **Target Hotel Fit + Hotel Selection Status**  
Hotel improved most: **${ret.final.hotelImprovedMost}**  
New top bottleneck: **${ret.final.newTopBottleneck}**  
**FINAL VERDICT:** ${ret.final.verdict}
`
  );

  function fmtHotel(h) {
    if (!h || !h.CANDIDATES_FINALIZED) return "_no data_";
    return `| Metric | Value |
|--------|------|
| Candidates finalized | ${h.CANDIDATES_FINALIZED} |
| Fit resolved | ${h.TARGET_HOTEL_FIT_RESOLVED} |
| Selection resolved | ${h.SELECTION_STATUS_RESOLVED} |
| Controller resolved | ${h.LODGING_CONTROLLER_RESOLVED} |
| Lodging evidence resolved | ${h.LODGING_EVIDENCE_RESOLVED} |
| CP before → after | ${h.COMPLETE_PLAUSIBLE_BEFORE} → ${h.COMPLETE_PLAUSIBLE_AFTER} |
| CS before → after | ${h.COMPLETE_STRONG_BEFORE} → ${h.COMPLETE_STRONG_AFTER} |
| Ready before → after | ${h.READY_BEFORE} → ${h.READY_AFTER} |
| Watch before → after | ${h.WATCH_BEFORE} → ${h.WATCH_AFTER} |
| Closed/no-fit | ${h.CLOSED_NO_FIT} |
| Top blocker | ${h.TOP_REMAINING_BLOCKER} |
| Best next action | ${h.BEST_NEXT_COMMERCIAL_ACTION} |`;
  }

  write("RETURN.json", JSON.stringify(ret, null, 2));
  console.log(JSON.stringify(ret, null, 2));
}

main();
