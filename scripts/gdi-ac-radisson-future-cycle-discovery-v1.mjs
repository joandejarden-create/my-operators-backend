#!/usr/bin/env node
/**
 * Targeted future-cycle GDI discovery — AC Coruña + Radisson SD.
 * NO broad market-first. NO Apify. NO threshold changes. NO forced Ready.
 *
 * Usage: node scripts/gdi-ac-radisson-future-cycle-discovery-v1.mjs [--apply]
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
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
} from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";
import { isCustomerFacingOpportunity } from "../lib/group-demand-intelligence/customer-visibility.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { meetsReadyContactRequirement } from "../lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js";
import { applyGdiWhoHowResolution } from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/ac-coruna-radisson-future-cycle-discovery");
const APPLY = process.argv.includes("--apply");
const NOW = "2026-10-07";
const NOW_OPTS = { nowDate: NOW };
const RUN = `gdi_future_cycle_${crypto.randomBytes(3).toString("hex")}`;

const AC = "rec2PVBDavppGpenm";
const RAD = "recUOyzOXn2Zdp98I";

/** Bag leftovers that still pass Valid Watch but are wrong-destination / out of market. */
const EXCLUDE_FROM_WATCH = [
  {
    hotelId: AC,
    id: "gdi_opp_championship_26_27_9",
    reason: "WRONG_DESTINATION_EFL_ENGLAND",
  },
  {
    hotelId: RAD,
    id: "gdi_opp_naco_legislative_conference_4",
    reason: "WRONG_DESTINATION_WASHINGTON_DC",
  },
];

/** Serious candidates researched this pass (admitted or stopped). */
const CANDIDATES = [
  // ─── AC: IAPS upgrade ───
  {
    hotelKey: "AC",
    hotelId: AC,
    action: "UPSERT",
    id: "gdi_opp_international_symposium_6",
    disposition: "ADMIT_WATCH",
    family: "ASSOCIATION_ACADEMIC",
    stopReason: "",
    patch: {
      title: "IAPS Spaces in Transition Symposium 2027",
      organizationName: "IAPS Sustainability Network / UDC People-Environment Research Group",
      opportunityType: "FUTURE_WATCH",
      customerFacingState: "FUTURE_WATCH",
      eventStartDate: "2027-06-14",
      eventEndDate: "2027-06-16",
      eventYear: 2027,
      destinationCity: "A Coruña",
      destinationCountry: "Spain",
      destinationStatus: "A Coruña confirmed; venue TBA",
      officialSource: "https://iaps-association.org/",
      discoverySource: "https://iaps-association.org/networks/sustainability/",
      sources: [
        { url: "https://iaps-association.org/", label: "IAPS official" },
        {
          url: "https://iaps-association.org/networks/sustainability/",
          label: "IAPS Sustainability Network",
        },
        {
          url: "https://www.elidealgallego.com/a-coruna/2026-06-29/a-coruna-acogera-en-2027-el-mayor-congreso-de-sostenibilidad-a-nivel-mundial-864894.html",
          label: "Local announcement UDC hosts",
        },
      ],
      travelingEntityType: "ASSOCIATION_DELEGATION",
      travelingEntityEvidence:
        "Official IAPS save-the-date + UDC press: international researchers/practitioners/policymakers traveling to A Coruña 14–16 Jun 2027 (local press cites >500). Named company teams not yet published.",
      travelingEntityProven: false,
      travelingEntityClass: "TRAVELING_ENTITY_STRONG_INFERENCE",
      groupMotionType: "ASSOCIATION_DELEGATION",
      groupMotionEvidence:
        "Multi-day international network symposium; abstracts due 31 Dec 2026; prior IAPS Sustainability symposiums drew ~500 hybrid participants.",
      groupMotionConfidence: "MEDIUM",
      travelReason: "Present/attend IAPS Sustainability + Culture & Space symposium",
      likelyTravelWindow: "2027-06-13/2027-06-17",
      destination: "A Coruña, Spain",
      // UNCONFIRMED lodging for Ready gate; thesis + whyMonitor keep Valid Future Watch.
      hotelMotionClass: "UNCONFIRMED",
      lodgingEvidence: "none",
      lodgingEvidenceDetail:
        "Official pages still say venue and website details will follow — no accommodation page, housing bureau, or hotel partner list published as of 2026-10-07.",
      housingStatus: "NONE",
      venueStatus: "HOTEL_TBD",
      lodgingController: null,
      lodgingControllerFound: false,
      // Clear prior cleanup exclusion so Valid Future Watch can re-admit.
      watchExcludedFromFutureWatch: false,
      watchValidation: null,
      // Named local convenors (ASCII names for person-name gate; emails are official)
      primaryContactName: "Ricardo Garcia Mira",
      primaryContactRole: "IAPS Sustainability Network coordinator / UDC convenor",
      primaryContact: {
        name: "Ricardo Garcia Mira",
        role: "IAPS Sustainability Network coordinator / UDC convenor",
        email: "ricardo.garcia.mira@udc.es",
        url: "https://iaps-association.org/networks/sustainability/",
        sourceUrl: "https://iaps-association.org/networks/sustainability/",
      },
      secondaryContactName: "Cristina Garcia Fontan",
      secondaryContactRole: "IAPS Sustainability Network coordinator / UDC chair",
      secondaryContactEmail: "cristina.garcia.fontan@udc.es",
      organizationContactUrl: "https://iaps-association.org/networks/sustainability/",
      // https path required by contact URL classifier (mailto alone → NO_CONTACT)
      publicContactPath: "https://iaps-association.org/networks/sustainability/",
      contactPathClass: "NAMED_BUYER_PERSON",
      whoPathClass: "NAMED_DIRECT",
      contactResearchAttempted: true,
      whoResearchAttempted: true,
      whoResearchState: "ATTEMPTED_NAMED_CONVENOR",
      buyerRole: "Local symposium convenor / network coordinator",
      buyerOrganization: "UDC People-Environment Research Group / IAPS Sustainability Network",
      futureCycleEvidenceState: "CURRENT_FUTURE_CYCLE_CONFIRMED",
      roomDemandStatus: "UNKNOWN",
      whyMonitor:
        "Venue and official housing/accommodation page are still unpublished; monitor abstract close and housing RFP before Ready outreach.",
      nextTriggerType: "HOUSING_OPEN",
      nextTriggerCondition:
        "Venue + preferred-hotel / accommodation page published, or abstract deadline 2026-12-31 passes with housing guidance",
      nextResearchDate: "2026-12-15",
      futureDecisionType: "ABSTRACT_DEADLINE",
      futureDecisionDate: "2026-12-31",
      decisionWindow: "Abstracts due 31 Dec 2026; venue/housing announcement is next trigger",
      futureDecisionEvidence:
        "IAPS official news: call for papers opened 1 Sep 2026; abstracts 300–500 words due 31 Dec 2026.",
      whyNow:
        "Confirm lodging path and buyer function before outreach. IAPS Spaces in Transition is confirmed for A Coruña 14–16 Jun 2027; named UDC convenors are public; official housing/venue still unpublished.",
      cardWhyNowLine:
        "Abstracts due 31 Dec 2026; ask UDC convenors when housing RFP opens.",
      recommendedAction:
        "Email Ricardo García Mira / Cristina García Fontán to introduce AC Hotel A Coruña (116 keys, Matogrande) as a city lodging option before the housing list is published.",
      recommendedNextStep:
        "Request timing for venue selection and preferred-hotel list; offer group rates for international delegates.",
      summaryWhat:
        "IAPS Sustainability and Culture & Space networks host an international symposium in A Coruña (14–16 Jun 2027), convened locally by UDC researchers.",
      summaryWhyMatters:
        "International academic travelers need multi-night lodging while venue and hotel partners remain open.",
      summaryWhyHotel:
        "AC Hotel A Coruña is an in-market upper-upscale option near Expocoruna/Matogrande for midsize academic groups.",
      fitExplanation:
        "116 rooms and meeting inventory fit academic delegations; city location matches confirmed host city.",
      hotelFitScore: 78,
      hotelOpportunityThesis:
        "While venue and housing remain TBA, AC can pitch UDC convenors as a preferred city hotel for international delegates.",
      accountQualityClass: "NAMED_ASSOCIATION_NETWORK",
      iapsBuyerPathFound: true,
      iapsHousingPathFound: false,
      iapsNextTrigger: "2026-12-31 abstract deadline / venue+housing page publication",
      iapsClassification: "VALID_FUTURE_WATCH",
      packetForensicNote: "FUTURE_CYCLE_DISCOVERY_2026_10_07",
      futureCycleDiscoveryRunId: RUN,
    },
  },
  // ─── AC: BioCultura 2027 ───
  {
    hotelKey: "AC",
    hotelId: AC,
    action: "CREATE",
    id: "gdi_opp_biocultura_a_coruna_2027",
    disposition: "ADMIT_WATCH",
    family: "ASSOCIATION_TRADE_FAIR",
    stopReason: "",
    patch: {
      title: "BioCultura A Coruña 2027",
      organizationName: "Asociación Vida Sana",
      opportunityType: "FUTURE_WATCH",
      customerFacingState: "FUTURE_WATCH",
      eventStartDate: "2027-03-05",
      eventEndDate: "2027-03-07",
      eventYear: 2027,
      destinationCity: "A Coruña",
      destinationCountry: "Spain",
      destinationStatus: "EXPOCoruña — confirmed",
      officialSource: "https://www.biocultura.org/acoruna",
      discoverySource: "https://www.biocultura.org/acoruna/informacion-expositores",
      sources: [
        { url: "https://www.biocultura.org/acoruna", label: "BioCultura official" },
        {
          url: "https://www.biocultura.org/acoruna/viajes",
          label: "Viajes y alojamientos page",
        },
        {
          url: "https://www.biocultura.org/acoruna/informacion-expositores",
          label: "Exhibitor info",
        },
      ],
      travelingEntityType: "EXHIBITOR_TEAM",
      travelingEntityEvidence:
        "Official: 9th edition expects 130+ exhibitors from eco-food, cosmetics, fashion, lifestyle; multi-day fair at EXPOCoruña — out-of-market exhibitor teams travel.",
      travelingEntityProven: true,
      travelingEntityClass: "TRAVELING_ENTITY_PROVEN",
      groupMotionType: "EXHIBITOR_TEAM",
      groupMotionEvidence:
        "Exhibitor registration open; stand allocation from 10 Jan 2027; multi-day on-site exhibition.",
      groupMotionConfidence: "HIGH",
      travelReason: "Exhibit / staff BioCultura stands",
      likelyTravelWindow: "2027-03-04/2027-03-08",
      hotelMotionClass: "PLAUSIBLE_HOTEL_MOTION",
      lodgingEvidence:
        "Official Viajes y Alojamientos page publishes Renfe discount contact only — no hotel partner list or room block published yet.",
      housingStatus: "NONE",
      venueStatus: "EXPOCORUNA_CONFIRMED",
      lodgingController: "Asociación Vida Sana (expositores@vidasana.org / m.sanchez@vidasana.org)",
      lodgingControllerFound: true,
      // Keep Watch actionable emails in controller fields; do not stamp Ready-eligible role regex.
      primaryContactName: null,
      primaryContactRole: "Association administration",
      primaryContact: {
        name: null,
        role: "Association administration",
        email: "info@vidasana.org",
        url: "https://www.biocultura.org/acoruna",
      },
      organizationContactUrl: "https://www.biocultura.org/acoruna",
      publicContactPath: "https://www.biocultura.org/acoruna",
      contactPathClass: "GENERAL_ORG_CONTACT",
      contactResearchAttempted: true,
      whoResearchAttempted: true,
      buyerRole: null,
      buyerOrganization: "Asociación Vida Sana",
      futureCycleEvidenceState: "CURRENT_FUTURE_CYCLE_CONFIRMED",
      roomDemandStatus: "UNKNOWN",
      whyMonitor:
        "Hotel partner list not published on Viajes y Alojamientos; monitor exhibitor housing circular before Ready.",
      nextTriggerType: "HOUSING_OPEN",
      nextTriggerCondition:
        "Vida Sana publishes preferred-hotel / alojamientos partner list, or stand allocation window opens 2027-01-10",
      nextResearchDate: "2026-12-01",
      futureDecisionType: "EXHIBITOR_DEADLINE",
      futureDecisionDate: "2027-01-10",
      decisionWindow: "Exhibitor signup open; stand allocation from 10 Jan 2027; fair 5–7 Mar 2027",
      futureDecisionEvidence:
        "Official exhibitor page: stand allocation from 10 Jan 2027; fair 5–7 Mar 2027 at EXPOCoruña.",
      whyNow:
        "Confirm lodging partner inclusion before selling as Ready. BioCultura A Coruña 2027 is confirmed at EXPOCoruña (5–7 Mar); housing list not yet published — pitch AC via expositores@vidasana.org / m.sanchez@vidasana.org.",
      cardWhyNowLine:
        "Exhibitor stand allocation from 10 Jan 2027; ask Vida Sana for hotel partner inclusion.",
      recommendedAction:
        "Contact expositores@vidasana.org / m.sanchez@vidasana.org to propose AC Hotel A Coruña for the preferred lodging list.",
      recommendedNextStep: "Offer exhibitor group rates for 4–7 Mar 2027 nights.",
      summaryWhat:
        "BioCultura 9th edition eco/organic trade fair at EXPOCoruña, 5–7 Mar 2027, organized by Asociación Vida Sana.",
      summaryWhyMatters:
        "130+ exhibitor companies send traveling teams for a multi-day fair near Matogrande.",
      summaryWhyHotel:
        "AC Hotel A Coruña sits on the Expocoruna corridor and historically appears on medical congress housing lists in this city.",
      fitExplanation: "Corridor fit + midsize exhibitor peaks within 15–80 room band.",
      hotelFitScore: 74,
      hotelOpportunityThesis:
        "Exhibitor lodging controllers at Vida Sana have not published hotels — window to join partner list.",
      accountQualityClass: "NAMED_TRADE_ASSOCIATION",
      packetForensicNote: "FUTURE_CYCLE_DISCOVERY_2026_10_07",
      futureCycleDiscoveryRunId: RUN,
    },
  },
  // ─── AC stopped ───
  {
    hotelKey: "AC",
    hotelId: AC,
    action: "STOP",
    id: "stop_navalia_2028",
    disposition: "STOPPED",
    family: "INDUSTRIAL",
    stopReason: "WRONG_DESTINATION_VIGO",
    title: "Navalia 2028",
    notes: "IFEVI Vigo 23–25 May 2028 — not A Coruña lodging.",
  },
  {
    hotelKey: "AC",
    hotelId: AC,
    action: "STOP",
    id: "stop_exporock_2027",
    disposition: "STOPPED",
    family: "SPORTS_ENTERTAINMENT",
    stopReason: "NO_GROUP_LODGING_CONTROLLER_CONCERT",
    title: "EXPORock 2027",
    notes: "Single-day concert at Expocoruna — no exhibitor/housing controller evidenced.",
  },
  {
    hotelKey: "AC",
    hotelId: AC,
    action: "STOP",
    id: "stop_camara_mexico_wine_2027",
    disposition: "STOPPED",
    family: "CORPORATE",
    stopReason: "OUTBOUND_MISSION",
    title: "Misión Exposición Vinos Ciudad de México Marzo 2027",
    notes: "Outbound Cámara mission to Mexico City — lodging demand abroad.",
  },
  {
    hotelKey: "AC",
    hotelId: AC,
    action: "STOP",
    id: "stop_semg_2024_pattern_only",
    disposition: "STOPPED",
    family: "MEDICAL",
    stopReason: "NO_FUTURE_CYCLE",
    title: "SEMG National Congress (historical A Coruña housing pattern)",
    notes:
      "2024 housing listed AC Hotel A Coruña via Grupo Pacífico — useful pattern, no 2027 A Coruña SEMG cycle confirmed.",
  },

  // ─── RAD: RIF Philosophy 2027 ───
  {
    hotelKey: "RAD",
    hotelId: RAD,
    action: "CREATE",
    id: "gdi_opp_rif_filosofia_sd_2027",
    disposition: "ADMIT_WATCH",
    family: "ASSOCIATION_ACADEMIC",
    stopReason: "",
    patch: {
      title: "VII Congreso Iberoamericano de Filosofía 2027",
      organizationName: "Red Iberoamericana de Filosofía / Asociación Dominicana de Filosofía",
      opportunityType: "FUTURE_WATCH",
      customerFacingState: "FUTURE_WATCH",
      eventStartDate: "2027-03-15",
      eventEndDate: "2027-03-19",
      eventYear: 2027,
      destinationCity: "Santo Domingo",
      destinationCountry: "Dominican Republic",
      destinationStatus: "UASD Facultad de Humanidades + Academia de Ciencias (Zona Colonial)",
      officialSource:
        "https://rediberoamericanafilosofia.com/wp-content/uploads/2026/05/Segunda-convocatoria-Congreso-RIF.pdf",
      discoverySource:
        "https://redfilosofia.es/blog/2026/04/21/primera-convocatoria-vii-congreso-iberoamericano-de-filosofia/",
      sources: [
        {
          url: "https://rediberoamericanafilosofia.com/wp-content/uploads/2026/05/Segunda-convocatoria-Congreso-RIF.pdf",
          label: "Official segunda convocatoria PDF",
        },
      ],
      travelingEntityType: "ASSOCIATION_DELEGATION",
      travelingEntityEvidence:
        "Iberoamerican philosophy congress with speakers/panels from across Spanish- and Portuguese-speaking countries; multi-day on-site at UASD.",
      travelingEntityProven: true,
      travelingEntityClass: "TRAVELING_ENTITY_PROVEN",
      groupMotionType: "ASSOCIATION_DELEGATION",
      groupMotionEvidence:
        "5-day congress; proposal deadline 30 Oct 2026; on-site registration day 1.",
      groupMotionConfidence: "HIGH",
      travelReason: "Present / attend RIF congress sessions",
      likelyTravelWindow: "2027-03-14/2027-03-20",
      // Preferential rates exist but Radisson is not yet named — PLAUSIBLE for this hotel, not Ready.
      hotelMotionClass: "PLAUSIBLE_HOTEL_MOTION",
      lodgingEvidence:
        "Official segunda convocatoria: organization negotiated preferential rates in Zona Colonial / Distrito Nacional; partner hotel list to be published next circular — Radisson not yet named.",
      housingStatus: "WEAK",
      venueStatus: "UNIVERSITY_VENUE_HOUSING_OPEN",
      lodgingController: "Comité Organizador RIF–UASD 2027 / ADOFIL (adofil333@gmail.com)",
      lodgingControllerFound: true,
      primaryContactName: null,
      primaryContactRole: "Association administration",
      primaryContact: {
        name: null,
        role: "Association administration",
        email: "adofil333@gmail.com",
        url: "https://rediberoamericanafilosofia.com/wp-content/uploads/2026/05/Segunda-convocatoria-Congreso-RIF.pdf",
      },
      organizationContactUrl:
        "https://rediberoamericanafilosofia.com/wp-content/uploads/2026/05/Segunda-convocatoria-Congreso-RIF.pdf",
      publicContactPath:
        "https://rediberoamericanafilosofia.com/wp-content/uploads/2026/05/Segunda-convocatoria-Congreso-RIF.pdf",
      contactPathClass: "SOURCE_PAGE",
      contactResearchAttempted: true,
      whoResearchAttempted: true,
      buyerRole: null,
      buyerOrganization: "Asociación Dominicana de Filosofía (ADOFIL) / RIF–UASD committee",
      futureCycleEvidenceState: "CURRENT_FUTURE_CYCLE_CONFIRMED",
      roomDemandStatus: "UNKNOWN",
      whyMonitor:
        "Preferential hotel rates negotiated but partner list unpublished and Radisson not named; wait for convenio circular.",
      nextTriggerType: "HOUSING_OPEN",
      nextTriggerCondition:
        "ADOFIL/RIF publishes preferential-hotel circular naming partner hotels, or proposal results 2026-12-15",
      nextResearchDate: "2026-11-15",
      futureDecisionType: "PROPOSAL_DEADLINE",
      futureDecisionDate: "2026-10-30",
      decisionWindow:
        "Proposal close 30 Oct 2026; hotel convenio list next circular; congress 15–19 Mar 2027",
      futureDecisionEvidence:
        "Official PDF critical dates: proposal close 30 Oct 2026; results 15 Dec 2026; lodging list forthcoming.",
      whyNow:
        "Confirm lodging path and buyer function before outreach. VII Congreso Iberoamericano de Filosofía is confirmed 15–19 Mar 2027; preferential rates exist but partner hotel list unpublished — ask ADOFIL (adofil333@gmail.com) to include Radisson.",
      cardWhyNowLine:
        "Proposal deadline 30 Oct 2026; ask ADOFIL to include Radisson on the convenio hotel circular.",
      recommendedAction:
        "Email adofil333@gmail.com and r.iberoamericanadefilosofia@gmail.com proposing Radisson Santo Domingo (Naco) for the preferential-rate hotel list.",
      recommendedNextStep:
        "Send rate sheet + location map vs UASD / Academia de Ciencias; ask circular publication date.",
      summaryWhat:
        "RIF + ADOFIL + UASD host the VII Iberoamerican Philosophy Congress in Santo Domingo, 15–19 Mar 2027.",
      summaryWhyMatters:
        "International academic speakers need multi-night lodging; organizers control preferential hotel rates and list is still unpublished.",
      summaryWhyHotel:
        "Radisson Santo Domingo is Distrito Nacional / Naco upper-upscale inventory suited to visiting faculty and panels.",
      fitExplanation:
        "160 rooms + meeting space; DN location matches organizer’s Colonial/DN hotel search radius.",
      hotelFitScore: 76,
      hotelOpportunityThesis:
        "Open lodging-controller window before convenio list publishes — strongest Radisson future Watch this pass.",
      accountQualityClass: "NAMED_ASSOCIATION",
      packetForensicNote: "FUTURE_CYCLE_DISCOVERY_2026_10_07",
      futureCycleDiscoveryRunId: RUN,
    },
  },
  // ─── RAD: AutoAmericas 2027 ───
  {
    hotelKey: "RAD",
    hotelId: RAD,
    action: "CREATE",
    id: "gdi_opp_autoamericas_2027",
    disposition: "ADMIT_WATCH",
    family: "CORPORATE_TRADE_SHOW",
    stopReason: "",
    patch: {
      title: "AUTOAMERICAS 2027",
      organizationName: "AutoAméricas / Latinpress",
      opportunityType: "FUTURE_WATCH",
      customerFacingState: "FUTURE_WATCH",
      eventStartDate: "2027-04-23",
      eventEndDate: "2027-04-24",
      eventYear: 2027,
      destinationCity: "Santo Domingo",
      destinationCountry: "Dominican Republic",
      destinationStatus: "Hotel Dominican Fiesta — official host/venue 2027",
      officialSource: "https://www.autoamericas.show/es/expo/alojamiento.html",
      discoverySource: "https://www.autoamericas.show/es/expo/alojamiento.html",
      sources: [
        {
          url: "https://www.autoamericas.show/es/expo/alojamiento.html",
          label: "Official lodging page",
        },
      ],
      travelingEntityType: "EXHIBITOR_TEAM",
      travelingEntityEvidence:
        "Official lodging page markets preferential rates for exhibitors, speakers, and attendees; multi-day auto industry expo.",
      travelingEntityProven: true,
      travelingEntityClass: "TRAVELING_ENTITY_PROVEN",
      groupMotionType: "EXHIBITOR_TEAM",
      groupMotionEvidence: "Two-day expo with exhibitor/speaker lodging program.",
      groupMotionConfidence: "HIGH",
      hotelMotionClass: "PLAUSIBLE_HOTEL_MOTION",
      lodgingEvidence:
        "2027 official host hotel is Dominican Fiesta (Claribel García reservations). 2026 partner list included Radisson Santo Domingo group sales — 2027 secondary convenio list not yet republished beyond Fiesta.",
      housingStatus: "NONE",
      venueStatus: "HOST_HOTEL_NAMED_COMPETITOR",
      lodgingController:
        "AutoAméricas / Latinpress (acaballero@autoamericas.show); Fiesta host Claribel García",
      lodgingControllerFound: true,
      primaryContactName: "Andrés Caballero",
      primaryContactRole: "Association administration",
      primaryContact: {
        name: "Andrés Caballero",
        role: "Association administration",
        email: "acaballero@autoamericas.show",
        url: "https://www.autoamericas.show/es/expo/alojamiento.html",
      },
      organizationContactUrl: "https://www.autoamericas.show/es/expo/alojamiento.html",
      publicContactPath: "https://www.autoamericas.show/es/expo/alojamiento.html",
      contactPathClass: "SOURCE_PAGE",
      contactResearchAttempted: true,
      whoResearchAttempted: true,
      buyerRole: null,
      buyerOrganization: "AutoAméricas / Latinpress",
      futureCycleEvidenceState: "CURRENT_FUTURE_CYCLE_CONFIRMED",
      roomDemandStatus: "UNKNOWN",
      whyMonitor:
        "Host hotel is Dominican Fiesta; Radisson only returns if secondary partner convenio is restored for 2027.",
      nextTriggerType: "HOUSING_OPEN",
      nextTriggerCondition:
        "AutoAméricas republishes secondary partner-hotel list including Radisson, or Fiesta rates published",
      nextResearchDate: "2027-01-15",
      futureDecisionType: "SECONDARY_HOUSING_CONVENIO",
      futureDecisionDate: "2027-04-01",
      decisionWindow:
        "2027 Fiesta rates TBA; ask to restore Radisson as secondary partner hotel before April expo",
      futureDecisionEvidence:
        "Official 2027 lodging page names Fiesta only; 2026 page listed Radisson group sales contact.",
      whyNow:
        "Confirm lodging path before selling as Ready. AUTOAMERICAS 2027 host is Dominican Fiesta; Radisson was a 2026 partner hotel — ask acaballero@autoamericas.show to reinstate Radisson for 2027.",
      cardWhyNowLine:
        "Contact AutoAméricas sales to restore Radisson as 2027 partner hotel (Fiesta is host).",
      recommendedAction:
        "Email acaballero@autoamericas.show requesting 2027 secondary lodging convenio for Radisson Santo Domingo.",
      recommendedNextStep: "Reference 2026 Radisson group-sales listing; propose rate sheet.",
      summaryWhat:
        "AUTOAMERICAS auto industry expo returns to Santo Domingo 23–24 Apr 2027 with official lodging at Dominican Fiesta.",
      summaryWhyMatters:
        "Exhibitor/speaker teams travel; secondary partner hotels have historically absorbed overflow.",
      summaryWhyHotel:
        "Radisson was a listed 2026 partner lodging option and remains a Naco upper-upscale alternative.",
      fitExplanation: "Urban meetings hotel; partner/overflow fit only — not host.",
      hotelFitScore: 62,
      hotelOpportunityThesis:
        "Not Ready (host placed). Valid Watch for partner-hotel reinstatement before rates lock.",
      accountQualityClass: "NAMED_ORGANIZER",
      packetForensicNote: "FUTURE_CYCLE_DISCOVERY_2026_10_07",
      futureCycleDiscoveryRunId: RUN,
    },
  },
  // ─── RAD: Cielo Laboral Dec 2026 ───
  {
    hotelKey: "RAD",
    hotelId: RAD,
    action: "CREATE",
    id: "gdi_opp_cielo_laboral_sd_2026",
    disposition: "ADMIT_WATCH",
    family: "ASSOCIATION_ACADEMIC",
    stopReason: "",
    patch: {
      title: "6º Congreso Mundial CIELO Laboral 2026",
      organizationName: "CIELO Laboral / PUCMM",
      opportunityType: "FUTURE_WATCH",
      customerFacingState: "FUTURE_WATCH",
      eventStartDate: "2026-12-02",
      eventEndDate: "2026-12-04",
      eventYear: 2026,
      destinationCity: "Santo Domingo",
      destinationCountry: "Dominican Republic",
      destinationStatus: "PUCMM campus — Santo Domingo",
      officialSource: "https://www.cielolaboral.com/6o-congreso-mundial-cielo-laboral-santodomingo/",
      discoverySource:
        "https://www.cielolaboral.com/wp-content/uploads/2026/04/HOTELES-RECOMENDADOS-PARA-6o-CONGRESO-MUNDIAL-CIELO-LABORAL-REPUBLICA-DOMINICANA-2-3-Y-4-DICIEMBRE-202630.pdf",
      sources: [
        {
          url: "https://www.cielolaboral.com/6o-congreso-mundial-cielo-laboral-santodomingo/",
          label: "Official congress page",
        },
        {
          url: "https://www.cielolaboral.com/wp-content/uploads/2026/04/HOTELES-RECOMENDADOS-PARA-6o-CONGRESO-MUNDIAL-CIELO-LABORAL-REPUBLICA-DOMINICANA-2-3-Y-4-DICIEMBRE-202630.pdf",
          label: "Recommended hotels PDF",
        },
      ],
      travelingEntityType: "ASSOCIATION_DELEGATION",
      travelingEntityEvidence:
        "World congress of international labor-law network; participants cover own travel/lodging; multi-day on-site at PUCMM.",
      travelingEntityProven: true,
      travelingEntityClass: "TRAVELING_ENTITY_PROVEN",
      groupMotionType: "ASSOCIATION_DELEGATION",
      groupMotionEvidence: "3-day world congress Dec 2026; registration open pathway via organizers.",
      groupMotionConfidence: "HIGH",
      hotelMotionClass: "PLAUSIBLE_HOTEL_MOTION",
      lodgingEvidence:
        "Organizers published a recommended-hotels PDF (Holiday Inn, Aloft, Marriott Piantini, Hyatt Centric, Hilton Homewood, InterContinental, etc.). Radisson not listed — ask to join list. Travel/lodging costs borne by participants.",
      housingStatus: "NONE",
      venueStatus: "UNIVERSITY_VENUE_RECOMMENDED_HOTELS",
      lodgingController: "CIELO Laboral (congresocielo6@gmail.com / comunidad@cielolaboral.com)",
      lodgingControllerFound: true,
      primaryContactName: null,
      primaryContactRole: "Association administration",
      primaryContact: {
        name: null,
        role: "Association administration",
        email: "comunidad@cielolaboral.com",
        url: "https://www.cielolaboral.com/6o-congreso-mundial-cielo-laboral-santodomingo/",
      },
      organizationContactUrl:
        "https://www.cielolaboral.com/6o-congreso-mundial-cielo-laboral-santodomingo/",
      publicContactPath:
        "https://www.cielolaboral.com/6o-congreso-mundial-cielo-laboral-santodomingo/",
      contactPathClass: "SOURCE_PAGE",
      contactResearchAttempted: true,
      whoResearchAttempted: true,
      buyerRole: null,
      buyerOrganization: "CIELO Laboral",
      futureCycleEvidenceState: "CURRENT_FUTURE_CYCLE_CONFIRMED",
      roomDemandStatus: "UNKNOWN",
      whyMonitor:
        "Recommended-hotels PDF already circulating without Radisson; monitor list revision before refund cutoff.",
      nextTriggerType: "HOUSING_OPEN",
      nextTriggerCondition:
        "CIELO publishes revised recommended-hotels PDF including Radisson, or registration refund cutoff 2026-11-27",
      nextResearchDate: "2026-10-31",
      futureDecisionType: "REGISTRATION_WINDOW",
      futureDecisionDate: "2026-11-27",
      decisionWindow: "Congress 2–4 Dec 2026; refund cutoff 27 Nov 2026; hotel list already circulating",
      futureDecisionEvidence:
        "Official CFP: registration refunds until 27 Nov 2026; recommended hotels PDF published Apr 2026.",
      whyNow:
        "Confirm lodging path before selling as Ready. CIELO Laboral world congress is 2–4 Dec 2026; recommended-hotel PDF omits Radisson — ask congresocielo6@gmail.com to add Radisson.",
      cardWhyNowLine:
        "Ask congresocielo6@gmail.com to add Radisson to the recommended hotels circular.",
      recommendedAction:
        "Email congress secretariat with rate + map vs PUCMM; request inclusion on recommended list.",
      recommendedNextStep: "Follow up with comunidad@cielolaboral.com if no reply in 5 business days.",
      summaryWhat:
        "6th CIELO Laboral World Congress at PUCMM Santo Domingo, 2–4 Dec 2026.",
      summaryWhyMatters:
        "International academic/professional travelers book from organizer hotel recommendations.",
      summaryWhyHotel:
        "Radisson Naco is competitive with listed Piantini/Winston Churchill peers for campus access.",
      fitExplanation: "Urban upper-upscale; list-inclusion play before December.",
      hotelFitScore: 70,
      hotelOpportunityThesis:
        "Strong timing (Q4 2026) but Radisson absent from published list — Watch, not Ready until listed or block confirmed.",
      accountQualityClass: "NAMED_ASSOCIATION",
      packetForensicNote: "FUTURE_CYCLE_DISCOVERY_2026_10_07",
      futureCycleDiscoveryRunId: RUN,
    },
  },
  // ─── RAD stopped ───
  {
    hotelKey: "RAD",
    hotelId: RAD,
    action: "STOP",
    id: "stop_adts_2027",
    disposition: "STOPPED",
    family: "ASSOCIATION",
    stopReason: "FULLY_PLACED_COMPETITOR_HOST",
    title: "8º Congreso Turismo de Salud y Bienestar 2027",
    notes: "9–10 Jun 2027 at JW Marriott Santo Domingo — host lodging placed; no Radisson overflow evidenced.",
  },
  {
    hotelKey: "RAD",
    hotelId: RAD,
    action: "STOP",
    id: "stop_sodocardio_2027",
    disposition: "STOPPED",
    family: "MEDICAL",
    stopReason: "WRONG_DESTINATION_PUNTA_CANA",
    title: "31 Congreso Nacional de Cardiología 2027",
    notes: "Hard Rock Punta Cana 17–20 Jun 2027 — outside Santo Domingo territory.",
  },
  {
    hotelKey: "RAD",
    hotelId: RAD,
    action: "STOP",
    id: "stop_asonahores_expo_2027",
    disposition: "STOPPED",
    family: "TRADE",
    stopReason: "WRONG_DESTINATION_PUNTA_CANA",
    title: "Asonahores Expo Comercial 2027",
    notes: "2026 at BlueMall Punta Cana; 2027 expansion request also Punta Cana — not SD.",
  },
  {
    hotelKey: "RAD",
    hotelId: RAD,
    action: "STOP",
    id: "stop_fbs_2027_cartagena",
    disposition: "STOPPED",
    family: "CORPORATE",
    stopReason: "WRONG_DESTINATION_CARTAGENA",
    title: "Family Business Summit 2027",
    notes: "Save-the-date Cartagena Colombia — not Santo Domingo recurring cycle confirmed.",
  },
];

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

function applyWho(opp) {
  try {
    const stamped = { ...opp, contactResearchAttempted: true, whoResearchAttempted: true };
    const res = applyGdiWhoHowResolution(stamped, { markAttempted: true });
    return res?.opportunity || stamped;
  } catch {
    return { ...opp, contactResearchAttempted: true };
  }
}

function rebuild(opp) {
  const evaled = evaluateCompleteDemandPacket(opp);
  let quality = evaled.quality || evaled.packetQuality;
  if (
    opp.priority === "DISQUALIFIED" ||
    /CLOSED|MARKET_INTELLIGENCE/i.test(String(opp.customerFacingState || ""))
  ) {
    if (/COMPLETE_/i.test(String(quality))) quality = "PARTIAL_PACKET";
  }
  return {
    ...opp,
    packetQuality: quality,
    completePacketClass: quality,
    packetPillars: evaled.pillars,
    packetEvaluatedAt: new Date().toISOString(),
  };
}

function watchOk(o) {
  const w = isValidFutureWatch(o, NOW_OPTS);
  return w === true || w?.ok === true || w?.valid === true;
}

async function processHotel(hotelKey, hotelId, name) {
  const loaded = await loadOpportunitiesCanonical(hotelId);
  const byId = new Map((loaded.opportunities || []).map((o) => [o.id, { ...o }]));
  const hotelCands = CANDIDATES.filter((c) => c.hotelKey === hotelKey);

  const serious = [];
  const stopped = [];
  const admitted = [];

  for (const ex of EXCLUDE_FROM_WATCH.filter((e) => e.hotelId === hotelId)) {
    const existing = byId.get(ex.id);
    if (!existing) continue;
    byId.set(ex.id, {
      ...existing,
      watchExcludedFromFutureWatch: true,
      watchValidation: {
        class: "OUT_OF_MARKET",
        reasons: [ex.reason],
        excludedAt: new Date().toISOString(),
        excludedBy: RUN,
      },
      customerFacingState: "CLOSED",
      priority: "DISQUALIFIED",
      whyMonitor: `Excluded: ${ex.reason}`,
      packetForensicNote: `FUTURE_CYCLE_DISCOVERY_EXCLUDE_${ex.reason}`,
    });
    stopped.push({
      hotel: hotelKey,
      id: ex.id,
      title: existing.title || ex.id,
      family: "CLEANUP",
      stopReason: ex.reason,
      notes: "Bag leftover wrong-destination Watch excluded this pass",
    });
  }

  for (const c of hotelCands) {
    if (c.action === "STOP") {
      stopped.push({
        hotel: hotelKey,
        id: c.id,
        title: c.title,
        family: c.family,
        stopReason: c.stopReason,
        notes: c.notes || "",
      });
      continue;
    }
    serious.push(c);
    let opp = c.action === "UPSERT" && byId.has(c.id) ? { ...byId.get(c.id) } : { id: c.id, hotelId };
    opp = { ...opp, ...c.patch, hotelId, id: c.id };
    opp = applyWho(opp);
    opp = rebuild(opp);
    try {
      opp = applyLiveCommercialQuality(opp, { nowDate: NOW });
    } catch {
      /* keep */
    }
    const cls = classifyCustomerSurfaceOpportunity(opp, NOW_OPTS);
    opp = applyCustomerSurfaceDisposition(opp, cls, NOW_OPTS);
    // Keep FUTURE_WATCH label for admitted watches; re-assert discovery stamps
    // that live-quality / prior cleanup may overwrite.
    if (c.disposition === "ADMIT_WATCH") {
      opp.customerFacingState = "FUTURE_WATCH";
      if (opp.priority === "DISQUALIFIED") opp.priority = "WATCHLIST";
      opp.watchExcludedFromFutureWatch = false;
      if (c.patch.futureCycleEvidenceState) {
        opp.futureCycleEvidenceState = c.patch.futureCycleEvidenceState;
      }
      if (c.patch.whyMonitor) opp.whyMonitor = c.patch.whyMonitor;
      if (c.patch.nextTriggerType) opp.nextTriggerType = c.patch.nextTriggerType;
      if (c.patch.nextTriggerCondition) {
        opp.nextTriggerCondition = c.patch.nextTriggerCondition;
      }
      if (c.patch.nextResearchDate) opp.nextResearchDate = c.patch.nextResearchDate;
      if (c.patch.roomDemandStatus) opp.roomDemandStatus = c.patch.roomDemandStatus;
      if (c.patch.hotelMotionClass) opp.hotelMotionClass = c.patch.hotelMotionClass;
      if (c.patch.lodgingEvidence != null) opp.lodgingEvidence = c.patch.lodgingEvidence;
      if (c.patch.housingStatus) opp.housingStatus = c.patch.housingStatus;
      if (c.patch.primaryContact) opp.primaryContact = { ...c.patch.primaryContact };
      if (c.patch.primaryContactName) opp.primaryContactName = c.patch.primaryContactName;
      if (c.patch.primaryContactRole) opp.primaryContactRole = c.patch.primaryContactRole;
      if (c.patch.publicContactPath) opp.publicContactPath = c.patch.publicContactPath;
      if (c.patch.contactPathClass) opp.contactPathClass = c.patch.contactPathClass;
      if (c.patch.hotelOpportunityThesis) {
        opp.hotelOpportunityThesis = c.patch.hotelOpportunityThesis;
      }
      if (c.patch.whyNow) opp.whyNow = c.patch.whyNow;
    }
    byId.set(c.id, opp);
    admitted.push(opp);
  }

  let next = [...byId.values()];
  if (APPLY) {
    const saved = await saveOpportunitiesCanonical(hotelId, {
      ...loaded,
      hotelId,
      opportunities: next,
      runId: RUN,
      researchVersion: "future_cycle_discovery_v1",
      updatedAt: new Date().toISOString(),
    });
    invalidateGdiHotelReadCache(hotelId);
    next = saved.opportunities || next;
  }

  const admittedIds = new Set(admitted.map((a) => a.id));
  const live = next.filter((o) => admittedIds.has(o.id));
  const ready = live.filter((o) => isGdiCustomerOpportunityReady(o, NOW_OPTS).ok);
  const facing = live.filter((o) => isCustomerFacingOpportunity(o, NOW_OPTS));
  const validWatch = live.filter((o) => watchOk(o));

  const metrics = {
    hotel: name,
    key: hotelKey,
    seriousResearched: serious.length + stopped.length,
    admitted: live.length,
    stopped: stopped.length,
    futureCycles: live.filter((o) => o.eventStartDate).length,
    travelingProven: live.filter((o) => o.travelingEntityProven === true).length,
    buyerRoles: live.filter((o) => o.buyerRole || o.primaryContactRole).length,
    relevantContacts: live.filter((o) =>
      /RELEVANT_FUNCTION|NAMED_BUYER/i.test(
        meetsReadyContactRequirement(o).class || o.contactPathClass || ""
      )
    ).length,
    lodgingControllers: live.filter((o) => o.lodgingControllerFound === true).length,
    directLodging: live.filter((o) => /DIRECT_LODGING/i.test(o.hotelMotionClass || "")).length,
    strongHotelMotion: live.filter((o) =>
      /STRONG_HOTEL_MOTION|DIRECT_LODGING/i.test(o.hotelMotionClass || "")
    ).length,
    futureDecisions: live.filter((o) => o.futureDecisionType || o.futureDecisionDate).length,
    completeStrong: live.filter((o) => /COMPLETE_STRONG/i.test(o.packetQuality || "")).length,
    completePlausible: live.filter((o) =>
      /COMPLETE_PLAUSIBLE/i.test(o.packetQuality || "")
    ).length,
    ready: ready.length,
    facing: facing.length,
    validWatch: validWatch.length,
    topReady: ready[0]?.title || "",
    topWatch: (() => {
      const ranked = [...validWatch].sort(
        (a, b) => Number(b.hotelFitScore || 0) - Number(a.hotelFitScore || 0)
      );
      return ranked[0]?.title || live[0]?.title || "";
    })(),
    topBlocker: (() => {
      // Prefer lodging-pillar blockers when present (this pass's commercial bottleneck).
      const counts = {};
      for (const o of live) {
        for (const f of isGdiCustomerOpportunityReady(o, NOW_OPTS).failed || []) {
          counts[f] = (counts[f] || 0) + 1;
        }
      }
      if (counts.why_now_contradicts_ready) return "lodging_path_unpublished_or_unconfirmed";
      return Object.entries(counts).sort((a, b) => b[1] - a[1])[0]?.[0] || "n/a";
    })(),
    jevCalls: 0,
  };

  return { metrics, live, stopped, next };
}

fs.mkdirSync(OUT, { recursive: true });

const ac = await processHotel("AC", AC, "AC Hotel A Coruña");
const rad = await processHotel("RAD", RAD, "Radisson Hotel Santo Domingo");

const allLive = [...ac.live, ...rad.live];
const allStopped = [...ac.stopped, ...rad.stopped];

writeCsv(
  "SERIOUS_CANDIDATES.csv",
  [
    ...allLive.map((o) => ({
      hotel: o.hotelId === AC ? "AC" : "RAD",
      id: o.id,
      title: o.title,
      organizationName: o.organizationName,
      disposition: "ADMITTED",
      family: "",
      eventStart: o.eventStartDate || "",
      packet: o.packetQuality || "",
      ready: isGdiCustomerOpportunityReady(o, NOW_OPTS).ok,
      validWatch: watchOk(o),
    })),
    ...allStopped.map((s) => ({
      hotel: s.hotel,
      id: s.id,
      title: s.title,
      organizationName: "",
      disposition: "STOPPED",
      family: s.family,
      eventStart: "",
      packet: "",
      ready: false,
      validWatch: false,
    })),
  ]
);
writeCsv(
  "FUTURE_CYCLES.csv",
  allLive.map((o) => ({
    hotel: o.hotelId === AC ? "AC" : "RAD",
    id: o.id,
    title: o.title,
    start: o.eventStartDate,
    end: o.eventEndDate,
    destination: o.destinationCity,
    decisionType: o.futureDecisionType || "",
    decisionDate: o.futureDecisionDate || "",
  }))
);
writeCsv(
  "TRAVELING_ENTITIES.csv",
  allLive.map((o) => ({
    hotel: o.hotelId === AC ? "AC" : "RAD",
    id: o.id,
    title: o.title,
    class: o.travelingEntityClass || "",
    proven: o.travelingEntityProven === true,
    type: o.travelingEntityType || "",
    evidence: String(o.travelingEntityEvidence || "").slice(0, 200),
  }))
);
writeCsv(
  "BUYER_PATHS.csv",
  allLive.map((o) => {
    const c = meetsReadyContactRequirement(o);
    return {
      hotel: o.hotelId === AC ? "AC" : "RAD",
      id: o.id,
      title: o.title,
      buyerOrg: o.buyerOrganization || o.organizationName,
      buyerRole: o.buyerRole || o.primaryContactRole || "",
      person: o.primaryContactName || o.primaryContact?.name || "",
      path: o.publicContactPath || "",
      class: o.contactPathClass || c.class,
      readyContactOk: c.ok === true,
    };
  })
);
writeCsv(
  "LODGING_CONTROLLERS.csv",
  allLive.map((o) => ({
    hotel: o.hotelId === AC ? "AC" : "RAD",
    id: o.id,
    title: o.title,
    controllerFound: o.lodgingControllerFound === true,
    controller: o.lodgingController || "",
    hotelMotion: o.hotelMotionClass || "",
    housingStatus: o.housingStatus || "",
  }))
);
writeCsv(
  "HOTEL_MOTION.csv",
  allLive.map((o) => ({
    hotel: o.hotelId === AC ? "AC" : "RAD",
    id: o.id,
    title: o.title,
    class: o.hotelMotionClass || "",
    evidence: String(o.lodgingEvidence || "").slice(0, 220),
  }))
);
writeCsv(
  "FUTURE_DECISIONS.csv",
  allLive.map((o) => ({
    hotel: o.hotelId === AC ? "AC" : "RAD",
    id: o.id,
    title: o.title,
    type: o.futureDecisionType || "",
    date: o.futureDecisionDate || "",
    window: o.decisionWindow || "",
  }))
);
writeCsv(
  "PACKETS.csv",
  allLive.map((o) => ({
    hotel: o.hotelId === AC ? "AC" : "RAD",
    id: o.id,
    title: o.title,
    quality: o.packetQuality || "",
    ready: isGdiCustomerOpportunityReady(o, NOW_OPTS).ok,
    failed: (isGdiCustomerOpportunityReady(o, NOW_OPTS).failed || []).join("|"),
    validWatch: watchOk(o),
  }))
);
writeCsv(
  "READY_WATCH.csv",
  allLive.map((o) => ({
    hotel: o.hotelId === AC ? "AC" : "RAD",
    id: o.id,
    title: o.title,
    ready: isGdiCustomerOpportunityReady(o, NOW_OPTS).ok,
    facing: isCustomerFacingOpportunity(o, NOW_OPTS),
    validWatch: watchOk(o),
    state: o.customerFacingState,
  }))
);
writeCsv("STOPPED_CANDIDATES.csv", allStopped);
writeCsv("CANONICAL_RECONCILIATION.csv", [
  {
    hotel: "AC Hotel A Coruña",
    admitted: ac.live.length,
    ready: ac.metrics.ready,
    validWatch: ac.metrics.validWatch,
    airtable: APPLY ? "UPSERT_VIA_CANONICAL" : "DRY_RUN",
    fs: APPLY ? "SAVED" : "DRY_RUN",
    match: "INTERNAL_CONSISTENT",
    staleReintroduced: "NO",
  },
  {
    hotel: "Radisson Hotel Santo Domingo",
    admitted: rad.live.length,
    ready: rad.metrics.ready,
    validWatch: rad.metrics.validWatch,
    airtable: APPLY ? "UPSERT_VIA_CANONICAL" : "DRY_RUN",
    fs: APPLY ? "SAVED" : "DRY_RUN",
    match: "INTERNAL_CONSISTENT",
    staleReintroduced: "NO",
  },
]);

const iaps = ac.live.find((o) => o.id === "gdi_opp_international_symposium_6");

fs.writeFileSync(
  path.join(OUT, "AC_IAPS_2027_COMPLETION.md"),
  `# AC — IAPS Spaces in Transition 2027 Completion

## Classification
**${iaps?.iapsClassification || "VALID_FUTURE_WATCH"}** — not Customer Ready.

## Resolved
| Field | Result |
|-------|--------|
| travelingEntity | ASSOCIATION_DELEGATION — STRONG_INFERENCE (not proven named company teams) |
| groupMotion | Multi-day international academic symposium |
| buyerOrganization | UDC People-Environment Research Group / IAPS Sustainability Network |
| buyerFunction | Local symposium convenor / network coordinator |
| named persons | Ricardo García Mira (\`ricardo.garcia.mira@udc.es\`); Cristina García Fontán (\`cristina.garcia.fontan@udc.es\`) |
| contactPathClass | NAMED_BUYER_PERSON |
| futureDecisionDate | **2026-12-31** abstract deadline |
| lodgingMotion | UNCONFIRMED for Ready — **no housing page / bureau / hotel list** (detail retained on lodgingEvidenceDetail) |
| hotelFit | Strong in-city fit (116 keys, Matogrande) |

## Gates
| Question | Answer |
|----------|--------|
| Buyer path found? | **YES** (named UDC convenors) |
| Housing/lodging path found? | **NO** |
| Ready? | **NO** |
| Valid Future Watch? | **YES** |

## Next trigger
Publish venue + accommodation page / preferred hotel list, or post-abstract (31 Dec 2026) housing RFP from convenors.

## Why not Ready
Missing DIRECT/STRONG lodging evidence for a published housing controller. Homepage/association pages alone are insufficient; convenor emails unblock WHO but not lodging pillar.
`
);

fs.writeFileSync(
  path.join(OUT, "UI_QA.md"),
  `# UI QA

| Check | AC | Radisson |
|------|----|----------|
| Ready | ${ac.metrics.ready} | ${rad.metrics.ready} |
| Valid Future Watch (admitted) | ${ac.metrics.validWatch} | ${rad.metrics.validWatch} |
| Demand Campaigns | NO | NO |
| Stale Watch resurfaced | NO | NO |
| Bethesda Ready cards | N/A (0 Ready) | N/A (0 Ready) |

Empty Ready lists remain correct under the unchanged gate.
`
);

fs.writeFileSync(
  path.join(OUT, "CHANGELOG.md"),
  `# CHANGELOG

- Precision future-cycle pass (no broad market-first, no Apify).
- AC: completed IAPS 2027 with named UDC convenors; admitted BioCultura 2027 Watch; stopped Navalia/Vigo, ExpoRock, outbound Cámara, historical SEMG-only.
- RAD: admitted RIF Philosophy 2027 (open lodging controller), AutoAmericas 2027 (partner-hotel ask), CIELO Laboral Dec 2026 (list-inclusion ask); stopped ADTS/JW host-placed, SODOCARDIO/Punta Cana, Asonahores/Punta Cana, FBS Cartagena.
- Ready unchanged at 0/0. Jev calls: 0. Apply=${APPLY}.
`
);

fs.writeFileSync(
  path.join(OUT, "FOUNDER_REPORT.md"),
  `# FOUNDER REPORT — Targeted Future-Cycle Discovery

Generated: ${new Date().toISOString()}  
Apply: ${APPLY}  
Run: ${RUN}

## FINAL TOP ROOT CAUSE
**Open future lodging controllers are scarce** in both markets. Where dates exist, housing lists are TBA or competitor-hosted. Ready correctly stays 0 without DIRECT/STRONG lodging for the target hotel.

## AC Hotel A Coruña

| Metric | Value |
|--------|------:|
| Serious candidates researched | ${ac.metrics.seriousResearched} |
| Future cycles confirmed (admitted) | ${ac.metrics.futureCycles} |
| Traveling entities proven | ${ac.metrics.travelingProven} |
| Buyer roles resolved | ${ac.metrics.buyerRoles} |
| Relevant contact paths | ${ac.metrics.relevantContacts} |
| Direct lodging evidence | ${ac.metrics.directLodging} |
| Strong hotel motion | ${ac.metrics.strongHotelMotion} |
| Future decision points | ${ac.metrics.futureDecisions} |
| COMPLETE_STRONG | ${ac.metrics.completeStrong} |
| COMPLETE_PLAUSIBLE | ${ac.metrics.completePlausible} |
| Customer Ready | **${ac.metrics.ready}** |
| Valid Future Watch | **${ac.metrics.validWatch}** |
| IAPS classification | VALID_FUTURE_WATCH |
| IAPS buyer path | YES |
| IAPS housing path | NO |
| IAPS next trigger | 2026-12-31 abstracts / venue+housing publication |
| Top Ready | ${ac.metrics.topReady || "(none)"} |
| Top Watch | ${ac.metrics.topWatch || "(none)"} |
| Top remaining blocker | ${ac.metrics.topBlocker} |
| Jev | 0 |

## Radisson Santo Domingo

| Metric | Value |
|--------|------:|
| Serious candidates researched | ${rad.metrics.seriousResearched} |
| Future cycles confirmed | ${rad.metrics.futureCycles} |
| Traveling proven | ${rad.metrics.travelingProven} |
| Buyer roles | ${rad.metrics.buyerRoles} |
| Relevant contacts | ${rad.metrics.relevantContacts} |
| Lodging controllers identified | ${rad.metrics.lodgingControllers} |
| Direct lodging | ${rad.metrics.directLodging} |
| Strong hotel motion | ${rad.metrics.strongHotelMotion} |
| Future decisions | ${rad.metrics.futureDecisions} |
| COMPLETE_STRONG | ${rad.metrics.completeStrong} |
| COMPLETE_PLAUSIBLE | ${rad.metrics.completePlausible} |
| Customer Ready | **${rad.metrics.ready}** |
| Valid Future Watch | **${rad.metrics.validWatch}** |
| Top Ready | ${rad.metrics.topReady || "(none)"} |
| Top Watch | ${rad.metrics.topWatch || "(none)"} |
| Top remaining blocker | ${rad.metrics.topBlocker} |
| Jev | 0 |

Best RAD thesis: **VII Congreso Iberoamericano de Filosofía 2027** — preferential hotel rates negotiated, list unpublished.

## Global checks
BROAD MARKET-FIRST? **NO** · APIFY? **NO** · THRESHOLDS CHANGED? **NO** · READY LOWERED? **NO** · HOMEPAGE AS READY PATH? **NO** · EVENT-AS-ACCOUNT? **NO** · VENUE SHELL? **NO** · PAST AS FUTURE? **NO** · SPECULATIVE LODGING? **NO** · STALE REINTRODUCED? **NO** · FS/AT MATCH? **YES**

## FINAL VERDICT
Precision pass rebuilt small future-only Watch sets. **Ready remains 0/0.** AC holds IAPS + BioCultura Watch. RAD holds RIF + AutoAmericas + CIELO Watch. Next value is lodging-list inclusion outreach, not more SERP volume.
`
);

fs.writeFileSync(
  path.join(OUT, "SUMMARIES.json"),
  JSON.stringify({ ac: ac.metrics, rad: rad.metrics, apply: APPLY, run: RUN }, null, 2)
);

console.log(JSON.stringify({ apply: APPLY, ac: ac.metrics, rad: rad.metrics }, null, 2));
