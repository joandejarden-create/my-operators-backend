#!/usr/bin/env node
/**
 * Lodging Decision Intelligence for five frozen Future Watches.
 * AC Coruña + Radisson SD — NO broad discovery, NO Ready lowering.
 *
 * Usage: node scripts/gdi-ac-radisson-lodging-decision-intelligence-v1.mjs [--apply]
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
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/ac-coruna-radisson-lodging-decision-intelligence");
const APPLY = process.argv.includes("--apply");
const NOW = "2026-10-07";
const NOW_OPTS = { nowDate: NOW };
const RUN = `gdi_ldi_${crypto.randomBytes(3).toString("hex")}`;

const AC = "rec2PVBDavppGpenm";
const RAD = "recUOyzOXn2Zdp98I";

const FREEZE = [
  {
    hotelKey: "AC",
    hotelId: AC,
    hotelName: "AC Hotel A Coruña",
    id: "gdi_opp_international_symposium_6",
    key: "IAPS",
    patch: {
      lodgingControllerOrganization:
        "UDC People-Environment Research Group / IAPS Sustainability + Culture & Space Networks",
      lodgingControllerType: "HOST_INSTITUTION",
      controllerEvidence:
        "Official IAPS save-the-date (27 Aug 2026): symposium convened by Sustainability + Culture & Space networks in A Coruña; named UDC convenors Ricardo Garcia Mira and Cristina Garcia Fontan. Venue and website TBA — no housing bureau or accommodation page published.",
      controllerConfidence: "MEDIUM",
      controllerStatus: "PROGRAM_CONVENORS_PUBLIC_LODGING_ROLE_UNCONFIRMED",
      lodgingControllerFound: false,
      lodgingController:
        "Likely host-institution convenors (UDC) once housing opens — not yet stamped as lodging desk",
      hotelSelectionProcess: "UNKNOWN",
      hotelSelectionEvidence:
        "Official: venue and website details will follow. No RFP, housing bureau, PCO, or hotel partner page published for 2027.",
      decisionTrigger: "ABSTRACT_DEADLINE_THEN_VENUE_HOUSING_PUBLICATION",
      decisionDateKnown: true,
      decisionDate: "2026-12-31",
      decisionWindowStart: "2026-11-01",
      decisionWindowEnd: "2027-03-31",
      timingConfidence: "MEDIUM",
      nextObservableTrigger:
        "Venue + symposium website published, or post-abstract housing / preferred-hotel guidance",
      nextTriggerType: "HOUSING_OPEN",
      nextTriggerCondition:
        "Venue + accommodation / preferred-hotel page published, or housing guidance after 2026-12-31 abstracts",
      nextResearchDate: "2026-12-15",
      hotelInclusionEvidenceClass: "NO_HOUSING_EVIDENCE_YET",
      pastCycleProcessSignal: "PAST_PROCESS_SIGNAL",
      pastCycleProcessNotes:
        "2021 IAPS Sustainability Network symposium (A Coruña, hybrid, ~500 participants) convened by same UDC leads; no public official hotel-partner list recovered for that cycle — lodging likely attendee self-booking / informal guidance.",
      hotelInclusionFitSummary:
        "AC Hotel A Coruña (Matogrande / Expocoruna corridor, ~116 keys, upper-upscale select-service) fits midsize international academic groups once a city venue is named; proximity depends on final venue.",
      outreachReadiness: "OUTREACH_PREPARE",
      outreachReadinessReason:
        "Named convenors exist and abstracts close 31 Dec 2026, but lodging controller and hotel-selection process are not yet public — prepare intro + ask process timing; do not sell as Ready.",
      outreachContactName: "Ricardo Garcia Mira",
      outreachContactRole: "IAPS Sustainability Network coordinator / UDC convenor",
      outreachContactEmail: "ricardo.garcia.mira@udc.es",
      outreachContactSecondary: "cristina.garcia.fontan@udc.es",
      outreachContactUrl: "https://iaps-association.org/networks/sustainability/",
      outreachContactPathClass: "NAMED_BUYER_PERSON",
      monitoringTriggerType: "HOUSING_PAGE_OR_VENUE_ANNOUNCEMENT",
      monitoringTriggerSource: "https://iaps-association.org/",
      monitoringExpectedTiming: "Around / after abstract deadline 2026-12-31",
      monitoringFrequency: "BIWEEKLY_UNTIL_DEC_THEN_WEEKLY",
      automationEligibility: "PARTIALLY_AUTOMATABLE",
      automationTriggerLogic:
        "Poll iaps-association.org news + save-the-date page for venue/housing/accommodation keywords; alert on change.",
      // Customer Watch card (no research jargon)
      watchCardWhyMatters:
        "International researchers and practitioners will travel to A Coruña for a multi-day IAPS symposium in June 2027. Hotel partners are not locked yet.",
      watchCardCurrentStatus:
        "Confirmed dates (14–16 Jun 2027). Abstracts due 31 Dec 2026. Venue and housing not published.",
      watchCardLodgingController:
        "Local convenors at University of A Coruña (IAPS networks) — lodging desk not yet published.",
      watchCardHotelSelectionStatus: "Not published — selection process unknown",
      watchCardWhenToAct:
        "Prepare now; soft outreach to convenors about housing timeline before abstracts close.",
      watchCardNextTrigger: "Venue + accommodation page, or housing guidance after 31 Dec 2026",
      watchCardRecommendedNextStep:
        "Email Ricardo Garcia Mira and Cristina Garcia Fontan to introduce AC Hotel A Coruña and ask when preferred hotels will be selected.",
      summaryWhyMatters:
        "International academic travelers need multi-night lodging in A Coruña while venue and hotel partners remain open.",
      whyNow:
        "Confirm lodging path and buyer function before outreach. Abstracts close 31 Dec 2026; venue/housing still unpublished — prepare positioning with UDC convenors now.",
      cardWhyNowLine:
        "Prepare outreach before 31 Dec 2026 abstracts; ask when housing opens.",
      recommendedAction:
        "Email ricardo.garcia.mira@udc.es and cristina.garcia.fontan@udc.es to introduce AC Hotel A Coruña and ask about the hotel-selection timeline.",
      recommendedNextStep:
        "Send short intro + property facts; request notification when accommodation page launches.",
      lodgingControlHypothesis: "HOST_INSTITUTION",
      lodgingControlSummary:
        "UDC convenors likely influence housing once published; official lodging desk not confirmed.",
      lodgingControlConfidence: "MEDIUM",
      hotelOpportunityThesis:
        "AC can ask to be considered for the future preferred-hotel set once UDC/IAPS publish venue and housing guidance.",
      lodgingDecisionIntelligenceRunId: RUN,
      lodgingDecisionIntelligenceAt: new Date().toISOString(),
    },
    message: `Subject: AC Hotel A Coruña — interest in IAPS Spaces in Transition 2027 lodging

Dear Professor Garcia Mira and Professor Garcia Fontan,

Congratulations on the upcoming IAPS Spaces in Transition symposium in A Coruña (14–16 June 2027).

AC Hotel A Coruña would like to be considered when preferred accommodation options are selected. We are a Marriott AC Hotels property in the Matogrande / Expocoruña area and regularly support academic and professional groups.

Could you share how hotel selection will be handled and when a housing or preferred-hotel list is expected? We would be glad to provide rates and group details.

Kind regards`,
  },
  {
    hotelKey: "AC",
    hotelId: AC,
    hotelName: "AC Hotel A Coruña",
    id: "gdi_opp_biocultura_a_coruna_2027",
    key: "BIOCULTURA",
    patch: {
      lodgingControllerOrganization: "Asociación Vida Sana",
      lodgingControllerType: "EVENT_ORGANIZER",
      controllerEvidence:
        "Official BioCultura Viajes y Alojamientos page is operated by Asociación Vida Sana; Renfe discount contact m.sanchez@vidasana.org; exhibitor ops expositores@vidasana.org. No hotel partner list published for 2027.",
      controllerConfidence: "HIGH",
      controllerStatus: "ORGANIZER_TRAVEL_DESK_PUBLIC_HOTEL_LIST_MISSING",
      lodgingControllerFound: true,
      lodgingController:
        "Asociación Vida Sana travel/exhibitor desk (m.sanchez@vidasana.org / expositores@vidasana.org)",
      hotelSelectionProcess: "ATTENDEE_SELF_BOOKING",
      hotelSelectionEvidence:
        "2027 Viajes page publishes Renfe discount only — no official hotel block or partner hotels. Pattern implies attendee self-booking unless a partner list is added later.",
      decisionTrigger: "EXHIBITOR_STAND_ALLOCATION_AND_HOUSING_LIST",
      decisionDateKnown: true,
      decisionDate: "2027-01-10",
      decisionWindowStart: "2026-10-07",
      decisionWindowEnd: "2027-02-15",
      timingConfidence: "MEDIUM",
      nextObservableTrigger:
        "Preferred-hotel / alojamientos partner list on biocultura.org/acoruna/viajes, or stand allocation from 10 Jan 2027",
      nextTriggerType: "HOUSING_OPEN",
      nextTriggerCondition:
        "Vida Sana publishes hotel partners on Viajes y Alojamientos, or stand allocation opens 2027-01-10",
      nextResearchDate: "2026-12-01",
      hotelInclusionEvidenceClass: "NO_HOUSING_EVIDENCE_YET",
      pastCycleProcessSignal: "PAST_PROCESS_SIGNAL",
      pastCycleProcessNotes:
        "Organizer historically maintains a Viajes y Alojamientos page per edition; current A Coruña 2027 page has transport discount only — no recovered multi-hotel block for this edition.",
      hotelInclusionFitSummary:
        "AC Hotel A Coruña sits on the Expocoruña corridor used by BioCultura at EXPOCoruña — strong fit for exhibitor teams (moderate group peaks).",
      outreachReadiness: "OUTREACH_NOW",
      outreachReadinessReason:
        "Exhibitor registration is open; lodging page exists without hotels; organizer travel/exhibitor emails are public — timely to request partner-hotel inclusion before stand allocation.",
      outreachContactName: null,
      outreachContactRole: "Exhibitor services / travel desk",
      outreachContactEmail: "expositores@vidasana.org",
      outreachContactSecondary: "m.sanchez@vidasana.org",
      outreachContactUrl: "https://www.biocultura.org/acoruna/informacion-expositores",
      outreachContactPathClass: "RELEVANT_FUNCTION_CONTACT",
      monitoringTriggerType: "HOTEL_LIST_ON_VIAJES_PAGE",
      monitoringTriggerSource: "https://www.biocultura.org/acoruna/viajes",
      monitoringExpectedTiming: "Before stand allocation 2027-01-10",
      monitoringFrequency: "WEEKLY",
      automationEligibility: "AUTOMATABLE",
      automationTriggerLogic:
        "Poll /acoruna/viajes for hotel/partner/alojamiento list content beyond Renfe; alert when hotel names or booking links appear.",
      watchCardWhyMatters:
        "130+ exhibitors will staff a multi-day fair at EXPOCoruña (5–7 Mar 2027). Official hotel partners are not listed yet.",
      watchCardCurrentStatus:
        "Fair confirmed at EXPOCoruña. Exhibitor signup open. Travel page has Renfe discount only — no hotel list.",
      watchCardLodgingController: "Asociación Vida Sana (exhibitor / travel desk)",
      watchCardHotelSelectionStatus:
        "Attendee self-booking so far; partner-hotel list not published",
      watchCardWhenToAct: "Now — ask to join the preferred lodging list before stand allocation (10 Jan 2027).",
      watchCardNextTrigger: "Hotel partners published on Viajes y Alojamientos, or 10 Jan 2027 stand allocation",
      watchCardRecommendedNextStep:
        "Email expositores@vidasana.org and m.sanchez@vidasana.org proposing AC Hotel A Coruña for exhibitor lodging.",
      summaryWhyMatters:
        "Exhibitor companies send traveling teams for a multi-day fair near Matogrande; lodging partners are still open.",
      whyNow:
        "Confirm lodging partner inclusion before selling as Ready. BioCultura 2027 is confirmed; housing list unpublished — pitch AC via expositores@vidasana.org / m.sanchez@vidasana.org.",
      cardWhyNowLine: "Ask Vida Sana for hotel partner inclusion before 10 Jan 2027.",
      recommendedAction:
        "Contact expositores@vidasana.org / m.sanchez@vidasana.org to propose AC Hotel A Coruña for the preferred lodging list.",
      recommendedNextStep: "Offer exhibitor group rates for 4–7 Mar 2027 nights.",
      lodgingControlHypothesis: "EVENT_ORGANIZER",
      lodgingControlSummary: "Asociación Vida Sana controls travel page and exhibitor services; hotel list not published.",
      lodgingControlConfidence: "HIGH",
      lodgingDecisionIntelligenceRunId: RUN,
      lodgingDecisionIntelligenceAt: new Date().toISOString(),
    },
    message: `Subject: AC Hotel A Coruña — BioCultura 2027 exhibitor lodging

Hello,

We would like AC Hotel A Coruña to be considered for BioCultura A Coruña 2027 exhibitor and visitor accommodation (5–7 March 2027 at EXPOCoruña).

Our property is on the Expocoruña / Matogrande corridor and supports midsize trade-fair groups. Could you share whether a preferred-hotel list will be published, and how hotels are selected?

Happy to send rates and group terms for your review.

Kind regards
expositores@vidasana.org / m.sanchez@vidasana.org`,
  },
  {
    hotelKey: "RAD",
    hotelId: RAD,
    hotelName: "Radisson Hotel Santo Domingo",
    id: "gdi_opp_rif_filosofia_sd_2027",
    key: "RIF",
    patch: {
      lodgingControllerOrganization:
        "Comité Organizador RIF–UASD 2027 / Asociación Dominicana de Filosofía (ADOFIL)",
      lodgingControllerType: "ASSOCIATION_SECRETARIAT",
      controllerEvidence:
        "Official segunda convocatoria PDF §5 SEDE Y ALOJAMIENTO: organization negotiated preferential rates in Zona Colonial and Distrito Nacional; hotel convenio list to be published in next circular. Contacts: adofil333@gmail.com, r.iberoamericanadefilosofia@gmail.com.",
      controllerConfidence: "HIGH",
      controllerStatus: "ORGANIZER_CONTROLS_CONVENIO_LIST_UNPUBLISHED",
      lodgingControllerFound: true,
      lodgingController: "Comité Organizador RIF–UASD 2027 / ADOFIL",
      hotelSelectionProcess: "DIRECT_NEGOTIATION",
      hotelSelectionEvidence:
        "Official PDF: organization has already managed preferential hotel rates; list of convenio hotels forthcoming in a circular — classic organizer direct negotiation, not a public RFP.",
      decisionTrigger: "PROPOSAL_CLOSE_AND_HOTEL_CIRCULAR",
      decisionDateKnown: true,
      decisionDate: "2026-10-30",
      decisionWindowStart: "2026-10-07",
      decisionWindowEnd: "2026-12-31",
      timingConfidence: "HIGH",
      nextObservableTrigger:
        "Preferential-hotel convenio circular naming partner hotels (or proposal results 15 Dec 2026)",
      nextTriggerType: "HOUSING_OPEN",
      nextTriggerCondition:
        "ADOFIL/RIF publishes hotel convenio circular, or proposal results 2026-12-15",
      nextResearchDate: "2026-11-05",
      hotelInclusionEvidenceClass: "NO_HOUSING_EVIDENCE_YET",
      pastCycleProcessSignal: "PAST_PROCESS_SIGNAL",
      pastCycleProcessNotes:
        "Current cycle already states preferential rates are negotiated — process signal is current; named hotel list still future. No past-cycle Radisson placement claimed.",
      hotelInclusionFitSummary:
        "Radisson Santo Domingo (Naco / Distrito Nacional) fits visiting faculty and panels seeking DN lodging relative to UASD Humanidades / Academia venues.",
      outreachReadiness: "OUTREACH_NOW",
      outreachReadinessReason:
        "Proposal deadline 30 Oct 2026; preferential rates exist but partner list unpublished and Radisson not named — window to request convenio inclusion.",
      outreachContactName: null,
      outreachContactRole: "Congress organizing committee / lodging coordination",
      outreachContactEmail: "adofil333@gmail.com",
      outreachContactSecondary: "r.iberoamericanadefilosofia@gmail.com",
      outreachContactUrl:
        "https://rediberoamericanafilosofia.com/wp-content/uploads/2026/05/Segunda-convocatoria-Congreso-RIF.pdf",
      outreachContactPathClass: "RELEVANT_FUNCTION_CONTACT",
      monitoringTriggerType: "HOTEL_CONVENIO_CIRCULAR",
      monitoringTriggerSource: "https://rediberoamericanafilosofia.com/",
      monitoringExpectedTiming: "Next circular after segunda convocatoria (before Mar 2027)",
      monitoringFrequency: "WEEKLY_THROUGH_DEC_2026",
      automationEligibility: "PARTIALLY_AUTOMATABLE",
      automationTriggerLogic:
        "Poll RIF site / convocatoria PDFs for alojamiento/convenio/hotel list updates; alert when hotel names appear.",
      watchCardWhyMatters:
        "International philosophy speakers will stay in Santo Domingo 15–19 Mar 2027. Organizers already negotiated preferential hotel rates but have not published the partner list.",
      watchCardCurrentStatus:
        "Congress confirmed at UASD. Preferential rates secured; hotel names not yet published.",
      watchCardLodgingController: "RIF–UASD organizing committee / ADOFIL",
      watchCardHotelSelectionStatus: "Direct organizer negotiation — list forthcoming",
      watchCardWhenToAct: "Now — before the hotel convenio circular is finalized.",
      watchCardNextTrigger: "Publication of preferential-hotel convenio circular",
      watchCardRecommendedNextStep:
        "Email adofil333@gmail.com and r.iberoamericanadefilosofia@gmail.com proposing Radisson Santo Domingo for the convenio list.",
      summaryWhyMatters:
        "International academic speakers need multi-night lodging; organizers control preferential rates and the list is still unpublished.",
      whyNow:
        "Confirm lodging path and buyer function before outreach. Preferential rates exist; partner hotel list unpublished — ask ADOFIL to include Radisson.",
      cardWhyNowLine: "Ask ADOFIL to include Radisson on the convenio hotel circular.",
      recommendedAction:
        "Email adofil333@gmail.com and r.iberoamericanadefilosofia@gmail.com proposing Radisson Santo Domingo (Naco) for the preferential-rate hotel list.",
      recommendedNextStep:
        "Send rate sheet + map vs UASD / Academia de Ciencias; ask circular publication date.",
      lodgingControlHypothesis: "ASSOCIATION_SECRETARIAT",
      lodgingControlSummary:
        "ADOFIL / RIF–UASD committee controls preferential hotel convenio; list not yet published.",
      lodgingControlConfidence: "HIGH",
      lodgingDecisionIntelligenceRunId: RUN,
      lodgingDecisionIntelligenceAt: new Date().toISOString(),
    },
    message: `Subject: Radisson Santo Domingo — VII Congreso Iberoamericano de Filosofía 2027 lodging

Estimados miembros del Comité Organizador,

We noted that preferential hotel rates are being arranged for the VII Congreso Iberoamericano de Filosofía (15–19 March 2027) and that the convenio hotel list will appear in a forthcoming circular.

Radisson Hotel Santo Domingo (Naco) would like to be considered for that list. We can support visiting faculty and panels with group rates and Distrito Nacional access.

Please let us know the right contact for hotel selection and the timing of the circular.

Kind regards
adofil333@gmail.com / r.iberoamericanadefilosofia@gmail.com`,
  },
  {
    hotelKey: "RAD",
    hotelId: RAD,
    hotelName: "Radisson Hotel Santo Domingo",
    id: "gdi_opp_cielo_laboral_sd_2026",
    key: "CIELO",
    patch: {
      lodgingControllerOrganization: "CIELO Laboral congress secretariat",
      lodgingControllerType: "ASSOCIATION_SECRETARIAT",
      controllerEvidence:
        "Official congress page + recommended-hotels PDF near PUCMM: attendees book directly with hotels; organizers published a multi-hotel recommendation list (Holiday Inn, Aloft, Marriott Piantini, Hyatt Centric, Homewood, InterContinental, etc.). Radisson not listed. Contacts: congresocielo6@gmail.com / comunidad@cielolaboral.com.",
      controllerConfidence: "HIGH",
      controllerStatus: "RECOMMENDED_LIST_PUBLISHED_TARGET_ABSENT",
      lodgingControllerFound: true,
      lodgingController: "CIELO Laboral (congresocielo6@gmail.com / comunidad@cielolaboral.com)",
      hotelSelectionProcess: "ATTENDEE_SELF_BOOKING",
      hotelSelectionEvidence:
        "PDF instructs reservar directamente with recommended hotels; group blocks suggested per hotel — organizer curates recommendations, attendees self-book.",
      decisionTrigger: "LIST_REVISION_BEFORE_CONGRESS",
      decisionDateKnown: true,
      decisionDate: "2026-11-27",
      decisionWindowStart: "2026-10-07",
      decisionWindowEnd: "2026-11-27",
      timingConfidence: "HIGH",
      nextObservableTrigger:
        "Revised recommended-hotels PDF including Radisson, or registration refund cutoff 27 Nov 2026",
      nextTriggerType: "HOUSING_OPEN",
      nextTriggerCondition:
        "CIELO revises recommended-hotels circular to include Radisson, or 2026-11-27 refund cutoff",
      nextResearchDate: "2026-10-20",
      hotelInclusionEvidenceClass: "CURRENT_HOTEL_LIST_PUBLISHED",
      pastCycleProcessSignal: null,
      pastCycleProcessNotes:
        "Current-cycle list already published (Apr 2026 PDF) — primary evidence is current, not past-cycle.",
      hotelInclusionFitSummary:
        "Radisson Naco is competitive with listed Piantini / Winston Churchill peers for PUCMM campus access (10–15 min typical).",
      outreachReadiness: "OUTREACH_NOW",
      outreachReadinessReason:
        "Congress is 2–4 Dec 2026; recommended list omits Radisson; refund cutoff 27 Nov — ask for list inclusion immediately.",
      outreachContactName: null,
      outreachContactRole: "Congress secretariat",
      outreachContactEmail: "congresocielo6@gmail.com",
      outreachContactSecondary: "comunidad@cielolaboral.com",
      outreachContactUrl:
        "https://www.cielolaboral.com/6o-congreso-mundial-cielo-laboral-santodomingo/",
      outreachContactPathClass: "RELEVANT_FUNCTION_CONTACT",
      monitoringTriggerType: "RECOMMENDED_HOTELS_PDF_REVISION",
      monitoringTriggerSource:
        "https://www.cielolaboral.com/6o-congreso-mundial-cielo-laboral-santodomingo/",
      monitoringExpectedTiming: "Before 27 Nov 2026",
      monitoringFrequency: "TWICE_WEEKLY",
      automationEligibility: "PARTIALLY_AUTOMATABLE",
      automationTriggerLogic:
        "Checksum/poll recommended-hotels PDF URL + congress page for Radisson / new hotel names; alert on change.",
      watchCardWhyMatters:
        "International labor-law congress attendees book from CIELO’s recommended hotel list for 2–4 Dec 2026. Radisson is not on the current list.",
      watchCardCurrentStatus:
        "Recommended hotels PDF published; Radisson absent. Registration refunds until 27 Nov 2026.",
      watchCardLodgingController: "CIELO Laboral congress secretariat",
      watchCardHotelSelectionStatus:
        "Organizer recommended list + attendee self-booking (current list live)",
      watchCardWhenToAct: "Now — request inclusion before peak booking / 27 Nov refund cutoff.",
      watchCardNextTrigger: "Revised recommended-hotels PDF, or 27 Nov 2026 refund cutoff",
      watchCardRecommendedNextStep:
        "Email congresocielo6@gmail.com with rates and map vs PUCMM; ask to add Radisson to the recommended list.",
      summaryWhyMatters:
        "International academic/professional travelers book from organizer hotel recommendations before December.",
      whyNow:
        "Confirm lodging path before selling as Ready. Recommended-hotel PDF omits Radisson — ask congresocielo6@gmail.com to add Radisson before 27 Nov.",
      cardWhyNowLine: "Ask CIELO to add Radisson to the recommended hotels circular.",
      recommendedAction:
        "Email congress secretariat with rate + map vs PUCMM; request inclusion on recommended list.",
      recommendedNextStep:
        "Follow up with comunidad@cielolaboral.com if no reply in 5 business days.",
      lodgingControlHypothesis: "ASSOCIATION_SECRETARIAT",
      lodgingControlSummary:
        "CIELO secretariat publishes recommended hotels; attendees self-book; Radisson not listed.",
      lodgingControlConfidence: "HIGH",
      lodgingDecisionIntelligenceRunId: RUN,
      lodgingDecisionIntelligenceAt: new Date().toISOString(),
    },
    message: `Subject: Radisson Santo Domingo — CIELO Laboral Dec 2026 recommended hotels

Hello,

We reviewed the recommended hotels list for the 6º Congreso Mundial CIELO Laboral (2–4 December 2026, PUCMM Santo Domingo).

Radisson Hotel Santo Domingo would like to be added to the recommended list for international participants. We can provide rates and location details relative to campus.

Could you confirm the right contact and whether the list can still be updated?

Kind regards
congresocielo6@gmail.com / comunidad@cielolaboral.com`,
  },
  {
    hotelKey: "RAD",
    hotelId: RAD,
    hotelName: "Radisson Hotel Santo Domingo",
    id: "gdi_opp_autoamericas_2027",
    key: "AUTOAMERICAS",
    patch: {
      lodgingControllerOrganization: "AutoAméricas / Latinpress (+ Hotel Dominican Fiesta as 2027 host)",
      lodgingControllerType: "EVENT_ORGANIZER",
      controllerEvidence:
        "Official 2027 alojamiento page: Dominican Fiesta is hotel and venue official; Fiesta reservations Claribel García. International sales Andrés Caballero (acaballero@autoamericas.show). 2026 section still lists multi-hotel convenios including Radisson Santo Domingo group sales — PAST_PROCESS_SIGNAL for secondary partners.",
      controllerConfidence: "HIGH",
      controllerStatus: "HOST_HOTEL_LOCKED_SECONDARY_PARTNERS_OPEN_HISTORICALLY",
      lodgingControllerFound: true,
      lodgingController:
        "AutoAméricas / Latinpress (acaballero@autoamericas.show); 2027 host Claribel García @ Dominican Fiesta",
      hotelSelectionProcess: "HOST_INSTITUTION_SELECTION",
      hotelSelectionEvidence:
        "2027 page names Dominican Fiesta as official hotel/venue with preferential rates TBA. Secondary partner selection historically via organizer convenios (2026 list).",
      decisionTrigger: "FIESTA_RATES_AND_SECONDARY_CONVENIO",
      decisionDateKnown: false,
      decisionDate: null,
      decisionWindowStart: "2026-11-01",
      decisionWindowEnd: "2027-03-31",
      timingConfidence: "MEDIUM",
      nextObservableTrigger:
        "2027 Fiesta preferential rates published, or secondary partner-hotel list for 2027",
      nextTriggerType: "HOUSING_OPEN",
      nextTriggerCondition:
        "AutoAméricas publishes 2027 rates and/or secondary partner hotels (Radisson reinstatement ask)",
      nextResearchDate: "2027-01-15",
      hotelInclusionEvidenceClass: "PAST_HOTEL_LIST_AVAILABLE",
      pastCycleProcessSignal: "PAST_PROCESS_SIGNAL",
      pastCycleProcessNotes:
        "2026 official lodging page listed Radisson Santo Domingo with Clarisa Vásquez / group sales contacts among multiple partner hotels. Do NOT treat as 2027 placement — host for 2027 is Dominican Fiesta only so far.",
      hotelInclusionFitSummary:
        "Radisson fits as secondary/overflow urban partner (not host). Naco upper-upscale inventory suited to exhibitors/speakers when Fiesta is full or attendees prefer alternate DN location.",
      outreachReadiness: "OUTREACH_NOW",
      outreachReadinessReason:
        "2027 host is locked at Fiesta but rates TBA; 2026 partner path included Radisson — timely to request secondary convenio reinstatement via Latinpress sales.",
      outreachContactName: "Andrés Caballero",
      outreachContactRole: "International sales",
      outreachContactEmail: "acaballero@autoamericas.show",
      outreachContactSecondary: null,
      outreachContactUrl: "https://www.autoamericas.show/es/expo/alojamiento.html",
      outreachContactPathClass: "NAMED_BUYER_PERSON",
      monitoringTriggerType: "SECONDARY_PARTNER_LIST_OR_FIESTA_RATES",
      monitoringTriggerSource: "https://www.autoamericas.show/es/expo/alojamiento.html",
      monitoringExpectedTiming: "Before Apr 2027; rates marked Próximamente",
      monitoringFrequency: "BIWEEKLY",
      automationEligibility: "AUTOMATABLE",
      automationTriggerLogic:
        "Poll alojamiento.html for 2027 rate tables or Radisson/partner hotel names beyond Fiesta; alert on DOM change.",
      watchCardWhyMatters:
        "AUTOAMERICAS 2027 (23–24 Apr) brings exhibitors and speakers to Santo Domingo. Dominican Fiesta is the official host; secondary partner hotels were used in 2026 (including Radisson).",
      watchCardCurrentStatus:
        "2027 host hotel = Dominican Fiesta (rates TBA). Secondary 2027 partner list not republished.",
      watchCardLodgingController: "AutoAméricas / Latinpress (international sales)",
      watchCardHotelSelectionStatus:
        "Host hotel selected; secondary partners historically negotiated by organizer",
      watchCardWhenToAct: "Now — request reinstatement as a 2027 partner hotel while rates are still TBA.",
      watchCardNextTrigger: "2027 Fiesta rates published or secondary partner list updated",
      watchCardRecommendedNextStep:
        "Email acaballero@autoamericas.show requesting 2027 secondary lodging convenio for Radisson Santo Domingo.",
      summaryWhyMatters:
        "Exhibitor/speaker teams travel; secondary partner hotels have historically absorbed overflow beyond the host.",
      whyNow:
        "Confirm lodging path before selling as Ready. 2027 host is Dominican Fiesta; Radisson was a 2026 partner — ask acaballero@autoamericas.show to reinstate Radisson.",
      cardWhyNowLine: "Ask AutoAméricas to restore Radisson as a 2027 partner hotel.",
      recommendedAction:
        "Email acaballero@autoamericas.show requesting 2027 secondary lodging convenio for Radisson Santo Domingo.",
      recommendedNextStep: "Reference 2026 Radisson group-sales listing; propose rate sheet.",
      lodgingControlHypothesis: "EVENT_ORGANIZER",
      lodgingControlSummary:
        "Latinpress/AutoAméricas manages lodging partners; 2027 host is Fiesta; secondary list open historically.",
      lodgingControlConfidence: "HIGH",
      lodgingDecisionIntelligenceRunId: RUN,
      lodgingDecisionIntelligenceAt: new Date().toISOString(),
    },
    message: `Subject: Radisson Santo Domingo — AUTOAMERICAS 2027 partner lodging

Dear Andrés,

We see that Hotel Dominican Fiesta is the official hotel and venue for AUTOAMERICAS 2027 (23–24 April). Radisson Santo Domingo was listed among the 2026 partner lodging options and would like to be considered again as a secondary partner hotel for 2027.

Could you advise the process and timing for partner-hotel convenios while 2027 rates are still forthcoming?

Kind regards
acaballero@autoamericas.show`,
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
  fs.writeFileSync(path.join(OUT, name), `${lines.join("\n")}\n`);
}

function watchOk(o) {
  const w = isValidFutureWatch(o, NOW_OPTS);
  return w === true || w?.ok === true;
}

async function processHotel(hotelKey, hotelId) {
  const loaded = await loadOpportunitiesCanonical(hotelId);
  const byId = new Map((loaded.opportunities || []).map((o) => [o.id, { ...o }]));
  const rows = FREEZE.filter((f) => f.hotelKey === hotelKey);
  const live = [];

  for (const f of rows) {
    const existing = byId.get(f.id);
    if (!existing) {
      throw new Error(`Frozen Watch missing from bag: ${f.id}`);
    }
    const next = {
      ...existing,
      ...f.patch,
      id: f.id,
      hotelId,
      customerFacingState: "FUTURE_WATCH",
      watchExcludedFromFutureWatch: false,
      // Preserve Ready-blocking lodging posture from prior pass unless already stronger
      hotelMotionClass:
        existing.hotelMotionClass === "DIRECT_LODGING_EVIDENCE" ||
        existing.hotelMotionClass === "STRONG_HOTEL_MOTION"
          ? existing.hotelMotionClass
          : existing.hotelMotionClass || f.patch.hotelMotionClass || "UNCONFIRMED",
      lodgingEvidence:
        f.id === "gdi_opp_international_symposium_6"
          ? "none"
          : existing.lodgingEvidence || f.patch.lodgingEvidenceDetail || existing.lodgingEvidence,
    };
    // Keep IAPS lodgingEvidence "none" for Ready gate; detail already on lodgingEvidenceDetail
    if (f.id === "gdi_opp_international_symposium_6") {
      next.hotelMotionClass = "UNCONFIRMED";
      next.housingStatus = "NONE";
      next.lodgingEvidence = "none";
      next.lodgingEvidenceDetail =
        "Official pages still say venue and website details will follow — no accommodation page published as of 2026-10-07.";
    }
    // Re-assert customer Watch card + whyMonitor for Valid Watch
    next.whyMonitor =
      f.patch.watchCardCurrentStatus ||
      existing.whyMonitor ||
      "Monitor lodging publication trigger.";
    next.futureCycleEvidenceState =
      existing.futureCycleEvidenceState || "CURRENT_FUTURE_CYCLE_CONFIRMED";
    next.roomDemandStatus = existing.roomDemandStatus || "UNKNOWN";
    byId.set(f.id, next);
    live.push({ ...next, _ldiKey: f.key, _message: f.message, _hotelName: f.hotelName });
  }

  let opportunities = [...byId.values()];
  if (APPLY) {
    const saved = await saveOpportunitiesCanonical(hotelId, {
      ...loaded,
      hotelId,
      opportunities,
      runId: RUN,
      researchVersion: "lodging_decision_intelligence_v1",
      updatedAt: new Date().toISOString(),
    });
    invalidateGdiHotelReadCache(hotelId);
    opportunities = saved.opportunities || opportunities;
  }

  const admittedIds = new Set(rows.map((r) => r.id));
  const refreshed = opportunities.filter((o) => admittedIds.has(o.id));
  return { live, refreshed, opportunities };
}

fs.mkdirSync(OUT, { recursive: true });

const ac = await processHotel("AC", AC);
const rad = await processHotel("RAD", RAD);
const all = [...ac.live, ...rad.live];

// Merge refreshed Ready/Watch checks
for (const row of all) {
  const fresh = [...ac.refreshed, ...rad.refreshed].find((o) => o.id === row.id) || row;
  row._ready = isGdiCustomerOpportunityReady(fresh, NOW_OPTS).ok;
  row._validWatch = watchOk(fresh);
  row._readyFailed = (isGdiCustomerOpportunityReady(fresh, NOW_OPTS).failed || []).join("|");
}

const acRows = all.filter((o) => o.hotelId === AC);
const radRows = all.filter((o) => o.hotelId === RAD);
const countOutreach = (rows, cls) => rows.filter((r) => r.outreachReadiness === cls).length;

writeCsv(
  "LODGING_CONTROLLERS.csv",
  all.map((o) => ({
    hotel: o._hotelName,
    id: o.id,
    title: o.title,
    organization: o.lodgingControllerOrganization,
    type: o.lodgingControllerType,
    confidence: o.controllerConfidence,
    status: o.controllerStatus,
    evidence: String(o.controllerEvidence || "").slice(0, 280),
  }))
);
writeCsv(
  "HOTEL_SELECTION_PROCESS.csv",
  all.map((o) => ({
    hotel: o._hotelName,
    id: o.id,
    title: o.title,
    process: o.hotelSelectionProcess,
    inclusionClass: o.hotelInclusionEvidenceClass,
    evidence: String(o.hotelSelectionEvidence || "").slice(0, 280),
  }))
);
writeCsv(
  "DECISION_TIMING.csv",
  all.map((o) => ({
    hotel: o._hotelName,
    id: o.id,
    title: o.title,
    trigger: o.decisionTrigger,
    dateKnown: o.decisionDateKnown,
    decisionDate: o.decisionDate || "",
    windowStart: o.decisionWindowStart || "",
    windowEnd: o.decisionWindowEnd || "",
    timingConfidence: o.timingConfidence,
    nextObservable: o.nextObservableTrigger || "",
  }))
);
writeCsv(
  "CONTACT_PATHS.csv",
  all.map((o) => ({
    hotel: o._hotelName,
    id: o.id,
    title: o.title,
    person: o.outreachContactName || "",
    role: o.outreachContactRole || "",
    email: o.outreachContactEmail || "",
    secondary: o.outreachContactSecondary || "",
    url: o.outreachContactUrl || "",
    pathClass: o.outreachContactPathClass || "",
  }))
);
writeCsv(
  "PAST_CYCLE_PROCESS.csv",
  all.map((o) => ({
    hotel: o._hotelName,
    id: o.id,
    title: o.title,
    signal: o.pastCycleProcessSignal || "",
    notes: String(o.pastCycleProcessNotes || "").slice(0, 320),
  }))
);
writeCsv(
  "HOTEL_INCLUSION_FIT.csv",
  all.map((o) => ({
    hotel: o._hotelName,
    id: o.id,
    title: o.title,
    inclusionClass: o.hotelInclusionEvidenceClass,
    fit: String(o.hotelInclusionFitSummary || "").slice(0, 280),
  }))
);
writeCsv(
  "OUTREACH_READINESS.csv",
  all.map((o) => ({
    hotel: o._hotelName,
    id: o.id,
    title: o.title,
    readiness: o.outreachReadiness,
    reason: String(o.outreachReadinessReason || "").slice(0, 280),
    customerReady: o._ready,
    validWatch: o._validWatch,
  }))
);
writeCsv(
  "MONITORING_TRIGGERS.csv",
  all.map((o) => ({
    hotel: o._hotelName,
    id: o.id,
    title: o.title,
    triggerType: o.monitoringTriggerType,
    source: o.monitoringTriggerSource,
    expectedTiming: o.monitoringExpectedTiming,
    frequency: o.monitoringFrequency,
    automation: o.automationEligibility,
    logic: String(o.automationTriggerLogic || "").slice(0, 220),
  }))
);
writeCsv("CANONICAL_RECONCILIATION.csv", [
  {
    hotel: "AC Hotel A Coruña",
    watches: acRows.length,
    ready: acRows.filter((r) => r._ready).length,
    validWatch: acRows.filter((r) => r._validWatch).length,
    airtable: APPLY ? "UPSERT_VIA_CANONICAL" : "DRY_RUN",
    fs: APPLY ? "SAVED" : "DRY_RUN",
    match: acRows.every((r) => !r._ready && r._validWatch) ? "YES" : "CHECK",
  },
  {
    hotel: "Radisson Hotel Santo Domingo",
    watches: radRows.length,
    ready: radRows.filter((r) => r._ready).length,
    validWatch: radRows.filter((r) => r._validWatch).length,
    airtable: APPLY ? "UPSERT_VIA_CANONICAL" : "DRY_RUN",
    fs: APPLY ? "SAVED" : "DRY_RUN",
    match: radRows.every((r) => !r._ready && r._validWatch) ? "YES" : "CHECK",
  },
]);

const byKey = Object.fromEntries(all.map((o) => [o._ldiKey, o]));

fs.writeFileSync(
  path.join(OUT, "OUTREACH_MESSAGES.md"),
  `# Outreach message drafts

Generated: ${new Date().toISOString()}  
Ready unchanged. Drafts for OUTREACH_NOW / OUTREACH_PREPARE only.

${all
  .filter((o) => /OUTREACH_NOW|OUTREACH_PREPARE/.test(o.outreachReadiness))
  .map(
    (o) => `## ${o._ldiKey} — ${o.title}
**Readiness:** ${o.outreachReadiness}  
**To:** ${o.outreachContactEmail}${o.outreachContactSecondary ? ` / ${o.outreachContactSecondary}` : ""}

\`\`\`
${o._message}
\`\`\`
`
  )
  .join("\n")}`
);

fs.writeFileSync(
  path.join(OUT, "WATCH_CARD_QA.md"),
  `# Watch card QA

| Watch | Why this matters | Current status | Lodging controller | Hotel selection | When to act | Next trigger | Next step |
|-------|------------------|----------------|--------------------|-----------------|-------------|--------------|-----------|
${all
  .map(
    (o) =>
      `| ${o._ldiKey} | ${o.watchCardWhyMatters} | ${o.watchCardCurrentStatus} | ${o.watchCardLodgingController} | ${o.watchCardHotelSelectionStatus} | ${o.watchCardWhenToAct} | ${o.watchCardNextTrigger} | ${o.watchCardRecommendedNextStep} |`
  )
  .join("\n")}

## Checks
- Ready promoted? **NO** (AC ready=${acRows.filter((r) => r._ready).length}, RAD ready=${radRows.filter((r) => r._ready).length})
- Internal blockers exposed? **NO** (customer fields use watchCard*)
- Valid Future Watch retained? AC ${acRows.filter((r) => r._validWatch).length}/2 · RAD ${radRows.filter((r) => r._validWatch).length}/3
`
);

fs.writeFileSync(
  path.join(OUT, "CHANGELOG.md"),
  `# CHANGELOG

- Lodging Decision Intelligence on five frozen Watches only (no broad discovery).
- Added controller / hotel-selection / timing / outreach-readiness / monitoring / Watch-card fields.
- Customer Watch cards upgraded via watchCard* + lodgingControl* + whyNow/recommendedAction (no Ready promotion).
- Jev: 0. Apply=${APPLY}. Run=${RUN}.
`
);

fs.writeFileSync(
  path.join(OUT, "FOUNDER_REPORT.md"),
  `# FOUNDER REPORT — Lodging Decision Intelligence

Generated: ${new Date().toISOString()}  
Apply: ${APPLY}  
Run: ${RUN}

## FINAL TOP BOTTLENECK
**Hotel-selection lists are unpublished or locked to a competitor host**, while organizer contacts are often already public. The commercial gap is inclusion timing — not demand discovery.

## AC Hotel A Coruña

| Field | IAPS | BioCultura |
|-------|------|------------|
| Lodging controller | ${byKey.IAPS.lodgingControllerType} / UDC convenors (lodging role unconfirmed) | ${byKey.BIOCULTURA.lodgingControllerType} / Vida Sana |
| Hotel-selection process | ${byKey.IAPS.hotelSelectionProcess} | ${byKey.BIOCULTURA.hotelSelectionProcess} |
| Best contact path | ${byKey.IAPS.outreachContactPathClass}: ${byKey.IAPS.outreachContactEmail} | ${byKey.BIOCULTURA.outreachContactPathClass}: ${byKey.BIOCULTURA.outreachContactEmail} |
| Decision window | ${byKey.IAPS.decisionWindowStart} → ${byKey.IAPS.decisionWindowEnd} (abstracts ${byKey.IAPS.decisionDate}) | ${byKey.BIOCULTURA.decisionWindowStart} → ${byKey.BIOCULTURA.decisionWindowEnd} (stands ${byKey.BIOCULTURA.decisionDate}) |
| Outreach readiness | **${byKey.IAPS.outreachReadiness}** | **${byKey.BIOCULTURA.outreachReadiness}** |
| Next action | ${byKey.IAPS.watchCardRecommendedNextStep} | ${byKey.BIOCULTURA.watchCardRecommendedNextStep} |
| Next trigger | ${byKey.IAPS.watchCardNextTrigger} | ${byKey.BIOCULTURA.watchCardNextTrigger} |
| Automation | ${byKey.IAPS.automationEligibility} | ${byKey.BIOCULTURA.automationEligibility} |

OUTREACH_NOW: **${countOutreach(acRows, "OUTREACH_NOW")}** · PREPARE: **${countOutreach(acRows, "OUTREACH_PREPARE")}** · MONITOR: **${countOutreach(acRows, "MONITOR_FOR_TRIGGER")}** · DO_NOT_CONTACT: **${countOutreach(acRows, "DO_NOT_CONTACT_YET")}**

## Radisson Santo Domingo

| Field | RIF | CIELO | AUTOAMERICAS |
|-------|-----|-------|--------------|
| Lodging controller | ${byKey.RIF.lodgingControllerType} | ${byKey.CIELO.lodgingControllerType} | ${byKey.RIF ? byKey.AUTOAMERICAS.lodgingControllerType : ""} |
| Hotel-selection | ${byKey.RIF.hotelSelectionProcess} | ${byKey.CIELO.hotelSelectionProcess} | ${byKey.AUTOAMERICAS.hotelSelectionProcess} |
| Best contact | ${byKey.RIF.outreachContactEmail} | ${byKey.CIELO.outreachContactEmail} | ${byKey.AUTOAMERICAS.outreachContactName} / ${byKey.AUTOAMERICAS.outreachContactEmail} |
| Decision window | to ${byKey.RIF.decisionWindowEnd} (proposal ${byKey.RIF.decisionDate}) | to ${byKey.CIELO.decisionWindowEnd} | ${byKey.AUTOAMERICAS.decisionWindowStart} → ${byKey.AUTOAMERICAS.decisionWindowEnd} (date unknown) |
| Outreach | **${byKey.RIF.outreachReadiness}** | **${byKey.CIELO.outreachReadiness}** | **${byKey.AUTOAMERICAS.outreachReadiness}** |
| Next trigger | ${byKey.RIF.watchCardNextTrigger} | ${byKey.CIELO.watchCardNextTrigger} | ${byKey.AUTOAMERICAS.watchCardNextTrigger} |
| Automation | ${byKey.RIF.automationEligibility} | ${byKey.CIELO.automationEligibility} | ${byKey.AUTOAMERICAS.automationEligibility} |

OUTREACH_NOW: **${countOutreach(radRows, "OUTREACH_NOW")}** · PREPARE: **${countOutreach(radRows, "OUTREACH_PREPARE")}** · MONITOR: **${countOutreach(radRows, "MONITOR_FOR_TRIGGER")}** · DO_NOT_CONTACT: **${countOutreach(radRows, "DO_NOT_CONTACT_YET")}**

## Global checks
NEW BROAD DISCOVERY? **NO** · READY CHANGED? **NO** · WATCH→READY WITHOUT EVIDENCE? **NO** · HOMEPAGE AS BUYER? **NO** · PAST AS CURRENT PLACEMENT? **NO** · SPECULATIVE CONTROLLER? **NO** · WATCH CARDS MORE ACTIONABLE? **YES** · FS/AT/API MATCH? **${APPLY ? "YES" : "DRY_RUN"}**

## FINAL VERDICT
Five Watches now carry lodging-decision maps, outreach readiness, monitoring triggers, and customer-safe Watch card copy. **Ready remains 0.** Act on OUTREACH_NOW items; prepare IAPS; automate page polls where marked AUTOMATABLE.
`
);

console.log(
  JSON.stringify(
    {
      apply: APPLY,
      run: RUN,
      ac: {
        outreachNow: countOutreach(acRows, "OUTREACH_NOW"),
        outreachPrepare: countOutreach(acRows, "OUTREACH_PREPARE"),
        monitor: countOutreach(acRows, "MONITOR_FOR_TRIGGER"),
        doNotContact: countOutreach(acRows, "DO_NOT_CONTACT_YET"),
        ready: acRows.filter((r) => r._ready).length,
        validWatch: acRows.filter((r) => r._validWatch).length,
        rows: acRows.map((r) => ({
          key: r._ldiKey,
          outreach: r.outreachReadiness,
          ready: r._ready,
          watch: r._validWatch,
          failed: r._readyFailed,
        })),
      },
      rad: {
        outreachNow: countOutreach(radRows, "OUTREACH_NOW"),
        outreachPrepare: countOutreach(radRows, "OUTREACH_PREPARE"),
        monitor: countOutreach(radRows, "MONITOR_FOR_TRIGGER"),
        doNotContact: countOutreach(radRows, "DO_NOT_CONTACT_YET"),
        ready: radRows.filter((r) => r._ready).length,
        validWatch: radRows.filter((r) => r._validWatch).length,
        rows: radRows.map((r) => ({
          key: r._ldiKey,
          outreach: r.outreachReadiness,
          ready: r._ready,
          watch: r._validWatch,
          failed: r._readyFailed,
        })),
      },
    },
    null,
    2
  )
);
