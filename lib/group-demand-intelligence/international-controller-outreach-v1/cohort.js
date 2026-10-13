/**
 * Frozen outreach cohort: BioCultura + RIF + CIELO only.
 */

import { OUTREACH_READINESS } from "./constants.js";
import { verifyContactAuthority } from "./contact-authority.js";
import {
  buildBioculturaOutreachDraft,
  buildRifOutreachDraft,
  buildCieloOutreachDraft,
} from "./outreach-drafts-es.js";
import { interpretLikelyAnswers } from "./response-classifier.js";
import {
  buildControllerOutreachPursuitOverlay,
  buildControllerOutreachCustomerCard,
  toPursuitDraftFields,
} from "./pursuit-integration.js";

function followUpDate(days = 7) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

const RAW = [
  {
    outreachPriority: 1,
    key: "rif",
    opportunityId: "gdi_opp_rif_filosofia_sd_2027",
    campaignId: "radisscamp_vii_congreso_iberoamericano_de_filosofia_2027_2027",
    hotelId: "radisson_santo_domingo",
    hotelLabel: "Radisson Hotel Santo Domingo",
    controllerId: "dc_rad_adofil",
    controllerName: "ADOFIL / Comité Organizador RIF–UASD",
    contactEmail: "adofil333@gmail.com",
    lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
    officialSource:
      "https://rediberoamericanafilosofia.com/wp-content/uploads/2026/05/Segunda-convocatoria-Congreso-RIF.pdf",
    role: "congress secretariat",
    packet: "COMPLETE_PLAUSIBLE",
    fit: "STRONG_FIT",
    selectionState: "EXPECTED",
    watchState: true,
    pursuitState: "NOT_STARTED",
    evidenceQuestion:
      "Can Radisson still be included in the forthcoming hotel convenio list?",
    draftBuilder: buildRifOutreachDraft,
  },
  {
    outreachPriority: 2,
    key: "cielo",
    opportunityId: "gdi_opp_cielo_laboral_sd_2026",
    campaignId: "radisscamp_6_congreso_mundial_cielo_laboral_2026_2026",
    hotelId: "radisson_santo_domingo",
    hotelLabel: "Radisson Hotel Santo Domingo",
    controllerId: "dc_rad_cielo",
    controllerName: "CIELO Laboral congress secretariat",
    contactEmail: "congresocielo6@gmail.com",
    lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
    officialSource: "https://www.cielolaboral.com/",
    role: "congress secretariat",
    packet: "COMPLETE_PLAUSIBLE",
    fit: "PLAUSIBLE_FIT",
    selectionState: "UNDER_REVIEW",
    watchState: true,
    pursuitState: "NOT_STARTED",
    evidenceQuestion:
      "Is the recommended hotel list finalized, or can Radisson still be added?",
    draftBuilder: buildCieloOutreachDraft,
  },
  {
    outreachPriority: 3,
    key: "biocultura",
    opportunityId: "gdi_opp_biocultura_a_coruna_2027",
    campaignId: "accamp_biocultura_a_coruna_2027_2027",
    hotelId: "ac_hotel_a_coruna",
    hotelLabel: "AC Hotel A Coruña",
    controllerId: "dc_ac_vida_sana",
    controllerName: "Asociación Vida Sana",
    contactEmail: "expositores@vidasana.org",
    lodgingAuthority: "CONFIRMED_LODGING_CONTROLLER",
    officialSource: "https://www.biocultura.org/acoruna/viajes",
    role: "exhibitors / travel desk",
    packet: "COMPLETE_PLAUSIBLE",
    fit: "STRONG_FIT",
    selectionState: "EXPECTED",
    watchState: true,
    pursuitState: "NOT_STARTED",
    evidenceQuestion:
      "Will BioCultura publish a partner hotel list, and can AC still be considered?",
    draftBuilder: buildBioculturaOutreachDraft,
  },
];

/**
 * Build full outreach packets (drafts ready; emails not sent).
 */
export function buildControllerOutreachCohort() {
  return RAW.map((row) => {
    const contactAuthority = verifyContactAuthority({
      email: row.contactEmail,
      controllerName: row.controllerName,
      lodgingAuthority: row.lodgingAuthority,
      officialSource: row.officialSource,
      role: row.role,
    });
    const draft = row.draftBuilder({ hotelName: row.hotelLabel });
    const outreachReadiness = contactAuthority.okForOutreach
      ? OUTREACH_READINESS.READY_TO_SEND
      : OUTREACH_READINESS.NEEDS_CONTACT_RESEARCH;
    const answers = interpretLikelyAnswers(row.key);
    const packet = {
      ...row,
      contactAuthority,
      draft,
      outreachReadiness,
      suggestedFollowUpDate: followUpDate(7),
      whatYesWouldMean: answers.yes,
      whatNoWouldMean: answers.no,
      responseFactsNeeded: [
        "hotel_list_status",
        "target_can_apply_or_excluded",
        "selection_timing",
        "lodging_controller_confirmation",
      ],
      emailsActuallySent: false,
    };
    packet.pursuitOverlay = buildControllerOutreachPursuitOverlay(packet);
    packet.customerCard = buildControllerOutreachCustomerCard(packet);
    packet.pursuitDraftFields = toPursuitDraftFields(packet);
    return packet;
  });
}

export function getArchitectureStatus() {
  return {
    RESPONSE_MODEL_IMPLEMENTED: true,
    HOTEL_SUPPLIED_EVIDENCE_INGESTION_WIRED: true,
    RESPONSE_PACKET_RECOMPUTE_WIRED: true,
    PURSUIT_UI_WIRED: true,
    CUSTOMER_CARD_UPDATE_WIRED: true,
    EMAILS_ACTUALLY_SENT: false,
    READY_CHANGED_WITHOUT_REAL_RESPONSE: false,
    READY_THRESHOLD_CHANGED: false,
    WATCH_THRESHOLD_CHANGED: false,
    APIFY_USED: false,
  };
}
