/**
 * Ready-gate international assumption audit.
 * Documents US-specific implementation assumptions removed WITHOUT lowering thresholds.
 */

export const READY_GATE_INTL_AUDIT = Object.freeze({
  readyThresholdChanged: false,
  watchThresholdChanged: false,
  usReadyStandardWeakened: false,
  changes: [
    {
      id: "functional_path_multilingual",
      assumptionRemoved:
        "FUNCTIONAL_PATH_RE only recognized English path segments (housing/accommodation/passkey/onpeak)",
      fix: "Accept ES/DE/GL equivalents: alojamiento, hoteles, reserva, unterkunft, hotelkontingent, zimmerkontingent, partnerhotel, kontakt, anmeldung, inscripción",
      thresholdImpact: "NONE — same RELEVANT_FUNCTION_CONTACT class; more proof forms for same fact",
    },
    {
      id: "relevant_role_multilingual",
      assumptionRemoved: "RELEVANT_ROLE_RE English-only meeting/housing/procurement vocabulary",
      fix: "Accept secretaría técnica, alojamiento, unterkunft, kongress, tagung, einkauf, contratación, pco, dmc, agencia de eventos, Veranstaltungsagentur",
      thresholdImpact: "NONE",
    },
    {
      id: "lodging_blob_multilingual",
      assumptionRemoved: "hasLodgingContactEvidence required passkey/onpeak/EN housing phrases",
      fix: "Accept hotel oficial, hoteles recomendados, offizielles Hotel, Partnerhotel, Hotelkontingent, bloque de habitaciones, alojamiento oficial",
      thresholdImpact: "NONE — still requires positive lodging evidence; negated phrasing still rejected",
    },
    {
      id: "functional_desk_multilingual",
      assumptionRemoved: "Functional desk names English-only",
      fix: "Accept secretaría, alojamiento, Kongressbüro, Teilnehmermanagement as desk-style names",
      thresholdImpact: "NONE",
    },
    {
      id: "named_person_not_required_when_functional_pco",
      assumptionRemoved: "Implicit bias that Ready needs a named individual (US job-title path)",
      status: "UNCHANGED_POLICY",
      note: "meetsReadyContactRequirement already accepts RELEVANT_FUNCTION / NAMED_BUYER_ROLE_PATH; multilingual expansion makes functional PCO/secretariat paths recognizable internationally",
      thresholdImpact: "NONE",
    },
    {
      id: "public_hotel_block_vs_pco_process",
      assumptionRemoved: "US-style public hotel block as only lodging proof form",
      status: "POLICY_DOCUMENTED",
      note: "Equivalent Evidence Policy allows PCO accommodation instructions / hotel-supplied rate request / procurement notice as alternate forms for LODGING_SELECTION_ACTIVE — maturity gates unchanged; lodgingEvidenceClass taxonomy unchanged",
      thresholdImpact: "NONE",
    },
    {
      id: "participant_list_not_sole_path",
      assumptionRemoved: "Participant list as sole path to named entity (implementation habit, not gate text)",
      status: "ARCHITECTURE",
      note: "Demand Controller First + Account First spines can resolve buyer/controller and accounts without lists; Ready still requires full pillar set",
      thresholdImpact: "NONE",
    },
  ],
  explicitlyNotChanged: [
    "COMPLETE_STRONG / COMPLETE_PLAUSIBLE / PARTIAL_PACKET / SIGNAL_ONLY definitions",
    "DIRECT_LODGING_EVIDENCE / STRONG_HOTEL_MOTION / PLAUSIBLE_HOTEL_MOTION / UNCONFIRMED / NONE taxonomy",
    "Valid Future Watch gate",
    "No speculative lodging",
    "No auto-promote from outreach alone",
    "Historical evidence cannot prove current placement",
  ],
});

export function getReadyGateIntlAudit() {
  return READY_GATE_INTL_AUDIT;
}
