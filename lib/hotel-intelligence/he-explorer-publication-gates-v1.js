/**
 * Hotel Explorer 5.0 — permanent Intelligence Factory publication gates.
 * Idempotent research → Airtable → read-back → API → Live UI sequence.
 * Guest Sentiment is founder-disabled: certification uses DISABLED_BY_FOUNDER.
 */

import {
  HE_ACTIVE_TAB_IDS,
  HE_ALL_TAB_IDS,
  HE_PRESERVED_DISABLED_TAB_IDS,
} from "./he-canonical-explorer-assets-v1.js";
import {
  HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER,
  isHeGuestSentimentEnabled,
} from "./guest-sentiment/he-guest-sentiment-feature-flag-v1.js";
import { HE50_CANONICAL_HPC_BY_PILOT_KEY } from "./he-5-0-full-e2e/contract.js";

export const HE_EXPLORER_PUBLICATION_GATES_V1 = "he-explorer-publication-gates-v1";

/** Eight-hotel pilot HPC identities (canonical). */
export const HE50_EIGHT_HOTEL_HPC_IDS = Object.freeze(
  Object.values(HE50_CANONICAL_HPC_BY_PILOT_KEY),
);

export const HE_PUBLICATION_STEP = Object.freeze({
  RESOLVE_HPC: "1_RESOLVE_CANONICAL_HPC",
  REUSE_RESEARCH: "2_REUSE_EXISTING_VERIFIED_RESEARCH",
  IDENTIFY_GAPS: "3_IDENTIFY_ACTUAL_FIELD_GAPS",
  VALIDATE_SOURCES: "4_VALIDATE_SOURCES",
  DEDUPE_ENTITIES: "5_DEDUPLICATE_ENTITIES",
  APPLY_AIRTABLE: "6_APPLY_AUTHORIZED_AIRTABLE_MUTATIONS",
  READBACK: "7_READ_BACK_PERSISTED_RECORDS",
  API_HYDRATION: "8_CONFIRM_API_HYDRATION",
  LIVE_RENDER: "9_CONFIRM_LIVE_RENDERING",
  PROVENANCE: "10_RECORD_REFRESH_AND_PROVENANCE",
});

export const HE_PUBLICATION_SEQUENCE = Object.freeze(Object.values(HE_PUBLICATION_STEP));

export const HE50_ACCEPTANCE_GATES = Object.freeze({
  GUEST_SENTIMENT_DISABLED_BY_FOUNDER: "GUEST_SENTIMENT_DISABLED_BY_FOUNDER",
  ELEVEN_TAB_COMPONENT_PARITY: "ELEVEN_TAB_COMPONENT_PARITY",
  CANONICAL_FIELD_MAPPING_COMPLETE: "CANONICAL_FIELD_MAPPING_COMPLETE",
  VERIFIED_RESEARCH_PERSISTENCE_COMPLETE: "VERIFIED_RESEARCH_PERSISTENCE_COMPLETE",
  BETHESDA_FULL_LIVE_VALIDATED: "BETHESDA_FULL_LIVE_VALIDATED",
  EIGHT_HOTEL_AIRTABLE_READBACK_VALIDATED: "EIGHT_HOTEL_AIRTABLE_READBACK_VALIDATED",
  EIGHT_HOTEL_RADAR_BROWSER_VALIDATED: "EIGHT_HOTEL_RADAR_BROWSER_VALIDATED",
  FULL_HE_E2E_VALIDATED: "FULL_HE_E2E_VALIDATED",
});

/**
 * Build a fail-closed acceptance snapshot. Callers must supply evidence;
 * unset gates remain NOT_ASSESSED / NO — never auto-YES.
 */
export function buildHe50AcceptanceSnapshot(evidence = {}) {
  const guestSentimentDisabled =
    evidence.guestSentimentDisabled === true ||
    (!isHeGuestSentimentEnabled() &&
      HE_PRESERVED_DISABLED_TAB_IDS.includes("sentiment"));

  const elevenTabParity =
    evidence.elevenTabParity === true ||
    (HE_ACTIVE_TAB_IDS.length === 11 && HE_ALL_TAB_IDS.length === 12);

  const gates = {
    [HE50_ACCEPTANCE_GATES.GUEST_SENTIMENT_DISABLED_BY_FOUNDER]: guestSentimentDisabled
      ? "YES"
      : "NO",
    [HE50_ACCEPTANCE_GATES.ELEVEN_TAB_COMPONENT_PARITY]: elevenTabParity ? "YES" : "NO",
    [HE50_ACCEPTANCE_GATES.CANONICAL_FIELD_MAPPING_COMPLETE]:
      evidence.canonicalFieldMappingComplete === true ? "YES" : "NO",
    [HE50_ACCEPTANCE_GATES.VERIFIED_RESEARCH_PERSISTENCE_COMPLETE]:
      evidence.verifiedResearchPersistenceComplete === true ? "YES" : "NO",
    [HE50_ACCEPTANCE_GATES.BETHESDA_FULL_LIVE_VALIDATED]:
      evidence.bethesdaFullLiveValidated === true ? "YES" : "NO",
    [HE50_ACCEPTANCE_GATES.EIGHT_HOTEL_AIRTABLE_READBACK_VALIDATED]:
      evidence.eightHotelAirtableReadbackValidated === true ? "YES" : "NO",
    [HE50_ACCEPTANCE_GATES.EIGHT_HOTEL_RADAR_BROWSER_VALIDATED]:
      evidence.eightHotelRadarBrowserValidated === true ? "YES" : "NO",
    [HE50_ACCEPTANCE_GATES.FULL_HE_E2E_VALIDATED]: "NO",
  };

  const prerequisiteYes = [
    gates[HE50_ACCEPTANCE_GATES.GUEST_SENTIMENT_DISABLED_BY_FOUNDER],
    gates[HE50_ACCEPTANCE_GATES.ELEVEN_TAB_COMPONENT_PARITY],
    gates[HE50_ACCEPTANCE_GATES.CANONICAL_FIELD_MAPPING_COMPLETE],
    gates[HE50_ACCEPTANCE_GATES.VERIFIED_RESEARCH_PERSISTENCE_COMPLETE],
    gates[HE50_ACCEPTANCE_GATES.BETHESDA_FULL_LIVE_VALIDATED],
    gates[HE50_ACCEPTANCE_GATES.EIGHT_HOTEL_AIRTABLE_READBACK_VALIDATED],
    gates[HE50_ACCEPTANCE_GATES.EIGHT_HOTEL_RADAR_BROWSER_VALIDATED],
  ].every((v) => v === "YES");

  if (evidence.fullHeE2eValidated === true && prerequisiteYes) {
    gates[HE50_ACCEPTANCE_GATES.FULL_HE_E2E_VALIDATED] = "YES";
  }

  return {
    version: HE_EXPLORER_PUBLICATION_GATES_V1,
    at: new Date().toISOString(),
    activeTabCount: HE_ACTIVE_TAB_IDS.length,
    registeredTabCount: HE_ALL_TAB_IDS.length,
    guestSentimentStatus: guestSentimentDisabled
      ? HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER
      : "ENABLED_RIGHTS_GATED",
    publicationSequence: HE_PUBLICATION_SEQUENCE,
    hotelTabAcceptanceDenomination: HE_ACTIVE_TAB_IDS.length * 8, // 88 when eight hotels
    gates,
    evidenceNotes: evidence.notes || null,
    blockers: evidence.blockers || [],
  };
}

/**
 * Classify a field-gap row for next factory action (no research invention).
 */
export function classifyGapAction(row = {}) {
  // Guest Sentiment first — never treat as missing CONNECT work.
  if (
    row.tabId === "sentiment" ||
    row.fieldKey === "platformIdentities" ||
    row.fieldKey === "hotelNews"
  ) {
    return {
      action: "DISABLED_BY_FOUNDER",
      paidResearch: false,
      certification: HE_GUEST_SENTIMENT_DISABLED_BY_FOUNDER,
    };
  }
  const c = String(row.classification || "").trim();
  if (c === "EXISTING_VERIFIED") {
    return { action: "SKIP_REUSE", paidResearch: false };
  }
  if (c === "EXISTING_NOT_CONNECTED") {
    return {
      action: "CONNECT_PERSIST_READBACK",
      paidResearch: false,
      note: "Reuse existing verified research; dry-run Airtable then apply when authorized.",
    };
  }
  if (c === "SOURCE_CONFLICT") {
    return { action: "HUMAN_RECONCILE", paidResearch: false };
  }
  if (c === "RESEARCH_REQUIRED") {
    return {
      action: "FREE_FIRST_THEN_PAID_GATE",
      paidResearch: "only_after_free_exhausted_and_budget",
    };
  }
  if (c === "UNRESOLVED_AFTER_RESEARCH") {
    return { action: "ACCEPT_GAP_OR_ESCALATE", paidResearch: false };
  }
  return { action: "REVIEW", paidResearch: false };
}

export default {
  HE_EXPLORER_PUBLICATION_GATES_V1,
  HE_PUBLICATION_SEQUENCE,
  buildHe50AcceptanceSnapshot,
  classifyGapAction,
};
