/**
 * Shared Canonical Contact Intelligence V1 — architecture constants.
 * Airtable HI Org Contacts + HI Contact Channels are the durable SoT.
 * No parallel contacts database. No Hotel Census person SoT.
 */

export const SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1 =
  "shared-canonical-contact-intelligence-v1";

export const CANONICAL_PERSON_SOT = Object.freeze({
  table: "HI Org Contacts",
  personIdField: "Person ID",
  productConsumers: Object.freeze(["hotel-explorer", "gdi"]),
});

export const CANONICAL_CHANNEL_SOT = Object.freeze({
  table: "HI Contact Channels",
  kinds: Object.freeze(["Email", "Telephone", "LinkedIn"]),
});

export const CONTACT_CONSISTENCY_GATE = Object.freeze({
  CONTACT_CANONICAL_IDENTITY_RESOLVED: "CONTACT_CANONICAL_IDENTITY_RESOLVED",
  CONTACT_AIRTABLE_SOURCE_OF_TRUTH: "CONTACT_AIRTABLE_SOURCE_OF_TRUTH",
  CONTACT_CHANNEL_PROVENANCE_VALIDATED: "CONTACT_CHANNEL_PROVENANCE_VALIDATED",
  CONTACT_AIRTABLE_PERSISTENCE_READBACK: "CONTACT_AIRTABLE_PERSISTENCE_READBACK",
  CONTACT_GDI_HE_CHANNEL_PARITY: "CONTACT_GDI_HE_CHANNEL_PARITY",
  CONTACT_NO_DUPLICATE_PAID_ENRICHMENT: "CONTACT_NO_DUPLICATE_PAID_ENRICHMENT",
  CONTACT_DIRECT_CONTACTABILITY_VALIDATED: "CONTACT_DIRECT_CONTACTABILITY_VALIDATED",
  CONTACT_SURFE_TRANSIENT_ONLY: "CONTACT_SURFE_TRANSIENT_ONLY",
});

export const GATE_STATUS = Object.freeze({
  PASS: "PASS",
  FAIL: "FAIL",
  BLOCKED: "BLOCKED",
  NOT_TESTED: "NOT_TESTED",
  NOT_MEASURABLE: "NOT_MEASURABLE",
});

export const BETHESDA_GOLDEN = Object.freeze({
  hotelId: "recLuxvwwxID7U2B8",
  hotelName: "Bethesda Marriott",
  professionals: Object.freeze([
    "Jeff Mullen",
    "Katherine Button",
    "Patrick Reece",
    "Meinrad Lang III",
    "Jenny Pyne",
  ]),
});

export default {
  SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1,
  CANONICAL_PERSON_SOT,
  CANONICAL_CHANNEL_SOT,
  CONTACT_CONSISTENCY_GATE,
  GATE_STATUS,
  BETHESDA_GOLDEN,
};
