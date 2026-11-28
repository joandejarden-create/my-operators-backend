/**
 * V2.3 Airtable schema proposal for contact persistence pilot.
 * DOES NOT mutate Airtable. Founder-in-principle fields only.
 */

export const GDI_CONTACT_INTEL_V23_SCHEMA_PROPOSAL_V1 =
  "gdi-contact-intelligence-v2-3-airtable-schema-proposal-v1";

/** Live inspection snapshot reference (base appa2cE7FTRmIbB32). */
export const GDO_TABLE = Object.freeze({
  name: "Group Demand Opportunities",
  id: "tblRuReslJMwsfRQj",
});

export const RESEARCH_INVENTORY_TABLE = Object.freeze({
  name: "GDI Hotel Research Opportunities",
  id: "tblbkDj0vhQLfrssV",
});

/** Existing GDO contact columns (reuse — do not recreate). */
export const EXISTING_GDO_CONTACT_FIELDS = Object.freeze([
  { id: "fldCZWnLdrEsxu44g", name: "primaryContactId", type: "singleLineText" },
  { id: "fldJTScsgQBO7V5F1", name: "primaryContactName", type: "singleLineText" },
  { id: "fldYHxr3dTzGAbc7s", name: "primaryContactRole", type: "singleLineText" },
  { id: "fldxifqk66ATZMy0s", name: "primaryContactEmail", type: "singleLineText" },
  { id: "flddJIZOHKirEKpXK", name: "primaryContactPhone", type: "singleLineText" },
  { id: "fldXovJrkm6js6JFr", name: "contactQuality", type: "singleLineText" },
  { id: "fldr87N37whlwEwSE", name: "opportunityPayloadJson", type: "multilineText" },
]);

/**
 * Minimum typed fields for pilot (founder approval in principle).
 * All currently ABSENT on live GDO / research inventory.
 */
export const PROPOSED_GDO_TYPED_FIELDS = Object.freeze([
  {
    name: "personIdentityKey",
    type: "singleLineText",
    description:
      "Canonical person grouping/disambiguation key (groupingKey or groupingKey::fingerprint). Not a merge proof alone.",
    requiredForPilot: true,
  },
  {
    name: "contactSourceUrl",
    type: "url",
    description: "Authoritative public source URL for verified contact channels.",
    requiredForPilot: true,
  },
  {
    name: "contactVerificationStatus",
    type: "singleSelect",
    options: [
      "VERIFIED_DIRECT",
      "VERIFIED_ORGANIZATION_ROUTE",
      "CANDIDATE_UNVERIFIED",
      "REJECTED_IDENTITY_MISMATCH",
      "REJECTED_EMPLOYER_MISMATCH",
      "STALE_REVALIDATION_REQUIRED",
    ],
    description: "Channel verification classification (public-source).",
    requiredForPilot: true,
  },
  {
    name: "contactStorageAuthorization",
    type: "singleSelect",
    options: [
      "INTERNAL_RESTRICTED_PUBLIC_SOURCE",
      "RETENTION_NOT_PERMITTED",
      "PROVIDER_EPHEMERAL_PROHIBITED",
    ],
    description: "Lawful/internal storage authorization class — not inferred from public web alone.",
    requiredForPilot: true,
  },
  {
    name: "contactCustomerDisplayPermission",
    type: "singleSelect",
    options: ["DENIED", "ALLOWED_WHEN_PUBLISHED", "ALLOWED_INTERNAL_ONLY"],
    description: "Customer-facing display gate. Pilot must remain DENIED.",
    requiredForPilot: true,
  },
  {
    name: "contactRetentionRevalidationDays",
    type: "number",
    precision: 0,
    description: "Days until revalidation required (default 365).",
    requiredForPilot: true,
  },
  {
    name: "contactIdentityDisambiguationFingerprint",
    type: "singleLineText",
    description:
      "Non-PII fingerprint of independent identity evidence (profile/source). Prevents same-name/org collisions.",
    requiredForPilot: true,
  },
  {
    name: "contactLastVerifiedAt",
    type: "dateTime",
    description: "ISO verification timestamp for retention/expiry.",
    requiredForPilot: true,
  },
]);

export function getSchemaProposalPacket(inspection = null) {
  const missing = PROPOSED_GDO_TYPED_FIELDS.map((f) => f.name);
  return {
    version: GDI_CONTACT_INTEL_V23_SCHEMA_PROPOSAL_V1,
    strategy: "EMBEDDED_PRIMARY_CONTACT_PLUS_DURABLE_PERSON_INDEX",
    createNewContactsTable: false,
    applySchemaMutation: false,
    liveInspection: inspection,
    destinationPrimary: GDO_TABLE,
    destinationSecondary: RESEARCH_INVENTORY_TABLE,
    existingFieldsReused: EXISTING_GDO_CONTACT_FIELDS,
    proposedFields: PROPOSED_GDO_TYPED_FIELDS,
    missingOnLiveGdo: missing,
    researchInventoryNote:
      "Research inventory lacks typed contact columns; pilot writes target GDO embedded primaryContact* + proposed typed metadata. Person-index relationships live in durable fixture store until typed fields exist.",
    reversibleMigrationPlan: {
      steps: [
        "Dry-run ensure script lists create-only missing fields on GDO",
        "IC reviews exact field names/types/options",
        "Apply create-missing-only (no renames, no deletes)",
        "Rollback = clear unused empty fields via Airtable UI / meta delete after pilot abort (no data loss if unused)",
      ],
      impactOnExistingRecords: "None until write pilot; new fields empty on existing rows",
      idempotent: true,
    },
    stopCondition:
      "STOP before schema --apply and before live contact-value writes pending founder+IC approval of this packet",
  };
}

export default {
  GDI_CONTACT_INTEL_V23_SCHEMA_PROPOSAL_V1,
  GDO_TABLE,
  RESEARCH_INVENTORY_TABLE,
  EXISTING_GDO_CONTACT_FIELDS,
  PROPOSED_GDO_TYPED_FIELDS,
  getSchemaProposalPacket,
};
