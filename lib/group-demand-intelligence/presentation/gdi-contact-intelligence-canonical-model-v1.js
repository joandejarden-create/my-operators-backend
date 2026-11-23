/**
 * GDI Contact Intelligence V2.2 — canonical data-model recommendation.
 *
 * Design authority (reuse-first):
 *   docs/data-intelligence/gdi-3-0-slice2-airtable-persistence-partners-reuse-audit-v1.md
 *   gdi-public-contact-persistence-readiness-v1 (writes hard-off)
 *
 * Does NOT mutate Airtable schema. Does NOT enable live writes.
 */

export const GDI_CONTACT_INTELLIGENCE_CANONICAL_MODEL_V1 =
  "gdi-contact-intelligence-canonical-model-v1";

export const CONTACT_SOT_ENTITY = Object.freeze({
  PERSON: "PERSON",
  ORGANIZATION: "ORGANIZATION",
  EMPLOYMENT_RELATIONSHIP: "EMPLOYMENT_RELATIONSHIP",
  EMAIL_CHANNEL: "EMAIL_CHANNEL",
  PHONE_CHANNEL: "PHONE_CHANNEL",
  SOURCE_EVIDENCE: "SOURCE_EVIDENCE",
  RIGHTS_RETENTION: "RIGHTS_RETENTION",
  OPPORTUNITY_ASSOCIATION: "OPPORTUNITY_ASSOCIATION",
  HOTEL_ASSOCIATION: "HOTEL_ASSOCIATION",
  VERIFICATION_HISTORY: "VERIFICATION_HISTORY",
});

/**
 * Long-term SoT mapping. Prefer existing Airtable / GDI destinations.
 * CREATE_NEW_TABLE only when an entity cannot be represented without
 * duplicate person records or competing contact databases.
 */
export const CANONICAL_CONTACT_SOT_MAP = Object.freeze({
  [CONTACT_SOT_ENTITY.PERSON]: {
    identityKey: "personIdentityKey (sha256 name|org slice16)",
    primaryDestination: "LOGICAL_PERSON_INDEX_FILE_OR_FUTURE_AIRTABLE",
    airtableReuse: "NONE_TODAY — person exists only as embedded opportunity contact",
    recommendation: "KEEP_EMBEDDED_PLUS_DURABLE_INDEX",
    createNewContactsTable: false,
    rationale:
      "Person identity is derived (personIdentityKey). Durable cross-week reuse needs a governed person index; until founder approves Airtable fields/table, use fixture/file durable store only.",
  },
  [CONTACT_SOT_ENTITY.ORGANIZATION]: {
    identityKey: "organizationName + optional Demand Generator id / domain",
    primaryDestination: "Demand Generators (Airtable) + opportunity.organizationName",
    airtableReuse: "Demand Generators / GDO organization fields",
    recommendation: "REUSE_DEMAND_GENERATORS",
    createNewContactsTable: false,
  },
  [CONTACT_SOT_ENTITY.EMPLOYMENT_RELATIONSHIP]: {
    identityKey: "personIdentityKey + organizationKey + role",
    primaryDestination: "Embedded primaryContact.role/title on GDO + research inventory",
    airtableReuse: "primaryContactRole / title on GDO",
    recommendation: "REUSE_EMBEDDED_WITH_EMPLOYER_CHANGE_HISTORY_IN_DURABLE_INDEX",
    createNewContactsTable: false,
    note: "Employer change invalidates prior personIdentityKey (name|oldOrg) — new key + history link.",
  },
  [CONTACT_SOT_ENTITY.EMAIL_CHANNEL]: {
    identityKey: "personIdentityKey + email channel kind",
    primaryDestination: "GDO primaryContactEmail + research inventory payload/typed fields",
    airtableReuse: "primaryContactEmail (typed) + slim payload",
    recommendation: "REUSE_GDO_AND_RESEARCH_INVENTORY_EMBEDDED",
    createNewContactsTable: false,
    channelKinds: ["VERIFIED_DIRECT_WORK_EMAIL", "VERIFIED_ORGANIZATION_EMAIL_ROUTE"],
  },
  [CONTACT_SOT_ENTITY.PHONE_CHANNEL]: {
    identityKey: "personIdentityKey + phone channel kind",
    primaryDestination: "GDO primaryContactPhone + research inventory",
    airtableReuse: "primaryContactPhone / phone type fields",
    recommendation: "REUSE_GDO_AND_RESEARCH_INVENTORY_EMBEDDED",
    createNewContactsTable: false,
    channelKinds: ["VERIFIED_DIRECT_BUSINESS_PHONE", "VERIFIED_ORGANIZATION_PHONE_ROUTE"],
  },
  [CONTACT_SOT_ENTITY.SOURCE_EVIDENCE]: {
    identityKey: "sourceUrl + verificationTimestamp",
    primaryDestination: "primaryContact.sourceUrl / contactSourceUrl + evidence[]",
    airtableReuse: "Payload evidence + optional typed source URL (gap: research inventory typed columns)",
    recommendation: "REUSE_PAYLOAD_PLUS_OPTIONAL_TYPED_SOURCE_URL",
    justifiedSchemaAdditions: [
      "Optional typed contactSourceUrl / contactVerificationStatus / personIdentityKey on GDO + research inventory — founder approval required before mutation",
    ],
  },
  [CONTACT_SOT_ENTITY.RIGHTS_RETENTION]: {
    identityKey: "personIdentityKey + channel",
    primaryDestination: "Durable index rightsMetadata (not customer-facing)",
    airtableReuse: "NONE typed today",
    recommendation: "STORE_IN_DURABLE_INDEX_UNTIL_TYPED_RIGHTS_FIELDS_AUTHORIZED",
    createNewContactsTable: false,
  },
  [CONTACT_SOT_ENTITY.OPPORTUNITY_ASSOCIATION]: {
    identityKey: "opportunityId + personIdentityKey",
    primaryDestination: "GDO record + Hotel Research Opportunities inventory",
    airtableReuse: "GDO + research inventory compositeKey / opportunityId",
    recommendation: "REUSE_EXISTING_OPPORTUNITY_REGISTRIES",
  },
  [CONTACT_SOT_ENTITY.HOTEL_ASSOCIATION]: {
    identityKey: "hotelId + personIdentityKey",
    primaryDestination: "Hotel-scoped GDO / research inventory rows",
    airtableReuse: "Hotel-scoped opportunity tables",
    recommendation: "REUSE_HOTEL_SCOPED_ROWS_PLUS_DURABLE_PERSON_INDEX_LINKS",
  },
  [CONTACT_SOT_ENTITY.VERIFICATION_HISTORY]: {
    identityKey: "personIdentityKey + channel + verifiedAt",
    primaryDestination: "Durable index auditLog + field provenance supersessionHistory",
    airtableReuse: "contact-field-provenance-v1 (payload) — not a separate Airtable history table",
    recommendation: "REUSE_PROVENANCE_MODULE_PLUS_DURABLE_AUDIT",
  },
});

export const AIRTABLE_DESTINATIONS_REUSED = Object.freeze([
  {
    name: "Group Demand Opportunities (GDO)",
    role: "Qualified hotel-scoped opportunity registry with typed primaryContact*",
  },
  {
    name: "GDI Hotel Research Opportunities (research inventory)",
    role: "Research SoT mirror; contact often in slim payload; typed contact columns partial",
  },
  {
    name: "Demand Generators",
    role: "Organization identity reuse across programs/signals",
  },
  {
    name: "Embedded primaryContact* / contactSourceUrl / provenance payload",
    role: "Person channels + evidence without a Contacts table",
  },
]);

export const JUSTIFIED_SCHEMA_ADDITIONS_PENDING_FOUNDER = Object.freeze([
  {
    field: "personIdentityKey",
    tables: ["GDO", "Hotel Research Opportunities"],
    reason: "Deterministic cross-hotel / cross-week person matching",
    requiredForLivePilot: true,
  },
  {
    field: "contactSourceUrl / contactVerificationStatus",
    tables: ["GDO", "Hotel Research Opportunities"],
    reason: "Typed evidence + verification without relying only on JSON payload",
    requiredForLivePilot: true,
  },
  {
    field: "contactRightsRetentionDays / permissionToDisplayToHotelClients",
    tables: ["GDO or durable index only"],
    reason: "Lawful storage / customer-visibility gates",
    requiredForLivePilot: true,
  },
  {
    table: "Contacts (normalized)",
    reason: "Only if typed embedded fields + durable person index still cannot support multi-hotel person SoT after a live pilot",
    requiredForLivePilot: false,
    createNow: false,
  },
]);

export function getCanonicalContactModelRecommendation() {
  return {
    version: GDI_CONTACT_INTELLIGENCE_CANONICAL_MODEL_V1,
    createNewContactsTable: false,
    strategy: "EMBEDDED_PRIMARY_CONTACT_PLUS_DURABLE_PERSON_INDEX",
    liveWritesEnabled: false,
    sotMap: CANONICAL_CONTACT_SOT_MAP,
    airtableDestinationsReused: AIRTABLE_DESTINATIONS_REUSED,
    justifiedSchemaAdditionsPendingFounder: JUSTIFIED_SCHEMA_ADDITIONS_PENDING_FOUNDER,
    competingDatabasesProhibited: true,
    providerDataPersistenceProhibited: [
      "SURFE",
      "FINDYMAIL",
      "PDL",
      "PROSPEO",
      "FULLENRICH",
      "APOLLO",
    ],
  };
}

export default {
  GDI_CONTACT_INTELLIGENCE_CANONICAL_MODEL_V1,
  CONTACT_SOT_ENTITY,
  CANONICAL_CONTACT_SOT_MAP,
  AIRTABLE_DESTINATIONS_REUSED,
  JUSTIFIED_SCHEMA_ADDITIONS_PENDING_FOUNDER,
  getCanonicalContactModelRecommendation,
};
