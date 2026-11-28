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
/**
 * Shared Canonical Contact Intelligence V1:
 * HI Org Contacts + HI Contact Channels are the durable person/channel SoT.
 * GDO primaryContact* remains a product projection that must hydrate from the shared resolver.
 * See: docs/data-intelligence/shared-canonical-contact-intelligence-v1-architecture-audit.md
 * Module: lib/shared-contact-intelligence/
 */
export const CANONICAL_CONTACT_SOT_MAP = Object.freeze({
  [CONTACT_SOT_ENTITY.PERSON]: {
    identityKey: "HI Org Contacts Person ID (canonical) + reversible gdi personIdentityKey map",
    primaryDestination: "HI Org Contacts",
    airtableReuse: "HI Org Contacts — shared with Hotel Explorer",
    recommendation: "REUSE_HI_ORG_CONTACTS",
    createNewContactsTable: false,
    rationale:
      "Shared Canonical Contact Intelligence V1: one Person ID across GDI and HE. GDI personIdentityKey maps reversibly onto HI Person ID; do not create a parallel contacts DB.",
  },
  [CONTACT_SOT_ENTITY.ORGANIZATION]: {
    identityKey: "organizationName + optional Demand Generator id / domain",
    primaryDestination: "HI Organizations + Demand Generators (Airtable) + opportunity.organizationName",
    airtableReuse: "HI Organizations / Demand Generators / GDO organization fields",
    recommendation: "REUSE_HI_ORGANIZATIONS_AND_DEMAND_GENERATORS",
    createNewContactsTable: false,
  },
  [CONTACT_SOT_ENTITY.EMPLOYMENT_RELATIONSHIP]: {
    identityKey: "HI Person ID + Organization ID + role",
    primaryDestination: "HI Org Contacts affiliation + embedded primaryContact.role/title on GDO",
    airtableReuse: "HI Org Contacts + primaryContactRole / title on GDO",
    recommendation: "REUSE_HI_ORG_CONTACTS_AFFILIATION",
    createNewContactsTable: false,
    note: "Employer change creates a new affiliation row; preserve history; map new gdi personIdentityKey without destructive FK rewrite.",
  },
  [CONTACT_SOT_ENTITY.EMAIL_CHANNEL]: {
    identityKey: "HI Person ID + Email channel",
    primaryDestination: "HI Contact Channels (shared SoT); GDO primaryContactEmail is a hydrated projection",
    airtableReuse: "HI Contact Channels + GDO primaryContactEmail (typed projection)",
    recommendation: "REUSE_HI_CONTACT_CHANNELS",
    createNewContactsTable: false,
    channelKinds: ["VERIFIED_DIRECT_WORK_EMAIL", "VERIFIED_ORGANIZATION_EMAIL_ROUTE"],
  },
  [CONTACT_SOT_ENTITY.PHONE_CHANNEL]: {
    identityKey: "HI Person ID + Telephone channel",
    primaryDestination: "HI Contact Channels (shared SoT); GDO primaryContactPhone is a hydrated projection",
    airtableReuse: "HI Contact Channels + GDO primaryContactPhone (typed projection)",
    recommendation: "REUSE_HI_CONTACT_CHANNELS",
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
