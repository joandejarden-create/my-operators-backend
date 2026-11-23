/**
 * Public-source contact persistence readiness (writes DISABLED by default).
 *
 * Validates every prospective saved contact detail against founder gates
 * before any Airtable / GDI write may be considered. Does not invent schema.
 * Does not execute live writes. Provider-derived Surfe/FullEnrich/PDL/Prospeo
 * values are never persistence-eligible unless separately authorized.
 *
 * Canonical destination (existing): GDO / research inventory embedded
 * primaryContact* fields — no new Contacts table without founder approval.
 */

import { CONTACT_DATA_ORIGIN } from "../extract-contact-intelligence-from-source.js";
import {
  assessPublicContactVerification,
  publicContactPersonKey,
  PUBLIC_CONTACT_VERIFICATION_STATUS,
  GDI_PUBLIC_CONTACT_VERIFICATION_V1,
} from "./gdi-public-contact-verification-v1.js";
import { proposeAirtablePublicContactUpsert } from "./gdi-public-contact-channel-hierarchy-v1.js";

export const GDI_PUBLIC_CONTACT_PERSISTENCE_READINESS_V1 =
  "gdi-public-contact-persistence-readiness-v1";

/** Master write flag — must stay false until founder authorizes live writes. */
export const GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED = false;

export const PERSISTENCE_BLOCK_REASONS = Object.freeze({
  LIVE_WRITES_DISABLED: "LIVE_WRITES_DISABLED",
  NO_NAMED_IDENTITY: "NO_NAMED_IDENTITY",
  NO_PERSON_IDENTITY_KEY: "NO_PERSON_IDENTITY_KEY",
  EMPLOYER_OR_ROLE_UNVERIFIED: "EMPLOYER_OR_ROLE_UNVERIFIED",
  NO_VERIFIED_PUBLIC_CHANNEL: "NO_VERIFIED_PUBLIC_CHANNEL",
  PROVIDER_EPHEMERAL_ORIGIN: "PROVIDER_EPHEMERAL_ORIGIN",
  MISSING_SOURCE_EVIDENCE: "MISSING_SOURCE_EVIDENCE",
  RETENTION_NOT_PERMITTED: "RETENTION_NOT_PERMITTED",
  CUSTOMER_DISPLAY_NOT_PERMITTED: "CUSTOMER_DISPLAY_NOT_PERMITTED",
  GUESSED_OR_UNVERIFIED: "GUESSED_OR_UNVERIFIED",
});

const PROVIDER_ORIGIN_RE =
  /APOLLO|PROSPEO|PDL|PEOPLE.?DATA.?LABS|SURFE|FULLENRICH|PROVIDER_ENRICHED|PROVIDER_EPHEMERAL|ON_DEMAND_ENRICHMENT/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function isProviderOrigin(origin) {
  return PROVIDER_ORIGIN_RE.test(text(origin));
}

/**
 * Assess whether a verified public-source contact may be persisted.
 * Always returns execute:false while GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED is false.
 *
 * @param {object} args
 * @param {object} args.opportunity
 * @param {string} [args.hotelId]
 * @param {object} [args.verification] — from assessPublicContactVerification
 * @param {boolean} [args.permissionToRetain=true] — public-source default assumption
 * @param {boolean} [args.permissionToDisplayToHotelClients=true]
 * @param {boolean} [args.founderAuthorizeLiveWrite=false]
 * @param {boolean} [args.excludeLiveWritesDisabledBlock=false] — hypothetical readiness only;
 *   excludes LIVE_WRITES_DISABLED while keeping every other identity/evidence/rights gate.
 */
export function assessPublicContactPersistenceReadiness(args = {}) {
  const opportunity = args.opportunity || {};
  const verification =
    args.verification || assessPublicContactVerification(opportunity);
  const name =
    text(opportunity.primaryContact?.name) || text(opportunity.contactName);
  const org =
    text(opportunity.organizationName) ||
    text(opportunity.displayCompanyName) ||
    text(opportunity.primaryContact?.organization);
  const role =
    text(opportunity.primaryContact?.role) ||
    text(opportunity.primaryContact?.title) ||
    text(opportunity.contactRole);
  const personKey =
    publicContactPersonKey({ name, organization: org }) ||
    text(opportunity.personIdentityKey) ||
    null;
  const origin = text(
    opportunity.contactDataOrigin ||
      opportunity.primaryContact?.contactDataOrigin ||
      verification?.email?.origin ||
      verification?.phone?.origin ||
      "",
  );

  const blocks = [];
  const checklist = {
    verifiedNamedIdentity: Boolean(name && name.split(/\s+/).length >= 2),
    verifiedEmployerAndRole: Boolean(org && role),
    correctContactToPersonAssociation: Boolean(personKey),
    professionalEmailOrBusinessPhone: Boolean(
      verification?.hasVerifiedDirectEmail ||
        verification?.hasVerifiedDirectPhone ||
        verification?.hasVerifiedOrgEmailRoute ||
        verification?.hasVerifiedOrgPhoneRoute,
    ),
    sourceUrlOrEvidence: Boolean(
      verification?.email?.sourceUrl ||
        verification?.phone?.sourceUrl ||
        opportunity.primaryContact?.sourceUrl ||
        opportunity.primaryContact?.channelSourceUrl,
    ),
    sourceTypeAndVerificationTimestamp: Boolean(
      verification?.email?.status || verification?.phone?.status,
    ),
    verificationStatus: Boolean(verification?.overallStatus || verification?.email?.status),
    permissionToRetain: args.permissionToRetain !== false,
    permissionToDisplayToHotelClients:
      args.permissionToDisplayToHotelClients !== false,
    retentionReverificationPolicy: true, // documented default: 365d stale window
    canonicalPersonIdentityKey: Boolean(personKey),
    relatedGdiOpportunityLinks: Boolean(
      opportunity.id || opportunity.opportunityId || args.hotelId,
    ),
    publicSourceOrigin: !isProviderOrigin(origin) || origin === CONTACT_DATA_ORIGIN.PUBLIC_SOURCE,
    liveWritesEnabled: GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED === true,
    founderAuthorizeLiveWrite: args.founderAuthorizeLiveWrite === true,
  };

  if (!checklist.verifiedNamedIdentity) {
    blocks.push(PERSISTENCE_BLOCK_REASONS.NO_NAMED_IDENTITY);
  }
  if (!checklist.canonicalPersonIdentityKey) {
    blocks.push(PERSISTENCE_BLOCK_REASONS.NO_PERSON_IDENTITY_KEY);
  }
  if (!checklist.verifiedEmployerAndRole) {
    blocks.push(PERSISTENCE_BLOCK_REASONS.EMPLOYER_OR_ROLE_UNVERIFIED);
  }
  if (!checklist.professionalEmailOrBusinessPhone) {
    blocks.push(PERSISTENCE_BLOCK_REASONS.NO_VERIFIED_PUBLIC_CHANNEL);
  }
  if (!checklist.sourceUrlOrEvidence) {
    blocks.push(PERSISTENCE_BLOCK_REASONS.MISSING_SOURCE_EVIDENCE);
  }
  if (!checklist.permissionToRetain) {
    blocks.push(PERSISTENCE_BLOCK_REASONS.RETENTION_NOT_PERMITTED);
  }
  if (!checklist.permissionToDisplayToHotelClients) {
    blocks.push(PERSISTENCE_BLOCK_REASONS.CUSTOMER_DISPLAY_NOT_PERMITTED);
  }
  if (isProviderOrigin(origin) && origin !== CONTACT_DATA_ORIGIN.PUBLIC_SOURCE) {
    blocks.push(PERSISTENCE_BLOCK_REASONS.PROVIDER_EPHEMERAL_ORIGIN);
  }
  if (
    verification?.email?.status ===
      PUBLIC_CONTACT_VERIFICATION_STATUS.CANDIDATE_UNVERIFIED ||
    verification?.overallStatus ===
      PUBLIC_CONTACT_VERIFICATION_STATUS.CANDIDATE_UNVERIFIED
  ) {
    blocks.push(PERSISTENCE_BLOCK_REASONS.GUESSED_OR_UNVERIFIED);
  }
  if (!checklist.liveWritesEnabled || !checklist.founderAuthorizeLiveWrite) {
    if (args.excludeLiveWritesDisabledBlock !== true) {
      blocks.push(PERSISTENCE_BLOCK_REASONS.LIVE_WRITES_DISABLED);
    }
  }

  const gatePassed = blocks.length === 0;
  const blocksExcludingLiveWrite = blocks.filter(
    (b) => b !== PERSISTENCE_BLOCK_REASONS.LIVE_WRITES_DISABLED,
  );
  const hypotheticalGatePassedExcludingLiveWrite =
    blocksExcludingLiveWrite.length === 0;
  const dryRunProposal = proposeAirtablePublicContactUpsert(opportunity);

  return {
    version: GDI_PUBLIC_CONTACT_PERSISTENCE_READINESS_V1,
    verificationVersion: GDI_PUBLIC_CONTACT_VERIFICATION_V1,
    execute: false, // hard stop — never write from this module
    gatePassed,
    blocks,
    /** Same assessment with LIVE_WRITES_DISABLED removed (other gates intact). */
    blocksExcludingLiveWrite,
    hypotheticalGatePassedExcludingLiveWrite,
    excludeLiveWritesDisabledBlock: args.excludeLiveWritesDisabledBlock === true,
    checklist,
    personIdentityKey: personKey,
    hotelId: args.hotelId || null,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    channelKinds: {
      email: verification?.email?.channelKind || null,
      phone: verification?.phone?.channelKind || null,
      distinguishDirectVsOrgRoute: {
        directEmail: verification?.hasVerifiedDirectEmail === true,
        orgEmailRoute: verification?.hasVerifiedOrgEmailRoute === true,
        directPhone: verification?.hasVerifiedDirectPhone === true,
        orgPhoneRoute: verification?.hasVerifiedOrgPhoneRoute === true,
      },
    },
    proposedDestination: {
      tableStrategy: "EXISTING_GDO_AND_RESEARCH_INVENTORY_EMBEDDED_PRIMARY_CONTACT",
      createNewContactsTable: false,
      fieldsProposed: dryRunProposal?.fields || null,
      dedupeKey: dryRunProposal?.dedupeKey || personKey,
      overwritePolicy: dryRunProposal?.overwritePolicy || null,
      note:
        "Reuse GDO primaryContact* / research inventory contact block. No new Airtable table without founder approval.",
    },
    rightsMetadata: {
      sourceType: origin || CONTACT_DATA_ORIGIN.PUBLIC_SOURCE,
      permissionToRetain: checklist.permissionToRetain,
      permissionToDisplayToHotelClients: checklist.permissionToDisplayToHotelClients,
      retentionPolicyDays: 365,
      reverificationRequiredAfterDays: 365,
      providerDerivedPersistProhibited: [
        "SURFE",
        "FULLENRICH",
        "PDL",
        "PROSPEO",
        "APOLLO",
      ],
    },
    dryRunProposal,
    airtableSchemaChangesRequired: false,
    remainingFounderDecisions: [
      "Authorize live write flag GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED after field mapping review",
      "Confirm GDO primaryContact* field coverage for sourceUrl / verificationStatus / personIdentityKey / rights",
      "Confirm research-inventory typed columns vs slim payload contact block",
      "Approve cross-hotel reuse read path from Airtable (not only process cache)",
      "PDL / Surfe / FullEnrich storage rights remain separately blocked",
    ],
  };
}

/**
 * Persist hook for workflow — always no-ops unless both flags are true.
 * Even then refuses provider-origin and incomplete readiness.
 */
export async function persistPublicContactChannelsIfAuthorized(args = {}) {
  const readiness = assessPublicContactPersistenceReadiness(args);
  if (!readiness.gatePassed) {
    return {
      ok: false,
      skipped: true,
      reason: readiness.blocks[0] || PERSISTENCE_BLOCK_REASONS.LIVE_WRITES_DISABLED,
      readiness,
      opportunity: args.opportunity,
    };
  }
  // Live path intentionally unimplemented until founder schema + rights approval.
  return {
    ok: false,
    skipped: true,
    reason: "LIVE_PERSIST_IMPLEMENTATION_AWAITING_FOUNDER_APPROVAL",
    readiness,
    opportunity: args.opportunity,
  };
}

export default {
  GDI_PUBLIC_CONTACT_PERSISTENCE_READINESS_V1,
  GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED,
  PERSISTENCE_BLOCK_REASONS,
  assessPublicContactPersistenceReadiness,
  persistPublicContactChannelsIfAuthorized,
};
