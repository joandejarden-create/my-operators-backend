/**
 * Bethesda Marriott — HI Org Contacts controlled-apply mutation package.
 * PUBLIC_SOURCE only. No Surfe. No Economic/Legal Owner edge creation.
 */
import crypto from "node:crypto";
import { orgDedupeKey, personDedupeKey, channelDedupeKey } from "../research-engine-v1/entity-keys.js";
import {
  HE50_BETHESDA_HPC_ID,
} from "./contract.js";
import {
  HE50_HI_ORG_CONTACTS_TABLE_ID,
  HE50_HI_CONTACT_CHANNELS_TABLE_ID,
  HE50_HI_ORGANIZATIONS_TABLE_ID,
} from "./standing-authorization-v1.js";

export const HE50_BETH_CONTACTS_PACKAGE_V1 = "he50-beth-hi-org-contacts-v1";

function stableHex(seed, len = 26) {
  return crypto
    .createHash("sha256")
    .update(String(seed))
    .digest("hex")
    .slice(0, len)
    .toUpperCase();
}
function dlpId(seed) {
  return `dlp_${stableHex(seed || crypto.randomBytes(16).toString("hex"))}`;
}
function dlcId(seed) {
  return `dlc_${stableHex(seed || crypto.randomBytes(16).toString("hex"))}`;
}
function dleId(seed) {
  return `dle_${stableHex(seed)}`;
}

const MULLEN = {
  legalName: "Mullen Real Estate Capital",
  tradingName: "Mullen Real Estate Capital",
  organizationType: "Asset Manager",
  jurisdiction: "US-PA",
  website: "https://mullencap.com/",
  orgId: null, // filled at build
  dedupeKey: null,
};

const PEOPLE = [
  {
    slug: "katherine_button",
    displayName: "Katherine Button",
    title: "VP, Asset Management and Investments",
    functionRole: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    propertyResponsibility: "INFERRED",
    hotelResponsibility:
      "RELEVANT_COMMERCIAL_ENTRY_POINT — senior AM/investments at proposed economic sponsor; property-specific assignment unconfirmed. HE50_PILOT_HOTEL:recLuxvwwxID7U2B8",
    roleCurrency: "CURRENT_PROBABLE",
    confidence: "HIGH",
    linkedIn: "https://www.linkedin.com/in/katherine-button/",
    sources: ["https://mullencap.com/about/", "https://www.linkedin.com/in/katherine-button/"],
    verificationStates: {
      PERSON_IDENTITY_VERIFIED: true,
      CURRENT_ROLE_VERIFIED: false,
      ORGANIZATION_AFFILIATION_VERIFIED: true,
      HOTEL_ORGANIZATION_RELATIONSHIP_VERIFIED: false,
      PERSON_PROPERTY_RESPONSIBILITY_VERIFIED: false,
      PUBLIC_CONTACT_CHANNEL_VERIFIED: false,
    },
  },
  {
    slug: "jeff_mullen",
    displayName: "Jeff Mullen",
    title: "President and CEO",
    functionRole: "VERIFIED_ORGANIZATION_CONTACT",
    propertyResponsibility: "INFERRED",
    hotelResponsibility:
      "VERIFIED_ORGANIZATION_CONTACT — President/CEO of proposed economic sponsor; not automatically day-to-day AM for Bethesda Marriott. HE50_PILOT_HOTEL:recLuxvwwxID7U2B8",
    roleCurrency: "CURRENT_PROBABLE",
    confidence: "HIGH",
    linkedIn: "https://www.linkedin.com/in/jeffmmullen/",
    sources: ["https://mullencap.com/about/", "https://www.linkedin.com/in/jeffmmullen/"],
    verificationStates: {
      PERSON_IDENTITY_VERIFIED: true,
      CURRENT_ROLE_VERIFIED: false,
      ORGANIZATION_AFFILIATION_VERIFIED: true,
      HOTEL_ORGANIZATION_RELATIONSHIP_VERIFIED: false,
      PERSON_PROPERTY_RESPONSIBILITY_VERIFIED: false,
      PUBLIC_CONTACT_CHANNEL_VERIFIED: false,
    },
  },
  {
    slug: "patrick_reece",
    displayName: "Patrick Reece",
    title: "Director, Asset Management",
    functionRole: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    propertyResponsibility: "UNKNOWN",
    hotelResponsibility:
      "RELEVANT_COMMERCIAL_ENTRY_POINT — director-level AM referral; property responsibility not confirmed. HE50_PILOT_HOTEL:recLuxvwwxID7U2B8",
    roleCurrency: "CURRENT_PROBABLE",
    confidence: "MEDIUM",
    linkedIn: "https://www.linkedin.com/in/patrick-reece-375b8937/",
    sources: [
      "https://mullencap.com/about/",
      "https://www.linkedin.com/in/patrick-reece-375b8937/",
    ],
    verificationStates: {
      PERSON_IDENTITY_VERIFIED: true,
      CURRENT_ROLE_VERIFIED: false,
      ORGANIZATION_AFFILIATION_VERIFIED: true,
      HOTEL_ORGANIZATION_RELATIONSHIP_VERIFIED: false,
      PERSON_PROPERTY_RESPONSIBILITY_VERIFIED: false,
      PUBLIC_CONTACT_CHANNEL_VERIFIED: false,
    },
  },
];

const ORG_CHANNELS = [
  {
    kind: "Email",
    value: "info@mullencap.com",
    classification: "General / Switchboard",
    verificationStatus: "Public Listed",
    sourceUrl: "https://mullencap.com/contact/",
    notes: "VERIFIED_ORGANIZATION_EMAIL — not a named personal inbox",
  },
  {
    kind: "Telephone",
    value: "(610) 742-9167",
    classification: "General / Switchboard",
    verificationStatus: "Public Listed",
    sourceUrl:
      "https://www.sec.gov/Archives/edgar/data/2080980/0002080980-25-000001.txt",
    notes: "VERIFIED_ORGANIZATION_PHONE — SEC Form D business line; not named direct",
  },
  {
    kind: "Other",
    value: "https://mullencap.com/",
    classification: "Public",
    verificationStatus: "Public Listed",
    sourceUrl: "https://mullencap.com/",
    notes: "Organization website",
  },
];

/**
 * Build dry-run / apply mutations for Bethesda HI Org Contacts wave.
 */
export function buildBethHiOrgContactsMutationPackage(opts = {}) {
  const dedupe = orgDedupeKey({
    legalName: MULLEN.legalName,
    jurisdiction: MULLEN.jurisdiction,
  });
  // Stable org id from dedupe so re-runs stay idempotent without Airtable lookup.
  const orgId = opts.orgId || dleId(`he50-org:${dedupe}`);
  MULLEN.orgId = orgId;
  MULLEN.dedupeKey = dedupe;

  const mutations = [];
  const peopleOut = [];

  mutations.push({
    id: "M-BETH-ORG-MULLEN-001",
    action: "UPSERT",
    destinationTable: HE50_HI_ORGANIZATIONS_TABLE_ID,
    field: "organization",
    canonicalEntityId: HE50_BETHESDA_HPC_ID,
    verificationStatus: "PUBLICLY_VERIFIED",
    founderRecommendation: "READY",
    supportingSource: "https://mullencap.com/about/",
    reason:
      "Employer shell for verified organizational contacts — NOT an Economic/Legal Owner hotel edge",
    proposedValue: {
      orgId,
      legalName: MULLEN.legalName,
      tradingName: MULLEN.tradingName,
      organizationType: MULLEN.organizationType,
      jurisdiction: MULLEN.jurisdiction,
      officialWebsite: MULLEN.website,
      dedupeKey: dedupe,
      verificationState: "Evidence Collected",
      confidence: "MEDIUM",
      publicationState: "INTERNAL_ONLY",
      provenanceSummary:
        "Public company site + prior HE research. Proposed economic sponsor association only — not verified legal owner.",
      notes:
        "HE50_CONTACT_EMPLOYER · proposed economic sponsor for Bethesda Marriott · DO NOT treat as verified legal PropCo. HE50_PILOT_HOTEL:recLuxvwwxID7U2B8",
      promoteOwnershipRelationship: false,
      createEconomicOwnerEdge: false,
    },
  });

  for (const p of PEOPLE) {
    const personId =
      opts.personIds?.[p.slug] || dlpId(`he50-person:${p.slug}:${dedupe}`);
    const affiliationKey = personDedupeKey({
      displayName: p.displayName,
      orgDedupe: dedupe,
    });
    const personKey = personDedupeKey({
      displayName: p.displayName,
      orgId: "identity",
    });
    peopleOut.push({ ...p, personId, affiliationKey, personKey });

    mutations.push({
      id: `M-BETH-CONTACT-${p.slug}`,
      action: "UPSERT",
      destinationTable: HE50_HI_ORG_CONTACTS_TABLE_ID,
      field: "org_contact",
      canonicalEntityId: HE50_BETHESDA_HPC_ID,
      verificationStatus: "PUBLICLY_VERIFIED",
      founderRecommendation: "READY",
      supportingSource: p.sources.join(" | "),
      reason: `${p.functionRole}; property responsibility ${p.propertyResponsibility}`,
      proposedValue: {
        personId,
        displayName: p.displayName,
        organizationId: orgId,
        title: p.title,
        functionRole: p.functionRole,
        hotelResponsibility: p.hotelResponsibility,
        affiliationVerified: true,
        roleCurrency: p.roleCurrency,
        confidence: p.confidence,
        verificationState: "Evidence Collected",
        personDedupeKey: personKey,
        affiliationDedupeKey: affiliationKey,
        publicationState: "INTERNAL_ONLY",
        surfeDerived: false,
        notes: JSON.stringify({
          he50_pilot_hotel: HE50_BETHESDA_HPC_ID,
          propertyResponsibility: p.propertyResponsibility,
          verificationStates: p.verificationStates,
          sources: p.sources,
          lastVerified: "2026-11-20",
        }),
      },
    });

    // LinkedIn professional profile channel (person subject)
    const liDedupe = channelDedupeKey({
      subjectId: personId,
      kind: "LinkedIn",
      normalizedValue: p.linkedIn,
    });
    mutations.push({
      id: `M-BETH-CH-LI-${p.slug}`,
      action: "UPSERT",
      destinationTable: HE50_HI_CONTACT_CHANNELS_TABLE_ID,
      field: "contact_channel",
      canonicalEntityId: HE50_BETHESDA_HPC_ID,
      verificationStatus: "PUBLICLY_VERIFIED",
      founderRecommendation: "READY",
      supportingSource: p.linkedIn,
      proposedValue: {
        channelId: dlcId(`he50-ch-li:${p.slug}`),
        subjectType: "Person",
        subjectId: personId,
        channelKind: "LinkedIn",
        channelValue: p.linkedIn,
        classification: "Public",
        verificationStatus: "Public Listed",
        sourceName: "LinkedIn public profile",
        sourceUrl: p.linkedIn,
        observedAt: "2026-11-20",
        provider: "public_web",
        usageProvenance: "PUBLIC_SOURCE",
        accessRestriction: "INTERNAL_ONLY",
        dedupeKey: liDedupe,
        publicationState: "CUSTOMER_BLOCKED",
        surfeDerived: false,
        notes: "VERIFIED_PROFESSIONAL_PROFILE",
      },
    });
  }

  // Organization-level channels once (not per person)
  for (const [i, ch] of ORG_CHANNELS.entries()) {
    const dedupe = channelDedupeKey({
      subjectId: orgId,
      kind: ch.kind,
      normalizedValue: ch.value,
    });
    mutations.push({
      id: `M-BETH-CH-ORG-${i + 1}`,
      action: "UPSERT",
      destinationTable: HE50_HI_CONTACT_CHANNELS_TABLE_ID,
      field: "contact_channel",
      canonicalEntityId: HE50_BETHESDA_HPC_ID,
      verificationStatus: "PUBLICLY_VERIFIED",
      founderRecommendation: "READY",
      supportingSource: ch.sourceUrl,
      proposedValue: {
        channelId: dlcId(`he50-ch-org:${i + 1}:${dedupe}`),
        subjectType: "Organization",
        subjectId: orgId,
        channelKind: ch.kind,
        channelValue: ch.value,
        classification: ch.classification,
        verificationStatus: ch.verificationStatus,
        sourceName: "Public organization contact",
        sourceUrl: ch.sourceUrl,
        observedAt: "2026-11-20",
        provider: "public_web",
        usageProvenance: "PUBLIC_SOURCE",
        accessRestriction: "INTERNAL_ONLY",
        dedupeKey: dedupe,
        publicationState: "CUSTOMER_BLOCKED",
        surfeDerived: false,
        notes: ch.notes,
      },
    });
  }

  // Evidence: contacts relevant to hotel without ownership edge
  mutations.push({
    id: "M-BETH-EV-CONTACTS-001",
    action: "CREATE",
    destinationTable: "tblc9JhKT1edr4grE",
    field: "contact_intelligence_scope",
    canonicalEntityId: HE50_BETHESDA_HPC_ID,
    verificationStatus: "EVIDENCE_ONLY",
    founderRecommendation: "READY",
    supportingSource: "https://mullencap.com/about/",
    reason:
      "Scope evidence linking PUBLIC_SOURCE Mullen contacts to Bethesda pilot without Economic Owner edge",
    proposedValue: {
      orgId,
      people: peopleOut.map((p) => ({
        personId: p.personId,
        name: p.displayName,
        functionRole: p.functionRole,
        propertyResponsibility: p.propertyResponsibility,
      })),
      note: "RELEVANT_COMMERCIAL_ENTRY_POINT / VERIFIED_ORGANIZATION_CONTACT — not confirmed property AM",
    },
  });

  return {
    version: HE50_BETH_CONTACTS_PACKAGE_V1,
    hotelId: HE50_BETHESDA_HPC_ID,
    hotelName: "Bethesda Marriott",
    generatedAt: new Date().toISOString(),
    ownershipEdgesCreated: false,
    org: { ...MULLEN, orgId, dedupeKey: dedupe },
    people: peopleOut,
    mutationCount: mutations.length,
    mutations,
  };
}
