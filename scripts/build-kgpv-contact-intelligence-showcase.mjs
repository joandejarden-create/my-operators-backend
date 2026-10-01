#!/usr/bin/env node
/**
 * Build local/dev KGPV Contact Intelligence ViewModel from golden deep-research fixture.
 * No Surfe. No production writes. No invented emails/phones.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const deep = JSON.parse(
  fs.readFileSync(path.join(ROOT, "fixtures/golden-demo/krystal-grand-pv-deep-research-v1.json"), "utf8")
);
const lastVerified = deep.observed_date || "2026-09-03";
const gsf = deep.organizations.find((o) => /Santa Fe/i.test(o.name));
const ihvsf = deep.organizations.find((o) => /IHVSF|Vallarta Santa Fe/i.test(o.name) || o.slug === "ihvsf");
const contacts = deep.corporate_contacts || {};

function channel(type, value, opts = {}) {
  if (!value) return null;
  return {
    type,
    value,
    source: opts.source || "public_filings",
    source_type: opts.source_type || "issuer_disclosure",
    source_url: opts.source_url || null,
    verification_status: opts.verification_status || "VERIFIED_PUBLIC",
    verification_method: opts.verification_method || "public_issuer_disclosure",
    last_verified: opts.last_verified || lastVerified,
    confidence: opts.confidence || "HIGH",
    provider: opts.provider || null,
    client_display_allowed: true,
    discovery_method: opts.discovery_method || "golden_deep_research_fixture",
    note: opts.note || null,
  };
}

const ownerSidePrimary = [
  "Francisco Medina Elizalde",
  "Carlos Gerardo Ancira Elizondo",
  "Francisco Alejandro Zinser Cieslik",
];
const ownerSideSecondary = [
  "Enrique Gerardo Martínez Guerrero",
  "Maximilian Zimmermann",
  "Gabriela Ríos Palacios",
];
const propertySide = ["Luis Avelar", "Norma Garcia"];

function mapPerson(p, group) {
  const emails = [];
  const phones = [];
  if (p.contact && String(p.contact).includes("@")) {
    emails.push(
      channel("PERSON_BUSINESS_EMAIL", p.contact, {
        verification_status: p.contact_verified ? "VERIFIED_PUBLIC" : "PUBLICLY_PUBLISHED",
        source: "bmv_issuer_profile",
        source_type: "issuer_ir",
        source_url: contacts.website || "https://gsf-hotels.com",
      })
    );
  }
  const profiles = [];
  if (p.professional_profile_url && p.professional_profile_verified) {
    profiles.push({
      type: p.professional_profile_type || "LINKEDIN",
      url: p.professional_profile_url,
      verified: true,
      verification_status: "VERIFIED_PUBLIC",
      last_verified: lastVerified,
    });
  }
  const stakeholder = /Beneficial|Principal|Board/i.test(p.category || "")
    ? "Owner / Principal"
    : /Development/i.test(p.category || "")
      ? "Investment / Development"
      : /Investor Relations/i.test(p.title || "") || /IR\b/i.test(p.title || "")
        ? "Investor Relations"
        : /Asset Management|CFO/i.test(p.category || "")
          ? "Corporate Executive"
          : /Property/i.test(p.category || "")
            ? "Property Leadership"
            : /Operator|Operations/i.test(p.category || "")
              ? "Operations"
              : "Corporate Executive";

  const hasDirect = emails.length > 0 || phones.length > 0;
  return {
    personId: "person_" + String(p.name).toLowerCase().replace(/[^a-z0-9]+/g, "_"),
    name: p.name,
    title: p.title,
    organization: p.organization,
    stakeholderClass: stakeholder,
    group,
    whyRelevant: p.relationship_to_hotel || p.strategic_relevance || "",
    relationshipToHotel: p.relationship_to_hotel,
    emailContacts: emails,
    phoneContacts: phones,
    profileUrls: profiles,
    contactability: hasDirect ? "DIRECT_CHANNEL" : profiles.length ? "PROFILE_ONLY" : "ORG_FALLBACK",
    confidence: p.confidence || "HIGH",
    lastVerified,
    decisionAuthority: p.decision_authority || "Authority Not Verified",
    signingAuthority: p.legal_signing_authority || "Authority Not Verified",
    verificationNotes: p.authority_note || null,
    displayState: hasDirect ? "AVAILABLE" : profiles.length ? "PARTIAL" : "UNRESOLVED",
    fallbackPath: hasDirect
      ? null
      : {
          label: "Use owner organization contact path",
          organization: "Grupo Hotelero Santa Fe, S.A.B. de C.V.",
          channels: [
            contacts.business_email && {
              type: "ORG_GENERAL_EMAIL",
              value: contacts.business_email,
              label: "Investor Relations",
            },
            contacts.corporate_phone && {
              type: "ORG_MAIN_PHONE",
              value: contacts.corporate_phone,
              label: "Corporate office",
            },
            contacts.website && { type: "ORG_CONTACT_PAGE", value: contacts.website, label: "Company website" },
          ].filter(Boolean),
        },
    enrichAction: {
      label: "Find Contact Details",
      enabled: false,
      reason: "Surfe on-demand disabled in showcase",
    },
    sources: p.sources || [],
  };
}

const people = [];
for (const p of deep.people || []) {
  if (/Hyatt/i.test(p.organization || "")) continue;
  if (ownerSidePrimary.includes(p.name)) people.push(mapPerson(p, "PRIMARY_OWNER_SIDE"));
  else if (ownerSideSecondary.includes(p.name)) people.push(mapPerson(p, "SECONDARY_OWNER_CORPORATE"));
  else if (propertySide.includes(p.name)) people.push(mapPerson(p, "OPERATOR_PROPERTY"));
}

const orgEmails = [
  channel("ORG_GENERAL_EMAIL", contacts.business_email, {
    verification_status: "VERIFIED_PUBLIC",
    source: "bmv_issuer_profile",
    source_url: contacts.website,
    source_type: "issuer_ir",
  }),
].filter(Boolean);
const orgPhones = [
  channel("ORG_MAIN_PHONE", contacts.corporate_phone, {
    verification_status: "VERIFIED_PUBLIC",
    source: "gsf_corporate",
    source_url: contacts.website,
  }),
].filter(Boolean);

const propertyPhone = channel("ORG_MAIN_PHONE", contacts.property_phone, {
  verification_status: "PUBLICLY_PUBLISHED",
  source_type: "hotel_website",
  source_url: contacts.property_website,
});

const allVerifiedEmails = [...orgEmails, ...people.flatMap((p) => p.emailContacts)].filter((c) =>
  /VERIFIED/.test(c.verification_status)
);
const allPublicPhones = [...orgPhones, propertyPhone].filter(Boolean);

const vm = {
  version: "contact-intelligence-viewmodel-v1",
  staging: true,
  production_writes: false,
  surfe_live_calls: 0,
  newly_researched: false,
  source_fixture: "fixtures/golden-demo/krystal-grand-pv-deep-research-v1.json",
  hotelId: deep.hotel_airtable_record_id,
  hotelName: "Krystal Grand Puerto Vallarta",
  lastReviewed: lastVerified,
  ownershipSummary: {
    hotel: { name: "Krystal Grand Puerto Vallarta", role: "HOTEL" },
    propco: {
      name: ihvsf?.name || "Inmobiliaria en Hotelería Vallarta Santa Fe, S. de R.L. de C.V. (IHVSF)",
      role: "PROPERTY_OWNER_LEGAL_ENTITY",
      label: "Property Owner / PropCo",
      shortName: "IHVSF",
    },
    sponsor: {
      name: gsf?.name || "Grupo Hotelero Santa Fe, S.A.B. de C.V.",
      role: "OWNER_SPONSOR_GROUP",
      label: "Economic Owner / Contactable Owner Org",
    },
    operator: {
      name: "Grupo Hotelero Santa Fe",
      role: "OPERATOR",
      label: "Current Operator",
      sameAsSponsor: true,
    },
    brand: { name: "Krystal Grand", role: "CURRENT_BRAND", label: "Current Brand" },
    formerOwner: {
      name: "Grupo Chartwell",
      role: "FORMER_CO_OWNER",
      label: "Former co-owner",
      status: "FORMER",
    },
    brandHistory: [
      { name: "Hilton Puerto Vallarta", status: "FORMER" },
      { name: "Krystal Altitude", status: "FORMER" },
      {
        name: "Breathless Puerto Vallarta",
        status: "ANNOUNCED",
        note: "Announced future conversion — not current",
      },
    ],
  },
  qualitySummary: {
    ownerOrgStatus: "VERIFIED",
    primaryDecisionMakers: people.filter((p) => p.group === "PRIMARY_OWNER_SIDE").length,
    verifiedEmails: allVerifiedEmails.length,
    verifiedOrPublicPhones: allPublicPhones.length,
    fallbackContactPaths: people.filter((p) => p.fallbackPath).length,
    lastReviewed: lastVerified,
  },
  contactableOwnerOrg: {
    organizationId: "dle_06G6AB1VK0BCCD94DNN7W8DRWZ",
    name: gsf?.name || "Grupo Hotelero Santa Fe, S.A.B. de C.V.",
    entityType: "Public company (BMV: HOTEL)",
    relationship: "Economic owner / sponsor and current operator",
    relationshipBadges: ["OWNER / SPONSOR", "OPERATOR"],
    hq: gsf?.hq || contacts.headquarters,
    website: contacts.website || gsf?.website,
    phones: orgPhones,
    emails: orgEmails,
    contactPage: contacts.website || null,
    linkedInCompany: null,
    verification: "VERIFIED_PUBLIC",
    confidence: "HIGH",
    lastVerified,
    sourceCount: (deep.sources || []).length,
    displayState: "AVAILABLE",
  },
  organizations: [
    {
      organizationId: "org_ihvsf",
      name: ihvsf?.name || "Inmobiliaria en Hotelería Vallarta Santa Fe, S. de R.L. de C.V.",
      shortName: "IHVSF",
      role: "PROPERTY_OWNER_LEGAL_ENTITY",
      relationship: "Legal ownership entity / PropCo",
      relationshipBadges: ["LEGAL OWNERSHIP ENTITY"],
      website: null,
      phones: [],
      emails: [],
      contactPage: null,
      verification: "VERIFIED_PUBLIC",
      confidence: "HIGH",
      lastVerified,
      displayState: "PARTIAL",
      noSeparatePublicContactPath: true,
      useParentContactPath: "Grupo Hotelero Santa Fe, S.A.B. de C.V.",
      note: "Legal owner identified. No separate public contact path confirmed. Use parent/sponsor contact path.",
    },
    {
      organizationId: "dle_06G6AB1VK0BCCD94DNN7W8DRWZ",
      name: gsf?.name || "Grupo Hotelero Santa Fe, S.A.B. de C.V.",
      role: "CONTACTABLE_OWNER_ORG",
      relationship: "Economic owner / contactable owner org; also current operator",
      relationshipBadges: ["OWNER / SPONSOR", "OPERATOR"],
      website: contacts.website,
      phones: orgPhones,
      emails: orgEmails,
      contactPage: contacts.website,
      verification: "VERIFIED_PUBLIC",
      confidence: "HIGH",
      lastVerified,
      displayState: "AVAILABLE",
      sameOrganizationAlsoOperator: true,
    },
    {
      organizationId: "org_krystal_grand_brand",
      name: "Krystal Grand",
      role: "CURRENT_BRAND",
      relationship: "Current brand (not owner contact path)",
      relationshipBadges: ["CURRENT BRAND"],
      website: contacts.property_website || null,
      phones: propertyPhone ? [propertyPhone] : [],
      emails: contacts.property_email
        ? [
            channel("ORG_GENERAL_EMAIL", contacts.property_email, {
              verification_status: "PUBLICLY_PUBLISHED",
              source_type: "hotel_website",
              source_url: contacts.property_website,
              note: "Reservations mailbox — not owner contact",
            }),
          ]
        : [],
      contactPage: contacts.property_website,
      verification: "PUBLICLY_PUBLISHED",
      confidence: "HIGH",
      lastVerified,
      displayState: "PARTIAL",
      brandNotOwner: true,
      note: "Brand / property guest channels — do not confuse with owner contact.",
    },
  ],
  people,
  bestContactPaths: [
    {
      rank: 1,
      label: "GSF Investor Relations",
      organization: gsf?.name,
      value: contacts.business_email,
      type: "ORG_GENERAL_EMAIL",
      verification_status: "VERIFIED_PUBLIC",
    },
    {
      rank: 2,
      label: "GSF corporate phone",
      organization: gsf?.name,
      value: contacts.corporate_phone,
      type: "ORG_MAIN_PHONE",
      verification_status: "VERIFIED_PUBLIC",
    },
    {
      rank: 3,
      label: "GSF website",
      organization: gsf?.name,
      value: contacts.website,
      type: "ORG_CONTACT_PAGE",
      verification_status: "VERIFIED_OFFICIAL",
    },
  ],
  unresolvedItems: [
    "No verified person-direct business phones for owner-side principals",
    "Most owner-side executives: profile verified; direct email unresolved",
    "IHVSF PropCo: no separate public contact path",
    "Natural-person UBO beyond disclosed public-company / Chartwell principals",
    "Property GM tenure confirmation (Luis Avelar listing-only)",
  ],
  evidenceSummary: {
    sourceCount: (deep.sources || []).length,
    stagingSource: "golden_deep_research_fixture",
    resolverPath: "local_showcase_fixture",
    lastReviewed: lastVerified,
  },
};

const pubDir = path.join(ROOT, "public/data/hotel-contact-intelligence");
const fixDir = path.join(ROOT, "fixtures/hotel-intelligence/contact-intelligence");
fs.mkdirSync(pubDir, { recursive: true });
fs.mkdirSync(fixDir, { recursive: true });
fs.writeFileSync(path.join(pubDir, "kgpv-recUNycnMwOVFX0hc.json"), JSON.stringify(vm, null, 2));
fs.writeFileSync(path.join(fixDir, "kgpv-contact-intelligence-showcase-v1.json"), JSON.stringify(vm, null, 2));
console.log(
  JSON.stringify(
    {
      people: people.length,
      emails: vm.qualitySummary.verifiedEmails,
      phones: vm.qualitySummary.verifiedOrPublicPhones,
      fallbacks: vm.qualitySummary.fallbackContactPaths,
      primary: people.filter((p) => p.group === "PRIMARY_OWNER_SIDE").map((p) => p.name),
    },
    null,
    2
  )
);
