/**
 * Costa Rica corporate web research seeds (2026-07-05).
 */
import { isNamedPersonEmail } from "../registry-contact-verification.js";

/** @typedef {import("./mx-corporate-web-seeds.js").MxCorporateWebSeed} CrCorporateWebSeed */

/** @type {CrCorporateWebSeed[]} */
export const CR_CORPORATE_WEB_SEEDS = [
  {
    slug: "caribe-hospitality-cr",
    country: "Costa Rica",
    registrySystem: "CR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Caribe Hospitality"],
    entityType: "institutional",
    entityName: "Caribe Hospitality",
    website: "https://caribehospitality.com",
    phone: "+506 2208 8890",
    targetTitles: ["Director General", "CEO", "Partner"],
    knownContacts: [
      {
        name: "Daniel Campos",
        title: "Director General / Partner",
        email: "dcampos@caribehospitality.com",
        linkedIn: "https://www.linkedin.com/in/dcamposlara",
        verificationUrl: "https://interihotel.com/en/speakers/ihbcn25/daniel-campos-en",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Strike-list CALA Marriott/Hyatt developer — ALIS CALA 2026 attendee.",
      "CoStar email dcampos@caribehospitality.com — entity domain; corp proof via speaker bio.",
    ],
  },
  {
    slug: "grupo-leumi-cr",
    country: "Costa Rica",
    registrySystem: "CR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Grupo Leumi"],
    entityType: "developer_owner",
    entityName: "Grupo Leumi",
    website: "https://grupoleumi.com",
    targetTitles: ["President", "Vice President"],
    knownContacts: [
      {
        name: "Stanley Rattner",
        title: "President",
        linkedIn: "https://www.linkedin.com/in/stanley-rattner-896773112",
        verificationUrl: "https://ekaenlinea.com/tras-una-inversion-de-40-millones-abrira-sus-el-sabana-business-center/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "León Bibas Merenfeld",
        title: "Vice President",
        verificationUrl: "https://apetitoenlinea.com/un-hotel-business-center-y-restaurantes-de-cadena-internacional/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "vp",
      },
    ],
    researchNotes: [
      "Gran Hotel Costa Rica (Curio), Hilton Garden Inn Sabana — mixed-use hotel developer.",
    ],
  },
  {
    slug: "boena-lodges-cr",
    country: "Costa Rica",
    registrySystem: "CR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Böëna Lodges", "Boena Lodges", "Böëna Wilderness Lodges"],
    entityType: "owner_operator",
    entityName: "Böëna Wilderness Lodges",
    website: "https://boena.com",
    phone: "+506 4070-0420",
    targetTitles: ["Managing Director", "Chief Executive Officer", "Chief Marketing Officer"],
    knownContacts: [
      {
        name: "Jack Loeb",
        title: "Managing Director",
        email: "jack@boena.com",
        phone: "+506 8851-9276",
        linkedIn: "https://www.linkedin.com/in/jack-loeb",
        verificationUrl:
          "https://www.linkedin.com/pulse/b%C3%B6%C3%ABna-wilderness-lodges-embarks-new-chapter-jack-6owee",
        verificationTier: "V1R",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Flavia Loeb",
        title: "Chief Marketing Officer",
        email: "flavia@boena.com",
        linkedIn: "https://www.linkedin.com/in/flavialoeb",
        verificationUrl:
          "https://www.linkedin.com/pulse/b%C3%B6%C3%ABna-wilderness-lodges-embarks-new-chapter-jack-6owee",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "cmo",
      },
    ],
    researchNotes: [
      "Five-lodge eco portfolio — Pacuare Lodge flagship. Jack Loeb sole leadership since Dec 2023.",
      "jack@boena.com published on official LinkedIn company announcement (Dec 2023).",
    ],
  },
  {
    slug: "alojica-cr",
    country: "Costa Rica",
    registrySystem: "CR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Alojica", "Alójica"],
    entityType: "institutional",
    entityName: "Alójica",
    website: "https://alojica.com",
    targetTitles: ["Managing Partner", "Vice President Asset Management", "Chief Investment Officer"],
    knownContacts: [
      {
        name: "Ana Maria Añez",
        title: "Vice President — Asset Management",
        linkedIn: "https://www.linkedin.com/in/anamariaanez",
        verificationUrl: "https://alojica.com/team/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Oriol Gimenez",
        title: "Managing Partner & Chief Investment Officer",
        linkedIn: "https://www.linkedin.com/in/oriol-gimenez-1072175",
        verificationUrl: "https://alojica.com/team/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "cio",
      },
    ],
    researchNotes: [
      "Black Creek Mexico / CPG Hospitality lodging investment platform — CR assets under sub-advisory.",
      "Fiesta Resort, Marriott Hacienda Belen, Los Suenos — VP Asset Management oversees CR portfolio.",
    ],
  },
];

/**
 * @param {string} ownerName
 * @returns {CrCorporateWebSeed | null}
 */
export function resolveCrCorporateSeed(ownerName) {
  const norm = String(ownerName || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ");
  if (!norm) return null;

  for (const seed of CR_CORPORATE_WEB_SEEDS) {
    for (const alias of seed.ownerNameMatch) {
      const aliasNorm = alias.toLowerCase().replace(/[^a-z0-9]+/g, " ");
      if (norm === aliasNorm || norm.includes(aliasNorm) || aliasNorm.includes(norm)) {
        return seed;
      }
    }
  }
  return null;
}

/**
 * @param {CrCorporateWebSeed} seed
 */
export function pickRecommendedCrOutreachContact(seed) {
  const contacts = seed.knownContacts || [];
  const withNamedEmail = contacts.find(
    (c) => c.email && isNamedPersonEmail(c.email, c.name)
  );
  if (withNamedEmail) return withNamedEmail;
  const primary = contacts.find((c) => c.outreachRole === "primary" && (c.email || c.linkedIn));
  if (primary) return primary;
  const withLinkedIn = contacts.find((c) => c.linkedIn);
  if (withLinkedIn) return withLinkedIn;
  return contacts[0] || null;
}
