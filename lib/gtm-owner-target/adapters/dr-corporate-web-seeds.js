/**
 * Dominican Republic corporate web research seeds (2026-07-04).
 * V1R = named email on entity domain + proof; V2 = named exec + LinkedIn + proof URL.
 */
import { isNamedPersonEmail } from "../registry-contact-verification.js";

/** @typedef {import("./mx-corporate-web-seeds.js").MxCorporateWebSeed} DrCorporateWebSeed */

/** @type {DrCorporateWebSeed[]} */
export const DR_CORPORATE_WEB_SEEDS = [
  {
    slug: "grupo-pinero",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: [
      "Servicios Corporativos Piñero S.L.",
      "Grupo Piñero",
      "Bahia Principe",
    ],
    entityType: "integrated_operator",
    entityName: "Grupo Piñero (Bahia Principe Hotels & Resorts)",
    website: "https://www.pinero.com",
    phone: "+34 971 787 000",
    targetTitles: ["Chief Executive Officer", "President", "Director Comercial"],
    knownContacts: [
      {
        name: "Encarna Piñero",
        title: "Global CEO — Grupo Piñero",
        linkedIn: "https://www.linkedin.com/in/encarnapi%C3%B1ero",
        verificationUrl: "https://www.bahia-principe.com/public/pdf/bio-encarna-en.pdf",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Largest DR hotel owner by room count (Bahia Principe). CoStar owner label: Servicios Corporativos Piñero S.L.",
      "Role mailbox presidencia@grupo-pinero.com on bio PDF — not V1R.",
    ],
  },
  {
    slug: "impressive-resorts-dr",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "RNC",
    ownerNameMatch: ["Impressive Resorts & Spas"],
    entityType: "integrated_operator",
    entityName: "Impressive Hotels & Resorts Punta Cana",
    website: "https://www.impressiveresorts.com",
    targetTitles: ["Director General", "Chief Executive Officer"],
    knownContacts: [
      {
        name: "Oscar Martinez Carrascosa",
        title: "Director General",
        linkedIn: "https://www.linkedin.com/in/oscar-martinez-carrrascosa-7b72336b",
        verificationUrl: "https://do.linkedin.com/company/impressive-hotels-resorts-punta-cana-23000",
        verificationTier: "V2",
        verificationSource: "linkedin",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["Punta Cana all-inclusive operator — Impressive Punta Cana lead asset."],
  },
  {
    slug: "zemi-hotels",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "RNC",
    ownerNameMatch: ["Zemi Hotels & Resorts, S.R.L.", "Zemi Hotels & Resorts"],
    entityType: "private_operator",
    entityName: "Zemi Hotels & Resorts S.R.L.",
    website: "https://www.zemihotels.com",
    targetTitles: ["President", "Chief Executive Officer"],
    knownContacts: [
      {
        name: "Frank Elias Rainieri",
        title: "President & CEO",
        linkedIn: "https://www.linkedin.com/in/frank-elias-rainieri-240a932a",
        verificationUrl: "https://stories.hilton.com/releases/zemi-miches-all-inclusive-resort-curio-collection-by-hilton-dominican-republic-signing",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Miches Curio Collection by Hilton — Frank Rainieri also CEO Grupo Puntacana.",
    ],
  },
  {
    slug: "hodelpa-hotels",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "RNC",
    ownerNameMatch: ["Hodelpa Hotels", "Hodelpa Hospitality"],
    entityType: "integrated_operator",
    entityName: "Hodelpa Hotels & Resorts",
    website: "https://www.hodelpa.com",
    phone: "+1 809-683-3636",
    targetTitles: ["Presidente Ejecutivo", "Chief Executive Officer"],
    knownContacts: [
      {
        name: "Angel Hernandez Rojas",
        title: "Presidente Ejecutivo",
        linkedIn: "https://www.linkedin.com/in/angelhernandezrojas",
        verificationUrl: "https://www.elmundodelosnegocios.com.do/v1/activa-renovacion-hodelpa-hotels-con-nuevo-modelo-gobernanza/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "DR's largest domestic chain (~1,600 rooms). CoStar email ARojas@hodelpa.com — no public corp-page proof for V1R.",
    ],
  },
  {
    slug: "central-romana",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Central Romana Corporation"],
    entityType: "institutional",
    entityName: "Central Romana Corporation, Ltd.",
    website: "https://centralromana.com.do",
    targetTitles: ["President", "Resort President"],
    knownContacts: [
      {
        name: "Andres A Pichardo Rosenberg",
        title: "President — Casa de Campo Resort & Villas",
        linkedIn: "https://www.linkedin.com/in/apichardo",
        verificationUrl: "https://forbes.do/editors-picks/2025-09-10/andres-pichardo-presidente-de-casa-de-campo-habla-sobre-la-reinvencion-de-este-clasico-del-turismo/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "casa_de_campo",
      },
      {
        name: "José Fanjul Jr.",
        title: "President — Central Romana Corporation",
        verificationUrl: "https://centralromana.com.do/en/businessman-jose-fanjul-jr-appointed-as-president-of-central-romana-corporation-ltd/",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "corp_president",
      },
    ],
    researchNotes: [
      "Casa de Campo is hospitality arm — Pichardo is resort brand-decision contact.",
    ],
  },
  {
    slug: "majestic-resorts-dr",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Majestic Resorts"],
    entityType: "integrated_operator",
    entityName: "Majestic Resorts",
    website: "https://www.majestic-resorts.com",
    targetTitles: ["Director General", "Chief Executive Officer"],
    knownContacts: [
      {
        name: "Amil Maleck",
        title: "Director General — Caribbean",
        linkedIn: "https://www.linkedin.com/in/amil-maleck-84356b9a",
        verificationUrl: "https://www.linkedin.com/company/majestic-resorts",
        verificationTier: "V2",
        verificationSource: "linkedin",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Spanish Batle family DR/MX portfolio — Santiago Batle is corporate owner in Spain; Maleck leads DR ops.",
    ],
  },
  {
    slug: "zafera-investments",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "RNC",
    ownerNameMatch: ["Zafera Investments SRL", "Zafera Investment SRL"],
    entityType: "opaque_spv",
    entityName: "Zafera Investments S.R.L.",
    website: "https://www.marriott.com/en-us/hotels/azsak-donoma-las-terrenas-beach-resort-and-spa-autograph-collection/overview/",
    targetTitles: ["Owner", "Developer"],
    knownContacts: [
      {
        name: "Edward González",
        title: "Co-owner / Developer — Donoma Las Terrenas",
        verificationUrl: "https://www.prnewswire.com/news-releases/autograph-collection-hotels-debuts-donoma-las-terrenas-beach-resort--spa-a-distinctive-new-retreat-in-the-dominican-republic-302612954.html",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Autograph Collection Las Terrenas — press names Edward González & Georgette Almanzar; no LinkedIn/email proof.",
    ],
  },
  {
    slug: "green-earth-investments",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: [
      "Delveccio Investments Ltd.",
      "Green Earth Investments Ltd.",
    ],
    entityType: "private_operator",
    entityName: "Green Earth Investments Ltd.",
    website: "https://www.agora.com.do",
    targetTitles: ["Owner", "President"],
    knownContacts: [
      {
        name: "Miguel Barletta",
        title: "Owner — Green Earth Investments Ltd.",
        linkedIn: "https://www.linkedin.com/in/miguel-barletta-274bb05",
        verificationUrl: "https://stories.hilton.com/releases/curio-grows-in-caribbean-with-hotel-santiago",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "CoStar labels Hotel Santiago owner as Delveccio Investments Ltd. — Hilton press names Green Earth / Miguel Barletta.",
    ],
  },
  {
    slug: "vh-hotels-resorts",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "RNC",
    ownerNameMatch: ["VH Hotels & Resorts"],
    entityType: "private_operator",
    entityName: "VH Hotels & Resorts",
    website: "https://www.granventana.com",
    targetTitles: ["Executive Vice President", "President"],
    knownContacts: [
      {
        name: "Roberto Casoni",
        title: "Executive Vice President",
        linkedIn: "https://www.linkedin.com/in/roberto-casoni-70257539",
        verificationUrl: "https://www.caribjournal.com/2013/12/13/talking-tourism-in-the-dominican-republic-with-roberto-casoni/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Puerto Plata boutique portfolio — Gran Ventana, Casa Colonial, Atmosphere.",
    ],
  },
  {
    slug: "noval-properties-dr",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "RNC",
    ownerNameMatch: ["Noval Properties", "Noval SRL", "Grupo Noval"],
    entityType: "developer_owner",
    entityName: "Noval Properties",
    website: "https://www.novalproperties.com",
    phone: "+1 809-552-6221",
    targetTitles: ["Chief Executive Officer", "President"],
    knownContacts: [
      {
        name: "Cesar Latrilla Rodero",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/cesarlatrillarodero",
        verificationUrl: "https://inmobiliario.do/cesar-latrilla-un-amante-de-la-reinvencion/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Juan Nathanael Perez",
        title: "Director — Asset Management & Caribbean Connection",
        linkedIn: "https://www.linkedin.com/in/nathanael-perez-824999a0",
        verificationUrl: "https://www.linkedin.com/company/noval-properties-rd",
        verificationTier: "V2",
        verificationSource: "linkedin",
        outreachRole: "asset_management",
      },
    ],
    researchNotes: [
      "ALIS CALA 2026: Juan Perez (asset manager) on roster — CEO Latrilla is brand-decision contact.",
      "Reserva Real by Harper reflag signal; Aimbridge partnership (2026).",
    ],
  },
  {
    slug: "grupo-puntacana-dr",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Grupo Puntacana S.A.", "Grupo Puntacana", "Puntacana Resort & Club"],
    entityType: "integrated_operator",
    entityName: "Grupo Puntacana",
    website: "https://www.puntacana.com",
    targetTitles: [
      "Vice President Institutional Relations",
      "Chief Executive Officer",
    ],
    knownContacts: [
      {
        name: "Simon Suarez",
        title: "Consultant — Institutional Relations & Projects",
        linkedIn: "https://www.linkedin.com/in/simonbsuarez",
        verificationUrl: "https://www.hotel-online.com/news/simon-suarez-was-named-the-2017-caribbean-hotelier-of-the-year-by-the-carib",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Frank Elias Rainieri",
        title: "Chairman & CEO",
        linkedIn: "https://www.linkedin.com/in/frank-elias-rainieri-240a932a",
        verificationUrl: "https://www.puntacana.com",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "chairman",
      },
    ],
    researchNotes: [
      "ALIS CALA 2026 attendee (Simon Suarez). Frank Rainieri also listed on Zemi Hotels owner row.",
    ],
  },
  {
    slug: "grupo-abrisa-dr",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: [
      "Grupo Abrisa",
      "The Abrisa Group",
      "Rincon Bay",
      "Rincón Bay",
    ],
    entityType: "developer_owner",
    entityName: "Grupo Abrisa",
    website: "https://www.grupoabrisa.com",
    phone: "+1 809-682-0508",
    targetTitles: ["President", "Vice President", "Chief Executive Officer"],
    knownContacts: [
      {
        name: "Abraham Hazoury Toral",
        title: "President — Grupo Abrisa",
        verificationUrl: "https://digitalpuntacana.com/ingeniero-abraham-hazoury-presidente-de-grupo-abrisa/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Jorge Hazoury",
        title: "Vice President",
        linkedIn: "https://www.linkedin.com/in/jorge-hazoury-995b5212",
        verificationUrl: "https://www.grupoabrisa.com/aboutus",
        verificationTier: "V2",
        verificationSource: "linkedin",
        outreachRole: "institutional",
      },
      {
        name: "Rafael Fernandez de Castro",
        title: "Executive President — Rincón Bay / Aeropuerto Internacional de Bávaro",
        verificationUrl: "https://www.elcaribe.com.do/destacado/inauguran-primera-oficina-del-aeropuerto-internacional-de-bavaro/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "rincon_bay_project",
      },
    ],
    researchNotes: [
      "Not in CoStar owner rollup — Cap Cana/Rincón Bay developer. ALIS CALA 2026: Hazoury + Fernandez de Castro.",
      "Rincón Bay master plan: 7 resorts (~4,572 keys) + branded residence pipeline.",
    ],
  },
  {
    slug: "ocama-boutique-dr",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Ocama", "Ocama-Luxury Boutique Hotel", "MPC-DR"],
    entityType: "boutique_developer",
    entityName: "Ocama Luxury Boutique Hotel",
    website: "https://www.ocama.com",
    targetTitles: ["Owner", "Founder", "Developer"],
    knownContacts: [
      {
        name: "Mark Andrus",
        title: "Founder & Owner",
        linkedIn: "https://www.linkedin.com/in/mark-andrus-175503174",
        verificationUrl: "https://www.ocama.com/about/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "ALIS CALA 2026 attendee. 8-villa Samaná retreat — seeking partners for branded boutique expansion.",
      "Prospect-only (no CoStar owner row); track on Dealality acquisition list.",
    ],
  },
  {
    slug: "grupo-santa-maria-dr",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: [
      "Grupo Santa Maria SA",
      "Grupo Santa Maria",
      "GSM Investissements Dominicana S.R.L.",
    ],
    entityType: "developer_owner",
    entityName: "GSM Investissements Dominicana S.R.L.",
    website: "https://groupesantamaria.com",
    targetTitles: ["Founder", "Manager", "President"],
    knownContacts: [
      {
        name: "Georges Santa-Maria",
        title: "Founder — Grupo Santa Maria",
        verificationUrl:
          "https://www.hotel-online.com/news/hyatt-expands-all-inclusive-portfolio-in-the-dominican-republic-with-plans-for-new-secrets-resort-spa-branded-resort",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Dreams Macao Beach + Secrets Macao pipeline — GSM Investissements Dominicana is operating entity.",
    ],
  },
  {
    slug: "mullen-real-estate-capital",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: [
      "Mullen Real Estate Capital",
      "Mullen Hospitality Management",
    ],
    entityType: "institutional",
    entityName: "Mullen Real Estate Capital",
    website: "https://mullenhospitality.com",
    targetTitles: ["Chief Executive Officer", "President"],
    knownContacts: [
      {
        name: "Jeff Mullen",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/jeffmmullen",
        verificationUrl: "https://mullenhospitality.com/about/team/jeffmullen/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Javier Coll",
        title: "President — Mullen Hospitality Management",
        linkedIn: "https://www.linkedin.com/in/javiercoll",
        verificationUrl:
          "https://www.globenewswire.com/news-release/2026/03/09/3252050/0/en/Mullen-Hospitality-Management-Appoints-Javier-Coll-as-President.html",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "operations_president",
      },
    ],
    researchNotes: [
      "Secrets Cap Cana investor/operator supervisor — ALIS CALA 2026 (Javier Coll).",
    ],
  },
  {
    slug: "rizek-group-dr",
    country: "Dominican Republic",
    registrySystem: "DR_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Rizek Group", "Constructora Rizek"],
    entityType: "opaque_spv",
    entityName: "Rizek Group",
    website: "https://www.constructorarizek.com",
    targetTitles: ["President", "Chief Executive Officer"],
    knownContacts: [
      {
        name: "Raúl Nazario Rizek",
        title: "Founder & CEO — Constructora Rizek",
        verificationUrl: "https://thebusinessyear.com/interview/high-ways/",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "CoStar owner label Rizek Group for Dreams Dominicus — no press proof linking Rizek family to asset ownership; V3 research-only.",
    ],
  },
];

/**
 * @param {string} ownerName
 * @returns {DrCorporateWebSeed | null}
 */
export function resolveDrCorporateSeed(ownerName) {
  const norm = String(ownerName || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ");
  if (!norm) return null;

  for (const seed of DR_CORPORATE_WEB_SEEDS) {
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
 * @param {DrCorporateWebSeed} seed
 */
export function pickRecommendedDrOutreachContact(seed) {
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
