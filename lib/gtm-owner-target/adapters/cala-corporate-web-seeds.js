/**
 * CALA (non-Mexico) corporate web research seeds for brand-decision owner outreach.
 * V1R = named email on entity domain; V2 = named exec + LinkedIn + proof URL.
 */
import { isNamedPersonEmail } from "../registry-contact-verification.js";

/** @typedef {import("./mx-corporate-web-seeds.js").MxCorporateWebSeed} CalaCorporateWebSeed */

/** @type {CalaCorporateWebSeed[]} */
export const CALA_CORPORATE_WEB_SEEDS = [
  {
    slug: "gaviota",
    country: "Cuba",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Grupo de Turismo Gaviota", "Gaviota Grupo de Turismo"],
    entityType: "foreign_hq",
    entityName: "Grupo de Turismo Gaviota S.A.",
    website: "https://www.gaviota-grupo.com",
    phone: "+53 7 866 0811",
    targetTitles: ["President", "Director General", "Director Comercial"],
    knownContacts: [
      {
        name: "Carlos Latuff",
        title: "President",
        linkedIn: "https://www.linkedin.com/in/carlos-latuff-64547b222",
        verificationUrl: "https://www.reportur.com/mexico/2018/06/09/ranking-reportur-los-12-lideres-del-turismo-cubano/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Arturo de la Torre Diaz",
        title: "Director General",
        linkedIn: "https://www.linkedin.com/in/arturo-de-la-torre-diaz-aa7678123",
        verificationUrl: "https://www.gaviota-grupo.com/en/business",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "director_general",
      },
    ],
    researchNotes: ["Cuban state tourism conglomerate — 80+ hotels CALA footprint."],
  },
  {
    slug: "essendi",
    country: "France",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Essendi", "AccorInvest"],
    entityType: "foreign_hq",
    entityName: "Essendi (formerly AccorInvest)",
    website: "https://www.essendi.com",
    targetTitles: ["Chief Executive Officer", "Chief Operating Officer", "Asset Manager"],
    knownContacts: [
      {
        name: "Gilles Clavié",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/gilles-clavie-65a26757",
        verificationUrl: "https://www.essendi.com/en/about-us/our-governance",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Colombia/CALA assets (e.g. MGallery Cartagena) in divestiture — CEO still brand-decision contact.",
    ],
  },
  {
    slug: "urbanova",
    country: "Peru",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "RUC",
    ownerNameMatch: ["Urbanova", "Urbanova Inmobiliaria"],
    entityType: "private_operator",
    entityName: "Urbanova Inmobiliaria S.A.C.",
    website: "https://www.urbanova.com",
    phone: "+51 1 200 0222",
    targetTitles: ["Chief Executive Officer", "Director of Hospitality", "Asset Manager"],
    knownContacts: [
      {
        name: "Giacomo Sissa",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/giacomo-sissa-78a02b25",
        verificationUrl: "https://www.prnewswire.com/news-releases/highgate-to-operate-award-winning-portfolio-of-hotels-in-peru-301503020.html",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Maximiliano Pazo",
        title: "Director of Hospitality Business",
        verificationUrl: "https://pe.linkedin.com/company/urbanova-inmobiliaria",
        verificationTier: "V3",
        verificationSource: "linkedin",
        outreachRole: "hospitality",
      },
    ],
    researchNotes: [
      "Peru luxury Marriott portfolio (Palacio del Inka, Westin Lima, etc.) — Highgate managed.",
    ],
  },
  {
    slug: "interlink-group",
    country: "Puerto Rico",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Interlink Group", "Interlink Construction"],
    entityType: "private_operator",
    entityName: "Interlink Group",
    website: "https://www.interlinkpr.com",
    phone: "+1 787 753 8455",
    targetTitles: ["President", "Chief Executive Officer", "Asset Manager"],
    knownContacts: [
      {
        name: "Federico J. Sánchez",
        title: "President & CEO",
        linkedIn: "https://www.linkedin.com/in/federico-j-sanchez-59186bb6",
        verificationUrl: "https://www.interlinkpr.com/our-team",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["AC Hotel San Juan Condado owner-developer; Highgate operator."],
  },
  {
    slug: "ghl-hoteles",
    country: "Colombia",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "NIT",
    ownerNameMatch: ["GHL Hoteles", "Grupo GHL"],
    entityType: "private_operator",
    entityName: "GHL Hoteles",
    website: "https://www.ghlhoteles.com",
    phone: "+57 601 313 9330",
    targetTitles: ["Chief Executive Officer", "President", "Director Comercial"],
    knownContacts: [
      {
        name: "Andrés Fajardo",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/andres-fajardo-3581a5",
        verificationUrl: "https://www.sahic.com/andresfajardo",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Jorge Londoño Riani",
        title: "President — Grupo GHL",
        verificationUrl: "https://colombia.ladevi.info/ghl-hoteles/jorge-londono-generar-experiencias-gente-feliz-significa-innovacion-n62935",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "chairman",
      },
    ],
    researchNotes: ["Advent International-backed LATAM operator — 60+ hotels."],
  },
  {
    slug: "atlantica-hotels",
    country: "Brazil",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "CNPJ",
    ownerNameMatch: [
      "Atlantica Hotels International (Brasil) Ltda.",
      "Atlantica Hotels International",
      "Atlantica Hospitality International",
    ],
    entityType: "private_operator",
    entityName: "Atlantica Hospitality International",
    website: "https://www.ahi.com.br",
    phone: "+55 11 3531 4800",
    targetTitles: ["Chief Executive Officer", "President", "Director Comercial"],
    knownContacts: [
      {
        name: "Eduardo Giestas",
        title: "Chief Executive Officer & President",
        linkedIn: "https://www.linkedin.com/in/eduardo-giestas-17199916",
        verificationUrl: "https://www.ahi.com.br/atlantica-hospitality-international-aposta-em-engajamento-para-uma-transformacao-organizacional-cultural-e-digital/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["Largest multi-brand hotel operator in South America."],
  },
  {
    slug: "grupo-martinon-grumasa",
    country: "Spain",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "CIF",
    ownerNameMatch: [
      "Grupo Martinon Grumasa S.L.",
      "Grupo Martinon Grumasa",
      "Grupo Invercan Grumasa",
    ],
    entityType: "private_operator",
    entityName: "Grupo Martinón / Invercan Grumasa",
    website: "https://www.grupomartinon.com",
    targetTitles: ["President", "Chief Executive Officer", "Director de Desarrollo"],
    knownContacts: [
      {
        name: "Alicia Martinón García",
        title: "President / Administradora Única",
        linkedIn: "https://www.linkedin.com/in/alicia-martin%C3%B3n-418a6b166",
        verificationUrl: "https://www.fundacionmapfrecanarias.org/conocenos/organos-de-gobierno/alicia-martinon/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["Spain/Caribbean/Mexico hotel developer — Dreams Jade Puerto Morelos among CALA assets."],
  },
  {
    slug: "real-hotels",
    country: "Portugal",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "NIPC",
    ownerNameMatch: ["Real Hotels & Resorts", "Real Hotels Group", "Grupo Hotéis Real"],
    entityType: "private_operator",
    entityName: "Real Hotels Group",
    website: "https://www.realhotelsandresorts.com",
    targetTitles: ["Chief Executive Officer", "Director Comercial"],
    knownContacts: [
      {
        name: "Mafalda Alves Dias",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/mafaldalvesdias",
        verificationUrl: "https://www.ambitur.pt/real-hotels-group-nomeia-mafalda-alves-dias-como-nova-ceo/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "CEO appointed 2026 — press proof; LinkedIn profile not yet confirmed for V2.",
      "CALA assets include Comfort Inn properties in Brazil via portfolio.",
    ],
  },
  {
    slug: "globalia",
    country: "Spain",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "CIF",
    ownerNameMatch: ["Globalia Corporacion Empresarial", "Globalia Corporación Empresarial"],
    entityType: "private_operator",
    entityName: "Globalia Corporación Empresarial S.A.",
    website: "https://www.globalia.com",
    targetTitles: ["Chairman", "Managing Director", "Director Hotelero"],
    knownContacts: [
      {
        name: "Javier Blanco",
        title: "Director General — Be Live Hotels",
        linkedIn: "https://www.linkedin.com/in/javier-blanco-6228055a",
        verificationUrl: "https://www.cndenglish.com/noticia/q-javier-blanco-director-general-hotels-be-live-globalia-group",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "be_live_dg",
      },
      {
        name: "Juan José Hidalgo Acera",
        title: "Founder & Chairman",
        verificationUrl: "https://www.aireuropa.com/us/en/aea/travel-information/corporate-information/globalia-group.html",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "chairman",
      },
    ],
    researchNotes: [
      "Be Live Hotels Caribbean portfolio — Javier Blanco is hotel division DG; chairman is V3 press-only.",
    ],
  },
  {
    slug: "gran-caribe-cuba",
    country: "Cuba",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: [
      "Grupo Empresarial Hotelero Gran Caribe S.A",
      "Grupo Empresarial Hotelero Gran Caribe S.A.",
      "Gran Caribe Grupo Hotelero",
    ],
    entityType: "state_enterprise",
    entityName: "Grupo Hotelero Gran Caribe",
    website: "https://grancaribehotels.com",
    phone: "+53 7 204 0578",
    targetTitles: ["President", "Director General", "Director Comercial"],
    knownContacts: [
      {
        name: "Jesús Pérez Balsa",
        title: "President",
        verificationUrl: "https://excelenciascuba.com/turismo/gran-caribe-cerro-el-ano-de-su-aniversario-30-con-una-vision-optimista",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Cuban state hotel group — corp site lists role mailboxes only (centralreservas@grancaribe.gca.tur.cu).",
    ],
  },
  {
    slug: "a3-property-investments",
    country: "Chile",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "RUT",
    ownerNameMatch: ["A3 Property Investment", "A3 Property Investments"],
    entityType: "institutional",
    entityName: "A3 Property Investments",
    website: "https://www.a3pinvest.com",
    phone: "+56 2 2382 3710",
    targetTitles: ["Director General", "Chief Executive Officer", "Head of Development"],
    knownContacts: [
      {
        name: "Manuel Tamés",
        title: "Director General",
        linkedIn: "https://www.linkedin.com/in/manuel-tam%C3%A9s-99565736",
        verificationUrl: "https://www.linkedin.com/company/a3-property-investments",
        verificationTier: "V2",
        verificationSource: "linkedin",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["Accor-managed Atton/Pullman/Novotel portfolio across Chile, Peru, Colombia."],
  },
  {
    slug: "jhsf",
    country: "Brazil",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "CNPJ",
    ownerNameMatch: ["JHSF", "JHSF Participações"],
    entityType: "public_company",
    entityName: "JHSF Participações S.A.",
    website: "https://www.jhsf.com.br",
    phone: "+55 11 3702 1900",
    targetTitles: ["Chief Executive Officer", "Chairman", "Hotels Director"],
    knownContacts: [
      {
        name: "Augusto Martins",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/augusto-martins-914176",
        verificationUrl: "https://ri.jhsf.com.br/en/corporate-governance/executive-management-boards-and-committees/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "ceo",
      },
      {
        name: "José Auriemo Neto",
        title: "Chairman of the Board",
        linkedIn: "https://www.linkedin.com/in/jos%C3%A9-auriemo-neto",
        verificationUrl: "https://ri.jhsf.com.br/en/corporate-governance/executive-management-boards-and-committees/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "chairman",
      },
    ],
    researchNotes: ["Fasano hotels + luxury retail — CEO Augusto Martins; chairman José Auriemo Neto."],
  },
  {
    slug: "ich-administracao",
    country: "Brazil",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "CNPJ",
    ownerNameMatch: ["ICH Administracao de Hoteis S.A.", "ICH Administração de Hotéis"],
    entityType: "private_operator",
    entityName: "ICH Administração de Hotéis S.A.",
    website: "https://www.intercityhoteis.com.br",
    phone: "+55 51 3594 6000",
    targetTitles: ["Chief Executive Officer", "President", "Director Comercial"],
    knownContacts: [
      {
        name: "Alexandre Gehlen",
        title: "Chief Executive Officer & Founder",
        linkedIn: "https://www.linkedin.com/in/alexandre-gehlen-6a018b6",
        verificationUrl: "https://www.revistahotelnews.com.br/ich-comemora-25-anos-com-evento-na-capital-paulista/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["Brazil multi-brand operator — Intercity, Tru by Hilton, Yoo2; 40+ CALA-relevant hotels."],
  },
  {
    slug: "mohari-gencom",
    country: "Costa Rica",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: [
      "Owner 1: Mohari Hospitality Limited | Owner 2: Gencom",
      "Mohari Hospitality",
      "Gencom",
    ],
    entityType: "institutional",
    entityName: "Mohari Hospitality / Gencom (Peninsula Papagayo JV)",
    website: "https://moharihospitality.com",
    targetTitles: ["Founder", "Principal", "Chief Executive Officer"],
    knownContacts: [
      {
        name: "Mark Scheinberg",
        title: "Founder & Principal — Mohari Hospitality",
        linkedIn: "https://www.linkedin.com/in/mark-scheinberg-538b24215",
        verificationUrl: "https://moharihospitality.com/about/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Karim Alibhai",
        title: "Founder & Principal — Gencom",
        linkedIn: "https://www.linkedin.com/in/karim-alibhai-2789728",
        verificationUrl: "https://gencomgrp.com/about/karim-alibhai/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "gencom_principal",
      },
    ],
    researchNotes: [
      "Four Seasons Costa Rica at Peninsula Papagayo — Mohari/Gencom JV; role mailboxes only on corp sites.",
    ],
  },
  {
    slug: "grace-bay-resorts",
    country: "Turks and Caicos",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Grace Bay Resorts"],
    entityType: "private_operator",
    entityName: "Grace Bay Resorts",
    website: "https://www.gracebayresorts.com",
    targetTitles: ["Chief Executive Officer", "Founder", "Director of Development"],
    knownContacts: [
      {
        name: "Mark Durliat",
        title: "CEO & Co-Founder",
        linkedIn: "https://www.linkedin.com/in/mark-durliat-0926241b",
        verificationUrl: "https://rockhouse.gracebayresorts.com/team/mark-durliat/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["Turks & Caicos luxury owner-developer — South Bank, Rock House pipeline."],
  },
];

/**
 * @param {string} ownerName
 * @returns {CalaCorporateWebSeed | null}
 */
export function resolveCalaCorporateSeed(ownerName) {
  const norm = String(ownerName || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ");
  if (!norm) return null;

  for (const seed of CALA_CORPORATE_WEB_SEEDS) {
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
 * @param {CalaCorporateWebSeed} seed
 */
export function pickRecommendedOutreachContact(seed) {
  const contacts = seed.knownContacts || [];
  const withNamedEmail = contacts.find(
    (c) => c.email && isNamedPersonEmail(c.email, c.name)
  );
  if (withNamedEmail) return withNamedEmail;
  const primary = contacts.find((c) => c.outreachRole === "primary" && (c.email || c.linkedIn));
  if (primary) return primary;
  const dev = contacts.find((c) => c.outreachRole === "development" && (c.email || c.linkedIn));
  if (dev) return dev;
  const withLinkedIn = contacts.find(
    (c) => c.linkedIn && !String(c.outreachRole || "").endsWith("_downgrade")
  );
  if (withLinkedIn) return withLinkedIn;
  return contacts[0] || null;
}
