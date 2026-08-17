/**
 * Curated Mexico owner corporate web seeds for Wave 1 outreach (no SIGER/RNT signup).
 * Sources: company IR/management pages, public filings, CoStar profiles.
 */
import { isNamedPersonEmail } from "../registry-contact-verification.js";

/**
 * @typedef {{
 *   slug: string,
 *   ownerNameMatch: string[],
 *   entityType: "public_reit" | "private_operator" | "foreign_hq" | "opaque_spv",
 *   entityName: string,
 *   website: string,
 *   investorRelationsUrl?: string,
 *   managementUrl?: string,
 *   linkedInCompany?: string,
 *   ticker?: string,
 *   phone?: string,
 *   targetTitles: string[],
 *   knownContacts?: Array<{
 *     name: string,
 *     title: string,
 *     email?: string,
 *     phone?: string,
     businessPhone?: string,
     mobilePhone?: string,
     phoneType?: "mobile" | "business" | "switchboard",
     businessPhoneTier?: "VP1" | "VP3",
     mobilePhoneTier?: "VP2",
     phoneVerificationTier?: "VP1" | "VP2" | "VP3",
 *     linkedIn?: string,
 *     verificationUrl: string,
 *     verificationTier?: "V1R" | "V2",
 *     verificationSource?: string,
 *     outreachRole?: "primary" | "ir" | "legal" | "development",
 *   }>,
 *   researchNotes?: string[],
 * }} MxCorporateWebSeed
 */

/** @type {MxCorporateWebSeed[]} */
export const MX_CORPORATE_WEB_SEEDS = [
  {
    slug: "fibra-hotel-mexico",
    ownerNameMatch: ["Fibra Hotel Mexico", "FibraHotel", "Concentradora Fibra Hotelera"],
    entityType: "public_reit",
    entityName: "Concentradora Fibra Hotelera Mexicana, S.A. de C.V.",
    website: "https://fibrahotel.mx",
    investorRelationsUrl: "https://www.fibrahotel.com",
    linkedInCompany: "fibrahotel",
    ticker: "FIHO12",
    phone: "+52 55 5292 8050",
    targetTitles: [
      "Director General",
      "CEO",
      "Director de Relaciones con Inversionistas",
      "Director Jurídico",
      "Director de Inversiones",
    ],
    knownContacts: [
      {
        name: "Eduardo López García",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/eduardo-l%C3%B3pez-garc%C3%ADa-8a5b5a1a",
        verificationUrl: "https://www.reuters.com/markets/companies/FIHO12.MX/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Guillermo Bravo Escobosa",
        title: "Chief Investment Officer",
        verificationUrl: "https://www.reuters.com/markets/companies/FIHO12.MX/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "development",
      },
    ],
    researchNotes: [
      "Listed REIT (FIHO12, BMV). IR phone +52 55 5292 8050.",
      "Management listed on Reuters/MarketScreener — no SIGER account required.",
      "Outreach angle: portfolio analytics across Fiesta Inn / One / AC Marriott assets.",
    ],
  },
  {
    slug: "norte-19",
    ownerNameMatch: ["Norte 19"],
    entityType: "private_operator",
    entityName: "Norte 19",
    website: "https://norte19.com",
    phone: "+52 55 5249 8050",
    targetTitles: ["Chief Executive Officer", "Chief Operating Officer", "Chief New Projects Officer"],
    knownContacts: [
      {
        name: "Alberto Granados",
        title: "Chief Operating Officer",
        email: "AGranados@norte19.com",
        businessPhone: "+52 55 5249 8050",
        businessPhoneTier: "VP3",
        phoneVerificationTier: "VP3",
        verificationUrl: "https://norte19.com",
        verificationTier: "V1R",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "HQ switchboard +52 55 5249 8050 on norte19.com — VP3 entity line; named emails are V1R.",
    ],
  },
  {
    slug: "grupo-posadas",
    ownerNameMatch: ["Grupo Posadas", "Grupo Posadas, S.A.B. DE C.V."],
    entityType: "public_reit",
    entityName: "Grupo Posadas, S.A.B. DE C.V.",
    website: "https://www.posadas.com",
    phone: "+52 55 5326 6700",
    targetTitles: ["Chief Executive Officer", "Chief Financial Officer", "Chief Operating Officer"],
    knownContacts: [
      {
        name: "José Carlos Azcárraga Andrade",
        title: "Chief Executive Officer",
        email: "JoseCarlos.AzcarragaAndrade@posadas.com",
        businessPhone: "+52 55 5326 6700",
        businessPhoneTier: "VP3",
        phoneVerificationTier: "VP3",
        verificationUrl: "https://www.posadas.com",
        verificationTier: "V1R",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Corp HQ +52 55 5326 6700 — entity switchboard (VP3). CEO named email is primary outreach path.",
    ],
  },
  {
    slug: "fibra-inn",
    ownerNameMatch: ["Fibra Inn"],
    entityType: "public_reit",
    entityName: "Fibra Inn",
    website: "https://fibrainn.mx",
    investorRelationsUrl: "https://fibrainn.mx/en/investors/home",
    managementUrl: "https://fibrainn.mx/en/corporate/management",
    linkedInCompany: "fibrainn",
    ticker: "FINN13",
    phone: "+52 81 5000 0200",
    targetTitles: [
      "Director General",
      "Director de Relaciones con Inversionistas",
      "Director de Estrategia y Desarrollo",
      "Director Jurídico",
    ],
    knownContacts: [
      {
        name: "Jaime Cohen Bistre",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/jcohenb",
        verificationUrl: "https://fibrainn.mx/en/corporate/management",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Sergio Martinez Richo",
        title: "Director of Investor Relations & ESG",
        linkedIn: "https://mx.linkedin.com/in/sergiomr",
        verificationUrl: "https://fibrainn.mx/en/corporate/management",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "ir",
      },
      {
        name: "Raúl Mateo",
        title: "Director of Strategy and Development",
        verificationUrl: "https://fibrainn.mx/en/corporate/management",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "development",
      },
      {
        name: "Investor Relations (role mailbox)",
        title: "IR channel — not a named person",
        email: "ir@fibrainn.mx",
        businessPhone: "+52 81 5000 0200",
        businessPhoneTier: "VP3",
        phoneVerificationTier: "VP3",
        verificationUrl: "https://fibrainn.mx/en/investors/contacts",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "ir_downgrade",
      },
    ],
    researchNotes: [
      "Listed REIT (FINN13). Corp IR channel ir@fibrainn.mx exists but is filtered — use LinkedIn for Sergio Martinez or find named email.",
      "Best first send: CEO Jaime Cohen (LinkedIn) or IR Sergio Martinez (LinkedIn).",
    ],
  },
  {
    slug: "grupo-brisas",
    ownerNameMatch: ["Grupo Brisas"],
    entityType: "private_operator",
    entityName: "Grupo Brisas",
    website: "https://brisas.com.mx",
    managementUrl: "https://www.brisas.com.mx/en/contact/",
    phone: "+52 55 5339 1010",
    targetTitles: ["Director General", "CEO", "Director Comercial", "Director de Desarrollo"],
    knownContacts: [
      {
        name: "Antonio Cosío Pando",
        title: "Director General / CEO",
        linkedIn: "https://www.linkedin.com/in/antonio-cosio-63a75619",
        verificationUrl: "https://www.brisas.com.mx/en/contact/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "General Inquiries (switchboard)",
        title: "Corp switchboard — not a named person",
        email: "info@brisas.com.mx",
        businessPhone: "+52 55 5339 1010",
        businessPhoneTier: "VP3",
        phoneVerificationTier: "VP3",
        verificationUrl: "https://www.brisas.com.mx/en/contact/",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "info_downgrade",
      },
    ],
    researchNotes: [
      "CEO Antonio Cosío Pando — public interviews (Forbes México, Reporte Lobby).",
      "CEO Antonio Cosío Pando — use LinkedIn InMail; info@brisas.com.mx is switchboard only (not verified person email).",
    ],
  },
  {
    slug: "grupo-diestra",
    ownerNameMatch: ["Grupo Diestra"],
    entityType: "private_operator",
    entityName: "Grupo Diestra",
    website: "https://grupodiestra.com",
    phone: "+52 55 5062 6000",
    targetTitles: ["Director General", "Director de Desarrollo", "Director Comercial"],
    knownContacts: [
      {
        name: "Jorge Paoli Díaz",
        title: "Chief Executive Officer",
        linkedIn: "https://mx.linkedin.com/in/jorge-paoli-diaz-8954a61b5",
        verificationUrl: "https://hotelespormexico.org/prensa/boletin-de-prensa-renovacion-comite-ejecutivo-anch-2025-2/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Oscar Rivera",
        title: "Director General Adjunto / COO",
        linkedIn: "https://www.linkedin.com/in/oscar-rivera-6a4b2934",
        verificationUrl: "https://www.linkedin.com/company/grupo-diestra-hotels-resorts",
        verificationTier: "V2",
        verificationSource: "linkedin",
        outreachRole: "development",
      },
    ],
    researchNotes: ["Emporio + Marriott franchise portfolio. CEO Jorge Paoli — ANCH president 2025."],
  },
  {
    slug: "pueblo-bonito",
    ownerNameMatch: ["Pueblo Bonito Hotels and Resorts", "Pueblo Bonito"],
    entityType: "private_operator",
    entityName: "Pueblo Bonito Hotels and Resorts",
    website: "https://pueblobonito.com",
    targetTitles: ["Director General", "Director Comercial", "Director de Operaciones"],
    knownContacts: [
      {
        name: "Alberto Ernesto Coppel",
        title: "Chief Executive Officer",
        linkedIn: "https://mx.linkedin.com/in/alberto-coppel-096272104",
        verificationUrl: "https://theorg.com/org/pueblo-bonito-golf-and-spa-resorts/org-chart/alberto-ernesto-coppel",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["Cabo/Mazatlán resort owner-operator. No public named CEO email — LinkedIn V2 path."],
  },
  {
    slug: "park-royal",
    ownerNameMatch: ["Park Royal Hotels & Resorts", "Park Royal"],
    entityType: "private_operator",
    entityName: "Park Royal Hotels & Resorts",
    website: "https://parkroyalhotels.com",
    targetTitles: ["Director General", "Director Comercial"],
    researchNotes: ["Mexico + Caribbean footprint. Corporate site + LinkedIn company page."],
  },
  {
    slug: "estancias-extendidas",
    ownerNameMatch: ["Estancias Extendidas"],
    entityType: "private_operator",
    entityName: "Estancias Extendidas",
    website: "https://estanciashoteles.com",
    targetTitles: ["Director General", "Director Comercial"],
    researchNotes: ["Extended-stay Mexico operator. Website footer for entity name."],
  },
  {
    slug: "hoteles-mx",
    ownerNameMatch: ["Hoteles MX"],
    entityType: "private_operator",
    entityName: "Hoteles MX (Somos Grupo MX)",
    website: "https://hotelesmx.com",
    targetTitles: ["Director General", "Director Comercial"],
    knownContacts: [
      {
        name: "Eduardo Sánchez Moreno",
        title: "Director General",
        linkedIn: "https://www.linkedin.com/in/eduardo-s%C3%A1nchez-moreno-",
        verificationUrl: "https://mx.linkedin.com/company/somos-grupo-mx",
        verificationTier: "V2",
        verificationSource: "linkedin",
        outreachRole: "primary",
      },
      {
        name: "Angel David Alvarez Nunez",
        title: "Director Ejecutivo Fundador",
        linkedIn: "https://www.linkedin.com/in/davidalvarezmx",
        verificationUrl: "https://www.hotelesmx.com/contactanos",
        verificationTier: "V2",
        verificationSource: "linkedin",
        outreachRole: "development",
      },
    ],
    researchNotes: ["Parent: Somos Grupo MX. Wyndham Trademark / VIVE MX portfolio."],
  },
  {
    slug: "arriva-hospitality",
    ownerNameMatch: ["Arriva Hospitality Group", "Arriva Hospitality"],
    entityType: "private_operator",
    entityName: "Arriva Hospitality Group",
    website: "https://arrivahospitality.com",
    targetTitles: ["Director General", "Director de Desarrollo"],
    researchNotes: ["Check arrivahospitality.com team page."],
  },
  {
    slug: "pulso-inmobiliario",
    ownerNameMatch: ["Pulso Inmobiliario"],
    entityType: "private_operator",
    entityName: "Pulso Inmobiliario, S.A. de C.V.",
    website: "https://pulsoinmobiliario.com",
    phone: "+52 55 5202 3100",
    targetTitles: ["Presidente", "Director General", "Director de Inversiones"],
    knownContacts: [
      {
        name: "Salomón Kamkhaji Ambe",
        title: "Chairman / Presidente",
        linkedIn: "https://www.linkedin.com/in/salomon-kamaji-219b7b9b",
        verificationUrl: "https://pulsoinmobiliario.com/safety/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Dreams/Hyatt Inclusive development partner in Mexico.",
      "Chairman letter on pulsoinmobiliario.com/safety/ — primary outreach via LinkedIn.",
    ],
  },
  {
    slug: "irawadi-corp",
    ownerNameMatch: ["Irawadi Corp S.A.", "Irawadi Corp", "RCD Hotels"],
    entityType: "foreign_hq",
    entityName: "RCD Hotels / Irawadi Corp S.A.",
    website: "https://rcdhotels.com",
    phone: "+52 998 254 6500",
    targetTitles: ["President", "CEO", "Director de Desarrollo"],
    knownContacts: [
      {
        name: "Philipp Hofer",
        title: "Managing Director",
        linkedIn: "https://www.linkedin.com/in/philipp-hofer-0b9720201",
        verificationUrl: "https://www.jtbonline.org/BoardofDirectors/mr-philipp-hofer/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Roberto Chapur Duarte",
        title: "President & Owner",
        verificationUrl: "https://www.hotel-online.com/news/nobu-hospitality-grows-partnership-with-rcd-hotels-in-north-america",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "owner",
      },
    ],
    researchNotes: [
      "Panama HQ (RCD Hotels). Hard Rock / Nobu / UNICO Mexico assets.",
      "Philipp Hofer MD — operational V2 contact; Roberto Chapur owner press-only V3.",
    ],
  },
  {
    slug: "grupo-hotelero-santa-fe",
    ownerNameMatch: ["Grupo Hotelero Santa Fe", "Santa Fe Grupo Hotelero"],
    entityType: "public_reit",
    entityName: "Grupo Hotelero Santa Fe, S.A.B. de C.V.",
    website: "https://gsf-hotels.com",
    investorRelationsUrl: "https://bmv.com.mx/en/issuers/profile/HOTEL-31284",
    phone: "+52 55 5261 0800",
    ticker: "HOTEL",
    targetTitles: ["Chief Executive Officer", "Chief Financial Officer", "Director Comercial"],
    knownContacts: [
      {
        name: "Francisco Medina Elizalde",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/francisco-medina-elizalde-83aa26a3",
        businessPhone: "+52 55 5261 0800",
        businessPhoneTier: "VP3",
        phoneVerificationTier: "VP3",
        verificationUrl: "https://www.reuters.com/markets/companies/HOTEL.MX/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Enrique Martínez Guerrero",
        title: "Chief Financial Officer",
        email: "emartinez@gsf-hotels.com",
        businessPhone: "+52 55 5261 0807",
        verificationUrl: "https://gsf-hotels.com/corporativo/contacto-inversionistas.php",
        verificationTier: "V1R",
        verificationSource: "company_website",
        outreachRole: "cfo_bridge",
      },
    ],
    researchNotes: [
      "Listed HOTEL on BMV. Krystal + Hilton/Hyatt franchise mix.",
      "CEO LinkedIn-only — CFO emartinez@ on IR page is V1R bridge contact.",
    ],
  },
  {
    slug: "velas-resorts",
    ownerNameMatch: ["Velas Resorts", "Grupo Velas"],
    entityType: "private_operator",
    entityName: "Velas Resorts",
    website: "https://www.velasresorts.com",
    phone: "+52 322 226 8000",
    targetTitles: ["Chief Executive Officer", "Director Comercial"],
    knownContacts: [
      {
        name: "Juan Vela Ruiz",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/juan-vela-ruiz-03192a167",
        verificationUrl: "https://www.velasresorts.com/thirty-five-years",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Eduardo Vela Ruiz (founder) deceased Nov 2023 — Juan Vela Ruiz is current CEO (2025).",
    ],
  },
  {
    slug: "karisma-hotels",
    ownerNameMatch: ["Karisma Hotels & Resorts", "Karisma Hotels"],
    entityType: "private_operator",
    entityName: "Karisma Hotels & Resorts",
    website: "https://www.karismahotels.com",
    targetTitles: ["Chief Executive Officer", "Chief Strategy Officer", "Director Comercial"],
    knownContacts: [
      {
        name: "Miguel Ortiz Millán",
        title: "Chief Strategy Officer",
        linkedIn: "https://www.linkedin.com/in/miguelo1",
        verificationUrl: "https://www.linkedin.com/company/karisma-hotels",
        verificationTier: "V2",
        verificationSource: "linkedin",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "FL HQ; heavy Mexico/Dominica/Jamaica footprint. CEO transition early 2026 — CSO Miguel Ortiz is stable exec contact.",
    ],
  },
  {
    slug: "riu-hotels",
    ownerNameMatch: ["Riu Hotels & Resorts", "RIU Hotels & Resorts"],
    entityType: "foreign_hq",
    entityName: "RIU Hotels & Resorts",
    website: "https://www.riu.com",
    targetTitles: ["Chief Executive Officer", "Managing Director"],
    knownContacts: [
      {
        name: "Luis Riu Güell",
        title: "Chief Executive Officer",
        linkedIn: "https://www.linkedin.com/in/luisriug",
        verificationUrl: "https://www.riu.com/en/about/riu-family",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Félix Casado",
        title: "Board Member — Atlantic Zone (CoStar contact unverified)",
        email: "fcasado@riu.com",
        verificationUrl: "https://www.riu.com/en/about/riu-family",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "legacy_costar",
      },
    ],
    researchNotes: [
      "Spain HQ. Luis Riu sole CEO since Jul 2024. fcasado@riu.com from CoStar — not on public site; use Luis Riu V2.",
    ],
  },
  {
    slug: "grupo-questro",
    ownerNameMatch: ["Grupo Questro"],
    entityType: "private_operator",
    entityName: "Grupo Questro",
    website: "https://www.questro.com",
    phone: "+52 624 173 9200",
    targetTitles: ["Chief Executive Officer", "Director General", "Director de Desarrollo"],
    knownContacts: [
      {
        name: "Alfonso Pasquel Barcenas",
        title: "Chief Executive Officer / Director General",
        verificationUrl: "https://www.questro.com/en/contacto",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
      {
        name: "Eduardo Sánchez Navarro Redo",
        title: "Chairman of the Board",
        linkedIn: "https://mx.linkedin.com/in/eduardo-s%C3%A1nchez-navarro-redo-68b9aa46",
        verificationUrl: "https://www.questro.com/en/contacto",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "chairman",
      },
    ],
    researchNotes: [
      "Los Cabos developer — Puerto Los Cabos, marina, golf, hotels.",
      "CEO Alfonso Pasquel on corp directory; chairman Eduardo Sánchez Navarro Redo is LinkedIn V2 path.",
    ],
  },
  {
    slug: "oasis-hotels",
    ownerNameMatch: ["Oasis Hotels and Resorts", "Oasis Hotels & Resorts"],
    entityType: "private_operator",
    entityName: "Oasis Hotels & Resorts",
    website: "https://www.linkedin.com/company/oasishotels",
    phone: "+52 998 881 3100",
    targetTitles: ["Director General", "Director Corporativo", "Asset Manager"],
    knownContacts: [
      {
        name: "Jean Agarrista",
        title: "Director General Adjunto & Asset Manager",
        linkedIn: "https://linkedin.com/in/jean-agarrista-81436522",
        verificationUrl: "https://www.linkedin.com/company/oasishotels",
        verificationTier: "V2",
        verificationSource: "linkedin",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["Cancún/Riviera Maya all-inclusive operator. Dreams Sands Cancun and other assets."],
  },
  {
    slug: "american-hotels-group",
    ownerNameMatch: ["American Hotels Group", "American Hotel Group"],
    entityType: "private_operator",
    entityName: "American Hotels Group",
    website: "https://www.ahg.com.mx",
    phone: "+52 844 415 1500",
    targetTitles: ["Chief Executive Officer", "Director General", "Director de Operaciones"],
    knownContacts: [
      {
        name: "Karim Saade Charur",
        title: "President / Founder",
        email: "ksaade@ahg.com.mx",
        linkedIn: "https://www.linkedin.com/in/karim-saade-20258425",
        verificationUrl: "https://mx.linkedin.com/in/md-roig-44b90728",
        verificationTier: "V1R",
        verificationSource: "linkedin",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["Saltillo HQ. voco, Homewood, Tru, Hyatt Place portfolio in Coahuila."],
  },
  {
    slug: "park-mizgal",
    ownerNameMatch: ["Park Mizgal, S.C.", "Park Mizgal", "Operadora Hotel Dorado Pacifico"],
    entityType: "private_operator",
    entityName: "Operadora Hotel Dorado Pacifico S.A.",
    website: "https://www.sunscaperesorts.com/en/resorts-hotels/sunscape/mexico/dorado-pacifico-ixtapa/",
    phone: "+52 55 5255 1963",
    targetTitles: ["Owner", "Asset Manager", "Director General"],
    knownContacts: [
      {
        name: "Isaac Mizrahi",
        title: "Owner / Asset Manager",
        linkedIn: "https://www.linkedin.com/in/isaac-mizrahi-0503b43b",
        verificationUrl: "https://www.travelweekly.com/Mexico-Travel/From-EP-to-all-inclusive-Ixtapas-Sunscape-Dorado-Pacifico",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "CoStar owner Park Mizgal S.C. — operating entity Operadora Hotel Dorado Pacifico.",
      "Sunscape Dorado Pacifico Ixtapa; family-owned since 1982.",
    ],
  },
  {
    slug: "club-viva-international",
    ownerNameMatch: ["Club Viva International Inc", "Club Viva International", "Viva Wyndham"],
    entityType: "foreign_hq",
    entityName: "Club Viva International Inc / Viva Resorts by Wyndham",
    website: "https://vivaresortsbywyndham.com",
    phone: "+52 99 8689 1101",
    targetTitles: ["President", "Executive Vice President", "Director General"],
    knownContacts: [
      {
        name: "Amanda Santana",
        title: "Executive Vice President of Sales and Marketing",
        linkedIn: "https://www.linkedin.com/in/amanda-santana-2ba71634",
        verificationUrl: "https://vivaresortsbywyndham.com/discover-viva/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "DR HQ (Ettore Colussi president). Mexico: Viva Maya / Viva Azteca Playacar.",
      "Amanda Santana — stable exec contact for Mexico/CALA portfolio.",
    ],
  },
  {
    slug: "flynn-varde-esperanza",
    ownerNameMatch: [
      "Owner 1: Varde Partners, Inc. | Owner 2: Flynn Properties, Inc.",
      "Flynn Properties",
      "Varde Partners",
    ],
    entityType: "foreign_hq",
    entityName: "Flynn Properties (Esperanza, Auberge Collection)",
    website: "https://www.flynnholdings.com/flynnproperties/",
    targetTitles: ["Chief Executive Officer", "Managing Director", "Asset Manager"],
    knownContacts: [
      {
        name: "Greg Flynn",
        title: "Founder, Chairman & CEO — Flynn Properties",
        linkedIn: "https://www.linkedin.com/in/gflynn",
        verificationUrl: "https://www.flynnholdings.com/flynnproperties/",
        verificationTier: "V2",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Esperanza Auberge Collection Cabo — Flynn Properties luxury portfolio.",
      "Varde Partners JV on select-service; Esperanza owned via Flynn Properties.",
    ],
  },
  {
    slug: "galicott-macari",
    ownerNameMatch: ["Galicott & Macari", "Galicott and Macari", "Ganzi"],
    entityType: "opaque_spv",
    entityName: "Ganzi, S. de R.L. de C.V.",
    website: "https://www.hyattinclusivecollection.com/en/resorts-hotels/dreams/mexico/aventuras-riviera-maya/",
    targetTitles: ["Owner", "Director General", "Asset Manager"],
    knownContacts: [
      {
        name: "Ganzi, S. de R.L. de C.V. (entity)",
        title: "Owner entity — Dreams Aventuras Riviera Maya",
        verificationUrl: "https://newsroom.hyatt.com/news-releases?item=124367",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "CoStar owner 'Galicott & Macari' maps to Ganzi S. de R.L. de C.V. (Dreams Aventuras).",
      "Hyatt 2023 owner award — trade name also Grand Hyatt Playa del Carmen on registry.",
      "No public named owner exec found; SIGER/legal rep purchase may be required for V2.",
    ],
  },
  {
    slug: "arotesa",
    ownerNameMatch: ["Arotesa Servicions Integrales SA de CV", "Arotesa Servicios Integrales"],
    entityType: "opaque_spv",
    entityName: "Arotesa Servicios Integrales S.A. de C.V.",
    website: "https://www.marriott.com/en-us/hotels/cunpb-paraiso-de-la-bonita-a-luxury-collection-resort-riviera-maya-adult-all-inclusive/overview/",
    targetTitles: ["Representante Legal", "Director General"],
    knownContacts: [
      {
        name: "Balear Inmobiliaria, S.A. de C.V.",
        title: "Owner entity — Paraiso de la Bonita (Luxury Collection)",
        verificationUrl: "https://www.villanuevaortiz.com/noticias/credito-para-el-resort-paraiso-de-la-bonita-integrante-de-la-luxury-collection-de-marriott",
        verificationTier: "V3",
        verificationSource: "company_website",
        outreachRole: "owner_entity",
      },
      {
        name: "Arotesa Servicios Integrales S.A. de C.V.",
        title: "CoStar-listed services entity (RFC ASI190722BR1)",
        verificationUrl: "https://convenios.tecnm.mx/Accesopublico/ver/18813",
        verificationTier: "V3",
        verificationSource: "public_registry",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "CoStar lists Arotesa; Balear Inmobiliaria is recorded owner per 2020 refinance filing.",
      "2025 ownership transition via Tortuga/KSL; Blue Diamond assumed management.",
      "Founded by Carlos Gosselin (d. 2020) — need named successor or asset manager for V2.",
    ],
  },
  {
    slug: "eurostars",
    ownerNameMatch: ["Eurostars Hotel Company S.L.", "Eurostars Hotel Company"],
    entityType: "foreign_hq",
    entityName: "Eurostars Hotel Company S.L.",
    website: "https://eurostarshotels.com",
    targetTitles: ["Director General", "Director de Expansión"],
    researchNotes: ["Spain HQ (Hotusa). Mexico assets — use Eurostars corporate + LinkedIn."],
  },
  {
    slug: "landstar-hotels",
    ownerNameMatch: ["Landstar Hotels", "Landstar Hotels & Resorts"],
    entityType: "private_operator",
    entityName: "Landstar Hotels",
    website: "https://landstarhotels.com",
    phone: "+1 915 731 9994",
    targetTitles: ["Chief Executive Officer", "Director General"],
    knownContacts: [
      {
        name: "Alberto Hernandez De Santiago",
        title: "Chief Executive Officer / Director General",
        email: "ahernandez@landstarhotels.com",
        linkedIn: "https://mx.linkedin.com/in/ahdesantiago",
        verificationUrl: "https://mx.linkedin.com/in/ahdesantiago",
        verificationTier: "V1R",
        verificationSource: "linkedin",
        outreachRole: "primary",
      },
    ],
    researchNotes: [
      "Binational Mexico/US developer-operator. CoStar owner label may truncate to Alberto De Santiago.",
    ],
  },
  {
    slug: "hoteles-costa-del-sol",
    country: "Peru",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "RUC",
    ownerNameMatch: ["Hoteles Costa del Sol", "Costa del Sol Wyndham"],
    entityType: "private_operator",
    entityName: "Costa del Sol S.A.",
    website: "https://www.costadelsolperu.com",
    phone: "+51 1 200 9222",
    targetTitles: ["Gerente General", "Chief Executive Officer"],
    knownContacts: [
      {
        name: "Mario Mustafá Aguinaga",
        title: "Group CEO / Gerente General",
        email: "mmustafa@costadelsolperu.com",
        linkedIn: "https://www.linkedin.com/in/mario-mustaf%C3%A1-aguinaga-23a555125",
        verificationUrl: "https://camaraperuchile.org/hoteles-costa-del-sol/",
        verificationTier: "V1R",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["Peru chain with Mexico footprint. Chamber listing ties CEO to entity domain."],
  },
  {
    slug: "collective-hospitality",
    country: "United States",
    registrySystem: "CALA_CORPORATE_WEB",
    entityIdLabel: "Registry ID",
    ownerNameMatch: ["Collective Hospitality"],
    entityType: "foreign_hq",
    entityName: "Collective Hospitality",
    website: "https://collectivehospitality.com",
    targetTitles: ["Director of Business Development", "Chief Executive Officer"],
    knownContacts: [
      {
        name: "Gerardo Valdes Mingramm",
        title: "Director of Business Development Americas",
        email: "gerardo.valdes@collectivehospitality.com",
        linkedIn: "https://linkedin.com/in/gerardo-valdes-5783859",
        verificationUrl: "https://collectivehospitality.com/development/",
        verificationTier: "V1R",
        verificationSource: "company_website",
        outreachRole: "primary",
      },
    ],
    researchNotes: ["Socialtel / Slumber Party parent — CALA hostel portfolio."],
  },
];

/**
 * @param {string} ownerName
 * @returns {MxCorporateWebSeed | null}
 */
export function resolveMxCorporateSeed(ownerName) {
  const norm = String(ownerName || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ");
  if (!norm) return null;

  for (const seed of MX_CORPORATE_WEB_SEEDS) {
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
 * @param {MxCorporateWebSeed} seed
 * @returns {import("./mx-corporate-web-first.js").MxCorporateWebPlan["recommendedContact"] | null}
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
