/**
 * CALA public registry systems for hotel owner entity → legal representative lookup.
 * Internal GTM only — not product-facing.
 *
 * Phase 1: human/agent lookup via portals; automated fetch adapters added per country later.
 */

/** @typedef {{
 *   id: string,
 *   label: string,
 *   commercialRegistryUrl: string,
 *   tourismRegistryUrl?: string,
 *   taxRegistryUrl?: string,
 *   entityIdLabel: string,
 *   lookupNotes: string[],
 *   automatedLookup: "none" | "planned" | "partial",
 * }} RegistrySystemConfig */

/** @type {Record<string, RegistrySystemConfig>} canonical CALA country → registry */
export const CALA_REGISTRY_BY_COUNTRY = {
  Mexico: {
    id: "MX_CORPORATE_WEB",
    label: "Mexico — corporate web / IR first [SIGER optional]",
    commercialRegistryUrl: "https://www.siger.gob.mx/",
    tourismRegistryUrl: null,
    taxRegistryUrl: "https://agsc.siat.sat.gob.mx/PTSC/ValidaRFC/index.jsf",
    entityIdLabel: "RFC",
    lookupNotes: [
      "WAVE 1 PRIMARY: corporate website, IR/management page, LinkedIn — no SIGER CURP signup required.",
      "Public REITs (Fibra Inn, Fibra Hotel): IR email or CEO LinkedIn from fibrainn.mx / fibrahotel.com.",
      "Private operators: brisas.com.mx, grupodiestra.com, pueblobonito.com leadership pages.",
      "Optional — SIEM bulk CSV on datos.gob.mx (entity name / RFC, no legal rep).",
      "Optional — SIGER only if you have CURP and need V1R legal-rep proof.",
      "RNT skipped unless MX_RNT_LOOKUP_ENABLED=1 and portal loads.",
      "SPV owners: hotel website footer → operating entity → corporate site.",
    ],
    automatedLookup: "partial",
  },
  Colombia: {
    id: "CO_RUES",
    label: "Colombia — RUES + RNT",
    commercialRegistryUrl: "https://www.rues.org.co/",
    tourismRegistryUrl: "https://www.rues.org.co/",
    taxRegistryUrl: "https://www.rues.org.co/",
    entityIdLabel: "NIT",
    lookupNotes: [
      "Free RUES search by NIT or razón social returns representante legal.",
      "Tourism (RNT) consultable within RUES portal by NIT or RNT number.",
    ],
    automatedLookup: "planned",
  },
  "Dominican Republic": {
    id: "DO_REGISTRO_MERCANTIL",
    label: "Dominican Republic — Registro Mercantil + DGII RNC",
    commercialRegistryUrl: "https://app.registromercantil.do/",
    taxRegistryUrl: "https://dgii.gov.do/",
    entityIdLabel: "RNC",
    lookupNotes: [
      "Account required on app.registromercantil.do for Santo Domingo / DN searches.",
      "Returns razón social, RNC, legal representative, registered address.",
      "Provincial chambers for assets outside DN.",
    ],
    automatedLookup: "planned",
  },
  Brazil: {
    id: "BR_CNPJ",
    label: "Brazil — Receita Federal CNPJ + Cadastur",
    commercialRegistryUrl: "https://solucoes.receita.fazenda.gov.br/Servicos/cnpjreva/Cnpjreva_Solicitacao.asp",
    tourismRegistryUrl: "https://cadastur.turismo.gov.br/",
    entityIdLabel: "CNPJ",
    lookupNotes: [
      "CNPJ lookup returns razão social, status, address; QSA/admins vary by state junta.",
      "Cadastur for tourism operator registration.",
    ],
    automatedLookup: "planned",
  },
  "Costa Rica": {
    id: "CR_REGISTRO_NACIONAL",
    label: "Costa Rica — Registro Nacional + ICT",
    commercialRegistryUrl: "https://www.registronacional.go.cr/",
    entityIdLabel: "Cédula jurídica",
    lookupNotes: ["Search by company name or cédula jurídica for legal representatives."],
    automatedLookup: "none",
  },
  Panama: {
    id: "PA_REGISTRO_PUBLICO",
    label: "Panama — Registro Público + ATP",
    commercialRegistryUrl: "https://www.registropublico.gob.pa/",
    entityIdLabel: "RUC",
    lookupNotes: ["Panama Digital / Registro Público for entity and officers."],
    automatedLookup: "none",
  },
  Peru: {
    id: "PE_SUNARP",
    label: "Peru — SUNARP + MINCETUR",
    commercialRegistryUrl: "https://www.sunarp.gob.pe/",
    entityIdLabel: "RUC",
    lookupNotes: ["SUNARP for legal entity; MINCETUR for tourism registry."],
    automatedLookup: "none",
  },
  Chile: {
    id: "CL_REGISTRO_EMPRESAS",
    label: "Chile — Registro de Empresas",
    commercialRegistryUrl: "https://www.registrodeempresasysociedades.cl/",
    entityIdLabel: "RUT",
    lookupNotes: ["Free search by RUT or razón social for representatives."],
    automatedLookup: "planned",
  },
  Jamaica: {
    id: "JM_COMPANIES_OFFICE",
    label: "Jamaica — Companies Office of Jamaica",
    commercialRegistryUrl: "https://www.orcjamaica.com/",
    entityIdLabel: "TRN",
    lookupNotes: ["Companies Office search for registered entity and directors."],
    automatedLookup: "none",
  },
  "Puerto Rico": {
    id: "PR_CORPORATIONS",
    label: "Puerto Rico — Departamento de Estado (corporations)",
    commercialRegistryUrl: "https://prcorpfiling.f1hst.com/CorpInfo/CorpSearch.aspx",
    entityIdLabel: "EIN/local registry",
    lookupNotes: ["PR corporation search for registered agents and officers."],
    automatedLookup: "none",
  },
};

/** Opaque SPV name patterns — prefer tourism registry bridge. */
const OPAQUE_SPV_PATTERN =
  /\b(spe|s\.p\.e|sap[ií]\s*de\s*c\.?v\.?|s\.a\.p\.i|holding\s+llc|property\s+llc|investment\s+llc|fondo\s+de\s+inversion|fideicomiso|trust|spv)\b/i;

/**
 * @param {string} country
 * @returns {RegistrySystemConfig | null}
 */
export function resolveRegistryForCountry(country) {
  const key = String(country || "").trim();
  if (!key) return null;
  if (CALA_REGISTRY_BY_COUNTRY[key]) return CALA_REGISTRY_BY_COUNTRY[key];
  for (const [canonical, config] of Object.entries(CALA_REGISTRY_BY_COUNTRY)) {
    if (key.toLowerCase().includes(canonical.toLowerCase()) || canonical.toLowerCase().includes(key.toLowerCase())) {
      return config;
    }
  }
  return null;
}

/**
 * @param {string} ownerName
 * @returns {"direct_entity" | "rnt_bridge" | "opaque_spv"}
 */
export function inferEntityBridgeStrategy(ownerName) {
  const name = String(ownerName || "").trim();
  if (!name) return "opaque_spv";
  if (OPAQUE_SPV_PATTERN.test(name)) return "rnt_bridge";
  if (/\b(hoteles?|hotels?|resorts?|hospitality|hotelero|grupo)\b/i.test(name)) return "direct_entity";
  if (name.length < 12 && /\b(llc|inc|corp|sa|srl|ltda)\b/i.test(name)) return "rnt_bridge";
  return "direct_entity";
}

/**
 * @param {string} ownerName
 * @param {"direct_entity" | "rnt_bridge" | "opaque_spv"} strategy
 */
export function entitySearchHints(ownerName, strategy) {
  const hints = [`CoStar True Owner: ${ownerName}`];
  if (strategy === "rnt_bridge" || strategy === "opaque_spv") {
    hints.push("Bridge via tourism registry (RNT/Cadastur/etc.) using hotel commercial name + state.");
    hints.push("Cross-check hotel website footer / privacy policy for razón social.");
  }
  if (strategy === "direct_entity") {
    hints.push("Search commercial registry by True Owner razón social (normalize SA de CV, S.A., etc.).");
  }
  return hints;
}
