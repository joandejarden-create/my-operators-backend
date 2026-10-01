/**
 * Source-field semantics for Ownership Intelligence P1.
 * No adapter may emit OWNED_BY from label intuition alone.
 *
 * Status meanings:
 * - proves_owned_by: field establishes (or strongly claims) property ownership
 * - proves_operated_by: operator / management company
 * - proves_registered_lodging_entity: tourism/commercial registration identity — NOT PropCo
 * - proves_legal_representative: person who represents company — NOT owner
 * - proves_identifier_only: tax/corporate ID for entity resolution
 * - none: do not create hotel→entity ownership edges
 */

export const OWNERSHIP_SOURCE_SEMANTICS_VERSION = "ownership-source-semantics-v1";

/**
 * @typedef {{
 *   field: string,
 *   source: string,
 *   country: string,
 *   proves: string,
   *   allowed_relationship: null | "OWNED_BY" | "CONTROLLED_BY" | "SPONSORED_BY" | "DEVELOPED_BY" | "OPERATED_BY" | "ASSET_MANAGED_BY" | "LEASED_FROM" | "INVESTED_IN_BY",
 *   max_verification: "probable" | "needs_review" | "high" | "verified" | "none",
 *   notes: string,
 * }} FieldSemantics
 */

/** @type {Record<string, FieldSemantics>} */
export const FIELD_SEMANTICS = Object.freeze({
  // --- Colombia RNT ---
  "colombia_rnt.razon_social_establecimiento": {
    field: "razon_social_establecimiento",
    source: "colombia_rnt",
    country: "Colombia",
    proves: "proves_registered_lodging_entity",
    allowed_relationship: null,
    max_verification: "none",
    notes:
      "Commercial/legal name of the registered lodging establishment. Identifies the tourism-registered PJ, not proven real-estate PropCo.",
  },
  "colombia_rnt.nit": {
    field: "nit",
    source: "colombia_rnt",
    country: "Colombia",
    proves: "proves_identifier_only",
    allowed_relationship: null,
    max_verification: "none",
    notes: "NIT of the registered lodging provider — entity resolution only.",
  },

  // --- Peru MINCETUR ---
  "peru_mincetur.razon_social": {
    field: "razon_social",
    source: "peru_mincetur",
    country: "Peru",
    proves: "proves_registered_lodging_entity",
    allowed_relationship: null,
    max_verification: "none",
    notes:
      "Registered lodging company name. Not sufficient alone for OWNED_BY PropCo.",
  },
  "peru_mincetur.ruc": {
    field: "ruc",
    source: "peru_mincetur",
    country: "Peru",
    proves: "proves_identifier_only",
    allowed_relationship: null,
    max_verification: "none",
    notes: "RUC tax ID — entity resolution only.",
  },
  "peru_mincetur.rep_legal": {
    field: "REP_LEGAL",
    source: "peru_mincetur",
    country: "Peru",
    proves: "proves_legal_representative",
    allowed_relationship: null,
    max_verification: "none",
    notes:
      "Legal representative of the registered company. Never Hotel→OWNED_BY. Later: Person→REPRESENTS→Company.",
  },

  // --- Brazil Cadastur ---
  "brazil_cadastur.nome_pessoa_juridica": {
    field: "Nome da Pessoa Jurídica",
    source: "brazil_cadastur",
    country: "Brazil",
    proves: "proves_registered_lodging_entity",
    allowed_relationship: null,
    max_verification: "none",
    notes:
      "Cadastur registered lodging PJ. Often operator/license holder; NOT automatic PropCo OWNED_BY.",
  },
  "brazil_cadastur.cnpj": {
    field: "Número de Inscrição do CNPJ",
    source: "brazil_cadastur",
    country: "Brazil",
    proves: "proves_identifier_only",
    allowed_relationship: null,
    max_verification: "none",
    notes: "CNPJ for entity resolution / GLEIF bridge — not ownership proof.",
  },

  // --- Guatemala INGUAT ---
  "guatemala_inguat.propietario": {
    field: "propietario",
    source: "guatemala_inguat",
    country: "Guatemala",
    proves: "proves_owned_by",
    allowed_relationship: "OWNED_BY",
    max_verification: "needs_review",
    notes:
      "INGUAT listing field labeled propietario. Treated as weak ownership claim of the establishment; may mean license holder. Cap at needs_review until corroborated.",
  },

  // --- Ecuador MINTUR ---
  "ecuador_mintur.nombre_comercial": {
    field: "Nombre Comercial",
    source: "ecuador_mintur",
    country: "Ecuador",
    proves: "proves_registered_lodging_entity",
    allowed_relationship: null,
    max_verification: "none",
    notes: "Commercial name of tourism establishment — identity only, not PropCo.",
  },

  // --- Mexico RNT ---
  "mexico_rnt.razon_social": {
    field: "razon_social / prestador",
    source: "mexico_rnt",
    country: "Mexico",
    proves: "proves_registered_lodging_entity",
    allowed_relationship: null,
    max_verification: "none",
    notes:
      "SECTUR RNT registered tourism provider. Identity/tax lead — not proven PropCo without corroboration.",
  },
  "mexico_rnt.rfc": {
    field: "RFC",
    source: "mexico_rnt",
    country: "Mexico",
    proves: "proves_identifier_only",
    allowed_relationship: null,
    max_verification: "none",
    notes: "Mexican tax ID when exposed by RNT — entity resolution only.",
  },

  "hotel_website.rfc": {
    field: "RFC (footer / privacy / invoicing)",
    source: "hotel_website",
    country: "Mexico",
    proves: "proves_identifier_only",
    allowed_relationship: null,
    max_verification: "needs_review",
    notes:
      "RFC scraped from official hotel legal/footer/privacy pages. Entity resolution only — not PropCo OWNED_BY.",
  },
  "hotel_website.razon_social": {
    field: "razón social (footer / privacy)",
    source: "hotel_website",
    country: "Mexico",
    proves: "proves_registered_lodging_entity",
    allowed_relationship: null,
    max_verification: "needs_review",
    notes:
      "Operating/legal name on the hotel's own site. Identity seed only until independently corroborated.",
  },
  "owner_portfolio.asset_list": {
    field: "portfolio / properties list",
    source: "owner_first_party",
    country: "global",
    proves: "proves_sponsored_by",
    allowed_relationship: "SPONSORED_BY",
    max_verification: "high",
    notes:
      "First-party owner portfolio listing. Classify owned vs managed vs developed vs historical before creating a hotel edge. Never copy CoStar True Owner.",
  },

  // --- Mexico SIGER / RPC ---
  "mexico_siger.accionistas": {
    field: "accionistas / socios",
    source: "mexico_siger",
    country: "Mexico",
    proves: "proves_owned_by",
    allowed_relationship: "CONTROLLED_BY",
    max_verification: "high",
    notes:
      "Corporate shareholders when retrieved from RPC/SIGER for a known PropCo entity. Proves corporate control of the company, not automatically hotel→PropCo (hotel→PropCo must already be established).",
  },

  // --- GLEIF ---
  "gleif.direct_parent": {
    field: "direct parent LEI relationship",
    source: "gleif",
    country: "global",
    proves: "proves_controlled_by",
    allowed_relationship: "CONTROLLED_BY",
    max_verification: "high",
    notes:
      "GLEIF Level-2 direct parent of a company. Never invents hotel ownership. Requires prior hotel→company edge.",
  },
  "gleif.ultimate_parent": {
    field: "ultimate parent LEI relationship",
    source: "gleif",
    country: "global",
    proves: "proves_sponsored_by",
    allowed_relationship: "SPONSORED_BY",
    max_verification: "high",
    notes:
      "GLEIF ultimate accounting parent — economic/control hierarchy for a company, not hotel PropCo discovery.",
  },
});

/**
 * @param {string} key
 * @returns {FieldSemantics | null}
 */
export function getFieldSemantics(key) {
  return FIELD_SEMANTICS[key] || null;
}

/**
 * Map a legacy ownership_signal object using documented semantics.
 * @param {object} signal
 * @returns {{
 *   entity_name: string|null,
 *   identifiers: {kind:string,value:string,country?:string}[],
 *   allowed_relationship: string|null,
 *   max_verification: string,
 *   source_semantics_keys: string[],
 *   notes: string[],
 *   legal_representative: string|null,
 * }}
 */
export function interpretOwnershipSignal(signal) {
  const s = signal && typeof signal === "object" ? signal : {};
  const country = String(s.country || "").trim();
  const notes = [];
  const identifiers = [];
  const keys = [];
  let entityName = null;
  let allowed = null;
  let maxVer = "none";
  let legalRep = null;

  const tax = String(s.tax_id || s.nit || s.ruc || s.cnpj || s.rfc || "").trim();
  const taxType = String(s.tax_id_type || "").toUpperCase();

  if (/colombia/i.test(country) || taxType === "NIT") {
    keys.push("colombia_rnt.razon_social_establecimiento", "colombia_rnt.nit");
    entityName =
      String(s.legal_name_from_registry || s.razon_social || s.owner_name || "").trim() ||
      null;
    if (tax) identifiers.push({ kind: "nit", value: tax, country: "Colombia" });
    notes.push("colombia_rnt_registered_lodging_entity_not_propco");
  } else if (/peru/i.test(country) || taxType === "RUC") {
    keys.push("peru_mincetur.razon_social", "peru_mincetur.ruc", "peru_mincetur.rep_legal");
    entityName =
      String(s.legal_name_from_registry || s.razon_social || s.commercial_name || "").trim() ||
      null;
    if (tax) identifiers.push({ kind: "ruc", value: tax, country: "Peru" });
    legalRep = String(s.legal_representative || "").trim() || null;
    if (legalRep) notes.push("peru_rep_legal_is_not_owner");
    notes.push("peru_mincetur_registered_lodging_entity_not_propco");
  } else if (/brazil|brasil/i.test(country) || taxType === "CNPJ") {
    keys.push("brazil_cadastur.nome_pessoa_juridica", "brazil_cadastur.cnpj");
    entityName =
      String(s.legal_name_from_registry || s.razao_social || s.owner_name || "").trim() ||
      null;
    if (tax) identifiers.push({ kind: "cnpj", value: tax.replace(/\D/g, ""), country: "Brazil" });
    notes.push("brazil_cadastur_registered_lodging_entity_not_propco");
  } else if (/guatemala/i.test(country)) {
    keys.push("guatemala_inguat.propietario");
    const raw = String(s.raw || s.propietario || s.owner_name || "").trim();
    entityName = raw || null;
    const sem = FIELD_SEMANTICS["guatemala_inguat.propietario"];
    allowed = sem.allowed_relationship;
    maxVer = sem.max_verification;
    notes.push("guatemala_propietario_weak_ownership_claim");
  } else if (/ecuador/i.test(country)) {
    keys.push("ecuador_mintur.nombre_comercial");
    entityName = String(s.razon_social || s.owner_name || s.nombre_comercial || "").trim() || null;
    notes.push("ecuador_mintur_identity_only");
  } else if (/mexico/i.test(country) || taxType === "RFC") {
    keys.push("mexico_rnt.razon_social", "mexico_rnt.rfc");
    entityName =
      String(s.legal_name_from_registry || s.razon_social || s.owner_name || "").trim() ||
      null;
    if (tax) identifiers.push({ kind: "rfc", value: tax, country: "Mexico" });
    notes.push("mexico_rnt_registered_provider_not_propco");
  } else {
    // Unknown country signal — entity lead only
    entityName =
      String(s.owner_name || s.propietario || s.razon_social || s.legal_name_from_registry || "").trim() ||
      null;
    if (tax) {
      identifiers.push({
        kind: taxType.toLowerCase() || "tax_id",
        value: tax,
      });
    }
    notes.push("unknown_ownership_signal_entity_lead_only");
  }

  return {
    entity_name: entityName,
    identifiers,
    allowed_relationship: allowed,
    max_verification: maxVer,
    source_semantics_keys: keys,
    notes,
    legal_representative: legalRep,
  };
}
