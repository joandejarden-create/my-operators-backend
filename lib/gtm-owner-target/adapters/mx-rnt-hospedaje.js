/**
 * Mexico RNT (Registro Nacional de Turismo) hospedaje lookup helpers.
 * Phase 2 adapter — official SECTUR consulta portal.
 *
 * Env overrides:
 *   MX_RNT_PORTAL_URL — public consulta base URL (default: rnt-consulta.sectur.gob.mx)
 *   MX_RNT_SEARCH_URL — POST endpoint if discovered/proxied
 */
import {
  resolveMxRntPortalUrl,
  mxRegistryFallbackSteps,
  MX_RNT_PORTAL_OFFICIAL,
  MX_RNT_PORTAL_DEPRECATED,
} from "./mx-rnt-portal-config.js";

export { resolveMxRntPortalUrl, MX_RNT_PORTAL_OFFICIAL, MX_RNT_PORTAL_DEPRECATED };
export const MX_RNT_PORTAL_URL = MX_RNT_PORTAL_OFFICIAL;
export const MX_RNT_PST_HOSPEDAJE = "Hospedaje";

/** RNT estado dropdown labels (must match portal). */
export const MX_RNT_STATE_OPTIONS = [
  "Aguascalientes",
  "Baja California",
  "Baja California Sur",
  "Campeche",
  "Chiapas",
  "Chihuahua",
  "Ciudad de México",
  "Coahuila de Zaragoza",
  "Colima",
  "Durango",
  "Guerrero",
  "Guanajuato",
  "Hidalgo",
  "Jalisco",
  "México",
  "Michoacán de Ocampo",
  "Morelos",
  "Nayarit",
  "Nuevo León",
  "Oaxaca",
  "Puebla",
  "Querétaro",
  "Quintana Roo",
  "San Luis Potosí",
  "Sinaloa",
  "Sonora",
  "Tabasco",
  "Tamaulipas",
  "Tlaxcala",
  "Veracruz de Ignacio de la Llave",
  "Yucatán",
  "Zacatecas",
];

/** CoStar / census city hints → RNT estado label. */
export const MX_CITY_TO_RNT_STATE = new Map([
  ["mexico city", "Ciudad de México"],
  ["ciudad de mexico", "Ciudad de México"],
  ["cdmx", "Ciudad de México"],
  ["cancun", "Quintana Roo"],
  ["playa del carmen", "Quintana Roo"],
  ["cozumel", "Quintana Roo"],
  ["tulum", "Quintana Roo"],
  ["riviera maya", "Quintana Roo"],
  ["merida", "Yucatán"],
  ["guadalajara", "Jalisco"],
  ["puerto vallarta", "Jalisco"],
  ["nuevo vallarta", "Nayarit"],
  ["monterrey", "Nuevo León"],
  ["los cabos", "Baja California Sur"],
  ["cabo san lucas", "Baja California Sur"],
  ["san jose del cabo", "Baja California Sur"],
  ["la paz", "Baja California Sur"],
  ["acapulco", "Guerrero"],
  ["mazatlan", "Sinaloa"],
  ["oaxaca", "Oaxaca"],
  ["puebla", "Puebla"],
  ["queretaro", "Querétaro"],
  ["leon", "Guanajuato"],
  ["san miguel de allende", "Guanajuato"],
  ["toluca", "México"],
  ["hermosillo", "Sonora"],
  ["tijuana", "Baja California"],
  ["veracruz", "Veracruz de Ignacio de la Llave"],
]);

/**
 * @param {string} city
 * @param {string} [submarket]
 * @param {string} [market]
 */
export function inferMxRntState(city, submarket, market) {
  for (const candidate of [city, submarket, market]) {
    const key = normalizeCityKey(candidate);
    if (MX_CITY_TO_RNT_STATE.has(key)) return MX_CITY_TO_RNT_STATE.get(key);
  }
  for (const candidate of [city, submarket, market]) {
    const key = normalizeCityKey(candidate);
    for (const [cityKey, state] of MX_CITY_TO_RNT_STATE) {
      if (key.includes(cityKey) || cityKey.includes(key)) return state;
    }
  }
  return null;
}

function normalizeCityKey(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * @param {object} hit parsed RNT provider record
 */
export function normalizeRntHospedajeHit(hit) {
  return {
    pstType: hit.pstType || hit.tipoPst || MX_RNT_PST_HOSPEDAJE,
    razonSocial: String(hit.razonSocial || hit.razon_social || "").trim(),
    nombreComercial: String(hit.nombreComercial || hit.nombre_comercial || hit.nombreComercial || "").trim(),
    certificateNumber: String(hit.certificateNumber || hit.numeroCertificado || hit.folio || "").trim(),
    status: String(hit.status || hit.estado || "unknown").trim(),
    state: String(hit.state || hit.estadoGeo || "").trim(),
    city: String(hit.city || hit.municipio || "").trim(),
    website: String(hit.website || hit.paginaWeb || "").trim(),
    address: String(hit.address || hit.calle || "").trim(),
    verificationUrl: String(hit.verificationUrl || resolveMxRntPortalUrl()).trim(),
  };
}

/**
 * Build draft registry enrichment from RNT hit (entity bridge — legal rep still from SIGER).
 * @param {object} options
 * @param {object} options.queueItem
 * @param {object} options.rntHit
 * @param {object} [options.property]
 */
export function buildDraftEnrichmentFromRntHit({ queueItem, rntHit, property }) {
  const hit = normalizeRntHospedajeHit(rntHit);
  const slug = slugify(queueItem.ownerName);
  return {
    ownerName: queueItem.ownerName,
    ownerTargetId: queueItem.id || null,
    enrichedAt: null,
    enrichedBy: "mx_rnt_adapter_draft",
    status: "draft",
    bridgeProperty: property
      ? {
          buildingName: property.buildingName,
          city: property.city,
          country: property.country,
        }
      : null,
    registry: {
      system: "MX_RNT",
      country: "Mexico",
      entityName: hit.razonSocial || hit.nombreComercial || queueItem.entitySearchName,
      entityId: hit.certificateNumber || null,
      entityIdLabel: "RNT Certificate",
      legalRepresentative: null,
      entityStatus: hit.status,
      verificationUrl: hit.verificationUrl,
      lookupNotes: [
        "RNT hospedaje hit — confirm razón social then lookup representante legal in SIGER.",
        property ? `Bridged via property: ${property.buildingName} (${property.city})` : null,
        hit.nombreComercial ? `Nombre comercial RNT: ${hit.nombreComercial}` : null,
      ].filter(Boolean),
      nextStep: "SIGER lookup by razón social for legal representative + RFC",
      commercialRegistrySystem: "MX_SIGER",
      commercialRegistryUrl: queueItem.commercialRegistryUrl || "https://www.siger.gob.mx/",
    },
    contact: {
      name: null,
      title: null,
      email: null,
      phone: null,
      linkedIn: null,
      website: hit.website || null,
      verificationTier: "V3",
      verificationSource: "public_registry",
    },
    _draftFileHint: `data/internal/gtm-registry-enrichments/drafts/${slug}.json`,
  };
}

/**
 * Build RNT search plan for manual or automated lookup.
 * @param {object} queueItem
 * @param {object} [property]
 */
export function buildMxRntSearchPlan(queueItem, property) {
  const portalUrl = resolveMxRntPortalUrl();
  const state = inferMxRntState(property?.city, property?.submarket, property?.market);
  const commercialName = property?.buildingName || queueItem.entitySearchName;
  const bridgeStrategy = queueItem.bridgeStrategy || "direct_entity";
  if (!portalUrl) {
    return {
      portalUrl: null,
      skipped: true,
      reason: "MX_RNT_LOOKUP_ENABLED not set or RNT disabled",
      commercialName,
      state,
      ownerName: queueItem.ownerName,
      property: property || null,
      manualSteps: [],
      fallbackSteps: mxRegistryFallbackSteps(bridgeStrategy),
    };
  }
  return {
    portalUrl,
    deprecatedPortalUrl: MX_RNT_PORTAL_DEPRECATED,
    pstType: MX_RNT_PST_HOSPEDAJE,
    commercialName,
    state: state || "REQUIRED — infer from property city",
    ownerName: queueItem.ownerName,
    property: property || null,
    manualSteps: [
      `Open official RNT consulta: ${portalUrl}`,
      `(Do NOT use rntsecturgob.com — unofficial/dead mirror.)`,
      `Search: Nombre Comercial = "${commercialName}"`,
      state ? `Estado = "${state}"` : "Set Estado from property city (see inferMxRntState)",
      `Tipo PST = ${MX_RNT_PST_HOSPEDAJE}`,
      "Save razón social + certificate folio + verification URL",
      "Then SIGER lookup for representante legal",
    ],
    fallbackSteps: mxRegistryFallbackSteps(bridgeStrategy),
  };
}

/**
 * Optional fetch when MX_RNT_SEARCH_URL is configured.
 * @param {{ commercialName: string, state: string, pstType?: string }} params
 */
export async function fetchMxRntHospedajeSearch(params) {
  const endpoint = process.env.MX_RNT_SEARCH_URL;
  if (!endpoint) {
    return { ok: false, skipped: true, reason: "MX_RNT_SEARCH_URL not configured" };
  }

  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "User-Agent": "DealCapture-GTM-Registry/1.0",
    },
    body: JSON.stringify({
      nombreComercial: params.commercialName,
      estado: params.state,
      tipoPst: params.pstType || MX_RNT_PST_HOSPEDAJE,
    }),
  });

  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    return { ok: false, error: "non_json_response", status: res.status, raw: text.slice(0, 500) };
  }

  if (!res.ok) {
    return { ok: false, error: "http_error", status: res.status, body: json };
  }

  const hits = Array.isArray(json) ? json : json.results || json.data || [json];
  return { ok: true, hits: hits.map(normalizeRntHospedajeHit) };
}

function slugify(value) {
  return String(value || "owner")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 64);
}
