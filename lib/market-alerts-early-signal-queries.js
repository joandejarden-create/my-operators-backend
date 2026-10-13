/**
 * Centralized Early Signal discovery queries for Market Alerts.
 * Pre-Opening Capture V1: all eight families production-enabled + bounded ES/PT CALA packs.
 */

import {
  EARLY_SIGNAL_DISABLED_FAMILIES,
  EARLY_SIGNAL_PRODUCTION_FAMILIES,
  EARLY_SIGNAL_PRODUCTION_WHEN,
} from "./market-alerts-early-signal-config.js";

export const EARLY_SIGNAL_WHEN = EARLY_SIGNAL_PRODUCTION_WHEN;

export const EARLY_SIGNAL_FAMILIES = [
  "landSite",
  "planning",
  "earlyDevelopment",
  "mixedUse",
  "capitalFormation",
  "projectFormation",
  "adaptiveReuse",
  "openDecision",
];

export const EARLY_SIGNAL_FAMILY_LABELS = {
  landSite: "Land/Site",
  planning: "Planning",
  earlyDevelopment: "Early Development",
  mixedUse: "Mixed Use",
  capitalFormation: "Capital Formation",
  projectFormation: "Project Formation",
  adaptiveReuse: "Adaptive Reuse",
  openDecision: "Open Decision",
};

export const EARLY_SIGNAL_FAMILY_TAGS = {
  landSite: "EARLY_SIGNAL_LAND",
  planning: "EARLY_SIGNAL_PLANNING",
  earlyDevelopment: "EARLY_SIGNAL_DEVELOPMENT",
  mixedUse: "EARLY_SIGNAL_MIXED_USE",
  capitalFormation: "EARLY_SIGNAL_CAPITAL",
  projectFormation: "EARLY_SIGNAL_PROJECT_FORMATION",
  adaptiveReuse: "EARLY_SIGNAL_ADAPTIVE_REUSE",
  openDecision: "EARLY_SIGNAL_OPEN_DECISION",
};

const LOCALE_EN = { lang: "en", hl: "en-US", gl: "US", ceid: "US:en" };
const LOCALE_ES = { lang: "es", hl: "es-419", gl: "MX", ceid: "MX:es-419" };
const LOCALE_PT = { lang: "pt", hl: "pt-BR", gl: "BR", ceid: "BR:pt-419" };

function q(family, id, query, label, extra = {}) {
  return {
    family,
    id,
    query,
    label,
    lang: extra.lang || "en",
    hl: extra.hl || LOCALE_EN.hl,
    gl: extra.gl || LOCALE_EN.gl,
    ceid: extra.ceid || LOCALE_EN.ceid,
    regionPack: extra.regionPack || null,
    cala: !!extra.cala,
    requireTitleHospitality: extra.requireTitleHospitality !== false,
    ...extra,
  };
}

function cala(family, prefix, phrase, geo, n, locale = LOCALE_EN) {
  return q(family, `${prefix}-cala-${n}`, `"${phrase}" ${geo}`, `CALA ${phrase} · ${geo}`, {
    ...locale,
    cala: true,
    requireTitleHospitality: true,
    regionPack: geo,
  });
}

/** Bounded regional overlays — one geo pack string per phrase (not every city). */
const ES_MEXICO = "México OR Cancún OR \"Riviera Maya\" OR Tulum OR \"Los Cabos\" OR CDMX";
const ES_CARIBBEAN = "\"República Dominicana\" OR \"Punta Cana\" OR \"Puerto Rico\" OR Jamaica OR Bahamas";
const ES_LATAM = "Colombia OR Bogotá OR Panamá OR \"Costa Rica\" OR Perú OR Argentina";
const PT_BRAZIL = "Brasil OR \"São Paulo\" OR \"Rio de Janeiro\"";

function esPack(family, prefix, phrases, packLabel, packQuery, start = 1) {
  return phrases.map((phrase, i) =>
    q(family, `${prefix}-${start + i}`, `"${phrase}" (${packQuery})`, `ES ${phrase} · ${packLabel}`, {
      ...LOCALE_ES,
      cala: true,
      requireTitleHospitality: true,
      regionPack: packLabel,
    })
  );
}

function ptPack(family, prefix, phrases, packLabel, packQuery, start = 1) {
  return phrases.map((phrase, i) =>
    q(family, `${prefix}-${start + i}`, `"${phrase}" (${packQuery})`, `PT ${phrase} · ${packLabel}`, {
      ...LOCALE_PT,
      cala: true,
      requireTitleHospitality: true,
      regionPack: packLabel,
    })
  );
}

/** @type {Record<string, Array<object>>} */
export const EARLY_SIGNAL_QUERIES = {
  landSite: [
    q("landSite", "land-1", `"land acquired for hotel"`, "Land acquired for hotel"),
    q("landSite", "land-2", `"site acquired for hotel"`, "Site acquired for hotel"),
    q("landSite", "land-3", `"parcel acquired for hotel"`, "Parcel acquired for hotel"),
    q("landSite", "land-4", `"hotel development site"`, "Hotel development site"),
    q("landSite", "land-6", `"land acquired for resort"`, "Land acquired for resort"),
    q("landSite", "land-8", `"hotel site purchase"`, "Hotel site purchase"),
    q("landSite", "land-9", `"developer buys site for hotel"`, "Developer buys hotel site"),
    q("landSite", "land-10", `"hospitality development site"`, "Hospitality development site"),
    q("landSite", "land-11", `"ground lease" hotel`, "Ground lease hotel"),
    cala("landSite", "land", "hotel development site", "Mexico", 1),
    cala("landSite", "land", "land acquired for hotel", "Mexico", 2),
    cala("landSite", "land", "hotel development site", "Caribbean", 3),
    cala("landSite", "land", "land acquired for resort", "Jamaica", 4),
    cala("landSite", "land", "hotel development site", `"Dominican Republic"`, 5),
    ...esPack("landSite", "land-es-mx", ["terreno para hotel", "compra terreno hotel"], "Mexico", ES_MEXICO, 20),
    ...esPack("landSite", "land-es-car", ["terreno hotelero"], "Caribbean", ES_CARIBBEAN, 30),
    ...ptPack("landSite", "land-pt", ["terreno para hotel", "compra terreno hotel"], "Brazil", PT_BRAZIL, 40),
  ],
  planning: [
    q("planning", "plan-1", `"hotel planning application"`, "Hotel planning application"),
    q("planning", "plan-2", `"hotel planning approval"`, "Hotel planning approval"),
    q("planning", "plan-3", `"hotel zoning approval"`, "Hotel zoning approval"),
    q("planning", "plan-4", `"hotel zoning application"`, "Hotel zoning application"),
    q("planning", "plan-5", `"hotel plan approved"`, "Hotel plan approved"),
    q("planning", "plan-6", `"hotel development approved"`, "Hotel development approved"),
    q("planning", "plan-7", `"planning board" hotel`, "Planning board hotel"),
    q("planning", "plan-8", `"planning permission" hotel`, "Planning permission hotel"),
    q("planning", "plan-9", `"hotel site plan approval"`, "Hotel site plan approval"),
    q("planning", "plan-10", `"hotel conditional use approval"`, "Hotel conditional use approval"),
    q("planning", "plan-11", `"building permit" hotel`, "Building permit hotel"),
    q("planning", "plan-12", `"environmental approval" hotel`, "Environmental approval hotel"),
    cala("planning", "plan", "hotel planning", "Mexico", 1),
    cala("planning", "plan", "proposed hotel", "Jamaica", 2),
    cala("planning", "plan", "hotel zoning", `"Puerto Rico"`, 3),
    ...esPack(
      "planning",
      "plan-es-mx",
      ["proyecto hotelero", "aprobación proyecto hotelero", "licencia ambiental hotel", "permiso de construcción hotel"],
      "Mexico",
      ES_MEXICO,
      20
    ),
    ...esPack(
      "planning",
      "plan-es-car",
      ["aprueban hotel", "permiso hotel"],
      "Caribbean",
      ES_CARIBBEAN,
      30
    ),
    ...esPack("planning", "plan-es-latam", ["proyecto hotelero", "uso de suelo hotel"], "LatAm", ES_LATAM, 40),
    ...ptPack(
      "planning",
      "plan-pt",
      ["projeto hoteleiro", "aprovação projeto hoteleiro", "licença ambiental hotel", "licença de construção hotel"],
      "Brazil",
      PT_BRAZIL,
      50
    ),
  ],
  earlyDevelopment: [
    q(
      "earlyDevelopment",
      "dev-global-1",
      '("proposed hotel" OR "proposed resort" OR "planned hotel" OR "planned resort" OR "developer proposes hotel" OR "hotel project unveiled")',
      "Proposed/planned hotel or resort"
    ),
    q(
      "earlyDevelopment",
      "dev-global-2",
      '("new hotel proposed" OR "hotel development planned" OR "future hotel project" OR "hospitality development planned" OR "lodging development proposed")',
      "Future hotel development"
    ),
    cala("earlyDevelopment", "dev", "proposed hotel", "Mexico", 1),
    cala("earlyDevelopment", "dev", "planned resort", "Jamaica", 2),
    cala("earlyDevelopment", "dev", "hotel development", `"Costa Rica"`, 3),
    cala("earlyDevelopment", "dev", "hotel project", "Colombia", 4),
    ...esPack(
      "earlyDevelopment",
      "dev-es-mx",
      ["nuevo hotel", "proyecto hotelero", "desarrollo hotelero"],
      "Mexico",
      ES_MEXICO,
      20
    ),
    ...esPack("earlyDevelopment", "dev-es-latam", ["nuevo hotel", "proyecto hotelero"], "LatAm", ES_LATAM, 30),
    ...ptPack(
      "earlyDevelopment",
      "dev-pt",
      ["novo hotel", "projeto hoteleiro", "empreendimento hoteleiro"],
      "Brazil",
      PT_BRAZIL,
      40
    ),
  ],
  mixedUse: [
    q(
      "mixedUse",
      "mix-global-1",
      '("mixed-use" OR "mixed use") (hotel OR resort) (component OR development OR project OR tower OR masterplan OR residences)',
      "Mixed-use with hotel component"
    ),
    q(
      "mixedUse",
      "mix-global-2",
      '("branded residences" hotel OR "development includes hotel" OR "masterplan includes hotel" OR "tourism mixed-use")',
      "Hotel inside larger scheme"
    ),
  ],
  capitalFormation: [
    q("capitalFormation", "cap-1", `"hotel construction loan"`, "Hotel construction loan"),
    q("capitalFormation", "cap-2", `"hotel construction financing"`, "Hotel construction financing"),
    q("capitalFormation", "cap-3", `"hotel development financing"`, "Hotel development financing"),
    q("capitalFormation", "cap-4", `"hotel project financing"`, "Hotel project financing"),
    q("capitalFormation", "cap-5", `"hotel development loan"`, "Hotel development loan"),
    q("capitalFormation", "cap-6", `"credit approval" "hotel project"`, "Credit approval hotel project"),
    q("capitalFormation", "cap-8", `"hotel joint venture" development`, "Hotel development JV"),
    q("capitalFormation", "cap-9", `"resort development financing"`, "Resort development financing"),
    ...esPack(
      "capitalFormation",
      "cap-es",
      ["financiamiento hotel", "crédito construcción hotel", "financiamiento proyecto hotelero"],
      "Mexico+LatAm",
      `(${ES_MEXICO}) OR (${ES_LATAM})`,
      20
    ),
    ...ptPack(
      "capitalFormation",
      "cap-pt",
      ["financiamento hotel", "crédito construção hotel"],
      "Brazil",
      PT_BRAZIL,
      40
    ),
  ],
  projectFormation: [
    q("projectFormation", "team-1", `"architect appointed hotel"`, "Architect appointed hotel"),
    q("projectFormation", "team-2", `"hotel architect appointed"`, "Hotel architect appointed"),
    q("projectFormation", "team-4", `"hotel developer selected"`, "Hotel developer selected"),
    q("projectFormation", "team-5", `"hotel project manager appointed"`, "Hotel PM appointed"),
    q("projectFormation", "lead-1", `"pre-opening general manager" hotel`, "Pre-opening GM hotel"),
    q("projectFormation", "lead-2", `"pre-opening director of sales" hotel`, "Pre-opening DOS hotel"),
    q("projectFormation", "lead-3", `"opening team" "new hotel"`, "Opening team new hotel"),
    ...esPack(
      "projectFormation",
      "lead-es",
      ["gerente general preapertura", "director de ventas preapertura", "equipo de apertura hotel"],
      "CALA-ES",
      `(${ES_MEXICO}) OR (${ES_CARIBBEAN})`,
      20
    ),
    ...ptPack(
      "projectFormation",
      "lead-pt",
      ["gerente geral pré-abertura", "equipe de abertura hotel"],
      "Brazil",
      PT_BRAZIL,
      40
    ),
  ],
  adaptiveReuse: [
    q("adaptiveReuse", "reuse-1", `"office to hotel conversion"`, "Office to hotel conversion"),
    q("adaptiveReuse", "reuse-2", `"office tower to hotel"`, "Office tower to hotel"),
    q("adaptiveReuse", "reuse-3", `"office conversion hotel"`, "Office conversion hotel"),
    q("adaptiveReuse", "reuse-4", `"adaptive reuse hotel"`, "Adaptive reuse hotel"),
    q("adaptiveReuse", "reuse-5", `"warehouse to hotel"`, "Warehouse to hotel"),
    q("adaptiveReuse", "reuse-6", `"office tower" "JW Marriott"`, "Office tower JW Marriott"),
    q("adaptiveReuse", "reuse-8", `"historic building hotel conversion"`, "Historic building hotel conversion"),
  ],
  openDecision: [
    q("openDecision", "open-1", `"hotel brand under consideration"`, "Brand under consideration"),
    q("openDecision", "open-2", `"hotel operator under consideration"`, "Operator under consideration"),
    q("openDecision", "open-3", `"hotel management RFP"`, "Hotel management RFP"),
    q("openDecision", "open-4", `"hotel operator RFP"`, "Hotel operator RFP"),
    q("openDecision", "open-6", `"request for qualifications" hotel`, "RFQ hotel"),
    q("openDecision", "open-7", `"developer interest" "proposed hotel"`, "Developer interest proposed hotel"),
    q("openDecision", "open-8", `"hotel operating partner"`, "Hotel operating partner"),
    ...esPack(
      "openDecision",
      "open-es",
      ["será operado por", "acuerdo de gestión hotelera", "contrato de franquicia hotel"],
      "CALA-ES",
      `(${ES_MEXICO}) OR (${ES_LATAM})`,
      20
    ),
    ...ptPack(
      "openDecision",
      "open-pt",
      ["será operado por", "acordo de gestão hoteleira"],
      "Brazil",
      PT_BRAZIL,
      40
    ),
  ],
};

/**
 * Queries explicitly left out of production as too noisy (documented for audits).
 * land-5 "hotel development site sold" and cap-7 broad "hotel development funding" removed.
 */
export const EARLY_SIGNAL_QUERIES_LEFT_DISABLED = [
  { family: "landSite", id: "land-5", query: `"hotel development site sold"`, reason: "high secondary-market noise" },
  { family: "capitalFormation", id: "cap-7", query: `"hotel development funding"`, reason: "too broad / fundraising fluff" },
  { family: "openDecision", id: "open-5", query: `"hotel brand search"`, reason: "consumer SEO leakage" },
  { family: "adaptiveReuse", id: "reuse-7", query: `"office tower" "luxury resort"`, reason: "off-topic luxury content" },
];

/**
 * @param {string|null|undefined} family
 * @param {{ productionOnly?: boolean }} [opts]
 */
export function listEarlySignalQueries(family = null, opts = {}) {
  const productionOnly = opts.productionOnly === true;
  const families = productionOnly
    ? EARLY_SIGNAL_PRODUCTION_FAMILIES
    : EARLY_SIGNAL_FAMILIES;

  if (!family) {
    return families.flatMap((f) => EARLY_SIGNAL_QUERIES[f] || []);
  }
  const key = String(family).trim();
  const aliases = {
    land: "landSite",
    site: "landSite",
    "land/site": "landSite",
    "early-development": "earlyDevelopment",
    development: "earlyDevelopment",
    "mixed-use": "mixedUse",
    mixed: "mixedUse",
    capital: "capitalFormation",
    "project-formation": "projectFormation",
    team: "projectFormation",
    "adaptive-reuse": "adaptiveReuse",
    reuse: "adaptiveReuse",
    open: "openDecision",
    "open-decision": "openDecision",
    planning: "planning",
  };
  const resolved = EARLY_SIGNAL_QUERIES[key]
    ? key
    : aliases[key] || aliases[key.toLowerCase()] || key;
  if (productionOnly && !EARLY_SIGNAL_PRODUCTION_FAMILIES.includes(resolved)) {
    return [];
  }
  return EARLY_SIGNAL_QUERIES[resolved] || [];
}

/**
 * Diagnostic counts by language / family / region pack.
 */
export function summarizeEarlySignalQueryCatalog(queries = listEarlySignalQueries(null, { productionOnly: true })) {
  const byLanguage = {};
  const byFamily = {};
  const byRegionPack = {};
  for (const row of queries) {
    const lang = row.lang || "en";
    const fam = row.family || "unknown";
    const region = row.regionPack || (row.cala ? "cala" : "global");
    byLanguage[lang] = (byLanguage[lang] || 0) + 1;
    byFamily[fam] = (byFamily[fam] || 0) + 1;
    byRegionPack[region] = (byRegionPack[region] || 0) + 1;
  }
  return { total: queries.length, byLanguage, byFamily, byRegionPack };
}

export { EARLY_SIGNAL_PRODUCTION_FAMILIES, EARLY_SIGNAL_DISABLED_FAMILIES };

export function buildGoogleNewsRssUrl(query, opts = {}) {
  const when = opts.when || EARLY_SIGNAL_WHEN;
  const hl = opts.hl || "en-US";
  const gl = opts.gl || "US";
  const ceid = opts.ceid || "US:en";
  const qtext = `${query} when:${when}`;
  const params = new URLSearchParams({
    q: qtext,
    hl,
    gl,
    ceid,
  });
  return `https://news.google.com/rss/search?${params.toString()}`;
}

/** Build URL from a query catalog row (carries locale). */
export function buildGoogleNewsRssUrlForQuery(queryRow, opts = {}) {
  return buildGoogleNewsRssUrl(queryRow.query, {
    when: opts.when || EARLY_SIGNAL_WHEN,
    hl: queryRow.hl || opts.hl,
    gl: queryRow.gl || opts.gl,
    ceid: queryRow.ceid || opts.ceid,
  });
}

export function googleNewsSourceLabel(_family) {
  return "Google News";
}

export function googleNewsInternalFamilyLabel(family) {
  const tag = EARLY_SIGNAL_FAMILY_TAGS[family] || "EARLY_SIGNAL";
  return `Google News (${tag})`;
}
