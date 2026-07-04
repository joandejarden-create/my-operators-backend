/**
 * Phase 5C — Operator Setup alignment backfill plans (differentiated per operator).
 */

import { normalizeOptionKey } from "./operator-alignment-airtable-options-loader.js";
import { buildAliasToLiveMap } from "./operator-alignment-airtable-option-aliases.js";
import {
  normalizeAirtableMultiSelectValues,
  normalizeAirtableSelectValue,
} from "./operator-alignment-airtable-option-normalize.js";
import { getLiveOptionsList } from "./operator-alignment-airtable-options-loader.js";

const SVC = {
  fullMgmt: "Full hotel management",
  preOpen: "Pre-opening planning",
  openTrans: "Opening / transition support",
  rm: "Revenue management",
  sales: "Sales",
  dist: "Distribution / channel management",
  digital: "Digital marketing",
  acct: "Accounting / finance",
  hr: "HR / staffing",
  proc: "Procurement",
  fb: "F&B operations",
  brand: "Brand compliance support",
  ownerRep: "Owner reporting",
  asset: "Asset management support",
  tech: "Technical services coordination",
};

/** @typedef {{ value: unknown, source: string, confidence: string, note?: string }} FieldProposal */
/** @typedef {{ operatorId: string, companyName: string, skip?: boolean, skipReason?: string, fields: Record<string, FieldProposal>, notes?: string[] }} OperatorBackfillPlan */

const CITY_ALIASES = {
  cancun: "Cancún",
  "mexico city": "Mexico City",
  monterrey: "Monterrey",
  guadalajara: "Guadalajara",
  "riviera maya": "Riviera Maya",
  "punta cana": "Punta Cana",
  "santo domingo": "Santo Domingo",
  "san juan": "San Juan",
  "panama city": "Panama City",
  bogota: "Bogotá",
  medellin: "Medellín",
  lima: "Lima",
  santiago: "Santiago",
  "sao paulo": "São Paulo",
  "são paulo": "São Paulo",
  "rio de janeiro": "Rio de Janeiro",
  "san jose": "San José",
  montevideo: "Montevideo",
};

function proposal(value, source, confidence, note) {
  return { value, source, confidence, note };
}

function splitMarketTokens(text) {
  return String(text || "")
    .split(/[,;|]/)
    .map((s) => s.replace(/\([^)]*\)/g, "").trim())
    .filter(Boolean);
}

/**
 * Infer countries from market narrative (live labels only).
 * @param {string} text
 * @param {string[]} liveCountries
 */
export function inferCountriesFromMarkets(text, liveCountries) {
  const allowed = liveCountries || [];
  const lookup = buildAliasToLiveMap(allowed);
  const t = String(text || "").toLowerCase();
  const out = new Set();
  const rules = [
    [/mexico|cancún|cancun|monterrey|guadalajara|riviera maya|méxico/, "Mexico"],
    [/costa rica/, "Costa Rica"],
    [/panama/, "Panama"],
    [/colombia|bogotá|bogota|medellín|medellin|barranquilla/, "Colombia"],
    [/peru|lima/, "Peru"],
    [/chile|santiago/, "Chile"],
    [/argentina|buenos aires/, "Argentina"],
    [/brazil|são paulo|sao paulo|rio de janeiro/, "Brazil"],
    [/dominican|santo domingo|punta cana/, "Dominican Republic"],
    [/puerto rico|san juan/, "Puerto Rico"],
    [/jamaica/, "Jamaica"],
    [/cura[cç]ao|curacao/, "Curaçao"],
    [/trinidad/, "Trinidad & Tobago"],
    [/spain/, "Spain"],
    [/united states|u\.s\.|usa|miami/, "United States"],
  ];
  for (const [re, country] of rules) {
    if (re.test(t)) {
      const r = normalizeAirtableSelectValue(country, allowed, lookup);
      if (r.value) out.add(r.value);
    }
  }
  return { countries: [...out], uruguayMentioned: /montevideo|uruguay/i.test(t) };
}

/**
 * Map market tokens to live city options.
 */
export function inferCitiesFromMarkets(text, liveMarkets) {
  const allowed = liveMarkets || [];
  const lookup = buildAliasToLiveMap(allowed);
  const out = new Set();
  for (const token of splitMarketTokens(text)) {
    const nk = normalizeOptionKey(token);
    const direct = normalizeAirtableSelectValue(token, allowed, lookup);
    if (direct.value) {
      out.add(direct.value);
      continue;
    }
    for (const [alias, label] of Object.entries(CITY_ALIASES)) {
      if (nk.includes(alias) || alias.includes(nk)) {
        const r = normalizeAirtableSelectValue(label, allowed, lookup);
        if (r.value) out.add(r.value);
      }
    }
  }
  return [...out];
}

function chainFromProfile(chainStr) {
  const raw = String(chainStr || "")
    .split(/[,;]/)
    .map((s) => s.trim())
    .filter(Boolean);
  const map = {
    "upper midscale": "Upper Midscale",
    midscale: "Midscale",
    upscale: "Upscale",
    "upper upscale": "Upper Upscale",
    luxury: "Luxury",
    economy: "Economy",
    independent: "Independent",
  };
  return raw.map((s) => map[normalizeOptionKey(s)] || s).filter(Boolean);
}

import { OPERATOR_FIELD_TO_TABLE_KEY } from "./operator-alignment-operator-field-map.js";

function validateFieldSync(liveIndex, fieldName, value) {
  const tableKey = OPERATOR_FIELD_TO_TABLE_KEY[fieldName];
  if (!tableKey || !liveIndex) return { ok: true, value };
  const allowed = getLiveOptionsList(liveIndex, tableKey, fieldName);
  const lookup = buildAliasToLiveMap(allowed);
  if (Array.isArray(value)) {
    const m = normalizeAirtableMultiSelectValues(value, allowed, lookup);
    return { ok: m.warnings.length === 0 && m.values.length > 0, value: m.values, warnings: m.warnings };
  }
  const s = normalizeAirtableSelectValue(value, allowed, lookup);
  return { ok: s.ok, value: s.value, warnings: s.warning ? [s.warning] : [] };
}

export function validateOperatorProposal(fieldName, value, liveIndex) {
  return validateFieldSync(liveIndex, fieldName, value);
}

/** Differentiated profiles keyed by operatorId */
const OPERATOR_PROFILES = {
  recq3NiRxOerg4kZU: {
    archetype: "select_service_mexico",
    serviceModels: ["Select-service", "Focused-service"],
    services: [
      SVC.fullMgmt,
      SVC.preOpen,
      SVC.openTrans,
      SVC.rm,
      SVC.sales,
      SVC.dist,
      SVC.brand,
      SVC.ownerRep,
      SVC.hr,
    ],
    mgmt: ["Full third-party management", "Franchise support", "Pre-opening / transition support"],
    newBuild: "Strong",
    preOpenCap: "Advanced",
    reporting: "Monthly operating review",
    fb: "Limited F&B",
    rm: "Centralized support",
    salesPlatform: ["Regional sales", "Digital / e-commerce"],
    governance: "Monthly",
    brandFamilies: ["Independent"],
  },
  recQ6Cf8O2z0tiqBz: {
    archetype: "yucatan_select_upper_mid",
    serviceModels: ["Select-service", "Boutique"],
    services: [
      SVC.fullMgmt,
      SVC.preOpen,
      SVC.rm,
      SVC.sales,
      SVC.brand,
      SVC.ownerRep,
      SVC.proc,
      SVC.tech,
    ],
    mgmt: ["Full third-party management", "Franchise support", "Hybrid / project-specific"],
    newBuild: "Moderate",
    preOpenCap: "Standard",
    reporting: "Monthly operating review",
    fb: "Limited F&B",
    rm: "Centralized support",
    salesPlatform: ["Local sales", "Regional sales"],
    governance: "Monthly",
    brandFamilies: ["Independent", "Wyndham"],
  },
  recZPHT2zqc8K6itx: {
    archetype: "andean_commercial_platform",
    serviceModels: ["Select-service", "Full-service"],
    services: [
      SVC.fullMgmt,
      SVC.preOpen,
      SVC.openTrans,
      SVC.rm,
      SVC.sales,
      SVC.dist,
      SVC.digital,
      SVC.brand,
      SVC.ownerRep,
      SVC.acct,
    ],
    mgmt: ["Full third-party management", "Franchise support", "Pre-opening / transition support"],
    newBuild: "Strong",
    preOpenCap: "Advanced",
    reporting: "Monthly operating review",
    fb: "Moderate F&B",
    rm: "Advanced centralized platform",
    salesPlatform: ["Regional sales", "Group sales"],
    governance: "Monthly",
    brandFamilies: ["Marriott", "Hilton"],
  },
  recZgNR85WZKDItLF: {
    archetype: "full_service_lifestyle_fb",
    serviceModels: ["Full-service", "Lifestyle", "Boutique"],
    services: [
      SVC.fullMgmt,
      SVC.rm,
      SVC.sales,
      SVC.fb,
      SVC.brand,
      SVC.ownerRep,
      SVC.hr,
      SVC.proc,
    ],
    mgmt: ["Full third-party management", "Hybrid / project-specific"],
    newBuild: "Moderate",
    preOpenCap: "Limited",
    reporting: "Monthly operating review",
    fb: "Significant F&B",
    rm: "Centralized support",
    salesPlatform: ["Local sales", "Group sales"],
    governance: "Quarterly",
    brandFamilies: ["Independent", "Soft brands / collections"],
  },
  recbT3q8ApRIBu4j5: {
    archetype: "institutional_luxury",
    serviceModels: ["Full-service", "Boutique"],
    services: [
      SVC.fullMgmt,
      SVC.rm,
      SVC.sales,
      SVC.brand,
      SVC.ownerRep,
      SVC.asset,
      SVC.acct,
    ],
    mgmt: ["Full third-party management", "Asset management support"],
    newBuild: "Moderate",
    preOpenCap: "Standard",
    reporting: "Institutional reporting",
    fb: "Moderate F&B",
    rm: "Advanced centralized platform",
    salesPlatform: ["Global sales support", "Group sales"],
    governance: "Asset-management style",
    brandFamilies: ["Marriott", "Hilton", "Accor"],
  },
  recTUjuDxL96yWcQA: {
    archetype: "caribbean_upscale",
    serviceModels: ["Full-service", "Resort"],
    services: [
      SVC.fullMgmt,
      SVC.preOpen,
      SVC.rm,
      SVC.sales,
      SVC.dist,
      SVC.brand,
      SVC.ownerRep,
      SVC.fb,
    ],
    mgmt: ["Full third-party management", "Franchise support"],
    newBuild: "Moderate",
    preOpenCap: "Standard",
    reporting: "Institutional reporting",
    fb: "Moderate F&B",
    rm: "Centralized support",
    salesPlatform: ["Regional sales", "Group sales"],
    governance: "Quarterly",
    brandFamilies: ["Marriott", "Wyndham"],
  },
  recWPKu5laVZxsvpn: {
    archetype: "cala_resort_all_inclusive",
    serviceModels: ["Resort", "All-inclusive", "Full-service"],
    services: [
      SVC.fullMgmt,
      SVC.rm,
      SVC.sales,
      SVC.fb,
      SVC.brand,
      SVC.ownerRep,
      SVC.hr,
      SVC.proc,
    ],
    mgmt: ["Full third-party management", "Franchise support"],
    newBuild: "Limited",
    preOpenCap: "Limited",
    reporting: "Institutional reporting",
    fb: "Significant F&B",
    rm: "Property-level only",
    salesPlatform: ["Group sales", "Global sales support"],
    governance: "Asset-management style",
    brandFamilies: ["Marriott", "Hilton", "Hyatt"],
  },
  reckO98E46sKTn3F3: {
    archetype: "southern_cone_full_service",
    serviceModels: ["Full-service", "Boutique"],
    services: [
      SVC.fullMgmt,
      SVC.rm,
      SVC.sales,
      SVC.brand,
      SVC.ownerRep,
      SVC.acct,
    ],
    mgmt: ["Full third-party management", "Hybrid / project-specific"],
    newBuild: "Limited",
    preOpenCap: "Limited",
    reporting: "Monthly operating review",
    fb: "Moderate F&B",
    rm: "Property-level only",
    salesPlatform: ["Local sales", "Regional sales"],
    governance: "Monthly",
    brandFamilies: ["Independent"],
  },
  recwbyY4qfNP1bV3r: {
    archetype: "brazil_regional_mixed",
    serviceModels: ["Select-service", "Full-service"],
    services: [
      SVC.fullMgmt,
      SVC.preOpen,
      SVC.rm,
      SVC.sales,
      SVC.ownerRep,
      SVC.proc,
      SVC.tech,
    ],
    mgmt: ["Full third-party management", "Pre-opening / transition support", "Commercial-only support"],
    newBuild: "Moderate",
    preOpenCap: "Standard",
    reporting: "Monthly operating review",
    fb: "Limited F&B",
    rm: "Centralized support",
    salesPlatform: ["Regional sales", "Digital / e-commerce"],
    governance: "Monthly",
    brandFamilies: ["Choice", "IHG"],
  },
  recxAa86Qoc0nFRSt: {
    archetype: "central_america_resort",
    serviceModels: ["Resort", "Lifestyle", "Select-service"],
    services: [
      SVC.fullMgmt,
      SVC.preOpen,
      SVC.openTrans,
      SVC.rm,
      SVC.sales,
      SVC.brand,
      SVC.ownerRep,
      SVC.fb,
    ],
    mgmt: ["Full third-party management", "Franchise support", "Pre-opening / transition support"],
    newBuild: "Strong",
    preOpenCap: "Standard",
    reporting: "Monthly operating review",
    fb: "Moderate F&B",
    rm: "Centralized support",
    salesPlatform: ["Regional sales", "Group sales"],
    governance: "Monthly",
    brandFamilies: ["Independent", "Wyndham"],
  },
};

/**
 * Build plan from live operator candidate row.
 * @param {object} candidate — from loadActiveOperatorCandidatesForAlignment
 * @param {object} liveIndex
 */
export function buildOperatorBackfillPlan(candidate, liveIndex) {
  const operatorId = candidate.operatorId;
  const companyName = candidate.companyName;
  const profile = candidate.profile?.fields || {};
  const platform = candidate.platform?.fields || {};
  const notes = [];

  const marketText =
    platform.specificMarkets ||
    platform.topMarkets ||
    candidate.regionsSupported?.join?.(", ") ||
    "";

  const liveCountries = getLiveOptionsList(liveIndex, "platform", "Active Countries");
  const liveMarkets = getLiveOptionsList(liveIndex, "platform", "Active Markets / Cities");
  const { countries, uruguayMentioned } = inferCountriesFromMarkets(marketText, liveCountries);
  if (uruguayMentioned) {
    notes.push("Montevideo/Uruguay mentioned in markets but Uruguay is not a live country option — not written.");
  }
  const cities = inferCitiesFromMarkets(marketText, liveMarkets);

  const profileDef = OPERATOR_PROFILES[operatorId] || {
    archetype: "generic_cala",
    serviceModels: ["Select-service", "Full-service"],
    services: [SVC.fullMgmt, SVC.rm, SVC.sales, SVC.ownerRep, SVC.brand],
    mgmt: ["Full third-party management", "Franchise support"],
    newBuild: "Moderate",
    preOpenCap: "Standard",
    reporting: "Monthly operating review",
    fb: "Limited F&B",
    rm: "Centralized support",
    salesPlatform: ["Regional sales"],
    governance: "Monthly",
    brandFamilies: [],
  };

  const chainRaw = profile.chainScalesSupported || profile.chainScale || candidate.chainScales?.join(", ") || "";
  const chains = chainFromProfile(chainRaw);
  const validatedChains = validateFieldSync(liveIndex, "chainScalesSupported", chains);

  const today = new Date().toISOString().slice(0, 10);

  const fields = {
    "Active Countries": proposal(countries, "inferred", "High", "From specificMarkets / regions text"),
    "Active Markets / Cities": proposal(cities, "inferred", "High", "Exact/alias match to live city options only"),
    "Market Presence Type": proposal(
      ["Active operations"],
      "manual_sample_assumption",
      "High",
      "CALA active sample operator"
    ),
    "Service Models Supported": proposal(profileDef.serviceModels, "inferred", "Medium", profileDef.archetype),
    chainScalesSupported: proposal(
      validatedChains.ok ? validatedChains.value : ["Upper Midscale"],
      "existing",
      "High",
      "From profile chain scales"
    ),
    "Management Structures Supported": proposal(profileDef.mgmt, "inferred", "High", profileDef.archetype),
    "Offered Services": proposal(profileDef.services, "manual_sample_assumption", "High", "Differentiated service bundle"),
    "New-Build Opening Experience": proposal(profileDef.newBuild, "inferred", "Medium", profileDef.archetype),
    "Pre-Opening Support Capability": proposal(profileDef.preOpenCap, "inferred", "Medium", profileDef.archetype),
    "Owner Reporting Level": proposal(profileDef.reporting, "inferred", "Medium", profileDef.archetype),
    "F&B Capability Level": proposal(profileDef.fb, "inferred", "Medium", profileDef.archetype),
    "Revenue Management Capability": proposal(profileDef.rm, "inferred", "Medium", profileDef.archetype),
    "Sales Platform": proposal(profileDef.salesPlatform, "inferred", "Medium", profileDef.archetype),
    "Governance Cadence": proposal(profileDef.governance, "inferred", "Medium", profileDef.archetype),
    "Data Confidence Level": proposal("Inferred", "manual_sample_assumption", "High", "Sample/demo operator"),
    "Source Type": proposal(["Imported sample data"], "manual_sample_assumption", "High", "CALA fictional sample set"),
    "Last Updated Date": proposal(today, "manual_sample_assumption", "High", "Phase 5C backfill"),
  };

  if (profileDef.brandFamilies?.length) {
    fields["Brand Families Operated"] = proposal(
      profileDef.brandFamilies,
      "inferred",
      "Low",
      "Illustrative only where archetype supports"
    );
  }

  return { operatorId, companyName, fields, notes };
}

/** @returns {Map<string, OperatorBackfillPlan>} */
export function loadActiveOperatorBackfillPlans(liveIndex, candidates) {
  const map = new Map();
  for (const c of candidates || []) {
    map.set(c.operatorId, buildOperatorBackfillPlan(c, liveIndex));
  }
  return map;
}

export const ACTIVE_OPERATOR_IDS = Object.keys(OPERATOR_PROFILES);
