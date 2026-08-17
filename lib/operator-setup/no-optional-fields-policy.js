/**
 * D.4C — No-Optional-Fields product policy.
 * Every retained Setup field is REQUIRED (value OR controlled state). No OPTIONAL.
 */

export const POLICY_VERSION = "operator-setup-no-optional-fields-v1";

/** Explicit response states (auditable). Not empty cells. */
export const CTRL = Object.freeze({
  NPD: "Not publicly disclosed",
  NA: "Not applicable",
  RV: "Requires validation",
  NO_DIFF: "No sufficiently differentiated public evidence identified",
  NO_OPS: "No sufficiently specific public evidence identified",
  NO_CALA_COUNTRY: "No verified CALA operating presence",
  NO_CALA_MARKET: "No verified CALA market/city mapping",
  NO_MULTI: "No verified evidence",
  NO_MARKETS_NOTE: "No verified non-taxonomy market notes beyond Active Countries and Market Presence Type",
});

export const STATE = Object.freeze({
  VERIFIED: "VERIFIED",
  SUPPORTED_SYNTHESIS: "SUPPORTED_SYNTHESIS",
  NOT_PUBLICLY_DISCLOSED: "NOT_PUBLICLY_DISCLOSED",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  REQUIRES_VALIDATION: "REQUIRES_VALIDATION",
});

/**
 * Binary classification only — no OPTIONAL.
 * @typedef {'RETAIN — REQUIRED'|'MOVE TO CLAIMS'|'PRESENTATION / WORKFLOW'|'DEPRECATE'} FieldClass
 */

/** Profile product columns retained as REQUIRED */
export const PROFILE_RETAIN_REQUIRED = Object.freeze([
  {
    key: "company_name",
    why: "Canonical operator identity",
    type: "singleLineText",
  },
  {
    key: "website",
    why: "Official company URL",
    type: "url",
  },
  {
    key: "headquarters",
    why: "Where the company is based",
    type: "multilineText",
  },
  {
    key: "companySize",
    why: "Approximate portfolio scale band",
    type: "singleSelect",
  },
  {
    key: "Brand Families Operated",
    why: "Brand-family experience for Fit brand compatibility",
    type: "multipleSelects",
  },
  {
    key: "Service Models Supported",
    why: "Service-model experience for Fit segment/asset",
    type: "multipleSelects",
  },
  {
    key: "propertyTypes",
    why: "Hotel-type experience",
    type: "multipleSelects",
  },
  {
    key: "additionalExperience",
    why: "Urban/resort/conversion experience flags",
    type: "multipleSelects",
  },
  {
    key: "chainScalesSupported",
    why: "Chain-scale experience when evidenced; else controlled state",
    type: "multipleSelects",
  },
  {
    key: "Soft Brand / Lifestyle Experience",
    why: "Soft-brand depth with existing controlled options",
    type: "singleSelect",
  },
  {
    key: "companyDescription",
    why: "Owner-facing who-is-this (1–3 factual sentences)",
    type: "multilineText",
    narrative: true,
  },
  {
    key: "companyHistory",
    why: "Founding/evolution/current form — researchable; controlled NPD when thin",
    type: "multilineText",
    narrative: true,
  },
  {
    key: "differentiators",
    why: "Evidence-backed differentiators or explicit no-diff state",
    type: "multilineText",
    narrative: true,
  },
]);

/** Master fields required for Profile product completeness (not Profile columns) */
export const MASTER_RETAIN_REQUIRED = Object.freeze([
  { key: "Operator Parent Company", why: "Parent / corporate context" },
  { key: "Operating Model", why: "How the company operates hotels" },
  { key: "Management Availability", why: "Third-party management availability" },
]);

/** Platform product columns retained as REQUIRED */
export const PLATFORM_RETAIN_REQUIRED = Object.freeze([
  { key: "company_name", why: "Row identity", type: "singleLineText" },
  {
    key: "Active Countries",
    why: "Verified CALA taxonomy countries OR controlled no-presence state",
    type: "multipleSelects",
  },
  {
    key: "Market Presence Type",
    why: "Active / pipeline / no known presence posture",
    type: "multipleSelects",
  },
  {
    key: "specificMarkets",
    why: "Non-taxonomy geography notes or controlled empty-note state",
    type: "multilineText",
  },
  {
    key: "Active Markets / Cities",
    why: "CALA city/corridor mapping OR controlled no-mapping state",
    type: "multipleSelects",
  },
  {
    key: "cap_profile_operational",
    why: "Operating platform narrative — Writer v2 + controlled no-evidence",
    type: "multilineText",
    narrative: true,
  },
]);

/** Explicitly removed from retained product (were optional / unjustified) */
export const REMOVED_FROM_RETAINED = Object.freeze({
  profile: [
    {
      key: "primaryServiceModel",
      class: "DEPRECATE",
      why: "Redundant with Service Models Supported; fails field-value test as separate required column",
    },
    {
      key: "companyTagline",
      class: "DEPRECATE",
      why: "Low owner decision value; inventing taglines forbidden — do not require 36/36 marketing slogans",
    },
  ],
  platform: [
    {
      key: "cap_profile_commercial",
      class: "MOVE TO CLAIMS",
      why: "No completed Writer v2 commercial contract; not justified as required Setup narrative before Fit",
    },
    {
      key: "cap_profile_transition",
      class: "MOVE TO CLAIMS",
      why: "Already MOVE TO CLAIMS per D.2/D.5; not Setup narrative",
    },
  ],
});

export const EXEMPLAR_MASTER_IDS = Object.freeze([
  "recWPKu5laVZxsvpn", // Hotel Equities
  "recF5Z87OAqFgndoq", // Arbor
]);

export const PREVIEW_OPERATOR_NAMES = Object.freeze([
  "Marriott International (Managed)",
  "Hilton (Managed)",
  "Accor (Managed)",
  "IHG Hotels & Resorts (Managed)",
  "Highgate",
  "Aimbridge Hospitality (LATAM)",
  "GHL Hoteles (GHL Holding)",
  "Remington Hospitality",
  "Grupo Iberostar",
  "Barceló Hotel Group",
  "Meliá Hotels International",
  "Shangri-La Group",
  "Four Seasons Hotels and Resorts",
  "Rosewood Hotel Group",
  "Hyatt (Managed)",
]);

const BRAND_FAMILY_RULES = [
  { re: /marriott|autograph|tribute|design hotels|moxy|aloft|w hotels|sheraton|westin|st\.?\s*regis|ritz|edition|jw marriott|courtyard|residence inn|fairfield|springhill|towneplace|ac hotels|delta hotels|protea|city express/i, family: "Marriott" },
  { re: /hilton|curio|tapestry|lxr|canopy|motto|tempo|signia|waldorf|doubletree|embassy|hilton garden|hampton|homewood|home2|tru\b/i, family: "Hilton" },
  { re: /hyatt|andaz|unbound|alila|thompson|destination|caption|jdv|miraval|park hyatt|grand hyatt|hyatt regency|hyatt place|hyatt house|hyatt centric/i, family: "Hyatt" },
  { re: /\bihg\b|intercontinental|holiday inn|crowne plaza|hotel indigo|voco|kimpton|six senses|regent|even hotels|avid|candlewood|staybridge|garner/i, family: "IHG" },
  { re: /accor|sofitel|pullman|novotel|mercure|ibis|fairmont|raffles|swissôtel|swissotel|mgallery|tribe|greet|movenpick|mövenpick|grand mercure/i, family: "Accor" },
  { re: /wyndham|ramada|days inn|super 8|la quinta|tryp|dolce|registry collection|trademark|baymont/i, family: "Wyndham" },
  { re: /choice|radisson|park inn|country inn|quality inn|comfort|econo lodge|ascend|cambria|radisson collection/i, family: "Choice" },
  { re: /sonesta|royal sonesta|sonesta select|sonesta es|sonesta simply/i, family: "Sonesta" },
  { re: /soft brand|autograph|curio|tapestry|unbound|tribute|design hotels|mgallery|kimpton|voco/i, family: "Soft brands / collections" },
];

export function mapBrandToFamilies(brandNames = []) {
  const out = new Set();
  for (const b of brandNames) {
    const s = String(b || "");
    let hit = false;
    for (const rule of BRAND_FAMILY_RULES) {
      if (rule.re.test(s)) {
        out.add(rule.family);
        hit = true;
      }
    }
    if (!hit && s.trim()) out.add("Independent");
  }
  return [...out].sort();
}

export function mapOmToServiceModels(om, existing = []) {
  const set = new Set((existing || []).map(String));
  const o = String(om || "");
  if (/Third-Party|Asset Manager/i.test(o)) set.add("Third-Party Management");
  if (/Integrated Owner|Owner-Operator/i.test(o)) {
    set.add("Full-service");
  }
  if (/Brand \/ Operator|Integrated Brand/i.test(o)) {
    set.add("Full-service");
  }
  if (!set.size) set.add(CTRL.NO_MULTI);
  return [...set];
}

export function derivePropertyTypes(counts = {}, hotelTypes = []) {
  const set = new Set();
  if (counts.resort > 0) set.add("Resort");
  if (counts.allInclusive > 0) set.add("All-Inclusive");
  if (counts.extendedStay > 0) set.add("Extended Stay");
  for (const t of hotelTypes || []) {
    const s = String(t);
    if (/full.?service/i.test(s)) set.add("Full Service");
    if (/select.?service|limited/i.test(s)) set.add("Select Service");
    if (/boutique/i.test(s)) set.add("Boutique");
    if (/lifestyle/i.test(s)) set.add("Lifestyle");
    if (/convention|conference/i.test(s)) set.add("Conference Center");
    if (/resort/i.test(s)) set.add("Resort");
    if (/all.?inclusive/i.test(s)) set.add("All-Inclusive");
    if (/extended/i.test(s)) set.add("Extended Stay");
  }
  return set.size ? [...set] : [CTRL.NO_MULTI];
}

export function deriveAdditionalExperience(counts = {}, developmentContexts = []) {
  const set = new Set();
  if (counts.resort > 0) set.add("Resort");
  if (counts.urban > 0) set.add("Urban");
  if (counts.extendedStay > 0) set.add("Extended Stay");
  if (counts.conversion > 0) set.add("Conversion");
  if (counts.allInclusive > 0) set.add("Resort");
  for (const d of developmentContexts || []) {
    if (/conversion|reflag|reposition/i.test(String(d))) set.add("Conversion");
    if (/mixed.?use/i.test(String(d))) set.add("Mixed-Use");
    if (/airport/i.test(String(d))) set.add("Airport");
  }
  return set.size ? [...set] : [CTRL.NO_MULTI];
}

export function softBrandFromFamilies(families = [], om = "") {
  const soft = (families || []).includes("Soft brands / collections");
  if (soft) return "Moderate";
  if (/Brand \/ Operator|Integrated Brand|Integrated Owner/i.test(String(om))) return "Limited";
  if ((families || []).some((f) => ["Marriott", "Hilton", "Hyatt", "IHG", "Accor"].includes(f))) return "Limited";
  return "None documented";
}

export function isBlankValue(v) {
  if (v === null || v === undefined) return true;
  if (typeof v === "string") return v.trim().length === 0;
  if (Array.isArray(v)) return v.length === 0;
  return false;
}

export function isControlledText(v) {
  const s = String(v || "").trim();
  return Object.values(CTRL).includes(s);
}

export function isControlledMulti(v) {
  if (!Array.isArray(v) || !v.length) return false;
  return v.every((x) => [CTRL.NO_CALA_COUNTRY, CTRL.NO_CALA_MARKET, CTRL.NO_MULTI, CTRL.RV].includes(String(x)));
}

export function classifyStateFromValue(fieldKey, value) {
  if (isBlankValue(value)) return null;
  if (Array.isArray(value)) {
    if (value.includes(CTRL.NO_CALA_COUNTRY) || value.includes(CTRL.NO_CALA_MARKET) || value.includes(CTRL.NO_MULTI)) {
      return STATE.NOT_PUBLICLY_DISCLOSED;
    }
    if (value.includes(CTRL.RV)) return STATE.REQUIRES_VALIDATION;
    return STATE.VERIFIED;
  }
  const s = String(value);
  if (s === CTRL.NPD || s === CTRL.NO_DIFF || s === CTRL.NO_OPS || s === CTRL.NO_MARKETS_NOTE) {
    return STATE.NOT_PUBLICLY_DISCLOSED;
  }
  if (s === CTRL.NA) return STATE.NOT_APPLICABLE;
  if (s === CTRL.RV || s === "Not disclosed" || s === "Unknown") return STATE.REQUIRES_VALIDATION;
  if (fieldKey === "Soft Brand / Lifestyle Experience" && (s === "None documented" || s === "Unknown")) {
    return STATE.NOT_PUBLICLY_DISCLOSED;
  }
  if (fieldKey === "companySize" && s === "Not disclosed") return STATE.NOT_PUBLICLY_DISCLOSED;
  return STATE.VERIFIED;
}
