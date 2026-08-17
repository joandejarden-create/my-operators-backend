/**
 * Acquisition Intelligence — Stage 2 classification rules (central config).
 *
 * Deterministic only. No web research. No relationship-strength inference.
 * Company context required before treating generic titles as owner signals.
 */

import { normalizeOwnerKey } from "../gtm-owner-target/normalize.js";
import {
  FRANCHISOR_BRAND_OWNER_KEYS,
  INTEGRATED_OPERATOR_ALLOWLIST_KEYS,
} from "../gtm-owner-target/icp-classify.js";

export const CLASSIFIER_VERSION = "acquisition-classify-v1";

/** Band rank for sorting review samples. */
export const BAND_RANK = { High: 3, Medium: 2, Low: 1, Unknown: 0 };

/**
 * Brand / franchisor company tokens (substring match on normalized company).
 * Prefer matching via FRANCHISOR_BRAND_OWNER_KEYS when full name aligns.
 */
export const BRAND_COMPANY_TOKENS = [
  "marriott",
  "hilton",
  "hyatt",
  "ihg",
  "intercontinental hotels",
  "accor",
  "choice hotels",
  "wyndham",
  "radisson",
  "best western",
  "melia",
  "meliá",
  "minor hotels",
  "four seasons",
  "shangri la",
  "kempinski",
  "iberostar",
  "riu hotels",
  "barcelo",
  "barceló",
];

/** Known management / operator company tokens. */
export const OPERATOR_COMPANY_TOKENS = [
  "aimbridge",
  "remington hospitality",
  "highgate",
  "crescent hotels",
  "white lodging",
  "host hotels",
  "pyramid hotel",
  "heigroup",
  "hotel equities",
  "arbor lodging",
  "ghl hoteles",
  "grupo posadas",
  "hodelpa",
  "karisma",
  "palace resorts",
  "pueblo bonito",
];

/** Strong hospitality owner/developer company tokens. */
export const OWNER_DEVELOPER_COMPANY_TOKENS = [
  "hotel investment",
  "hospitality investment",
  "hotel capital",
  "lodging partners",
  "hotel partners",
  "hospitality partners",
  "hotel group",
  "hoteles",
  "hotels & resorts",
  "hotels and resorts",
  "resort development",
  "hotel development",
  "hospitality development",
  "real estate development",
  "family office",
  "fibra hotel",
  "fibra inn",
  "reit",
];

/** Broker / advisory firm tokens. */
export const BROKER_ADVISORY_COMPANY_TOKENS = [
  "jll",
  "cbre",
  "colliers",
  "hvs",
  "horwath",
  "pwc",
  "deloit",
  "kpmg",
  "ey ",
  "ernst & young",
  "cushman",
  "newmark",
  "savills",
  "knight frank",
];

/** Lender / capital markets firm tokens. */
export const LENDER_CAPITAL_COMPANY_TOKENS = [
  "bank",
  "capital markets",
  "private equity",
  "credit",
  "lending",
  "mortgage",
  "debt fund",
  "mezzanine",
];

/**
 * Title patterns — scored with company context.
 * @typedef {{ id: string, pattern: RegExp, weight: number, signals: string[] }} TitleRule
 */

/** @type {TitleRule[]} */
export const TITLE_DIRECT_PROSPECT_RULES = [
  {
    id: "cio_hotel_invest",
    pattern: /\b(chief investment officer|cio)\b/i,
    weight: 3,
    signals: ["cio_title"],
  },
  {
    id: "head_hotel_dev",
    pattern: /\b(head|svp|evp|vp|director|principal).{0,24}(hotel|hospitality|lodging).{0,16}(development|acquisitions|investment)/i,
    weight: 3,
    signals: ["hotel_dev_acq_title"],
  },
  {
    id: "acquisitions_director",
    pattern: /\b(director|vp|head|svp|evp).{0,20}(acquisitions?|investments?)\b/i,
    weight: 2,
    signals: ["acquisitions_title"],
  },
  {
    id: "hotel_development",
    pattern: /\b(hotel|hospitality|lodging).{0,20}(development|developer)\b|\b(development|developer).{0,20}(hotel|hospitality)\b/i,
    weight: 2,
    signals: ["hotel_development_title"],
  },
  {
    id: "founder_principal_ownerish",
    pattern: /\b(founder|co-?founder|principal|managing partner|owner)\b/i,
    weight: 1,
    signals: ["founder_or_principal"],
  },
  {
    id: "asset_mgmt_exec",
    pattern: /\b(asset\s+manag|portfolio\s+manag).{0,30}(hotel|hospitality|lodging)?/i,
    weight: 2,
    signals: ["asset_mgmt_title"],
  },
  {
    id: "real_estate_investor",
    pattern: /\b(real\s+estate\s+investor|hotel\s+investor|hospitality\s+investor)\b/i,
    weight: 3,
    signals: ["investor_title"],
  },
];

/** @type {TitleRule[]} */
export const TITLE_CONNECTOR_RULES = [
  {
    id: "hospitality_attorney",
    pattern: /\b(attorney|counsel|lawyer|legal).{0,40}(hotel|hospitality|lodging)?|(hotel|hospitality).{0,20}(attorney|counsel)/i,
    weight: 3,
    signals: ["attorney"],
  },
  {
    id: "hotel_broker",
    pattern: /\b(broker|brokerage|investment\s+sales)\b/i,
    weight: 3,
    signals: ["broker"],
  },
  {
    id: "feasibility_advisory",
    pattern: /\b(feasibility|valuation|appraisal|hospitality\s+advisor|hotel\s+advisor|consultant)\b/i,
    weight: 3,
    signals: ["feasibility_advisory"],
  },
  {
    id: "lender_capital_markets",
    pattern: /\b(lender|lending|capital\s+markets|debt|credit|mortgage)\b/i,
    weight: 3,
    signals: ["lender_capital"],
  },
  {
    id: "architect_pm",
    pattern: /\b(architect|architecture|project\s+manag).{0,30}(hotel|hospitality)?/i,
    weight: 2,
    signals: ["architect_pm"],
  },
  {
    id: "brand_dev_exec",
    pattern: /\b(development|growth|franchise).{0,20}(director|vp|svp|evp|manager|executive)|cdo\b|chief\s+development/i,
    weight: 3,
    signals: ["brand_or_operator_dev"],
  },
  {
    id: "asset_manager_connector",
    pattern: /\basset\s+manag/i,
    weight: 2,
    signals: ["asset_manager"],
  },
];

/** @type {TitleRule[]} */
export const TITLE_DECISION_VISIBILITY_RULES = [
  {
    id: "dev_acq_brand_ops",
    pattern:
      /\b(development|acquisitions?|reflag|conversion|franchise|brand\s+selection|operator\s+selection|hma|financing|reposition|mixed-?use|branded\s+residenc)/i,
    weight: 2,
    signals: ["decision_vocab"],
  },
  {
    id: "c_suite_invest",
    pattern: /\b(cio|cdo|cfo|ceo|chief\s+investment|chief\s+development)\b/i,
    weight: 2,
    signals: ["c_suite"],
  },
];

/** Titles that alone are weak / ambiguous without company proof. */
export const AMBIGUOUS_TITLE_PATTERNS = [
  /\bpartner\b/i,
  /\bpresident\b/i,
  /\bmanaging\s+director\b/i,
  /\bdirector\b/i,
  /\bvp\b|\bvice\s+president\b/i,
];

/** Explicit low-relevance exclusions (vendors / non-deal roles). */
export const LOW_RELEVANCE_TITLE_PATTERNS = [
  /\b(software|saas|salesperson|account\s+executive|marketing\s+manager|hr\b|human\s+resources|recruiter|supply\s+chain|procurement|it\s+manager|engineer\b(?!.*(civil|structural|hotel)))\b/i,
  /\b(front\s+desk|housekeeping|revenue\s+manager|general\s+manager|gm\b|rooms\s+division)\b/i,
];

/** Geography tokens — only apply when clearly present in company/title text. */
export const CALA_GEO_RULES = [
  { value: "Mexico", pattern: /\b(mexico|méxico|mexican|cancun|cancún|cabo|riviera\s+maya|guadalajara|monterrey|cdmx|ciudad\s+de\s+m[eé]xico)\b/i },
  { value: "Dominican Republic", pattern: /\b(dominican|santo\s+domingo|punta\s+cana|cap\s*cana)\b/i },
  { value: "Costa Rica", pattern: /\b(costa\s+rica|guanacaste|san\s+jos[eé])\b/i },
  { value: "Colombia", pattern: /\b(colombia|bogot[aá]|cartagena|medell[ií]n)\b/i },
  { value: "Guatemala", pattern: /\b(guatemala|antigua\s+guatemala)\b/i },
  {
    value: "Wider CALA",
    pattern: /\b(caribbean|central\s+america|latin\s+america|latam|cala|panama|jamaica|bahamas|barbados|aruba|curacao|curaçao)\b/i,
  },
  {
    value: "Warm Europe",
    pattern: /\b(spain|espa[nñ]a|madrid|barcelona|portugal|lisbon|lisboa|italy|france|uk\b|united\s+kingdom|london)\b/i,
  },
];

/**
 * @param {string} company
 */
export function classifyCompanyTypeHints(company) {
  const raw = String(company || "").trim();
  const key = normalizeOwnerKey(raw);
  const lower = key;

  /** @type {string[]} */
  const classes = [];
  /** @type {string[]} */
  const signals = [];
  let isBrand = false;
  let isOperator = false;
  let isOwnerDeveloperLikely = false;
  let isBrokerAdvisory = false;
  let isLenderCapital = false;

  if (!raw) {
    return {
      classes,
      signals,
      isBrand,
      isOperator,
      isOwnerDeveloperLikely,
      isBrokerAdvisory,
      isLenderCapital,
      confidenceBoost: 0,
    };
  }

  if (FRANCHISOR_BRAND_OWNER_KEYS.has(key) || BRAND_COMPANY_TOKENS.some((t) => lower.includes(t))) {
    isBrand = true;
    classes.push("Brand");
    signals.push("known_brand_company");
  }

  if (
    INTEGRATED_OPERATOR_ALLOWLIST_KEYS.has(key) ||
    OPERATOR_COMPANY_TOKENS.some((t) => lower.includes(t))
  ) {
    isOperator = true;
    classes.push("Operator / Management Company");
    signals.push("known_operator_company");
  }

  if (OWNER_DEVELOPER_COMPANY_TOKENS.some((t) => lower.includes(t))) {
    isOwnerDeveloperLikely = true;
    signals.push("owner_developer_company_token");
  }
  if (/\bfamily\s+office\b/i.test(raw)) {
    classes.push("Family Office");
    isOwnerDeveloperLikely = true;
    signals.push("family_office_company");
  }
  if (/\b(fibra|reit)\b/i.test(raw)) {
    classes.push("Real Estate Investor");
    isOwnerDeveloperLikely = true;
    signals.push("reit_or_fibra");
  }
  if (/\b(desarroll|developer|development)\b/i.test(raw) && /\b(hotel|hospitality|real\s+estate|inmobili)/i.test(raw)) {
    classes.push("Developer");
    isOwnerDeveloperLikely = true;
    signals.push("developer_company");
  }
  if (/\b(hotel|hoteles|hospitality|lodging|resort)/i.test(raw) && !isBrand) {
    if (!classes.includes("Hotel Owner") && isOwnerDeveloperLikely) {
      classes.push("Hotel Owner");
    }
    signals.push("hospitality_company_name");
  }

  if (BROKER_ADVISORY_COMPANY_TOKENS.some((t) => lower.includes(` ${t}`) || lower.startsWith(t) || lower.includes(t))) {
    isBrokerAdvisory = true;
    classes.push("Broker");
    signals.push("broker_advisory_firm");
  }
  if (/\b(hvs|horwath|feasibility)\b/i.test(raw)) {
    classes.push("Feasibility / Advisory");
    isBrokerAdvisory = true;
    signals.push("feasibility_firm");
  }
  if (LENDER_CAPITAL_COMPANY_TOKENS.some((t) => lower.includes(t))) {
    isLenderCapital = true;
    classes.push("Lender");
    signals.push("lender_capital_firm");
  }

  let confidenceBoost = 0;
  if (isBrand || isOperator) confidenceBoost += 2;
  if (isOwnerDeveloperLikely) confidenceBoost += 1;
  if (isBrokerAdvisory || isLenderCapital) confidenceBoost += 1;

  return {
    classes: [...new Set(classes)],
    signals,
    isBrand,
    isOperator,
    isOwnerDeveloperLikely,
    isBrokerAdvisory,
    isLenderCapital,
    confidenceBoost,
  };
}

/**
 * Match company against Owner Target name index.
 * @param {string} company
 * @param {Map<string, { id: string, ownerName: string }>} ownerIndexByKey
 */
export function matchOwnerTarget(company, ownerIndexByKey) {
  const key = normalizeOwnerKey(company);
  if (!key || !ownerIndexByKey?.size) {
    return { match: "No", confidence: "High", ownerTargetId: null, ownerName: null };
  }
  const exact = ownerIndexByKey.get(key);
  if (exact) {
    return {
      match: "Yes",
      confidence: "High",
      ownerTargetId: exact.id,
      ownerName: exact.ownerName,
    };
  }

  // Conservative contains match — only if company key length >= 8 and unique hit
  const hits = [];
  for (const [ok, row] of ownerIndexByKey.entries()) {
    if (ok.length < 8 || key.length < 8) continue;
    if (ok.includes(key) || key.includes(ok)) hits.push(row);
  }
  if (hits.length === 1) {
    return {
      match: "Uncertain",
      confidence: "Low",
      ownerTargetId: hits[0].id,
      ownerName: hits[0].ownerName,
    };
  }
  return { match: "No", confidence: "Medium", ownerTargetId: null, ownerName: null };
}
