/**
 * Realistic backfill values for Operator Setup - Leadership Team Members.
 * Only uses allowed select options from operator-leadership-member-map.js.
 */

import {
  MAP_LEADERSHIP_MEMBER,
  LEADERSHIP_MEMBER_SELECT_OPTIONS,
  filterToAllowedOptions,
  normalizeWhitespace,
  parseExperienceYears,
} from "../api/lib/operator-leadership-member-map.js";

export { MAP_LEADERSHIP_MEMBER };

/** Last-name keyed samples (Antillano / demo executives). */
export const SAMPLES_BY_LAST_NAME = {
  santos: {
    hospitalityExperienceYears: 25,
    companyTenureYears: 12,
    priorBackground: "Marriott divisional operator — Caribbean & island markets",
    languages: ["English", "Spanish"],
    marketExperience: ["Puerto Rico", "Dominican Republic", "Caribbean"],
    coreExpertise: ["Operations", "Owner Relations", "Brand Compliance"],
    relevantAssetTypes: ["Resort", "Full-Service", "Branded"],
  },
  fernandez: {
    hospitalityExperienceYears: 25,
    companyTenureYears: 8,
    priorBackground: "Major branded flags — resort & full-service operations",
    languages: ["English", "Spanish"],
    marketExperience: ["Dominican Republic", "Puerto Rico", "Caribbean"],
    coreExpertise: ["Operations", "Brand Compliance"],
    relevantAssetTypes: ["Resort", "Full-Service"],
  },
  ohara: {
    hospitalityExperienceYears: 22,
    companyTenureYears: 5,
    priorBackground: "Listed hospitality REIT finance — US GAAP consolidation",
    languages: ["English"],
    marketExperience: ["United States", "Puerto Rico", "Dominican Republic"],
    coreExpertise: ["Finance & Owner Reporting", "Legal / Compliance"],
    relevantAssetTypes: ["Full-Service", "Branded", "Mixed-Use"],
  },
  reyes: {
    hospitalityExperienceYears: 18,
    companyTenureYears: 7,
    priorBackground: "Caribbean hospitality labor — union & contract properties",
    languages: ["English", "Spanish"],
    marketExperience: ["Puerto Rico", "Dominican Republic", "Caribbean"],
    coreExpertise: ["HR / Talent", "Operations"],
    relevantAssetTypes: ["Resort", "Full-Service", "Independent"],
  },
  alvarez: {
    hospitalityExperienceYears: 20,
    companyTenureYears: 12,
    priorBackground: "LATAM hospitality platforms — growth & governance",
    languages: ["English", "Spanish", "Portuguese"],
    marketExperience: ["Mexico", "CALA — Regional", "Brazil"],
    coreExpertise: ["Development", "Owner Relations"],
    relevantAssetTypes: ["Lifestyle", "Branded", "Mixed-Use"],
  },
  chen: {
    hospitalityExperienceYears: 22,
    companyTenureYears: 6,
    priorBackground: "Marriott regional operations — brand-managed full-service",
    languages: ["English"],
    marketExperience: ["United States", "Mexico", "Caribbean"],
    coreExpertise: ["Operations", "Pre-Opening / Transitions"],
    relevantAssetTypes: ["Full-Service", "Select-Service", "Urban"],
  },
  corvinos: {
    hospitalityExperienceYears: 28,
    companyTenureYears: 9,
    priorBackground: "Hilton CALA development — 350+ management & franchise agreements",
    languages: ["English", "Spanish"],
    marketExperience: ["CALA — Regional", "Caribbean", "Mexico", "Brazil"],
    coreExpertise: ["Development", "Owner Relations", "Pre-Opening / Transitions"],
    relevantAssetTypes: ["Resort", "Full-Service", "Branded"],
  },
  register: {
    hospitalityExperienceYears: 24,
    companyTenureYears: 4,
    priorBackground: "Highgate Caribbean & Latin America — owner relations & deal sourcing",
    languages: ["English", "Spanish"],
    marketExperience: ["CALA — Regional", "Caribbean", "Mexico", "Dominican Republic"],
    coreExpertise: ["Development", "Owner Relations", "Sales & Marketing"],
    relevantAssetTypes: ["Resort", "Lifestyle", "Branded"],
  },
  pergola: {
    hospitalityExperienceYears: 20,
    companyTenureYears: 6,
    priorBackground: "Branded resort operations — revenue & pre-opening excellence",
    languages: ["English", "Spanish", "Portuguese"],
    marketExperience: ["Brazil", "CALA — Regional", "Caribbean"],
    coreExpertise: ["Operations", "Revenue Management", "Brand Compliance"],
    relevantAssetTypes: ["Resort", "Full-Service", "Branded"],
  },
  larralde: {
    hospitalityExperienceYears: 18,
    companyTenureYears: 5,
    priorBackground: "Hilton All-Inclusive finance — cross-border planning & forecasting",
    languages: ["English", "Spanish"],
    marketExperience: ["CALA — Regional", "Caribbean", "Mexico"],
    coreExpertise: ["Finance & Owner Reporting", "Owner Relations"],
    relevantAssetTypes: ["Resort", "Full-Service", "Branded"],
  },
};

function hashSeed(str) {
  let h = 0;
  const s = String(str || "");
  for (let i = 0; i < s.length; i++) {
    h = (h * 31 + s.charCodeAt(i)) >>> 0;
  }
  return h;
}

export function nameLastToken(name) {
  const parts = String(name || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[''\u2018\u2019`]/g, "")
    .split(/\s+/)
    .filter(Boolean);
  return parts[parts.length - 1] || "";
}

export function isFieldEmpty(value, kind) {
  if (value == null || value === "") return true;
  if (kind === "multi") return !Array.isArray(value) || value.length === 0;
  if (kind === "number") return parseExperienceYears(value) == null;
  if (kind === "attachment") {
    if (typeof value === "string") return !normalizeWhitespace(value);
    if (Array.isArray(value)) return !value.length;
    return true;
  }
  return !normalizeWhitespace(value);
}

function pickMarketsFromText(text) {
  const t = String(text || "").toLowerCase();
  const picks = [];
  const rules = [
    [/puerto rico|\bpr\b/, "Puerto Rico"],
    [/dominican|\bdr\b/, "Dominican Republic"],
    [/mexico/, "Mexico"],
    [/brazil/, "Brazil"],
    [/colombia/, "Colombia"],
    [/costa rica/, "Costa Rica"],
    [/panama/, "Panama"],
    [/chile/, "Chile"],
    [/peru/, "Peru"],
    [/argentina/, "Argentina"],
    [/caribbean|island/, "Caribbean"],
    [/\bcala\b|latin america|latam/, "CALA — Regional"],
    [/united states|\busa\b|\bu\.s\./, "United States"],
    [/europe/, "Europe"],
  ];
  for (const [re, label] of rules) {
    if (re.test(t) && !picks.includes(label)) picks.push(label);
  }
  if (!picks.length) picks.push("CALA — Regional", "Caribbean");
  return filterToAllowedOptions(picks, "marketExperience").slice(0, 5);
}

function pickLanguagesFromText(text, name) {
  const t = `${text} ${name}`.toLowerCase();
  const langs = ["English"];
  if (/spanish|español|bilingual|latam|cala|caribbean|mexico|dr\b|puerto rico|dominican/i.test(t)) {
    langs.push("Spanish");
  }
  if (/portuguese|brazil/i.test(t)) langs.push("Portuguese");
  if (/french/i.test(t)) langs.push("French");
  return filterToAllowedOptions(langs, "languages");
}

function pickExpertiseFromTitle(title, role, summary) {
  const t = `${title} ${role} ${summary}`.toLowerCase();
  const picks = [];
  const rules = [
    [/chief executive|\bceo\b|president|managing director/i, ["Owner Relations", "Operations", "Development"]],
    [/chief operating|\bcoo\b|vp of operations|vp operations|director of operations/i, ["Operations", "Pre-Opening / Transitions", "Brand Compliance"]],
    [/chief development|\bcdr\b|development officer|vp development|business development/i, ["Development", "Owner Relations", "Pre-Opening / Transitions"]],
    [/chief financial|\bcfo\b|vp finance|finance director|ops finance/i, ["Finance & Owner Reporting", "Owner Relations"]],
    [/revenue|commercial|sales|marketing/i, ["Revenue Management", "Sales & Marketing", "Distribution"]],
    [/human resources|\bhr\b|people|talent/i, ["HR / Talent", "Operations"]],
    [/technology|\bit\b|systems|digital/i, ["Technology", "Operations"]],
    [/legal|compliance|counsel/i, ["Legal / Compliance", "Brand Compliance"]],
    [/f&b|food|beverage|culinary|lifestyle/i, ["F&B / Lifestyle", "Operations"]],
    [/pre-opening|transition|opening/i, ["Pre-Opening / Transitions", "Operations"]],
    [/owner relation/i, ["Owner Relations", "Finance & Owner Reporting"]],
  ];
  for (const [re, tags] of rules) {
    if (re.test(t)) tags.forEach((x) => picks.push(x));
  }
  if (!picks.length) picks.push("Operations", "Owner Relations");
  return filterToAllowedOptions(picks, "coreExpertise").slice(0, 4);
}

function pickAssetTypesFromText(title, role, summary) {
  const t = `${title} ${role} ${summary}`.toLowerCase();
  const picks = [];
  if (/resort|beach|all-inclusive/i.test(t)) picks.push("Resort");
  if (/lifestyle/i.test(t)) picks.push("Lifestyle");
  if (/select[- ]?service|limited[- ]?service/i.test(t)) picks.push("Select-Service");
  if (/full[- ]?service|urban|convention/i.test(t)) picks.push("Full-Service", "Urban");
  if (/independent/i.test(t)) picks.push("Independent");
  if (/mixed[- ]?use/i.test(t)) picks.push("Mixed-Use");
  if (/airport/i.test(t)) picks.push("Airport");
  if (!picks.length) picks.push("Full-Service", "Branded", "Resort");
  return filterToAllowedOptions(picks, "relevantAssetTypes").slice(0, 4);
}

function pickPriorBackground(title, role) {
  const t = `${title} ${role}`.toLowerCase();
  if (/hilton/i.test(t)) return "Hilton — CALA / all-inclusive & full-service portfolio";
  if (/marriott/i.test(t)) return "Marriott — regional operations & brand-managed assets";
  if (/hyatt|ihg|accor|wyndham|choice/i.test(t)) return "Major branded hospitality group — multi-market portfolio";
  if (/development|business development/i.test(t)) return "Third-party management & owner relations — growth markets";
  if (/finance|cfo/i.test(t)) return "Hospitality finance — owner reporting & asset-level P&L";
  if (/revenue|commercial/i.test(t)) return "Revenue management & commercial strategy — branded hotels";
  if (/hr|human resources/i.test(t)) return "Hospitality HR — openings, labor relations & talent systems";
  if (/technology|it\b/i.test(t)) return "Hospitality technology — PMS, integrations & cybersecurity";
  return "CALA hospitality operator — branded and independent assets";
}

function deterministicYears(name, title) {
  const h = hashSeed(`${name}|${title}`);
  const hospitality = 14 + (h % 14);
  const company = 3 + (h % 12);
  return { hospitalityExperienceYears: hospitality, companyTenureYears: company };
}

function uiAvatarUrl(name) {
  const q = encodeURIComponent(normalizeWhitespace(name) || "Executive");
  return `https://ui-avatars.com/api/?name=${q}&size=512&background=0f172a&color=f1f5f9&bold=true`;
}

function buildSummary(name, title, role) {
  const n = normalizeWhitespace(name) || "This executive";
  const t = normalizeWhitespace(title) || "leadership";
  const r = normalizeWhitespace(role);
  const region = r ? r.split("·")[0].trim() : "the portfolio";
  return `${n} serves as ${t}, with accountability for operating performance, owner alignment, and team execution across ${region}. Brings a track record of stabilizing assets, supporting transitions, and translating strategy into property-level results.`;
}

function buildBio(name, title, priorBackground) {
  const n = normalizeWhitespace(name) || "The executive";
  const t = normalizeWhitespace(title) || "a senior leadership role";
  const prior = normalizeWhitespace(priorBackground) || "leading branded and independent hospitality assets";
  return `${n} leads in ${t} with depth from ${prior}. Known for clear owner communication, disciplined operating cadence, and hands-on support during pre-opening, repositioning, and stabilization assignments.`;
}

function sampleByLastName(name) {
  const key = nameLastToken(name);
  return key ? SAMPLES_BY_LAST_NAME[key] : null;
}

/**
 * Build full Airtable field patch (snake_case keys) for one leadership row.
 * @param {object} fields - existing Airtable fields
 * @param {{ displayOrder?: number, operatorLabel?: string }} [ctx]
 */
export function buildLeadershipMemberBackfillPatch(fields, ctx) {
  const f = fields || {};
  const name = normalizeWhitespace(f[MAP_LEADERSHIP_MEMBER.name] || f.Name);
  const title = normalizeWhitespace(f[MAP_LEADERSHIP_MEMBER.title]);
  const role = normalizeWhitespace(f[MAP_LEADERSHIP_MEMBER.role]);
  const summaryExisting = normalizeWhitespace(f[MAP_LEADERSHIP_MEMBER.summary]);
  const bioExisting = normalizeWhitespace(f[MAP_LEADERSHIP_MEMBER.bio]);
  const narrative = [summaryExisting, bioExisting, title, role, ctx?.operatorLabel].join(" ");

  const named = sampleByLastName(name);
  const years = named
    ? {
        hospitalityExperienceYears: named.hospitalityExperienceYears,
        companyTenureYears: named.companyTenureYears,
      }
    : deterministicYears(name, title);

  const prior =
    named?.priorBackground || pickPriorBackground(title, role);
  const languages = named?.languages || pickLanguagesFromText(narrative, name);
  const markets = named?.marketExperience || pickMarketsFromText(narrative);
  const expertise =
    named?.coreExpertise || pickExpertiseFromTitle(title, role, summaryExisting);
  const assets =
    named?.relevantAssetTypes || pickAssetTypesFromText(title, role, summaryExisting);

  const patch = {};

  if (ctx?.displayOrder != null && isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.displayOrder], "number")) {
    patch[MAP_LEADERSHIP_MEMBER.displayOrder] = ctx.displayOrder;
  }
  if (!name && ctx?.fallbackName) {
    patch[MAP_LEADERSHIP_MEMBER.name] = ctx.fallbackName;
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.title], "text") && title) {
    patch[MAP_LEADERSHIP_MEMBER.title] = title;
  } else if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.title], "text")) {
    patch[MAP_LEADERSHIP_MEMBER.title] = "Senior Vice President";
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.role], "text")) {
    patch[MAP_LEADERSHIP_MEMBER.role] = role || "CALA · Regional leadership";
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.summary], "text")) {
    patch[MAP_LEADERSHIP_MEMBER.summary] = buildSummary(name || "Executive", patch[MAP_LEADERSHIP_MEMBER.title] || title, patch[MAP_LEADERSHIP_MEMBER.role] || role);
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.bio], "text")) {
    patch[MAP_LEADERSHIP_MEMBER.bio] = buildBio(
      name || "Executive",
      patch[MAP_LEADERSHIP_MEMBER.title] || title,
      prior
    );
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.headshot], "attachment")) {
    patch[MAP_LEADERSHIP_MEMBER.headshot] = uiAvatarUrl(name || "Executive");
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears], "number")) {
    patch[MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears] = years.hospitalityExperienceYears;
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.companyTenureYears], "number")) {
    patch[MAP_LEADERSHIP_MEMBER.companyTenureYears] = years.companyTenureYears;
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.priorBackground], "text")) {
    patch[MAP_LEADERSHIP_MEMBER.priorBackground] = prior;
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.languages], "multi") && languages.length) {
    patch[MAP_LEADERSHIP_MEMBER.languages] = languages;
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.marketExperience], "multi") && markets.length) {
    patch[MAP_LEADERSHIP_MEMBER.marketExperience] = markets;
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.coreExpertise], "multi") && expertise.length) {
    patch[MAP_LEADERSHIP_MEMBER.coreExpertise] = expertise;
  }
  if (isFieldEmpty(f[MAP_LEADERSHIP_MEMBER.relevantAssetTypes], "multi") && assets.length) {
    patch[MAP_LEADERSHIP_MEMBER.relevantAssetTypes] = assets;
  }

  return patch;
}

/**
 * Merge patch into existing fields; skip keys that are already populated unless overwrite.
 */
export function mergeLeadershipBackfill(fields, patch, { overwrite = false } = {}) {
  const out = {};
  const kinds = {
    [MAP_LEADERSHIP_MEMBER.languages]: "multi",
    [MAP_LEADERSHIP_MEMBER.marketExperience]: "multi",
    [MAP_LEADERSHIP_MEMBER.coreExpertise]: "multi",
    [MAP_LEADERSHIP_MEMBER.relevantAssetTypes]: "multi",
    [MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears]: "number",
    [MAP_LEADERSHIP_MEMBER.companyTenureYears]: "number",
    [MAP_LEADERSHIP_MEMBER.displayOrder]: "number",
    [MAP_LEADERSHIP_MEMBER.headshot]: "attachment",
  };

  for (const [key, value] of Object.entries(patch || {})) {
    const kind = kinds[key] || "text";
    if (overwrite || isFieldEmpty(fields?.[key], kind)) {
      out[key] = value;
    }
  }
  return out;
}

export function listTrackedFieldKeys() {
  return Object.values(MAP_LEADERSHIP_MEMBER);
}

export function validatePatchOptions(patch) {
  const warnings = [];
  for (const key of [
    "languages",
    "marketExperience",
    "coreExpertise",
    "relevantAssetTypes",
  ]) {
    const airtableKey = MAP_LEADERSHIP_MEMBER[key];
    const vals = patch[airtableKey];
    if (!vals) continue;
    const allowed = new Set(LEADERSHIP_MEMBER_SELECT_OPTIONS[key] || []);
    const bad = (vals || []).filter((v) => !allowed.has(v));
    if (bad.length) warnings.push(`${key}: invalid ${bad.join(", ")}`);
  }
  return warnings;
}
