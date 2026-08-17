/**
 * Operator Setup — Leadership Team Members child table field map + select options.
 * Keep in sync with public/js/operator-leadership-member-map.js (browser copy).
 */

/** Airtable column names (snake_case) on Operator Setup - Leadership Team Members */
export const MAP_LEADERSHIP_MEMBER = {
  displayOrder: "display_order",
  name: "name",
  title: "title",
  role: "role",
  summary: "summary",
  bio: "bio",
  headshot: "headshot",
  hospitalityExperienceYears: "hospitality_experience_years",
  companyTenureYears: "company_tenure_years",
  priorBackground: "prior_background",
  languages: "languages",
  marketExperience: "market_experience",
  coreExpertise: "core_expertise",
  relevantAssetTypes: "relevant_asset_types",
};

/** Allowed multiple-select choices (Airtable + form + validation). */
export const LEADERSHIP_MEMBER_SELECT_OPTIONS = {
  languages: [
    "English",
    "Spanish",
    "Portuguese",
    "French",
    "Italian",
    "German",
    "Mandarin",
    "Japanese",
    "Arabic",
    "Other",
  ],
  marketExperience: [
    "United States",
    "Mexico",
    "Dominican Republic",
    "Puerto Rico",
    "Costa Rica",
    "Panama",
    "Colombia",
    "Brazil",
    "Chile",
    "Peru",
    "Argentina",
    "Caribbean",
    "CALA — Regional",
    "Europe",
    "Middle East",
    "Central America",
    "South America",
  ],
  coreExpertise: [
    "Revenue Management",
    "Direct Booking",
    "Distribution",
    "Operations",
    "Pre-Opening / Transitions",
    "Development",
    "Finance & Owner Reporting",
    "F&B / Lifestyle",
    "Brand Compliance",
    "Sales & Marketing",
    "Technology",
    "HR / Talent",
    "Legal / Compliance",
    "Owner Relations",
  ],
  relevantAssetTypes: [
    "Resort",
    "Lifestyle",
    "Independent",
    "Full-Service",
    "Select-Service",
    "Extended-Stay",
    "Urban",
    "Airport",
    "Convention",
    "Mixed-Use",
    "Branded",
    "Soft Brand",
    "All-Inclusive",
  ],
};

const MULTI_KEYS = ["languages", "marketExperience", "coreExpertise", "relevantAssetTypes"];

export function normalizeWhitespace(s) {
  return String(s == null ? "" : s).replace(/\s+/g, " ").trim();
}

export function parseExecMultiSelectValue(value) {
  if (value == null || value === "") return [];
  if (Array.isArray(value)) {
    return value.map((v) => normalizeWhitespace(v)).filter(Boolean);
  }
  if (typeof value === "string") {
    const t = value.trim();
    if (!t) return [];
    if (t.startsWith("[")) {
      try {
        const p = JSON.parse(t);
        return Array.isArray(p) ? parseExecMultiSelectValue(p) : [];
      } catch {
        /* fall through */
      }
    }
    return t
      .split(/[,;\n|]+/)
      .map((x) => normalizeWhitespace(x))
      .filter(Boolean);
  }
  return [];
}

export function filterToAllowedOptions(values, optionKey) {
  const allowed = new Set(LEADERSHIP_MEMBER_SELECT_OPTIONS[optionKey] || []);
  return parseExecMultiSelectValue(values).filter((v) => allowed.has(v));
}

/** Read path — preserve every option stored on the Airtable row (no whitelist drop). */
export function multiSelectFromAirtableField(values) {
  return parseExecMultiSelectValue(values);
}

export function parseExperienceYears(value) {
  if (value == null || value === "") return null;
  const n = Number(String(value).replace(/[^\d.]/g, ""));
  if (!Number.isFinite(n) || n < 0 || n > 80) return null;
  return Math.round(n * 10) / 10;
}

/** Owner-facing line: "15 yrs hospitality | 5 yrs with company" */
export function formatLeaderExperienceLine(hospitalityYears, companyTenureYears) {
  const parts = [];
  const h = parseExperienceYears(hospitalityYears);
  const c = parseExperienceYears(companyTenureYears);
  if (h != null) parts.push(`${h} yrs hospitality`);
  if (c != null) parts.push(`${c} yrs with company`);
  return parts.join(" | ");
}

/**
 * Map Airtable child row fields → Explorer / DNA leader object.
 * @param {object} rf - Airtable fields
 */
export function mapLeadershipMemberFieldsFromAirtable(rf) {
  const f = rf || {};
  const languages = multiSelectFromAirtableField(f[MAP_LEADERSHIP_MEMBER.languages]);
  const marketExperience = multiSelectFromAirtableField(f[MAP_LEADERSHIP_MEMBER.marketExperience]);
  const coreExpertise = multiSelectFromAirtableField(f[MAP_LEADERSHIP_MEMBER.coreExpertise]);
  const relevantAssetTypes = multiSelectFromAirtableField(
    f[MAP_LEADERSHIP_MEMBER.relevantAssetTypes]
  );
  const hospitalityExperienceYears = parseExperienceYears(
    f[MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears]
  );
  const companyTenureYears = parseExperienceYears(f[MAP_LEADERSHIP_MEMBER.companyTenureYears]);

  return {
    hospitalityExperienceYears,
    companyTenureYears,
    priorBackground: normalizeWhitespace(f[MAP_LEADERSHIP_MEMBER.priorBackground]),
    languages,
    marketExperience,
    coreExpertise,
    relevantAssetTypes,
    experienceLine: formatLeaderExperienceLine(hospitalityExperienceYears, companyTenureYears),
  };
}

/**
 * Build Airtable child payload from one executive form index bucket.
 * @param {object} row - keyed by short field names (name, languages, …)
 */
export function mapExecRowToAirtableChildFields(row) {
  const out = {
    [MAP_LEADERSHIP_MEMBER.displayOrder]: row.display_order,
    [MAP_LEADERSHIP_MEMBER.name]: normalizeWhitespace(row.name || ""),
    [MAP_LEADERSHIP_MEMBER.title]: normalizeWhitespace(row.title || ""),
    [MAP_LEADERSHIP_MEMBER.role]: normalizeWhitespace(row.role || ""),
    [MAP_LEADERSHIP_MEMBER.summary]: normalizeWhitespace(row.summary || ""),
    [MAP_LEADERSHIP_MEMBER.bio]: normalizeWhitespace(row.bio || ""),
    [MAP_LEADERSHIP_MEMBER.headshot]: row.headshot || "",
    [MAP_LEADERSHIP_MEMBER.priorBackground]: normalizeWhitespace(row.prior_background || ""),
  };

  const h = parseExperienceYears(row.hospitality_experience_years);
  const c = parseExperienceYears(row.company_tenure_years);
  if (h != null) out[MAP_LEADERSHIP_MEMBER.hospitalityExperienceYears] = h;
  if (c != null) out[MAP_LEADERSHIP_MEMBER.companyTenureYears] = c;

  MULTI_KEYS.forEach(function (key) {
    const airtableKey = MAP_LEADERSHIP_MEMBER[key];
    const formKey =
      key === "marketExperience"
        ? "market_experience"
        : key === "coreExpertise"
          ? "core_expertise"
          : key === "relevantAssetTypes"
            ? "relevant_asset_types"
            : "languages";
    const vals = filterToAllowedOptions(row[formKey], key);
    if (vals.length) out[airtableKey] = vals;
  });

  return out;
}

/** Form repeater suffixes collected from exec_N_* keys */
export const EXEC_FORM_FIELD_SUFFIXES = [
  "name",
  "title",
  "role",
  "summary",
  "bio",
  "headshot",
  "hospitality_experience_years",
  "company_tenure_years",
  "prior_background",
  "languages",
  "market_experience",
  "core_expertise",
  "relevant_asset_types",
];

export const EXEC_FORM_FIELD_PATTERN = new RegExp(
  "^exec_(\\d+)_(" + EXEC_FORM_FIELD_SUFFIXES.join("|") + ")$"
);

function nzLeadershipText(v) {
  return v != null && String(v).trim() !== "" ? String(v).trim() : "";
}

/**
 * Flatten detail API `leadershipTeam` into form `prefill` keys (`exec_N_*`) for the generic prefill loop.
 */
export function applyLeadershipTeamToExecPrefill(prefill, team) {
  if (!prefill || !Array.isArray(team) || team.length === 0) return prefill;
  team.slice(0, 24).forEach(function (row, idx) {
    const n = idx + 1;
    const p = "exec_" + n + "_";
    const name = nzLeadershipText(row.name);
    if (name) prefill[p + "name"] = name;
    const title = nzLeadershipText(row.title);
    if (title) prefill[p + "title"] = title;
    const roleLine = nzLeadershipText(row.function) || nzLeadershipText(row.role);
    if (roleLine) prefill[p + "role"] = roleLine;
    const sum =
      nzLeadershipText(row.summary) ||
      nzLeadershipText(row.experienceSummary) ||
      nzLeadershipText(row.shortBio);
    if (sum) prefill[p + "summary"] = sum;
    const bioText =
      nzLeadershipText(row.bio) ||
      nzLeadershipText(row.shortBio) ||
      nzLeadershipText(row.experienceSummary);
    if (bioText) prefill[p + "bio"] = bioText;
    if (row.headshotUrl) prefill[p + "headshot"] = String(row.headshotUrl);
    if (row.hospitalityExperienceYears != null && row.hospitalityExperienceYears !== "") {
      prefill[p + "hospitality_experience_years"] = row.hospitalityExperienceYears;
    }
    if (row.companyTenureYears != null && row.companyTenureYears !== "") {
      prefill[p + "company_tenure_years"] = row.companyTenureYears;
    }
    if (row.priorBackground) prefill[p + "prior_background"] = String(row.priorBackground);
    if (Array.isArray(row.languages) && row.languages.length) prefill[p + "languages"] = row.languages;
    if (Array.isArray(row.marketExperience) && row.marketExperience.length) {
      prefill[p + "market_experience"] = row.marketExperience;
    }
    if (Array.isArray(row.coreExpertise) && row.coreExpertise.length) {
      prefill[p + "core_expertise"] = row.coreExpertise;
    }
    if (Array.isArray(row.relevantAssetTypes) && row.relevantAssetTypes.length) {
      prefill[p + "relevant_asset_types"] = row.relevantAssetTypes;
    }
  });
  return prefill;
}
