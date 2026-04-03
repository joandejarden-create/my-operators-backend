import {
  buildThirdPartyOperatorPrefillFromContext,
  fetchAllRecordsFromAirtable,
  fetchThirdPartyOperatorPrefillContext,
  THIRD_PARTY_OPERATOR_BASICS_TABLE,
} from "./lib/build-third-party-operator-prefill.js";
import { loadDefaultThirdPartyOperatorFormConstraints, REGIONS_HIDDEN_ALLOWED } from "./lib/third-party-operator-form-constraints.js";

const SAMPLE_OPERATOR_COMPANIES = [
  "Summit Harbor Hospitality",
  "Northbridge Hotel Management",
  "BluePeak Lodging Partners",
  "Atlas Crest Operations",
  "Lighthouse Urban Stays",
  "Crescent Trail Management",
  "Harborline Resorts & Hotels",
  "Pinnacle Select Service Group",
  "Evergreen Hospitality Operators",
  "Waypoint Hotel Services",
];

const SKIP_PREFILL_KEYS = new Set(["caseStudiesDetail", "ownerDiligenceQa"]);

/**
 * @param {unknown} raw
 * @param {{ type: string, allowed: Set<string> }} meta
 * @returns {string[]}
 */
function tokensForConstraint(raw, meta) {
  if (raw == null || raw === "") return [];
  if (meta.type === "select") {
    if (Array.isArray(raw)) return raw.map((x) => String(x).trim()).filter(Boolean);
    const s = String(raw).trim();
    return s ? [s] : [];
  }
  if (Array.isArray(raw)) return raw.map((x) => String(x).trim()).filter(Boolean);
  return String(raw)
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);
}

/**
 * @param {string} fieldName
 * @param {unknown} raw
 * @param {Map<string, { type: string, allowed: Set<string> }>} fields
 * @returns {object[]}
 */
function issuesForPrefillField(fieldName, raw, fields) {
  const meta = fields.get(fieldName);
  if (!meta) return [];

  const issues = [];
  const tokens = tokensForConstraint(raw, meta);

  if (meta.type === "select" && tokens.length > 1) {
    issues.push({
      field: fieldName,
      value: tokens,
      problem: "multiple_values_for_single_select",
    });
    return issues;
  }

  const check = (t) => {
    if (!meta.allowed.has(t)) {
      issues.push({
        field: fieldName,
        value: t,
        allowedOptionsPreview: [...meta.allowed].slice(0, 20),
        allowedOptionCount: meta.allowed.size,
      });
    }
  };

  if (meta.type === "select") {
    const t = tokens[0];
    if (t !== undefined) check(t);
    return issues;
  }

  for (const t of tokens) check(t);
  return issues;
}

function issuesForRegions(raw) {
  const list = Array.isArray(raw)
    ? raw.map((x) => String(x).trim()).filter(Boolean)
    : String(raw || "")
        .split(",")
        .map((t) => t.trim())
        .filter(Boolean);

  const issues = [];
  for (const t of list) {
    if (!REGIONS_HIDDEN_ALLOWED.has(t)) {
      issues.push({
        field: "regions",
        value: t,
        hint: "Must match a geo-grid region label (hidden field)",
        allowedOptionsPreview: [...REGIONS_HIDDEN_ALLOWED],
      });
    }
  }
  return issues;
}

/**
 * @param {object} prefill
 * @param {Map<string, { type: string, allowed: Set<string> }>} fields
 */
export function auditPrefillAgainstForm(prefill, fields) {
  const issues = [];
  for (const [key, val] of Object.entries(prefill)) {
    if (SKIP_PREFILL_KEYS.has(key)) continue;
    if (key === "regions") {
      issues.push(...issuesForRegions(val));
      continue;
    }
    issues.push(...issuesForPrefillField(key, val, fields));
  }
  return issues;
}

export default async function getThirdPartyOperatorPrefillQa(req, res) {
  try {
    const { fields } = loadDefaultThirdPartyOperatorFormConstraints();

    let companies = SAMPLE_OPERATOR_COMPANIES;
    const q = String((req.query && req.query.companies) || "").trim();
    if (q) {
      companies = q
        .split(",")
        .map((c) => c.trim())
        .filter(Boolean);
    }

    const [basics, ctx] = await Promise.all([
      fetchAllRecordsFromAirtable(THIRD_PARTY_OPERATOR_BASICS_TABLE),
      fetchThirdPartyOperatorPrefillContext(),
    ]);
    const byCompany = new Map();
    for (const r of basics) {
      const name = String((r.fields || {})["Company Name"] || "").trim().toLowerCase();
      if (name) byCompany.set(name, r);
    }

    const operators = [];
    for (const companyName of companies) {
      const row = byCompany.get(companyName.trim().toLowerCase());
      if (!row) {
        operators.push({
          companyName,
          recordId: null,
          error: "not_found_in_airtable_basics",
          issueCount: 0,
          issues: [],
        });
        continue;
      }

      const { prefill } = buildThirdPartyOperatorPrefillFromContext(row, ctx);
      const issues = auditPrefillAgainstForm(prefill, fields);
      operators.push({
        companyName: String((row.fields || {})["Company Name"] || "").trim() || companyName,
        recordId: row.id,
        issueCount: issues.length,
        issues,
      });
    }

    const totalIssues = operators.reduce((n, o) => n + (o.issueCount || 0), 0);

    return res.json({
      success: true,
      report: "third_party_operator_prefill_qa",
      description:
        "Prefill values compared to option values in public/third-party-operator-intake.html (selects + checkboxes; regions vs geo-grid labels). Free-text fields are skipped.",
      constrainedFieldCount: fields.size,
      sampleCompanyCount: companies.length,
      totalIssues,
      operators,
    });
  } catch (err) {
    const status = err && typeof err.statusCode === "number" ? err.statusCode : 500;
    return res.status(status).json({
      success: false,
      error: (err && err.message) || "Prefill QA failed",
    });
  }
}
