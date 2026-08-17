/**
 * Branded Residences capability — Brand Setup field registry, schema setup, API shape, extraction rules.
 *
 * @see docs/data-intelligence/brand-residences-status-field.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import {
  MAP_BRAND_RESIDENCES,
  RESIDENCES_STATUS_VALUES,
  RESIDENCES_REVIEW_STATUS_VALUES,
  DEFAULT_RESIDENCES_STATUS,
  DEFAULT_RESIDENCES_REVIEW_STATUS,
  normalizeResidencesStatus,
  normalizeResidencesReviewStatus,
  buildResidencesApiShape,
} from "../brand-explorer/brand-residences-api-shape.js";

export {
  MAP_BRAND_RESIDENCES,
  RESIDENCES_STATUS_VALUES,
  RESIDENCES_REVIEW_STATUS_VALUES,
  DEFAULT_RESIDENCES_STATUS,
  DEFAULT_RESIDENCES_REVIEW_STATUS,
  normalizeResidencesStatus,
  normalizeResidencesReviewStatus,
  buildResidencesApiShape,
};

export const SETUP_VERSION = "1";
export const REPORT_JSON_NAME = "brand-residences-status-audit.json";
export const REPORT_MD_NAME = "brand-residences-status-audit.md";
export const DOC_MD_NAME = "brand-residences-status-field.md";

export const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";

export const RESIDENCES_NOTES_HELPER =
  "Confirm market, license structure, and brand approval requirements before underwriting.";

export const BRAND_RESIDENCES_REGISTRY_FIELD_KEY = "be.development.brandedResidencesStatus";

export const BRAND_RESIDENCES_FIELD_DEFS = [
  {
    name: MAP_BRAND_RESIDENCES.status,
    type: "singleSelect",
    description:
      "Whether the brand accepts or supports branded residences. Default Not Confirmed unless source-backed or founder-reviewed.",
    options: {
      choices: RESIDENCES_STATUS_VALUES.map((name) => ({ name })),
    },
  },
  {
    name: MAP_BRAND_RESIDENCES.notes,
    type: "multilineText",
    description: "Short owner-facing note on branded residences basis, limits, or approval conditions.",
  },
  {
    name: MAP_BRAND_RESIDENCES.sourceUrl,
    type: "url",
    description: "Source URL supporting the branded residences status.",
  },
  {
    name: MAP_BRAND_RESIDENCES.reviewStatus,
    type: "singleSelect",
    description: "Governance state for the branded residences status value.",
    options: {
      choices: RESIDENCES_REVIEW_STATUS_VALUES.map((name) => ({ name })),
    },
  },
];

const YES_RE =
  /\b(branded residences?|residential component|condo hotel|private residences?)\b.{0,80}\b(supported|available|offered|accepted|permitted|allowed)\b|\b(supports?|offers?|accepts?|permits?)\b.{0,80}\b(branded residences?|residential component|condo hotel)\b/i;
const CASE_RE =
  /\b(case[- ]by[- ]case|selected markets?|subject to approval|special licensing|market[- ]specific|approval required|select projects?)\b/i;
const NO_RE =
  /\b(does not|do not|not offer|not support|excludes?|prohibits?|no branded residences?)\b/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

/**
 * Infer status from approved/source text. Never infer No from silence.
 */
export function inferResidencesStatusFromEvidence(text) {
  const hay = nz(text);
  if (!hay) return DEFAULT_RESIDENCES_STATUS;
  if (NO_RE.test(hay) && !YES_RE.test(hay)) return "No";
  if (CASE_RE.test(hay)) return "Case-by-Case";
  if (YES_RE.test(hay)) return "Yes";
  return DEFAULT_RESIDENCES_STATUS;
}

export function validateResidencesRecord(fields = {}) {
  const issues = [];
  const status = normalizeResidencesStatus(fields[MAP_BRAND_RESIDENCES.status]);
  const reviewStatus = normalizeResidencesReviewStatus(fields[MAP_BRAND_RESIDENCES.reviewStatus]);
  const notes = nz(fields[MAP_BRAND_RESIDENCES.notes]);
  const sourceUrl = nz(fields[MAP_BRAND_RESIDENCES.sourceUrl]);
  const rawStatus = nz(fields[MAP_BRAND_RESIDENCES.status]);
  const rawReview = nz(fields[MAP_BRAND_RESIDENCES.reviewStatus]);

  if (!rawStatus) {
    issues.push({ code: "missing_status", severity: "medium", message: "Branded Residences Status is empty" });
  }
  if (rawStatus && !RESIDENCES_STATUS_VALUES.includes(status)) {
    issues.push({ code: "invalid_status", severity: "high", message: `Invalid status value: ${rawStatus}` });
  }
  if (rawReview && !RESIDENCES_REVIEW_STATUS_VALUES.includes(reviewStatus)) {
    issues.push({ code: "invalid_review_status", severity: "high", message: `Invalid review status: ${rawReview}` });
  }
  if ((status === "Yes" || status === "No") && !["Source-Backed", "Founder-Reviewed"].includes(reviewStatus)) {
    issues.push({
      code: "unsupported_yes_no_without_review",
      severity: "high",
      message: `${status} requires Source-Backed or Founder-Reviewed review status`,
    });
  }
  if (status === "Yes" && !sourceUrl && !notes) {
    issues.push({
      code: "yes_without_source_or_notes",
      severity: "high",
      message: "Yes status without source URL or notes",
    });
  }
  if (status === "No" && reviewStatus === "Not Confirmed") {
    issues.push({
      code: "no_inferred_without_review",
      severity: "high",
      message: "No status without source/founder review support",
    });
  }
  if (notes && sourceUrl && /contradict|does not support|not offered/i.test(notes) && status === "Yes") {
    issues.push({ code: "notes_source_mismatch", severity: "medium", message: "Notes contradict Yes status" });
  }
  return { status, reviewStatus, notes, sourceUrl, issues };
}

/**
 * Populate Brand Setup basics fields from an approved branded-residences fact.
 * Returns empty object when inference is Not Confirmed.
 */
export function applyBrandedResidencesFromFacts(factByKey) {
  const fact = factByKey.get(BRAND_RESIDENCES_REGISTRY_FIELD_KEY);
  if (!fact || fact.dataGap === "Yes") return {};
  const text = nz(fact.extractedValue) || nz(fact.evidenceText);
  const status = inferResidencesStatusFromEvidence(text);
  if (status === DEFAULT_RESIDENCES_STATUS) return {};

  const human = nz(fact.humanReviewStatus);
  const reviewStatus =
    human === "Approved" || human === "Edited"
      ? "Source-Backed"
      : human === "Rejected"
        ? "Needs Review"
        : "Needs Review";

  const patch = {
    [MAP_BRAND_RESIDENCES.status]: status,
    [MAP_BRAND_RESIDENCES.reviewStatus]: reviewStatus,
  };
  const evidence = nz(fact.evidenceText);
  if (evidence) patch[MAP_BRAND_RESIDENCES.notes] = evidence.slice(0, 900);
  const sourceUrl = nz(fact.sourceUrl);
  if (sourceUrl) patch[MAP_BRAND_RESIDENCES.sourceUrl] = sourceUrl;
  return patch;
}

export async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

export async function runBrandResidencesStatusFieldSetup({ dryRun = true } = {}) {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required");

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed ${listRes.status}: ${JSON.stringify(listJson)}`);

  const table = (listJson.tables || []).find((t) => t.name === BRAND_BASICS_TABLE);
  if (!table) throw new Error(`Table not found: ${BRAND_BASICS_TABLE}`);

  const existingNames = new Set((table.fields || []).map((f) => f.name));
  const present = [];
  const wouldCreate = [];
  const created = [];
  const failed = [];

  for (const fieldDef of BRAND_RESIDENCES_FIELD_DEFS) {
    if (existingNames.has(fieldDef.name)) {
      present.push(fieldDef.name);
      continue;
    }
    if (dryRun) {
      wouldCreate.push({ name: fieldDef.name, type: fieldDef.type });
      continue;
    }
    const payload = {
      name: fieldDef.name,
      type: fieldDef.type,
      ...(fieldDef.description ? { description: fieldDef.description } : {}),
      ...(fieldDef.options ? { options: fieldDef.options } : {}),
    };
    const { res, json } = await metaFetch(baseId, token, `/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      failed.push({ name: fieldDef.name, error: json?.error?.message || JSON.stringify(json) });
    } else {
      created.push({ name: fieldDef.name, id: json.id });
    }
  }

  return {
    setupVersion: SETUP_VERSION,
    generatedAt: new Date().toISOString(),
    mode: dryRun ? "dry-run" : "apply",
    airtableModified: !dryRun && created.length > 0,
    table: BRAND_BASICS_TABLE,
    fieldsPresent: present,
    fieldsWouldCreate: wouldCreate,
    fieldsCreated: created,
    fieldsFailed: failed,
    companyValidatedUntouched: true,
    exactApplyCommand: "npm run setup-brand-residences-status-fields -- --apply",
  };
}

export function resolveBrandTarget(brandArg) {
  const normalized = nz(brandArg).toLowerCase();
  const bySlug = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === normalized);
  if (bySlug) return bySlug;
  const byId = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.recordId === brandArg);
  if (byId) return byId;
  return { slug: normalized || "unknown", recordId: nz(brandArg), name: nz(brandArg) };
}

export async function buildBrandResidencesStatusAuditReport(options = {}) {
  const allActive = Boolean(options.allActive);
  const brandArg = nz(options.brandIdOrName || "tribute-portfolio");
  const targets = allActive ? ACTIVE_BRAND_AUDIT_TARGETS : [resolveBrandTarget(brandArg)];

  const atelierSrc = await import("fs").then((fs) => {
    try {
      return fs.readFileSync(
        new URL("../../public/js/brand-explorer-atelier-from-api.js", import.meta.url),
        "utf8"
      );
    } catch {
      return "";
    }
  });
  const frontendRenders =
    /brandedResidencesLine|Branded Residences/.test(atelierSrc) &&
    /brand\.residences/.test(atelierSrc);

  const brandAudits = [];
  for (const target of targets) {
    const basics = await fetchBrandBasics(target.recordId);
    const fields = basics?.fields || {};
    const validation = validateResidencesRecord(fields);
    const apiShape = buildResidencesApiShape(fields);
    brandAudits.push({
      brand: { slug: target.slug, name: basics?.name || target.name, recordId: target.recordId },
      airtable: {
        status: nz(fields[MAP_BRAND_RESIDENCES.status]) || null,
        notes: nz(fields[MAP_BRAND_RESIDENCES.notes]) || null,
        sourceUrl: nz(fields[MAP_BRAND_RESIDENCES.sourceUrl]) || null,
        reviewStatus: nz(fields[MAP_BRAND_RESIDENCES.reviewStatus]) || null,
      },
      apiShape,
      issues: validation.issues,
      companyValidated: nz(fields["Company Validated"]) || null,
      companyValidationDate: nz(fields["Company Validation Date"]) || null,
    });
  }

  const tribute = brandAudits.find((b) => b.brand.recordId === "recCvV0PuZOi8c3hC") || brandAudits[0];

  return {
    auditVersion: SETUP_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    allActive,
    filesRead: [
      "AGENTS.md",
      "docs/brand-explorer-presentation-slots.md",
      "docs/data-intelligence/BRAND_PROFILE_DATA_MODEL.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "api/lib/partner-intelligence-explorer-field-registry.js",
      "lib/partner-intelligence/brand-explorer-registry-catalog.js",
      "lib/partner-intelligence/apply-brand-extraction-to-airtable.js",
      "live Brand Setup - Brand Basics records",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-residences-status-setup.js",
      "scripts/setup-brand-residences-status-fields.mjs",
      "lib/partner-intelligence/brand-residences-status-audit.js",
      "scripts/brand-residences-status-audit.mjs",
      "api/brand-library.js",
      "lib/partner-intelligence/brand-explorer-registry-catalog.js",
      "lib/partner-intelligence/apply-brand-extraction-to-airtable.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "docs/data-intelligence/brand-residences-status-field.md",
      "package.json",
    ],
    fieldsExpected: BRAND_RESIDENCES_FIELD_DEFS.map((f) => f.name),
    apiShapeContract: {
      path: "brand.residences",
      shape: {
        status: RESIDENCES_STATUS_VALUES.join(" | "),
        notes: "string | null",
        sourceUrl: "string | null",
        reviewStatus: RESIDENCES_REVIEW_STATUS_VALUES.join(" | "),
      },
    },
    frontendDisplayLocation: "Overview snapshot → Development & positioning KV block",
    frontendRendersIndicator: frontendRenders,
    populationRules: [
      "Explicit source support → Yes",
      "Subject to approval / selected markets / special licensing → Case-by-Case",
      "Explicit exclusion → No",
      "No source / silence → Not Confirmed (never infer No)",
      "Approved fact write sets Review Status to Source-Backed",
    ],
    tributeCurrentStatus: tribute?.apiShape || null,
    activeBrandAudits: brandAudits,
    defectCounts: {
      total: brandAudits.reduce((a, b) => a + b.issues.length, 0),
      brandsWithIssues: brandAudits.filter((b) => b.issues.length > 0).length,
    },
    companyValidatedUntouched: true,
    exactApplyCommand: "npm run setup-brand-residences-status-fields -- --apply",
  };
}

export function buildBrandResidencesStatusAuditMarkdown(report) {
  const lines = [];
  lines.push("# Brand Residences Status Audit");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Frontend renders indicator: **${report.frontendRendersIndicator ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## API shape");
  lines.push("```json");
  lines.push(JSON.stringify(report.apiShapeContract, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Tribute current status");
  lines.push("```json");
  lines.push(JSON.stringify(report.tributeCurrentStatus, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Active brand audits");
  for (const b of report.activeBrandAudits || []) {
    lines.push(`### ${b.brand.name}`);
    lines.push(`- Status: **${b.apiShape.status}** · Review: **${b.apiShape.reviewStatus}**`);
    if (!b.issues.length) lines.push("- Issues: none");
    for (const issue of b.issues) lines.push(`- [${issue.severity}] ${issue.message}`);
    lines.push("");
  }
  lines.push("## Apply command");
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  return lines.join("\n");
}
