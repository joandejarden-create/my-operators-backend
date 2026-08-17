/**
 * Revert factory preview candidates Brand Status Active/Live → Under Review.
 *
 * Presentation/content untouched. Company Validated / Source / Registry untouched.
 * Restores protected 24-brand Active/Live universe after preview drift.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const WRITER_VERSION = "factory-candidate-status-revert-v1";
export const TARGET_STATUS = "Under Review";
export const BASICS_TABLE = "Brand Setup - Brand Basics";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-factory-candidate-status-revert",
  "--confirm-brand-status-only",
  "--confirm-under-review-target",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-presentation-writes",
]);

export const DEFAULT_SLUGS = Object.freeze([
  "tapestry-collection-by-hilton",
  "dazzler-by-wyndham",
  "trademark-collection-by-wyndham",
]);

const FORBIDDEN = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
export const REPORT_JSON = "brand-explorer-factory-candidate-status-revert.json";
export const REPORT_MD = "brand-explorer-factory-candidate-status-revert.md";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function mockRes() {
  return {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
      return this;
    },
  };
}

async function fetchBrand(id) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: id }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`fetch failed ${id}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

async function patchBasics({ baseId, apiKey, recordId, fields }) {
  for (const k of Object.keys(fields || {})) {
    if (FORBIDDEN.includes(k)) throw new Error(`Forbidden field: ${k}`);
  }
  if (Object.keys(fields).length !== 1 || !Object.prototype.hasOwnProperty.call(fields, "Brand Status")) {
    throw new Error(`Status revert allows only Brand Status; got ${JSON.stringify(fields)}`);
  }
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`PATCH ${recordId}: ${res.status} ${JSON.stringify(json)}`);
  return json;
}

/**
 * @param {{ apply?: boolean, approveFlags?: boolean, slugs?: string[] }} options
 */
export async function runFactoryCandidateStatusRevert(options = {}) {
  const apply = Boolean(options.apply);
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const slugs = options.slugs?.length ? options.slugs : [...DEFAULT_SLUGS];
  const candidates = [];
  const patches = [];

  for (const slug of slugs) {
    const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    if (!identity?.recordId) {
      candidates.push({ slug, error: "missing_identity" });
      continue;
    }
    const brand = await fetchBrand(identity.recordId);
    const current = nz(brand.brandStatus || brand.status);
    const needsRevert = Boolean(current && current !== TARGET_STATUS);
    const row = {
      slug,
      name: brand.name || identity.name,
      recordId: brand.id || identity.recordId,
      currentBrandStatus: current,
      targetBrandStatus: TARGET_STATUS,
      needsRevert,
      alreadyUnderReview: current === TARGET_STATUS,
    };
    candidates.push(row);
    if (row.needsRevert) {
      patches.push({
        table: BASICS_TABLE,
        recordId: row.recordId,
        slug,
        from: current,
        to: TARGET_STATUS,
        fields: { "Brand Status": TARGET_STATUS },
      });
    }
  }

  const applied = [];
  const errors = [];
  if (apply) {
    if (!options.approveFlags) {
      throw new Error(`Apply requires: ${REQUIRED_APPLY_FLAGS.join(" ")}`);
    }
    for (const p of patches) {
      try {
        await patchBasics({
          baseId,
          apiKey,
          recordId: p.recordId,
          fields: p.fields,
        });
        applied.push({ slug: p.slug, recordId: p.recordId, from: p.from, to: p.to });
        await new Promise((r) => setTimeout(r, 250));
      } catch (err) {
        errors.push({ slug: p.slug, error: err.message || String(err) });
      }
    }
  }

  const report = {
    version: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    airtableWrites: apply && applied.length > 0,
    brandStatusWrites: apply && applied.length > 0,
    companyValidatedWrites: false,
    sourceLibraryWrites: false,
    registryWrites: false,
    presentationWrites: false,
    targetStatus: TARGET_STATUS,
    candidates,
    plannedPatches: patches,
    applied,
    errors,
    nextSteps: [
      "npm run test:brand-explorer-24-active-public-full-baseline",
      "Use Factory Preview for visual QA: ?beInternalPreview=1&factoryPreview=1",
      "Finish Tapestry factory → founder approve → Active → baseline 24→25",
    ],
  };
  return report;
}

export function writeStatusRevertReports(report) {
  const dir = path.join(ROOT, "reports");
  fs.mkdirSync(dir, { recursive: true });
  const jsonPath = path.join(dir, REPORT_JSON);
  const mdPath = path.join(dir, REPORT_MD);
  const lines = [
    `# Factory Candidate Status Revert`,
    ``,
    `> ${report.dryRun ? "Dry-run" : "Applied"} · \`${report.generatedAt}\``,
    ``,
    `| Brand | From | To | Action |`,
    `|---|---|---|---|`,
  ];
  for (const c of report.candidates) {
    if (c.error) {
      lines.push(`| \`${c.slug}\` | — | — | ERROR ${c.error} |`);
      continue;
    }
    const action = c.alreadyUnderReview
      ? "noop (already Under Review)"
      : c.needsRevert
        ? report.dryRun
          ? "planned revert"
          : "reverted"
        : "noop";
    lines.push(
      `| ${c.name} (\`${c.slug}\`) | ${c.currentBrandStatus || "—"} | ${c.targetBrandStatus} | ${action} |`
    );
  }
  lines.push(``);
  lines.push(`Applied: ${report.applied?.length || 0} · Errors: ${report.errors?.length || 0}`);
  lines.push(``);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");
  return { jsonPath, mdPath };
}
