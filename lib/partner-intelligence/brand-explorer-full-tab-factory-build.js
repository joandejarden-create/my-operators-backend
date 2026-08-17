/**
 * Full Tab Factory build for true-incomplete Brand Explorer brands.
 * Presentation creates/patches only. No Company Validated, Source Library,
 * Registry, or release-field writes. No active release.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS,
  FULL_BUILD_SLUG_ALIASES,
  FULL_BUILD_IDENTITIES,
  FULL_BUILD_CONTENT_BY_SLUG,
  resolveFullBuildSlug,
  getFullBuildContent,
} from "./brand-explorer-full-build-content.js";
import { BUILT_BLOCKED_PROTECTED_PUBLIC_FULL } from "./brand-explorer-built-blocked-content.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import { TAB_FACTORY_PROTECTED_BRANDS } from "./brand-explorer-tab-contracts.js";
import {
  isFlexibilitySlotKey,
  sanitizeFlexibilityPresentationBody,
} from "../brand-explorer-flexibility-levels.mjs";

export {
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS,
  FULL_BUILD_SLUG_ALIASES,
  FULL_BUILD_IDENTITIES,
  resolveFullBuildSlug,
  getFullBuildContent,
};

/** Cohorts allowed to run full-tab-factory Presentation builds. */
const FULL_BUILD_ALLOWED_SLUGS = Object.freeze([
  ...FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  ...UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS,
]);

export const FULL_BUILD_VERSION = "full-tab-factory-build-v1";
export const FULL_BUILD_REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-full-tab-factory-build",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-release-field-writes",
  "--confirm-tab-factory-contracts",
  "--confirm-source-provenance-by-tab",
  "--confirm-image-uniqueness",
  "--confirm-image-role-match",
  "--confirm-section-pattern-parity",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeSlotKey(slotKey) {
  if (/^insight\.similar\.\d+$/i.test(slotKey)) return "insight.similar";
  return slotKey;
}

function findSlotExact(rows, slotKey, title) {
  const key = normalizeSlotKey(slotKey);
  const list = (rows || []).filter(
    (r) =>
      nz(r.slotKey) === key &&
      r.active !== false &&
      !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );
  if (nz(title)) {
    return list.find((r) => nz(r.title) === nz(title)) || null;
  }
  return list[0] || null;
}

export function parseFullBuildApplyFlags(argv = []) {
  const missing = FULL_BUILD_REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

function resolveBrandIdentity(slug) {
  const identity = FULL_BUILD_IDENTITIES[slug];
  const cfg = getActiveProfileBrandConfig(slug);
  return {
    slug,
    recordId: cfg?.recordId || identity?.recordId || null,
    name: cfg?.name || identity?.name || slug,
    reportSlug: identity?.reportSlug || slug,
    parentCompany: cfg?.parentCompany || identity?.parentCompany || null,
  };
}

function escapeFormulaValue(v) {
  return nz(v).replace(/'/g, "\\'");
}

/** Lightweight Presentation fetch — avoids heavy factory context / complete-build hang. */
async function listPresentationRowsLight(brandRecordId, brandName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey || !brandRecordId) return [];
  const table = PRESENTATION_TABLE;
  // ARRAYJOIN({Brand}) returns primary-field names, not record IDs — match Brand Name.
  const formula = `{Brand Name}='${escapeFormulaValue(brandName)}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(json.error?.message || `Presentation list failed for ${brandName}: ${res.status}`);
    }
    for (const rec of json.records || []) {
      const f = rec.fields || {};
      rows.push({
        recordId: rec.id,
        slotKey: nz(f["Slot Key"]),
        title: nz(f.Title),
        body: nz(f.Body),
        active: f.Active !== false,
        externalDisplayStatus: nz(f["External Display Status"]),
        caseSummaryOverview: nz(f["Case Summary Overview"]),
      });
    }
    offset = json.offset || "";
  } while (offset);
  return rows;
}

export async function planFullTabFactoryBrand(brandSlug) {
  const slug = resolveFullBuildSlug(brandSlug);
  if (!FULL_BUILD_ALLOWED_SLUGS.includes(slug)) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: [`not_in_full_build_allowed_cohort`],
      patches: [],
    };
  }
  if (
    BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(slug) ||
    TAB_FACTORY_PROTECTED_BRANDS.includes(slug)
  ) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: [`protected_brand_refuse`],
      patches: [],
    };
  }

  const pack = FULL_BUILD_CONTENT_BY_SLUG[slug] || getFullBuildContent(slug);
  if (!pack?.presentation?.length) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: [`missing_full_build_content_pack`],
      patches: [],
    };
  }

  const identity = resolveBrandIdentity(slug);
  if (!identity.recordId) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: [`missing_brand_record_id`],
      patches: [],
    };
  }

  let rows = [];
  let ctxError = null;
  try {
    rows = await listPresentationRowsLight(identity.recordId, identity.name);
  } catch (err) {
    // True incomplete may have zero rows or API issues — plan POSTs from identity alone.
    ctxError = err?.message || String(err);
    rows = [];
  }

  const patches = [];
  const blockers = [];
  const usedRecordIds = new Set();

  for (const row of pack.presentation) {
    const slotKey = normalizeSlotKey(row.slotKey);
    let body = nz(row.body);
    const title = nz(row.title);
    if (isFlexibilitySlotKey(slotKey)) {
      body = sanitizeFlexibilityPresentationBody({
        slotKey,
        body,
        brandName: identity.name,
      }).level;
    }
    if (!body) continue;

    const forbidden = scanForbiddenLanguage(`${title}\n${body}`);
    if (forbidden.length) {
      blockers.push(`forbidden:${slotKey}:${forbidden.map((h) => h.id).join(",")}`);
      continue;
    }
    if (/https?:\/\//i.test(body) || /https?:\/\//i.test(title)) {
      blockers.push(`raw_url:${slotKey}`);
      continue;
    }

    let existing = findSlotExact(rows, slotKey, title);
    if (existing?.recordId && usedRecordIds.has(existing.recordId)) {
      existing = null;
    }
    if (!existing && !title) {
      // Prefer empty/missing body row for untitled slots
      const candidates = (rows || []).filter(
        (r) =>
          nz(r.slotKey) === slotKey &&
          r.active !== false &&
          !usedRecordIds.has(r.recordId) &&
          !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
      );
      existing = candidates.find((r) => !nz(r.body)) || candidates[0] || null;
    }

    const caseFields = {};
    for (const [api, airtable] of [
      ["caseSummaryOverview", "Case Summary Overview"],
      ["caseSummaryBrandRelevance", "Case Summary Brand Relevance"],
      ["caseSummaryOwnerObjective", "Case Summary Owner Objective"],
      ["caseSummaryInterpretation", "Case Summary Interpretation"],
      ["caseSummaryTags", "Case Summary Tags"],
    ]) {
      if (nz(row[api])) caseFields[airtable] = nz(row[api]);
    }

    if (
      existing?.recordId &&
      nz(existing.body) === body &&
      nz(existing.title) === title &&
      (!caseFields["Case Summary Overview"] ||
        nz(existing.caseSummaryOverview) === caseFields["Case Summary Overview"])
    ) {
      usedRecordIds.add(existing.recordId);
      continue;
    }

    if (existing?.recordId) {
      usedRecordIds.add(existing.recordId);
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: existing.recordId,
        slotKey,
        fields: {
          Body: body,
          ...(title ? { Title: title } : {}),
          ...caseFields,
        },
        before: { body: existing.body, title: existing.title },
        after: { body, title },
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey,
        fields: {
          "Slot Key": slotKey,
          "Brand Name": identity.name,
          Brand: [identity.recordId],
          Active: true,
          "Sort Order": row.sortOrder ?? 0,
          Title: title || "",
          Body: body,
          ...caseFields,
        },
        before: null,
        after: { body, title },
      });
    }
  }

  return {
    brandSlug: slug,
    reportSlug: identity.reportSlug,
    brandName: identity.name,
    recordId: identity.recordId,
    parentCompany: identity.parentCompany,
    sourcePack: pack.sourcePack || null,
    brandLens: pack.brandLens || null,
    presentationRowCount: pack.presentation.length,
    existingPresentationCount: rows.length,
    ctxError,
    patches,
    blockers,
    blocked: blockers.length > 0,
    releaseFieldsWritten: false,
    activeRelease: false,
    companyValidatedUntouched: true,
  };
}

export async function planFullTabFactoryBuild({
  brands = [...FULL_BUILD_TRUE_INCOMPLETE_SLUGS],
  reportsDir = path.join(ROOT, "reports"),
} = {}) {
  const brandResults = [];
  for (const raw of brands) {
    brandResults.push(await planFullTabFactoryBrand(raw));
  }
  const plannedWriteCount = brandResults.reduce((n, b) => n + (b.patches?.length || 0), 0);
  const blockedSlugs = brandResults.filter((b) => b.blocked).map((b) => b.brandSlug);
  return {
    version: FULL_BUILD_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands: brands.map(resolveFullBuildSlug),
    brandResults,
    summary: {
      brandCount: brandResults.length,
      plannedWriteCount,
      blockedSlugs,
      postCount: brandResults.reduce(
        (n, b) => n + (b.patches || []).filter((p) => p.action === "POST").length,
        0
      ),
      patchCount: brandResults.reduce(
        (n, b) => n + (b.patches || []).filter((p) => p.action === "PATCH").length,
        0
      ),
    },
    requiredApplyFlags: [...FULL_BUILD_REQUIRED_APPLY_FLAGS],
    reportsDir,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function airtableWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const maxAttempts = 8;
  let lastErr = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const res = await fetch(url, {
      method,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields }),
    });
    const json = await res.json().catch(() => ({}));
    if (res.ok) return json;
    const msg = json.error?.message || `${method} ${table} failed: ${res.status}`;
    lastErr = new Error(msg);
    const retryable = res.status === 429 || res.status >= 500;
    if (!retryable || attempt === maxAttempts) break;
    const backoffMs = Math.min(30_000, 800 * 2 ** (attempt - 1));
    await sleep(backoffMs);
  }
  throw lastErr || new Error(`${method} ${table} failed`);
}

export async function applyFullTabFactoryBuild({
  plan,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseFullBuildApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing, flagCheck };
  }
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of plan.brandResults || []) {
    if (brand.blocked) {
      resultsByBrand[brand.brandSlug] = {
        applied: false,
        reason: "blocked",
        blockers: brand.blockers,
      };
      continue;
    }
    const created = [];
    const updated = [];
    const errors = [];
    for (const patch of brand.patches || []) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_WRITE_FIELDS.has(key)) {
          throw new Error(`Forbidden field write: ${key}`);
        }
      }
      try {
        if (patch.action === "POST") {
          const json = await airtableWrite({
            baseId,
            apiKey,
            table: PRESENTATION_TABLE,
            fields: patch.fields,
            method: "POST",
          });
          created.push(json.id);
        } else {
          await airtableWrite({
            baseId,
            apiKey,
            table: PRESENTATION_TABLE,
            recordId: patch.recordId,
            fields: patch.fields,
            method: "PATCH",
          });
          updated.push(patch.recordId);
        }
        // Pace writes to reduce Airtable 429s during large factory POSTs.
        await sleep(220);
      } catch (err) {
        errors.push({ slotKey: patch.slotKey, error: err?.message || String(err) });
      }
    }
    resultsByBrand[brand.brandSlug] = {
      applied: errors.length === 0,
      created,
      updated,
      errors,
      releaseFieldsWritten: false,
      companyValidatedUntouched: true,
    };
  }

  return {
    applied: Object.values(resultsByBrand).some((r) => r.applied),
    reason: "full_tab_factory_build_applied",
    flagCheck,
    resultsByBrand,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    releaseFieldsWritten: false,
    activeRelease: false,
  };
}

export function writeFullBuildBrandReport(brand, { reportsDir = path.join(ROOT, "reports") } = {}) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const slug = brand.reportSlug || brand.brandSlug;
  const mdPath = path.join(reportsDir, `brand-explorer-full-build-${slug}.md`);
  const jsonPath = path.join(reportsDir, `brand-explorer-full-build-${slug}.json`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(brand, null, 2)}\n`, "utf8");
  const lines = [
    `# Full Tab Factory Build — ${brand.brandName || brand.brandSlug}`,
    ``,
    `- Slug: \`${brand.brandSlug}\``,
    `- Record: \`${brand.recordId || "—"}\``,
    `- Parent: ${brand.parentCompany || "—"}`,
    `- Presentation pack rows: ${brand.presentationRowCount ?? 0}`,
    `- Existing Presentation rows: ${brand.existingPresentationCount ?? 0}`,
    `- Planned writes: ${brand.patches?.length ?? 0}`,
    `- Blocked: **${brand.blocked === true}**`,
    `- Active release: **false**`,
    `- Company Validated untouched: **true**`,
    ``,
    `## Brand lens`,
    ``,
    "```json",
    JSON.stringify(brand.brandLens || {}, null, 2),
    "```",
    ``,
    `## Source pack`,
    ``,
    "```json",
    JSON.stringify(brand.sourcePack || {}, null, 2),
    "```",
    ``,
    `## Blockers`,
    ``,
    ...(brand.blockers?.length ? brand.blockers.map((b) => `- ${b}`) : ["- (none)"]),
    ``,
    `## Planned writes (sample)`,
    ``,
    ...((brand.patches || []).slice(0, 25).map(
      (p) => `- \`${p.action}\` \`${p.slotKey}\`${p.recordId ? ` (${p.recordId})` : ""}`
    ) || ["- (none)"]),
    (brand.patches || []).length > 25
      ? `\n_…and ${(brand.patches || []).length - 25} more_\n`
      : "",
    ``,
    `## Gates still required after apply`,
    ``,
    `- rendered field completeness`,
    `- no empty rendered components`,
    `- source provenance by tab`,
    `- image uniqueness (6 gallery + 3 property examples + 3 scenario images)`,
    `- image role-match`,
    `- section pattern parity`,
    `- golden content quality`,
    `- founder visual review`,
    `- explicit public restore / active release (separate command)`,
    ``,
  ];
  fs.writeFileSync(mdPath, `${lines.filter(Boolean).join("\n")}\n`, "utf8");
  return { mdPath, jsonPath };
}
