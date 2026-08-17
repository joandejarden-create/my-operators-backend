/**
 * Wave 12 Stage 4 — Tab Factory content build (plan + apply).
 *
 * Allowed writes: target-brand Presentation rows + limited Brand Basics
 * visible positioning fields (Brand Positioning, Guest Psychographics,
 * Target Guest Segments when validated).
 *
 * Forbidden: Brand Status, release fields, CV, Source Library, Registry,
 * protected 27, Radisson Collection, images, non-target brands.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import {
  isFlexibilitySlotKey,
  sanitizeFlexibilityPresentationBody,
} from "../brand-explorer-flexibility-levels.mjs";
import { TAB_FACTORY_PROTECTED_BRANDS } from "./brand-explorer-tab-contracts.js";
import { BUILT_BLOCKED_PROTECTED_PUBLIC_FULL } from "./brand-explorer-built-blocked-content.js";
import {
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
} from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE12_VERSION,
  WAVE12_SLUGS,
  WAVE12_FORBIDDEN_WRITE_FIELDS,
} from "./brand-explorer-wave12-factory-plan.js";
import {
  generateWave12TabFactoryPack,
  WAVE12_TAB_FACTORY_GENERATOR_VERSION,
  WAVE12_GENERIC_AUDIENCE_PROSE_RE,
  assessWave12TgsRisk,
} from "./brand-explorer-wave12-tab-factory-build-generator.js";
import { EXPECTED_ACTIVE_COUNT_27 } from "./brand-explorer-27-active-public-full-baseline.js";

export const WAVE12_TAB_FACTORY_BUILD_VERSION = "wave12-tab-factory-build-v1";

export const WAVE12_TAB_FACTORY_BUILD_APPLY_FLAGS = Object.freeze([
  "--approve-wave12-tab-factory-build",
  "--confirm-target-brands-only",
  "--confirm-source-pack-grounded",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-protected-27-brand-changes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-image-writes",
  "--confirm-no-broad-rewrites",
  "--confirm-target-guest-segments-validated",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS_TABLE = "Brand Setup - Brand Basics";

const URL_ALLOWED_SLOTS = new Set(["footprint.momentum", "footprint.openings"]);

const FORBIDDEN_WRITE_FIELDS = new Set([
  ...WAVE12_FORBIDDEN_WRITE_FIELDS,
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Brand Status",
  "Image",
  "Images",
  "Gallery Image",
]);

const ALLOWED_BASICS_FIELDS = new Set([
  "Brand Positioning",
  "Guest Psychographics Description",
  "Target Guest Segments",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeSlotKey(slotKey) {
  if (/^insight\.similar\.\d+$/i.test(slotKey)) return "insight.similar";
  return slotKey;
}

function escapeFormulaValue(v) {
  return nz(v).replace(/'/g, "\\'");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function parseWave12TabFactoryBuildFlags(argv = []) {
  const missing = WAVE12_TAB_FACTORY_BUILD_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
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

async function listPresentationRowsLight(brandName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey || !brandName) return [];
  const formula = `{Brand Name}='${escapeFormulaValue(brandName)}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
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

async function fetchBasicsRecord(recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey || !recordId) return null;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Basics get failed ${res.status}`);
  return json;
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
    await sleep(Math.min(30_000, 800 * 2 ** (attempt - 1)));
  }
  throw lastErr || new Error(`${method} ${table} failed`);
}

function resolveWave12Identity(slug) {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!id?.recordId || !id?.name) {
    throw new Error(`Missing factory-preview identity for ${slug}`);
  }
  return {
    slug,
    recordId: id.recordId,
    name: id.name,
  };
}

export async function planWave12TabFactoryBrand(slug) {
  const identity = resolveWave12Identity(slug);
  if (
    BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(slug) ||
    TAB_FACTORY_PROTECTED_BRANDS.includes(slug)
  ) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["protected_brand_refuse"],
      patches: [],
      basicsPatches: [],
    };
  }
  if (!WAVE12_SLUGS.includes(slug)) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["not_wave12_target"],
      patches: [],
      basicsPatches: [],
    };
  }

  const pack = generateWave12TabFactoryPack(slug, {
    airtableName: identity.name,
    recordId: identity.recordId,
  });

  let rows = [];
  let ctxError = null;
  try {
    rows = await listPresentationRowsLight(identity.name);
  } catch (err) {
    ctxError = err?.message || String(err);
    rows = [];
  }

  let basicsBefore = {};
  try {
    const basics = await fetchBasicsRecord(identity.recordId);
    basicsBefore = basics?.fields || {};
  } catch (err) {
    ctxError = ctxError || err?.message || String(err);
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
    if (!body) {
      blockers.push(`empty_body:${slotKey}`);
      continue;
    }

    const forbidden = scanForbiddenLanguage(`${title}\n${body}`).filter((h) => {
      if (URL_ALLOWED_SLOTS.has(slotKey) && h.id === "raw_url") return false;
      return true;
    });
    if (forbidden.length) {
      blockers.push(`forbidden:${slotKey}:${forbidden.map((h) => h.id).join(",")}`);
      continue;
    }
    if (
      !URL_ALLOWED_SLOTS.has(slotKey) &&
      (/https?:\/\//i.test(body) || /https?:\/\//i.test(title))
    ) {
      blockers.push(`raw_url:${slotKey}`);
      continue;
    }

    let existing = findSlotExact(rows, slotKey, title);
    if (existing?.recordId && usedRecordIds.has(existing.recordId)) {
      existing = null;
    }
    if (!existing && !title) {
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

  const basicsPatches = [];
  const basicsFields = pack.basicsFields || {};
  const nextBasics = {};
  for (const [field, value] of Object.entries(basicsFields)) {
    if (!ALLOWED_BASICS_FIELDS.has(field)) {
      blockers.push(`basics_field_not_allowed:${field}`);
      continue;
    }
    if (field === "Target Guest Segments") {
      if (!pack.tgsWriteEligible) {
        blockers.push("tgs_generic_audience_prose_risk");
        continue;
      }
      const before = Array.isArray(basicsBefore["Target Guest Segments"])
        ? basicsBefore["Target Guest Segments"]
        : [];
      const after = [...value];
      const same =
        before.length === after.length && before.every((v, i) => v === after[i]);
      if (same) continue;
      nextBasics[field] = after;
    } else {
      const before = nz(basicsBefore[field]);
      const after = nz(value);
      if (before === after) continue;
      nextBasics[field] = after;
    }
  }
  if (Object.keys(nextBasics).length) {
    basicsPatches.push({
      table: BASICS_TABLE,
      action: "PATCH",
      recordId: identity.recordId,
      fields: nextBasics,
      before: Object.fromEntries(
        Object.keys(nextBasics).map((k) => [k, basicsBefore[k] ?? null])
      ),
      after: nextBasics,
      fieldMapping: Object.fromEntries(
        Object.keys(nextBasics).map((k) => [k, `Brand Basics.${k}`])
      ),
      validation: {
        pass: true,
        checks: [
          "target_brand_only",
          "allowed_basics_fields_only",
          "no_company_validation",
          "no_source_library",
          "no_registry",
          "no_brand_status",
          "no_release",
          "no_images",
        ],
        failedChecks: [],
      },
    });
  }

  return {
    brandSlug: slug,
    reportSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    parentCompany: pack.identity.parentCompany,
    sourcePack: pack.sourcePackMeta,
    brandLens: pack.brandLens,
    presentationRowCount: pack.presentation.length,
    existingPresentationCount: rows.length,
    targetGuestSegments: pack.targetGuestSegments,
    tgsWriteEligible: pack.tgsWriteEligible,
    tgsAssessment: pack.tgsAssessment,
    ctxError,
    patches,
    basicsPatches,
    blockers,
    blocked: blockers.length > 0,
    releaseFieldsWritten: false,
    brandStatusUntouched: true,
    companyValidatedUntouched: true,
    imagesWritten: false,
  };
}

export async function planWave12TabFactoryBuild({
  brands = [...WAVE12_SLUGS],
  reportsDir = REPORTS_DIR,
} = {}) {
  const brandResults = [];
  for (const slug of brands) {
    brandResults.push(await planWave12TabFactoryBrand(slug));
  }

  const tgsReport = brandResults.map((b) => ({
    slug: b.brandSlug,
    name: b.brandName,
    recommended: b.targetGuestSegments || [],
    writeEligible: b.tgsWriteEligible === true,
    risk: b.tgsAssessment || null,
    willWrite: (b.basicsPatches || []).some((p) =>
      Object.prototype.hasOwnProperty.call(p.fields || {}, "Target Guest Segments")
    ),
  }));

  const anyTgsRisk = tgsReport.some((t) => t.risk?.risk);
  const plannedPresentationWrites = brandResults.reduce(
    (n, b) => n + (b.patches?.length || 0),
    0
  );
  const plannedBasicsWrites = brandResults.reduce(
    (n, b) => n + (b.basicsPatches?.length || 0),
    0
  );

  return {
    version: WAVE12_TAB_FACTORY_BUILD_VERSION,
    factoryVersion: WAVE12_VERSION,
    generatorVersion: WAVE12_TAB_FACTORY_GENERATOR_VERSION,
    stage: "tab-factory-build",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    airtableWrites: false,
    protectedBaselineCount: EXPECTED_ACTIVE_COUNT_27,
    brands: brands.map((s) => String(s)),
    brandResults,
    targetGuestSegments: tgsReport,
    tgsValidated: !anyTgsRisk && tgsReport.every((t) => t.writeEligible),
    summary: {
      brandCount: brandResults.length,
      contentPacks: brandResults.filter((b) => (b.presentationRowCount || 0) > 0).length,
      plannedPresentationWrites,
      plannedBasicsWrites,
      postCount: brandResults.reduce(
        (n, b) => n + (b.patches || []).filter((p) => p.action === "POST").length,
        0
      ),
      patchCount: brandResults.reduce(
        (n, b) => n + (b.patches || []).filter((p) => p.action === "PATCH").length,
        0
      ),
      blockedSlugs: brandResults.filter((b) => b.blocked).map((b) => b.brandSlug),
      tgsWriteCount: tgsReport.filter((t) => t.willWrite).length,
    },
    requiredApplyFlags: [...WAVE12_TAB_FACTORY_BUILD_APPLY_FLAGS],
    reportsDir,
  };
}

export async function applyWave12TabFactoryBuild({ plan, apply = false, argv = [] } = {}) {
  const flagCheck = parseWave12TabFactoryBuildFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return {
      applied: false,
      reason: "missing_apply_flags",
      missing: flagCheck.missing,
      flagCheck,
    };
  }
  if (plan.tgsValidated === false) {
    return {
      applied: false,
      reason: "target_guest_segments_not_validated",
      flagCheck,
    };
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
    if (!WAVE12_SLUGS.includes(brand.brandSlug)) {
      throw new Error(`Refusing non-target brand write: ${brand.brandSlug}`);
    }

    const created = [];
    const updated = [];
    const basicsUpdated = [];
    const errors = [];

    for (const patch of [...(brand.basicsPatches || []), ...(brand.patches || [])]) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_WRITE_FIELDS.has(key)) {
          throw new Error(`Forbidden field write: ${key}`);
        }
        if (patch.table === BASICS_TABLE && !ALLOWED_BASICS_FIELDS.has(key)) {
          throw new Error(`Forbidden Brand Basics field: ${key}`);
        }
      }
      try {
        if (patch.action === "POST") {
          const json = await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            fields: patch.fields,
            method: "POST",
          });
          created.push(json.id);
        } else {
          await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            recordId: patch.recordId,
            fields: patch.fields,
            method: "PATCH",
          });
          if (patch.table === BASICS_TABLE) basicsUpdated.push(patch.recordId);
          else updated.push(patch.recordId);
        }
        await sleep(220);
      } catch (err) {
        errors.push({
          table: patch.table,
          slotKey: patch.slotKey || Object.keys(patch.fields || {})[0],
          error: err?.message || String(err),
        });
      }
    }

    resultsByBrand[brand.brandSlug] = {
      applied: errors.length === 0,
      created,
      updated,
      basicsUpdated,
      errors,
      releaseFieldsWritten: false,
      brandStatusUntouched: true,
      companyValidatedUntouched: true,
      imagesWritten: false,
    };
  }

  return {
    applied: Object.values(resultsByBrand).some((r) => r.applied),
    reason: "wave12_tab_factory_build_applied",
    flagCheck,
    resultsByBrand,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    brandStatusUntouched: true,
    releaseFieldsWritten: false,
    imagesWritten: false,
    protected27Untouched: true,
  };
}

function writeBrandMd(brand, reportsDir) {
  const mdPath = path.join(reportsDir, `brand-explorer-wave12-tab-factory-build-${brand.reportSlug || brand.brandSlug}.md`);
  const lines = [
    `# Wave 12 Tab Factory Build — ${brand.brandName || brand.brandSlug}`,
    ``,
    `- Slug: \`${brand.brandSlug}\``,
    `- Record: \`${brand.recordId || "—"}\``,
    `- Parent: ${brand.parentCompany || "—"}`,
    `- Presentation pack rows: ${brand.presentationRowCount ?? 0}`,
    `- Existing Presentation rows: ${brand.existingPresentationCount ?? 0}`,
    `- Planned Presentation writes: ${brand.patches?.length ?? 0}`,
    `- Planned Brand Basics writes: ${brand.basicsPatches?.length ?? 0}`,
    `- Target Guest Segments: ${(brand.targetGuestSegments || []).join(", ") || "—"}`,
    `- TGS write eligible: **${brand.tgsWriteEligible === true}**`,
    `- Blocked: **${brand.blocked === true}**`,
    `- Brand Status untouched: **true**`,
    `- Release fields written: **false**`,
    `- Images written: **false**`,
    `- Company Validated untouched: **true**`,
    ``,
    `## Brand lens`,
    ``,
    "```json",
    JSON.stringify(brand.brandLens || {}, null, 2),
    "```",
    ``,
    `## Source pack meta`,
    ``,
    "```json",
    JSON.stringify(brand.sourcePack || {}, null, 2),
    "```",
    ``,
    `## Blockers`,
    ``,
    ...(brand.blockers?.length ? brand.blockers.map((b) => `- ${b}`) : ["- (none)"]),
    ``,
    `## Planned Presentation writes (sample)`,
    ``,
    ...((brand.patches || []).slice(0, 30).map(
      (p) => `- \`${p.action}\` \`${p.slotKey}\`${p.recordId ? ` (${p.recordId})` : ""}`
    ) || ["- (none)"]),
    (brand.patches || []).length > 30
      ? `\n_…and ${(brand.patches || []).length - 30} more_\n`
      : "",
    ``,
    `## Planned Brand Basics writes`,
    ``,
    ...((brand.basicsPatches || []).map(
      (p) =>
        `- \`${p.action}\` fields: ${Object.keys(p.fields || {}).join(", ")}`
    ) || ["- (none)"]),
    ``,
  ];
  fs.writeFileSync(mdPath, `${lines.filter((l) => l != null).join("\n")}\n`, "utf8");
  return mdPath;
}

export function writeWave12TabFactoryBuildReports(plan, applyResult = null) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const packPaths = [];
  for (const brand of plan.brandResults || []) {
    packPaths.push(writeBrandMd(brand, REPORTS_DIR));
  }

  const tgsMdPath = path.join(REPORTS_DIR, "brand-explorer-wave12-target-guest-segments.md");
  const tgsLines = [
    `# Wave 12 — Target Guest Segments`,
    ``,
    `Generated: ${plan.generatedAt}`,
    `Validated (no generic_audience_prose risk): **${plan.tgsValidated === true}**`,
    ``,
    `| Slug | Recommended | Write eligible | Will write |`,
    `| --- | --- | --- | --- |`,
    ...(plan.targetGuestSegments || []).map(
      (t) =>
        `| \`${t.slug}\` | ${(t.recommended || []).join(", ")} | ${t.writeEligible} | ${t.willWrite} |`
    ),
    ``,
    `## Rule`,
    ``,
    `- Avoid Luxury / Discerning + Leisure (or Experience-Oriented) adjacency.`,
    `- Pattern: \`${WAVE12_GENERIC_AUDIENCE_PROSE_RE}\``,
    `- Do not write Target Guest Segments when risk is detected.`,
    ``,
  ];
  fs.writeFileSync(tgsMdPath, tgsLines.join("\n"), "utf8");

  const report = {
    ...plan,
    dryRun: !applyResult?.applied,
    airtableWrites: applyResult?.applied === true,
    applyResult,
  };
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave12-tab-factory-build.json");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave12-tab-factory-build.md");
  const md = [
    `# Brand Explorer Wave 12 — Tab Factory Build`,
    ``,
    `Generated: ${plan.generatedAt}`,
    `Dry-run: **${!applyResult?.applied}** · Applied: **${applyResult?.applied === true}**`,
    `Content packs: **${plan.summary?.contentPacks ?? 0}/12**`,
    `Presentation writes planned: **${plan.summary?.plannedPresentationWrites ?? 0}**`,
    `Brand Basics writes planned: **${plan.summary?.plannedBasicsWrites ?? 0}**`,
    `TGS validated: **${plan.tgsValidated === true}**`,
    `Blocked: **${(plan.summary?.blockedSlugs || []).join(", ") || "none"}**`,
    ``,
    `## Guardrails`,
    ``,
    `- Target brands only (Wave 12)`,
    `- No Brand Status / release / CV / Source Library / Registry writes`,
    `- No protected 27 / Radisson Collection / image writes`,
    `- Source-pack grounded Presentation + limited Basics positioning fields`,
    ``,
    `## Target Guest Segments`,
    ``,
    ...((plan.targetGuestSegments || []).map(
      (t) =>
        `- \`${t.slug}\`: ${(t.recommended || []).join(", ")} (write=${t.willWrite})`
    ) || []),
    ``,
    `## Brands`,
    ``,
    `| Slug | Name | Rows | Pres writes | Basics writes | Blocked |`,
    `| --- | --- | ---: | ---: | ---: | --- |`,
    ...(plan.brandResults || []).map(
      (b) =>
        `| \`${b.brandSlug}\` | ${b.brandName} | ${b.presentationRowCount ?? 0} | ${b.patches?.length ?? 0} | ${b.basicsPatches?.length ?? 0} | ${b.blocked === true} |`
    ),
    ``,
    `## Apply flags`,
    ``,
    ...WAVE12_TAB_FACTORY_BUILD_APPLY_FLAGS.map((f) => `- \`${f}\``),
    ``,
  ].join("\n");
  fs.writeFileSync(mdPath, md, "utf8");

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave12-tab-factory-build.md");
  fs.writeFileSync(
    docPath,
    [
      `# Wave 12 Tab Factory Build`,
      ``,
      `Stage 4 of the Wave 12 factory generates owner-facing Presentation packs for 12 Under Review brands from Stage 3 source packs.`,
      ``,
      `- Reports: \`reports/brand-explorer-wave12-tab-factory-build.{json,md}\``,
      `- Per-brand: \`reports/brand-explorer-wave12-tab-factory-build-{slug}.md\``,
      `- Target Guest Segments: \`reports/brand-explorer-wave12-target-guest-segments.md\``,
      ``,
      `## Allowed writes`,
      ``,
      `- Presentation Title / Body / Case Summary / chips`,
      `- Brand Basics: Brand Positioning, Guest Psychographics Description`,
      `- Brand Basics: Target Guest Segments only when validated`,
      ``,
      `## Forbidden`,
      ``,
      `- Brand Status, release fields, Company Validated, Source Library, Registry`,
      `- Protected 27 brands, Radisson Collection, images`,
      ``,
      `## Commands`,
      ``,
      "```bash",
      "npm run brand-explorer-wave12-factory -- --stage tab-factory-build --dry-run",
      "npm run brand-explorer-wave12-factory -- --stage tab-factory-build --apply \\",
      "  --approve-wave12-tab-factory-build \\",
      "  --confirm-target-brands-only \\",
      "  --confirm-source-pack-grounded \\",
      "  --confirm-no-company-validation-changes \\",
      "  --confirm-no-source-library-status-changes \\",
      "  --confirm-no-registry-approval-changes \\",
      "  --confirm-no-brand-status-changes \\",
      "  --confirm-no-release-field-writes \\",
      "  --confirm-no-protected-27-brand-changes \\",
      "  --confirm-no-radisson-collection-changes \\",
      "  --confirm-no-image-writes \\",
      "  --confirm-no-broad-rewrites \\",
      "  --confirm-target-guest-segments-validated",
      "```",
      ``,
    ].join("\n"),
    "utf8"
  );

  return { jsonPath, mdPath, tgsMdPath, docPath, packPaths };
}

export async function runWave12TabFactoryBuild({ dryRun = true, argv = [] } = {}) {
  const plan = await planWave12TabFactoryBuild({ brands: [...WAVE12_SLUGS] });
  let applyResult = null;
  if (!dryRun && argv.includes("--apply")) {
    applyResult = await applyWave12TabFactoryBuild({ plan, apply: true, argv });
    if (!applyResult.applied) {
      const paths = writeWave12TabFactoryBuildReports(plan, applyResult);
      return {
        ...plan,
        pass: false,
        stopRecommended: true,
        applyResult,
        paths,
        summary: {
          ...plan.summary,
          applyReason: applyResult.reason,
          missingFlags: applyResult.missing || [],
        },
      };
    }
  }
  const paths = writeWave12TabFactoryBuildReports(plan, applyResult);
  const blocked = (plan.summary?.blockedSlugs || []).length > 0;
  return {
    ...plan,
    dryRun: dryRun || !applyResult?.applied,
    airtableWrites: applyResult?.applied === true,
    applyResult,
    paths,
    pass: !blocked && plan.tgsValidated === true && (plan.summary?.contentPacks || 0) === 12,
    stopRecommended: blocked || plan.tgsValidated === false,
  };
}

export { assessWave12TgsRisk, WAVE12_GENERIC_AUDIENCE_PROSE_RE };
