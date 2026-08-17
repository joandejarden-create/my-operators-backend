/**
 * Protected 27 PVQL re-green — targeted Target Guest Segments adjacency fix.
 *
 * Clears golden `generic_audience_prose` (and thus PVQL `generic_copy_scan` +
 * `tab_factory_audit`) for Preferred / Radisson Individuals / SLH only.
 *
 * Allowed write: Brand Basics.`Target Guest Segments` (multi-select).
 * Forbidden: CV / Source / Registry / Brand Status / release / images /
 * Presentation broad rewrites / Wave 12 brands.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { evaluateGoldenContentQuality } from "./brand-explorer-golden-content-quality.js";
import { evaluateTabFactoryFromPayload } from "./brand-explorer-tab-factory-evaluate.js";
import { evaluateBrandPublicVisibility } from "./brand-explorer-public-visibility-quality-lock.js";

export const REGREEN_VERSION = "27-protected-pvql-regreen-v1";

export const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";

export const TARGET_BRANDS = Object.freeze([
  Object.freeze({
    slug: "preferred-hotels-and-resorts",
    recordId: "recwl5JOYxlChuCAr",
    name: "Preferred Hotels & Resorts",
    segmentsAfter: Object.freeze([
      "Experience-Oriented",
      "Leisure",
      "International Inbound",
    ]),
  }),
  Object.freeze({
    slug: "radisson-individuals-by-choice",
    recordId: "recRyvM8OmLlDj9G7",
    name: "Radisson Individuals by Choice",
    segmentsAfter: Object.freeze(["Experience-Oriented", "Leisure"]),
  }),
  Object.freeze({
    slug: "small-luxury-hotels-of-the-world",
    recordId: "recjjSnY2opb8P4DG",
    name: "Small Luxury Hotels of the World",
    segmentsAfter: Object.freeze([
      "Experience-Oriented",
      "Leisure",
      "International Inbound",
    ]),
  }),
]);

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-protected-27-pvql-regreen",
  "--confirm-target-brands-only",
  "--confirm-targeted-field-fixes-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-image-writes",
  "--confirm-no-wave12-brand-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-raw-urls",
  "--confirm-no-generic-copy",
]);

const FORBIDDEN_AIRTABLE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Source Library Status",
  "Registry Status",
  "Brand Status",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Image URL",
  "Image",
  "Primary Image",
  "Title",
  "Body",
]);

const ALLOWED_AIRTABLE_FIELDS = new Set(["Target Guest Segments"]);

const GENERIC_AUDIENCE_PROSE =
  /Luxury\s*\/\s*Discerning[,\s]+(?:Experience-Oriented|Leisure)|Leisure Discerning travelers/i;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

export const FAILURES_JSON = "brand-explorer-27-protected-pvql-regreen-failures.json";
export const FAILURES_MD = "brand-explorer-27-protected-pvql-regreen-failures.md";
export const REPORT_JSON = "brand-explorer-27-protected-pvql-regreen.json";
export const REPORT_MD = "brand-explorer-27-protected-pvql-regreen.md";
export const DOC_MD = "brand-explorer-27-protected-pvql-regreen.md";

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
    },
  };
}

async function fetchBrand(recordId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (!res.payload?.brand) throw new Error(`Brand fetch failed for ${recordId}`);
  return res.payload.brand;
}

function presentationRows(brand) {
  return (brand?.brandExplorer?.blocks || []).map((b) => ({
    recordId: b.recordId || b.id,
    slotKey: b.slotKey,
    title: b.title,
    body: b.body,
    caseSummaryOverview: b.caseSummaryOverview,
    caseSummaryTags: b.caseSummaryTags,
    externalDisplayStatus: b.externalDisplayStatus,
    active: b.active !== false,
    imageUrl: b.imageUrl || b.image?.[0]?.url,
  }));
}

function currentSegments(brand) {
  const raw = brand?.targetGuestSegments ?? brand?.targetSegments ?? [];
  return (Array.isArray(raw) ? raw : [raw]).map(String).filter(Boolean);
}

function segmentsNeedAdjacencyFix(segments) {
  return segments.includes("Luxury / Discerning") && segments.includes("Leisure");
}

export function parseProtected27PvqlRegreenFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

export function resolveTargetBrands(brandSlugs) {
  const wanted = (brandSlugs || []).map((s) => String(s).trim().toLowerCase()).filter(Boolean);
  if (!wanted.length) return [...TARGET_BRANDS];
  const out = [];
  for (const slug of wanted) {
    const hit = TARGET_BRANDS.find((b) => b.slug === slug);
    if (!hit) throw new Error(`Refuse non-target brand slug: ${slug}`);
    out.push(hit);
  }
  return out;
}

/**
 * Extract exact PVQL-facing failures for the three protected brands.
 */
export async function extractProtected27PvqlRegreenFailures(brandSlugs) {
  const targets = resolveTargetBrands(brandSlugs);
  const failures = [];
  const brandRows = [];

  for (const target of targets) {
    const brand = await fetchBrand(target.recordId);
    const rows = presentationRows(brand);
    const html = renderBrandExplorerHtmlForTest(brand, {
      allPanels: true,
      internalPreview: false,
    });
    const golden = evaluateGoldenContentQuality(brand, rows, html, {
      brandSlug: target.slug,
    });
    const tf = evaluateTabFactoryFromPayload({
      brand,
      rows,
      html,
      brandSlug: target.slug,
    });
    const segments = currentSegments(brand);
    const htmlText = String(html || "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ");
    const audienceMatch = htmlText.match(/Audience\s+([^.]{0,160})/i);
    const currentAudience = audienceMatch ? audienceMatch[1].trim() : segments.join(", ");

    const brandFailures = [];
    if ((golden.failures || []).includes("generic_audience_prose") || GENERIC_AUDIENCE_PROSE.test(htmlText)) {
      brandFailures.push({
        brand: target.name,
        slug: target.slug,
        tab: "Overview",
        section: "Brand Positioning / Audience",
        recordId: target.recordId,
        field: "Target Guest Segments",
        airtableTable: BRAND_BASICS_TABLE,
        failureType: "generic_copy_scan / generic_audience_prose",
        currentValue: segments.join(", "),
        currentRendered: currentAudience,
        whyItFails:
          'Rendered Audience joins multi-select as "Luxury / Discerning, Leisure…", which matches golden GENERIC_AUDIENCE_PROSE and fails PVQL generic_copy_scan. Tab Factory also fails because golden_content_quality is a hard gate (failFindings may still be 0).',
        proposedFix: `Replace Target Guest Segments with: ${target.segmentsAfter.join(", ")} (drop Luxury / Discerning while keeping Leisure / Experience-Oriented / International Inbound as applicable).`,
        proposedValue: [...target.segmentsAfter],
      });
    }

    if (tf.auditPass !== true) {
      brandFailures.push({
        brand: target.name,
        slug: target.slug,
        tab: "Tab Factory (aggregate)",
        section: "golden_content_quality gate",
        recordId: target.recordId,
        field: "Target Guest Segments (via rendered Audience)",
        airtableTable: BRAND_BASICS_TABLE,
        failureType: "tab_factory_audit",
        currentValue: segments.join(", "),
        currentRendered: `auditPass=${tf.auditPass}; failFindings=${tf.failFindings}; golden.pass=${golden.pass}`,
        whyItFails:
          "tab_factory_audit.pass requires golden_content_quality.pass. Completeness failFindings can be 0 while audit still fails on generic_audience_prose.",
        proposedFix: "Same Target Guest Segments adjacency remediation clears the golden gate and re-greens tab_factory_audit.",
        proposedValue: [...target.segmentsAfter],
      });
    }

    failures.push(...brandFailures);
    brandRows.push({
      slug: target.slug,
      name: target.name,
      recordId: target.recordId,
      segments,
      goldenPass: golden.pass === true,
      goldenFailures: golden.failures || [],
      tabFactoryAuditPass: tf.auditPass === true,
      tabFactoryFailFindings: tf.failFindings,
      failureCount: brandFailures.length,
    });
  }

  return {
    version: REGREEN_VERSION,
    generatedAt: new Date().toISOString(),
    readOnly: true,
    targets: targets.map((t) => t.slug),
    summary: {
      brands: targets.length,
      failureRows: failures.length,
      brandsNeedingFix: brandRows.filter((b) => b.failureCount > 0).length,
    },
    brands: brandRows,
    failures,
  };
}

export function writeProtected27PvqlRegreenFailureReports(extract) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, FAILURES_JSON);
  const mdPath = path.join(reportsDir, FAILURES_MD);
  fs.writeFileSync(jsonPath, `${JSON.stringify(extract, null, 2)}\n`, "utf8");

  const lines = [
    "# Protected 27 PVQL Re-Green — Failure Extraction",
    "",
    `Version: \`${extract.version}\` · Generated: ${extract.generatedAt}`,
    "Read-only. Exact failures only (no guessed fields).",
    "",
    "## Summary",
    "",
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Brands | ${extract.summary.brands} |`,
    `| Failure rows | ${extract.summary.failureRows} |`,
    `| Brands needing fix | ${extract.summary.brandsNeedingFix} |`,
    "",
    "## Failure table",
    "",
    "| Brand | Tab | Section | Record ID | Field | Failure Type | Current Value | Why It Fails | Proposed Fix |",
    "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
  ];
  for (const f of extract.failures || []) {
    lines.push(
      `| ${f.brand} | ${f.tab} | ${f.section} | \`${f.recordId}\` | ${f.field} | ${f.failureType} | ${nz(f.currentValue).replace(/\|/g, "\\|")} | ${nz(f.whyItFails).replace(/\|/g, "\\|")} | ${nz(f.proposedFix).replace(/\|/g, "\\|")} |`
    );
  }
  lines.push(
    "",
    "## Root cause",
    "",
    "All three brands share Brand Basics multi-select `Target Guest Segments` containing both `Luxury / Discerning` and `Leisure`. Public HTML Audience rendering creates the golden `generic_audience_prose` adjacency. That fails `generic_copy_scan` and, transitively, `tab_factory_audit`.",
    "",
    "No Presentation Title/Body/Case Summary offenders were required for these PVQL failures once rows include Case Summary Overview.",
    "",
    "## Out of scope",
    "",
    "- Wave 12 brands",
    "- Tapestry / Dazzler / Trademark / other protected 27",
    "- Company Validated / Source Library / Registry / Brand Status / release / images",
    ""
  );
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");
  return { jsonPath, mdPath };
}

function projectBrandForSegments(brand, segmentsAfter) {
  return { ...brand, targetGuestSegments: [...segmentsAfter] };
}

export async function planProtected27PvqlRegreen({ brands } = {}) {
  const targets = resolveTargetBrands(brands);
  const extract = await extractProtected27PvqlRegreenFailures(targets.map((t) => t.slug));
  const brandPlans = [];
  const patches = [];

  for (const target of targets) {
    const brand = await fetchBrand(target.recordId);
    const before = currentSegments(brand);
    const needsFix = segmentsNeedAdjacencyFix(before);
    const after = [...target.segmentsAfter];
    const rows = presentationRows(brand);
    const htmlBefore = renderBrandExplorerHtmlForTest(brand, {
      allPanels: true,
      internalPreview: false,
    });
    const goldenBefore = evaluateGoldenContentQuality(brand, rows, htmlBefore, {
      brandSlug: target.slug,
    });
    const tfBefore = evaluateTabFactoryFromPayload({
      brand,
      rows,
      html: htmlBefore,
      brandSlug: target.slug,
    });

    const projected = projectBrandForSegments(brand, after);
    const htmlAfter = renderBrandExplorerHtmlForTest(projected, {
      allPanels: true,
      internalPreview: false,
    });
    const goldenAfter = evaluateGoldenContentQuality(projected, rows, htmlAfter, {
      brandSlug: target.slug,
    });
    const tfAfter = evaluateTabFactoryFromPayload({
      brand: projected,
      rows,
      html: htmlAfter,
      brandSlug: target.slug,
    });

    const patch = needsFix
      ? {
          table: BRAND_BASICS_TABLE,
          action: "PATCH",
          recordId: target.recordId,
          brandSlug: target.slug,
          brandName: target.name,
          reason: "golden_generic_audience_prose_segment_adjacency",
          fields: {
            "Target Guest Segments": after,
          },
          fieldMapping: {
            "Target Guest Segments": "Brand Basics.Target Guest Segments",
          },
          sanitizedPayloadPreview: {
            "Target Guest Segments": after,
          },
          before: { "Target Guest Segments": before },
          validation: {
            pass: true,
            checks: [
              "target_brand_only",
              "allowed_field_only",
              "no_company_validation",
              "no_source_library",
              "no_registry",
              "no_brand_status",
              "no_release",
              "no_images",
              "no_presentation_broad_rewrite",
            ],
            failedChecks: [],
          },
        }
      : null;

    if (patch) patches.push(patch);

    brandPlans.push({
      slug: target.slug,
      name: target.name,
      recordId: target.recordId,
      needsFix,
      segmentsBefore: before,
      segmentsAfter: after,
      before: {
        goldenPass: goldenBefore.pass === true,
        goldenFailures: goldenBefore.failures || [],
        tabFactoryAuditPass: tfBefore.auditPass === true,
      },
      projected: {
        goldenPass: goldenAfter.pass === true,
        goldenFailures: goldenAfter.failures || [],
        tabFactoryAuditPass: tfAfter.auditPass === true,
      },
      patch,
    });
  }

  const projectedClean = brandPlans.every(
    (b) => b.projected.goldenPass && b.projected.tabFactoryAuditPass
  );
  const validation = {
    pass: projectedClean && patches.every((p) => p.validation?.pass),
    failedChecks: [
      ...(!projectedClean ? ["projected_pvql_gates_not_clean"] : []),
      ...patches
        .filter((p) => !p.validation?.pass)
        .flatMap((p) => p.validation?.failedChecks || []),
    ],
  };

  return {
    version: REGREEN_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    targets: targets.map((t) => t.slug),
    extractSummary: extract.summary,
    failures: extract.failures,
    brands: brandPlans,
    patches,
    summary: {
      brands: brandPlans.length,
      needingFix: brandPlans.filter((b) => b.needsFix).length,
      patches: patches.length,
      projectedClean,
    },
    validation,
    protections: {
      companyValidatedUntouched: true,
      sourceLibraryUntouched: true,
      registryUntouched: true,
      brandStatusUntouched: true,
      releaseFieldsUntouched: true,
      imageFieldsUntouched: true,
      wave12Untouched: true,
      presentationBroadRewrite: false,
    },
    wave12ResumeGate:
      "Wave 12 may resume at Stage 3 only after apply + public-full PVQL pass + tightened 27 baseline regression pass.",
  };
}

export async function applyProtected27PvqlRegreen({ report, apply = false, argv = [] } = {}) {
  const flags = parseProtected27PvqlRegreenFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flags };
  if (!flags.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flags.missing, flags };
  }
  if (!report?.validation?.pass) {
    return {
      applied: false,
      reason: "validation_failed",
      failedChecks: report?.validation?.failedChecks || [],
    };
  }

  const allowedSlugs = new Set(TARGET_BRANDS.map((b) => b.slug));
  const allowedIds = new Set(TARGET_BRANDS.map((b) => b.recordId));
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const results = [];
  for (const patch of report.patches || []) {
    for (const key of Object.keys(patch.fields || {})) {
      if (FORBIDDEN_AIRTABLE_FIELDS.has(key) || !ALLOWED_AIRTABLE_FIELDS.has(key)) {
        throw new Error(`Refuse forbidden or unexpected field write: ${key}`);
      }
    }
    if (!allowedSlugs.has(patch.brandSlug)) {
      throw new Error(`Refuse non-target brand patch: ${patch.brandSlug}`);
    }
    if (patch.table !== BRAND_BASICS_TABLE) {
      throw new Error(`Refuse unexpected table write: ${patch.table}`);
    }
    if (!allowedIds.has(patch.recordId)) {
      throw new Error(`Refuse Brand Basics write to non-target record ${patch.recordId}`);
    }

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(patch.table)}/${patch.recordId}`;
    const res = await fetch(url, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: patch.fields }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(
        json.error?.message || `PATCH failed ${patch.recordId}: ${res.status}`
      );
    }
    results.push({
      recordId: patch.recordId,
      brandSlug: patch.brandSlug,
      applied: true,
      action: "PATCH",
      table: patch.table,
      fields: Object.keys(patch.fields),
      reason: patch.reason,
    });
  }

  return {
    applied: true,
    results,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    brandStatusUntouched: true,
    releaseFieldsUntouched: true,
    imageFieldsUntouched: true,
    wave12Untouched: true,
  };
}

export async function spotCheckProtected27PvqlRegreen(brandSlugs) {
  const targets = resolveTargetBrands(brandSlugs);
  const rows = [];
  for (const t of targets) {
    const row = await evaluateBrandPublicVisibility(t.slug);
    rows.push({
      slug: t.slug,
      lockPass: row.lockPass === true,
      failures: row.failures || [],
      genericCopyPass: row.gateResults?.generic_copy_scan?.pass === true,
      tabFactoryPass: row.gateResults?.tab_factory_audit?.pass === true,
    });
  }
  return {
    brands: rows,
    allPass: rows.every((r) => r.lockPass),
  };
}

function brandMd(plan, applyResult) {
  const lines = [
    `# Protected 27 PVQL Re-Green — ${plan.name}`,
    "",
    `Slug: \`${plan.slug}\` · Record: \`${plan.recordId}\``,
    `Needs fix: **${plan.needsFix}**`,
    "",
    "## Target Guest Segments",
    "",
    `- Before: ${plan.segmentsBefore.join(", ") || "(empty)"}`,
    `- After: ${plan.segmentsAfter.join(", ")}`,
    "",
    "## Gate projection",
    "",
    `| Gate | Before | After (projected) |`,
    `| --- | --- | --- |`,
    `| golden / generic_audience_prose | ${plan.before.goldenPass ? "pass" : (plan.before.goldenFailures || []).join(",") || "fail"} | ${plan.projected.goldenPass ? "pass" : (plan.projected.goldenFailures || []).join(",") || "fail"} |`,
    `| tab_factory_audit | ${plan.before.tabFactoryAuditPass} | ${plan.projected.tabFactoryAuditPass} |`,
    "",
    "## Write plan",
    "",
    plan.patch
      ? [
          `- Table: \`${plan.patch.table}\``,
          `- Field: \`Target Guest Segments\``,
          `- Reason: ${plan.patch.reason}`,
          `- Applied: ${applyResult?.applied === true && (applyResult.results || []).some((r) => r.recordId === plan.recordId)}`,
        ].join("\n")
      : "_No write required._",
    "",
    "## Protections",
    "",
    "- Company Validated / Source Library / Registry / Brand Status / release / images untouched",
    "- No Presentation broad rewrite",
    "- No Wave 12 brand changes",
    "",
  ];
  return `${lines.join("\n")}\n`;
}

export function writeProtected27PvqlRegreenReports(report, applyResult = null) {
  const reportsDir = path.join(ROOT, "reports");
  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const out = {
    ...report,
    dryRun: applyResult?.applied !== true,
    applyResult: applyResult || { applied: false, reason: "dry_run_only" },
  };

  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  const docPath = path.join(docsDir, DOC_MD);
  fs.writeFileSync(jsonPath, `${JSON.stringify(out, null, 2)}\n`, "utf8");

  const perBrand = {
    "preferred-hotels-and-resorts": "brand-explorer-27-protected-pvql-regreen-preferred.md",
    "radisson-individuals-by-choice":
      "brand-explorer-27-protected-pvql-regreen-radisson-individuals.md",
    "small-luxury-hotels-of-the-world": "brand-explorer-27-protected-pvql-regreen-slh.md",
  };

  for (const plan of report.brands || []) {
    const name = perBrand[plan.slug];
    if (!name) continue;
    fs.writeFileSync(path.join(reportsDir, name), brandMd(plan, applyResult), "utf8");
  }

  const lines = [
    "# Protected 27 PVQL Re-Green",
    "",
    `Version: \`${report.version}\` · Generated: ${report.generatedAt}`,
    `Applied: **${applyResult?.applied === true}**`,
    "",
    "## Targets (3 protected brands only)",
    "",
    ...(report.targets || []).map((s) => `- \`${s}\``),
    "",
    "## Summary",
    "",
    `| Metric | Count |`,
    `| --- | ---: |`,
    `| Brands | ${report.summary.brands} |`,
    `| Needing fix | ${report.summary.needingFix} |`,
    `| Patches | ${report.summary.patches} |`,
    `| Projected clean | ${report.summary.projectedClean} |`,
    "",
    "## Patches",
    "",
    "| Brand | Record ID | Field | Before | After | Reason |",
    "| --- | --- | --- | --- | --- | --- |",
  ];
  for (const p of report.patches || []) {
    lines.push(
      `| ${p.brandName} | \`${p.recordId}\` | Target Guest Segments | ${(p.before?.["Target Guest Segments"] || []).join(", ")} | ${(p.fields?.["Target Guest Segments"] || []).join(", ")} | ${p.reason} |`
    );
  }
  lines.push(
    "",
    "## Protections",
    "",
    "- Company Validated untouched",
    "- Source Library untouched",
    "- Registry untouched",
    "- Brand Status untouched",
    "- Release fields untouched",
    "- Images untouched",
    "- Wave 12 brands untouched",
    "- No Presentation broad rewrites",
    "",
    "## Wave 12 gate",
    "",
    report.wave12ResumeGate || "",
    "",
    "## Per-brand reports",
    "",
    "- `reports/brand-explorer-27-protected-pvql-regreen-preferred.md`",
    "- `reports/brand-explorer-27-protected-pvql-regreen-radisson-individuals.md`",
    "- `reports/brand-explorer-27-protected-pvql-regreen-slh.md`",
    ""
  );
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");

  const docLines = [
    "# Brand Explorer — Protected 27 PVQL Re-Green",
    "",
    `Version: \`${REGREEN_VERSION}\``,
    "",
    "## Purpose",
    "",
    "Clear PVQL `generic_copy_scan` + `tab_factory_audit` on three protected public-full brands before Wave 12 Stage 3 resumes.",
    "",
    "## Targets",
    "",
    "- Preferred Hotels & Resorts (`preferred-hotels-and-resorts`)",
    "- Radisson Individuals by Choice (`radisson-individuals-by-choice`)",
    "- Small Luxury Hotels of the World (`small-luxury-hotels-of-the-world`)",
    "",
    "## Exact fix",
    "",
    "Brand Basics multi-select **Target Guest Segments**: remove `Luxury / Discerning` when `Leisure` is also selected, so rendered Audience no longer matches golden `generic_audience_prose`.",
    "",
    "## Commands",
    "",
    "```bash",
    "npm run brand-explorer-27-protected-pvql-regreen -- --brands preferred-hotels-and-resorts,radisson-individuals-by-choice,small-luxury-hotels-of-the-world --dry-run",
    "",
    "npm run brand-explorer-27-protected-pvql-regreen -- --brands preferred-hotels-and-resorts,radisson-individuals-by-choice,small-luxury-hotels-of-the-world --apply \\",
    "  --approve-protected-27-pvql-regreen \\",
    "  --confirm-target-brands-only \\",
    "  --confirm-targeted-field-fixes-only \\",
    "  --confirm-no-company-validation-changes \\",
    "  --confirm-no-source-library-status-changes \\",
    "  --confirm-no-registry-approval-changes \\",
    "  --confirm-no-brand-status-changes \\",
    "  --confirm-no-release-field-changes \\",
    "  --confirm-no-image-writes \\",
    "  --confirm-no-wave12-brand-changes \\",
    "  --confirm-no-broad-rewrites \\",
    "  --confirm-no-raw-urls \\",
    "  --confirm-no-generic-copy",
    "```",
    "",
    "## Forbidden",
    "",
    "CV / Source Library / Registry / Brand Status / release / images / Wave 12 / unrelated protected brands / Presentation broad rewrites.",
    "",
    "## Acceptance",
    "",
    "1. Three target brands pass PVQL",
    "2. `test:brand-explorer-public-visibility-quality-lock -- --public-full-only` passes",
    "3. Tightened `test:brand-explorer-27-active-public-full-baseline` requires fresh/clean PVQL",
    "4. Wave 12 may resume at Stage 3 only after the above",
    "",
  ];
  fs.writeFileSync(docPath, `${docLines.join("\n")}\n`, "utf8");

  return {
    jsonPath,
    mdPath,
    docPath,
    perBrandPaths: Object.values(perBrand).map((n) => path.join(reportsDir, n)),
  };
}
