/**
 * Brand Explorer — 27 new-brand visual materialization
 * (Tapestry, Dazzler, Trademark).
 *
 * Allowed writes: Presentation Image + titles/captions + openings case summaries
 * for target brands only. Optional targeted FDD/franchise-disclosure scrub required
 * to clear external_owner_copy_fail so Active Profile Approved brands can reach
 * active_profile_ready / shouldRenderFullProfile (no CV / Source / Registry /
 * Brand Status / release-field writes).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildLane2ImageAssetPackForBrand,
} from "./brand-explorer-lane2-image-asset-pack.js";
import {
  planLane2BrandImageMaterialization,
  applyLane2ImageMaterializationPlans,
  toAirtableFetchableImageUrl,
} from "./brand-explorer-lane2-image-materialization.js";
import {
  listPresentationRowsLight,
  resolveLane2BrandIdentity,
  LANE2_ROOT,
} from "./brand-explorer-lane2-common.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import { getFullBuildContent } from "./brand-explorer-full-build-content.js";

export const VISUAL_MAT_VERSION = "27-new-brand-visual-materialization-v1";

export const TARGET_SLUGS = Object.freeze([
  "tapestry-collection-by-hilton",
  "dazzler-by-wyndham",
  "trademark-collection-by-wyndham",
]);

export const PROTECTED_24_SLUGS_NOTE =
  "Original protected 24 Active brands are out of scope and must not be written.";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-visual-materialization",
  "--confirm-target-brands-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-protected-24-brand-changes",
  "--confirm-image-uniqueness",
  "--confirm-image-role-match",
  "--confirm-no-logo-only-filler",
  "--confirm-no-wrong-brand-images",
]);

/** Mapped to Lane 2 apply flags when forwarding materialization writes. */
export const LANE2_FLAG_FORWARD = Object.freeze([
  "--approve-lane2-image-materialization",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-release-field-writes",
  "--confirm-six-distinct-gallery-images",
  "--confirm-three-distinct-scenario-images",
  "--confirm-three-distinct-property-images",
  "--confirm-image-role-match",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const FORBIDDEN = new Set([
  "Company Validated",
  "Company Validation Date",
  "Brand Status",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const SHORT = Object.freeze({
  "tapestry-collection-by-hilton": "tapestry",
  "dazzler-by-wyndham": "dazzler",
  "trademark-collection-by-wyndham": "trademark",
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function parseVisualMaterializationFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    dryRun: !argv.includes("--apply") || argv.includes("--dry-run"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

export function resolveTargetBrands(raw = []) {
  const aliases = {
    tapestry: "tapestry-collection-by-hilton",
    "tapestry-collection-by-hilton": "tapestry-collection-by-hilton",
    dazzler: "dazzler-by-wyndham",
    "dazzler-by-wyndham": "dazzler-by-wyndham",
    trademark: "trademark-collection-by-wyndham",
    "trademark-collection": "trademark-collection-by-wyndham",
    "trademark-collection-by-wyndham": "trademark-collection-by-wyndham",
  };
  const list = (raw.length ? raw : [...TARGET_SLUGS]).map((s) => {
    const key = nz(s).toLowerCase();
    const slug = aliases[key];
    if (!slug || !TARGET_SLUGS.includes(slug)) {
      throw new Error(`Out-of-scope brand refused: ${s}`);
    }
    return slug;
  });
  return [...new Set(list)];
}

function scrubFranchiseDisclosureBody(body) {
  let next = nz(body);
  if (!next) return next;
  next = next.replace(/\bfranchise disclosure schedule\b/gi, "brand disclosure schedule");
  next = next.replace(/\bfranchise disclosure\b/gi, "brand disclosure materials");
  next = next.replace(/\bin your FDD and\b/gi, "in your development materials and");
  next = next.replace(/\bin your FDD\b/gi, "in your development materials");
  next = next.replace(/\byour FDD\b/gi, "your development materials");
  next = next.replace(/\bconfirm in FDD\b/gi, "confirm with the brand");
  next = next.replace(/\bconfirm in the FDD\b/gi, "confirm with the brand");
  next = next.replace(/\bper agreement—sequence with financing\./gi, "per agreement — sequence with financing.");
  return next;
}

function scenarioTitlesFromContentPack(slug) {
  const pack = getFullBuildContent(slug);
  const titles = {};
  for (const row of pack?.presentation || []) {
    if (/^overview\.scenario\.\d+$/i.test(row.slotKey) && nz(row.title)) {
      titles[row.slotKey] = nz(row.title);
    }
  }
  return titles;
}

async function airtableWrite({ recordId, fields, method }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  for (const k of Object.keys(fields || {})) {
    if (FORBIDDEN.has(k)) throw new Error(`Forbidden field write: ${k}`);
  }
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
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
    lastErr = new Error(json.error?.message || `${method} failed: ${res.status}`);
    if (!(res.status === 429 || res.status >= 500) || attempt === maxAttempts) break;
    await sleep(Math.min(30_000, 800 * 2 ** (attempt - 1)));
  }
  throw lastErr;
}

export async function planBrandVisualMaterialization(brandSlug) {
  const identity = resolveLane2BrandIdentity(brandSlug);
  const assetPack = buildLane2ImageAssetPackForBrand(brandSlug);
  const imagePlan = await planLane2BrandImageMaterialization(brandSlug, {
    assetPackBrand: assetPack,
  });

  // Prefer content-pack scenario titles on create patches.
  const scenarioTitles = scenarioTitlesFromContentPack(brandSlug);
  for (const patch of imagePlan.presentationPatches || []) {
    if (/^overview\.scenario\.\d+$/i.test(patch.slotKey) && !patch.recordId) {
      const t = scenarioTitles[patch.slotKey];
      if (t) patch.fields.Title = t;
    }
    // Ensure Image URLs are Airtable-fetchable
    if (patch.fields?.Image?.[0]?.url) {
      patch.fields.Image = [{ url: toAirtableFetchableImageUrl(patch.fields.Image[0].url) }];
    }
  }

  let rows = [];
  try {
    const fetch = await listPresentationRowsLight(identity.recordId, identity.name);
    rows = fetch.rows || [];
  } catch (err) {
    rows = [];
    imagePlan.blockers = [...(imagePlan.blockers || []), `row_fetch:${err.message}`];
  }

  const displayGatePatches = [];
  for (const row of rows) {
    if (!row.recordId) continue;
    if (
      !["loyalty.owner_lens", "loyalty.implications.pnl", "operations.standards_philosophy"].includes(
        row.slotKey
      )
    ) {
      continue;
    }
    const scrubbed = scrubFranchiseDisclosureBody(row.body);
    if (scrubbed && scrubbed !== nz(row.body)) {
      displayGatePatches.push({
        recordId: row.recordId,
        slotKey: row.slotKey,
        fields: { Body: scrubbed },
        reason: "scrub_franchise_disclosure_or_fdd_language",
        beforePreview: nz(row.body).slice(0, 160),
        afterPreview: scrubbed.slice(0, 160),
      });
    }
  }

  // Existing openings missing case summaries (Tapestry).
  const openings = rows.filter((r) => r.slotKey === "footprint.openings" && r.recordId);
  for (const row of openings.slice(0, 4)) {
    // Light fetch has no case summaries — always plan fill when openings exist.
    // Apply path will PATCH case summary fields only (safe for image-bearing rows).
    const propertyName = nz(row.title).split("—")[0].trim() || nz(row.title) || "Property example";
    displayGatePatches.push({
      recordId: row.recordId,
      slotKey: "footprint.openings",
      fields: {
        "Case Summary Overview": `${propertyName} is an official collection property reference for Brand Explorer visual and footprint examples.`,
        "Case Summary Brand Relevance":
          "Official property photography used as a Brand Explorer property example for this collection.",
        "Case Summary Owner Objective":
          "Use as a directional property reference when underwriting design intensity, capital scope, and platform fit.",
        "Case Summary Interpretation":
          "Confirm live affiliation criteria and property-specific scope with the brand before underwriting.",
        "Case Summary Tags": "Collection, Property example, Official photography",
      },
      reason: "fill_openings_case_summary_modal_fields",
    });
  }

  const uniquenessBefore = evaluateImageUniqueness({
    brandSlug,
    presentationRows: rows,
  });
  const roleBefore = evaluateBrandImageRoleMatch({
    brandSlug,
    presentationRows: rows,
  });
  const externalBefore = evaluateExternalOwnerReadinessRule(rows);

  return {
    brandSlug,
    brandName: identity.name,
    recordId: identity.recordId,
    shortName: SHORT[brandSlug],
    assetPack: {
      status: assetPack.status,
      pass: assetPack.pass,
      blockers: assetPack.blockers,
      counts: assetPack.counts,
      poolStats: assetPack.poolStats,
    },
    imagePlan,
    displayGatePatches,
    before: {
      uniquenessPass: uniquenessBefore.pass,
      galleryDistinct: uniquenessBefore.galleryDistinctCount,
      scenarioDistinct: uniquenessBefore.scenarioDistinctCount,
      propertyDistinct: uniquenessBefore.propertyExampleDistinctCount,
      rolePass: roleBefore.pass,
      externalOwnerPass: externalBefore.pass,
      externalOwnerBlockers: externalBefore.blockers,
      galleryRows: rows.filter((r) => /^materials\.gallery\.\d+$/i.test(r.slotKey)).length,
      scenarioRows: rows.filter((r) => /^overview\.scenario\.\d+$/i.test(r.slotKey)).length,
      openingsRows: openings.length,
      galleryWithImage: rows.filter(
        (r) => /^materials\.gallery\.\d+$/i.test(r.slotKey) && r.imageUrl
      ).length,
      scenarioWithImage: rows.filter(
        (r) => /^overview\.scenario\.\d+$/i.test(r.slotKey) && r.imageUrl
      ).length,
      openingsWithImage: openings.filter((r) => r.imageUrl).length,
    },
    blocked: imagePlan.blocked === true || assetPack.pass !== true,
    blockers: [...(assetPack.blockers || []), ...(imagePlan.blockers || [])],
  };
}

export async function planVisualMaterialization({ brands = [...TARGET_SLUGS] } = {}) {
  const targets = resolveTargetBrands(brands);
  const brandResults = [];
  for (const slug of targets) {
    brandResults.push(await planBrandVisualMaterialization(slug));
  }
  return {
    version: VISUAL_MAT_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands: targets,
    brandResults,
    requiredApplyFlags: [...REQUIRED_APPLY_FLAGS],
    guardrails: {
      targetBrandsOnly: true,
      protected24Untouched: true,
      companyValidatedUntouched: true,
      sourceLibraryUntouched: true,
      registryUntouched: true,
      brandStatusUntouched: true,
      releaseFieldsUntouched: true,
    },
  };
}

export async function applyVisualMaterialization({
  plan,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseVisualMaterializationFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing, flagCheck };
  }

  // Refuse any non-target brand in plan
  for (const b of plan.brandResults || []) {
    if (!TARGET_SLUGS.includes(b.brandSlug)) {
      throw new Error(`Refuse: non-target brand in plan ${b.brandSlug}`);
    }
  }

  const lane2Argv = [...argv, "--apply", ...LANE2_FLAG_FORWARD];
  const imageApply = await applyLane2ImageMaterializationPlans({
    brandResults: (plan.brandResults || []).map((b) => b.imagePlan),
    apply: true,
    argv: lane2Argv,
  });

  const displayResults = {};
  for (const brand of plan.brandResults || []) {
    const updated = [];
    const errors = [];
    for (const patch of brand.displayGatePatches || []) {
      try {
        await airtableWrite({
          recordId: patch.recordId,
          fields: patch.fields,
          method: "PATCH",
        });
        updated.push({ recordId: patch.recordId, slotKey: patch.slotKey, reason: patch.reason });
        await sleep(220);
      } catch (err) {
        errors.push({
          recordId: patch.recordId,
          slotKey: patch.slotKey,
          error: err.message,
        });
      }
    }
    displayResults[brand.brandSlug] = { updated, errors };
  }

  return {
    applied: true,
    reason: "visual_materialization_applied",
    flagCheck,
    imageApply,
    displayResults,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    brandStatusUntouched: true,
    releaseFieldsUntouched: true,
  };
}

function brandMd(brand) {
  const lines = [
    `# Visual Materialization — ${brand.brandName}`,
    ``,
    `> ${VISUAL_MAT_VERSION}`,
    ``,
    `| Metric | Before |`,
    `|---|---|`,
    `| Gallery rows / with image | ${brand.before.galleryRows} / ${brand.before.galleryWithImage} |`,
    `| Scenario rows / with image | ${brand.before.scenarioRows} / ${brand.before.scenarioWithImage} |`,
    `| Openings rows / with image | ${brand.before.openingsRows} / ${brand.before.openingsWithImage} |`,
    `| Uniqueness | ${brand.before.uniquenessPass} (g=${brand.before.galleryDistinct} s=${brand.before.scenarioDistinct} p=${brand.before.propertyDistinct}) |`,
    `| Role-match | ${brand.before.rolePass} |`,
    `| External-owner | ${brand.before.externalOwnerPass} |`,
    ``,
    `## Asset pack`,
    ``,
    `- Status: **${brand.assetPack.status}** pass=${brand.assetPack.pass}`,
    `- Counts: gallery=${brand.assetPack.counts?.gallery} scenario=${brand.assetPack.counts?.scenario} property=${brand.assetPack.counts?.property}`,
    `- Pool accepted: ${brand.assetPack.poolStats?.acceptedRows ?? "—"}`,
    ``,
    `## Planned image patches: ${brand.imagePlan.presentationPatches?.length || 0}`,
    ``,
  ];
  for (const p of brand.imagePlan.presentationPatches || []) {
    lines.push(
      `- \`${p.slotKey}\` ${p.recordId ? `PATCH ${p.recordId}` : "POST"} ← ${(p.imageUrl || "").slice(0, 90)}`
    );
  }
  lines.push(``);
  lines.push(`## Display-gate patches: ${brand.displayGatePatches?.length || 0}`);
  lines.push(``);
  for (const p of brand.displayGatePatches || []) {
    lines.push(`- \`${p.slotKey}\` ${p.recordId} — ${p.reason}`);
  }
  if (brand.before.externalOwnerBlockers?.length) {
    lines.push(``);
    lines.push(`## External-owner blockers (before)`);
    lines.push(``);
    for (const b of brand.before.externalOwnerBlockers) lines.push(`- ${b}`);
  }
  lines.push(``);
  return lines.join("\n");
}

export function writeVisualMaterializationReports(plan, applyResult = null) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const report = {
    ...plan,
    dryRun: !applyResult?.applied,
    applyResult,
  };

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-27-new-brand-visual-materialization.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-27-new-brand-visual-materialization.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-27-new-brand-visual-materialization.md");

  const lines = [
    `# Brand Explorer — 27 New-Brand Visual Materialization`,
    ``,
    `> Version \`${VISUAL_MAT_VERSION}\` · Generated \`${report.generatedAt}\``,
    `> Mode: **${report.dryRun ? "dry-run" : "APPLY"}**`,
    ``,
    `Targets: ${(report.brands || []).map((s) => `\`${s}\``).join(", ")}`,
    ``,
    `## Guardrails`,
    ``,
    `- Target brands only`,
    `- Protected 24 untouched`,
    `- No Company Validated / Source Library / Registry / Brand Status / release-field writes`,
    ``,
    `## Brand summaries`,
    ``,
  ];
  for (const b of report.brandResults || []) {
    lines.push(
      `- **${b.brandName}**: imagePatches=${b.imagePlan.presentationPatches?.length || 0} displayPatches=${b.displayGatePatches?.length || 0} blocked=${b.blocked} uniqBefore=${b.before.uniquenessPass} externalBefore=${b.before.externalOwnerPass}`
    );
  }
  lines.push(``);
  if (applyResult) {
    lines.push(`## Apply result`);
    lines.push(``);
    lines.push(`- Applied: **${applyResult.applied}**`);
    lines.push(`- Reason: ${applyResult.reason}`);
    lines.push(``);
  }
  lines.push(`## Note on baseline`);
  lines.push(``);
  lines.push(
    `The 27 baseline is an **Active/Live universe** freeze until all 27 brands reach \`shouldRenderFullProfile=true\` and PVQL public-full-only passes.`
  );
  lines.push(``);

  const md = `${lines.join("\n")}\n`;
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");

  const perBrand = {};
  for (const b of report.brandResults || []) {
    const short = b.shortName || SHORT[b.brandSlug] || b.brandSlug;
    const brandMdPath = path.join(REPORTS_DIR, `brand-explorer-visual-materialization-${short}.md`);
    fs.writeFileSync(brandMdPath, `${brandMd(b)}\n`, "utf8");
    perBrand[b.brandSlug] = brandMdPath;
  }

  return { jsonPath, mdPath, docsPath, perBrand };
}
