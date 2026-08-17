/**
 * Brand Explorer image role-match remediation.
 *
 * Prefer: caption patch → slot swap → Accor/SLH typed replacement.
 * Never writes Company Validated, release fields, Source Library, or Registry approval.
 * Protected brands are refused.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import {
  MGALLERY_PROPERTY_CATALOG,
} from "./brand-explorer-lifestyle-affiliation-property-catalog.js";
import { ORIGINAL_GOLDEN_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";
import { LEGACY_SEED_SLUGS } from "./brand-explorer-legacy-approved-profile-reconciliation.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import {
  IMAGE_ROLES,
  DEFAULT_GALLERY_ROLE_SEQUENCE,
  GALLERY_ROLE_CAPTIONS,
  captionForRole,
  buildAccorRoleImageUrl,
  evaluateBrandImageRoleMatch,
  evaluateImageRoleMatch,
  detectVisualCategory,
} from "./brand-explorer-image-role-match.js";

export const ROLE_MATCH_REMEDIATION_VERSION = "image-role-match-remediation-v1";

export const TARGET_BRANDS = Object.freeze(["mgallery-collection"]);

export const PROTECTED_BRANDS = Object.freeze([
  "hotel-indigo",
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
  "design-hotels",
  "tribute-portfolio",
  "ascend",
  "comfort",
  "comfort-inn-suites",
  "curio",
  "curio-collection",
  "small-luxury-hotels-of-the-world",
  ...ORIGINAL_GOLDEN_RELEASE_SLUGS,
  ...LEGACY_SEED_SLUGS.filter((s) => s !== "mgallery-collection"),
]);

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-image-role-match-remediation",
  "--confirm-no-company-validation-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-protected-brands-unchanged",
  "--confirm-six-distinct-gallery-images",
  "--confirm-image-captions-match-visual-content",
  "--confirm-no-wrong-role-images",
]);

export const REPORT_JSON = "brand-explorer-image-role-match-remediation.json";
export const REPORT_MD = "brand-explorer-image-role-match-remediation.md";
export const REPORT_MGALLERY_MD = "brand-explorer-image-role-match-remediation-mgallery.md";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function parseRoleMatchApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

async function urlExists(url) {
  try {
    const res = await fetch(url, { method: "HEAD", redirect: "follow" });
    return res.status >= 200 && res.status < 400;
  } catch {
    return false;
  }
}

/**
 * MGallery: rebuild gallery from Accor typed assets so roles match codes.
 */
async function planMgalleryRoleTypedGallery(presentationRows) {
  const hotels = MGALLERY_PROPERTY_CATALOG.filter((p) => (p.galleryPriority || 1) === 1);
  // Spread roles across hotels for variety
  const plan = [
    { role: IMAGE_ROLES.exterior_arrival, hotel: hotels[0], typeHint: "ho", index: "00" },
    { role: IMAGE_ROLES.guest_room_suite, hotel: hotels[0], typeHint: "ro", index: "00" },
    { role: IMAGE_ROLES.wellness_pool_spa, hotel: hotels[1], typeHint: "sp", index: "00" },
    { role: IMAGE_ROLES.food_beverage_experience, hotel: hotels[0], typeHint: "ba", index: "00" },
    { role: IMAGE_ROLES.design_detail, hotel: hotels[1], typeHint: "sp", index: "01" },
    { role: IMAGE_ROLES.property_setting, hotel: hotels[2], typeHint: "ho", index: "01" },
  ];

  const galleryRows = presentationRows
    .filter((r) => /^materials\.gallery\.\d+$/.test(nz(r.slotKey)))
    .sort((a, b) => Number(String(a.slotKey).match(/\.(\d+)$/)?.[1] || 0) - Number(String(b.slotKey).match(/\.(\d+)$/)?.[1] || 0));

  const assignments = [];
  for (let i = 0; i < 6; i++) {
    const slotKey = `materials.gallery.${i + 1}`;
    const row = galleryRows.find((r) => r.slotKey === slotKey) || galleryRows[i] || null;
    const spec = plan[i];
    let imageUrl = buildAccorRoleImageUrl(spec.hotel.propertyKey, spec.role, spec.index);
    // Fallback index search
    if (!(await urlExists(imageUrl))) {
      for (const idx of ["00", "01", "02", "03"]) {
        const alt = buildAccorRoleImageUrl(spec.hotel.propertyKey, spec.role, idx);
        if (await urlExists(alt)) {
          imageUrl = alt;
          break;
        }
      }
    }
    const exists = await urlExists(imageUrl);
    const detected = detectVisualCategory({ imageUrl, filename: imageUrl.split("/").pop() });
    const title = captionForRole(spec.role, spec.hotel.propertyName);
    assignments.push({
      slotKey,
      recordId: row?.recordId || null,
      role: spec.role,
      title,
      imageUrl: exists ? imageUrl : null,
      sourcePageUrl: spec.hotel.sourcePageUrl,
      propertyName: spec.hotel.propertyName,
      propertyKey: spec.hotel.propertyKey,
      detectedVisualCategory: detected.category,
      currentCaption: row?.title || null,
      currentImageUrl: row?.imageUrl || null,
      strategy: "typed_accor_replacement",
      status: exists ? "ready" : "missing_asset",
    });
  }
  return assignments;
}

/**
 * Generic path: caption-patch mismatches when detection is high-confidence.
 */
function planCaptionPatches(roleMatch) {
  const patches = [];
  for (const ev of roleMatch.evaluations || []) {
    if (ev.section !== "gallery") continue;
    if (ev.matchStatus !== "wrong_role" && ev.matchStatus !== "caption_overclaim") continue;
    if (!ev.recommendedRole || !ev.recommendedCaption) continue;
    if (ev.detectionConfidence === "low") continue;
    const prop = ev.propertyName || "";
    const title = prop ? `${ev.recommendedCaption} - ${prop}` : ev.recommendedCaption;
    patches.push({
      slotKey: ev.slotKey,
      recordId: ev.recordId,
      strategy: "caption_patch",
      title,
      imageUrl: ev.imageUrl,
      role: ev.recommendedRole,
      status: "ready",
      currentCaption: ev.currentCaption,
      detectedVisualCategory: ev.detectedVisualCategory,
    });
  }
  return patches;
}

function projectFromAssignments(assignments, presentationRows) {
  const projected = [];
  for (const a of assignments) {
    projected.push({
      slotKey: a.slotKey,
      title: a.title,
      imageUrl: a.imageUrl,
      imageFilename: (a.imageUrl || "").split("/").pop(),
      recordId: a.recordId,
    });
  }
  for (const row of presentationRows) {
    const sk = nz(row.slotKey);
    if (/^overview\.scenario\./.test(sk) || sk === "footprint.openings") {
      projected.push(row);
    }
  }
  const uniqueness = evaluateImageUniqueness({
    brandSlug: "projection",
    presentationRows: projected,
  });
  const roleMatch = evaluateBrandImageRoleMatch({
    presentationRows: projected,
    brandSlug: "projection",
  });
  return { uniqueness, roleMatch };
}

export async function planBrandImageRoleMatchRemediation(brandSlug) {
  if (PROTECTED_BRANDS.includes(brandSlug) && !TARGET_BRANDS.includes(brandSlug)) {
    throw new Error(`Refuse role-match remediation for protected brand ${brandSlug}`);
  }
  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig) throw new Error(`No brand config for ${brandSlug}`);

  const ctx = await loadBrandFactoryContext(brandSlug);
  const presentationRows = ctx.presentationRows || [];
  const beforeRole = evaluateBrandImageRoleMatch({ presentationRows, brandSlug });
  const beforeUniq = evaluateImageUniqueness({ presentationRows, brandSlug });

  let assignments = [];
  let strategy = "none";

  if (brandSlug === "mgallery-collection") {
    strategy = "typed_accor_gallery_rebuild";
    assignments = await planMgalleryRoleTypedGallery(presentationRows);
  } else if (!beforeRole.pass) {
    strategy = "caption_patch";
    assignments = planCaptionPatches(beforeRole).map((p) => ({
      ...p,
      propertyName: p.propertyName || "",
      sourcePageUrl: null,
    }));
  }

  const projection = projectFromAssignments(assignments, presentationRows);
  const presentationPatches = [];
  for (const a of assignments) {
    if (!a.imageUrl || !a.recordId) continue;
    presentationPatches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: a.recordId,
      slotKey: a.slotKey,
      brandSlug,
      fields: {
        Title: a.title,
        Image: [{ url: a.imageUrl }],
      },
      reason: "image_role_match_remediation",
      strategy: a.strategy || strategy,
    });
  }

  const canApply =
    projection.roleMatch.pass === true &&
    projection.uniqueness.pass === true &&
    assignments.every((a) => a.imageUrl) &&
    presentationPatches.length >= 6;

  return {
    brandSlug,
    brandName: brandConfig.name,
    recordId: brandConfig.recordId,
    strategy,
    before: {
      roleMatchPass: beforeRole.pass,
      unresolvedRoleMismatchCount: beforeRole.unresolvedRoleMismatchCount,
      uniquenessPass: beforeUniq.pass,
      galleryDistinctCount: beforeUniq.galleryDistinctCount,
      mismatches: beforeRole.unresolved,
    },
    assignments,
    projected: {
      roleMatchPass: projection.roleMatch.pass,
      unresolvedRoleMismatchCount: projection.roleMatch.unresolvedRoleMismatchCount,
      uniquenessPass: projection.uniqueness.pass,
      galleryDistinctCount: projection.uniqueness.galleryDistinctCount,
      scenarioDistinctCount: projection.uniqueness.scenarioDistinctCount,
      propertyExampleDistinctCount: projection.uniqueness.propertyExampleDistinctCount,
      evaluations: projection.roleMatch.evaluations.filter((e) => e.section === "gallery"),
    },
    presentationPatches,
    canApply,
    requiredAction: canApply ? "apply_image_role_match_remediation" : "image_remediation",
    companyValidatedUntouched: true,
    releaseFieldsUntouched: true,
  };
}

async function airtablePatch({ baseId, apiKey, recordId, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH failed: ${res.status}`);
  return json;
}

export async function applyImageRoleMatchRemediation({
  brandResults,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseRoleMatchApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of brandResults) {
    if (PROTECTED_BRANDS.includes(brand.brandSlug) && !TARGET_BRANDS.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to protected brand ${brand.brandSlug}`);
    }
    if (!brand.canApply) {
      resultsByBrand[brand.brandSlug] = { applied: false, reason: "projection_not_ready" };
      continue;
    }
    const brandConfig = getActiveProfileBrandConfig(brand.brandSlug);
    const results = { updated: [], errors: [] };
    for (const patch of brand.presentationPatches || []) {
      try {
        const fields = {
          "Slot Key": patch.slotKey,
          "Brand Name": brandConfig.name,
          Brand: [brandConfig.recordId],
          Active: true,
          "Sort Order": Number(String(patch.slotKey).match(/(\d+)$/)?.[1] || 0) || 0,
          ...patch.fields,
        };
        await airtablePatch({ baseId, apiKey, recordId: patch.recordId, fields });
        results.updated.push({ recordId: patch.recordId, slotKey: patch.slotKey });
      } catch (err) {
        results.errors.push({ slotKey: patch.slotKey, error: err.message });
      }
    }
    resultsByBrand[brand.brandSlug] = {
      applied: results.errors.length === 0,
      ...results,
    };
  }
  return { applied: true, flagCheck, resultsByBrand };
}

export async function runImageRoleMatchRemediation({
  brands = TARGET_BRANDS,
  dryRun = true,
  argv = [],
} = {}) {
  for (const slug of brands) {
    if (PROTECTED_BRANDS.includes(slug) && !TARGET_BRANDS.includes(slug)) {
      throw new Error(`Protected brand ${slug} cannot be rematerialized by this tool`);
    }
  }

  const brandResults = [];
  for (const slug of brands) {
    brandResults.push(await planBrandImageRoleMatchRemediation(slug));
  }

  const applyResult = dryRun
    ? await applyImageRoleMatchRemediation({ brandResults, apply: false, argv })
    : await applyImageRoleMatchRemediation({ brandResults, apply: true, argv });

  return {
    version: ROLE_MATCH_REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    brands,
    brandResults,
    applyResult,
    summary: {
      brandCount: brandResults.length,
      canApplyCount: brandResults.filter((b) => b.canApply).length,
      projectedRolePass: brandResults.filter((b) => b.projected?.roleMatchPass).length,
      applied: applyResult.applied === true,
    },
    auditPass: brandResults.every((b) => b.canApply && b.projected?.roleMatchPass),
  };
}

export function writeImageRoleMatchRemediationReports(
  report,
  reportsDir = path.join(ROOT, "reports")
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const lines = [];
  lines.push("# Brand Explorer Image Role-Match Remediation");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`dryRun: **${report.dryRun}**`);
  lines.push(`auditPass (projected): **${report.auditPass}**`);
  lines.push("");
  for (const b of report.brandResults) {
    lines.push(`### ${b.brandSlug}`);
    lines.push(
      `- before unresolved=${b.before.unresolvedRoleMismatchCount} → projected rolePass=${b.projected.roleMatchPass} uniquenessPass=${b.projected.uniquenessPass}`
    );
    lines.push(`- canApply=${b.canApply} strategy=${b.strategy}`);
    lines.push("");
    lines.push("| Slot | Role | Title | Detected | Status | Image |");
    lines.push("| --- | --- | --- | --- | --- | --- |");
    for (const a of b.assignments) {
      lines.push(
        `| ${a.slotKey} | ${a.role} | ${(a.title || "").slice(0, 50)} | ${a.detectedVisualCategory || "—"} | ${a.status} | ${(a.imageUrl || "—").slice(0, 70)} |`
      );
    }
    lines.push("");
  }
  fs.writeFileSync(mdPath, lines.join("\n"));

  const brandPaths = {};
  for (const b of report.brandResults) {
    if (b.brandSlug !== "mgallery-collection") continue;
    const p = path.join(reportsDir, REPORT_MGALLERY_MD);
    fs.writeFileSync(p, lines.join("\n"));
    brandPaths[b.brandSlug] = p;
  }
  return { jsonPath, mdPath, brandPaths };
}
