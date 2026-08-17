/**
 * Distinct Gallery Rematerialization — MGallery + SLH.
 *
 * Replaces padded/near-duplicate gallery Image attachments with genuinely
 * distinct official property photography. Dry-run by default.
 *
 * Never writes Company Validated, release/active-profile fields,
 * Source Library status, or Registry approval. Protected brands are refused.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import {
  MGALLERY_PROPERTY_CATALOG,
  SLH_PROPERTY_CATALOG,
} from "./brand-explorer-lifestyle-affiliation-property-catalog.js";
import { probePropertyPage } from "./brand-explorer-lifestyle-affiliation-source-capture-v35C.js";
import {
  buildImageIdentity,
  evaluateImageUniqueness,
  pickDistinctImageAssets,
  GALLERY_DISTINCT_MIN,
  SCENARIO_DISTINCT_MIN,
  PROPERTY_DISTINCT_MIN,
} from "./brand-explorer-image-uniqueness.js";
import { ORIGINAL_GOLDEN_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";
import { LEGACY_SEED_SLUGS } from "./brand-explorer-legacy-approved-profile-reconciliation.js";

export const DISTINCT_GALLERY_VERSION = "distinct-gallery-rematerialization-v1";

export const TARGET_BRANDS = Object.freeze([
  "mgallery-collection",
  "small-luxury-hotels-of-the-world",
]);

export const PROTECTED_BRANDS = Object.freeze([
  "hotel-indigo",
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
  "design-hotels",
  "ascend",
  "comfort",
  "comfort-inn-suites",
  "curio",
  "curio-collection",
  "tribute-portfolio",
  ...ORIGINAL_GOLDEN_RELEASE_SLUGS,
  ...LEGACY_SEED_SLUGS.filter((s) => !TARGET_BRANDS.includes(s)),
]);

export const REPORT_JSON = "brand-explorer-distinct-gallery-rematerialization.json";
export const REPORT_MD = "brand-explorer-distinct-gallery-rematerialization.md";

export const BRAND_REPORT_MD = Object.freeze({
  "mgallery-collection": "brand-explorer-distinct-gallery-rematerialization-mgallery.md",
  "small-luxury-hotels-of-the-world": "brand-explorer-distinct-gallery-rematerialization-slh.md",
});

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-distinct-gallery-rematerialization",
  "--confirm-no-company-validation-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-protected-brands-unchanged",
  "--confirm-gallery-distinct-six",
  "--confirm-scenario-distinct-three",
  "--confirm-property-distinct-three",
  "--confirm-no-slot-padding",
]);

const GALLERY_TITLES = Object.freeze([
  "Exterior / Arrival",
  "Guest Room / Suite",
  "Public Space",
  "F&B or Local Experience",
  "Design Detail",
  "Property Setting",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const OPENINGS_SLOT = "footprint.openings";
const PREFERRED_AHSTATIC_SIZE = "1024x768";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function parseDistinctGalleryApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

function preferSizedUrl(url) {
  const u = nz(url);
  if (!u) return u;
  if (/ahstatic\.com\/photos\//i.test(u)) {
    return u.replace(/_p_\d+x\d+\./i, `_p_${PREFERRED_AHSTATIC_SIZE}.`);
  }
  if (/lucidcm\.imgix\.net/i.test(u) || /slh\.com\/-\/media\//i.test(u)) {
    try {
      const parsed = new URL(u);
      if (!parsed.searchParams.get("h") || !parsed.searchParams.get("w")) {
        parsed.searchParams.set("h", "1080");
        parsed.searchParams.set("w", "1920");
      }
      return parsed.toString();
    } catch {
      return u;
    }
  }
  return u;
}

function catalogFor(slug) {
  if (slug === "mgallery-collection") {
    return MGALLERY_PROPERTY_CATALOG.filter((p) => (p.galleryPriority || 1) <= 1);
  }
  if (slug === "small-luxury-hotels-of-the-world") return [...SLH_PROPERTY_CATALOG];
  return [];
}

/**
 * Collect distinct official image candidates from property pages.
 */
export async function collectDistinctInventory(brandSlug) {
  const catalog = catalogFor(brandSlug);
  const assets = [];
  const seen = new Set();

  for (const property of catalog) {
    const probed = await probePropertyPage(property, brandSlug);
    for (const candidate of probed.imageCandidates || []) {
      const imageUrl = preferSizedUrl(candidate.imageUrl);
      const identity = buildImageIdentity(imageUrl, {
        propertyName: property.propertyName,
        filename: imageUrl.split("/").pop(),
      });
      if (!identity.duplicateGroupId || seen.has(identity.duplicateGroupId)) continue;
      // Skip tiny Accor thumbs already collapsed by identity, but prefer 1024 only
      if (/ahstatic\.com\/photos\//i.test(imageUrl) && !/_p_1024x768\./i.test(imageUrl)) continue;
      // Prefer XL/canonical SLH media over tiny S crops when both exist
      if (/\/slh\/hotels\/.\/s\//i.test(imageUrl)) continue;

      seen.add(identity.duplicateGroupId);
      assets.push({
        imageUrl,
        sourcePageUrl: property.sourcePageUrl,
        propertyName: property.propertyName,
        propertyKey: property.propertyKey,
        geographyLabel: property.geographyLabel,
        sourceImageId: identity.sourceImageId,
        duplicateGroupId: identity.duplicateGroupId,
        accepted: true,
      });
    }
  }

  return {
    brandSlug,
    assetCount: assets.length,
    distinctCount: assets.length,
    assets,
  };
}

function internationalTitle(spaceLabel, property, geographyLabel) {
  const geo = nz(geographyLabel);
  const isIntl = /global|europe|portugal|france|international/i.test(geo);
  const name = nz(property);
  if (isIntl && name) return `${spaceLabel} - ${name} (International Reference)`;
  if (name) return `${spaceLabel} - ${name}`;
  return spaceLabel;
}

function planGalleryAssignments(brandSlug, inventoryAssets, presentationRows) {
  const picked = pickDistinctImageAssets(inventoryAssets, GALLERY_DISTINCT_MIN);
  const assignments = [];
  for (let i = 0; i < GALLERY_DISTINCT_MIN; i++) {
    const slotKey = `materials.gallery.${i + 1}`;
    const spaceLabel = GALLERY_TITLES[i];
    const asset = picked[i] || null;
    const row = presentationRows.find((r) => nz(r.slotKey) === slotKey) || null;
    const currentUrl = nz(row?.imageUrl);
    const currentId = currentUrl
      ? buildImageIdentity(currentUrl, {
          slotKey,
          title: row?.title,
          filename: row?.imageFilename,
        })
      : null;

    if (!asset) {
      assignments.push({
        slotKey,
        spaceLabel,
        recordId: row?.recordId || null,
        currentImageUrl: currentUrl || null,
        currentGroupId: currentId?.duplicateGroupId || null,
        proposedImageUrl: null,
        proposedGroupId: null,
        propertyName: null,
        title: spaceLabel,
        status: "missing_distinct_inventory",
        requiredFix: "source_acquisition_required",
      });
      continue;
    }

    const title = internationalTitle(spaceLabel, asset.propertyName, asset.geographyLabel);
    assignments.push({
      slotKey,
      spaceLabel,
      recordId: row?.recordId || null,
      currentImageUrl: currentUrl || null,
      currentGroupId: currentId?.duplicateGroupId || null,
      proposedImageUrl: asset.imageUrl,
      proposedGroupId: asset.duplicateGroupId,
      propertyName: asset.propertyName,
      propertyKey: asset.propertyKey,
      sourcePageUrl: asset.sourcePageUrl,
      geographyLabel: asset.geographyLabel,
      title,
      status: "ready_to_assign",
      requiredFix: "reassign_distinct_gallery_image",
      fields: {
        Title: title,
        Image: [{ url: asset.imageUrl }],
      },
    });
  }
  return { assignments, picked };
}

function planPropertyAssignments(brandSlug, inventoryAssets, presentationRows, galleryGroupIds) {
  const openings = presentationRows.filter((r) => nz(r.slotKey) === OPENINGS_SLOT);
  const remaining = inventoryAssets.filter((a) => !galleryGroupIds.has(a.duplicateGroupId));
  const pool = remaining.length >= PROPERTY_DISTINCT_MIN ? remaining : inventoryAssets;
  const picked = pickDistinctImageAssets(pool, PROPERTY_DISTINCT_MIN);

  return openings.slice(0, PROPERTY_DISTINCT_MIN).map((row, i) => {
    const asset = picked[i] || null;
    const currentId = buildImageIdentity(row.imageUrl, {
      slotKey: OPENINGS_SLOT,
      title: row.title,
      filename: row.imageFilename,
      recordId: row.recordId,
    });
    if (!asset) {
      return {
        slotKey: OPENINGS_SLOT,
        recordId: row.recordId,
        title: row.title,
        currentImageUrl: row.imageUrl,
        currentGroupId: currentId.duplicateGroupId,
        proposedImageUrl: null,
        status: "missing_distinct_inventory",
        requiredFix: "source_acquisition_required",
      };
    }
    return {
      slotKey: OPENINGS_SLOT,
      recordId: row.recordId,
      title: row.title,
      currentImageUrl: row.imageUrl,
      currentGroupId: currentId.duplicateGroupId,
      proposedImageUrl: asset.imageUrl,
      proposedGroupId: asset.duplicateGroupId,
      propertyName: asset.propertyName,
      sourcePageUrl: asset.sourcePageUrl,
      status: "ready_to_assign",
      requiredFix: "reassign_distinct_property_image",
      fields: {
        Image: [{ url: asset.imageUrl }],
      },
    };
  });
}

function projectUniqueness({ galleryAssignments, propertyAssignments, presentationRows }) {
  const projectedRows = [];
  for (const g of galleryAssignments) {
    projectedRows.push({
      slotKey: g.slotKey,
      title: g.title,
      imageUrl: g.proposedImageUrl || g.currentImageUrl,
      imageFilename: (g.proposedImageUrl || g.currentImageUrl || "").split("/").pop(),
      recordId: g.recordId,
    });
  }
  for (const row of presentationRows) {
    if (/^overview\.scenario\.\d+$/.test(nz(row.slotKey))) {
      projectedRows.push({
        slotKey: row.slotKey,
        title: row.title,
        imageUrl: row.imageUrl,
        imageFilename: row.imageFilename,
        recordId: row.recordId,
      });
    }
  }
  for (const p of propertyAssignments) {
    projectedRows.push({
      slotKey: OPENINGS_SLOT,
      title: p.title,
      imageUrl: p.proposedImageUrl || p.currentImageUrl,
      imageFilename: (p.proposedImageUrl || p.currentImageUrl || "").split("/").pop(),
      recordId: p.recordId,
    });
  }
  // Keep any leftover openings not remapped
  const mappedIds = new Set(propertyAssignments.map((p) => p.recordId).filter(Boolean));
  for (const row of presentationRows) {
    if (nz(row.slotKey) === OPENINGS_SLOT && row.recordId && !mappedIds.has(row.recordId)) {
      projectedRows.push(row);
    }
  }

  return evaluateImageUniqueness({ brandSlug: "projection", presentationRows: projectedRows });
}

function buildSourceAcquisitionPlan(brandSlug, galleryAssignments, propertyAssignments, uniqueness) {
  const missing = [];
  for (const g of galleryAssignments) {
    if (!g.proposedImageUrl) {
      missing.push({
        brandSlug,
        section: "gallery",
        slotKey: g.slotKey,
        spaceLabel: g.spaceLabel,
        need: "distinct_official_property_photograph",
      });
    }
  }
  for (const p of propertyAssignments) {
    if (!p.proposedImageUrl) {
      missing.push({
        brandSlug,
        section: "property_example",
        slotKey: p.slotKey,
        need: "distinct_property_specific_photograph",
      });
    }
  }
  if (uniqueness.galleryDistinctCount < GALLERY_DISTINCT_MIN) {
    missing.push({
      brandSlug,
      section: "gallery",
      need: `galleryDistinct_${uniqueness.galleryDistinctCount}_lt_${GALLERY_DISTINCT_MIN}`,
    });
  }
  return missing;
}

export async function planBrandDistinctGalleryRematerialization(brandSlug) {
  if (PROTECTED_BRANDS.includes(brandSlug) && !TARGET_BRANDS.includes(brandSlug)) {
    throw new Error(`Refuse rematerialization for protected brand ${brandSlug}`);
  }
  if (!TARGET_BRANDS.includes(brandSlug)) {
    throw new Error(`Brand ${brandSlug} is not a distinct-gallery rematerialization target`);
  }

  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig) throw new Error(`No brand config for ${brandSlug}`);

  const ctx = await loadBrandFactoryContext(brandSlug);
  const presentationRows = ctx.presentationRows || [];
  const before = evaluateImageUniqueness({
    brandSlug,
    presentationRows,
  });

  const inventory = await collectDistinctInventory(brandSlug);
  const { assignments: galleryAssignments, picked } = planGalleryAssignments(
    brandSlug,
    inventory.assets,
    presentationRows
  );
  const galleryGroupIds = new Set(picked.map((a) => a.duplicateGroupId).filter(Boolean));
  const propertyAssignments =
    before.propertyExampleDistinctCount >= PROPERTY_DISTINCT_MIN
      ? presentationRows
          .filter((r) => nz(r.slotKey) === OPENINGS_SLOT)
          .slice(0, PROPERTY_DISTINCT_MIN)
          .map((row) => {
            const currentId = buildImageIdentity(row.imageUrl, {
              slotKey: OPENINGS_SLOT,
              title: row.title,
              filename: row.imageFilename,
              recordId: row.recordId,
            });
            return {
              slotKey: OPENINGS_SLOT,
              recordId: row.recordId,
              title: row.title,
              currentImageUrl: row.imageUrl,
              currentGroupId: currentId.duplicateGroupId,
              proposedImageUrl: row.imageUrl,
              proposedGroupId: currentId.duplicateGroupId,
              status: "retain_existing_distinct",
              requiredFix: "no_action",
            };
          })
      : planPropertyAssignments(brandSlug, inventory.assets, presentationRows, galleryGroupIds);

  const projected = projectUniqueness({
    galleryAssignments,
    propertyAssignments,
    presentationRows,
  });

  const sourceAcquisitionPlan = buildSourceAcquisitionPlan(
    brandSlug,
    galleryAssignments,
    propertyAssignments,
    projected
  );

  const presentationPatches = [];
  for (const g of galleryAssignments) {
    if (!g.proposedImageUrl || !g.fields) continue;
    presentationPatches.push({
      table: PRESENTATION_TABLE,
      action: g.recordId ? "PATCH" : "POST",
      recordId: g.recordId || null,
      slotKey: g.slotKey,
      brandSlug,
      fields: g.fields,
      reason: "distinct_gallery_rematerialization",
    });
  }
  for (const p of propertyAssignments) {
    if (p.status === "retain_existing_distinct") continue;
    if (!p.proposedImageUrl || !p.fields || !p.recordId) continue;
    presentationPatches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: p.recordId,
      slotKey: OPENINGS_SLOT,
      brandSlug,
      fields: p.fields,
      reason: "distinct_property_rematerialization",
    });
  }

  const canApply =
    projected.pass === true &&
    projected.galleryDistinctCount >= GALLERY_DISTINCT_MIN &&
    projected.scenarioDistinctCount >= SCENARIO_DISTINCT_MIN &&
    projected.propertyExampleDistinctCount >= PROPERTY_DISTINCT_MIN &&
    sourceAcquisitionPlan.length === 0 &&
    galleryAssignments.every((g) => g.proposedImageUrl);

  return {
    brandSlug,
    brandName: brandConfig.name,
    recordId: brandConfig.recordId,
    protected: false,
    before: {
      pass: before.pass,
      galleryDistinctCount: before.galleryDistinctCount,
      scenarioDistinctCount: before.scenarioDistinctCount,
      propertyExampleDistinctCount: before.propertyExampleDistinctCount,
      findings: before.findings || [],
    },
    inventory: {
      assetCount: inventory.assetCount,
      distinctCount: inventory.distinctCount,
      sample: inventory.assets.slice(0, 12).map((a) => ({
        propertyName: a.propertyName,
        duplicateGroupId: a.duplicateGroupId,
        imageUrl: a.imageUrl,
      })),
    },
    galleryAssignments,
    propertyAssignments,
    projected: {
      pass: projected.pass,
      galleryDistinctCount: projected.galleryDistinctCount,
      scenarioDistinctCount: projected.scenarioDistinctCount,
      propertyExampleDistinctCount: projected.propertyExampleDistinctCount,
      findings: projected.findings || [],
    },
    sourceAcquisitionPlan,
    presentationPatches,
    canApply,
    requiredAction: canApply ? "apply_distinct_gallery_rematerialization" : "image_remediation",
    companyValidatedUntouched: true,
    releaseFieldsUntouched: true,
  };
}

async function airtablePresentationWrite({ baseId, apiKey, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `${method} failed: ${res.status}`);
  return json;
}

function buildWriteFields(patch, brandConfig) {
  return {
    "Slot Key": patch.slotKey,
    "Brand Name": brandConfig.name,
    Brand: [brandConfig.recordId],
    Active: true,
    "Sort Order":
      patch.slotKey === OPENINGS_SLOT
        ? 10
        : Number(String(patch.slotKey).match(/(\d+)$/)?.[1] || 0) || 0,
    ...patch.fields,
  };
}

export async function applyDistinctGalleryRematerialization({
  brandResults,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseDistinctGalleryApplyFlags(argv);
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
      resultsByBrand[brand.brandSlug] = {
        applied: false,
        reason: "projection_not_ready",
        requiredAction: brand.requiredAction,
        sourceAcquisitionPlan: brand.sourceAcquisitionPlan,
      };
      continue;
    }

    const brandConfig = getActiveProfileBrandConfig(brand.brandSlug);
    const results = { updated: [], created: [], errors: [] };
    for (const patch of brand.presentationPatches || []) {
      try {
        const fields = buildWriteFields(patch, brandConfig);
        if (patch.recordId) {
          await airtablePresentationWrite({
            baseId,
            apiKey,
            recordId: patch.recordId,
            fields,
            method: "PATCH",
          });
          results.updated.push({ recordId: patch.recordId, slotKey: patch.slotKey });
        } else {
          const json = await airtablePresentationWrite({
            baseId,
            apiKey,
            recordId: null,
            fields,
            method: "POST",
          });
          results.created.push({ recordId: json.id, slotKey: patch.slotKey });
        }
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

export async function runDistinctGalleryRematerialization({
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
    brandResults.push(await planBrandDistinctGalleryRematerialization(slug));
  }

  const applyResult = dryRun
    ? await applyDistinctGalleryRematerialization({ brandResults, apply: false, argv })
    : await applyDistinctGalleryRematerialization({ brandResults, apply: true, argv });

  const summary = {
    brandCount: brandResults.length,
    canApplyCount: brandResults.filter((b) => b.canApply).length,
    imageRemediationCount: brandResults.filter((b) => !b.canApply).length,
    projectedPassCount: brandResults.filter((b) => b.projected?.pass).length,
    dryRun,
    applied: applyResult.applied === true,
  };

  return {
    version: DISTINCT_GALLERY_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    brands,
    protectedBrands: [...PROTECTED_BRANDS],
    brandResults,
    applyResult,
    summary,
    auditPass: summary.projectedPassCount === brandResults.length && summary.canApplyCount === brandResults.length,
  };
}

function brandMd(brand) {
  const lines = [];
  lines.push(`# Distinct Gallery Rematerialization — ${brand.brandName}`);
  lines.push("");
  lines.push(`Slug: \`${brand.brandSlug}\``);
  lines.push(`canApply: **${brand.canApply}**`);
  lines.push(
    `Before: galleryDistinct=${brand.before.galleryDistinctCount} scenario=${brand.before.scenarioDistinctCount} property=${brand.before.propertyExampleDistinctCount} pass=${brand.before.pass}`
  );
  lines.push(
    `Projected: galleryDistinct=${brand.projected.galleryDistinctCount} scenario=${brand.projected.scenarioDistinctCount} property=${brand.projected.propertyExampleDistinctCount} pass=${brand.projected.pass}`
  );
  lines.push(`Inventory distinct assets: **${brand.inventory.distinctCount}**`);
  lines.push(`Required action: \`${brand.requiredAction}\``);
  lines.push("");
  lines.push("## Gallery assignments");
  lines.push("");
  lines.push(
    "| Slot | Title | Current Group | Proposed Group | Property | Status | Proposed URL |"
  );
  lines.push("| --- | --- | --- | --- | --- | --- | --- |");
  for (const g of brand.galleryAssignments) {
    lines.push(
      `| ${g.slotKey} | ${g.title || g.spaceLabel} | ${g.currentGroupId || "—"} | ${g.proposedGroupId || "—"} | ${g.propertyName || "—"} | ${g.status} | ${(g.proposedImageUrl || "—").slice(0, 80)} |`
    );
  }
  if (brand.sourceAcquisitionPlan?.length) {
    lines.push("");
    lines.push("## Source acquisition plan");
    lines.push("");
    for (const m of brand.sourceAcquisitionPlan) {
      lines.push(`- ${m.section}${m.slotKey ? ` / ${m.slotKey}` : ""}: ${m.need}`);
    }
  }
  lines.push("");
  lines.push(`Company Validated untouched: **${brand.companyValidatedUntouched}**`);
  lines.push(`Release fields untouched: **${brand.releaseFieldsUntouched}**`);
  lines.push("");
  return lines.join("\n");
}

export function writeDistinctGalleryReports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const lines = [];
  lines.push("# Brand Explorer Distinct Gallery Rematerialization");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Version: ${report.version}`);
  lines.push(`dryRun: **${report.dryRun}**`);
  lines.push(`auditPass (projected): **${report.auditPass}**`);
  lines.push("");
  lines.push(`- Brands: **${report.summary.brandCount}**`);
  lines.push(`- Can apply: **${report.summary.canApplyCount}**`);
  lines.push(`- Still image_remediation: **${report.summary.imageRemediationCount}**`);
  lines.push("");
  for (const b of report.brandResults) {
    lines.push(`### ${b.brandSlug}`);
    lines.push(
      `- before galleryDistinct=${b.before.galleryDistinctCount} → projected **${b.projected.galleryDistinctCount}** · canApply=${b.canApply}`
    );
    lines.push(`- Per-brand: \`${BRAND_REPORT_MD[b.brandSlug]}\``);
    lines.push("");
  }
  fs.writeFileSync(mdPath, lines.join("\n"));

  const brandPaths = {};
  for (const b of report.brandResults) {
    const name = BRAND_REPORT_MD[b.brandSlug];
    if (!name) continue;
    const p = path.join(reportsDir, name);
    fs.writeFileSync(p, brandMd(b));
    brandPaths[b.brandSlug] = p;
  }

  return { jsonPath, mdPath, brandPaths };
}
