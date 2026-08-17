/**
 * Lane 2 — Presentation Image materialization (no registry / no release fields).
 */
import path from "path";
import {
  LANE2_VERSION,
  GALLERY_MIN,
  SCENARIO_MIN,
  PROPERTY_MIN,
  resolveFullBuildSlug,
  resolveLane2BrandIdentity,
  refuseProtectedOrOutOfCohort,
  listPresentationRowsLight,
  writeLane2Reports,
  LANE2_ROOT,
  isLane2RejectedImageUrl,
} from "./brand-explorer-lane2-common.js";
import {
  buildLane2ImageAssetPackForBrand,
  readLane2ImageAssetPackReport,
  REPORT_JSON as ASSET_PACK_JSON,
} from "./brand-explorer-lane2-image-asset-pack.js";
import { BUILT_BLOCKED_PROTECTED_PUBLIC_FULL } from "./brand-explorer-built-blocked-content.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";

export const REPORT_JSON = "brand-explorer-lane2-image-materialization.json";
export const REPORT_MD = "brand-explorer-lane2-image-materialization.md";

export const APPLY_FLAG_APPROVE = "--approve-lane2-image-materialization";
export const APPLY_FLAG_NO_CV = "--confirm-no-company-validation-changes";
export const APPLY_FLAG_NO_SOURCE = "--confirm-no-source-library-status-changes";
export const APPLY_FLAG_NO_REGISTRY = "--confirm-no-registry-approval-changes";
export const APPLY_FLAG_NO_RELEASE = "--confirm-no-release-field-writes";
export const APPLY_FLAG_SIX_GALLERY = "--confirm-six-distinct-gallery-images";
export const APPLY_FLAG_THREE_SCENARIO = "--confirm-three-distinct-scenario-images";
export const APPLY_FLAG_THREE_PROPERTY = "--confirm-three-distinct-property-images";
export const APPLY_FLAG_ROLE_MATCH = "--confirm-image-role-match";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_NO_CV,
  APPLY_FLAG_NO_SOURCE,
  APPLY_FLAG_NO_REGISTRY,
  APPLY_FLAG_NO_RELEASE,
  APPLY_FLAG_SIX_GALLERY,
  APPLY_FLAG_THREE_SCENARIO,
  APPLY_FLAG_THREE_PROPERTY,
  APPLY_FLAG_ROLE_MATCH,
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const OPENINGS_SLOT = "footprint.openings";
const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

/**
 * Airtable attachment fetch is blocked/rate-limited by some official CDNs
 * (notably Marriott cache.marriott.com and Accor ahstatic). Proxy those through
 * wsrv.nl so Airtable can ingest the same official bytes.
 *
 * Hilton `hilton.com/im/` URLs:
 * - Must NOT be proxied through wsrv — weserv returns 404 for Hilton CDN hosts
 *   (Wave 15 Stage 5 incident).
 * - Bare Hilton CDN URLs return ~2KB JFIF thumbnails; append a standard
 *   impolicy crop so Airtable receives a usable ~200KB JPEG.
 */
export function toAirtableFetchableImageUrl(imageUrl) {
  const u = nz(imageUrl);
  if (!u) return u;
  if (/wsrv\.nl\/|images\.weserv\.nl\//i.test(u)) return u;

  // Hilton CDN — direct URL + size policy (no wsrv).
  if (
    /(?:www\.)?hilton\.com\/im\//i.test(u) ||
    /(?:assets\.)?hiltonstatic\.com\//i.test(u) ||
    /cache\.hilton\.com\//i.test(u)
  ) {
    if (/[?&]impolicy=/i.test(u)) return u;
    const sep = u.includes("?") ? "&" : "?";
    return `${u}${sep}impolicy=crop&cw=5000&ch=3333&gravity=NorthWest&rw=1600&rh=1067`;
  }

  // Marriott + Accor CDNs often block Airtable's attachment fetcher.
  if (/cache\.marriott\.com\//i.test(u) || /ahstatic\.com\/photos\//i.test(u)) {
    return `https://wsrv.nl/?url=${encodeURIComponent(u)}&w=1600&output=jpg`;
  }
  return u;
}

export function parseLane2MaterializationApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

function findPresentationRow(rows, { slotKey, title, planSlotKey }, { usedRecordIds = new Set() } = {}) {
  const available = (rows || []).filter((r) => !usedRecordIds.has(r.recordId));
  if (slotKey === OPENINGS_SLOT) {
    const openings = available.filter((r) => r.slotKey === OPENINGS_SLOT);
    if (title) {
      const exact = openings.find(
        (r) => nz(r.title).toLowerCase() === nz(title).toLowerCase()
      );
      if (exact) return exact;
      const partial = openings.find((r) =>
        nz(r.title)
          .toLowerCase()
          .includes(nz(title).split("—")[0].trim().toLowerCase())
      );
      if (partial) return partial;
    }
    const idx = Number(String(planSlotKey || "").match(/(\d+)$/)?.[1] || 0) - 1;
    if (idx >= 0 && openings[idx]) return openings[idx];
    return null;
  }
  return available.find((r) => r.slotKey === slotKey) || null;
}

function buildImageOnlyPatch({ asset, row }) {
  const fields = {
    Image: [{ url: toAirtableFetchableImageUrl(asset.imageUrl) }],
  };
  // Gallery captions must track the selected property/role (CALA-first + variety).
  if (String(asset.slotKey || "").startsWith("materials.gallery.") && asset.title) {
    fields.Title = nz(asset.title);
    if (asset.caption) fields.Body = nz(asset.caption);
  } else if (!row?.recordId) {
    if (asset.title) fields.Title = nz(asset.title);
    if (asset.body) fields.Body = nz(asset.body);
  } else if (asset.slotKey === OPENINGS_SLOT) {
    if (asset.title) fields.Title = nz(asset.title);
    if (asset.body) fields.Body = nz(asset.body);
  }
  // Openings modal case summaries — required to clear modal_placeholders blockers.
  if ((asset.slotKey || OPENINGS_SLOT) === OPENINGS_SLOT) {
    const overview =
      nz(asset.caseSummaryOverview) ||
      nz(asset.teaser) ||
      nz(asset.caption) ||
      nz(asset.propertyName);
    const tags = nz(asset.caseSummaryTags) || nz(asset.marketCity) || "Property example";
    if (overview) fields["Case Summary Overview"] = overview;
    if (tags) fields["Case Summary Tags"] = tags;
    if (nz(asset.caseSummaryBrandRelevance)) {
      fields["Case Summary Brand Relevance"] = nz(asset.caseSummaryBrandRelevance);
    } else if (overview) {
      fields["Case Summary Brand Relevance"] =
        "Official property photography used as a Brand Explorer property example for this collection.";
    }
    if (nz(asset.caseSummaryOwnerObjective)) {
      fields["Case Summary Owner Objective"] = nz(asset.caseSummaryOwnerObjective);
    } else if (overview) {
      fields["Case Summary Owner Objective"] =
        "Use as a directional property reference when underwriting design intensity, capital scope, and platform fit.";
    }
    if (nz(asset.caseSummaryInterpretation)) {
      fields["Case Summary Interpretation"] = nz(asset.caseSummaryInterpretation);
    } else if (overview) {
      fields["Case Summary Interpretation"] =
        "Confirm live affiliation criteria and property-specific scope with the brand before underwriting.";
    }
  }
  for (const forbidden of FORBIDDEN_WRITE_FIELDS) {
    if (fields[forbidden] != null) delete fields[forbidden];
  }
  return {
    recordId: row?.recordId || null,
    slotKey: asset.slotKey || OPENINGS_SLOT,
    planSlotKey: asset.planSlotKey || asset.slotKey,
    createIfMissing: !row?.recordId,
    fields,
    imageUrl: nz(asset.imageUrl),
    sourcePageUrl: nz(asset.sourcePageUrl),
  };
}

export async function planLane2BrandImageMaterialization(brandSlug, { assetPackBrand } = {}) {
  const refuse = refuseProtectedOrOutOfCohort(brandSlug);
  if (refuse.refused) {
    return {
      brandSlug: resolveFullBuildSlug(brandSlug),
      blocked: true,
      blockers: [refuse.reason],
      presentationPatches: [],
    };
  }

  const slug = refuse.brandSlug;
  const packRow =
    assetPackBrand ||
    buildLane2ImageAssetPackForBrand(slug);

  if (packRow.pass !== true || packRow.status !== "asset_pack_ready") {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: packRow.blockers || ["asset_pack_not_ready"],
      presentationPatches: [],
    };
  }

  const brandConfig = getActiveProfileBrandConfig(slug) || resolveLane2BrandIdentity(slug);
  let presentationRows = [];
  try {
    const fetch = await listPresentationRowsLight(brandConfig.recordId, brandConfig.name);
    presentationRows = fetch.rows;
  } catch (err) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: [`presentation_fetch_error:${err.message}`],
      presentationPatches: [],
    };
  }

  const visual = packRow.visualAssetPack || {};
  const assets = [
    ...(visual.galleryCandidates || []).map((a) => ({ ...a, kind: "gallery" })),
    ...(visual.scenarioCandidates || []).map((a) => ({ ...a, kind: "scenario" })),
    ...(visual.propertyExampleCandidates || []).map((a) => ({ ...a, kind: "property" })),
  ];

  const blockers = [];
  const patches = [];
  const usedRecordIds = new Set();

  for (const asset of assets) {
    const gate = isLane2RejectedImageUrl(asset.imageUrl, { brandSlug: slug });
    if (gate.rejected) {
      blockers.push(`${asset.slotKey || asset.planSlotKey}:${gate.reason}`);
      continue;
    }
    const row = findPresentationRow(presentationRows, asset, { usedRecordIds });
    if (row?.recordId) usedRecordIds.add(row.recordId);
    patches.push(
      buildImageOnlyPatch({
        asset,
        row,
      })
    );
  }

  const galleryCount = patches.filter((p) => String(p.slotKey).startsWith("materials.gallery.")).length;
  const scenarioCount = patches.filter((p) => String(p.slotKey).startsWith("overview.scenario.")).length;
  const propertyCount = patches.filter((p) => p.slotKey === OPENINGS_SLOT).length;

  if (galleryCount < GALLERY_MIN) blockers.push(`gallery_patches_${galleryCount}_lt_${GALLERY_MIN}`);
  if (scenarioCount < SCENARIO_MIN) blockers.push(`scenario_patches_${scenarioCount}_lt_${SCENARIO_MIN}`);
  if (propertyCount < PROPERTY_MIN) blockers.push(`property_patches_${propertyCount}_lt_${PROPERTY_MIN}`);

  return {
    brandSlug: slug,
    brandName: brandConfig.name,
    recordId: brandConfig.recordId,
    blocked: blockers.length > 0,
    blockers,
    presentationPatches: patches,
    counts: { gallery: galleryCount, scenario: scenarioCount, property: propertyCount },
    guardrails: {
      presentationImageOnly: true,
      registryWrites: false,
      sourceLibraryWrites: false,
      releaseFieldWrites: false,
      companyValidatedChanges: false,
    },
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

function buildPresentationWriteFields(patch, brandConfig) {
  const sortOrder =
    patch.slotKey === OPENINGS_SLOT
      ? 10
      : Number(String(patch.planSlotKey || patch.slotKey).match(/(\d+)$/)?.[1] || 0) || 0;
  return {
    "Slot Key": patch.slotKey,
    "Brand Name": brandConfig.name,
    Brand: [brandConfig.recordId],
    Active: true,
    "Sort Order": sortOrder,
    ...patch.fields,
  };
}

export async function applyLane2ImageMaterializationPlans({
  brandResults,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseLane2MaterializationApplyFlags(argv);
  if (!apply) {
    return { applied: false, reason: "dry_run_only", flagCheck };
  }
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of brandResults) {
    if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to protected public-full brand ${brand.brandSlug}`);
    }
    if (brand.blocked) {
      resultsByBrand[brand.brandSlug] = {
        applied: false,
        reason: "materialization_blocked",
        blockers: brand.blockers,
      };
      continue;
    }

    const brandConfig =
      getActiveProfileBrandConfig(brand.brandSlug) || resolveLane2BrandIdentity(brand.brandSlug);
    if (!brandConfig?.recordId || !brandConfig?.name) {
      resultsByBrand[brand.brandSlug] = {
        applied: false,
        reason: "missing_brand_identity",
      };
      continue;
    }
    const results = { presentationCreated: [], presentationUpdated: [], errors: [] };

    for (const patch of brand.presentationPatches || []) {
      const fields = buildPresentationWriteFields(patch, brandConfig);
      for (const forbidden of FORBIDDEN_WRITE_FIELDS) {
        if (fields[forbidden] != null) delete fields[forbidden];
      }
      try {
        let recordId = patch.recordId;
        if (recordId) {
          // Two-step: metadata first (without Image), then Image-only — Airtable
          // attachment URL fetch is more reliable as a dedicated PATCH.
          const meta = { ...fields };
          delete meta.Image;
          if (Object.keys(meta).length) {
            await airtablePresentationWrite({
              baseId,
              apiKey,
              recordId,
              fields: meta,
              method: "PATCH",
            });
          }
          if (fields.Image) {
            await airtablePresentationWrite({
              baseId,
              apiKey,
              recordId,
              fields: { Image: fields.Image },
              method: "PATCH",
            });
          }
          results.presentationUpdated.push({
            recordId,
            slotKey: patch.slotKey,
            planSlotKey: patch.planSlotKey,
          });
        } else {
          const createFields = { ...fields };
          delete createFields.Image;
          const json = await airtablePresentationWrite({
            baseId,
            apiKey,
            recordId: "",
            fields: createFields,
            method: "POST",
          });
          recordId = json.id;
          if (fields.Image) {
            await airtablePresentationWrite({
              baseId,
              apiKey,
              recordId,
              fields: { Image: fields.Image },
              method: "PATCH",
            });
          }
          results.presentationCreated.push({
            recordId,
            slotKey: patch.slotKey,
            planSlotKey: patch.planSlotKey,
          });
        }
        await new Promise((r) => setTimeout(r, 280));
      } catch (err) {
        results.errors.push({
          recordId: patch.recordId || null,
          slotKey: patch.planSlotKey || patch.slotKey,
          message: err.message,
        });
      }
    }

    resultsByBrand[brand.brandSlug] = {
      applied:
        results.errors.length === 0 &&
        results.presentationCreated.length + results.presentationUpdated.length > 0,
      results,
    };
  }

  return { applied: true, resultsByBrand, flagCheck };
}

export async function runLane2ImageMaterialization({
  brands = [],
  dryRun = true,
  argv = [],
  reportsDir = path.join(LANE2_ROOT, "reports"),
  useCachedAssetPack = true,
} = {}) {
  let assetPackReport = null;
  if (useCachedAssetPack) {
    try {
      assetPackReport = readLane2ImageAssetPackReport(reportsDir);
    } catch {
      assetPackReport = null;
    }
  }

  const bySlug = {};
  for (const row of assetPackReport?.brandResults || []) {
    bySlug[row.brandSlug] = row;
  }

  const brandResults = [];
  for (const raw of brands) {
    const slug = refuseProtectedOrOutOfCohort(raw).brandSlug || raw;
    brandResults.push(
      await planLane2BrandImageMaterialization(slug, {
        assetPackBrand: bySlug[slug] || buildLane2ImageAssetPackForBrand(slug),
      })
    );
  }

  const applyResult = dryRun
    ? { applied: false, reason: "dry_run_only" }
    : await applyLane2ImageMaterializationPlans({ brandResults, apply: true, argv });

  const result = {
    version: LANE2_VERSION,
    lane: "image-materialization",
    generatedAt: new Date().toISOString(),
    dryRun,
    brands: brandResults.map((b) => b.brandSlug),
    assetPackReport: assetPackReport ? ASSET_PACK_JSON : null,
    brandResults,
    applyResult,
    requiredApplyFlags: REQUIRED_APPLY_FLAGS,
    summary: {
      plannedBrands: brandResults.length,
      blocked: brandResults.filter((b) => b.blocked).length,
      patchCount: brandResults.reduce((n, b) => n + (b.presentationPatches?.length || 0), 0),
      applied: applyResult.applied === true,
    },
  };

  const md = [
    `# Lane 2 — Image materialization`,
    ``,
    `- Generated: ${result.generatedAt}`,
    `- Mode: **${dryRun ? "dry-run" : "APPLY"}**`,
    `- Patches planned: ${result.summary.patchCount}`,
    `- Blocked brands: ${brandResults.filter((b) => b.blocked).map((b) => b.brandSlug).join(", ") || "—"}`,
    ``,
    `## Apply flags`,
    ``,
    ...REQUIRED_APPLY_FLAGS.map((f) => `- \`${f}\``),
    ``,
  ];

  writeLane2Reports({
    jsonPath: path.join(reportsDir, REPORT_JSON),
    mdPath: path.join(reportsDir, REPORT_MD),
    json: result,
    mdLines: md,
  });

  return result;
}

/** Optional founder-packet helper (report-only stub for a future 4th lane). */
export function planLane2FounderPacketGeneration({ brands = [] } = {}) {
  return {
    lane: "founder-packet-generation",
    status: "not_implemented",
    brands,
    note: "TODO: wire founder review packet export after post-draft integrity + image materialization pass.",
  };
}
