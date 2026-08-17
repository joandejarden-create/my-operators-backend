/**
 * Brand Explorer Active Profile Draft Builder v34B.
 *
 * Converts asset pack into dry-run presentation/registry patch proposals.
 */
import { scanCopySafety } from "./brand-explorer-choice-expansion-partial-profile-backfill-writer.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import {
  buildCalaPropertyOpeningCopy,
} from "./brand-explorer-cala-property-example-rules.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  applyRegistryRecords,
} from "./brand-asset-registry-workflow.js";
import {
  ASSET_STATUS,
  ASSET_TYPE,
  SOURCE_BASIS,
} from "./brand-asset-pr-package-governance.js";

export const DRAFT_BUILDER_VERSION = "v34B";

const OPENINGS_SLOT = "footprint.openings";
const HIDE_DISPLAY = "Do Not Display";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeUrlKey(url) {
  return nz(url).toLowerCase().replace(/\?.*$/, "");
}

function buildPropertyOpeningTitle(catalog) {
  return `${catalog.propertyName} — U.S. Property Example`;
}

function buildPropertyOpeningCopy(catalog, brandConfig = null) {
  const rule = brandConfig?.geographicFallbackRule || "";
  if (rule.includes("cala_first")) {
    return buildCalaPropertyOpeningCopy(catalog, {
      sectionLabel: brandConfig?.propertyExampleSectionLabel,
    });
  }
  return {
    title: buildPropertyOpeningTitle(catalog),
    body: catalog.teaser || "",
    meta: catalog.meta || "U.S. Property Example",
    chips: catalog.chips || "",
    scenario: catalog.scenario || "PROPERTY EXAMPLE",
  };
}

function sanitizeCopy(text, disallowedTerms = []) {
  let out = nz(text);
  for (const term of disallowedTerms) {
    const re = new RegExp(`\\b${term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "gi");
    out = out.replace(re, "").replace(/\s{2,}/g, " ").trim();
  }
  return out;
}

function buildGalleryPresentationPatch({ row, asset, title }) {
  if (!asset.imageUrl) return null;
  const spaceLabel = title || row?.title || asset.intendedSlot;
  const propertyName = nz(asset.propertyName);
  const galleryTitle =
    propertyName && spaceLabel && !spaceLabel.includes(propertyName)
      ? `${spaceLabel} - ${propertyName}`
      : spaceLabel || row?.title || asset.intendedSlot;
  const fields = {
    Title: galleryTitle,
    Image: [{ url: asset.imageUrl }],
    "External Display Status": null,
  };
  const needsPatch =
    !row?.visible ||
    normalizeUrlKey(row?.imageUrl) !== normalizeUrlKey(asset.imageUrl) ||
    !row?.imageUrl;
  if (!needsPatch) return null;
  return {
    recordId: row?.recordId,
    slotKey: asset.slotKey,
    fields,
    reason: "active_profile_gallery_materialization",
    imageSourcePageUrl: asset.sourcePageUrl,
  };
}

function buildPropertyPresentationPatch({ row, catalog, asset, brandConfig = null }) {
  if (!asset.imageUrl) return null;
  const copy = buildPropertyOpeningCopy(catalog, brandConfig);
  const fields = {
    Title: copy.title,
    Body: copy.body,
    Image: [{ url: asset.imageUrl }],
    "External Display Status": null,
  };
  const needsPatch =
    !row?.recordId ||
    normalizeUrlKey(row?.imageUrl) !== normalizeUrlKey(asset.imageUrl) ||
    !row?.imageUrl;
  if (!needsPatch) return null;
  return {
    recordId: row?.recordId || null,
    slotKey: OPENINGS_SLOT,
    fields,
    reason: "active_profile_property_example",
    createIfMissing: !row?.recordId,
    catalogPropertyKey: catalog.propertyKey,
    imageSourcePageUrl: catalog.sourcePageUrl,
  };
}

function buildScenarioPresentationPatch({ row, asset, copyPackage, disallowedTerms }) {
  const title = sanitizeCopy(copyPackage?.title || row?.title || "", disallowedTerms);
  const body = sanitizeCopy(copyPackage?.body || row?.body || "", disallowedTerms);
  const fields = {
    Title: title,
    Body: body,
    "External Display Status": null,
  };
  if (asset.imageUrl) {
    fields.Image = [{ url: asset.imageUrl }];
  } else {
    fields["External Display Status"] = HIDE_DISPLAY;
  }
  const riskyCopyRemoved = [];
  const safetyIds = scanCopySafety(`${title}\n${body}`);
  for (const id of safetyIds) riskyCopyRemoved.push(id);

  return {
    recordId: row?.recordId || null,
    slotKey: asset.slotKey,
    fields,
    reason: asset.imageUrl ? "active_profile_scenario_build" : "hide_scenario_without_image",
    riskyCopyRemoved,
    createIfMissing: !row?.recordId,
  };
}

function buildRegistryCreateStub({ asset, brandConfig, parentCompany, slotPurpose }) {
  if (!asset.imageUrl || isTemporaryAirtableUrl(asset.imageUrl)) return null;
  return {
    assetName: `${brandConfig.name} — ${asset.intendedSlot}`,
    brandRecordId: brandConfig.recordId,
    parentCompany,
    sourcePageUrl: asset.sourcePageUrl,
    sourceUrl: asset.imageUrl,
    recommendedExplorerSlot: asset.slotKey,
    slotPurpose,
    explorerUsePermission: "Approved For Explorer",
    usageReviewStatus: "Usage Review Complete",
    validationStatus: "Valid for Slot",
    calaRelevant: "No",
    propertyConfirmed: asset.assetRole === "property_example" ? "Yes" : "No",
    notes: `v34B generic factory draft — ${asset.assetRole}`,
  };
}

export function buildActiveProfileDraftPlan({
  brandSlug,
  assetPack,
  presentationRows = [],
  brandBasics = null,
  brandApi = null,
} = {}) {
  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig) throw new Error(`No active profile brand config for: ${brandSlug}`);
  if (!assetPack) throw new Error("assetPack is required");

  const parentCompany =
    nz(brandBasics?.fields?.["Parent Company"]) || brandConfig.parentCompany;
  const presentationPatches = [];
  const registryCreates = [];
  const visibilityPatches = [];
  const copyPatches = [];
  const riskyCopyRemoved = [];
  const pendingGovernanceGates = [];
  const sectionsPatched = new Set();

  for (const asset of assetPack.gallery) {
    const row = presentationRows.find((r) => r.slotKey === asset.slotKey);
    const title =
      brandConfig.gallerySlotTitles[
        Number(asset.slotKey.replace("materials.gallery.", "")) - 1
      ] || asset.intendedSlot;
    const patch = buildGalleryPresentationPatch({ row, asset, title });
    if (patch) {
      presentationPatches.push(patch);
      sectionsPatched.add("materials.gallery");
    }
    const reg = buildRegistryCreateStub({
      asset,
      brandConfig,
      parentCompany,
      slotPurpose: `${brandConfig.name} gallery — ${title}`,
    });
    if (reg) registryCreates.push(reg);
  }

  for (let i = 0; i < assetPack.propertyExamples.length; i += 1) {
    const asset = assetPack.propertyExamples[i];
    const catalog = brandConfig.propertyCatalog[i];
    if (!catalog) continue;
    const row =
      presentationRows.find((r) => r.recordId === catalog.presentationRecordId) ||
      presentationRows.find(
        (r) =>
          r.slotKey === OPENINGS_SLOT &&
          nz(r.title).toLowerCase().includes(nz(catalog.propertyName).toLowerCase())
      ) ||
      presentationRows.find(
        (r) =>
          r.slotKey === OPENINGS_SLOT &&
          nz(r.title).toLowerCase().includes(nz(catalog.marketCity).toLowerCase())
      ) ||
      null;
    const patch = buildPropertyPresentationPatch({ row, catalog, asset, brandConfig });
    if (patch) {
      presentationPatches.push(patch);
      sectionsPatched.add("footprint.openings");
    }
    const reg = buildRegistryCreateStub({
      asset,
      brandConfig,
      parentCompany,
      slotPurpose: brandConfig.geographicFallbackRule?.includes("cala_first")
        ? `${catalog.propertyName} CALA property example`
        : `${catalog.propertyName} U.S. property example`,
    });
    if (reg) registryCreates.push(reg);
  }

  const scenarioCopy = brandConfig.overviewScenarioCopy || {};
  for (const asset of assetPack.scenarios) {
    const row = presentationRows.find((r) => r.slotKey === asset.slotKey);
    const copyPackage = scenarioCopy[asset.slotKey] || null;
    const patch = buildScenarioPresentationPatch({
      row,
      asset,
      copyPackage,
      disallowedTerms: brandConfig.disallowedCopyTerms,
    });
    if (patch) {
      presentationPatches.push(patch);
      sectionsPatched.add("overview.scenario");
      riskyCopyRemoved.push(...(patch.riskyCopyRemoved || []));
      if (patch.fields["External Display Status"] === HIDE_DISPLAY) {
        visibilityPatches.push({
          recordId: row?.recordId,
          slotKey: asset.slotKey,
          action: "hide",
          reason: "no_scenario_image_source",
        });
      }
    }
    if (copyPackage) {
      copyPatches.push({ slotKey: asset.slotKey, title: copyPackage.title, source: "brand_config" });
    }
  }

  if (brandConfig.standardDetailGovernanceRequired) {
    pendingGovernanceGates.push("standard_detail_governance_review");
  }
  pendingGovernanceGates.push("founder_visual_review");

  const missingSourceEvidence = [];
  for (const asset of [...assetPack.gallery, ...assetPack.propertyExamples, ...assetPack.scenarios]) {
    if (!asset.imageUrl && asset.assetRole !== "momentum") {
      missingSourceEvidence.push({
        slotKey: asset.slotKey,
        assetRole: asset.assetRole,
        issue: asset.renderReadiness,
      });
    }
  }

  const willApply = {
    presentationPatchCount: presentationPatches.length,
    registryCreateCount: registryCreates.length,
    visibilityPatchCount: visibilityPatches.length,
    sections: [...sectionsPatched],
    slots: presentationPatches.map((p) => p.slotKey),
  };

  const remainsHumanReviewed = [
    "Company Validated field — never auto-written",
    "Summary URL field — never auto-written",
    "Standard Detail governance sign-off",
    "Founder visual review approval",
    ...missingSourceEvidence.map((m) => `${m.slotKey}: ${m.issue}`),
  ];

  return {
    draftBuilderVersion: DRAFT_BUILDER_VERSION,
    brandSlug,
    mode: "dry-run",
    presentationPatches,
    registryCreates,
    registryPatches: [],
    visibilityPatches,
    copyPatches,
    riskyCopyRemoved: [...new Set(riskyCopyRemoved)],
    missingSourceEvidence,
    pendingGovernanceGates,
    willApply,
    remainsHumanReviewed,
    summary: {
      patchCount: presentationPatches.length,
      registryCreates: registryCreates.length,
      galleryPatches: presentationPatches.filter((p) => p.slotKey.startsWith("materials.gallery.")).length,
      propertyPatches: presentationPatches.filter((p) => p.slotKey === OPENINGS_SLOT).length,
      scenarioPatches: presentationPatches.filter((p) => p.slotKey.startsWith("overview.scenario.")).length,
      readyForDryRunReview: presentationPatches.length > 0,
    },
  };
}

function sortOrderForSlotKey(slotKey) {
  const gallery = nz(slotKey).match(/^materials\.gallery\.(\d+)$/);
  if (gallery) return Number(gallery[1]);
  const scenario = nz(slotKey).match(/^overview\.scenario\.(\d+)$/);
  if (scenario) return Number(scenario[1]);
  if (slotKey === OPENINGS_SLOT) return 10;
  return 0;
}

function buildPresentationWriteFields(patch, brandConfig) {
  return {
    "Slot Key": patch.slotKey,
    "Brand Name": brandConfig.name,
    Brand: [brandConfig.recordId],
    Active: true,
    "Sort Order": sortOrderForSlotKey(patch.slotKey),
    ...patch.fields,
  };
}

function registryStubToStaged(stub) {
  return {
    assetName: stub.assetName,
    assetType: ASSET_TYPE.PR_IMAGE,
    assetStatus: ASSET_STATUS.NEEDS_USAGE_REVIEW,
    sourceBasis: SOURCE_BASIS.RENDERED_OFFICIAL,
    sourceUrl: stub.sourceUrl,
    sourcePageUrl: stub.sourcePageUrl,
    usageReviewStatus: "Pending Review",
    explorerUsePermission: "Candidate Only",
    recommendedExplorerSlot: stub.recommendedExplorerSlot,
    isPrimaryCandidate: true,
    sourceNotes: stub.notes || stub.slotPurpose || "",
    reviewNotes: `v34B factory draft — ${stub.slotPurpose || ""}`,
  };
}

async function airtablePresentationWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`
    : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `${method} failed: ${res.status}`);
  return json;
}

export async function applyActiveProfileDraftPlan({
  draftPlan,
  apply = false,
  guardFlags = {},
  baseId,
  apiKey,
} = {}) {
  if (!apply) {
    return { applied: false, reason: "dry_run_only", preview: draftPlan.willApply };
  }

  const required = [
    "approveBrandExplorerActiveProfileDraft",
    "confirmNoCompanyValidationClaim",
    "confirmMinimumSixVisibleGalleryImages",
    "confirmPropertyExamplesHaveHotelImages",
    "confirmNoLogoLifestylePropertyImages",
    "confirmNoSummaryUrlField",
    "confirmBrandOnly",
  ];
  const missing = required.filter((k) => !guardFlags[k]);
  if (missing.length) {
    return { applied: false, reason: "missing_guard_flags", missing };
  }

  const brandConfig = getActiveProfileBrandConfig(draftPlan.brandSlug);
  if (!brandConfig) {
    return { applied: false, reason: "missing_brand_config", brandSlug: draftPlan.brandSlug };
  }

  const results = {
    presentationCreated: [],
    presentationUpdated: [],
    registryCreated: [],
    registrySkipped: [],
    errors: [],
  };
  const presentationTable = "Brand Setup - Brand Explorer Presentation";
  const parentCompany =
    nz(draftPlan.parentCompany) || brandConfig.parentCompany || "";

  for (const patch of draftPlan.presentationPatches || []) {
    const fields = buildPresentationWriteFields(patch, brandConfig);
    try {
      if (patch.recordId) {
        await airtablePresentationWrite({
          baseId,
          apiKey,
          table: presentationTable,
          recordId: patch.recordId,
          fields,
          method: "PATCH",
        });
        results.presentationUpdated.push({ recordId: patch.recordId, slotKey: patch.slotKey });
      } else {
        const json = await airtablePresentationWrite({
          baseId,
          apiKey,
          table: presentationTable,
          recordId: "",
          fields,
          method: "POST",
        });
        results.presentationCreated.push({ recordId: json.id, slotKey: patch.slotKey });
      }
      await new Promise((r) => setTimeout(r, 220));
    } catch (err) {
      results.errors.push({
        recordId: patch.recordId || null,
        slotKey: patch.slotKey,
        message: err.message,
      });
    }
  }

  const registryStaged = (draftPlan.registryCreates || []).map(registryStubToStaged);
  if (registryStaged.length) {
    try {
      const registryApply = await applyRegistryRecords({
        brandRecordId: brandConfig.recordId,
        parentCompany,
        stagedAssets: registryStaged,
        stagingRunId: "v34D-active-profile-draft-apply",
      });
      results.registryCreated = (registryApply.created || []).map((r) => r.recordId);
      results.registrySkipped = registryApply.recordsSkippedDuplicates || [];
      if (registryApply.validationFailed?.length) {
        for (const fail of registryApply.validationFailed) {
          results.errors.push({
            slotKey: fail.assetName,
            message: (fail.errors || []).join("; "),
          });
        }
      }
    } catch (err) {
      results.errors.push({ stage: "registry", message: err.message });
    }
  }

  return {
    applied: results.errors.length === 0 && results.presentationCreated.length + results.presentationUpdated.length > 0,
    results,
  };
}
