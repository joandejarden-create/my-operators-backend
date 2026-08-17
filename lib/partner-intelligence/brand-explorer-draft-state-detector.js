/**
 * Brand Explorer v36C — draft state detector (read-only).
 *
 * Distinguishes draft-not-applied vs draft-applied-with-defects vs active-ready.
 */
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";

export const DRAFT_STATE_DETECTOR_VERSION = "v36C";

export const DRAFT_STATES = Object.freeze([
  "not_started",
  "sources_seeded",
  "asset_pack_ready",
  "draft_not_applied",
  "draft_applied",
  "draft_applied_with_defects",
  "remediation_required",
  "founder_visual_review_ready",
  "active_approval_ready",
  "active_profile_ready",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function countVisibleRows(presentationRows) {
  return (presentationRows || []).filter((r) => r.visible !== false && r.active !== false).length;
}

function countGalleryWithImageUrl(presentationRows, apiBlocks) {
  const blocks = apiBlocks || [];
  const fromApi = blocks.filter(
    (b) => /^materials\.gallery\.\d+$/.test(b.slotKey) && nz(b.imageUrl)
  ).length;
  if (fromApi) return fromApi;
  return (presentationRows || []).filter(
    (r) => /^materials\.gallery\.\d+$/.test(r.slotKey) && nz(r.imageUrl)
  ).length;
}

function draftMaterializationSignals(presentationRows, draftPlan) {
  const patches = draftPlan?.presentationPatches || [];
  const rows = presentationRows || [];
  let noopPatches = 0;
  let pendingPatches = 0;

  for (const patch of patches) {
    const row = rows.find((r) => r.recordId === patch.recordId || r.slotKey === patch.slotKey);
    const wantsImage = Boolean(patch.fields?.Image);
    const hasImage = Boolean(row?.imageUrl);
    if (wantsImage && hasImage) noopPatches += 1;
    else if (patch.reason) pendingPatches += 1;
  }

  const factoryTaggedRows = rows.filter(
    (r) =>
      /v34[b-d]|v35[f-z]|active_profile|factory draft|generic factory/i.test(
        `${r.title}\n${r.body}`
      ) || (r.externalDisplayStatus && r.externalDisplayStatus !== "Do Not Display")
  ).length;

  return { noopPatches, pendingPatches, factoryTaggedRows, totalPatches: patches.length };
}

/**
 * @param {object} ctx
 * @param {string} ctx.brandSlug
 * @param {object[]} ctx.presentationRows
 * @param {object} ctx.brandApi
 * @param {object} ctx.assetPack
 * @param {object} ctx.draftPlan
 * @param {object} ctx.factoryRules
 * @param {object} ctx.renderContract
 * @param {number} ctx.approvedSourcesCount
 * @param {object} ctx.completeBuildReport
 * @param {object} ctx.brandBasics
 */
export function detectDraftState(ctx = {}) {
  const {
    brandSlug,
    presentationRows = [],
    brandApi,
    assetPack,
    draftPlan,
    factoryRules,
    renderContract,
    approvedSourcesCount = 0,
    completeBuildReport,
    brandBasics,
  } = ctx;

  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  const visibleCount = countVisibleRows(presentationRows);
  const apiBlocks = brandApi?.brandExplorer?.blocks || [];
  const galleryVisible = countGalleryWithImageUrl(presentationRows, apiBlocks);
  const galleryMin = brandConfig?.galleryMinimum || 6;
  const matSignals = draftMaterializationSignals(presentationRows, draftPlan);
  const readyForActiveProfile =
    completeBuildReport?.readyForActiveProfile ??
    completeBuildReport?.brandResults?.find((b) => b.brand?.slug === brandSlug)?.readyForActiveProfile ??
    Boolean(brandBasics?.["Ready for Active Profile"]);

  const assetPackReady =
    assetPack?.summary?.readinessBand === "full" ||
    assetPack?.summary?.readinessBand === "partial" ||
    assetPack?.summary?.canProceedWithConfigAndAssetPack;

  const draftPlanHasPatches = (draftPlan?.presentationPatches?.length || 0) > 0;
  const factoryPass = factoryRules?.pass === true;
  const renderPass = renderContract?.pass === true;
  const hasDefects = !factoryPass || !renderPass;

  const draftLikelyApplied =
    visibleCount >= 40 ||
    galleryVisible >= galleryMin ||
    matSignals.noopPatches >= 3 ||
    (galleryVisible > 0 && matSignals.pendingPatches < matSignals.totalPatches / 2);

  const draftNotApplied =
    draftPlanHasPatches &&
    matSignals.pendingPatches >= Math.max(3, Math.floor(matSignals.totalPatches * 0.5)) &&
    galleryVisible < galleryMin;

  let primaryState = "not_started";
  const secondaryStates = [];
  let readyForApplyDraft = false;

  if (readyForActiveProfile) {
    primaryState = "active_profile_ready";
  } else if (approvedSourcesCount < 3 && visibleCount < 10) {
    primaryState = visibleCount > 0 ? "sources_seeded" : "not_started";
  } else if (approvedSourcesCount >= 3 && !assetPackReady && visibleCount < 20) {
    primaryState = "sources_seeded";
  } else if (assetPackReady && draftNotApplied) {
    primaryState = "draft_not_applied";
    readyForApplyDraft = draftPlanHasPatches && approvedSourcesCount >= 3;
  } else if (draftLikelyApplied && hasDefects) {
    primaryState = "draft_applied_with_defects";
    secondaryStates.push("remediation_required");
    readyForApplyDraft = false;
  } else if (draftLikelyApplied && !hasDefects && factoryPass) {
    primaryState = "founder_visual_review_ready";
    secondaryStates.push("draft_applied");
  } else if (draftLikelyApplied) {
    primaryState = "draft_applied";
    if (hasDefects) secondaryStates.push("remediation_required");
  } else if (assetPackReady && draftPlanHasPatches) {
    primaryState = "asset_pack_ready";
    readyForApplyDraft = true;
  } else if (approvedSourcesCount >= 3) {
    primaryState = "sources_seeded";
  }

  if (hasDefects && primaryState !== "draft_not_applied" && primaryState !== "not_started") {
    if (!secondaryStates.includes("remediation_required")) {
      secondaryStates.push("remediation_required");
    }
  }

  if (
    factoryPass &&
    renderPass &&
    !readyForActiveProfile &&
    primaryState === "founder_visual_review_ready"
  ) {
    secondaryStates.push("active_approval_ready");
  }

  return {
    version: DRAFT_STATE_DETECTOR_VERSION,
    brandSlug,
    primaryState,
    secondaryStates: [...new Set(secondaryStates)],
    readyForApplyDraft,
    signals: {
      visiblePresentationRows: visibleCount,
      approvedSourcesCount,
      galleryVisibleInApi: galleryVisible,
      galleryMinimum: galleryMin,
      assetPackReadinessBand: assetPack?.summary?.readinessBand || null,
      draftPatchCount: matSignals.totalPatches,
      draftPendingPatches: matSignals.pendingPatches,
      draftNoopPatches: matSignals.noopPatches,
      factoryPass,
      renderPass,
      readyForActiveProfile,
      draftLikelyApplied,
      draftNotApplied,
    },
    interpretation:
      draftLikelyApplied && readyForApplyDraft
        ? "Live rows suggest prior draft apply — do not recommend apply-draft until remediation completes"
        : draftNotApplied
          ? "Draft plan exists but live API/materialization incomplete — apply-draft may be next step"
          : null,
  };
}

export function draftStateBlocksApplyDraft(draftState) {
  if (!draftState) return true;
  const blocked = [
    "draft_applied",
    "draft_applied_with_defects",
    "remediation_required",
    "founder_visual_review_ready",
    "active_approval_ready",
    "active_profile_ready",
  ];
  return (
    blocked.includes(draftState.primaryState) ||
    draftState.secondaryStates?.includes("remediation_required") ||
    draftState.signals?.draftLikelyApplied
  );
}
