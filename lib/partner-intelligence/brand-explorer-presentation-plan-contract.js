/**
 * Brand Explorer v36B — Presentation Plan Row Contract (read-only validation).
 *
 * Validates planned presentation patches before build-draft or apply-draft.
 */
import {
  auditPresentationRowExternalOwner,
  auditExternalOwnerPhrase,
} from "./brand-explorer-external-owner-content-governance.js";
import { isGenericBoilerplate } from "./brand-explorer-active-profile-copy-governance-builder.js";
import { classifySlotKey } from "./brand-explorer-full-tab-content-contract.js";
import { evaluateRenderReadiness } from "./brand-explorer-render-readiness-contract.js";

export const PRESENTATION_PLAN_CONTRACT_VERSION = "v36B";

const FRANCHISE_MODEL_TYPES = new Set([
  "affiliation_curation_platform",
  "independent_luxury_consortium",
  "soft_brand_collection",
  "lifestyle_collection",
]);

const FRANCHISE_LANGUAGE_RE =
  /\b(franchise flag|franchise conversion|standard prototype|franchise fee schedule|franchise license fee|item\s*19|fdd|franchise disclosure)\b/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function tabFromSlot(slotKey) {
  return classifySlotKey(slotKey).tab || "Unknown";
}

function sectionFromSlot(slotKey) {
  const key = nz(slotKey);
  if (/^overview\.scenario\./.test(key)) return "Overview scenarios";
  if (/^materials\.gallery\./.test(key)) return "Gallery visuals";
  if (key === "footprint.openings") return "Property examples / openings";
  if (/^footprint\.momentum/.test(key)) return "Recent momentum";
  if (/^economics\./.test(key)) return "Economics editorial";
  if (/^commercial\./.test(key)) return "Commercial engine";
  if (/^loyalty\./.test(key)) return "Loyalty";
  if (/^standards\./.test(key)) return "Owner considerations / standards";
  if (/^operations\./.test(key)) return "Operating model";
  if (/^valueOwners\./.test(key)) return "Value to owners";
  if (/^insight\./.test(key)) return "Dealality insight";
  return key.split(".")[0] || "General";
}

function externalBodyFromPatch(patch) {
  const fields = patch.fields || {};
  return [fields.Title, fields.Body].filter(Boolean).join("\n\n");
}

function runDisallowedTermsCheck(text, { disallowedTerms = [] } = {}) {
  const hits = auditExternalOwnerPhrase(text);
  const termHits = (disallowedTerms || []).filter((term) =>
    new RegExp(`\\b${term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i").test(text)
  );
  return {
    pass: hits.filter((h) => h.severity === "critical" || h.severity === "high").length === 0 && termHits.length === 0,
    hits,
    termHits,
  };
}

function runSourceTraceabilityCheck({ sourceRecordIds = [], internalEvidenceRefs = [] } = {}) {
  return {
    pass: sourceRecordIds.length > 0 || internalEvidenceRefs.length > 0,
    sourceRecordIds,
    internalEvidenceRefs,
    note: "Internal trace required in plan row; must not appear in externalBody",
  };
}

function runBrandModelFitCheck(externalBody, brandConfig) {
  const modelType = brandConfig?.brandModelType || brandConfig?.copyGovernanceMode || "";
  const franchiseBlocked = brandConfig?.franchiseLanguageBlocked === true;
  const isAffiliation = FRANCHISE_MODEL_TYPES.has(modelType) || franchiseBlocked;
  const hasFranchiseLanguage = FRANCHISE_LANGUAGE_RE.test(externalBody);
  const brandName = brandConfig?.name || "";
  const hasBrandAnchor = brandName && externalBody.toLowerCase().includes(brandName.toLowerCase().split(" ")[0]);

  let pass = true;
  const blockers = [];
  if (isAffiliation && hasFranchiseLanguage) {
    pass = false;
    blockers.push("franchise_language_on_affiliation_brand");
  }
  if (isAffiliation && /\b(franchise flag|convert to franchise)\b/i.test(externalBody)) {
    pass = false;
    blockers.push("franchise_flag_framing");
  }
  if (!hasBrandAnchor && externalBody.length > 120 && isAffiliation) {
    blockers.push("weak_brand_anchor");
  }
  return { pass: blockers.length === 0, blockers, modelType, franchiseBlocked: isAffiliation };
}

export function buildPresentationPlanRowFromPatch(patch, ctx = {}) {
  const {
    brandSlug,
    brandRecordId,
    brandConfig = {},
    approvedSources = [],
    asset = null,
    presentationRow = null,
    apiBlock = null,
  } = ctx;

  const slotKey = patch.slotKey || nz(patch.fields?.["Slot Key"]);
  const externalBody = externalBodyFromPatch(patch);
  const sourceRecordIds = patch.sourceIds || patch.sourceRecordIds || [];
  const internalEvidenceRefs = patch.internalEvidenceRefs || patch.imageSourcePageUrl
    ? [patch.imageSourcePageUrl || patch.reason].filter(Boolean)
    : [];

  const disallowedTermsCheck = runDisallowedTermsCheck(externalBody, {
    disallowedTerms: brandConfig.disallowedCopyTerms,
  });
  const rowAudit = auditPresentationRowExternalOwner(
    {
      slotKey,
      title: patch.fields?.Title || "",
      body: patch.fields?.Body || "",
    },
    new Map(approvedSources.map((s) => [s.id || s.sourceId, s]))
  );

  const renderReadinessCheck = asset
    ? evaluateRenderReadiness(asset, { presentationRow, apiBlock })
    : { pass: !patch.fields?.Image, blockers: patch.fields?.Image ? ["image_patch_requires_render_check"] : [] };

  if (patch.fields?.Image && !presentationRow?.imageUrl) {
    renderReadinessCheck.pass = false;
    renderReadinessCheck.blockers = [...(renderReadinessCheck.blockers || []), "registry_only_until_image_materialized"];
  }

  const brandModelFitCheck = runBrandModelFitCheck(externalBody, brandConfig);
  const genericFiller = isGenericBoilerplate(externalBody, {
    brandName: brandConfig.name,
    parentPlatform: brandConfig.parentCompany,
  });

  const sourceTraceabilityCheck = runSourceTraceabilityCheck({
    sourceRecordIds,
    internalEvidenceRefs,
  });

  const founderReviewRequired =
    !renderReadinessCheck.pass ||
    !brandModelFitCheck.pass ||
    genericFiller ||
    rowAudit.hits.some((h) => h.severity === "critical");

  const blockers = [
    ...(!disallowedTermsCheck.pass ? ["disallowed_terms"] : []),
    ...(!renderReadinessCheck.pass ? renderReadinessCheck.blockers || [] : []),
    ...(!brandModelFitCheck.pass ? brandModelFitCheck.blockers : []),
    ...(genericFiller ? ["generic_filler"] : []),
    ...(rowAudit.hits.some((h) => h.patternId === "http_url" && slotKey !== "footprint.openings" && slotKey !== "footprint.momentum")
      ? ["visible_source_url"]
      : []),
    ...(rowAudit.hits.some((h) => ["sources_block", "source_line"].includes(h.patternId)) ? ["sources_block_in_body"] : []),
  ];

  return {
    brandSlug,
    brandRecordId,
    tab: tabFromSlot(slotKey),
    section: sectionFromSlot(slotKey),
    slotKey,
    title: patch.fields?.Title || "",
    externalBody,
    internalEvidenceRefs,
    sourceRecordIds,
    claimIds: patch.claimIds || [],
    visualAssetIds: patch.visualAssetIds || [],
    sortOrder: patch.fields?.["Sort Order"] ?? null,
    externalDisplayStatus: patch.fields?.["External Display Status"] ?? null,
    ownerReadinessStatus: blockers.length ? "blocked" : "ready",
    founderReviewRequired,
    reasonForFounderReview: founderReviewRequired
      ? blockers.slice(0, 3).join("; ")
      : null,
    disallowedTermsCheck,
    sourceTraceabilityCheck,
    renderReadinessCheck,
    brandModelFitCheck,
    isGenericFiller: genericFiller,
    isExternalOwnerReady: blockers.length === 0,
    blockers,
    patchMeta: {
      recordId: patch.recordId,
      reason: patch.reason,
      mode: patch.recordId ? "update" : "create",
    },
  };
}

export function validatePresentationPlanRows(planRows = []) {
  const rows = planRows || [];
  const summary = {
    total: rows.length,
    externalOwnerReady: rows.filter((r) => r.isExternalOwnerReady).length,
    founderReviewRequired: rows.filter((r) => r.founderReviewRequired).length,
    genericFiller: rows.filter((r) => r.isGenericFiller).length,
    renderBlocked: rows.filter((r) => !r.renderReadinessCheck?.pass).length,
    modelFitBlocked: rows.filter((r) => !r.brandModelFitCheck?.pass).length,
  };
  return {
    contractVersion: PRESENTATION_PLAN_CONTRACT_VERSION,
    pass: summary.externalOwnerReady === summary.total && summary.total > 0,
    summary,
    rows,
    blockers: rows.flatMap((r) => r.blockers.map((b) => `${r.slotKey}:${b}`)),
  };
}

export function buildPresentationPlanFromDraftPlan(draftPlan, ctx = {}) {
  if (!draftPlan?.presentationPatches?.length) {
    return validatePresentationPlanRows([]);
  }

  const assetBySlot = new Map();
  for (const list of [ctx.assetPack?.gallery, ctx.assetPack?.propertyExamples, ctx.assetPack?.scenarios]) {
    for (const asset of list || []) {
      assetBySlot.set(asset.slotKey, asset);
    }
  }

  const planRows = draftPlan.presentationPatches.map((patch) =>
    buildPresentationPlanRowFromPatch(patch, {
      ...ctx,
      asset: assetBySlot.get(patch.slotKey) || null,
      presentationRow: (ctx.presentationRows || []).find((r) => r.slotKey === patch.slotKey) || null,
      apiBlock: (ctx.brandApi?.brandExplorer?.blocks || []).find((b) => b.slotKey === patch.slotKey) || null,
    })
  );

  return validatePresentationPlanRows(planRows);
}

export function validateExistingPresentationRowsAsPlan(presentationRows = [], ctx = {}) {
  const rows = (presentationRows || [])
    .filter((r) => r.visible !== false)
    .map((row) =>
      buildPresentationPlanRowFromPatch(
        {
          recordId: row.recordId,
          slotKey: row.slotKey,
          fields: { Title: row.title, Body: row.body },
          reason: "existing_row_audit",
        },
        {
          ...ctx,
          presentationRow: row,
          apiBlock: (ctx.brandApi?.brandExplorer?.blocks || []).find((b) => b.slotKey === row.slotKey) || null,
        }
      )
    );
  return validatePresentationPlanRows(rows);
}
