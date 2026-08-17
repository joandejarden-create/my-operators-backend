/**

 * Brand Explorer Active Profile Factory v34B — orchestrated stages.

 *

 * Stages: preflight → asset-pack → build-draft → copy-governance → apply-draft → founder-review → apply-approved → final-qa

 */

import fs from "fs";

import path from "path";

import { fileURLToPath } from "url";

import { getBrandLibraryBrandById } from "../../api/brand-library.js";

import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";

import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";

import { normalizeRegistryRecordExtended } from "./brand-explorer-radisson-individuals-asset-registry-normalization-writer.js";

import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";

import { resolveBrandTarget } from "./brand-explorer-brand-target-resolver.js";

import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";

import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";

import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

import { buildBrandExplorerRequiredSectionPopulationContractReport } from "./brand-explorer-required-section-population-contract.js";

import {

  FACTORY_VERSION,

  FACTORY_GUARD_FLAGS,

  FACTORY_SUPPORTED_SLUGS,

  ACTIVE_PROFILE_GALLERY_MINIMUM,

  evaluateAllFactoryRules,

  factoryGuardrailsSummary,

  resolveFactoryBrand,

} from "./brand-explorer-active-profile-factory-rules.js";

import {

  getActiveProfileBrandConfig,

  BRAND_CONFIG_VERSION,

} from "./brand-explorer-active-profile-brand-config.js";

import { buildActiveProfileAssetPack } from "./brand-explorer-active-profile-asset-pack-builder.js";

import {

  applyActiveProfileDraftPlan,

  buildActiveProfileDraftPlan,

} from "./brand-explorer-active-profile-draft-builder.js";

import {

  applyCopyGovernancePlan,

  buildCopyGovernanceMarkdown,

  buildCopyGovernancePlan,

  COPY_GOVERNANCE_VERSION,

} from "./brand-explorer-active-profile-copy-governance-builder.js";

import { buildFounderQueueAuditMarkdown } from "./brand-explorer-active-profile-copy-governance-queue-resolver.js";

import { fetchAndResolveApprovedBrandSources, resolveApprovedBrandSources } from "./brand-source-auto-resolver.js";

import {
  STAGED_APPLY_VERSION,
  buildDraftApplyCommand,
  buildActiveApprovalCommand,
  validateDraftApplyRequest,
  validateActiveApprovalRequest,
  evaluateFounderVisualReview,
  buildFounderVisualReviewMarkdown,
  buildPostDraftApplySummary,
} from "./brand-explorer-active-profile-staged-apply.js";



export {

  FACTORY_VERSION,

  FACTORY_GUARD_FLAGS,

  FACTORY_SUPPORTED_SLUGS,

  ACTIVE_PROFILE_GALLERY_MINIMUM,

  factoryGuardrailsSummary,

  resolveFactoryBrand,

  loadBrandFactoryContext,

};



export const REPORT_BASENAME = "brand-explorer-active-profile-factory";

export const DOC_MD_NAME = "brand-explorer-active-profile-staged-apply-v34D.md";



const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ROOT = path.resolve(__dirname, "../..");



/** Legacy writer chains — reference only; generic factory is primary path. */

const LEGACY_WRITER_CHAINS = Object.freeze({

  "woodspring-suites": [

    "brand-explorer-woodspring-six-image-gallery-completion-writer",

    "brand-explorer-woodspring-real-property-examples-writer",

  ],

  "everhome-suites": ["brand-explorer-everhome-active-profile-finalization-writer"],

  "suburban-studios": [

    "brand-explorer-choice-extended-stay-source-capture-writer",

    "brand-explorer-choice-expansion-partial-profile-backfill-writer",

  ],

});



function nz(v) {

  return v == null ? "" : String(v).trim();

}



function escapeFormulaValue(v) {

  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");

}



async function fetchBrandApiShape(brandRecordId) {

  const req = { query: { brandId: brandRecordId, refresh: "1" }, headers: {} };

  const res = {

    statusCode: 200,

    payload: null,

    setHeader() {},

    status(code) {

      this.statusCode = code;

      return this;

    },

    json(payload) {

      this.payload = payload;

      return this;

    },

  };

  await getBrandLibraryBrandById(req, res);

  if (res.statusCode !== 200 || !res.payload?.brand) return null;

  return res.payload.brand;

}



async function listPresentationRows(baseId, apiKey, brandRecordId, brandName) {

  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;

  const rows = [];

  let offset = "";

  do {

    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });

    if (offset) params.set("offset", offset);

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;

    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });

    const json = await res.json().catch(() => ({}));

    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);

    rows.push(...(json.records || []));

    offset = json.offset || "";

  } while (offset);

  return rows.map((rec) => {

    const f = rec.fields || {};

    const imageAtt = f.Image?.[0] || f["Scenario Image"]?.[0];

    return {

      recordId: rec.id,

      slotKey: nz(f["Slot Key"]),

      title: nz(f.Title),

      body: nz(f.Body),

      sortOrder: f["Sort Order"] ?? 0,

      active: f.Active !== false,

      externalDisplayStatus: nz(f["External Display Status"]),

      visible: f.Active !== false && !/do not display|internal only/i.test(nz(f["External Display Status"])),

      caseSummaryOverview: nz(f["Case Summary Overview"]),

      caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),

      caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),

      caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),

      caseSummaryTags: nz(f["Case Summary Tags"]),

      imageUrl: imageAtt?.url ? nz(imageAtt.url) : "",

      imageFilename: nz(imageAtt?.filename),

      registryLinkIds: Array.isArray(f["Brand Asset Registry"]) ? f["Brand Asset Registry"] : [],

    };

  });

}



async function loadBrandFactoryContext(brandArg) {

  const slug = nz(brandArg).toLowerCase();

  const activeProfileConfig = getActiveProfileBrandConfig(slug);

  const brandConfig = getDiscoveryBrandConfig(slug) || null;

  const { FACTORY_PREVIEW_CANDIDATE_IDENTITIES, getFactoryPreviewIdentity } = await import(
    "./brand-explorer-factory-preview-candidates.js"
  );
  const factoryIdentity =
    FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug] || getFactoryPreviewIdentity(brandArg);

  const brand = activeProfileConfig

    ? {

        slug: activeProfileConfig.slug,

        recordId: activeProfileConfig.recordId,

        name: activeProfileConfig.name,

        parentCompany: activeProfileConfig.parentCompany,

        consumerUrl: activeProfileConfig.consumerUrl,

      }

    : brandConfig

      ? resolveFactoryBrand(brandArg, brandConfig)

      : factoryIdentity?.recordId

        ? {
            slug: factoryIdentity.slug || slug,
            recordId: factoryIdentity.recordId,
            name: factoryIdentity.name || slug,
            parentCompany: null,
            consumerUrl: null,
          }

      : resolveFactoryBrand(brandArg);



  const baseId = process.env.AIRTABLE_BASE_ID;

  const apiKey = process.env.AIRTABLE_API_KEY;

  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");



  const brandTarget = await resolveBrandTarget(brand.recordId).catch(() => null);



  const [brandBasics, presentationRows, registryAssetsRaw, brandApi] = await Promise.all([

    fetchBrandBasics(brand.recordId),

    listPresentationRows(baseId, apiKey, brand.recordId, brand.name),

    listRegistryAssetsForBrand(brand.recordId).catch(() => []),

    fetchBrandApiShape(brand.recordId),

  ]);



  const registryAssets = registryAssetsRaw.map((rec) =>
    rec?.fields ? normalizeRegistryRecordExtended(rec) : rec
  );

  const contractReport = await buildBrandExplorerRequiredSectionPopulationContractReport({

    brandIdOrName: brand.slug,

  }).catch(() => null);

  const completeBuildReport = await buildBrandExplorerCompleteBuildOrchestratorReport({

    brandIdOrName: brand.slug,

    targetQuality: "active-profile",

  }).catch(() => null);



  const factoryRules = evaluateAllFactoryRules({

    brandApi,

    presentationRows,

    registryAssets,

    brandConfig,

    brandTarget,

    contractReport,

    completeBuildReport,

  });



  return {

    brand,

    brandConfig,

    activeProfileConfig,

    brandTarget,

    brandBasics,

    presentationRows,

    registryAssets,

    brandApi,

    contractReport,

    completeBuildReport,

    factoryRules,

    baseId,

    apiKey,

  };

}



function buildApplyCommand(brandSlug) {
  return buildActiveApprovalCommand(brandSlug);
}



function assessSuburbanReadiness(ctx, assetPack, draftPlan) {

  const summary = assetPack?.summary || {};

  let recommendation = "proceed_with_config_and_asset_pack";

  let pathRequired = "config + asset pack";



  if (summary.customCodeRequired) {

    recommendation = "custom_code_required";

    pathRequired = "custom code";

  } else if (summary.readinessBand === "blocked_no_assets") {

    recommendation = "blocked_by_assets";

    pathRequired = "config + asset pack (assets missing)";

  } else if (summary.partialReadiness) {

    recommendation = "proceed_partial_readiness";

    pathRequired = "config + asset pack (partial — fewer than minimum examples allowed)";

  } else if (summary.readinessBand === "full") {

    recommendation = "proceed";

    pathRequired = "config + asset pack";

  }



  return {

    canProceedThroughFactory: !summary.customCodeRequired,

    recommendation,

    pathRequired,

    configOnly: false,

    configPlusAssetPack: summary.canProceedWithConfigAndAssetPack,

    customCodeRequired: summary.customCodeRequired,

    blockers: ctx.factoryRules.blockers,

    assetPackSummary: summary,

    draftPatchCount: draftPlan?.summary?.patchCount || 0,

    legacyWritersOptional: LEGACY_WRITER_CHAINS["suburban-studios"].filter((w) => writerExists(w)),

  };

}



function writerExists(scriptName) {

  try {

    const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, "package.json"), "utf8"));

    return Boolean(pkg.scripts?.[scriptName]);

  } catch {

    return false;

  }

}



function buildFounderReviewMarkdown(ctx, assetPack, draftPlan, copyGovernancePlan) {

  const lines = [];

  const fr = ctx.factoryRules;

  lines.push(`# Brand Explorer Active Profile Founder Review ${FACTORY_VERSION}`);

  lines.push("");

  lines.push(`- Brand: **${ctx.brand.name}** (\`${ctx.brand.slug}\`)`);

  lines.push(`- Generated: ${new Date().toISOString()}`);

  lines.push(`- Factory pass: **${fr.pass ? "yes" : "no"}**`);

  lines.push("");

  lines.push("## Visual readiness");

  lines.push(`- Gallery (${ACTIVE_PROFILE_GALLERY_MINIMUM} visible + imageUrl): **${fr.rules.gallery.pass ? "PASS" : "FAIL"}** (${fr.rules.gallery.withImageUrl}/${ACTIVE_PROFILE_GALLERY_MINIMUM})`);

  lines.push(`- Property examples: **${fr.rules.propertyExamples.pass ? "PASS" : "FAIL"}**`);

  lines.push(`- Scenario cards (no IMAGE placeholder): **${fr.rules.scenarioImages.pass ? "PASS" : "FAIL"}**`);

  lines.push(`- Registry traceability: **${fr.rules.registryTraceability.pass ? "PASS" : "FAIL"}**`);

  lines.push(`- UI fallback risk: **${fr.rules.uiFallback.pass ? "PASS" : "FAIL"}**`);

  lines.push(`- Copy safety: **${fr.rules.copySafety.pass ? "PASS" : "FAIL"}**`);

  lines.push(`- Standard Detail governance: **${fr.rules.standardDetail.pass ? "PASS" : "FAIL"}**`);

  lines.push("");

  if (copyGovernancePlan) {

    lines.push("## Copy governance (v34C)");

    lines.push(`- Rows audited (unsafe): **${copyGovernancePlan.summary.rowsAudited}**`);

    lines.push(`- Brand-specific repairs proposed: **${copyGovernancePlan.summary.repairsProposed}**`);

    lines.push(`- Founder review queue: **${copyGovernancePlan.summary.founderReviewRequired}**`);

    lines.push(`- Segment: ${copyGovernancePlan.segment}`);

    lines.push("");

  }

  lines.push("## Property cards (asset pack)");

  for (const p of assetPack?.propertyExamples || []) {

    lines.push(`- **${p.propertyName || p.intendedSlot}**: image ${p.imageUrl ? "✓" : "MISSING"} | ${p.renderReadiness}`);

  }

  lines.push("");

  lines.push("## Gallery image pack");

  for (const g of assetPack?.gallery || []) {

    lines.push(`- \`${g.slotKey}\`: ${g.imageUrl ? "source found" : "MISSING"} | ${g.renderReadiness}`);

  }

  lines.push("");

  lines.push("## Scenario cards");

  for (const s of assetPack?.scenarios || []) {

    lines.push(`- \`${s.slotKey}\`: image ${s.imageUrl ? "✓" : "hide/partial"} | ${s.notes}`);

  }

  lines.push("");

  lines.push("## Proof cards");

  for (const p of assetPack?.proofSupport || []) {

    lines.push(`- \`${p.slotKey}\`: ${p.renderReadiness}`);

  }

  lines.push("");

  if (draftPlan?.riskyCopyRemoved?.length) {

    lines.push("## Risky copy removed (draft sanitizer)");

    for (const r of draftPlan.riskyCopyRemoved) lines.push(`- ${r}`);

    lines.push("");

  }

  if (copyGovernancePlan?.repairs?.length) {

    lines.push("## Copy repairs (before → after sample)");

    for (const r of copyGovernancePlan.repairs.slice(0, 8)) {

      lines.push(`- \`${r.slotKey}\` (${r.rewriteStrategy}): ${r.findingsBefore.map((f) => f.patternId).join(", ")}`);

    }

    if (copyGovernancePlan.repairs.length > 8) {

      lines.push(`- … and ${copyGovernancePlan.repairs.length - 8} more (see copy-governance report)`);

    }

    lines.push("");

  }

  if (copyGovernancePlan?.founderReviewQueue?.length) {

    lines.push("## Copy — founder review required (no generic filler)");

    for (const q of copyGovernancePlan.founderReviewQueue) {

      lines.push(`- \`${q.slotKey}\`: ${q.reason}`);

    }

    lines.push("");

  }

  if (draftPlan?.missingSourceEvidence?.length) {

    lines.push("## Missing source evidence");

    for (const m of draftPlan.missingSourceEvidence) lines.push(`- \`${m.slotKey}\`: ${m.issue}`);

    lines.push("");

  }

  if (draftPlan?.pendingGovernanceGates?.length) {

    lines.push("## Pending governance gates");

    for (const g of draftPlan.pendingGovernanceGates) lines.push(`- ${g}`);

    lines.push("");

  }

  lines.push("## What will be applied (dry-run draft)");

  lines.push(`- Presentation patches: **${draftPlan?.willApply?.presentationPatchCount || 0}**`);

  lines.push(`- Registry creates: **${draftPlan?.willApply?.registryCreateCount || 0}**`);

  lines.push(`- Sections: ${(draftPlan?.willApply?.sections || []).join(", ") || "(none)"}`);

  lines.push("");

  lines.push("## Remains human-reviewed");

  for (const item of draftPlan?.remainsHumanReviewed || []) lines.push(`- ${item}`);

  lines.push("");

  if (fr.blockers.length) {

    lines.push("## Blockers");

    for (const b of fr.blockers) lines.push(`- ${b}`);

    lines.push("");

  }

  if (!fr.pass) {

    lines.push("## Staged apply workflow (v34D)");

    lines.push("");

    lines.push("### Stage 1 — Draft apply (preview writes)");

    lines.push("_Run after dry-runs are clean. Does not approve active profile._");

    lines.push("```bash");

    lines.push(buildDraftApplyCommand(ctx.brand.slug));

    lines.push("```");

    lines.push("");

    lines.push("### Stage 2 — Founder visual review");

    lines.push("```bash");

    lines.push(`npm run brand-explorer-active-profile-founder-review -- --brand ${ctx.brand.slug} --dry-run`);

    lines.push("```");

    lines.push("");

    lines.push("### Stage 3 — Active approval");

    lines.push("_Blocked until founder visual review passes._");

  } else {

    lines.push("## Staged apply workflow (v34D)");

    lines.push("");

    lines.push("### Stage 1 — Draft apply");

    lines.push("```bash");

    lines.push(buildDraftApplyCommand(ctx.brand.slug));

    lines.push("```");

    lines.push("");

    lines.push("### Stage 2 — Founder visual review");

    lines.push("```bash");

    lines.push(`npm run brand-explorer-active-profile-founder-review -- --brand ${ctx.brand.slug} --dry-run`);

    lines.push("```");

    lines.push("");

    lines.push("### Stage 3 — Active approval");

    lines.push("_After founder visual review passes._");

    lines.push("```bash");

    lines.push(buildActiveApprovalCommand(ctx.brand.slug));

    lines.push("```");

  }

  return lines.join("\n");

}



function buildAssetPackMarkdown(assetPack) {

  const lines = [];

  lines.push(`# Active Profile Asset Pack ${assetPack.assetPackVersion}`);

  lines.push("");

  lines.push(`- Brand: **${assetPack.brandConfig.name}** (\`${assetPack.brandSlug}\`)`);

  lines.push(`- Readiness: **${assetPack.summary.readinessBand}**`);

  lines.push(`- Gallery: ${assetPack.summary.galleryReady}`);

  lines.push(`- Property examples: ${assetPack.summary.propertyExamplesReady}`);

  lines.push(`- Scenarios with image: ${assetPack.summary.scenariosWithImage}`);

  lines.push("");

  lines.push("## Gallery assets");

  for (const g of assetPack.gallery) {

    lines.push(`### ${g.slotKey}`);

    lines.push(`- Source page: ${g.sourcePageUrl || "(none)"}`);

    lines.push(`- Image URL: ${g.imageUrl ? "present" : "MISSING"}`);

    lines.push(`- Registry candidate: ${g.registryRowCandidate}`);

    lines.push(`- Confidence: ${g.sourceConfidence}`);

    lines.push(`- Classification: ${g.imageTypeClassification}`);

    lines.push(`- Render readiness: ${g.renderReadiness}`);

    lines.push("");

  }

  return lines.join("\n");

}



function buildDraftMarkdown(draftPlan) {

  const lines = [];

  lines.push(`# Active Profile Draft Build ${draftPlan.draftBuilderVersion}`);

  lines.push("");

  lines.push(`- Brand: \`${draftPlan.brandSlug}\``);

  lines.push(`- Mode: **${draftPlan.mode}**`);

  lines.push(`- Presentation patches: **${draftPlan.summary.patchCount}**`);

  lines.push(`- Registry creates: **${draftPlan.summary.registryCreates}**`);

  lines.push("");

  for (const p of draftPlan.presentationPatches.slice(0, 20)) {

    lines.push(`- \`${p.slotKey}\` (${p.recordId || "create"}): ${p.reason}`);

  }

  if (draftPlan.presentationPatches.length > 20) {

    lines.push(`- … and ${draftPlan.presentationPatches.length - 20} more`);

  }

  return lines.join("\n");

}



export async function buildBrandExplorerActiveProfileFactoryReport({

  brandArg,

  stage = "preflight",

  apply = false,

  guardFlags = {},

} = {}) {

  const ctx = await loadBrandFactoryContext(brandArg);



  let assetPack = null;

  let draftPlan = null;

  let copyGovernancePlan = null;

  let approvedSources = [];



  if (["asset-pack", "build-draft", "copy-governance", "apply-draft", "founder-review", "apply-approved"].includes(stage)) {

    assetPack = await buildActiveProfileAssetPack({

      brandSlug: ctx.brand.slug,

      presentationRows: ctx.presentationRows,

      registryAssets: ctx.registryAssets,

      brandApi: ctx.brandApi,

    });

  }



  if (["build-draft", "copy-governance", "apply-draft", "founder-review", "apply-approved"].includes(stage) && assetPack) {

    draftPlan = buildActiveProfileDraftPlan({

      brandSlug: ctx.brand.slug,

      assetPack,

      presentationRows: ctx.presentationRows,

      brandBasics: ctx.brandBasics,

      brandApi: ctx.brandApi,

    });

  }



  if (["copy-governance", "apply-draft", "founder-review", "apply-approved"].includes(stage)) {

    const sourceResolution = await fetchAndResolveApprovedBrandSources({

      recordId: ctx.brand.recordId,

      companyDomains: ctx.activeProfileConfig?.officialSourceDomains || [],

    }).catch(() => ({ sources: [] }));

    approvedSources = resolveApprovedBrandSources(sourceResolution.sources || [], {

      recordId: ctx.brand.recordId,

      companyDomains: ctx.activeProfileConfig?.officialSourceDomains || [],

    });

    copyGovernancePlan = buildCopyGovernancePlan({

      brandSlug: ctx.brand.slug,

      presentationRows: ctx.presentationRows,

      brandConfig: ctx.activeProfileConfig,

      approvedSources,

      assetPack,

    });

  }



  const companyValidatedBefore = Boolean(ctx.brandBasics?.["Company Validated"]);

  let applyResult = null;

  let postDraftContext = null;

  const draftApplyValidation = validateDraftApplyRequest({

    guardFlags,

    copyGovernancePlan,

    draftPlan,

    apply: stage === "apply-draft" && apply,

  });

  let founderVisualReview = evaluateFounderVisualReview({

    factoryRules: ctx.factoryRules,

    brandBasics: ctx.brandBasics,

    companyValidatedBefore,

  });

  const activeApprovalValidation = validateActiveApprovalRequest({

    guardFlags,

    founderVisualReview,

    apply: stage === "apply-approved" && apply,

  });

  if (stage === "apply-draft" && apply) {

    if (!draftApplyValidation.allowed) {

      applyResult = { blocked: true, reason: "draft_apply_validation_failed", blockers: draftApplyValidation.blockers };

    } else {

      const partial = {};

      if (copyGovernancePlan && guardFlags.approveCopyGovernance) {

        partial.copyGovernance = await applyCopyGovernancePlan({

          plan: copyGovernancePlan,

          apply: true,

          guardFlags: { approveCopyGovernance: true },

          baseId: ctx.baseId,

          apiKey: ctx.apiKey,

        });

      }

      if (draftPlan && guardFlags.approveBrandExplorerActiveProfileDraft) {

        partial.draft = await applyActiveProfileDraftPlan({

          draftPlan,

          apply: true,

          guardFlags,

          baseId: ctx.baseId,

          apiKey: ctx.apiKey,

        });

      }

      applyResult = {

        ...partial,

        stage: "apply-draft",

        activeProfileApproved: false,

        readyForActiveProfileSet: false,

      };

      postDraftContext = await loadBrandFactoryContext(brandArg);

      founderVisualReview = evaluateFounderVisualReview({

        factoryRules: postDraftContext.factoryRules,

        brandBasics: postDraftContext.brandBasics,

        companyValidatedBefore,

      });

    }

  }

  if (stage === "apply-approved" && apply) {

    if (!activeApprovalValidation.allowed) {

      applyResult = {

        blocked: true,

        reason: "active_approval_validation_failed",

        blockers: activeApprovalValidation.blockers,

      };

    } else {

      applyResult = {

        stage: "apply-approved",

        activeProfileApproved: true,

        readyForActiveProfileSet: false,

        approvedAt: new Date().toISOString(),

        founderVisualReviewPass: founderVisualReview.pass,

        note: "Active-profile approval recorded. readyForActiveProfile remains computed by Complete Build from live state.",

      };

    }

  }



  let finalQaReport = null;

  let visualReport = null;

  if (stage === "final-qa" || stage === "preflight" || stage === "founder-review") {

    [finalQaReport, visualReport] = await Promise.all([

      buildBrandExplorerFinalQaAuditorReport({ brandIdOrName: ctx.brand.slug }).catch(() => null),

      buildBrandExplorerVisualDisplayDefectAuditReport({ brandIdOrName: ctx.brand.recordId }).catch(

        () => null

      ),

    ]);

  }



  const qaBrand =

    finalQaReport?.brandReports?.find((b) => b.brand?.slug === ctx.brand.slug) || null;



  const suburbanReadiness =

    ctx.brand.slug === "suburban-studios" && assetPack && draftPlan

      ? assessSuburbanReadiness(ctx, assetPack, draftPlan)

      : ctx.brand.slug === "suburban-studios"

        ? { recommendation: "run_asset_pack_and_build_draft_stages", blockers: ctx.factoryRules.blockers }

        : null;



  const report = {

    factoryVersion: FACTORY_VERSION,

    brandConfigVersion: BRAND_CONFIG_VERSION,

    stage,

    generatedAt: new Date().toISOString(),

    brand: ctx.brand,

    activeProfileConfig: ctx.activeProfileConfig

      ? {

          slug: ctx.activeProfileConfig.slug,

          recordId: ctx.activeProfileConfig.recordId,

          brandFamily: ctx.activeProfileConfig.brandFamily,

          propertyExampleStrategy: ctx.activeProfileConfig.propertyExampleStrategy,

          galleryMinimum: ctx.activeProfileConfig.galleryMinimum,

        }

      : null,

    mode: apply ? "apply" : "dry-run",

    guardrails: factoryGuardrailsSummary(),

    factoryRules: ctx.factoryRules,

    assetPack,

    draftPlan,

    copyGovernancePlan,

    approvedSourcesCount: approvedSources.length,

    applyResult,

    legacyWriterChainReference: LEGACY_WRITER_CHAINS[ctx.brand.slug] || [],

    finalQa: qaBrand

      ? {

          readiness: qaBrand.scores?.overallActiveProfileReadiness,

          numeric: qaBrand.scores?.overallNumeric,

          defectCount: qaBrand.defects?.length,

        }

      : null,

    visualDefects: visualReport?.summary || null,

    completeBuild: ctx.completeBuildReport?.brandResults?.find(

      (b) => b.brand?.slug === ctx.brand.slug

    )

      ? {

          readyForActiveProfile: ctx.completeBuildReport.brandResults.find(

            (b) => b.brand?.slug === ctx.brand.slug

          )?.readyForActiveProfile,

          readinessBand: ctx.completeBuildReport.brandResults.find(

            (b) => b.brand?.slug === ctx.brand.slug

          )?.readinessBand,

        }

      : null,

    suburbanReadiness,

    applyApproved: {

      allowed: activeApprovalValidation.allowed && founderVisualReview.pass,

      exactCommand: buildActiveApprovalCommand(ctx.brand.slug),

      blockers: activeApprovalValidation.blockers,

    },

    applyDraft: {

      allowed: draftApplyValidation.allowed,

      exactCommand: buildDraftApplyCommand(ctx.brand.slug),

      blockers: draftApplyValidation.blockers,

      writesActiveProfileApproval: false,

    },

    stagedApplyVersion: STAGED_APPLY_VERSION,

    founderVisualReview,

    draftApplyValidation,

    activeApprovalValidation,

    postDraftApply:

      stage === "apply-draft" && apply

        ? buildPostDraftApplySummary({

            applyResult,

            companyValidatedBefore,

            companyValidatedAfter: Boolean(

              (postDraftContext || ctx).brandBasics?.["Company Validated"]

            ),

            founderVisualReview,

          })

        : null,

    postDraftFactoryRules: postDraftContext?.factoryRules || null,

  };



  report.founderVisualReviewMarkdown = buildFounderVisualReviewMarkdown({

    brand: ctx.brand,

    founderVisualReview,

    draftApply: draftApplyValidation,

    activeApproval: activeApprovalValidation,

    postDraftApply: report.postDraftApply,

  });

  report.markdown =

    stage === "founder-review" || stage === "apply-draft"

      ? report.founderVisualReviewMarkdown

      : buildFounderReviewMarkdown(ctx, assetPack, draftPlan, copyGovernancePlan);

  report.assetPackMarkdown = assetPack ? buildAssetPackMarkdown(assetPack) : null;

  report.draftMarkdown = draftPlan ? buildDraftMarkdown(draftPlan) : null;

  report.copyGovernanceMarkdown = copyGovernancePlan

    ? buildCopyGovernanceMarkdown(copyGovernancePlan)

    : null;

  report.founderQueueAuditMarkdown =

    copyGovernancePlan?.founderQueueResolution

      ? buildFounderQueueAuditMarkdown(

          copyGovernancePlan.founderQueueResolution,

          copyGovernancePlan.brandName

        )

      : null;

  return report;

}



export function perBrandReportBasename(slug) {

  return `${REPORT_BASENAME}-${slug}`;

}



export function perBrandStageReportBasename(slug, stage) {

  return `${REPORT_BASENAME}-${slug}-${stage}`;

}


