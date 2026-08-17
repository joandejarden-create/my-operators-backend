/**
 * v37C-R2 — External Display Gating Verification + UI Proof Test (read-only).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { resolveBrandExplorerDisplayState } from "./brand-explorer-display-state.js";
import { scanRenderedHtmlForForbiddenStrings } from "./brand-explorer-external-display-forbidden-patterns.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { buildActiveProfileAssetPack } from "./brand-explorer-active-profile-asset-pack-builder.js";
import { buildActiveProfileDraftPlan } from "./brand-explorer-active-profile-draft-builder.js";
import { extendAssetPackWithRenderReadiness } from "./brand-explorer-render-readiness-contract.js";
import { readJsonIfExists as readJson } from "./brand-explorer-v37c-r2-utils.js";

export const V37C_R2_VERSION = "v37C-R2";
export const DEFAULT_BRANDS = Object.freeze(["hotel-indigo", "mgallery-collection"]);
export const REPORT_JSON = "brand-explorer-v37c-r2-external-display-gating-verification.json";
export const REPORT_MD = "brand-explorer-v37c-r2-external-display-gating-verification.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const RENDERER_EMITTERS = Object.freeze([
  {
    string: "Scenario cards will appear when overview.scenario presentation rows are visible in Brand Explorer.",
    function: "renderAtelierOverview",
    source: "fallback_renderer",
    field: "hardcoded helper when scenarioApiBlocks empty",
  },
  {
    string: "IHG",
    function: "renderAtelierOverview → whyList / ownerOut",
    source: "brand_basics_fallback",
    field: "brandValueProposition",
  },
  {
    string: "neighborhood focus",
    function: "renderAtelierOverview → whyList",
    source: "brand_basics_fallback",
    field: "brandValueProposition",
  },
  {
    string: "boutique design",
    function: "renderAtelierOverview → whyList",
    source: "brand_basics_fallback",
    field: "brandValueProposition",
  },
  {
    string: "conversion-friendly.",
    function: "renderAtelierOverview → whyList / diffUl / bestCards",
    source: "brand_basics_fallback",
    field: "brandValueProposition / keyBrandDifferentiators",
  },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

async function fetchBrandApiShape(slug) {
  const config = getActiveProfileBrandConfig(slug);
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
      return this;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`brand API fetch failed for ${slug}`);
  }
  return res.payload.brand;
}

function auditRendererPath(brand, ctx) {
  const suppressBranchExecutes = brand.shouldSuppressIncompleteExternalSections === true;
  const displayState = brand.brandExplorerDisplayState || "unknown";
  return {
    slugReceivedByRenderer: ctx.brandSlug,
    brandRecordId: brand.id || ctx.brand?.recordId,
    profileCompleteness: brand.brandExplorerDisplayCompleteness || null,
    brandExplorerDisplayState: displayState,
    targetBrandGatingBranchExecutes: suppressBranchExecutes,
    helperTextGeneratedBeforeSuppression: true,
    helperTextSuppressedInOutput: suppressBranchExecutes,
    contentSources: {
      livePresentationRows: (ctx.presentationRows || []).length,
      fallbackRendererRisk: !brand.brandExplorerDisplayCompleteness?.hasScenarioRows,
      brandBasicsFields: {
        brandValueProposition: nz(brand.brandValueProposition).slice(0, 120),
        keyBrandDifferentiators: nz(brand.keyBrandDifferentiators).slice(0, 120),
      },
      sourceLibrarySeeded: Boolean(readJson(path.join(ROOT, "reports", "brand-explorer-v37b-lifestyle-batch-source-seeding.json"))),
    },
    emitterAudit: RENDERER_EMITTERS,
    staleBuildNote:
      "If owner UI still shows staging text after deploy, hard-refresh browser cache and confirm Webflow/static host serves latest public/js/brand-explorer-atelier-from-api.js plus API returns brandExplorerDisplayState.",
  };
}

function buildUiProof(brand) {
  const overviewHtml = renderBrandExplorerHtmlForTest(brand, { allPanels: false });
  const companyValidated = brand?.governance?.companyValidated === true;
  const overviewScan = scanRenderedHtmlForForbiddenStrings(overviewHtml, { companyValidated });

  return {
    should_hide_external_profile: brand.shouldHideExternalProfile === true,
    actual_helper_text_visible: overviewHtml.includes("Scenario cards will appear"),
    forbidden_strings_found: overviewScan.forbiddenStringsFound.length,
    forbidden_matches: overviewScan.matches,
    rendered_sections_suppressed: overviewHtml.includes('data-be-display-gate="profile-in-preparation"'),
    ui_proof_test_pass: overviewScan.forbiddenStringsFound.length === 0,
    screenshot_manual_qa_status: "pending_founder_recheck_after_deploy",
    overviewHtmlLength: overviewHtml.length,
  };
}

function parseRatio(value) {
  const m = nz(value).match(/^(\d+)\s*\/\s*(\d+)$/);
  if (!m) return { got: 0, min: 0 };
  return { got: Number(m[1]), min: Number(m[2]) };
}

function buildApplyDraftEligibility(brandSlug, assetPack, draftPlan, renderContract, uiProof) {
  const gallery = parseRatio(assetPack?.summary?.galleryReady);
  const openings = parseRatio(assetPack?.summary?.propertyExamplesReady);
  const summary = draftPlan?.summary || {};
  const assetPackReady = gallery.got >= 6 && openings.got >= 3;
  const buildDraftReady =
    (summary.galleryPatches || 0) >= 6 &&
    (summary.propertyPatches || 0) >= 3 &&
    (summary.scenarioPatches || 0) >= 3;
  const applyDraftAllowed =
    assetPackReady && buildDraftReady && renderContract?.pass === true && uiProof.ui_proof_test_pass;
  const reasons = [];
  if (!assetPackReady) reasons.push("asset_pack_incomplete");
  if (!buildDraftReady) reasons.push("build_draft_incomplete");
  if (renderContract?.pass !== true) reasons.push("render_contract_fail");
  if (!uiProof.ui_proof_test_pass) reasons.push("external_display_not_safe");
  if (brandSlug === "hotel-indigo" && openings.got < 3) {
    reasons.push("hotel_indigo_property_specific_images_below_minimum");
  }
  return {
    apply_draft_allowed: applyDraftAllowed,
    apply_draft_blocked_reason: applyDraftAllowed ? [] : reasons,
    ready_for_active_approval: false,
  };
}

export async function runV37CR2ExternalDisplayGatingVerification({ brands = DEFAULT_BRANDS, dryRun = true } = {}) {
  const v37cR1 = readJson(path.join(ROOT, "reports", "brand-explorer-v37c-r1-display-gating-visual-integration.json"));
  const brandResults = [];

  for (const brandSlug of brands) {
    const ctx = await loadBrandFactoryContext(brandSlug);
    const brandApi = await fetchBrandApiShape(brandSlug);
    const displayMeta = resolveBrandExplorerDisplayState(brandApi, {
      presentationRows: ctx.presentationRows,
      brandBasics: ctx.brandBasics,
      sourceLibrarySeeded: true,
    });

    const assetPack = await buildActiveProfileAssetPack({
      brandSlug,
      presentationRows: ctx.presentationRows,
      registryAssets: ctx.registryAssets,
      brandApi,
    });
    const draftPlan = buildActiveProfileDraftPlan({
      brandSlug,
      assetPack,
      presentationRows: ctx.presentationRows,
      brandBasics: ctx.brandBasics,
      brandApi,
    });
    const renderContract = extendAssetPackWithRenderReadiness(assetPack, {
      presentationRows: ctx.presentationRows,
      brandApi,
      registryAssets: ctx.registryAssets,
    });

    const rendererAudit = auditRendererPath(brandApi, { ...ctx, brandSlug });
    const uiProof = buildUiProof(brandApi, brandSlug);
    const applyDraft = buildApplyDraftEligibility(brandSlug, assetPack, draftPlan, renderContract, uiProof);
    const v37cR1Brand = (v37cR1?.brandResults || []).find((b) => b.brandSlug === brandSlug) || null;

    brandResults.push({
      brandSlug,
      recordId: brandApi.id,
      dryRun,
      displayState: {
        api: {
          brandExplorerDisplayState: brandApi.brandExplorerDisplayState,
          shouldSuppressIncompleteExternalSections: brandApi.shouldSuppressIncompleteExternalSections,
          shouldHideExternalProfile: brandApi.shouldHideExternalProfile,
          completeness: brandApi.brandExplorerDisplayCompleteness,
          blockers: brandApi.brandExplorerDisplayBlockers,
        },
        recomputed: displayMeta,
      },
      rendererAudit,
      uiProof,
      assetPackSummary: assetPack?.summary || null,
      draftPlanSummary: draftPlan?.summary || null,
      renderContractPass: renderContract?.pass === true,
      applyDraft,
      v37cR1CrossCheck: v37cR1Brand
        ? {
            prior_should_hide_external_profile: v37cR1Brand.batchReadiness?.should_hide_external_profile,
            prior_external_display_safe: v37cR1Brand.batchReadiness?.external_display_safe,
          }
        : null,
    });
  }

  return {
    version: V37C_R2_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    brands,
    brandResults,
    summary: {
      brandsTested: brandResults.length,
      uiProofPassCount: brandResults.filter((b) => b.uiProof?.ui_proof_test_pass).length,
      allUiProofPass: brandResults.every((b) => b.uiProof?.ui_proof_test_pass),
      applyDraftAllowedCount: brandResults.filter((b) => b.applyDraft?.apply_draft_allowed).length,
    },
    guardrails: {
      airtableWrites: false,
      presentationWrites: false,
      registryWrites: false,
      imageFieldWrites: false,
      activeProfileApproval: false,
      companyValidatedChanges: false,
    },
  };
}

function buildBrandUiProofMarkdown(result) {
  const lines = [];
  lines.push(`# v37C-R2 ${result.brandSlug} UI Proof`);
  lines.push("");
  lines.push(`- Record ID: \`${result.recordId}\``);
  lines.push(`- brandExplorerDisplayState: **${result.displayState.api.brandExplorerDisplayState}**`);
  lines.push(`- should_hide_external_profile: **${result.uiProof.should_hide_external_profile ? "yes" : "no"}**`);
  lines.push(`- actual_helper_text_visible: **${result.uiProof.actual_helper_text_visible ? "yes" : "no"}**`);
  lines.push(`- forbidden_strings_found: **${result.uiProof.forbidden_strings_found}**`);
  lines.push(`- rendered_sections_suppressed: **${result.uiProof.rendered_sections_suppressed ? "yes" : "no"}**`);
  lines.push(`- UI proof test: **${result.uiProof.ui_proof_test_pass ? "PASS" : "FAIL"}**`);
  lines.push(`- apply_draft_allowed: **${result.applyDraft.apply_draft_allowed ? "yes" : "no"}**`);
  if (result.uiProof.forbidden_matches?.length) {
    lines.push("- Forbidden matches:");
    for (const m of result.uiProof.forbidden_matches) {
      lines.push(`  - \`${m.pattern}\`: ${m.snippet}`);
    }
  }
  lines.push(`- Manual QA: ${result.uiProof.screenshot_manual_qa_status}`);
  return lines.join("\n");
}

export function writeV37CR2Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const mdLines = [
    "# v37C-R2 External Display Gating Verification",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "## Summary",
    `- Brands tested: ${report.summary.brandsTested}`,
    `- UI proof pass: ${report.summary.uiProofPassCount}/${report.summary.brandsTested}`,
    `- apply_draft_allowed: ${report.summary.applyDraftAllowedCount} brand(s)`,
    "",
  ];

  for (const b of report.brandResults) {
    mdLines.push(`## ${b.brandSlug}`);
    mdLines.push(`- displayState: ${b.displayState.api.brandExplorerDisplayState}`);
    mdLines.push(`- should_hide_external_profile: ${b.uiProof.should_hide_external_profile}`);
    mdLines.push(`- forbidden_strings_found: ${b.uiProof.forbidden_strings_found}`);
    mdLines.push(`- UI proof: ${b.uiProof.ui_proof_test_pass ? "PASS" : "FAIL"}`);
    mdLines.push(`- apply_draft_allowed: ${b.applyDraft.apply_draft_allowed}`);
    mdLines.push("");
  }

  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, mdLines.join("\n"));

  const slugToProofFile = {
    "hotel-indigo": "hotel-indigo",
    "mgallery-collection": "mgallery",
  };

  for (const b of report.brandResults) {
    const slugFile = slugToProofFile[b.brandSlug] || b.brandSlug;
    const proofPath = path.join(reportsDir, `brand-explorer-v37c-r2-${slugFile}-ui-proof.md`);
    fs.writeFileSync(proofPath, buildBrandUiProofMarkdown(b));
  }

  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  fs.mkdirSync(docsDir, { recursive: true });
  fs.writeFileSync(
    path.join(docsDir, "brand-explorer-v37c-r2-external-display-gating-verification.md"),
    [
      "# v37C-R2 External Display Gating Verification",
      "",
      "Read-only verification that API `brandExplorerDisplayState` gates owner-facing staging copy.",
      "",
      "```bash",
      "npm run brand-explorer-v37c-r2-external-display-gating-verification -- --brands hotel-indigo,mgallery-collection --dry-run",
      "npm run test:brand-explorer-external-display-gating -- --brands hotel-indigo,mgallery-collection",
      "```",
      "",
      "## Display states",
      "- `hidden_incomplete` — suppress scenario/helper/fallback bullets externally",
      "- `internal_preview_only` — same suppression; internal preview via `?beInternalPreview=1`",
      "- `draft_applied_with_defects`",
      "- `founder_review_ready`",
      "- `external_owner_ready`",
      "",
      "Source Library seeding alone must never set `external_owner_ready`.",
    ].join("\n")
  );

  return { jsonPath, mdPath };
}
