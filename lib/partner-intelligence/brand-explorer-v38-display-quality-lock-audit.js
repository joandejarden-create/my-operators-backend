/**
 * v38 — Brand Explorer display quality lock audit (read-only).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import {
  evaluateBrandExternalQualityLock,
  FALLBACK_RENDERER_INVENTORY,
  V38_QUALITY_LOCK_VERSION,
} from "./brand-explorer-display-quality-lock.js";
import { resolveBrandExplorerDisplayState } from "./brand-explorer-display-state.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { buildActiveProfileAssetPack } from "./brand-explorer-active-profile-asset-pack-builder.js";
import { extendAssetPackWithRenderReadiness } from "./brand-explorer-render-readiness-contract.js";
import { applyDomFailureToExternalOwnerReadiness } from "./brand-explorer-display-quality-lock.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import {
  PRIMARY_RELEASE_SLUGS,
  INCOMPLETE_CONTROL_SLUGS,
} from "./brand-explorer-os-state-machine.js";

export const DEFAULT_INCOMPLETE_BRANDS = Object.freeze([...INCOMPLETE_CONTROL_SLUGS]);

export const DEFAULT_COMPLETE_BRANDS = Object.freeze([...PRIMARY_RELEASE_SLUGS]);

export const DEFAULT_BENCHMARK_BRANDS = Object.freeze(["tribute-portfolio", "woodspring-suites"]);

export const DEFAULT_BRANDS = Object.freeze([
  ...DEFAULT_INCOMPLETE_BRANDS,
  ...DEFAULT_COMPLETE_BRANDS,
]);

export const REPORT_JSON = "brand-explorer-v38-display-quality-lock-audit.json";
export const REPORT_MD = "brand-explorer-v38-display-quality-lock-audit.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

async function fetchBrandApiShape(slug) {
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

function buildReadinessGates(ctx, assetPack, renderContract) {
  const approvedSourcesCount = ctx.registryAssets?.filter(
    (r) => r.explorerUsePermission === "Approved For Explorer" || r.fields?.["Approved For Explorer Use"] === "Yes"
  ).length;
  return {
    source_ready: approvedSourcesCount >= 3,
    knowledge_pack_ready: Boolean(ctx.brandBasics),
    visual_asset_pack_ready: Boolean(assetPack?.summary),
    render_ready: renderContract?.pass === true,
    presentation_plan_ready: (ctx.presentationRows || []).length > 0,
    copy_ready: true,
    external_owner_ready: evaluateExternalOwnerReadinessRule(ctx.presentationRows || []).pass,
    founder_visual_review_passed: true,
    active_profile_approval_passed: true,
  };
}

export async function runV38DisplayQualityLockAudit({ brands = DEFAULT_BRANDS, dryRun = true } = {}) {
  const brandResults = [];

  for (const brandSlug of brands) {
    const config = getActiveProfileBrandConfig(brandSlug);
    const ctx = await loadBrandFactoryContext(brandSlug).catch(() => null);
    const brandApi = await fetchBrandApiShape(brandSlug);

    const assetPack = ctx
      ? await buildActiveProfileAssetPack({
          brandSlug,
          presentationRows: ctx.presentationRows,
          registryAssets: ctx.registryAssets,
          brandApi,
        }).catch(() => null)
      : null;

    const renderContract = ctx
      ? extendAssetPackWithRenderReadiness(assetPack, {
          presentationRows: ctx.presentationRows,
          brandApi,
          registryAssets: ctx.registryAssets,
        })
      : null;

    const readinessGates = ctx ? buildReadinessGates(ctx, assetPack, renderContract) : null;

    const displayMeta = resolveBrandExplorerDisplayState(brandApi, {
      presentationRows: ctx?.presentationRows,
      brandBasics: ctx?.brandBasics,
      renderContract,
      readinessGates,
    });

    const renderedHtml = renderBrandExplorerHtmlForTest(
      {
        ...brandApi,
        brandExplorerDisplayState: displayMeta.brandExplorerDisplayState,
        shouldRenderFullProfile: displayMeta.shouldRenderFullProfile,
        shouldSuppressIncompleteExternalSections: displayMeta.shouldSuppressIncompleteExternalSections,
      },
      { allPanels: true }
    );

    const qualityLock = evaluateBrandExternalQualityLock(
      {
        ...brandApi,
        brandExplorerDisplayState: displayMeta.brandExplorerDisplayState,
        shouldRenderFullProfile: displayMeta.shouldRenderFullProfile,
      },
      renderedHtml,
      { brandSlug, brandBasics: ctx?.brandBasics, renderContract, readinessGates }
    );

    const externalOwnerBase = evaluateExternalOwnerReadinessRule(ctx?.presentationRows || brandApi.brandExplorer?.blocks || []);
    const externalOwnerWithDom = applyDomFailureToExternalOwnerReadiness(
      { pass: externalOwnerBase.pass, blockers: externalOwnerBase.blockers || [], numericScore: externalOwnerBase.pass ? 100 : 60, categories: [] },
      qualityLock.domScan
    );

    brandResults.push({
      brandSlug,
      recordId: brandApi.id || config?.recordId,
      brandName: brandApi.name,
      dryRun,
      cohort: DEFAULT_INCOMPLETE_BRANDS.includes(brandSlug)
        ? "incomplete"
        : DEFAULT_COMPLETE_BRANDS.includes(brandSlug)
          ? "complete"
          : DEFAULT_BENCHMARK_BRANDS.includes(brandSlug)
            ? "benchmark"
            : "other",
      displayState: qualityLock.displayState,
      shouldRenderFullProfile: qualityLock.shouldRenderFullProfile,
      actualRenderedFullProfile: qualityLock.actualRenderedFullProfile,
      tabsRenderedExternally: qualityLock.tabsRenderedExternally,
      forbiddenStringsFound: qualityLock.forbiddenStringsFound,
      forbiddenMatches: qualityLock.forbiddenMatches,
      emptyCardsFound: qualityLock.emptyCardsFound,
      helperTextFound: qualityLock.helperTextFound,
      internalNotesFound: qualityLock.internalNotesFound,
      profileInPreparationRendered: qualityLock.profileInPreparationRendered,
      profileInPreparationCount: qualityLock.profileInPreparationCount,
      externalQualityLockPass: qualityLock.externalQualityLockPass,
      allowedNextAction: qualityLock.allowedNextAction,
      displayBlockers: displayMeta.blockers,
      externalOwnerReadiness: externalOwnerWithDom,
      assetPackSummary: assetPack?.summary || null,
      renderContractPass: renderContract?.pass === true,
      readyForActiveApproval: false,
    });
  }

  return {
    version: V38_QUALITY_LOCK_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    brands,
    fallbackInventory: FALLBACK_RENDERER_INVENTORY,
    brandResults,
    summary: {
      brandsTested: brandResults.length,
      externalQualityLockPassCount: brandResults.filter((b) => b.externalQualityLockPass).length,
      allExternalQualityLockPass: brandResults.every((b) => b.externalQualityLockPass),
      incompletePassCount: brandResults.filter((b) => b.cohort === "incomplete" && b.externalQualityLockPass).length,
      completePassCount: brandResults.filter((b) => b.cohort === "complete" && b.externalQualityLockPass).length,
    },
    guardrails: {
      airtableWrites: false,
      presentationWrites: false,
      registryWrites: false,
      companyValidatedChanges: false,
      activeProfileApproval: false,
    },
  };
}

export function writeV38Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const mdLines = [
    "# v38 Brand Explorer Display Quality Lock Audit",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "## Summary",
    `- Brands tested: ${report.summary.brandsTested}`,
    `- External quality lock pass: ${report.summary.externalQualityLockPassCount}/${report.summary.brandsTested}`,
    `- Incomplete cohort pass: ${report.summary.incompletePassCount}/${DEFAULT_INCOMPLETE_BRANDS.length}`,
    `- Complete cohort pass: ${report.summary.completePassCount}/${DEFAULT_COMPLETE_BRANDS.length}`,
    "",
    "## Per brand",
  ];

  for (const b of report.brandResults) {
    mdLines.push(`### ${b.brandSlug} (${b.cohort})`);
    mdLines.push(`- displayState: \`${b.displayState}\``);
    mdLines.push(`- shouldRenderFullProfile: ${b.shouldRenderFullProfile}`);
    mdLines.push(`- actualRenderedFullProfile: ${b.actualRenderedFullProfile}`);
    mdLines.push(`- forbiddenStringsFound: ${b.forbiddenStringsFound}`);
    mdLines.push(`- externalQualityLockPass: **${b.externalQualityLockPass ? "PASS" : "FAIL"}**`);
    mdLines.push(`- allowedNextAction: ${b.allowedNextAction}`);
    mdLines.push("");
  }

  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, mdLines.join("\n"));

  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  fs.mkdirSync(docsDir, { recursive: true });
  fs.writeFileSync(
    path.join(docsDir, "brand-explorer-v38-display-quality-lock.md"),
    [
      "# v38 Brand Explorer Display Quality Lock",
      "",
      "Global external rendering contract: only `external_owner_ready` and `active_profile_ready` render full tabs.",
      "",
      "```bash",
      "npm run brand-explorer-v38-display-quality-lock-audit -- --brands hotel-indigo,mgallery-collection,design-hotels,small-luxury-hotels-of-the-world,everhome-suites,kimpton-hotels,radisson-individuals-by-choice --dry-run",
      "npm run test:brand-explorer-external-quality-lock -- --brands hotel-indigo,mgallery-collection,design-hotels,small-luxury-hotels-of-the-world,everhome-suites,kimpton-hotels,radisson-individuals-by-choice",
      "```",
      "",
      "## External rule",
      "All other display states render **Profile in Preparation** only — no tabs, no fallback cards, no helper text.",
      "",
      "## Internal preview",
      "Append `?beInternalPreview=1` to see incomplete sections labeled **Internal preview · Not owner-ready**.",
    ].join("\n")
  );

  return { jsonPath, mdPath };
}
