/**
 * v39 — Brand Explorer Active Profile Release Audit (read-only).
 *
 * Live API/render contract is source of truth. Report-only readiness is never release readiness.
 * Does not apply active approval.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { buildActiveProfileAssetPack } from "./brand-explorer-active-profile-asset-pack-builder.js";
import { extendAssetPackWithRenderReadiness } from "./brand-explorer-render-readiness-contract.js";
import { resolveBrandExplorerDisplayState } from "./brand-explorer-display-state.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import {
  inventoryReleaseGates,
  reconcileReadinessSignals,
  classifyReleaseCandidate,
  buildActiveReleaseApplyCommandDesign,
  V39_RELEASE_GATE_VERSION,
} from "./brand-explorer-active-release-gate.js";
import {
  buildActiveReleasePlan,
  buildIncompleteBrandControlCheck,
  renderReleasePlanMarkdown,
  renderIncompleteControlMarkdown,
  PRIMARY_RELEASE_SLUGS,
  INCOMPLETE_CONTROL_SLUGS,
  V39_RELEASE_PLAN_VERSION,
} from "./brand-explorer-active-release-plan.js";

export const V39_AUDIT_VERSION = "v39";
export const REPORT_JSON = "brand-explorer-v39-active-release-audit.json";
export const REPORT_MD = "brand-explorer-v39-active-release-audit.md";

export const DEFAULT_PRIMARY_BRANDS = Object.freeze([
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
]);

export const DEFAULT_INCOMPLETE_BRANDS = Object.freeze([
  "hotel-indigo",
  "mgallery-collection",
  "design-hotels",
  "small-luxury-hotels-of-the-world",
]);

export const DEFAULT_BRANDS = Object.freeze([
  ...DEFAULT_PRIMARY_BRANDS,
  ...DEFAULT_INCOMPLETE_BRANDS,
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function readJsonIfExists(p) {
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function resolveConfig(slug) {
  return getActiveProfileBrandConfig(slug) || getDiscoveryBrandConfig(slug) || null;
}

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
    throw new Error(`brand API fetch failed for ${slug}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

function loadCompleteBuild(slug) {
  return readJsonIfExists(path.join(ROOT, "reports", `brand-explorer-complete-build-${slug}.json`));
}

function loadFinalQaForBrand(slug) {
  const all = readJsonIfExists(path.join(ROOT, "reports", "brand-explorer-final-qa-auditor.json"));
  if (!all) return null;
  const list = all.brandResults || all.brands || all.results || [];
  if (Array.isArray(list)) {
    return list.find((b) => b.slug === slug || b.brandSlug === slug) || all;
  }
  return all;
}

function loadVisualDefectForBrand(slug) {
  const candidates = [
    path.join(ROOT, "reports", `brand-explorer-visual-display-defect-audit-${slug}.json`),
    path.join(ROOT, "reports", "brand-explorer-visual-display-defect-audit.json"),
  ];
  for (const p of candidates) {
    const j = readJsonIfExists(p);
    if (!j) continue;
    if (j.brandSlug === slug || j.slug === slug) return j;
    const hit = (j.brandResults || []).find((b) => b.brandSlug === slug || b.slug === slug);
    if (hit) return hit;
    if (p.endsWith(`-${slug}.json`)) return j;
  }
  return null;
}

function loadV36cForBrand(slug) {
  const j = readJsonIfExists(path.join(ROOT, "reports", "brand-explorer-v36c-remediation-planner.json"));
  if (!j) return null;
  return (j.brandResults || []).find((b) => b.brandSlug === slug || b.slug === slug) || null;
}

function loadV38ForBrand(slug) {
  const j = readJsonIfExists(path.join(ROOT, "reports", "brand-explorer-v38-display-quality-lock-audit.json"));
  if (!j) return null;
  return (j.brandResults || []).find((b) => b.brandSlug === slug) || null;
}

function cohortFor(slug) {
  if (PRIMARY_RELEASE_SLUGS.has(slug) || DEFAULT_PRIMARY_BRANDS.includes(slug)) return "primary_release";
  if (INCOMPLETE_CONTROL_SLUGS.has(slug) || DEFAULT_INCOMPLETE_BRANDS.includes(slug)) return "incomplete";
  return "other";
}

export async function runV39ActiveReleaseAudit({ brands = DEFAULT_BRANDS, dryRun = true } = {}) {
  const brandResults = [];

  for (const brandSlug of brands) {
    const config = resolveConfig(brandSlug);
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

    const displayMeta = resolveBrandExplorerDisplayState(brandApi, {
      presentationRows: ctx?.presentationRows,
      brandBasics: ctx?.brandBasics,
      renderContract,
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
      { brandSlug, brandBasics: ctx?.brandBasics, renderContract }
    );

    const gateInventory = inventoryReleaseGates({
      brandSlug,
      brandApi,
      brandConfig: config,
      brandBasics: ctx?.brandBasics,
      presentationRows: ctx?.presentationRows,
      registryAssets: ctx?.registryAssets || [],
      assetPack,
      renderContract,
      qualityLock,
      displayMeta,
    });

    const completeBuild = loadCompleteBuild(brandSlug);
    const finalQa = loadFinalQaForBrand(brandSlug);
    const visualDefect = loadVisualDefectForBrand(brandSlug);
    const v36c = loadV36cForBrand(brandSlug);
    const v38 = loadV38ForBrand(brandSlug);

    const reconciliation = reconcileReadinessSignals({
      brandSlug,
      gateInventory,
      completeBuildReport: completeBuild,
      finalQaReport: finalQa,
      visualDefectReport: visualDefect,
      v36cReport: v36c,
      v38BrandResult: v38,
    });

    const classification = classifyReleaseCandidate({ gateInventory, reconciliation });
    const cohort = cohortFor(brandSlug);

    // Incomplete brands never get unlock outcomes
    const safeClassification =
      cohort === "incomplete"
        ? {
            outcome: "not_owner_ready",
            reason: "Incomplete cohort — must remain locked; no active release path",
          }
        : classification;

    const releasePlan = buildActiveReleasePlan({
      brandSlug,
      gateInventory,
      reconciliation,
      classification: safeClassification,
      qualityLock,
      cohort,
    });

    brandResults.push({
      brandSlug,
      recordId: brandApi.id || config?.recordId,
      brandName: brandApi.name,
      cohort,
      dryRun,
      displayState: gateInventory.displayState,
      shouldRenderFullProfile: gateInventory.shouldRenderFullProfile,
      gateInventory,
      reconciliation,
      classification: safeClassification,
      releasePlan,
      qualityLock: {
        externalQualityLockPass: qualityLock.externalQualityLockPass,
        forbiddenStringsFound: qualityLock.forbiddenStringsFound,
        emptyCardsFound: qualityLock.emptyCardsFound,
        helperTextFound: qualityLock.helperTextFound,
        internalNotesFound: qualityLock.internalNotesFound,
        profileInPreparationRendered: qualityLock.profileInPreparationRendered,
        tabsRenderedExternally: qualityLock.tabsRenderedExternally,
      },
      reportSignals: {
        completeBuildReadyForActiveProfile: completeBuild?.readyForActiveProfile ?? null,
        completeBuildGeneratedAt: completeBuild?.generatedAt ?? null,
        finalQaPass: finalQa?.pass ?? finalQa?.readyForActiveProfile ?? null,
        v38DisplayState: v38?.displayState ?? null,
      },
      readyForActiveApproval: false,
      companyValidatedUntouched: true,
    });
  }

  const incompleteControl = buildIncompleteBrandControlCheck(brandResults);
  const primary = brandResults.filter((b) => b.cohort === "primary_release");

  return {
    version: V39_AUDIT_VERSION,
    gateVersion: V39_RELEASE_GATE_VERSION,
    planVersion: V39_RELEASE_PLAN_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    brands,
    liveApiIsSourceOfTruth: true,
    brandResults,
    incompleteControl,
    activeReleaseApplyDesign: buildActiveReleaseApplyCommandDesign(DEFAULT_PRIMARY_BRANDS),
    summary: {
      brandsAudited: brandResults.length,
      primaryCount: primary.length,
      incompleteCount: brandResults.filter((b) => b.cohort === "incomplete").length,
      safeToUnlockAfterApproval: primary.filter(
        (b) => b.classification.outcome === "safe_to_unlock_after_active_approval"
      ).length,
      remediationRequired: primary.filter(
        (b) => b.classification.outcome === "release_remediation_required"
      ).length,
      mappingFixRequired: primary.filter(
        (b) => b.classification.outcome === "false_blocker_due_to_mapping"
      ).length,
      notOwnerReady: primary.filter((b) => b.classification.outcome === "not_owner_ready").length,
      incompleteControlPass: incompleteControl.allControlPass,
      applyExecuted: false,
    },
    guardrails: {
      airtableWrites: false,
      presentationWrites: false,
      registryWrites: false,
      sourceLibraryWrites: false,
      companyValidatedChanges: false,
      activeProfileApproval: false,
      activeReleaseApply: false,
    },
  };
}

export function writeV39Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const mdLines = [
    "# v39 Brand Explorer Active Profile Release Audit",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "**Live API / render contract is source of truth.** Report-only readiness is not release readiness.",
    "",
    "## Summary",
    `- Brands audited: ${report.summary.brandsAudited}`,
    `- Primary safe_to_unlock_after_active_approval: ${report.summary.safeToUnlockAfterApproval}`,
    `- Primary release_remediation_required: ${report.summary.remediationRequired}`,
    `- Primary mapping_fix_required: ${report.summary.mappingFixRequired}`,
    `- Primary not_owner_ready: ${report.summary.notOwnerReady}`,
    `- Incomplete control pass: **${report.summary.incompleteControlPass ? "yes" : "no"}**`,
    `- Active release apply executed: **no**`,
    "",
    "## Per brand",
  ];

  for (const b of report.brandResults) {
    mdLines.push(`### ${b.brandSlug} (${b.cohort})`);
    mdLines.push(`- displayState: \`${b.displayState}\``);
    mdLines.push(`- shouldRenderFullProfile: ${b.shouldRenderFullProfile}`);
    mdLines.push(`- release outcome: **${b.classification.outcome}**`);
    mdLines.push(`- allowed next action: ${b.releasePlan.allowedNextAction}`);
    mdLines.push(`- failed gates: ${(b.gateInventory.failedGates || []).join(", ") || "none"}`);
    mdLines.push(
      `- report readyForActiveProfile: ${b.reportSignals.completeBuildReadyForActiveProfile}`
    );
    mdLines.push(
      `- mismatches: ${(b.reconciliation.mismatchClasses || []).join(", ") || "none"}`
    );
    mdLines.push("");
  }

  mdLines.push("## Designed release apply (NOT executed)");
  mdLines.push("```");
  mdLines.push(report.activeReleaseApplyDesign.command);
  mdLines.push("```");

  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, mdLines.join("\n"));

  const planSlugFile = {
    "everhome-suites": "everhome-suites",
    kimpton: "kimpton",
    "radisson-individuals-by-choice": "radisson-individuals-by-choice",
  };

  for (const b of report.brandResults.filter((x) => x.cohort === "primary_release")) {
    const fileSlug = planSlugFile[b.brandSlug] || b.brandSlug;
    fs.writeFileSync(
      path.join(reportsDir, `brand-explorer-v39-release-plan-${fileSlug}.md`),
      renderReleasePlanMarkdown(b.releasePlan)
    );
  }

  fs.writeFileSync(
    path.join(reportsDir, "brand-explorer-v39-incomplete-brand-control-check.md"),
    renderIncompleteControlMarkdown(report.incompleteControl)
  );

  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  fs.mkdirSync(docsDir, { recursive: true });
  fs.writeFileSync(
    path.join(docsDir, "brand-explorer-v39-active-release-audit.md"),
    [
      "# v39 Brand Explorer Active Profile Release Audit",
      "",
      "Read-only audit that reconciles report readiness vs live Brand Library API / Presentation imageUrl gates.",
      "",
      "```bash",
      "npm run brand-explorer-v39-active-release-audit -- --brands everhome-suites,kimpton,radisson-individuals-by-choice,hotel-indigo,mgallery-collection,design-hotels,small-luxury-hotels-of-the-world --dry-run",
      "```",
      "",
      "## Source of truth",
      "Live `shouldRenderFullProfile` + Presentation `imageUrl` + external DOM quality lock.",
      "complete-build `readyForActiveProfile` alone is never sufficient.",
      "",
      "## Release outcomes",
      "- `safe_to_unlock_after_active_approval`",
      "- `release_remediation_required`",
      "- `false_blocker_due_to_mapping`",
      "- `not_owner_ready`",
      "",
      "## Active release apply",
      "Designed but not executed in v39. Requires explicit gated command with founder + DOM + imageUrl confirms.",
      "",
      "Guardrails: no Company Validated changes; no incomplete brand unlock; no blind active approval.",
    ].join("\n")
  );

  return { jsonPath, mdPath };
}
