/**
 * v41 — Brand Explorer OS consolidation runner (read-only).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { buildActiveProfileAssetPack } from "./brand-explorer-active-profile-asset-pack-builder.js";
import { extendAssetPackWithRenderReadiness } from "./brand-explorer-render-readiness-contract.js";
import {
  V41_OS_VERSION,
  DEFAULT_OS_BRANDS,
  PRIMARY_RELEASE_SLUGS,
  INCOMPLETE_CONTROL_SLUGS,
  resolveCanonicalBrandState,
} from "./brand-explorer-os-state-machine.js";
import { evaluateBrandExplorerOsGates } from "./brand-explorer-os-gate-evaluator.js";
import { routeBrandExplorerOsAction } from "./brand-explorer-os-action-router.js";
import {
  buildFounderReviewPacket,
  renderFounderReviewPacketMarkdown,
} from "./brand-explorer-os-founder-review-packet.js";
import { evaluateV40cPatchSafety } from "./brand-explorer-os-patch-safety.js";
import { BUILT_BLOCKED_IDENTITIES } from "./brand-explorer-built-blocked-content.js";

export { V41_OS_VERSION, DEFAULT_OS_BRANDS };

export const REPORT_JSON = "brand-explorer-v41-os-consolidation.json";
export const REPORT_MD = "brand-explorer-v41-os-consolidation.md";

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

function resolveOsLookupId(slug) {
  const config = resolveConfig(slug);
  if (config?.recordId) return config.recordId;
  const builtBlocked = BUILT_BLOCKED_IDENTITIES[slug];
  if (builtBlocked?.recordId) return builtBlocked.recordId;
  return slug;
}

async function fetchBrandApiShape(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const lookupId = resolveOsLookupId(slug);
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
  await getBrandLibraryBrandById({ query: { brandId: lookupId }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`brand API fetch failed for ${slug} (lookup=${lookupId}): HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

function cohortFor(slug) {
  if (PRIMARY_RELEASE_SLUGS.includes(slug)) return "primary";
  if (INCOMPLETE_CONTROL_SLUGS.includes(slug)) return "incomplete";
  return "other";
}

export async function evaluateBrandExplorerOsBrand(brandSlug) {
  const config = resolveConfig(brandSlug);
  const ctx = await loadBrandFactoryContext(brandSlug).catch(() => null);
  const brandApi = await fetchBrandApiShape(brandSlug);
  // Live Brand Library blocks are source of truth for rendered gates (align with PVQL).
  // Factory context rows are fallback only when the API returns no Presentation blocks.
  const liveBlocks = brandApi?.brandExplorer?.blocks || [];
  const presentationRows =
    liveBlocks.length > 0 ? liveBlocks : ctx?.presentationRows || [];
  const registryAssets = ctx?.registryAssets || [];

  let assetPack = null;
  let renderContract = null;
  if (ctx) {
    assetPack = await buildActiveProfileAssetPack({
      brandSlug,
      presentationRows,
      registryAssets,
      brandApi,
    }).catch(() => null);
    renderContract = extendAssetPackWithRenderReadiness(assetPack, {
      presentationRows,
      brandApi,
      registryAssets,
    });
  }

  const completeBuild = readJsonIfExists(
    path.join(ROOT, "reports", `brand-explorer-complete-build-${brandSlug}.json`)
  );

  const gateEval = evaluateBrandExplorerOsGates({
    brandSlug,
    brandApi,
    brandConfig: config,
    brandBasics: ctx?.brandBasics,
    presentationRows,
    registryAssets,
    assetPack,
    renderContract,
    completeBuildReport: completeBuild,
  });

  const m = gateEval.metrics;
  const stateResolved = resolveCanonicalBrandState({
    brandExists: m.brandExists,
    factoryConfigExists: m.factoryConfigExists,
    sourceCoverageReady: m.sourceCoverageReady,
    galleryReady: m.galleryReady,
    propertyExamplesReady: m.propertyExamplesReady,
    visualAssetPackReady: m.visualPackReady,
    liveInternalPreviewClean: m.liveInternalPreviewClean,
    residualPresentationDirty: m.residualPresentationDirty,
    externalQualityLockPass: m.externalQualityLockPass,
    externalFullProfileRendered: m.externalFullProfileRendered,
    founderVisualReviewPassed: m.founderVisualReviewPassed,
    activeReleaseApproved: m.activeReleaseApproved,
    reportReadyButLiveBlocked: m.reportReadyButLiveBlocked,
    companyValidatedClaimWithoutEvidence: false,
    brandSpecificSourceValidationPass: m.brandSpecificSourceValidationPass,
    renderedFieldCompletenessPass: m.renderedFieldCompletenessPass,
    goldenContentQualityPass: m.goldenContentQualityPass,
    tabFactoryAuditPass: m.tabFactoryAuditPass,
    sourceProvenanceByTabPass: m.sourceProvenanceByTabPass,
    noEmptyRenderedComponentsPass: m.noEmptyRenderedComponentsPass,
    imageDistinctivenessPass: m.imageDistinctivenessPass,
    imageRoleMatchPass: m.imageRoleMatchPass,
    cohort: cohortFor(brandSlug),
  });

  // Patch safety for this brand (local residual plan)
  const localSafety = evaluateV40cPatchSafety({
    brandResults: [{ brandSlug, residualPlan: gateEval.residualPlan }],
    brands: [brandSlug],
  });

  const routing = routeBrandExplorerOsAction({
    brandSlug,
    canonicalState: stateResolved.canonicalState,
    metrics: m,
    failedGates: gateEval.failedGates,
    stateConflicts: stateResolved.conflicts,
    residualPatchCount: gateEval.residualPlan?.summary?.patchCount || 0,
    v40cApplyAllowed: localSafety.v40cApplyAllowed,
  });

  const brandResult = {
    brandSlug,
    brandName: brandApi.name || brandSlug,
    recordId: brandApi.id || config?.recordId || null,
    cohort: cohortFor(brandSlug),
    displayState: brandApi.brandExplorerDisplayState,
    shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
    canonicalState: stateResolved.canonicalState,
    stateRationale: stateResolved.rationale,
    stateConflicts: stateResolved.conflicts,
    metrics: m,
    visuals: { galleryCount: m.galleryCount, openingsCount: m.openingsCount },
    gateEval: {
      failedGates: gateEval.failedGates,
      trueBlockers: gateEval.trueBlockers,
      falseBlockers: gateEval.falseBlockers,
      gates: gateEval.gates,
      liveInternalHits: gateEval.liveInternalHits,
      presentationForbidden: gateEval.presentationForbidden,
      residualPlan: {
        summary: gateEval.residualPlan.summary,
        patches: gateEval.residualPlan.patches,
      },
      sourceValidation: gateEval.sourceValidation,
      renderedFieldCompleteness: gateEval.renderedFieldCompleteness,
      goldenQuality: gateEval.goldenQuality,
    },
    routing,
    patchSafety: localSafety.byBrand[0] || null,
    guardrails: {
      airtableWrites: false,
      activeRelease: false,
      unlock: false,
      companyValidatedChanges: false,
    },
  };

  brandResult.founderPacket = buildFounderReviewPacket({
    ...brandResult,
    gateEval: brandResult.gateEval,
  });

  return brandResult;
}

export async function runBrandExplorerOs({
  brands = DEFAULT_OS_BRANDS,
  stage = "release-readiness",
  dryRun = true,
} = {}) {
  if (!dryRun) {
    throw new Error("brand-explorer-os is read-only. Use --dry-run only.");
  }

  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await evaluateBrandExplorerOsBrand(brandSlug));
  }

  const patchSafety = evaluateV40cPatchSafety({
    brandResults: brandResults.map((b) => ({
      brandSlug: b.brandSlug,
      residualPlan: b.gateEval.residualPlan,
    })),
    brands: brands.filter((s) => PRIMARY_RELEASE_SLUGS.includes(s)),
  });

  // Re-route primary brands with overall patch safety
  for (const b of brandResults) {
    if (!PRIMARY_RELEASE_SLUGS.includes(b.brandSlug)) continue;
    b.routing = routeBrandExplorerOsAction({
      brandSlug: b.brandSlug,
      canonicalState: b.canonicalState,
      metrics: b.metrics,
      failedGates: b.gateEval.failedGates,
      stateConflicts: b.stateConflicts,
      residualPatchCount: b.gateEval.residualPlan?.summary?.patchCount || 0,
      v40cApplyAllowed: patchSafety.v40cApplyAllowed,
    });
    b.founderPacket = buildFounderReviewPacket({
      ...b,
      gateEval: b.gateEval,
      routing: b.routing,
    });
  }

  const table = brandResults.map((b) => ({
    brandSlug: b.brandSlug,
    canonicalState: b.canonicalState,
    allowedNextAction: b.routing.allowedNextAction,
    blockedActions: b.routing.blockedActions,
    failedGates: b.gateEval.failedGates,
    trueBlockers: b.gateEval.trueBlockers,
    falseBlockers: b.gateEval.falseBlockers,
    stateConflicts: b.stateConflicts,
    remediationRequired: b.routing.remediationRequired,
    founderReviewAllowed: b.routing.founderReviewAllowed,
    activeReleaseAllowed: b.routing.activeReleaseAllowed,
    exactNextCommand: b.routing.exactNextCommand,
  }));

  return {
    version: V41_OS_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands,
    brandResults,
    table,
    patchSafety,
    summary: {
      brandsEvaluated: brandResults.length,
      byState: brandResults.reduce((acc, b) => {
        acc[b.canonicalState] = (acc[b.canonicalState] || 0) + 1;
        return acc;
      }, {}),
      byAction: brandResults.reduce((acc, b) => {
        acc[b.routing.allowedNextAction] = (acc[b.routing.allowedNextAction] || 0) + 1;
        return acc;
      }, {}),
      v40cApplyAllowed: patchSafety.v40cApplyAllowed,
      anyUnlock: false,
      activeReleaseExecuted: false,
    },
    guardrails: {
      airtableWrites: false,
      activeRelease: false,
      unlock: false,
      companyValidatedChanges: false,
      newBrandContent: false,
    },
  };
}

export function writeV41OsReports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");

  const md = [
    "# v41 Brand Explorer OS Consolidation",
    "",
    `Generated: ${report.generatedAt} · stage=\`${report.stage}\``,
    "",
    "Read-only. Live API + internal preview + external DOM + Presentation are source of truth. Report-only readiness is never unlock readiness.",
    "",
    "## Summary",
    "",
    `- Brands: ${report.summary.brandsEvaluated}`,
    `- States: ${JSON.stringify(report.summary.byState)}`,
    `- Actions: ${JSON.stringify(report.summary.byAction)}`,
    `- v40C apply allowed (safety): **${report.summary.v40cApplyAllowed ? "yes" : "no"}**`,
    `- Unlock: **no**`,
    "",
    "## Batch table",
    "",
    "| Brand | State | Next action | Founder review | Active release | True blockers |",
    "|---|---|---|---|---|---|",
  ];

  for (const row of report.table) {
    md.push(
      `| ${row.brandSlug} | ${row.canonicalState} | ${row.allowedNextAction} | ${row.founderReviewAllowed} | ${row.activeReleaseAllowed} | ${(row.trueBlockers || []).slice(0, 4).join("; ") || "—"} |`
    );
  }

  md.push("", "## Exact next commands", "");
  for (const row of report.table) {
    md.push(`### ${row.brandSlug}`);
    md.push("```");
    md.push(row.exactNextCommand || "(none)");
    md.push("```");
    md.push("");
  }

  md.push("## Guardrails", "", "- No Airtable writes · no unlock · no active release · no Company Validated changes", "");
  fs.writeFileSync(mdPath, md.join("\n"), "utf8");

  const founderPaths = {};
  for (const b of report.brandResults.filter((x) => PRIMARY_RELEASE_SLUGS.includes(x.brandSlug))) {
    const fname = `brand-explorer-v41-founder-review-packet-${b.brandSlug}.md`;
    const fpath = path.join(reportsDir, fname);
    fs.writeFileSync(fpath, renderFounderReviewPacketMarkdown(b.founderPacket), "utf8");
    founderPaths[b.brandSlug] = fpath;
  }

  const safetyPath = path.join(reportsDir, "brand-explorer-v41-v40c-patch-safety-check.md");
  const safety = report.patchSafety;
  const safetyMd = [
    "# v41 · v40C Patch Safety Check",
    "",
    `v40C apply allowed: **${safety.v40cApplyAllowed ? "yes" : "no"}**`,
    "",
    `- Total patches: ${safety.totals.totalPatches}`,
    `- Safe: ${safety.totals.safePatches}`,
    `- Risky (mechanical): ${safety.totals.riskyPatches}`,
    `- Rejected: ${safety.totals.rejectedPatches}`,
    "",
  ];
  if (safety.exactApplyCommand) {
    safetyMd.push("## Exact apply command (NOT executed by OS)", "```", safety.exactApplyCommand, "```", "");
  }
  if (safety.blockReasons?.length) {
    safetyMd.push("## Block reasons", "");
    for (const r of safety.blockReasons) safetyMd.push(`- ${r}`);
    safetyMd.push("");
  }
  for (const b of safety.byBrand || []) {
    safetyMd.push(`## ${b.brandSlug}`);
    safetyMd.push(`- patches=${b.totalPatches} safe=${b.safePatches} risky=${b.riskyPatches} rejected=${b.rejectedPatches} applyAllowed=${b.applyAllowed}`);
    for (const ex of b.examples || []) {
      safetyMd.push(`### ${ex.slotKey} · ${ex.field} (${ex.safetyClass})`);
      safetyMd.push(`- before: ${JSON.stringify(ex.before)}`);
      safetyMd.push(`- after: ${JSON.stringify(ex.after)}`);
      safetyMd.push("");
    }
  }
  fs.writeFileSync(safetyPath, safetyMd.join("\n"), "utf8");

  return { jsonPath, mdPath, founderPaths, safetyPath };
}
