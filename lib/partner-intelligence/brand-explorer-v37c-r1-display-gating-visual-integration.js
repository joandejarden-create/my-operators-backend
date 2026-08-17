/**
 * v37C-R1 — Lifestyle Batch Display Gating + Visual Candidate Integration (read-only).
 *
 * No Airtable writes. Audits owner-facing display safety and bridges visual candidates
 * from report packs into factory-consumable readiness diagnostics.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { buildActiveProfileAssetPack } from "./brand-explorer-active-profile-asset-pack-builder.js";
import { buildActiveProfileDraftPlan } from "./brand-explorer-active-profile-draft-builder.js";
import { extendAssetPackWithRenderReadiness } from "./brand-explorer-render-readiness-contract.js";
import { validateBrandV36BContracts } from "./brand-explorer-v36b-contract-validation.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";

export const V37C_R1_VERSION = "v37C-R1";
export const DEFAULT_BRANDS = Object.freeze(["hotel-indigo", "mgallery-collection"]);
export const REPORT_JSON = "brand-explorer-v37c-r1-display-gating-visual-integration.json";
export const REPORT_MD = "brand-explorer-v37c-r1-display-gating-visual-integration.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeUrlKey(url) {
  return nz(url).toLowerCase().replace(/\/+$/, "").split("?")[0];
}

function readJsonIfExists(p) {
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function isGenericIhgHero(url) {
  const u = normalizeUrlKey(url);
  return /digital\.ihg\.com\/is\/image\/ihg\/ihg-[a-z0-9-]+/i.test(u);
}

function classifyCandidate(c, brandSlug) {
  const imageUrl = nz(c.imageUrl);
  const sourcePageUrl = nz(c.sourcePageUrl);
  const propertyName = nz(c.propertyName);
  const genericHeroRisk =
    brandSlug === "hotel-indigo" ? isGenericIhgHero(imageUrl) : /accor.*logo|brand/i.test(imageUrl);
  const wrongBrandRisk =
    brandSlug === "hotel-indigo"
      ? /accor|mgallery|sofitel|pullman|handwritten/i.test(`${imageUrl} ${sourcePageUrl}`)
      : /ihg|hotelindigo|intercontinental/i.test(`${imageUrl} ${sourcePageUrl}`);
  const propertySpecificImage =
    Boolean(imageUrl) &&
    !genericHeroRisk &&
    !wrongBrandRisk &&
    !/logo|icon|placeholder/i.test(imageUrl) &&
    Boolean(sourcePageUrl);
  const acceptedForDraftPlan = propertySpecificImage;
  return {
    ...c,
    imageClassification: nz(c.imageClassification) || "hotel_property_photography",
    propertySpecificImage,
    genericHeroRisk,
    wrongBrandRisk,
    renderReadinessProjection: nz(c.renderReadinessProjection) || (acceptedForDraftPlan ? "candidate_ready" : "candidate_blocked"),
    registryReadinessProjection:
      nz(c.registryReadinessProjection) || (acceptedForDraftPlan ? "registry_candidate_after_review" : "registry_blocked"),
    acceptedForDraftPlan,
    rejectionReason: acceptedForDraftPlan
      ? null
      : !imageUrl
        ? "missing_image_url"
        : genericHeroRisk
          ? "generic_hero_risk"
          : wrongBrandRisk
            ? "wrong_brand_risk"
            : "not_property_specific",
  };
}

function buildNormalizedVisualCandidatePack(brandSlug) {
  const visualPack = readJsonIfExists(path.join(ROOT, "reports", `visual-asset-pack-${brandSlug}-v37a.json`));
  if (!visualPack) {
    return { brandSlug, candidates: [], summary: { accepted: 0, rejected: 0, galleryAccepted: 0, propertyAccepted: 0 } };
  }
  const allRaw = [
    ...(Array.isArray(visualPack.gallery) ? visualPack.gallery : []),
    ...(Array.isArray(visualPack.propertyExamples) ? visualPack.propertyExamples : []),
  ];
  const seen = new Set();
  const candidates = [];
  for (const raw of allRaw) {
    const key = `${nz(raw.intendedSlot)}|${nz(raw.propertyName)}|${normalizeUrlKey(raw.imageUrl)}|${normalizeUrlKey(raw.sourcePageUrl)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    candidates.push(
      classifyCandidate(
        {
          brandSlug,
          propertyName: nz(raw.propertyName),
          propertyMarket: nz(raw.propertyMarket),
          propertyRegion: nz(raw.propertyRegion),
          sourcePageUrl: nz(raw.sourcePageUrl),
          imageUrl: nz(raw.imageUrl),
          imageClassification: nz(raw.imageClassification),
          intendedSlot: nz(raw.intendedSlot),
        },
        brandSlug
      )
    );
  }
  const accepted = candidates.filter((c) => c.acceptedForDraftPlan);
  const galleryAccepted = accepted.filter((c) => c.intendedSlot.startsWith("materials.gallery.")).length;
  const propertyAccepted = accepted.filter((c) => c.intendedSlot === "footprint.openings").length;
  return {
    brandSlug,
    candidates,
    summary: {
      accepted: accepted.length,
      rejected: candidates.length - accepted.length,
      galleryAccepted,
      propertyAccepted,
      completeForDraft: galleryAccepted >= 6 && propertyAccepted >= 3,
    },
  };
}

function parseRatio(value) {
  const m = nz(value).match(/^(\d+)\s*\/\s*(\d+)$/);
  if (!m) return { got: 0, min: 0 };
  return { got: Number(m[1]), min: Number(m[2]) };
}

function detectFallbackRenderingRisk(brandSlug, ctx) {
  const blocks = ctx.brandApi?.brandExplorer?.blocks || [];
  const hasScenarioRows = [1, 2, 3].every((i) => blocks.some((b) => b.slotKey === `overview.scenario.${i}`));
  const hasGallerySix = blocks.filter((b) => /^materials\.gallery\.\d+$/.test(nz(b.slotKey)) && nz(b.imageUrl)).length >= 6;
  const hasOpeningsThree = blocks.filter((b) => b.slotKey === "footprint.openings" && nz(b.imageUrl)).length >= 3;
  const helperTextWouldRender = !hasScenarioRows;
  const stagingPlaceholderRisk = !hasScenarioRows || !hasGallerySix || !hasOpeningsThree;
  const roughKeywordBulletRisk = brandSlug === "hotel-indigo" && !hasScenarioRows;
  return {
    hasScenarioRows,
    hasGallerySix,
    hasOpeningsThree,
    helperTextWouldRender,
    roughKeywordBulletRisk,
    stagingPlaceholderRisk,
  };
}

function classifyDisplayRows(ctx) {
  const rows = ctx.presentationRows || [];
  return rows.map((r) => ({
    recordId: r.recordId,
    slotKey: r.slotKey,
    visible: r.visible !== false,
    imageUrl: nz(r.imageUrl),
    classification: "live_presentation_row",
  }));
}

function buildEligibility(brandSlug, normalizedPack, assetPack, draftPlan, renderContract, externalOwnerRule, fallbackRisk) {
  const gallery = parseRatio(assetPack?.summary?.galleryReady);
  const openings = parseRatio(assetPack?.summary?.propertyExamplesReady);
  const summary = draftPlan?.summary || {};
  const visualCandidatePackReady = normalizedPack.summary.completeForDraft;
  const assetPackReady = gallery.got >= 6 && openings.got >= 3;
  const buildDraftReady =
    (summary.galleryPatches || 0) >= 6 &&
    (summary.propertyPatches || 0) >= 3 &&
    (summary.scenarioPatches || 0) >= 3;
  const externalDisplaySafe =
    !fallbackRisk.helperTextWouldRender &&
    !fallbackRisk.roughKeywordBulletRisk &&
    (externalOwnerRule?.pass !== false);

  const applyDraftAllowed =
    visualCandidatePackReady &&
    assetPackReady &&
    buildDraftReady &&
    renderContract?.pass === true &&
    externalDisplaySafe;

  const reasons = [];
  if (!visualCandidatePackReady) reasons.push("visual_candidate_pack_incomplete");
  if (!assetPackReady) reasons.push("asset_pack_incomplete");
  if (!buildDraftReady) reasons.push("build_draft_incomplete");
  if (renderContract?.pass !== true) reasons.push("render_contract_fail");
  if (!externalDisplaySafe) reasons.push("external_display_not_safe");
  if (brandSlug === "hotel-indigo" && normalizedPack.summary.propertyAccepted < 3) {
    reasons.push("hotel_indigo_property_specific_images_below_minimum");
  }

  return {
    source_ready: true,
    visual_candidate_pack_ready: visualCandidatePackReady,
    asset_pack_ready: assetPackReady,
    build_draft_ready: buildDraftReady,
    external_display_safe: externalDisplaySafe,
    apply_draft_allowed: applyDraftAllowed,
    apply_draft_blocked_reason: applyDraftAllowed ? [] : reasons,
    should_render_externally: applyDraftAllowed && externalDisplaySafe,
    should_hide_external_profile: !applyDraftAllowed || !externalDisplaySafe,
    recommended_next_action: applyDraftAllowed
      ? `run apply-draft dry-run for ${brandSlug}`
      : brandSlug === "hotel-indigo"
        ? "image remediation required (CALA → U.S. → global property-specific replacements)"
        : "bridge candidates into presentation/registry materialization pipeline before apply-draft",
    ready_for_active_approval: false,
  };
}

function findBrandResult(report, slug) {
  return (report?.brandResults || []).find((b) => b.brandSlug === slug || b.slug === slug) || null;
}

function buildBrandMarkdown(result) {
  const lines = [];
  lines.push(`## ${result.brandSlug}`);
  lines.push(`- source_ready: ${result.batchReadiness.source_ready}`);
  lines.push(`- visual_candidate_pack_ready: ${result.batchReadiness.visual_candidate_pack_ready}`);
  lines.push(`- asset_pack_ready: ${result.batchReadiness.asset_pack_ready}`);
  lines.push(`- build_draft_ready: ${result.batchReadiness.build_draft_ready}`);
  lines.push(`- external_display_safe: ${result.batchReadiness.external_display_safe}`);
  lines.push(`- apply_draft_allowed: ${result.batchReadiness.apply_draft_allowed}`);
  lines.push(`- should_hide_external_profile: ${result.batchReadiness.should_hide_external_profile}`);
  lines.push(`- recommended_next_action: ${result.batchReadiness.recommended_next_action}`);
  if (result.batchReadiness.apply_draft_blocked_reason.length) {
    lines.push("- apply_draft_blocked_reason:");
    for (const r of result.batchReadiness.apply_draft_blocked_reason) lines.push(`  - ${r}`);
  }
  return lines.join("\n");
}

export async function runV37CR1DisplayGatingVisualIntegration({ brands = DEFAULT_BRANDS, dryRun = true } = {}) {
  const latestV36B = readJsonIfExists(path.join(ROOT, "reports", "brand-explorer-v36b-contract-validation.json"));
  const latestV36C = readJsonIfExists(path.join(ROOT, "reports", "brand-explorer-v36c-remediation-planner.json"));
  const v37aBatch = readJsonIfExists(path.join(ROOT, "reports", "brand-explorer-v37a-lifestyle-batch-intake.json"));
  const v37bBatch = readJsonIfExists(path.join(ROOT, "reports", "brand-explorer-v37b-lifestyle-batch-source-seeding.json"));

  const brandResults = [];
  for (const brandSlug of brands) {
    const ctx = await loadBrandFactoryContext(brandSlug);
    const assetPack = await buildActiveProfileAssetPack({
      brandSlug,
      presentationRows: ctx.presentationRows,
      registryAssets: ctx.registryAssets,
      brandApi: ctx.brandApi,
    });
    const draftPlan = buildActiveProfileDraftPlan({
      brandSlug,
      assetPack,
      presentationRows: ctx.presentationRows,
      brandBasics: ctx.brandBasics,
      brandApi: ctx.brandApi,
    });
    const renderContract = extendAssetPackWithRenderReadiness(assetPack, {
      presentationRows: ctx.presentationRows,
      brandApi: ctx.brandApi,
      registryAssets: ctx.registryAssets,
    });
    const externalOwnerRule = evaluateExternalOwnerReadinessRule(ctx.presentationRows || []);
    const fallbackRisk = detectFallbackRenderingRisk(brandSlug, ctx);
    const normalizedPack = buildNormalizedVisualCandidatePack(brandSlug);
    const v36bCurrent = await validateBrandV36BContracts(brandSlug, { dryRun: true });
    const v36bLatest = findBrandResult(latestV36B, brandSlug);
    const v36cLatest = findBrandResult(latestV36C, brandSlug);

    const batchReadiness = buildEligibility(
      brandSlug,
      normalizedPack,
      assetPack,
      draftPlan,
      renderContract,
      externalOwnerRule,
      fallbackRisk
    );

    brandResults.push({
      brandSlug,
      displayGatingAudit: {
        liveRowsRendering: classifyDisplayRows(ctx),
        liveRowCount: (ctx.brandApi?.brandExplorer?.blocks || []).length,
        fallbackRendererRisk: fallbackRisk,
        helperFallbackTextVisibleExternally: fallbackRisk.helperTextWouldRender,
        sourceOnlyTreatedAsDraftReadyInUi: batchReadiness.apply_draft_allowed,
        shouldSuppressProfileSections: !batchReadiness.external_display_safe,
        hardRuleStatus:
          "source_only_must_not_change_external_owner_facing_profile_until_presentation_visual_contract_founder_and_approval_gates_pass",
      },
      visualCandidatePack: normalizedPack,
      assetPackSummary: assetPack.summary,
      renderReadinessSummary: renderContract?.summary || null,
      draftPlanSummary: draftPlan.summary,
      v36bCurrent: {
        batchQueue: v36bCurrent.batchQueue,
        externalOwnerScore: v36bCurrent.externalOwnerScore,
        factoryReportSummary: v36bCurrent.factoryReportSummary,
      },
      latestReports: {
        v36b: v36bLatest,
        v36c: v36cLatest,
        v37a: findBrandResult(v37aBatch, brandSlug),
        v37b: findBrandResult(v37bBatch, brandSlug),
      },
      batchReadiness,
    });
  }

  return {
    version: V37C_R1_VERSION,
    generatedAt: new Date().toISOString(),
    mode: dryRun ? "dry-run" : "apply",
    guardrails: {
      noAirtableWrites: true,
      noPresentationWrites: true,
      noRegistryWrites: true,
      noImageFieldWrites: true,
      noActiveProfileApproval: true,
      noCompanyValidatedChanges: true,
      readyForActiveApproval: false,
    },
    brands,
    brandResults,
  };
}

export function writeV37CR1Reports(report, rootDir = ROOT) {
  const reportsDir = path.join(rootDir, "reports");
  const docsDir = path.join(rootDir, "docs", "data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const mdLines = [];
  mdLines.push("# v37C-R1 Display Gating + Visual Candidate Integration");
  mdLines.push("");
  mdLines.push(`Generated: ${report.generatedAt}`);
  mdLines.push(`Mode: **${report.mode}**`);
  mdLines.push("");
  mdLines.push("## Batch readiness");
  for (const brand of report.brandResults) {
    mdLines.push(buildBrandMarkdown(brand));
    mdLines.push("");
  }
  mdLines.push("## Renderer suppression rules (owner-facing)");
  mdLines.push("- Hide helper/fallback text when scenario rows are missing.");
  mdLines.push("- Hide standards placeholder paragraphs for source-only/incomplete profiles.");
  mdLines.push("- Do not treat Source Library-only updates as profile-ready.");
  mdLines.push("- Keep QA/readiness diagnostics in reports, not external UI.");
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, mdLines.join("\n"));

  const hi = report.brandResults.find((b) => b.brandSlug === "hotel-indigo");
  const hiLines = [];
  hiLines.push("# v37C-R1 Hotel Indigo Image Blocker");
  hiLines.push("");
  hiLines.push("- Draft eligibility: **no** unless 3 property-specific and 6 gallery candidates pass.");
  hiLines.push("- Shared IHG hero images rejected (`digital.ihg.com/is/image/ihg/ihg-*`).");
  hiLines.push("- Replacement order: CALA first, then U.S., then global.");
  hiLines.push("");
  hiLines.push("## Accepted candidates");
  for (const c of hi?.visualCandidatePack?.candidates?.filter((x) => x.acceptedForDraftPlan) || []) {
    hiLines.push(`- ${c.propertyName} — ${c.imageUrl}`);
  }
  hiLines.push("");
  hiLines.push("## Rejected candidates");
  for (const c of hi?.visualCandidatePack?.candidates?.filter((x) => !x.acceptedForDraftPlan) || []) {
    hiLines.push(`- ${c.propertyName} — ${c.rejectionReason} — ${c.imageUrl}`);
  }
  const hiPath = path.join(reportsDir, "brand-explorer-v37c-r1-hotel-indigo-image-blocker.md");
  fs.writeFileSync(hiPath, hiLines.join("\n"));

  const mg = report.brandResults.find((b) => b.brandSlug === "mgallery-collection");
  const mgLines = [];
  mgLines.push("# v37C-R1 MGallery Asset-Pack Bridge");
  mgLines.push("");
  mgLines.push("- Candidate bridge source: `reports/visual-asset-pack-mgallery-collection-v37a.json`.");
  mgLines.push("- Goal: make approved candidates consumable by asset-pack builder without registry/presentation writes.");
  mgLines.push(
    `- Current asset-pack summary: gallery ${mg?.assetPackSummary?.galleryReady || "0/6"}, property ${mg?.assetPackSummary?.propertyExamplesReady || "0/3"}, readiness ${mg?.assetPackSummary?.readinessBand || "unknown"}.`
  );
  mgLines.push("");
  mgLines.push("## Candidate status");
  for (const c of mg?.visualCandidatePack?.candidates || []) {
    mgLines.push(
      `- ${c.propertyName} | slot=${c.intendedSlot} | accepted=${c.acceptedForDraftPlan} | reason=${c.rejectionReason || "ok"}`
    );
  }
  const mgPath = path.join(reportsDir, "brand-explorer-v37c-r1-mgallery-asset-pack-bridge.md");
  fs.writeFileSync(mgPath, mgLines.join("\n"));

  const docPath = path.join(
    docsDir,
    "brand-explorer-v37c-r1-display-gating-visual-integration.md"
  );
  fs.writeFileSync(
    docPath,
    [
      "# v37C-R1 Display Gating + Visual Candidate Integration",
      "",
      "Read-only operating-system gap closure for Hotel Indigo + MGallery source-only batch.",
      "",
      "## Command",
      "```bash",
      "npm run brand-explorer-v37c-r1-display-gating-visual-integration -- --brands hotel-indigo,mgallery-collection --dry-run",
      "```",
      "",
      "## Guardrails",
      "- No Airtable writes",
      "- No Presentation writes",
      "- No Registry writes",
      "- No image-field writes",
      "- No active-profile approval",
      "- No Company Validated changes",
    ].join("\n")
  );

  return { jsonPath, mdPath, hiPath, mgPath, docPath };
}
