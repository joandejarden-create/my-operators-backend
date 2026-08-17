/**
 * Lane 2 founder review packets — report-only; no release / restore writes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  LANE2_VERSION,
  resolveFullBuildSlug,
  resolveLane2BrandIdentity,
  listPresentationRowsLight,
  writeLane2Reports,
  LANE2_ROOT,
} from "./brand-explorer-lane2-common.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function readJsonSafe(rel) {
  const p = path.join(ROOT, "reports", rel);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function previewUrl(slug) {
  return `/brand-explorer?brand=${encodeURIComponent(slug)}&beInternalPreview=1`;
}

function recommendationFromGates({ uniquenessPass, roleMatchPass, locked, residual, tabFactoryPass }) {
  if (!locked) return "remediation_required";
  if (!uniquenessPass || !roleMatchPass) return "remediation_required";
  if (tabFactoryPass === false || residual.length) return "approve_after_minor_cleanup";
  return "approve_for_active_release";
}

export async function buildLane2FounderPacket(brandSlug, { gateNotes = {} } = {}) {
  const slug = resolveFullBuildSlug(brandSlug);
  const identity = resolveLane2BrandIdentity(slug);
  const { rows } = await listPresentationRowsLight(identity.recordId, identity.name);

  const uniqueness = evaluateImageUniqueness({ brandSlug: slug, presentationRows: rows });
  const roleMatch = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: rows });

  let brandApi = null;
  try {
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
      },
    };
    await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
    brandApi = res.payload?.brand || null;
  } catch (err) {
    brandApi = null;
  }

  let renderedHtml = "";
  try {
    renderedHtml = renderBrandExplorerHtmlForTest(brandApi || slug, {
      allPanels: true,
      internalPreview: true,
    });
  } catch (err) {
    renderedHtml = `<!-- render_error: ${err.message} -->`;
  }

  const lock = evaluateBrandExternalQualityLock(brandApi || { slug, name: identity.name }, renderedHtml, {
    presentationRows: rows,
  });
  const { shouldRenderFullBrandExplorerProfile } = await import("./brand-explorer-display-state.js");
  const fullPublic =
    brandApi != null ? shouldRenderFullBrandExplorerProfile(brandApi) === true : false;

  const galleryDistinct = uniqueness.galleryDistinctCount ?? 0;
  const scenarioDistinct = uniqueness.scenarioDistinctCount ?? 0;
  const propertyDistinct = uniqueness.propertyExampleDistinctCount ?? 0;

  const residual = [];
  if (galleryDistinct < 6) residual.push(`gallery_distinct_${galleryDistinct}_lt_6`);
  if (scenarioDistinct < 3) residual.push(`scenario_distinct_${scenarioDistinct}_lt_3`);
  if (propertyDistinct < 3) residual.push(`property_distinct_${propertyDistinct}_lt_3`);
  if (roleMatch?.pass !== true) residual.push("image_role_match_caution");
  if (gateNotes.tabFactoryCaution) residual.push("tab_factory_caution");

  const locked = fullPublic !== true;

  const recommendation = recommendationFromGates({
    uniquenessPass: uniqueness.pass === true,
    roleMatchPass: roleMatch?.pass !== false,
    locked,
    residual,
    tabFactoryPass: gateNotes.tabFactoryPass,
  });

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    reportSlug: identity.reportSlug,
    previewUrl: previewUrl(slug),
    publicRestore: false,
    activeRelease: false,
    gateSummary: {
      imageUniquenessPass: uniqueness.pass === true,
      imageRoleMatchPass: roleMatch?.pass !== false,
      externalLocked: locked,
      galleryDistinct,
      scenarioDistinct,
      propertyDistinct,
      ...gateNotes,
    },
    imageCounts: {
      galleryDistinct,
      scenarioDistinct,
      propertyDistinct,
    },
    imageRoleMatch: {
      pass: roleMatch?.pass !== false,
      findings: (roleMatch?.findings || []).slice(0, 12),
    },
    sourceProvenanceSummary: gateNotes.sourceProvenance || "see gate suite reports",
    tabLevel: gateNotes.tabLevel || { note: "see tab-factory-audit report" },
    remainingFounderJudgmentItems: [
      "Confirm property examples feel brand-correct (not sibling soft-brand).",
      "Confirm gallery role captions match the visual story.",
      "Confirm no Company Validated / Active Profile Approved before restore.",
      ...residual.map((r) => `Residual: ${r}`),
    ],
    recommendation,
  };
}

export async function runLane2FounderPackets({
  brands = [],
  gateNotesBySlug = {},
  reportsDir = path.join(LANE2_ROOT, "reports"),
} = {}) {
  const packets = [];
  for (const raw of brands) {
    const slug = resolveFullBuildSlug(raw);
    packets.push(await buildLane2FounderPacket(slug, { gateNotes: gateNotesBySlug[slug] || {} }));
  }

  const summary = {
    version: LANE2_VERSION,
    generatedAt: new Date().toISOString(),
    brands: packets.map((p) => p.brandSlug),
    recommendations: Object.fromEntries(packets.map((p) => [p.brandSlug, p.recommendation])),
    publicRestorePerformed: false,
    releasePerformed: false,
    packets,
  };

  for (const p of packets) {
    const alias = p.reportSlug || p.brandSlug;
    const md = [
      `# Lane 2 Founder Review — ${p.brandName}`,
      ``,
      `- Brand slug: \`${p.brandSlug}\``,
      `- Preview: \`${p.previewUrl}\``,
      `- Recommendation: **${p.recommendation}**`,
      `- Public restore: **false** (separate explicit command required)`,
      `- Active release: **false**`,
      ``,
      `## Gate summary`,
      ``,
      `- Image uniqueness: ${p.gateSummary.imageUniquenessPass}`,
      `- Image role-match: ${p.gateSummary.imageRoleMatchPass}`,
      `- External locked: ${p.gateSummary.externalLocked}`,
      `- Gallery distinct: ${p.imageCounts.galleryDistinct}`,
      `- Scenario distinct: ${p.imageCounts.scenarioDistinct}`,
      `- Property distinct: ${p.imageCounts.propertyDistinct}`,
      ``,
      `## Source provenance`,
      ``,
      `- ${p.sourceProvenanceSummary}`,
      ``,
      `## Tab-level`,
      ``,
      `- ${typeof p.tabLevel === "string" ? p.tabLevel : JSON.stringify(p.tabLevel)}`,
      ``,
      `## Remaining founder judgment`,
      ``,
      ...p.remainingFounderJudgmentItems.map((i) => `- ${i}`),
      ``,
    ];
    fs.writeFileSync(
      path.join(reportsDir, `brand-explorer-lane2-founder-review-${alias}.md`),
      `${md.join("\n")}\n`,
      "utf8"
    );
  }

  const summaryMd = [
    `# Lane 2 Founder Review — Summary`,
    ``,
    `- Generated: ${summary.generatedAt}`,
    `- Brands: ${summary.brands.join(", ")}`,
    `- Public restore performed: **false**`,
    `- Release performed: **false**`,
    ``,
    `## Recommendations`,
    ``,
    ...packets.map((p) => `- **${p.brandName}** (\`${p.brandSlug}\`): \`${p.recommendation}\` — preview \`${p.previewUrl}\``),
    ``,
    `## Next step`,
    ``,
    `Founder approval + explicit public restore command only after Active Profile Approved / Founder Visual Review Pass are intentionally set.`,
    ``,
  ];

  writeLane2Reports({
    jsonPath: path.join(reportsDir, "brand-explorer-lane2-founder-review-summary.json"),
    mdPath: path.join(reportsDir, "brand-explorer-lane2-founder-review-summary.md"),
    json: summary,
    mdLines: summaryMd,
  });

  return summary;
}

export default { runLane2FounderPackets, buildLane2FounderPacket };
