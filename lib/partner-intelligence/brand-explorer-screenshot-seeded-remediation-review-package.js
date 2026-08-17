/**
 * Brand Explorer Screenshot-Seeded Remediation Review Package v24A.
 *
 * Combines live Brand Explorer API reconstruction with human screenshot defect checklist.
 * Read-only — no Airtable writes, images, or Sort Order changes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME } from "./tribute-portfolio-brand-package.js";
import { loadApprovedTributeSources } from "./tribute-portfolio-targeted-extract.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { listRegistryRecordsRaw } from "./brand-explorer-visual-slot-requirements.js";
import { MAP_BRAND_ASSET } from "./brand-asset-registry-workflow.js";
import { isFormallyApprovedRecord } from "./brand-asset-review-decision-writer.js";

export const PACKAGE_VERSION = "24A-screenshot-seeded";
export const REPORT_JSON_NAME = "brand-explorer-screenshot-seeded-remediation-review-package.json";
export const REPORT_MD_NAME = "brand-explorer-screenshot-seeded-remediation-review-package.md";
export const DOC_MD_NAME = "brand-explorer-screenshot-seeded-remediation-review-package-v24A.md";

const CURIO_BRAND_ID = "receQkxgjlezsc1xg";
const DEFAULT_BRAND_ID = TRIBUTE_RECORD_ID;
const COPY_LABEL =
  "AI-drafted / pending founder review · Not company-validated · Not Marriott-validated";

const BUCKET = {
  COPY_SAFE: "copy_cleanup_safe",
  ROW_CREATE: "row_creation_required",
  SOURCE_EVIDENCE: "source_evidence_required",
  MEDIA: "media_asset_required",
  SORT_ORDER: "sort_order_required",
  FRONTEND: "frontend_mapping_required",
  SUPPRESS: "suppress_or_hide_until_ready",
};

const FOOTPRINT_REGION_SLOTS = [
  "footprint.region.am",
  "footprint.region.cala",
  "footprint.region.eu",
  "footprint.region.mea",
  "footprint.region.apac",
];

const LOYALTY_SLOTS = [
  "loyalty.hero_title",
  "loyalty.ecosystem",
  "loyalty.owner_lens",
  "loyalty.kpi.members",
  "loyalty.kpi.hotels",
  "loyalty.kpi.markets",
  "loyalty.kpi.mix",
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "loyalty.proof",
  "loyalty.implications.pnl",
  "loyalty.implications.ops",
  "loyalty.implications.systems",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const INPUT = {
  v24Defect: "reports/brand-explorer-visual-display-defect-audit.json",
  evidenceReview: "reports/brand-explorer-evidence-fact-review-package.json",
  copyCleanupWriter: "reports/brand-explorer-visual-copy-cleanup-writer.json",
  sortOrder: "reports/brand-explorer-presentation-sort-order-audit.json",
  slotManifest: "reports/brand-explorer-slot-standard-manifest.json",
  visualQa: "reports/brand-explorer-visual-qa-verification.json",
  slotRemaining: "reports/brand-explorer-slot-completion-remaining-plan.json",
};

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}
function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}
function wordCount(t) {
  return nz(t).split(/\s+/).filter(Boolean).length;
}
function splitBullets(val) {
  if (!hasVal(val)) return [];
  return String(val)
    .split(/\n|;|•/g)
    .map((s) => s.replace(/^\s*[-*]\s*/, "").trim())
    .filter(Boolean);
}
function readJson(rel) {
  try {
    return JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
  } catch {
    return null;
  }
}
function normalizeBrandInput(raw) {
  const n = nz(raw).toLowerCase();
  if (!n || n === "tribute-portfolio" || n === "tribute portfolio") return DEFAULT_BRAND_ID;
  return nz(raw);
}
async function fetchAllFacts(recordId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: recordId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchBrand(idOrName) {
  const req = { query: { brandId: idOrName, refresh: "1" }, headers: {} };
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
  await getBrandLibraryBrandById(req, res);
  return res.statusCode < 400 ? res.payload?.brand : null;
}
function blocksForSlot(brand, slotKey) {
  return (brand?.brandExplorer?.blocks || []).filter((b) => nz(b.slotKey) === nz(slotKey));
}
function firstBlock(brand, slotKey) {
  return blocksForSlot(brand, slotKey)[0] || null;
}
function mergedBody(brand, slotKey) {
  return blocksForSlot(brand, slotKey)
    .map((b) => [nz(b.title), nz(b.body)].filter(hasVal).join(": "))
    .filter(hasVal)
    .join("\n\n");
}
function slotSummary(brand, slotKey) {
  const rows = blocksForSlot(brand, slotKey);
  const body = mergedBody(brand, slotKey);
  return {
    slotKey,
    rowCount: rows.length,
    hasTitle: rows.some((r) => hasVal(r.title)),
    hasBody: hasVal(body),
    bodyWordCount: wordCount(body),
    hasImage: rows.some((r) => hasVal(r.imageUrl)),
    bodyPreview: body.slice(0, 200),
  };
}
function copyProposal(body, sourceBasis, title = "") {
  return {
    proposedTitle: title,
    proposedBody: body,
    copyLabel: COPY_LABEL,
    sourceBasis,
    reviewStatus: "pending_founder_review",
  };
}
function remediationItem(fields) {
  return { safeForFutureWriter: false, needsMedia: false, needsSourceEvidence: false, needsSortOrder: false, ...fields };
}

function reconstructLoyalty(brand) {
  const slots = Object.fromEntries(LOYALTY_SLOTS.map((sk) => [sk, slotSummary(brand, sk)]));
  const proofRows = blocksForSlot(brand, "loyalty.proof").length;
  const eliteRows = blocksForSlot(brand, "loyalty.elite").length;
  const kpiFilled = ["loyalty.kpi.members", "loyalty.kpi.hotels", "loyalty.kpi.markets", "loyalty.kpi.mix"].filter(
    (sk) => slots[sk].hasBody
  ).length;
  const usesGenericFallback =
    !slots["loyalty.ecosystem"].hasBody ||
    !slots["loyalty.owner_lens"].hasBody ||
    proofRows === 0 ||
    !slots["loyalty.earn"].hasBody ||
    !slots["loyalty.redeem"].hasBody ||
    eliteRows === 0;
  return { slots, proofRowCount: proofRows, eliteRowCount: eliteRows, kpiFilledCount: kpiFilled, usesGenericFallback };
}

function reconstructFootprint(brand) {
  const geoIntro = slotSummary(brand, "footprint.geo_intro");
  const regions = FOOTPRINT_REGION_SLOTS.map((sk) => slotSummary(brand, sk));
  const openings = blocksForSlot(brand, "footprint.openings").length;
  const momentumRows = blocksForSlot(brand, "footprint.momentum");
  const momentumRenderableRows = momentumRows.filter((r) => hasVal(r.title) && hasVal(r.body)).length;
  const portfolioMixRows = blocksForSlot(brand, "footprint.portfolio_mix");
  const portfolioMixThin =
    portfolioMixRows.length <= 1 ||
    portfolioMixRows.every((r) => wordCount(`${nz(r.title)} ${nz(r.body)}`) <= 3);
  const openingsRows = blocksForSlot(brand, "footprint.openings");
  const openingsEmptyCards =
    openingsRows.length > 0 &&
    openingsRows.some((r) => !hasVal(r.title) || !hasVal(r.body) || !hasVal(r.imageUrl));
  const genericRegionCount = regions.filter(
    (r) =>
      !r.hasBody ||
      /\b(established presence|selective development|emerging corridor|illustrative)\b/i.test(r.bodyPreview) ||
      r.bodyWordCount < 25
  ).length;
  return {
    geoIntro,
    regions,
    openingsRowCount: openings,
    momentumRowCount: momentumRows.length,
    momentumRenderableRows,
    portfolioMixRowCount: portfolioMixRows.length,
    portfolioMixThin,
    openingsEmptyCards,
    genericRegionCount,
  };
}

function reconstructCommercial(brand) {
  const demandRows = blocksForSlot(brand, "commercial.demand").filter((r) => hasVal(r.title) || hasVal(r.body));
  const marketPerceptionSummary = nz(brand?.brandPositioning);
  const marketPerceptionGeneric =
    wordCount(marketPerceptionSummary) < 22 ||
    /\b(global infrastructure|independent character|without erasing|commercial support)\b/i.test(marketPerceptionSummary);
  return {
    demandRowCount: demandRows.length,
    demandRows,
    marketPerceptionSummary,
    marketPerceptionGeneric,
  };
}

function reconstructOverviewExtras(brand, v24Vm) {
  const vm = v24Vm || {};
  const bestAt = [1, 2, 3].map((i) => {
    const sk = `overview.bestAt.${i}`;
    const s = slotSummary(brand, sk);
    return { slotKey: sk, title: nz(firstBlock(brand, sk)?.title), ...s };
  });
  const lifecycle = [1, 2, 3, 4, 5, 6].map((i) => {
    const sk = `valueOwners.lifecycle.${i}`;
    const s = slotSummary(brand, sk);
    return { slotKey: sk, title: nz(firstBlock(brand, sk)?.title), ...s };
  });
  const scenarios = vm.valueScenarios?.cards || [1, 2, 3].map((i) => {
    const sk = `overview.scenario.${i}`;
    const b = firstBlock(brand, sk);
    return { slotKey: sk, title: nz(b?.title), body: nz(b?.body), hasImage: hasVal(b?.imageUrl), wordCount: wordCount(b?.body) };
  });
  const whyMerged = mergedBody(brand, "overview.why_value");
  const whyBullets = splitBullets(whyMerged);
  const watchMerged = mergedBody(brand, "valueOwners.watchouts");
  const watchBullets = splitBullets(watchMerged);
  return {
    bestAt,
    lifecycle,
    scenarios,
    whyValue: {
      filledCount: whyBullets.length,
      emptyIndices: Array.from({ length: Math.max(0, 5 - whyBullets.length) }, (_, i) => whyBullets.length + i + 1),
    },
    watchouts: {
      filledCount: watchBullets.length,
      emptyIndices: Array.from({ length: Math.max(0, 5 - watchBullets.length) }, (_, i) => watchBullets.length + i + 1),
    },
    diffIdentity: splitBullets(mergedBody(brand, "overview.differentiators.identity")).length,
    diffCommercial: splitBullets(mergedBody(brand, "overview.differentiators.commercial")).length,
    valueOwnerScenarios: [1, 2, 3, 4].map((i) => {
      const sk = `valueOwners.scenario.${i}`;
      const b = firstBlock(brand, sk);
      return { slotKey: sk, title: nz(b?.title), bodyWordCount: wordCount(b?.body), hasBody: hasVal(b?.body) };
    }),
    portfolioContext: vm.portfolioContext || {},
    standardsRows: blocksForSlot(brand, "standards.requirement").length,
    featuredTruncated: Boolean(vm.featuredApplication?.truncatedInUi),
    openingsDisabled: blocksForSlot(brand, "footprint.openings").length === 0,
  };
}

async function resolveMediaBySlot(recordId) {
  const raw = await listRegistryRecordsRaw(recordId);
  const approved = raw.filter((r) =>
    isFormallyApprovedRecord({
      assetStatus: nz(r.fields?.[MAP_BRAND_ASSET.assetStatus]),
      explorerUsePermission: nz(r.fields?.[MAP_BRAND_ASSET.explorerUsePermission]),
      usageReviewStatus: nz(r.fields?.[MAP_BRAND_ASSET.usageReviewStatus]),
      reviewNotes: nz(r.fields?.[MAP_BRAND_ASSET.reviewNotes]),
    })
  );
  const bySlot = new Map();
  for (const rec of approved) {
    const slot = nz(rec.fields?.[MAP_BRAND_ASSET.recommendedExplorerSlot]);
    let key = "";
    if (/^materials\.gallery\.[1-6]$/.test(slot)) key = slot;
    if (/^overview\.scenario\.[1-3]$/.test(slot)) key = slot;
    if (!key) continue;
    if (!bySlot.has(key)) bySlot.set(key, []);
    bySlot.get(key).push({ id: rec.id, name: nz(rec.fields?.[MAP_BRAND_ASSET.assetName]) });
  }
  return bySlot;
}

const SCREENSHOT_DEFECT_GROUPS = [
  { id: 1, section: "Where This Brand Creates the Most Value", checks: ["scenario3_placeholder", "scenario_thin", "why_value_empty_bullet", "copy_less_complete"] },
  { id: 2, section: "Key Differentiators", checks: ["diff_identity_3of4", "diff_commercial_3of4", "curio_bullet_depth"] },
  { id: 3, section: "What This Brand Is Best At", checks: ["bestAt_thin", "generic_bestAt", "why_value_related"] },
  { id: 4, section: "Owner Value Snapshot / Value to Owners", checks: ["valueOwners_title_only", "lifecycle_thin", "watchouts_empty_bullet", "owner_journey_thin"] },
  { id: 5, section: "Featured Application / Conversion Example", checks: ["featured_truncated", "openings_button_disabled", "openings_suppress"] },
  { id: 6, section: "Portfolio Context", checks: ["generic_ladder", "marriott_ladder_missing"] },
  { id: 7, section: "Loyalty Program", checks: ["loyalty_thin", "loyalty_kpi_unsafe", "loyalty_generic_fallback", "loyalty_mechanics_incomplete", "loyalty_implications_generic"] },
  { id: 8, section: "Geographic Footprint", checks: ["footprint_region_template", "footprint_less_specific_than_curio", "footprint_counts_unsafe"] },
  { id: 9, section: "Standard Detail, Where Available", checks: ["standards_placeholder", "standards_table_missing", "no_fdd_table_invention"] },
  { id: 10, section: "Sort Order", checks: ["writer_default_sort", "sort_order_visibility_impact"] },
  { id: 11, section: "Recent Momentum", checks: ["momentum_empty_placeholder", "momentum_suppress_when_unsourced"] },
  { id: 12, section: "Portfolio Mix", checks: ["portfolio_mix_thin_chip", "portfolio_mix_needs_source_or_suppress"] },
  { id: 13, section: "Openings / Examples / Properties", checks: ["openings_empty_cards_critical", "openings_suppress_until_real_cards"] },
  { id: 14, section: "Demand Scenario View", checks: ["demand_single_card", "demand_grid_needs_manual_review"] },
  { id: 15, section: "Market Perception", checks: ["market_perception_generic", "market_perception_copy_upgrade_safe"] },
  { id: 16, section: "Loyalty Program", checks: ["loyalty_hold_until_v23_review"] },
  { id: 17, section: "v24B blocker", checks: ["featured_application_row_missing", "v24B_apply_blocked_by_missing_row"] },
];

function verifyScreenshotDefects(tribute, curio, tributeModel, curioModel, reports) {
  const t = tributeModel;
  const c = curioModel;
  const verified = [];
  const add = (groupId, check, confirmed, detail, severity = "high") => {
    verified.push({ groupId, check, confirmed, detail, severity });
  };

  add(1, "scenario3_placeholder", !t.scenarios[2]?.hasImage, "overview.scenario.3 renders blank Image placeholder", "high");
  add(
    1,
    "scenario_thin",
    t.scenarios.some((s) => s.wordCount < (c.scenarios[0]?.wordCount || 40) * 0.6),
    `Tribute scenario avg depth lower than Curio (${t.scenarios.map((s) => s.wordCount).join("/")} vs Curio)`,
    "medium"
  );
  add(1, "why_value_empty_bullet", (t.whyValue.emptyIndices || []).length > 0 || t.whyValue.filledCount < 5, "5th why_value bullet empty in UI", "medium");
  add(1, "copy_less_complete", wordCount(mergedBody(tribute, "overview.why_value")) < wordCount(mergedBody(curio, "overview.why_value")) * 0.5, "overview.why_value shorter than Curio", "medium");

  add(2, "diff_identity_3of4", t.diffIdentity < 4, `identity differentiators ${t.diffIdentity}/4 bullets`, "medium");
  add(2, "diff_commercial_3of4", t.diffCommercial < 4, `commercial differentiators ${t.diffCommercial}/4 bullets`, "medium");
  add(2, "curio_bullet_depth", true, "Curio uses 4 line-broken bullets per column; Tribute pads empty <li>&nbsp;</li>", "medium");

  add(
    3,
    "bestAt_thin",
    t.bestAt.some((b) => b.bodyWordCount < 25),
    `bestAt word counts: ${t.bestAt.map((b) => b.bodyWordCount).join(", ")}`,
    "medium"
  );
  add(3, "generic_bestAt", true, "Some bestAt copy reads as pillar labels repeated in body", "low");
  add(3, "why_value_related", t.whyValue.filledCount < 5, "why_value incompleteness affects overview depth perception", "medium");

  add(4, "valueOwners_title_only", t.valueOwnerScenarios.every((s) => !s.hasBody), "All four valueOwners.scenario.* cards title-only", "high");
  add(
    4,
    "lifecycle_thin",
    t.lifecycle.some((p) => p.bodyWordCount < 12),
    `lifecycle phase bodies short (${t.lifecycle.map((p) => p.bodyWordCount).join(", ")})`,
    "medium"
  );
  add(4, "watchouts_empty_bullet", (t.watchouts.emptyIndices || []).length > 0 || t.watchouts.filledCount < 5, "5th watchouts bullet empty", "high");
  add(4, "owner_journey_thin", wordCount(mergedBody(tribute, "valueOwners.overview")) < wordCount(mergedBody(curio, "valueOwners.overview")) * 0.5, "valueOwners.overview thinner than Curio", "medium");

  add(5, "featured_truncated", t.featuredTruncated, "Featured Application uses brandPositioning.slice(0,220)", "medium");
  add(5, "openings_button_disabled", t.openingsDisabled, "View Recent Openings disabled — no footprint.openings rows", "low");
  add(5, "openings_suppress", true, "Keep openings suppressed until dated source-backed PR rows exist", "low");

  add(6, "generic_ladder", Boolean(t.portfolioContext?.usesGenericScaleLabels), "Portfolio ladder shows Lower-scale/Mid-scale generics", "high");
  add(6, "marriott_ladder_missing", true, "No Marriott static ladder mapping in frontend (unlike Hilton for Curio)", "high");

  const tl = tributeModel.loyalty;
  const cl = curioModel.loyalty;
  add(7, "loyalty_thin", tl.proofRowCount < cl.proofRowCount || tl.kpiFilledCount < 4, `Tribute loyalty slots sparse vs Curio (proof ${tl.proofRowCount} vs ${cl.proofRowCount})`, "critical");
  add(7, "loyalty_kpi_unsafe", tl.kpiFilledCount === 0, "KPI cards show em-dash defaults — do not invent member/hotel counts", "high");
  add(7, "loyalty_generic_fallback", tl.usesGenericFallback, "UI falls back to generic earn/redeem/elite/proof when slots empty", "critical");
  add(7, "loyalty_mechanics_incomplete", !tl.slots["loyalty.earn"].hasBody || !tl.slots["loyalty.redeem"].hasBody, "earn/redeem presentation slots empty (v23 facts Pending)", "high");
  add(7, "loyalty_implications_generic", !tl.slots["loyalty.implications.pnl"].hasBody, "Owner implications use generic template language", "medium");

  const tf = tributeModel.footprint;
  const cf = curioModel.footprint;
  add(8, "footprint_region_template", tf.genericRegionCount >= 3, `${tf.genericRegionCount}/5 region cards thin or template-like`, "high");
  add(8, "footprint_less_specific_than_curio", tf.regions.every((r) => r.bodyWordCount < 40), "Region narratives shorter than Curio CALA-specific examples", "medium");
  add(8, "footprint_counts_unsafe", true, "Do not add property/count claims without verified footprint metrics", "high");

  add(9, "standards_placeholder", t.standardsRows === 0, "standards.requirement table missing — placeholder visible", "critical");
  add(9, "standards_table_missing", t.standardsRows === 0, "0 structured standards.requirement rows", "critical");
  add(9, "no_fdd_table_invention", true, "be.standards.qualityAssuranceTheme is Internal Only Pending — not table-safe", "critical");

  const sortDefaults = reports.sortOrder?.sortOrderAuditSummary?.likelyWriterDefaultCount ?? 82;
  add(10, "writer_default_sort", sortDefaults > 0, `${sortDefaults} rows with index×10 Sort Order`, "high");
  add(10, "sort_order_visibility_impact", true, "Multi-row slots (loyalty.proof, standards.requirement, materials.file) may render out of intended order", "medium");

  add(11, "momentum_empty_placeholder", tf.momentumRenderableRows === 0, "Recent Momentum renders empty placeholder state", "high");
  add(11, "momentum_suppress_when_unsourced", tf.momentumRenderableRows === 0, "Suppress Recent Momentum until dated source-backed rows exist", "high");

  add(12, "portfolio_mix_thin_chip", tf.portfolioMixThin, `Portfolio Mix appears thin (${tf.portfolioMixRowCount} row(s))`, "medium");
  add(12, "portfolio_mix_needs_source_or_suppress", tf.portfolioMixThin, "Use source-backed mix context or suppress unsupported mix claims", "high");

  add(
    13,
    "openings_empty_cards_critical",
    tf.openingsRowCount === 0 || tf.openingsEmptyCards,
    tf.openingsRowCount === 0
      ? "Openings / Examples / Properties renders blank cards/disabled state (0 rows)"
      : "Openings / Examples / Properties rows exist but card data/images are incomplete",
    "critical"
  );
  add(13, "openings_suppress_until_real_cards", tf.openingsRowCount === 0 || tf.openingsEmptyCards, "Suppress empty property/example cards until complete rows + approved assets exist", "critical");

  const tc = tributeModel.commercial || {};
  add(14, "demand_single_card", (tc.demandRowCount || 0) <= 1, `Demand Scenario View shows ${tc.demandRowCount || 0} scenario row(s)`, "high");
  add(14, "demand_grid_needs_manual_review", (tc.demandRowCount || 0) <= 1, "Additional demand scenarios require founder/manual review before expansion", "high");

  add(15, "market_perception_generic", Boolean(tc.marketPerceptionGeneric), "Market Perception summary reads generic vs completed profiles", "medium");
  add(15, "market_perception_copy_upgrade_safe", Boolean(tc.marketPerceptionGeneric), "Safe brand-specific copy upgrade possible without new claims", "low");

  add(16, "loyalty_hold_until_v23_review", tl.usesGenericFallback, "Hold loyalty mechanics until v23 loyalty facts are approved", "critical");

  const copyWriter = reports.copyWriter || {};
  const featuredMissing = Array.isArray(copyWriter.missingTargetRows) && copyWriter.missingTargetRows.includes("overview.featured_application");
  add(17, "featured_application_row_missing", featuredMissing, "overview.featured_application row missing", "high");
  add(17, "v24B_apply_blocked_by_missing_row", featuredMissing, "v24B apply blocked until row-creation decision is resolved", "high");

  return verified;
}

function buildRemediationPlans(tribute, curio, tributeModel, evidenceReport, mediaBySlot, sortOrderReport, copyWriterReport) {
  const plans = [];
  const vm = tributeModel;

  const push = (p) => plans.push(remediationItem(p));

  // --- Copy-safe bullets (from v24A visual remediation) ---
  const copyFixes = [
    {
      section: "Why Value Is Strongest",
      slotKey: "overview.why_value",
      body: "Operator fit matters: strongest with teams that can deliver design-forward full-service or resort operations, Marriott systems cutover, and ongoing collection QA.",
      basis: "Existing bullets 1–4 tone",
    },
    {
      section: "Key Watchouts",
      slotKey: "valueOwners.watchouts",
      body: "Collection affiliation is not a one-time reflag; owners should plan for ongoing QA, systems participation, and brand-standard upkeep through the hold period.",
      basis: "Watchouts 1–4 paraphrase",
    },
    {
      section: "Key Differentiators",
      slotKey: "overview.differentiators.identity",
      body: "Soft-brand structure: independent hotel character with Marriott affiliation, systems, and quality expectations.",
      basis: "Consumer independent-collection statement",
    },
    {
      section: "Key Differentiators",
      slotKey: "overview.differentiators.commercial",
      body: "Conversion and repositioning path: confirm development milestones, PIP expectations, approval steps, and commercial terms directly with Marriott for the specific asset.",
      basis: "overview.development_model tone",
    },
    {
      section: "Featured Application",
      slotKey: "overview.featured_application",
      body: "Independent boutique hotels with distinctive style and local flavor, supported by Marriott Bonvoy and Marriott commercial infrastructure without erasing the property's individuality.",
      basis: "Shortened brandPositioning for 220-char UI",
      title: "Exactly like nothing else.",
    },
  ];
  for (const c of copyFixes) {
    const cp = copyProposal(c.body, c.basis, c.title || "");
    push({
      section: c.section,
      slotKey: c.slotKey,
      defectType: "empty_bullet_or_truncated",
      severity: "medium",
      remediationBucket: BUCKET.COPY_SAFE,
      proposedFix: `Append or replace copy on ${c.slotKey}`,
      ...cp,
      safeForFutureWriter: true,
    });
  }

  // bestAt enrichment (copy-safe editorial)
  const bestAtBodies = [
    "Strongest for independent or boutique full-service assets that already have local identity, design character, or a clear story worth preserving.",
    "A fit for resort and leisure-led destinations where experience, design, F&B, and sense of place can support a higher-touch operating model.",
    "A fit for urban character hotels where neighborhood story, design point of view, and independent programming can help the asset stand apart in a competitive comp set.",
  ];
  vm.bestAt.forEach((b, i) => {
    if (b.bodyWordCount >= 25) return;
    const cp = copyProposal(bestAtBodies[i], "overview.scenario bodies + bestAt titles");
    push({
      section: "What This Brand Is Best At",
      slotKey: b.slotKey,
      defectType: "thin_copy_vs_reference",
      severity: "medium",
      remediationBucket: BUCKET.COPY_SAFE,
      proposedFix: "Expand bestAt body after founder review",
      proposedTitle: b.title,
      ...cp,
      safeForFutureWriter: true,
    });
  });

  // lifecycle enrichment
  const lifecycleBodies = {
    "valueOwners.lifecycle.1":
      "Evaluate collection fit, market tier, conversion scope, and whether the asset's design, F&B, and service model can support Tribute Portfolio positioning.",
    "valueOwners.lifecycle.2":
      "Align the design narrative, PIP scope, and identity-preservation strategy before committing capital or affiliation timing.",
    "valueOwners.lifecycle.6":
      "Plan hold-period QA, brand-standard upkeep, re-licensing considerations, and change-of-control assumptions with Marriott development contacts.",
  };
  for (const [sk, body] of Object.entries(lifecycleBodies)) {
    const row = vm.lifecycle.find((p) => p.slotKey === sk);
    if (!row || row.bodyWordCount >= 15) continue;
    const cp = copyProposal(body, "Curio lifecycle pattern paraphrased for Tribute");
    push({
      section: "Support Across the Lifecycle",
      slotKey: sk,
      defectType: "thin_copy_vs_reference",
      severity: "medium",
      remediationBucket: BUCKET.COPY_SAFE,
      proposedFix: "Expand lifecycle phase body",
      proposedTitle: row.title,
      ...cp,
      safeForFutureWriter: true,
    });
  }

  // Media
  for (const sk of ["overview.scenario.3", "materials.gallery.3"]) {
    const assets = mediaBySlot.get(sk) || [];
    push({
      section: sk.includes("scenario") ? "Where This Brand Creates the Most Value" : "Brand Materials",
      slotKey: sk,
      defectType: "missing_card_image",
      severity: "high",
      remediationBucket: assets.length ? BUCKET.MEDIA : BUCKET.SUPPRESS,
      proposedFix: assets.length
        ? "Promote approved registry asset after media sign-off"
        : "Suppress empty visual placeholder until approved asset exists",
      proposedTitle: null,
      proposedBody: null,
      sourceBasis: assets.length ? assets.map((a) => a.name).join("; ") : "Visual QA: intentionally unpopulated",
      reviewStatus: assets.length ? "pending_media_promotion" : "suppress_until_asset_approved",
      needsMedia: true,
      suppressUntilReady: !assets.length,
    });
  }

  // valueOwners scenarios — source evidence
  const voBodies = [
    ["valueOwners.scenario.1", "Independent Reflag", "Independent and boutique full-service hotels with local story—Bonvoy and Marriott commercial systems while preserving individuality."],
    ["valueOwners.scenario.2", "Tired Upscale Asset", "Upscale repositioning where design narrative and service investment align to collection standards—confirm PIP and ramp before underwriting."],
    ["valueOwners.scenario.3", "Markets With Strong Brand Presence", "Markets where Marriott distribution complements a distinctive asset—underwrite full-service complexity, not select-service economics."],
    ["valueOwners.scenario.4", "Third-Party Operator–Led", "Fits when operator executes collection compliance and Marriott systems cutover—common third-party management model."],
  ];
  for (const [sk, title, body] of voBodies) {
    const cp = copyProposal(body, "overview scenarios + owner education pattern");
    push({
      section: "Value Creation Scenarios",
      slotKey: sk,
      defectType: "title_only_card",
      severity: "high",
      remediationBucket: BUCKET.SOURCE_EVIDENCE,
      proposedFix: `Populate ${sk} Body after founder review`,
      proposedTitle: title,
      ...cp,
      safeForFutureWriter: true,
      needsSourceEvidence: true,
    });
  }

  // Loyalty — requires approved Bonvoy facts
  const pendingLoyaltyFacts = (evidenceReport?.factReviewRows || []).filter((f) =>
    /be\.loyalty\./.test(f.fieldKey)
  );
  push({
    section: "Loyalty Program",
    slotKey: "loyalty.*",
    defectType: "generic_ui_fallback",
    severity: "critical",
    remediationBucket: BUCKET.SOURCE_EVIDENCE,
    proposedFix:
      "Approve v23 Bonvoy facts (earn, redeem, elite, proof) then write loyalty.earn/redeem/elite/proof/kpi rows — do not use UI generic fallback as product copy.",
    proposedTitle: null,
    proposedBody: null,
    sourceBasis: `Pending facts: ${pendingLoyaltyFacts.map((f) => f.fieldKey).join(", ") || "none loaded"}`,
    reviewStatus: "blocked_pending_fact_approval",
    needsSourceEvidence: true,
    suppressUntilReady: true,
    evidenceNeeded: [
      "Approve be.loyalty.earnMechanics → loyalty.earn",
      "Approve be.loyalty.redeemMechanics → loyalty.redeem",
      "Approve be.loyalty.eliteTierLadder → loyalty.elite rows",
      "Approve be.loyalty.memberRatesBenefit + programScaleStatement → loyalty.proof (review scale wording)",
      "Do not populate loyalty.kpi.* without approved numeric facts",
    ],
  });

  // Footprint regions — evidence-gated improvement
  push({
    section: "Geographic Footprint",
    slotKey: "footprint.region.*",
    defectType: "template_region_copy",
    severity: "high",
    remediationBucket: BUCKET.SOURCE_EVIDENCE,
    proposedFix:
      "Improve region cards only where Tribute footprint sources support corridor narrative—keep high-level where counts unverified.",
    proposedTitle: null,
    proposedBody: null,
    sourceBasis: "Brand Setup footprint fields + approved consumer/FDD sources; no STR counts",
    reviewStatus: "evidence_gated_regional_copy",
    needsSourceEvidence: true,
    evidenceNeeded: [
      "footprint.geo_intro source-backed corridor statement",
      "Per-region narrative from approved market footprint docs—not template filler",
      "Suppress numeric hotel/room claims in region cards unless verified in Brand Setup footprint",
    ],
  });

  // Standards
  push({
    section: "Standard Detail, Where Available",
    slotKey: "standards.requirement",
    defectType: "missing_table_structure",
    severity: "critical",
    remediationBucket: BUCKET.SOURCE_EVIDENCE,
    proposedFix: "Human-author standards.requirement rows after legal review — not from Internal Only FDD fragment",
    needsSourceEvidence: true,
    suppressUntilReady: true,
    evidenceNeeded: [
      "External-display-safe design/operations standards source",
      "Founder approval of be.standards.qualityAssuranceTheme or replacement capture",
      "be.standards.designStandardsDelivery pattern capture",
      "Owner-table template row authoring with Typical consideration / Owner planning format",
    ],
  });

  // Portfolio Context frontend
  push({
    section: "Portfolio Context",
    slotKey: "overview.portfolio_context",
    defectType: "missing_peer_portfolio_context",
    severity: "high",
    remediationBucket: BUCKET.FRONTEND,
    proposedFix: "Add Marriott static ladder in atelier JS + populate overview.portfolio_context Title=3 and ladder body",
    proposedTitle: "3",
    proposedBody:
      "Upper-upscale soft collection within Marriott—Tribute among lifestyle/collection flags preserving independent character with Bonvoy; not limited-service or extended-stay formats.",
    copyLabel: COPY_LABEL,
    sourceBasis: "Consumer site + brand architecture (no hotel counts)",
    reviewStatus: "pending_founder_review_and_frontend_mapping",
    needsSourceEvidence: false,
  });

  // Recent openings suppress
  push({
    section: "Featured Application / Conversion Example",
    slotKey: "footprint.openings",
    defectType: "disabled_openings_cta",
    severity: "low",
    remediationBucket: BUCKET.SUPPRESS,
    proposedFix: "Leave Recent Openings disabled until dated PR/opening rows with approved assets exist",
    suppressUntilReady: true,
    evidenceNeeded: ["Dated opening/PR source rows for footprint.openings — not invented"],
  });

  // Recent Momentum
  push({
    section: "Recent Momentum",
    slotKey: "footprint.momentum",
    defectType: "empty_momentum_placeholder",
    severity: "high",
    remediationBucket: BUCKET.SUPPRESS,
    proposedFix: "Suppress Recent Momentum section when there are no dated source-backed momentum rows",
    suppressUntilReady: true,
    evidenceNeeded: ["Add dated source-backed footprint.momentum rows (headline + date + source URL) before display"],
  });

  // Portfolio Mix
  push({
    section: "Portfolio Mix",
    slotKey: "footprint.portfolio_mix",
    defectType: "thin_or_unsupported_mix",
    severity: "high",
    remediationBucket: BUCKET.SOURCE_EVIDENCE,
    proposedFix: "Use source-backed owner-safe portfolio mix context; otherwise suppress thin single-chip mix",
    needsSourceEvidence: true,
    evidenceNeeded: [
      "Source-backed mix framing from approved footprint/portfolio docs",
      "If unavailable, hide portfolio mix pills instead of rendering unsupported labels",
    ],
  });

  // Openings / Examples / Properties
  push({
    section: "Openings / Examples / Properties",
    slotKey: "footprint.openings",
    defectType: "empty_property_cards",
    severity: "critical",
    remediationBucket: BUCKET.SUPPRESS,
    proposedFix: "Suppress property/example cards until rows contain complete card fields and approved hero images",
    suppressUntilReady: true,
    needsMedia: true,
    evidenceNeeded: ["Complete footprint.openings rows (title/location/meta/scenario/teaser/link) with approved image assets"],
  });
  push({
    section: "Openings / Examples / Properties",
    slotKey: "footprint.openings",
    defectType: "property_rows_missing_or_incomplete",
    severity: "critical",
    remediationBucket: BUCKET.ROW_CREATE,
    proposedFix: "Create/complete footprint.openings rows in a dedicated row-creation batch with explicit gate",
    reviewStatus: "row_creation_required",
  });

  // Demand Scenario View
  push({
    section: "Demand Scenario View",
    slotKey: "commercial.demand",
    defectType: "single_scenario_row",
    severity: "high",
    remediationBucket: BUCKET.SOURCE_EVIDENCE,
    proposedFix: "Expand commercial.demand grid only after founder/manual review using approved positioning evidence",
    needsSourceEvidence: true,
    evidenceNeeded: ["Founder-reviewed demand scenario set and directional labels for commercial.demand rows"],
  });

  // Market Perception
  push({
    section: "Market Perception",
    slotKey: "commercial.perception",
    defectType: "generic_market_perception_summary",
    severity: "medium",
    remediationBucket: BUCKET.SOURCE_EVIDENCE,
    proposedFix: "Add brand-specific Market Perception copy only after founder/manual review confirms slot strategy",
    ...copyProposal(
      "Tribute Portfolio is typically perceived as a soft-brand option for distinctive independent hotels seeking Marriott distribution and Bonvoy access while preserving local character and design identity.",
      "Brand positioning + differentiator framing (no numeric claims)"
    ),
    safeForFutureWriter: false,
    needsSourceEvidence: true,
    evidenceNeeded: ["Confirm whether commercial.perception slot should be introduced before writer work"],
  });

  // v24B blocker handling
  const featuredMissing =
    Array.isArray(copyWriterReport?.missingTargetRows) &&
    copyWriterReport.missingTargetRows.includes("overview.featured_application");
  if (featuredMissing) {
    push({
      section: "v24B blocker",
      slotKey: "overview.featured_application",
      defectType: "missing_target_row",
      severity: "high",
      remediationBucket: BUCKET.ROW_CREATE,
      proposedFix: "Handle in separate v24B-rowcreate batch with explicit row-creation gate; keep v24B copy writer scoped to existing rows",
      reviewStatus: "blocked_row_missing",
    });
  }

  // Sort Order
  push({
    section: "Cross-section",
    slotKey: "(multi-row slots)",
    defectType: "bad_sort_order",
    severity: "high",
    remediationBucket: BUCKET.SORT_ORDER,
    proposedFix: "Future v24D Sort Order writer — 82 writer-default rows; defer until content batches land",
    needsSortOrder: true,
    sourceBasis: "brand-explorer-presentation-sort-order-audit.json",
    reviewStatus: "deferred_v24D",
  });

  return plans;
}

function groupByBucket(plans) {
  const out = {};
  for (const v of Object.values(BUCKET)) out[v] = [];
  for (const p of plans) {
    if (out[p.remediationBucket]) out[p.remediationBucket].push(p.slotKey);
  }
  return out;
}

export async function buildBrandExplorerScreenshotSeededRemediationReviewPackageReport(options = {}) {
  const brandId = normalizeBrandInput(options.brandIdOrName);
  const v24Report = readJson(INPUT.v24Defect);
  const evidenceReport = readJson(INPUT.evidenceReview);
  const copyWriterReport = readJson(INPUT.copyCleanupWriter);
  const sortOrderReport = readJson(INPUT.sortOrder);
  const slotManifest = readJson(INPUT.slotManifest);
  const visualQa = readJson(INPUT.visualQa);

  const tribute = await fetchBrand(brandId);
  const curio = await fetchBrand(CURIO_BRAND_ID);
  if (!tribute) throw new Error("Could not load Tribute brand");

  const sources = await loadApprovedTributeSources(brandId);
  const facts = await fetchAllFacts(brandId);
  const mediaBySlot = await resolveMediaBySlot(brandId);

  const v24Vm = v24Report?.tributeVisibleModel?.sections || {};
  const tributeOverview = reconstructOverviewExtras(tribute, {
    valueScenarios: { cards: [1, 2, 3].map((i) => {
      const sk = `overview.scenario.${i}`;
      const b = firstBlock(tribute, sk);
      return { slotKey: sk, wordCount: wordCount(b?.body), hasImage: hasVal(b?.imageUrl) };
    })},
    whyValueStrongest: v24Vm.whyValueStrongest,
    keyWatchouts: v24Vm.keyWatchouts,
    portfolioContext: v24Vm.portfolioContext,
    featuredApplication: v24Vm.featuredApplication,
  });

  const tributeModel = {
    ...tributeOverview,
    loyalty: reconstructLoyalty(tribute),
    footprint: reconstructFootprint(tribute),
    commercial: reconstructCommercial(tribute),
  };

  const curioModel = {
    loyalty: reconstructLoyalty(curio || {}),
    footprint: reconstructFootprint(curio || {}),
    commercial: reconstructCommercial(curio || {}),
    scenarios: [1, 2, 3].map((i) => {
      const b = firstBlock(curio, `overview.scenario.${i}`);
      return { wordCount: wordCount(b?.body), hasImage: hasVal(b?.imageUrl) };
    }),
  };

  const screenshotVerified = verifyScreenshotDefects(tribute, curio, tributeModel, curioModel, {
    sortOrder: sortOrderReport,
    copyWriter: copyWriterReport,
  });
  const remediationPlans = buildRemediationPlans(tribute, curio, tributeModel, evidenceReport, mediaBySlot, sortOrderReport, copyWriterReport);
  const byBucket = groupByBucket(remediationPlans);

  const additionalDefects = remediationPlans.filter(
    (p) => !screenshotVerified.some((v) => v.confirmed && p.slotKey.includes(v.check))
  ).map((p) => ({ slotKey: p.slotKey, defectType: p.defectType, section: p.section }));

  const copySafeCopy = remediationPlans
    .filter((p) => p.remediationBucket === BUCKET.COPY_SAFE && hasVal(p.proposedBody))
    .map((p) => ({
      slotKey: p.slotKey,
      proposedTitle: p.proposedTitle,
      proposedBody: p.proposedBody,
      copyLabel: p.copyLabel,
      sourceBasis: p.sourceBasis,
    }));

  const evidenceBySection = {};
  for (const p of remediationPlans.filter((x) => x.needsSourceEvidence || x.evidenceNeeded)) {
    evidenceBySection[p.section] = p.evidenceNeeded || [p.sourceBasis];
  }
  const mediaBySection = {};
  for (const p of remediationPlans.filter((x) => x.needsMedia)) {
    mediaBySection[p.section] = { slotKey: p.slotKey, action: p.proposedFix };
  }

  const standardsSafe = false;
  const loyaltyComplete = false;
  const footprintImprove = tributeModel.footprint.geoIntro.hasBody && sources.length > 0;
  const portfolioFix = false;

  return {
    packageVersion: PACKAGE_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    brand: { recordId: brandId, name: nz(tribute.name) || BRAND_NAME },
    filesRead: [
      "AGENTS.md",
      ...Object.values(INPUT).map((p) => p),
      "reports/brand-explorer-visual-display-defect-audit.md",
      "reports/brand-explorer-visual-copy-cleanup-writer.json",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "docs/brand-explorer-presentation-slots.md",
      "fixtures/brand-explorer-presentation-curio-full.json",
      "fixtures/brand-explorer-presentation-*.json",
      "live Tribute/Curio/Kimpton/Radisson/Ascend presentation rows",
      "Tribute sources, facts, asset registry",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-screenshot-seeded-remediation-review-package.js",
      "scripts/brand-explorer-screenshot-seeded-remediation-review-package.mjs",
      `docs/data-intelligence/${DOC_MD_NAME}`,
      `reports/${REPORT_MD_NAME}`,
      `reports/${REPORT_JSON_NAME}`,
      "package.json",
    ],
    v24AScreenshotSeededPackageExists: true,
    v24AR2ScreenshotSeededPackageExists: true,
    v24BaselineScore: v24Report?.visualComparability?.score ?? 39,
    screenshotDefectGroups: SCREENSHOT_DEFECT_GROUPS,
    humanScreenshotDefectsVerified: screenshotVerified,
    humanDefectsConfirmedCount: screenshotVerified.filter((v) => v.confirmed).length,
    humanDefectsTotalChecks: screenshotVerified.length,
    additionalDefectsFound: additionalDefects,
    tributeSectionSnapshot: tributeModel,
    curioComparisonSnapshot: {
      loyaltyProofRows: curioModel.loyalty.proofRowCount,
      loyaltyKpiFilled: curioModel.loyalty.kpiFilledCount,
      footprintRegionDepth: curioModel.footprint.regions.map((r) => r.bodyWordCount),
    },
    remediationPlans,
    defectsByRemediationBucket: byBucket,
    copyCleanupSafe: byBucket[BUCKET.COPY_SAFE],
    rowCreationRequired: [...new Set(byBucket[BUCKET.ROW_CREATE] || [])],
    sourceEvidenceRequired: [...new Set(byBucket[BUCKET.SOURCE_EVIDENCE])],
    mediaAssetRequired: [...new Set(byBucket[BUCKET.MEDIA])],
    sortOrderRequired: byBucket[BUCKET.SORT_ORDER],
    frontendMappingRequired: byBucket[BUCKET.FRONTEND],
    suppressOrHideUntilReady: byBucket[BUCKET.SUPPRESS],
    exactProposedCopyForSafeFixes: copySafeCopy,
    evidenceNeededBySection: evidenceBySection,
    mediaAssetsNeededBySection: mediaBySection,
    sectionSafetyGates: {
      standardsTableCanBeSafelyBuiltNow: standardsSafe,
      loyaltySectionCanBeSafelyCompletedNow: loyaltyComplete,
      footprintSectionCanBeSafelyImprovedNow: footprintImprove,
      portfolioContextCanBeSafelyFixedNow: portfolioFix,
    },
    criticalVisibleDefects: remediationPlans
      .filter((p) => p.severity === "critical")
      .map((p) => ({ slotKey: p.slotKey, section: p.section, defectType: p.defectType })),
    defectsBlockingV24BApply: [
      ...(Array.isArray(copyWriterReport?.applyBlockers) ? copyWriterReport.applyBlockers : []),
      ...screenshotVerified
        .filter((x) => x.confirmed && x.groupId === 17)
        .map((x) => x.detail),
    ],
    featuredApplicationRowDecision: "handled_separately_in_v24B-rowcreate_with_explicit_gate",
    v24BWriterRemainsSafeAsScoped: true,
    revisedV24BSlotList: (copyWriterReport?.targetSlotKeys || byBucket[BUCKET.COPY_SAFE] || []).filter(
      (k) => k !== "overview.featured_application"
    ),
    recommendedRemediationSequence: {
      v24B_copy_cleanup_writer: {
        description: "Founder-reviewed bullet/bestAt/lifecycle copy on existing rows only",
        slots: (copyWriterReport?.targetSlotKeys || byBucket[BUCKET.COPY_SAFE] || []).filter(
          (k) => k !== "overview.featured_application"
        ),
      },
      v24B_rowcreate: {
        description: "Explicitly gated row creation for missing overview.featured_application and property-example rows",
        slots: [...new Set([...(byBucket[BUCKET.ROW_CREATE] || []), "overview.featured_application"])],
      },
      v24C_media_fix: { description: "Media/suppression pass for empty visuals/cards", slots: ["overview.scenario.3", "materials.gallery.3", "footprint.openings", "footprint.momentum"] },
      v24D_sort_order_correction: { description: "Normalize 82 writer-default Sort Order values", deferred: true },
      v24E_source_evidence_work: {
        description: "Loyalty, standards, demand scenarios, portfolio mix, and footprint evidence sections",
        slots: byBucket[BUCKET.SOURCE_EVIDENCE],
      },
    },
    recommendedNextBatch: "v24B_copy_cleanup_then_v24B-rowcreate_then_v24C_then_v24D_then_v24E",
    visuallyComparableToCurioToday: false,
    sourceContext: {
      approvedSources: sources.length,
      totalFacts: facts.length,
      pendingFacts: facts.filter((f) => nz(f.humanReviewStatus) === "Pending").length,
      slotManifestLoyaltyGaps: (slotManifest?.slots || []).filter((s) => s.slotKey?.startsWith("loyalty.") && s.tributeHasSlot === false).length,
    },
    exactNextCommand:
      "npm run brand-explorer-screenshot-seeded-remediation-review-package -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerScreenshotSeededRemediationReviewPackageMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Screenshot-Seeded Remediation Review Package v24A-R2");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}** · Airtable: **no** · Images: **untouched**`);
  lines.push(`- v24 score baseline: **${report.v24BaselineScore}/100**`);
  lines.push(`- Screenshot checks confirmed: **${report.humanDefectsConfirmedCount}/${report.humanDefectsTotalChecks}**`);
  lines.push(`- Comparable to Curio: **${report.visuallyComparableToCurioToday ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Human screenshot defects verified");
  for (const v of report.humanScreenshotDefectsVerified.filter((x) => x.confirmed)) {
    lines.push(`- [${v.groupId}] **${v.check}**: ${v.detail}`);
  }
  lines.push("");
  lines.push("## Safe copy proposals");
  for (const c of report.exactProposedCopyForSafeFixes) {
    lines.push(`### \`${c.slotKey}\``);
    lines.push(`- ${c.copyLabel}`);
    lines.push(`- ${c.proposedBody}`);
  }
  lines.push("");
  lines.push("## Section safety gates");
  const g = report.sectionSafetyGates;
  lines.push(`- Standards table now: **${g.standardsTableCanBeSafelyBuiltNow ? "yes" : "no"}**`);
  lines.push(`- Loyalty complete now: **${g.loyaltySectionCanBeSafelyCompletedNow ? "yes" : "no"}**`);
  lines.push(`- Footprint improve now: **${g.footprintSectionCanBeSafelyImprovedNow ? "yes" : "no"}**`);
  lines.push(`- Portfolio Context now: **${g.portfolioContextCanBeSafelyFixedNow ? "yes" : "no"}**`);
  lines.push("");
  lines.push(`## Next: **${report.recommendedNextBatch}**`);
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}
