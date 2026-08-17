import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME } from "./tribute-portfolio-brand-package.js";

export const AUDIT_VERSION = "24D";
export const REPORT_JSON_NAME = "brand-explorer-sort-order-render-impact-audit.json";
export const REPORT_MD_NAME = "brand-explorer-sort-order-render-impact-audit.md";
export const DOC_MD_NAME = "brand-explorer-sort-order-render-impact-audit-v24D.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const DEFAULT_BRAND_ID = TRIBUTE_RECORD_ID;

const REFERENCE_BRANDS = [
  "Curio Collection by Hilton",
  "Kimpton Hotels",
  "Radisson Blu by Choice",
  "Radisson by Choice",
  "Ascend Hotel Collection",
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-presentation-sort-order-audit.md",
  "reports/brand-explorer-presentation-sort-order-audit.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "reports/brand-explorer-screenshot-seeded-remediation-review-package.md",
  "reports/brand-explorer-screenshot-seeded-remediation-review-package.json",
  "reports/brand-explorer-visual-minimums-backfill-writer.md",
  "reports/brand-explorer-visual-minimums-backfill-writer.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Tribute Brand Explorer Presentation rows",
  "live Curio/Kimpton/Radisson/Ascend presentation rows",
];

const SECTION_DEFS = [
  { section: "Geographic Footprint", slotKeys: ["footprint.geo_intro", "footprint.region.am", "footprint.region.cala", "footprint.region.eu", "footprint.region.mea", "footprint.region.apac"], frontendUsesSort: true, multiRowGrouping: true },
  { section: "Openings / Examples / Properties", slotKeys: ["footprint.openings"], frontendUsesSort: true, takeFirstN: true, multiRowGrouping: true },
  { section: "Recent Momentum", slotKeys: ["footprint.momentum"], frontendUsesSort: true, takeFirstN: true },
  { section: "Portfolio Mix", slotKeys: ["footprint.portfolio_mix"], frontendUsesSort: true, multiRowGrouping: true },
  { section: "Portfolio Context", slotKeys: ["overview.portfolio_context"], frontendUsesSort: false },
  { section: "Standard Detail / Where Available", slotKeys: ["standards.requirement"], frontendUsesSort: true, multiRowGrouping: true },
  { section: "Demand Scenario View", slotKeys: ["commercial.demand"], frontendUsesSort: true, multiRowGrouping: true },
  { section: "Loyalty Program", slotKeys: ["loyalty.proof", "loyalty.elite", "loyalty.earn", "loyalty.redeem", "loyalty.kpi.members", "loyalty.kpi.hotels", "loyalty.kpi.markets", "loyalty.kpi.mix"], frontendUsesSort: true, multiRowGrouping: true },
  { section: "Where This Brand Creates the Most Value", slotKeys: ["overview.scenario.1", "overview.scenario.2", "overview.scenario.3"], frontendUsesSort: true, firstSecondThirdMapping: true },
  { section: "Image Gallery", slotKeys: ["materials.gallery.1", "materials.gallery.2", "materials.gallery.3", "materials.gallery.4", "materials.gallery.5", "materials.gallery.6"], frontendUsesSort: false, firstSecondThirdMapping: true },
  { section: "Value Creation Scenarios", slotKeys: ["valueOwners.scenario.1", "valueOwners.scenario.2", "valueOwners.scenario.3", "valueOwners.scenario.4"], frontendUsesSort: true, firstSecondThirdMapping: true },
  { section: "Key Watchouts", slotKeys: ["valueOwners.watchouts"], frontendUsesSort: false },
  { section: "Why Value Is Strongest", slotKeys: ["overview.why_value"], frontendUsesSort: false },
  { section: "Differentiators", slotKeys: ["overview.differentiators.identity", "overview.differentiators.commercial"], frontendUsesSort: false },
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") return DEFAULT_BRAND_ID;
  return nz(raw);
}

function normalizeSort(v) {
  if (v == null || v === "") return 0;
  if (typeof v === "number" && !Number.isNaN(v)) return v;
  const n = parseFloat(String(v).replace(/,/g, ""));
  return Number.isNaN(n) ? 0 : n;
}

async function fetchBrand(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = { statusCode: 200, payload: null, setHeader() {}, status(code) { this.statusCode = code; return this; }, json(payload) { this.payload = payload; return this; } };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function presentationRows(brand) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.map((b) => ({
    recordId: nz(b.recordId || b.id),
    slotKey: nz(b.slotKey || b.slot_key),
    title: nz(b.title),
    body: nz(b.body),
    imageUrl: nz(b.imageUrl),
    sortOrder: normalizeSort(b.sort ?? b.sortOrder),
  }));
}

function rowsForSlots(rows, slotKeys) {
  const set = new Set(slotKeys);
  return rows.filter((r) => set.has(r.slotKey));
}

function mode(values) {
  if (!values.length) return null;
  const counts = new Map();
  values.forEach((v) => counts.set(v, (counts.get(v) || 0) + 1));
  let best = null;
  let bestCount = 0;
  for (const [v, c] of counts) {
    if (c > bestCount) {
      best = v;
      bestCount = c;
    }
  }
  return best;
}

function bySlotReferencePattern(referenceRows) {
  const bySlot = new Map();
  for (const row of referenceRows) {
    if (!bySlot.has(row.slotKey)) bySlot.set(row.slotKey, []);
    bySlot.get(row.slotKey).push(row.sortOrder);
  }
  const out = {};
  for (const [slotKey, sorts] of bySlot) out[slotKey] = { modeSort: mode(sorts), uniqueSorts: [...new Set(sorts)].sort((a, b) => a - b) };
  return out;
}

function isWriterDefault(sortOrder) {
  return sortOrder >= 10 && sortOrder % 10 === 0;
}

function readJson(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

export async function buildBrandExplorerSortOrderRenderImpactAuditReport(options = {}) {
  const brandId = normalizeBrandInput(options.brandIdOrName);
  const tribute = await fetchBrand(brandId);
  if (!tribute) throw new Error(`Unable to read brand: ${brandId}`);

  const refs = [];
  for (const refName of REFERENCE_BRANDS) {
    const b = await fetchBrand(refName);
    refs.push({ name: refName, recordId: nz(b?.id), readable: Boolean(b), rows: b ? presentationRows(b) : [] });
  }

  const tributeRows = presentationRows(tribute);
  const refRowsAll = refs.filter((r) => r.readable).flatMap((r) => r.rows);
  const refPattern = bySlotReferencePattern(refRowsAll);

  const displayDefect = readJson("reports/brand-explorer-visual-display-defect-audit.json") || {};
  const defectBySection = new Map();
  for (const d of displayDefect.defects || []) {
    const key = nz(d.section);
    if (!key) continue;
    if (!defectBySection.has(key)) defectBySection.set(key, []);
    defectBySection.get(key).push(d);
  }

  const sectionAssessments = SECTION_DEFS.map((def) => {
    const rows = rowsForSlots(tributeRows, def.slotKeys);
    const existingRows = rows.length;
    const missingRows = existingRows === 0;
    const imageMissingCount = rows.filter((r) => def.section.match(/Image|Scenario|Openings|Gallery|Value Creation/i) && !r.imageUrl).length;
    const missingContentCount = rows.filter((r) => !r.title && !r.body).length;
    const writerDefaultCount = rows.filter((r) => isWriterDefault(r.sortOrder)).length;
    const tributeSortOrders = rows.map((r) => ({ recordId: r.recordId, slotKey: r.slotKey, sortOrder: r.sortOrder }));

    const refSlots = def.slotKeys.map((slotKey) => ({ slotKey, modeSortOrder: refPattern[slotKey]?.modeSort ?? null, uniqueSortOrders: refPattern[slotKey]?.uniqueSorts || [] }));
    const hasSortMismatch = rows.some((r) => refPattern[r.slotKey] && refPattern[r.slotKey].modeSort !== r.sortOrder);
    const frontendUsesSort = Boolean(def.frontendUsesSort);
    const defects = defectBySection.get(def.section) || [];
    const visibleIssue = defects.length ? defects.map((d) => `${d.defectType}`).join(", ") : "no explicit defect in v24 audit";

    let classification = "sort_order_unlikely_related";
    if (missingRows) classification = "not_sort_order_related_missing_row";
    else if (imageMissingCount > 0) classification = "not_sort_order_related_missing_image";
    else if (["Standard Detail / Where Available", "Loyalty Program", "Demand Scenario View", "Portfolio Mix"].includes(def.section)) {
      classification = "not_sort_order_related_source_evidence";
    } else if (["Portfolio Context", "Openings / Examples / Properties", "Recent Momentum"].includes(def.section) && defects.some((d) => d.fixRequiresDisplayMapping)) {
      classification = "not_sort_order_related_frontend_mapping";
    } else if (frontendUsesSort && (writerDefaultCount > 0 || hasSortMismatch) && defects.length) {
      classification = "sort_order_likely_affects_render";
    } else if (frontendUsesSort && (writerDefaultCount > 0 || hasSortMismatch)) {
      classification = "sort_order_only_affects_ordering";
    }

    let recommendedAction = "No sort-order action";
    if (classification === "sort_order_likely_affects_render") recommendedAction = "Targeted sort-order correction candidate after founder review";
    if (classification === "sort_order_only_affects_ordering") recommendedAction = "Optional ordering normalization only";
    if (classification.startsWith("not_sort_order_related")) recommendedAction = "Address row/content/image/mapping first; do not apply global sort changes";

    return {
      section: def.section,
      visibleIssue,
      relevantSlotKeys: def.slotKeys,
      rowsExist: !missingRows,
      rowCount: existingRows,
      rowsHaveImageOrContent: rows.filter((r) => r.imageUrl || r.title || r.body).length,
      tributeSortOrderValues: tributeSortOrders,
      referenceSortOrderPattern: refSlots,
      frontendUsesSortOrder: frontendUsesSort,
      frontendSortBehavior: {
        rowSelection: frontendUsesSort,
        cardOrdering: frontendUsesSort,
        firstSecondThirdCardMapping: Boolean(def.firstSecondThirdMapping),
        multiRowGrouping: Boolean(def.multiRowGrouping),
        takeFirstN: Boolean(def.takeFirstN),
      },
      sortOrderCouldCauseIssue: classification === "sort_order_likely_affects_render" || classification === "sort_order_only_affects_ordering",
      classification,
      recommendedAction,
      writerDefaultSortRowCount: writerDefaultCount,
      sortOrderMismatchCount: hasSortMismatch ? 1 : 0,
      missingImageCount: imageMissingCount,
      missingContentCount,
    };
  });

  const highConfidenceCorrections = [];
  const mediumConfidenceCorrections = [];
  const doNotChangeRows = [];
  const notSortOrderRows = [];

  for (const s of sectionAssessments) {
    if (s.classification === "sort_order_likely_affects_render" || s.classification === "sort_order_only_affects_ordering") {
      for (const row of s.tributeSortOrderValues) {
        const ref = (s.referenceSortOrderPattern.find((r) => r.slotKey === row.slotKey) || {}).modeSortOrder;
        if (ref == null || ref === row.sortOrder) continue;
        const item = {
          recordId: row.recordId,
          slotKey: row.slotKey,
          currentSortOrder: row.sortOrder,
          proposedSortOrder: ref,
          section: s.section,
        };
        if (isWriterDefault(row.sortOrder)) highConfidenceCorrections.push(item);
        else mediumConfidenceCorrections.push(item);
      }
    } else {
      s.tributeSortOrderValues.forEach((r) => notSortOrderRows.push({ ...r, section: s.section, reason: s.classification }));
      s.tributeSortOrderValues.forEach((r) => doNotChangeRows.push({ ...r, section: s.section }));
    }
  }

  const sortLikely = sectionAssessments.filter((s) => s.classification === "sort_order_likely_affects_render").map((s) => s.section);
  const sortNot = sectionAssessments.filter((s) => s.classification !== "sort_order_likely_affects_render" && s.classification !== "sort_order_only_affects_ordering").map((s) => s.section);
  const missingRowsIssues = sectionAssessments.filter((s) => s.classification === "not_sort_order_related_missing_row").map((s) => s.section);
  const missingImageIssues = sectionAssessments.filter((s) => s.classification === "not_sort_order_related_missing_image").map((s) => s.section);
  const sourceEvidenceIssues = sectionAssessments.filter((s) => s.classification === "not_sort_order_related_source_evidence").map((s) => s.section);
  const mappingIssues = sectionAssessments.filter((s) => s.classification === "not_sort_order_related_frontend_mapping").map((s) => s.section);

  return {
    auditVersion: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    brand: { recordId: nz(tribute.id) || brandId, name: nz(tribute.name) || BRAND_NAME },
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-sort-order-render-impact-audit.js",
      "scripts/brand-explorer-sort-order-render-impact-audit.mjs",
      "docs/data-intelligence/brand-explorer-sort-order-render-impact-audit-v24D.md",
      "reports/brand-explorer-sort-order-render-impact-audit.md",
      "reports/brand-explorer-sort-order-render-impact-audit.json",
      "package.json",
    ],
    v24DSortOrderRenderImpactAuditExists: true,
    totalTributeRowsReviewed: tributeRows.length,
    rowsWithWriterDefaultSortOrder: tributeRows.filter((r) => isWriterDefault(r.sortOrder)).length,
    referenceBrandsInspected: refs.map((r) => ({ name: r.name, recordId: r.recordId, readable: r.readable, rowCount: r.rows.length })),
    sectionAssessments,
    sectionsLikelyAffectedBySortOrder: sortLikely,
    sectionsNotAffectedBySortOrder: sortNot,
    issuesCausedByMissingRows: missingRowsIssues,
    issuesCausedByMissingImages: missingImageIssues,
    issuesCausedBySourceEvidenceGaps: sourceEvidenceIssues,
    issuesCausedByFrontendMappingSuppressionGaps: mappingIssues,
    highConfidenceSortOrderCorrections: highConfidenceCorrections,
    mediumConfidenceSortOrderCorrections: mediumConfidenceCorrections,
    rowsShouldNotBeChanged: doNotChangeRows,
    rowsNotSortOrderRelated: notSortOrderRows,
    v24DSortOrderCorrectionWriterShouldBeBuilt: highConfidenceCorrections.length > 0,
    exactRecommendedCorrectionListIfSafe: highConfidenceCorrections,
    exactNextCommand: "npm run brand-explorer-sort-order-render-impact-audit -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerSortOrderRenderImpactAuditMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Sort Order Render Impact Audit v24D");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- Tribute rows reviewed: **${report.totalTributeRowsReviewed}**`);
  lines.push(`- Writer-default Sort Order rows: **${report.rowsWithWriterDefaultSortOrder}**`);
  lines.push("");
  lines.push("## Section Impact");
  for (const s of report.sectionAssessments) {
    lines.push(`### ${s.section}`);
    lines.push(`- Classification: **${s.classification}**`);
    lines.push(`- Visible issue: ${s.visibleIssue}`);
    lines.push(`- Rows exist: ${s.rowsExist ? "yes" : "no"} · count: ${s.rowCount}`);
    lines.push(`- Frontend uses Sort Order: ${s.frontendUsesSortOrder ? "yes" : "no"}`);
    lines.push(`- Sort likely causes issue: ${s.sortOrderCouldCauseIssue ? "yes" : "no"}`);
    lines.push(`- Recommended action: ${s.recommendedAction}`);
  }
  lines.push("");
  lines.push("## High-confidence Sort Order corrections");
  if (report.highConfidenceSortOrderCorrections.length) {
    lines.push("| Record | Slot | Current | Proposed | Section |");
    lines.push("|--------|------|---------|----------|---------|");
    for (const r of report.highConfidenceSortOrderCorrections) {
      lines.push(`| \`${r.recordId}\` | ${r.slotKey} | ${r.currentSortOrder} | ${r.proposedSortOrder} | ${r.section} |`);
    }
  } else {
    lines.push("- None");
  }
  lines.push("");
  lines.push("## Exact next command");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}
