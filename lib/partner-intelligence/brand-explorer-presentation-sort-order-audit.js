/**
 * Brand Explorer Presentation Sort Order Audit (read-only).
 *
 * Compares Tribute Portfolio presentation row Sort Order values against
 * completed active reference brands. Produces a correction plan only — no writes.
 */
import { fileURLToPath } from "url";
import path from "path";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { TRIBUTE_RECORD_ID, BRAND_NAME } from "./tribute-portfolio-brand-package.js";

export const AUDIT_VERSION = "1";
export const REPORT_JSON_NAME = "brand-explorer-presentation-sort-order-audit.json";
export const REPORT_MD_NAME = "brand-explorer-presentation-sort-order-audit.md";
export const DOC_MD_NAME = "brand-explorer-presentation-sort-order-audit-v1.md";

const DEFAULT_BRAND_ID = TRIBUTE_RECORD_ID;

function normalizeBrandInput(raw) {
  const normalized = nz(raw).toLowerCase();
  if (!normalized || normalized === "tribute-portfolio" || normalized === "tribute portfolio") {
    return DEFAULT_BRAND_ID;
  }
  return nz(raw);
}

const REFERENCE_BRANDS = [
  "Radisson Blu by Choice",
  "Radisson by Choice",
  "Kimpton Hotels",
  "Curio Collection by Hilton",
  "Ascend Hotel Collection",
  "Everhome Suites",
  "Radisson RED by Choice",
  "Radisson Individuals by Choice",
  "Comfort by Choice",
  "Quality by Choice",
  "Country Inn by Choice",
];

const FILES_READ = [
  "AGENTS.md",
  "api/brand-library.js",
  "docs/brand-explorer-presentation-slots.md",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "reports/brand-explorer-slot-completion-writer.json",
  "reports/brand-explorer-remaining-editorial-slot-completion-writer.json",
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function normalizeSortOrder(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number" && !Number.isNaN(v)) return v;
  const n = parseFloat(String(v).replace(/,/g, ""));
  return Number.isNaN(n) ? null : n;
}

function tabFromSlot(slotKey) {
  const key = nz(slotKey);
  if (/^hero\.|^overview\./i.test(key)) return "Overview";
  if (/^commercial\./i.test(key)) return "Commercial";
  if (/^economics\./i.test(key)) return "Economics";
  if (/^loyalty\./i.test(key)) return "Loyalty Program";
  if (/^operations\./i.test(key)) return "Operations & Standards";
  if (/^footprint\./i.test(key)) return "Footprint & Growth";
  if (/^materials\./i.test(key)) return "Brand Materials";
  if (/^standards\.|^valueOwners\./i.test(key)) return "Owner Considerations";
  if (/^insight\./i.test(key)) return "Dealality Insight";
  return "Other";
}

function slotFamily(slotKey) {
  const key = nz(slotKey);
  const parts = key.split(".");
  if (parts.length <= 2) return key;
  return `${parts[0]}.${parts[1]}`;
}

async function fetchBrand(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
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
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function presentationRows(brand) {
  const blocks = brand?.brandExplorer?.blocks || [];
  return (Array.isArray(blocks) ? blocks : []).map((b) => ({
    recordId: nz(b.recordId || b.id),
    slotKey: nz(b.slotKey || b.slot_key),
    title: nz(b.title),
    body: nz(b.body),
    sortOrder: normalizeSortOrder(b.sort ?? b.sortOrder),
    active: true,
    tab: tabFromSlot(b.slotKey || b.slot_key),
    slotFamily: slotFamily(b.slotKey || b.slot_key),
  }));
}

function groupRowsBySlot(rows) {
  const map = new Map();
  for (const row of rows) {
    if (!row.slotKey) continue;
    if (!map.has(row.slotKey)) map.set(row.slotKey, []);
    map.get(row.slotKey).push(row);
  }
  for (const [, list] of map) {
    list.sort((a, b) => (a.sortOrder ?? 9999) - (b.sortOrder ?? 9999));
  }
  return map;
}

function mode(values) {
  const counts = new Map();
  for (const v of values) {
    counts.set(v, (counts.get(v) || 0) + 1);
  }
  let best = null;
  let bestCount = 0;
  for (const [v, c] of counts) {
    if (c > bestCount) {
      best = v;
      bestCount = c;
    }
  }
  return { value: best, count: bestCount, total: values.length };
}

function buildReferenceSortPattern(referenceProfiles) {
  const bySlot = new Map();

  for (const profile of referenceProfiles) {
    if (!profile.readable) continue;
    const grouped = groupRowsBySlot(profile.rows);
    for (const [slotKey, rows] of grouped) {
      if (!bySlot.has(slotKey)) bySlot.set(slotKey, []);
      const entry = bySlot.get(slotKey);
      rows.forEach((row, rowIndex) => {
        if (!entry[rowIndex]) entry[rowIndex] = [];
        if (row.sortOrder != null) entry[rowIndex].push(row.sortOrder);
      });
    }
  }

  const pattern = [];
  for (const [slotKey, rowIndexes] of [...bySlot.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
    const rowPatterns = rowIndexes.map((sorts, rowIndex) => {
      const m = mode(sorts);
      const unique = [...new Set(sorts)].sort((a, b) => a - b);
      return {
        rowIndex,
        observedSortOrders: sorts,
        uniqueSortOrders: unique,
        modeSortOrder: m.value,
        modeAgreement: m.total ? Math.round((m.count / m.total) * 100) : 0,
        brandsReporting: m.total,
        ambiguous: unique.length > 1 && m.count / m.total < 0.6,
      };
    });
    pattern.push({
      slotKey,
      tab: tabFromSlot(slotKey),
      slotFamily: slotFamily(slotKey),
      rowCount: rowPatterns.length,
      rowPatterns,
    });
  }
  return pattern;
}

function isLikelyWriterBatchDefault(sortOrder, referenceMode) {
  if (sortOrder == null) return false;
  if (sortOrder >= 10 && sortOrder % 10 === 0 && referenceMode === 0) return true;
  if (sortOrder >= 10 && referenceMode != null && sortOrder !== referenceMode && sortOrder % 10 === 0) return true;
  return false;
}

function auditTributeSortOrders(tributeRows, referencePattern) {
  const refBySlot = new Map(referencePattern.map((p) => [p.slotKey, p]));
  const grouped = groupRowsBySlot(tributeRows);

  const missingSortOrder = [];
  const duplicateSortOrder = [];
  const outOfSequence = [];
  const inconsistentWithReference = [];
  const likelyWriterDefault = [];
  const manualReviewRequired = [];
  const proposedCorrections = [];

  for (const [slotKey, rows] of grouped) {
    const ref = refBySlot.get(slotKey);
    const refRowPatterns = ref?.rowPatterns || [];

    const sorts = rows.map((r) => r.sortOrder);
    const sortCounts = new Map();
    for (const s of sorts) {
      if (s == null) continue;
      sortCounts.set(s, (sortCounts.get(s) || 0) + 1);
    }

    rows.forEach((row, rowIndex) => {
      if (row.sortOrder == null) {
        missingSortOrder.push({
          recordId: row.recordId,
          slotKey,
          tab: row.tab,
          title: row.title,
          currentSortOrder: null,
        });
      }

      const refForRow = refRowPatterns[rowIndex] || refRowPatterns[0];
      const refMode = refForRow?.modeSortOrder;

      if (refForRow?.ambiguous) {
        manualReviewRequired.push({
          recordId: row.recordId,
          slotKey,
          rowIndex,
          reason: "reference_pattern_ambiguous",
          referencePattern: refForRow,
        });
      }

      if (row.sortOrder != null && sortCounts.get(row.sortOrder) > 1 && rows.length > 1) {
        duplicateSortOrder.push({
          recordId: row.recordId,
          slotKey,
          tab: row.tab,
          currentSortOrder: row.sortOrder,
          duplicateWith: rows.filter((r) => r.recordId !== row.recordId && r.sortOrder === row.sortOrder).map((r) => r.recordId),
        });
      }

      if (refMode != null && row.sortOrder != null && row.sortOrder !== refMode) {
        inconsistentWithReference.push({
          recordId: row.recordId,
          slotKey,
          tab: row.tab,
          currentSortOrder: row.sortOrder,
          referenceModeSortOrder: refMode,
          referenceAgreementPct: refForRow.modeAgreement,
          brandsReporting: refForRow.brandsReporting,
        });
      }

      if (isLikelyWriterBatchDefault(row.sortOrder, refMode)) {
        likelyWriterDefault.push({
          recordId: row.recordId,
          slotKey,
          tab: row.tab,
          currentSortOrder: row.sortOrder,
          referenceModeSortOrder: refMode,
          note: "Sort Order matches v20B/v21B writer index*10 batch default; completed brands typically use per-slot 0..n sequencing.",
        });
      }

      if (refMode != null && row.sortOrder !== refMode && !refForRow?.ambiguous) {
        const confidence =
          refForRow.modeAgreement >= 75 ? "high" : refForRow.modeAgreement >= 50 ? "medium" : "manual_review_required";
        proposedCorrections.push({
          recordId: row.recordId,
          slotKey,
          tab: row.tab,
          title: row.title,
          currentSortOrder: row.sortOrder,
          proposedSortOrder: refMode,
          rowIndexWithinSlot: rowIndex,
          referencePatternUsed: {
            modeSortOrder: refMode,
            modeAgreementPct: refForRow.modeAgreement,
            brandsReporting: refForRow.brandsReporting,
            uniqueSortOrders: refForRow.uniqueSortOrders,
          },
          confidence,
          safeForFutureWriter: confidence === "high",
          correctionReason:
            row.sortOrder == null
              ? "missing_sort_order"
              : isLikelyWriterBatchDefault(row.sortOrder, refMode)
                ? "writer_batch_default"
                : "reference_mode_mismatch",
        });
      } else if (row.sortOrder == null && refMode != null && !refForRow?.ambiguous) {
        proposedCorrections.push({
          recordId: row.recordId,
          slotKey,
          tab: row.tab,
          title: row.title,
          currentSortOrder: null,
          proposedSortOrder: refMode,
          rowIndexWithinSlot: rowIndex,
          referencePatternUsed: {
            modeSortOrder: refMode,
            modeAgreementPct: refForRow.modeAgreement,
            brandsReporting: refForRow.brandsReporting,
            uniqueSortOrders: refForRow.uniqueSortOrders,
          },
          confidence: refForRow.modeAgreement >= 75 ? "high" : "medium",
          safeForFutureWriter: refForRow.modeAgreement >= 75,
          correctionReason: "missing_sort_order",
        });
      }
    });

    if (rows.length > 1) {
      const ordered = rows.map((r) => r.sortOrder);
      const numeric = ordered.filter((s) => s != null);
      for (let i = 1; i < numeric.length; i++) {
        if (numeric[i] < numeric[i - 1]) {
          outOfSequence.push({
            slotKey,
            tab: rows[0].tab,
            sortOrders: ordered,
            recordIds: rows.map((r) => r.recordId),
          });
          break;
        }
      }
    }
  }

  return {
    missingSortOrder,
    duplicateSortOrder,
    outOfSequence,
    inconsistentWithReference,
    likelyWriterDefault,
    manualReviewRequired,
    proposedCorrections,
  };
}

export async function buildBrandExplorerPresentationSortOrderAuditReport(options = {}) {
  const brandId = normalizeBrandInput(options.brandIdOrName);
  const tribute = await fetchBrand(brandId);
  if (!tribute) throw new Error(`Unable to read brand: ${brandId}`);

  const referenceProfiles = [];
  for (const name of REFERENCE_BRANDS) {
    const brand = await fetchBrand(name);
    referenceProfiles.push({
      name,
      recordId: nz(brand?.id),
      readable: Boolean(brand),
      rows: brand ? presentationRows(brand) : [],
    });
  }

  const readableRefs = referenceProfiles.filter((p) => p.readable);
  const tributeRows = presentationRows(tribute);
  const referencePattern = buildReferenceSortPattern(readableRefs);
  const audit = auditTributeSortOrders(tributeRows, referencePattern);

  const futureWriterNeeded =
    audit.proposedCorrections.some((c) => c.safeForFutureWriter) ||
    audit.likelyWriterDefault.length > 0 ||
    audit.duplicateSortOrder.length > 0;

  return {
    auditVersion: AUDIT_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    sortOrderValuesChanged: false,
    companyValidatedUntouched: true,
    brand: {
      recordId: nz(tribute.id) || brandId,
      name: nz(tribute.name) || BRAND_NAME,
    },
    filesRead: FILES_READ,
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-presentation-sort-order-audit.js",
      "scripts/brand-explorer-presentation-sort-order-audit.mjs",
      "docs/data-intelligence/brand-explorer-presentation-sort-order-audit-v1.md",
      "reports/brand-explorer-presentation-sort-order-audit.md",
      "reports/brand-explorer-presentation-sort-order-audit.json",
      "package.json",
    ],
    sortOrderAuditAdded: true,
    tributeRowCount: tributeRows.length,
    referenceBrandsInspected: referenceProfiles.map((p) => ({
      name: p.name,
      readable: p.readable,
      rowCount: p.rows.length,
    })),
    referenceBrandsReadableCount: readableRefs.length,
    referenceSortOrderPattern: referencePattern,
    tributePresentationRows: tributeRows,
    sortOrderAuditSummary: {
      missingSortOrderCount: audit.missingSortOrder.length,
      duplicateSortOrderCount: audit.duplicateSortOrder.length,
      outOfSequenceCount: audit.outOfSequence.length,
      inconsistentWithReferenceCount: audit.inconsistentWithReference.length,
      likelyWriterDefaultCount: audit.likelyWriterDefault.length,
      manualReviewRequiredCount: audit.manualReviewRequired.length,
      proposedCorrectionCount: audit.proposedCorrections.length,
      highConfidenceCorrectionCount: audit.proposedCorrections.filter((c) => c.confidence === "high").length,
    },
    tributeRowsMissingSortOrder: audit.missingSortOrder,
    tributeRowsDuplicateSortOrder: audit.duplicateSortOrder,
    tributeRowsOutOfSequence: audit.outOfSequence,
    tributeRowsInconsistentWithReference: audit.inconsistentWithReference,
    tributeRowsLikelyWriterDefault: audit.likelyWriterDefault,
    manualReviewRequired: audit.manualReviewRequired,
    proposedSortOrderCorrectionPlan: audit.proposedCorrections,
    futureSortOrderCorrectionWriterNeeded: futureWriterNeeded,
    futureSortOrderCorrectionWriterNote: futureWriterNeeded
      ? "Completed-brand pattern differs from Tribute writer batch defaults (index*10). Build a separate gated Sort Order correction writer after founder review of this audit."
      : "No high-confidence Sort Order corrections identified.",
    exactNextCommand:
      "npm run brand-explorer-presentation-sort-order-audit -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerPresentationSortOrderAuditMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Presentation Sort Order Audit");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}** · Airtable modified: **no** · Sort Order changed: **no**`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- Tribute rows: **${report.tributeRowCount}** · Reference brands readable: **${report.referenceBrandsReadableCount}**`);
  lines.push("");
  lines.push("## Summary");
  const s = report.sortOrderAuditSummary;
  lines.push(`- Missing Sort Order: **${s.missingSortOrderCount}**`);
  lines.push(`- Duplicate Sort Order: **${s.duplicateSortOrderCount}**`);
  lines.push(`- Out of sequence: **${s.outOfSequenceCount}**`);
  lines.push(`- Inconsistent with reference: **${s.inconsistentWithReferenceCount}**`);
  lines.push(`- Likely writer batch default (index×10): **${s.likelyWriterDefaultCount}**`);
  lines.push(`- Proposed corrections: **${s.proposedCorrectionCount}** (${s.highConfidenceCorrectionCount} high confidence)`);
  lines.push(`- Future Sort Order writer needed: **${report.futureSortOrderCorrectionWriterNeeded ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Proposed correction plan (audit only)");
  if (report.proposedSortOrderCorrectionPlan.length) {
    lines.push("| Record | Slot | Current | Proposed | Confidence | Reason |");
    lines.push("|--------|------|---------|----------|------------|--------|");
    for (const row of report.proposedSortOrderCorrectionPlan.slice(0, 80)) {
      lines.push(
        `| \`${row.recordId}\` | ${row.slotKey} | ${row.currentSortOrder ?? "—"} | ${row.proposedSortOrder} | ${row.confidence} | ${row.correctionReason} |`
      );
    }
    if (report.proposedSortOrderCorrectionPlan.length > 80) {
      lines.push(`- …and ${report.proposedSortOrderCorrectionPlan.length - 80} more (see JSON).`);
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
