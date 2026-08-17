import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const WRITER_VERSION = "17";
export const REPORT_JSON_NAME = "brand-explorer-presentation-slot-coverage-audit.json";
export const REPORT_MD_NAME = "brand-explorer-presentation-slot-coverage-audit.md";
export const DOC_MD_NAME = "brand-explorer-presentation-slot-coverage-audit-v17.md";

const DEFAULT_TRIBUTE_BRAND_ID = "recCvV0PuZOi8c3hC";
const DEFAULT_TRIBUTE_BRAND_NAME = "Tribute Portfolio";

const REFERENCE_BRANDS = [
  "Radisson Blu by Choice",
  "Radisson by Choice",
  "Kimpton Hotels",
  "Curio Collection by Hilton",
  "Ascend Hotel Collection",
  "Comfort by Choice",
  "Quality by Choice",
  "Country Inn by Choice",
  "Everhome Suites",
  "Radisson RED by Choice",
  "Radisson Individuals by Choice",
];

const CORE_REQUIRED_SLOT_KEYS = new Set([
  "overview.typical_use_case",
  "overview.development_model",
  "overview.scenario.1",
  "overview.scenario.2",
  "valueOwners.overview",
  "standards.questions",
  "footprint.geo_intro",
  "insight.summary",
]);

const SHOULD_REMAIN_BLANK_FOR_TRIBUTE = new Set(["footprint.openings"]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return String(v).trim() !== "";
}

function toText(v) {
  if (Array.isArray(v)) return v.filter(hasVal).map(String).join(", ");
  return hasVal(v) ? String(v).trim() : "";
}

function firstUrl(text) {
  const match = toText(text).match(/https?:\/\/\S+/i);
  return match ? match[0] : "";
}

function normalizeBrandName(raw) {
  return toText(raw).trim().toLowerCase();
}

function normalizeBrandInput(raw) {
  const normalized = normalizeBrandName(raw);
  if (!normalized) return DEFAULT_TRIBUTE_BRAND_ID;
  if (normalized === "tribute-portfolio" || normalized === "tribute portfolio") return DEFAULT_TRIBUTE_BRAND_ID;
  return toText(raw).trim();
}

function tabSectionFromSlotKey(slotKey) {
  const key = toText(slotKey);
  if (!key) return { tab: "Unknown", section: "Unknown" };
  if (key.startsWith("overview.")) return { tab: "Overview", section: "Overview" };
  if (key.startsWith("valueOwners.")) return { tab: "Value to Owners", section: "Value to Owners" };
  if (key.startsWith("operations.")) return { tab: "Operating Model", section: "Operating Model & Standards" };
  if (key.startsWith("standards.")) return { tab: "Owner Considerations", section: "Owner Considerations" };
  if (key.startsWith("commercial.")) return { tab: "Commercial Engine", section: "Commercial Engine" };
  if (key.startsWith("economics.")) return { tab: "Economics & Obligations", section: "Economics & Obligations" };
  if (key.startsWith("loyalty.")) return { tab: "Loyalty Program", section: "Loyalty Program" };
  if (key.startsWith("footprint.")) return { tab: "Footprint & Growth", section: "Footprint & Growth" };
  if (key.startsWith("materials.")) return { tab: "Brand Materials", section: "Brand Materials" };
  if (key.startsWith("insight.")) return { tab: "Dealality Insight", section: "Dealality Insight" };
  if (key.startsWith("hero.")) return { tab: "Overview", section: "Hero" };
  return { tab: "Unknown", section: key.split(".")[0] || "Unknown" };
}

function classifySlotKey(slotKey, usedCount, completedCount, tributeHas) {
  if (SHOULD_REMAIN_BLANK_FOR_TRIBUTE.has(slotKey) && !tributeHas) return "should remain blank for Tribute";
  if (CORE_REQUIRED_SLOT_KEYS.has(slotKey)) return "core required";
  if (/^materials\.gallery\./i.test(slotKey)) return "media-only";
  if (/^materials\.(file|caseStudy)/i.test(slotKey)) return "source/material slot";
  if (/^footprint\.openings$/i.test(slotKey)) return "source/material slot";
  const freq = completedCount > 0 ? usedCount / completedCount : 0;
  if (freq >= 0.6) return "common completed-brand slot";
  return "optional / brand-specific";
}

function summarizeBrandSlots(brandPayload) {
  const blocks = Array.isArray(brandPayload?.brandExplorer?.blocks) ? brandPayload.brandExplorer.blocks : [];
  const slotMap = new Map();
  for (const block of blocks) {
    const slotKey = toText(block?.slotKey);
    if (!slotKey) continue;
    if (!slotMap.has(slotKey)) {
      slotMap.set(slotKey, {
        slotKey,
        rows: [],
        rowCount: 0,
        hasTitle: false,
        hasBody: false,
        hasImage: false,
        hasUrl: false,
      });
    }
    const row = slotMap.get(slotKey);
    const title = toText(block?.title);
    const body = toText(block?.body);
    const imageUrl = toText(block?.imageUrl);
    row.rows.push({
      recordId: toText(block?.recordId),
      title,
      body,
      imageUrl,
      url: firstUrl(`${title}\n${body}`),
    });
    row.rowCount += 1;
    row.hasTitle = row.hasTitle || hasVal(title);
    row.hasBody = row.hasBody || hasVal(body);
    row.hasImage = row.hasImage || hasVal(imageUrl);
    row.hasUrl = row.hasUrl || hasVal(firstUrl(`${title}\n${body}`));
  }
  return slotMap;
}

async function fetchBrandApiShape(brandIdOrName) {
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

function listAllReferenceFixtures() {
  const fixturesDir = path.join(ROOT, "fixtures");
  if (!fs.existsSync(fixturesDir)) return [];
  return fs
    .readdirSync(fixturesDir)
    .filter((n) => /^brand-explorer-presentation-.*\.(json)$/i.test(n))
    .map((n) => `fixtures/${n}`)
    .sort();
}

function buildWeakSlotReason(slotKey, tributeState, referenceStates, completedCount) {
  const usedRefs = referenceStates.filter((r) => r.hasSlot);
  if (!tributeState?.hasSlot) return "";
  if (!usedRefs.length) return "";
  const avgRows = usedRefs.reduce((sum, r) => sum + r.rowCount, 0) / usedRefs.length;
  const avgBodyRate = usedRefs.filter((r) => r.hasBody).length / usedRefs.length;
  const avgTitleRate = usedRefs.filter((r) => r.hasTitle).length / usedRefs.length;
  const avgImageRate = usedRefs.filter((r) => r.hasImage).length / usedRefs.length;
  const avgUrlRate = usedRefs.filter((r) => r.hasUrl).length / usedRefs.length;
  const issues = [];
  if (!tributeState.hasBody && avgBodyRate >= 0.6) issues.push("missing body where most completed brands include body");
  if (!tributeState.hasTitle && avgTitleRate >= 0.6) issues.push("missing title where most completed brands include title");
  if (!tributeState.hasImage && avgImageRate >= 0.6) issues.push("missing image where most completed brands include image");
  if (!tributeState.hasUrl && avgUrlRate >= 0.6) issues.push("missing URL/material link where most completed brands include one");
  if (tributeState.rowCount + 0.1 < avgRows && avgRows >= 1.5) {
    issues.push(`low row depth (${tributeState.rowCount} rows vs ${avgRows.toFixed(1)} avg)`);
  }
  const usageRate = completedCount > 0 ? usedRefs.length / completedCount : 0;
  if (!issues.length && usageRate >= 0.8 && tributeState.rowCount === 1 && /^materials\.file$/i.test(slotKey)) {
    issues.push("likely underpopulated compared with completed brands");
  }
  return issues.join("; ");
}

export async function buildBrandExplorerPresentationSlotCoverageAuditReport(options = {}) {
  const tributeBrandIdOrName = normalizeBrandInput(options.brandIdOrName);
  const tributePayload = await fetchBrandApiShape(tributeBrandIdOrName);
  if (!tributePayload) throw new Error(`Unable to read Tribute target brand: ${tributeBrandIdOrName}`);
  const tributeBrandName = toText(tributePayload?.name) || DEFAULT_TRIBUTE_BRAND_NAME;

  const referenceBrands = [];
  for (const name of REFERENCE_BRANDS) {
    const payload = await fetchBrandApiShape(name);
    referenceBrands.push({
      name,
      payload,
      source: payload ? "live-api" : "unavailable",
      readable: Boolean(payload),
    });
  }
  const completedReferences = referenceBrands.filter((b) => b.payload);

  const tributeSlots = summarizeBrandSlots(tributePayload);
  const refBrandSlotMaps = completedReferences.map((b) => ({
    name: b.name,
    slots: summarizeBrandSlots(b.payload),
  }));

  const unionSlotKeysSet = new Set();
  for (const { slots } of refBrandSlotMaps) {
    for (const slotKey of slots.keys()) unionSlotKeysSet.add(slotKey);
  }
  for (const slotKey of tributeSlots.keys()) unionSlotKeysSet.add(slotKey);
  const unionSlotKeys = Array.from(unionSlotKeysSet).sort((a, b) => a.localeCompare(b));

  const frequencyTable = unionSlotKeys.map((slotKey) => {
    const referenceStates = refBrandSlotMaps.map(({ name, slots }) => {
      const row = slots.get(slotKey);
      return {
        brand: name,
        hasSlot: Boolean(row),
        rowCount: row?.rowCount || 0,
        hasTitle: Boolean(row?.hasTitle),
        hasBody: Boolean(row?.hasBody),
        hasImage: Boolean(row?.hasImage),
        hasUrl: Boolean(row?.hasUrl),
      };
    });
    const brandsUsing = referenceStates.filter((r) => r.hasSlot).map((r) => r.brand);
    const usedCount = brandsUsing.length;
    const completedCount = completedReferences.length;
    const tributeRow = tributeSlots.get(slotKey);
    const tributeHas = Boolean(tributeRow);
    const classification = classifySlotKey(slotKey, usedCount, completedCount, tributeHas);
    const weakReason = buildWeakSlotReason(
      slotKey,
      {
        hasSlot: tributeHas,
        rowCount: tributeRow?.rowCount || 0,
        hasTitle: Boolean(tributeRow?.hasTitle),
        hasBody: Boolean(tributeRow?.hasBody),
        hasImage: Boolean(tributeRow?.hasImage),
        hasUrl: Boolean(tributeRow?.hasUrl),
      },
      referenceStates,
      completedCount
    );
    return {
      slotKey,
      ...tabSectionFromSlotKey(slotKey),
      brandsUsing,
      completedBrandsUsingCount: usedCount,
      completedBrandCount: completedCount,
      tributeHasSlot: tributeHas,
      tributeHasTitle: Boolean(tributeRow?.hasTitle),
      tributeHasBody: Boolean(tributeRow?.hasBody),
      tributeHasImage: Boolean(tributeRow?.hasImage),
      tributeHasUrlMaterialLink: Boolean(tributeRow?.hasUrl),
      tributeRowCount: tributeRow?.rowCount || 0,
      classification,
      weakReason,
    };
  });

  const tributeMissingSlots = frequencyTable.filter(
    (row) =>
      !row.tributeHasSlot &&
      row.classification !== "optional / brand-specific" &&
      row.classification !== "should remain blank for Tribute"
  );
  const coreRequiredSlotsMissing = tributeMissingSlots.filter((row) => row.classification === "core required");
  const commonCompletedBrandSlotsMissing = tributeMissingSlots.filter(
    (row) => row.classification === "common completed-brand slot"
  );
  const optionalBrandSpecificSlotsNotRequired = frequencyTable.filter(
    (row) => !row.tributeHasSlot && row.classification === "optional / brand-specific"
  );
  const weakTributeSlots = frequencyTable.filter((row) => row.tributeHasSlot && hasVal(row.weakReason));

  const scoringRows = frequencyTable.filter((row) => row.classification !== "should remain blank for Tribute");
  const slotCoverageCredits = scoringRows.map((row) => {
    const usageRate = row.completedBrandCount > 0 ? row.completedBrandsUsingCount / row.completedBrandCount : 0;
    const weight = row.classification === "core required" ? 3 : row.classification === "common completed-brand slot" ? 2 : 1;
    let credit = 0;
    if (row.tributeHasSlot) {
      credit = 1;
      if (!row.tributeHasBody && usageRate >= 0.6) credit -= 0.3;
      if (!row.tributeHasImage && row.classification === "media-only" && usageRate >= 0.6) credit -= 0.3;
      if (!row.tributeHasUrlMaterialLink && row.classification === "source/material slot" && usageRate >= 0.6) credit -= 0.2;
      if (hasVal(row.weakReason)) credit -= 0.15;
      if (credit < 0) credit = 0;
    }
    return { slotKey: row.slotKey, weight, credit };
  });
  const earned = slotCoverageCredits.reduce((sum, r) => sum + r.weight * r.credit, 0);
  const max = slotCoverageCredits.reduce((sum, r) => sum + r.weight, 0) || 1;
  const slotCoverageScore = Math.round((earned / max) * 100);
  const completedBrandComparableBySlotCoverage =
    slotCoverageScore >= 85 && coreRequiredSlotsMissing.length === 0 && commonCompletedBrandSlotsMissing.length <= 2;

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    copyUntouched: true,
    companyValidatedUntouched: true,
    brand: { recordId: toText(tributePayload?.id) || DEFAULT_TRIBUTE_BRAND_ID, name: tributeBrandName },
    filesRead: [
      "AGENTS.md",
      "api/brand-library.js",
      "docs/brand-explorer-presentation-slots.md",
      "lib/partner-intelligence/brand-explorer-display-parity-audit.js",
      "reports/brand-explorer-display-parity-audit.json",
      ...listAllReferenceFixtures(),
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-presentation-slot-coverage-audit.js",
      "scripts/brand-explorer-presentation-slot-coverage-audit.mjs",
      "docs/data-intelligence/brand-explorer-presentation-slot-coverage-audit-v17.md",
      "reports/brand-explorer-presentation-slot-coverage-audit.md",
      "reports/brand-explorer-presentation-slot-coverage-audit.json",
      "package.json",
    ],
    referenceBrandsInspected: referenceBrands.map((r) => ({ name: r.name, source: r.source, readable: r.readable })),
    completedReferenceBrandCount: completedReferences.length,
    slotCoverageModel: {
      classifications: [
        "core required",
        "common completed-brand slot",
        "optional / brand-specific",
        "media-only",
        "source/material slot",
        "should remain blank for Tribute",
      ],
      coreRequiredSlotKeys: Array.from(CORE_REQUIRED_SLOT_KEYS).sort((a, b) => a.localeCompare(b)),
      shouldRemainBlankSlotKeys: Array.from(SHOULD_REMAIN_BLANK_FOR_TRIBUTE).sort((a, b) => a.localeCompare(b)),
    },
    unionSlotKeyCountAcrossCompletedBrands: unionSlotKeys.length,
    frequencyTable,
    tributeSlotKeysPresent: Array.from(tributeSlots.keys()).sort((a, b) => a.localeCompare(b)),
    tributeSlotKeysMissing: tributeMissingSlots.map((row) => row.slotKey),
    coreRequiredSlotsMissing: coreRequiredSlotsMissing.map((row) => row.slotKey),
    commonCompletedBrandSlotsMissing: commonCompletedBrandSlotsMissing.map((row) => row.slotKey),
    optionalBrandSpecificSlotsNotRequired: optionalBrandSpecificSlotsNotRequired.map((row) => row.slotKey),
    weakTributeSlots: weakTributeSlots.map((row) => ({
      slotKey: row.slotKey,
      tab: row.tab,
      section: row.section,
      reason: row.weakReason,
    })),
    slotCoverageScore,
    completedBrandComparableBySlotCoverage,
    v18SlotCompletionWriterNeeded: !completedBrandComparableBySlotCoverage,
    exactNextCommand: "npm run brand-explorer-presentation-slot-coverage-audit -- --brand tribute-portfolio --dry-run",
  };
}

function short(text, max = 120) {
  const s = toText(text).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
}

export function buildBrandExplorerPresentationSlotCoverageAuditMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Presentation Slot Coverage Audit v17");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Reference brands inspected");
  report.referenceBrandsInspected.forEach((r) => {
    lines.push(`- ${r.name} · source: ${r.source} · readable: ${r.readable ? "yes" : "no"}`);
  });
  lines.push("");
  lines.push("## Slot inventory outcomes");
  lines.push(`- Total unique slot keys across completed brands: **${report.unionSlotKeyCountAcrossCompletedBrands}**`);
  lines.push(`- Tribute slot keys present: **${report.tributeSlotKeysPresent.length}**`);
  lines.push(`- Tribute slot keys missing: **${report.tributeSlotKeysMissing.length}**`);
  lines.push(`- Core required slots missing: **${report.coreRequiredSlotsMissing.length}**`);
  lines.push(`- Common completed-brand slots missing: **${report.commonCompletedBrandSlotsMissing.length}**`);
  lines.push(`- Weak Tribute slots: **${report.weakTributeSlots.length}**`);
  lines.push("");
  lines.push("## New slot-coverage score");
  lines.push(`- Slot-coverage score: **${report.slotCoverageScore}/100**`);
  lines.push(
    `- Completed-brand comparable by slot coverage: **${report.completedBrandComparableBySlotCoverage ? "yes" : "no"}**`
  );
  lines.push(`- v18 slot completion writer needed: **${report.v18SlotCompletionWriterNeeded ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Frequency table (selected)");
  lines.push("");
  lines.push("| Slot key | Tab | Class | Completed use | Tribute | Title | Body | Image | URL |");
  lines.push("|---|---|---|---:|---|---|---|---|---|");
  report.frequencyTable.slice(0, 80).forEach((row) => {
    lines.push(
      `| ${row.slotKey} | ${row.tab} | ${row.classification} | ${row.completedBrandsUsingCount}/${row.completedBrandCount} | ${row.tributeHasSlot ? "yes" : "no"} | ${row.tributeHasTitle ? "yes" : "no"} | ${row.tributeHasBody ? "yes" : "no"} | ${row.tributeHasImage ? "yes" : "no"} | ${row.tributeHasUrlMaterialLink ? "yes" : "no"} |`
    );
  });
  if (report.frequencyTable.length > 80) {
    lines.push(`| ... ${report.frequencyTable.length - 80} additional rows | | | | | | | | |`);
  }
  lines.push("");
  lines.push("## Missing slot details");
  lines.push(`- Core required missing: ${report.coreRequiredSlotsMissing.join(", ") || "None"}`);
  lines.push(`- Common completed-brand missing: ${report.commonCompletedBrandSlotsMissing.join(", ") || "None"}`);
  lines.push(`- Optional/brand-specific not required: ${report.optionalBrandSpecificSlotsNotRequired.join(", ") || "None"}`);
  lines.push("");
  lines.push("## Weak Tribute slots");
  report.weakTributeSlots.forEach((row) => {
    lines.push(`- \`${row.slotKey}\` (${row.tab}) — ${short(row.reason, 180)}`);
  });
  if (!report.weakTributeSlots.length) lines.push("- None");
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- No Airtable writes: **${report.airtableModified ? "no" : "yes"}**`);
  lines.push(`- Images changed: **${report.imagesUntouched ? "no" : "yes"}**`);
  lines.push(`- Copy changed: **${report.copyUntouched ? "no" : "yes"}**`);
  lines.push(`- Company Validated fields changed: **${report.companyValidatedUntouched ? "no" : "yes"}**`);
  lines.push("");
  lines.push("## Next command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}
