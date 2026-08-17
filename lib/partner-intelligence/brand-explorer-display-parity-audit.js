import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const WRITER_VERSION = "15";
export const REPORT_JSON_NAME = "brand-explorer-display-parity-audit.json";
export const REPORT_MD_NAME = "brand-explorer-display-parity-audit.md";
export const DOC_MD_NAME = "brand-explorer-display-parity-audit-v15.md";

const TRIBUTE_BRAND_ID = "recCvV0PuZOi8c3hC";
const TRIBUTE_BRAND_NAME = "Tribute Portfolio";

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

const UI_TABS = [
  "Overview",
  "Value to Owners",
  "Operating Model",
  "Owner Considerations",
  "Commercial Engine",
  "Economics & Obligations",
  "Loyalty Program",
  "Footprint & Growth",
  "Brand Materials",
  "Dealality Insight",
];

export const SECTION_DEFS = [
  { key: "brandPositioning", tab: "Overview", label: "Brand positioning section", extract: (b) => fromValue(b.brandPositioning) },
  { key: "heroSection", tab: "Overview", label: "Hero section", extract: (b) => fromSlotFirst(b, "overview.hero") },
  { key: "brandIdentity", tab: "Overview", label: "Brand identity / parent / collection context", extract: (b) => fromLines([b.name, b.parentCompany, b.brandArchitecture]) },
  { key: "positioningSummary", tab: "Overview", label: "Positioning summary", extract: (b) => fromValue(b.brandPositioning) },
  { key: "brandPromise", tab: "Overview", label: "Brand promise", extract: (b) => fromValue(b.brandCustomerPromise) },
  { key: "guestValueProp", tab: "Overview", label: "Guest value proposition", extract: (b) => fromValue(b.brandValueProposition) },
  { key: "ownerValueProp", tab: "Value to Owners", label: "Owner value proposition", extract: (b) => fromSlotMergedOrField(b, "valueOwners.overview", "brandValueProposition") },
  { key: "idealAsset", tab: "Overview", label: "Ideal asset / typical use case", extract: (b) => fromSlotMergedOrField(b, "overview.typical_use_case", "brandProfileAnalysis") },
  { key: "valueScenarios", tab: "Overview", label: "Where This Brand Creates the Most Value", extract: (b) => fromScenarioSlots(b) },
  { key: "galleryVisuals", tab: "Brand Materials", label: "Gallery / visual materials", extract: (b) => fromGallerySlots(b) },
  { key: "standardsIntro", tab: "Owner Considerations", label: "Brand Standards / Owner Considerations", extract: (b) => fromSlotMergedOrField(b, "standards.intro", "brandStandardsIntro") },
  { key: "ownerQuestions", tab: "Owner Considerations", label: "Owner questions", extract: (b) => fromSlotMergedOrField(b, "standards.questions", "questionsOwnersShouldAsk") },
  { key: "developmentModel", tab: "Overview", label: "Development model", extract: (b) => fromSlotMergedOrField(b, "overview.development_model", "brandModelFormat") },
  { key: "conversionFit", tab: "Overview", label: "Conversion / adaptive reuse fit", extract: (b) => fromScenarioSlots(b) },
  { key: "loyaltyDistribution", tab: "Loyalty Program", label: "Loyalty / distribution relationship", extract: (b) => fromLines([b.brandTaglineMotto, b?.loyaltyCommercial?.typicalLoyaltyProgramName]) },
  { key: "regionalRelevance", tab: "Footprint & Growth", label: "Market / regional relevance", extract: (b) => fromLines([b.regionOffered, slotMerged(b, "footprint.geo_intro")]) },
  { key: "sourceMaterials", tab: "Brand Materials", label: "Source links / PDFs / materials", extract: (b) => fromSlotRows(b, "materials.file") },
  { key: "recentOpeningsPr", tab: "Footprint & Growth", label: "Recent openings / PR", extract: (b) => fromSlotRows(b, "footprint.openings"), mayRemainBlank: true },
  { key: "dataGapsCaveats", tab: "Dealality Insight", label: "Data gaps / caveats", extract: (b) => fromDealalityInsightCaveats(b) },
  { key: "trustChip", tab: "Dealality Insight", label: "Trust chip / source basis", extract: (b) => fromGovernanceTrustChip(b) },
];

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

function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && String(b.slotKey) === String(slotKey));
}

function slotMerged(brand, slotKey) {
  return blocksForSlot(brand, slotKey)
    .map((b) => [toText(b.title), toText(b.body)].filter(hasVal).join(": "))
    .filter(hasVal)
    .join("\n\n");
}

function fromValue(v) {
  const text = toText(v);
  return { visible: hasVal(text), title: "", body: text, depth: wordCount(text) };
}

function fromLines(lines) {
  const text = lines.flatMap((v) => (Array.isArray(v) ? v : [v])).map(toText).filter(hasVal).join(" | ");
  return { visible: hasVal(text), title: "", body: text, depth: wordCount(text) };
}

function fromSlotFirst(brand, slotKey) {
  const row = blocksForSlot(brand, slotKey)[0];
  const title = toText(row?.title);
  const body = toText(row?.body);
  const text = [title, body].filter(hasVal).join(" | ");
  return { visible: hasVal(text), title, body, depth: wordCount(text) };
}

function fromSlotMergedOrField(brand, slotKey, field) {
  const slotText = slotMerged(brand, slotKey);
  const fieldText = toText(brand?.[field]);
  const text = slotText || fieldText;
  return { visible: hasVal(text), title: "", body: text, depth: wordCount(text) };
}

function fromSlotRows(brand, slotKey) {
  const rows = blocksForSlot(brand, slotKey);
  const text = rows
    .map((r) => [toText(r.title), toText(r.body)].filter(hasVal).join(" | "))
    .filter(hasVal)
    .join("\n");
  return { visible: rows.length > 0 && hasVal(text), title: "", body: text, depth: wordCount(text) };
}

function fromScenarioSlots(brand) {
  const keys = ["overview.scenario.1", "overview.scenario.2", "overview.scenario.3"];
  const rows = keys.flatMap((k) => blocksForSlot(brand, k));
  const text = rows
    .map((r) => [toText(r.title), toText(r.body)].filter(hasVal).join(" | "))
    .filter(hasVal)
    .join("\n");
  return { visible: rows.length > 0 && hasVal(text), title: "", body: text, depth: wordCount(text) };
}

function fromGallerySlots(brand) {
  const rows = Array.from({ length: 6 }, (_, i) => blocksForSlot(brand, `materials.gallery.${i + 1}`)[0]).filter(Boolean);
  const text = rows.map((r, i) => `gallery.${i + 1}: ${toText(r.title)} ${hasVal(r.imageUrl) ? "[image]" : ""}`.trim()).join("\n");
  return { visible: rows.length > 0, title: "", body: text, depth: wordCount(text) };
}

function fromLoadWarnings(loadWarnings) {
  const list = Array.isArray(loadWarnings) ? loadWarnings : [];
  const text = list.join(" | ");
  return { visible: list.length > 0, title: "", body: text, depth: wordCount(text) };
}

/** Matches Brand Explorer hero trust chip: brand.governance via ProfileGovernanceTrustChip. */
function fromGovernanceTrustChip(brand) {
  const gov = brand?.governance || {};
  const label = toText(gov.displayLabel);
  const subtitle = toText(gov.displaySubtitle);
  const text = [label, subtitle].filter(hasVal).join(" | ");
  return { visible: hasVal(text), title: label, body: text, depth: wordCount(text) };
}

/** Matches Dealality Insight tab: insight.summary slot first, then loadWarnings. */
function fromDealalityInsightCaveats(brand) {
  const insightSummary = slotMerged(brand, "insight.summary");
  const warningText = fromLoadWarnings(brand?.loadWarnings).body;
  const text = [insightSummary, warningText].filter(hasVal).join(" | ");
  return { visible: hasVal(text), title: "", body: text, depth: wordCount(text) };
}

function wordCount(text) {
  return toText(text).split(/\s+/).filter(Boolean).length;
}

function short(text, max = 240) {
  const s = toText(text).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
}

async function fetchBrandApiShape(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) { this.statusCode = code; return this; },
    json(payload) { this.payload = payload; return this; },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function sectionGapSeverity(tribute, referenceStats, def) {
  if (def.mayRemainBlank && !tribute.visible) return "complete";
  if (referenceStats.visibleRatio >= 0.6 && !tribute.visible) return "missing";
  if (!tribute.visible && referenceStats.visibleRatio < 0.6) return "not displayable";
  if (/overview\.scenario\./i.test(tribute.body) || /\bhotel\b/i.test(tribute.body) && def.key === "valueScenarios") return "wrong content model";
  const ratio = referenceStats.avgDepth > 0 ? tribute.depth / referenceStats.avgDepth : 1;
  if (ratio < 0.5) return "weak";
  if (ratio < 0.85) return "minor polish";
  return "complete";
}

export function severityScore(sev) {
  if (sev === "complete") return 1;
  if (sev === "minor polish") return 0.75;
  if (sev === "weak") return 0.4;
  if (sev === "not displayable") return 0.25;
  if (sev === "wrong content model") return 0.15;
  return 0;
}

function proposedContentBySection(sectionKey, severity) {
  if (severity === "complete") return "";
  if (sectionKey === "standardsIntro") return "Draft owner-facing standards intro with explicit flexibility boundaries, QA rhythm, and operator obligations.";
  if (sectionKey === "ownerQuestions") return "Add 5-7 diligence questions tied to PIP scope, loyalty economics, and conversion timeline assumptions.";
  if (sectionKey === "valueScenarios") return "Replace property-name-led cards with strategic use cases: resort leisure, urban repositioning, and conversion.";
  if (sectionKey === "recentOpeningsPr") return "Keep blank until source-backed openings/PR rows are captured.";
  if (sectionKey === "dataGapsCaveats") return "Add explicit caveat line summarizing unresolved evidence gaps and freshness risks.";
  return "Expand section copy to completed-brand depth and owner-facing specificity.";
}

function sourceBasisForProposal(sectionKey) {
  if (["brandIdentity", "positioningSummary", "brandPromise", "developmentModel", "trustChip"].includes(sectionKey)) {
    return "source-backed current fields";
  }
  if (sectionKey === "recentOpeningsPr") return "insufficient source-backed openings evidence";
  return "AI-drafted from completed-brand display standard";
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

export async function buildBrandExplorerDisplayParityAuditReport() {
  const tribute = await fetchBrandApiShape(TRIBUTE_BRAND_ID);
  if (!tribute) throw new Error("Unable to read Tribute normalized brand output.");

  const referenceBrands = [];
  for (const name of REFERENCE_BRANDS) {
    const payload = await fetchBrandApiShape(name);
    if (payload) referenceBrands.push({ name, payload, source: "live-api" });
    else referenceBrands.push({ name, payload: null, source: "unavailable" });
  }

  const sectionRows = [];
  for (const def of SECTION_DEFS) {
    const tributeSection = def.extract(tribute);
    const referenceSections = referenceBrands
      .filter((r) => r.payload)
      .map((r) => ({ name: r.name, section: def.extract(r.payload) }));
    const visibleRefs = referenceSections.filter((r) => r.section.visible);
    const avgDepth = visibleRefs.length
      ? Math.round(visibleRefs.reduce((sum, r) => sum + r.section.depth, 0) / visibleRefs.length)
      : 0;
    const bestRef = visibleRefs.sort((a, b) => b.section.depth - a.section.depth)[0] || null;
    const refStats = {
      visibleRatio: referenceSections.length ? visibleRefs.length / referenceSections.length : 0,
      avgDepth,
      bestRefName: bestRef?.name || "",
      bestRefText: bestRef ? short(bestRef.section.body, 220) : "",
    };
    const severity = sectionGapSeverity(tributeSection, refStats, def);
    sectionRows.push({
      key: def.key,
      tab: def.tab,
      section: def.label,
      tributeVisible: tributeSection.visible,
      referenceVisibility: Object.fromEntries(referenceSections.map((r) => [r.name, r.section.visible])),
      tributeTitle: short(tributeSection.title, 120),
      tributeBody: short(tributeSection.body, 260),
      averageReferenceDepthWords: refStats.avgDepth,
      bestReferenceExample: refStats.bestRefName ? `${refStats.bestRefName}: ${refStats.bestRefText}` : "",
      requiredContentLevelForCompletedBrandQuality:
        refStats.avgDepth > 0
          ? `Visible with approximately ${refStats.avgDepth}+ words owner-facing detail`
          : "Visible owner-facing section where applicable",
      tributeGapSeverity: severity,
      proposedTributeContent: proposedContentBySection(def.key, severity),
      sourceBasis: sourceBasisForProposal(def.key),
      humanReviewStatus:
        sourceBasisForProposal(def.key) === "source-backed current fields"
          ? "source-backed"
          : sourceBasisForProposal(def.key) === "insufficient source-backed openings evidence"
            ? "hold blank pending source-backed evidence"
            : "AI-drafted / human-review required",
      safeToWriteLater: !def.mayRemainBlank || severity !== "complete",
      shouldRemainBlank: Boolean(def.mayRemainBlank && severity === "complete"),
    });
  }

  const score = Math.round(
    (sectionRows.reduce((sum, row) => sum + severityScore(row.tributeGapSeverity), 0) / sectionRows.length) * 100
  );

  const tabSummary = UI_TABS.map((tab) => {
    const rows = sectionRows.filter((r) => r.tab === tab);
    return {
      tab,
      sections: rows.map((r) => r.section),
      gaps: rows.filter((r) => r.tributeGapSeverity !== "complete").map((r) => `${r.section} (${r.tributeGapSeverity})`),
    };
  });

  const worst = sectionRows
    .filter((r) => ["missing", "weak", "wrong content model", "not displayable"].includes(r.tributeGapSeverity))
    .map((r) => `${r.section} (${r.tributeGapSeverity})`);

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    companyValidatedUntouched: true,
    filesRead: [
      "AGENTS.md",
      "api/brand-library.js",
      "api/lib/partner-intelligence-field-map.js",
      "api/lib/partner-intelligence-explorer-field-registry.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "docs/brand-explorer-presentation-slots.md",
      "docs/data-intelligence/BRAND_PROFILE_DATA_MODEL.md",
      "reports/tribute-brand-explorer-content-parity-audit.md",
      "reports/tribute-brand-explorer-content-promotion-writer.md",
      "reports/tribute-existing-brand-field-validation-audit.md",
      "reports/tribute-portfolio-package-pipeline.md",
      "reports/brand-explorer-visual-qa-verification.md",
      ...listAllReferenceFixtures(),
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-display-parity-audit.js",
      "scripts/brand-explorer-display-parity-audit.mjs",
      "docs/data-intelligence/brand-explorer-display-parity-audit-v15.md",
      "reports/brand-explorer-display-parity-audit.md",
      "reports/brand-explorer-display-parity-audit.json",
      "package.json",
    ],
    referenceBrandsInspected: referenceBrands.map((r) => ({ name: r.name, source: r.source, readable: Boolean(r.payload) })),
    actualBrandExplorerDisplayMap: {
      tabs: UI_TABS,
      sections: SECTION_DEFS.map((s) => ({ key: s.key, tab: s.tab, section: s.label })),
    },
    tabByTabComparison: tabSummary,
    sectionBySectionComparison: sectionRows,
    completedBrandContentStandardBySection: sectionRows.map((r) => ({
      section: r.section,
      requiredLevel: r.requiredContentLevelForCompletedBrandQuality,
      bestReferenceExample: r.bestReferenceExample,
    })),
    tributeCurrentContentBySection: sectionRows.map((r) => ({ section: r.section, title: r.tributeTitle, body: r.tributeBody })),
    tributeMissingWeakWrongSections: sectionRows.filter((r) => r.tributeGapSeverity !== "complete").map((r) => ({ section: r.section, severity: r.tributeGapSeverity })),
    proposedTributeCompletionContentBySection: sectionRows
      .filter((r) => hasVal(r.proposedTributeContent))
      .map((r) => ({ section: r.section, proposed: r.proposedTributeContent })),
    sourceBackedProposals: sectionRows.filter((r) => r.sourceBasis === "source-backed current fields").map((r) => r.section),
    aiDraftedHumanReviewProposals: sectionRows
      .filter((r) => r.sourceBasis === "AI-drafted from completed-brand display standard")
      .map((r) => r.section),
    sectionsShouldRemainBlank: sectionRows.filter((r) => r.shouldRemainBlank).map((r) => r.section),
    tributeDisplayParityScore: score,
    completedBrandComparableToday: score >= 85 && worst.length === 0,
    biggestDiscrepanciesVersusCompletedBrands: worst,
    v16DisplayContentCompletionWriterNeeded: worst.length > 0,
    exactNextCommand: "npm run brand-explorer-display-parity-audit -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerDisplayParityAuditMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Display Parity Audit v15");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${TRIBUTE_BRAND_NAME} \`${TRIBUTE_BRAND_ID}\``);
  lines.push("");
  lines.push("## Reference brands inspected");
  report.referenceBrandsInspected.forEach((r) => {
    lines.push(`- ${r.name} · source: ${r.source} · readable: ${r.readable ? "yes" : "no"}`);
  });
  lines.push("");
  lines.push("## Actual Brand Explorer display map");
  lines.push(`- Tabs: ${report.actualBrandExplorerDisplayMap.tabs.join(" | ")}`);
  lines.push(`- Section count: ${report.actualBrandExplorerDisplayMap.sections.length}`);
  lines.push("");
  lines.push("## Tab-by-tab comparison table");
  lines.push("");
  lines.push("| Tab | Sections | Gaps |");
  lines.push("|---|---|---|");
  report.tabByTabComparison.forEach((row) => {
    lines.push(`| ${row.tab} | ${row.sections.join("; ")} | ${row.gaps.length ? row.gaps.join("; ") : "None"} |`);
  });
  lines.push("");
  lines.push("## Section-by-section comparison table");
  lines.push("");
  lines.push("| Section | Tribute visible | Avg ref depth | Severity | Proposed |");
  lines.push("|---|---|---:|---|---|");
  report.sectionBySectionComparison.forEach((row) => {
    lines.push(
      `| ${row.section} | ${row.tributeVisible ? "yes" : "no"} | ${row.averageReferenceDepthWords} | ${row.tributeGapSeverity} | ${short(
        row.proposedTributeContent,
        120
      )} |`
    );
  });
  lines.push("");
  lines.push("## Key outcomes");
  lines.push(`- New display-parity completion score: **${report.tributeDisplayParityScore}/100**`);
  lines.push(`- Completed-brand comparable today: **${report.completedBrandComparableToday ? "yes" : "no"}**`);
  lines.push(`- v16 completion writer needed: **${report.v16DisplayContentCompletionWriterNeeded ? "yes" : "no"}**`);
  lines.push(`- Biggest discrepancies: ${report.biggestDiscrepanciesVersusCompletedBrands.join("; ") || "None"}`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Next command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}
