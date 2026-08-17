import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const REMEDIATION_VERSION = "25C";
export const REPORT_JSON_NAME = "brand-explorer-missing-content-remediation.json";
export const REPORT_MD_NAME = "brand-explorer-missing-content-remediation.md";
export const DOC_MD_NAME = "brand-explorer-missing-content-remediation-v25C.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const TRIBUTE_BRAND_ID = "recCvV0PuZOi8c3hC";
const CURIO_BRAND_ID = "receQkxgjlezsc1xg";

const CLASSIFICATIONS = Object.freeze({
  READY: "ready_to_populate_now",
  ROW_CREATE: "row_creation_required",
  SOURCE_REQUIRED: "source_evidence_required",
  FOUNDER_REQUIRED: "founder_review_required",
  FRONTEND_REQUIRED: "frontend_mapping_required",
  SUPPRESS: "suppress_until_ready",
  FIXED: "already_fixed",
  NOT_SAFE: "not_safe_to_populate",
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function readJsonIfExists(relPath) {
  const full = path.join(ROOT, relPath);
  if (!fs.existsSync(full)) return null;
  try {
    return JSON.parse(fs.readFileSync(full, "utf8"));
  } catch {
    return null;
  }
}

function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && nz(b.slotKey) === nz(slotKey));
}

function parseBodyParagraphs(body) {
  return String(body || "")
    .split(/\n\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function firstHttp(paras) {
  return paras.find((p) => /^https?:\/\//i.test(p)) || "";
}

function openingCardCompleteness(row) {
  const title = nz(row?.title);
  const image = nz(row?.imageUrl);
  const paras = parseBodyParagraphs(row?.body);
  const filtered = paras.filter((p) => !/^https?:\/\//i.test(p));
  const location = filtered[1] || "";
  const summary = filtered[3] || filtered[4] || filtered[0] || "";
  const url = nz(row?.summaryUrl) || firstHttp(paras);
  return {
    hasTitle: hasVal(title),
    hasImage: hasVal(image),
    hasLocationOrDescriptor: hasVal(location),
    hasSummary: hasVal(summary),
    hasUrl: hasVal(url),
    complete: [title, image, location, summary, url].every(hasVal),
    preview: { title, imageUrl: image, locationOrDescriptor: location, summary, url },
  };
}

function momentumCompleteness(row) {
  const title = nz(row?.title);
  const paras = parseBodyParagraphs(row?.body);
  const date = paras[0] || "";
  const sourceUrl = firstHttp(paras);
  return {
    hasTitle: hasVal(title),
    hasDate: hasVal(date),
    hasSourceUrl: hasVal(sourceUrl),
    complete: hasVal(date) && hasVal(sourceUrl),
    preview: { title, date, sourceUrl },
  };
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

function sectionResult(input) {
  return {
    section: input.section,
    classification: input.classification,
    currentRenderedIssue: input.currentRenderedIssue,
    slotKeys: input.slotKeys,
    rowsFound: input.rowsFound ?? 0,
    rowsMissing: input.rowsMissing ?? 0,
    sourceFactSupportAvailable: input.sourceFactSupportAvailable,
    shouldRenderToday: Boolean(input.shouldRenderToday),
    shouldSuppressToday: Boolean(input.shouldSuppressToday),
    proposedRowPayloadsIfSafe: input.proposedRowPayloadsIfSafe || [],
    exactBlockerIfNotSafe: input.exactBlockerIfNotSafe || "",
    recommendedNextWriter: input.recommendedNextWriter || "",
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Missing Content Remediation v25C");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Section Status");
  for (const s of report.sections) {
    lines.push(`- **${s.section}**: \`${s.classification}\` · rows found ${s.rowsFound}, missing ${s.rowsMissing}`);
  }
  lines.push("");
  lines.push("## Suppress Today");
  for (const rule of report.exactSuppressionRulesRecommended) lines.push(`- ${rule}`);
  if (!report.exactSuppressionRulesRecommended.length) lines.push("- none");
  lines.push("");
  lines.push("## Safe Payloads");
  if (report.exactProposedRowPayloadsIfSafe.length) {
    for (const p of report.exactProposedRowPayloadsIfSafe) {
      lines.push(`- \`${p.slotKey}\`: ${p.reason}`);
    }
  } else {
    lines.push("- none safe for immediate write");
  }
  lines.push("");
  lines.push("## Writer Sequence");
  lines.push(`- v25C-1 suppression writer: ${report.recommendedWriterSequence.v25C_1_suppression_writer}`);
  lines.push(`- v25C-2 safe row creation writer: ${report.recommendedWriterSequence.v25C_2_safe_row_creation_writer}`);
  lines.push(`- v25C-3 frontend mapping writer: ${report.recommendedWriterSequence.v25C_3_frontend_mapping_writer}`);
  lines.push(`- v25C-4 evidence/source capture writer: ${report.recommendedWriterSequence.v25C_4_evidence_source_capture_writer}`);
  lines.push("");
  lines.push("## Exact next command");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}

export async function buildBrandExplorerMissingContentRemediationReport(options = {}) {
  const brandIdOrName = nz(options.brandIdOrName || "tribute-portfolio");
  const tribute = await fetchBrandApiShape(brandIdOrName === "tribute-portfolio" ? TRIBUTE_BRAND_ID : brandIdOrName);
  if (!tribute) throw new Error(`Could not load brand: ${brandIdOrName}`);
  const curio = await fetchBrandApiShape(CURIO_BRAND_ID);

  const screenshotPkg = readJsonIfExists("reports/brand-explorer-screenshot-seeded-remediation-review-package.json");
  const evidencePkg = readJsonIfExists("reports/brand-explorer-evidence-fact-review-package.json");
  const targetedExtract = readJsonIfExists("reports/tribute-portfolio-targeted-extract.json");

  const openingsRows = blocksForSlot(tribute, "footprint.openings");
  const openingChecks = openingsRows.map(openingCardCompleteness);
  const completeOpeningRows = openingChecks.filter((r) => r.complete);
  const openingsMissing = Math.max(0, 1 - completeOpeningRows.length);

  const momentumRows = blocksForSlot(tribute, "footprint.momentum");
  const momentumChecks = momentumRows.map(momentumCompleteness);
  const completeMomentumRows = momentumChecks.filter((r) => r.complete);

  const mixRows = blocksForSlot(tribute, "footprint.portfolio_mix");
  const contextRows = blocksForSlot(tribute, "overview.portfolio_context");
  const standardsRows = blocksForSlot(tribute, "standards.requirement");
  const demandRows = blocksForSlot(tribute, "commercial.demand");
  const loyaltyRows = [
    ...blocksForSlot(tribute, "loyalty.earn"),
    ...blocksForSlot(tribute, "loyalty.redeem"),
    ...blocksForSlot(tribute, "loyalty.elite"),
    ...blocksForSlot(tribute, "loyalty.proof"),
    ...blocksForSlot(tribute, "loyalty.kpi.members"),
    ...blocksForSlot(tribute, "loyalty.kpi.hotels"),
    ...blocksForSlot(tribute, "loyalty.kpi.markets"),
    ...blocksForSlot(tribute, "loyalty.kpi.mix"),
  ];
  const geoRows = [
    ...blocksForSlot(tribute, "footprint.region.am"),
    ...blocksForSlot(tribute, "footprint.region.cala"),
    ...blocksForSlot(tribute, "footprint.region.eu"),
    ...blocksForSlot(tribute, "footprint.region.mea"),
    ...blocksForSlot(tribute, "footprint.region.apac"),
  ];

  const curioOpeningsCount = blocksForSlot(curio, "footprint.openings").length;
  const curioDemandCount = blocksForSlot(curio, "commercial.demand").length;

  const sections = [];

  sections.push(
    sectionResult({
      section: "Openings / Examples / Properties",
      classification:
        completeOpeningRows.length > 0 ? CLASSIFICATIONS.ROW_CREATE : CLASSIFICATIONS.SUPPRESS,
      currentRenderedIssue:
        completeOpeningRows.length > 0
          ? "Rows exist but completeness is mixed; ensure only complete cards render."
          : "No complete property cards; UI risks empty/disabled opening state.",
      slotKeys: ["footprint.openings", "materials.caseStudy"],
      rowsFound: openingsRows.length,
      rowsMissing: openingsMissing,
      sourceFactSupportAvailable: completeOpeningRows.length > 0 ? "partial" : "insufficient",
      shouldRenderToday: completeOpeningRows.length > 0,
      shouldSuppressToday: completeOpeningRows.length === 0,
      proposedRowPayloadsIfSafe: [],
      exactBlockerIfNotSafe:
        completeOpeningRows.length === 0
          ? "No complete row with title + image + location/descriptor + summary + URL."
          : "",
      recommendedNextWriter:
        completeOpeningRows.length > 0 ? "v25C-2 safe row creation writer" : "v25C-1 suppression writer",
    })
  );

  sections.push(
    sectionResult({
      section: "Recent Momentum",
      classification: completeMomentumRows.length ? CLASSIFICATIONS.ROW_CREATE : CLASSIFICATIONS.SUPPRESS,
      currentRenderedIssue:
        completeMomentumRows.length ? "Momentum rows exist; ensure only dated/source-backed rows render." : "Recent Momentum empty placeholder can render.",
      slotKeys: ["footprint.momentum"],
      rowsFound: momentumRows.length,
      rowsMissing: completeMomentumRows.length ? 0 : 1,
      sourceFactSupportAvailable: completeMomentumRows.length ? "partial" : "insufficient",
      shouldRenderToday: completeMomentumRows.length > 0,
      shouldSuppressToday: completeMomentumRows.length === 0,
      proposedRowPayloadsIfSafe: [],
      exactBlockerIfNotSafe:
        completeMomentumRows.length === 0 ? "No dated + source-backed momentum rows." : "",
      recommendedNextWriter:
        completeMomentumRows.length ? "v25C-2 safe row creation writer" : "v25C-1 suppression writer",
    })
  );

  sections.push(
    sectionResult({
      section: "Portfolio Mix",
      classification: mixRows.length > 1 ? CLASSIFICATIONS.FOUNDER_REQUIRED : CLASSIFICATIONS.SOURCE_REQUIRED,
      currentRenderedIssue: `Thin portfolio mix (${mixRows.length} row${mixRows.length === 1 ? "" : "s"}) may render as unsupported chip(s).`,
      slotKeys: ["footprint.portfolio_mix"],
      rowsFound: mixRows.length,
      rowsMissing: mixRows.length > 0 ? 0 : 1,
      sourceFactSupportAvailable: "limited",
      shouldRenderToday: mixRows.length > 1,
      shouldSuppressToday: mixRows.length <= 1,
      proposedRowPayloadsIfSafe: [],
      exactBlockerIfNotSafe: "No source-backed Tribute portfolio-mix framing approved for external display.",
      recommendedNextWriter: "v25C-4 evidence/source capture writer",
    })
  );

  const ctxBody = nz(contextRows[0]?.body);
  const genericLadder = /lower-scale|mid-scale|upscale|upper-scale/i.test(ctxBody) || !hasVal(ctxBody);
  sections.push(
    sectionResult({
      section: "Portfolio Context",
      classification: CLASSIFICATIONS.FRONTEND_REQUIRED,
      currentRenderedIssue: genericLadder
        ? "Generic ladder labels render; Marriott sibling ladder mapping is missing."
        : "Context body exists but frontend still requires parent-specific ladder mapping.",
      slotKeys: ["overview.portfolio_context"],
      rowsFound: contextRows.length,
      rowsMissing: contextRows.length ? 0 : 1,
      sourceFactSupportAvailable: "n/a_frontend_mapping",
      shouldRenderToday: !genericLadder,
      shouldSuppressToday: false,
      proposedRowPayloadsIfSafe: [],
      exactBlockerIfNotSafe: "No Marriott-specific frontend ladder mapping implemented.",
      recommendedNextWriter: "v25C-3 frontend mapping writer",
    })
  );

  sections.push(
    sectionResult({
      section: "Standard Detail / Where Available",
      classification: standardsRows.length ? CLASSIFICATIONS.FOUNDER_REQUIRED : CLASSIFICATIONS.NOT_SAFE,
      currentRenderedIssue: standardsRows.length
        ? "Structured standards rows exist but require external-display safety review."
        : "No structured standards table rows; placeholder fallback appears.",
      slotKeys: ["standards.requirement"],
      rowsFound: standardsRows.length,
      rowsMissing: standardsRows.length ? 0 : 1,
      sourceFactSupportAvailable: standardsRows.length ? "partial" : "insufficient",
      shouldRenderToday: standardsRows.length > 0,
      shouldSuppressToday: standardsRows.length === 0,
      proposedRowPayloadsIfSafe: [],
      exactBlockerIfNotSafe:
        "No external-display-safe owner-planning-table source set approved for standards.requirement.",
      recommendedNextWriter: standardsRows.length
        ? "v25C-4 evidence/source capture writer"
        : "v25C-1 suppression writer",
    })
  );

  const demandClassification =
    demandRows.length > 1
      ? CLASSIFICATIONS.FOUNDER_REQUIRED
      : demandRows.length === 1
      ? CLASSIFICATIONS.SOURCE_REQUIRED
      : CLASSIFICATIONS.SUPPRESS;
  sections.push(
    sectionResult({
      section: "Demand Scenario View",
      classification: demandClassification,
      currentRenderedIssue: `Only ${demandRows.length} demand row(s) found (Curio reference: ${curioDemandCount}).`,
      slotKeys: ["commercial.demand"],
      rowsFound: demandRows.length,
      rowsMissing: demandRows.length >= 2 ? 0 : 2 - demandRows.length,
      sourceFactSupportAvailable: demandRows.length > 1 ? "partial" : "insufficient",
      shouldRenderToday: demandRows.length > 1,
      shouldSuppressToday: demandRows.length <= 1,
      proposedRowPayloadsIfSafe: [],
      exactBlockerIfNotSafe:
        "Additional source-backed or founder-reviewed demand scenarios are not yet approved.",
      recommendedNextWriter:
        demandRows.length > 1 ? "v25C-2 safe row creation writer" : "v25C-4 evidence/source capture writer",
    })
  );

  const pendingLoyaltyFacts =
    evidencePkg?.factsByReviewBucket?.safeForFounderReview?.filter((x) => x.startsWith("be.loyalty.")).length || 0;
  sections.push(
    sectionResult({
      section: "Loyalty Program",
      classification: pendingLoyaltyFacts ? CLASSIFICATIONS.FOUNDER_REQUIRED : CLASSIFICATIONS.SOURCE_REQUIRED,
      currentRenderedIssue:
        loyaltyRows.length > 0
          ? "Partial loyalty rows exist; unsupported KPI counts and fallback mechanics remain."
          : "Loyalty mechanics rows absent; UI fallback risk remains.",
      slotKeys: [
        "loyalty.earn",
        "loyalty.redeem",
        "loyalty.elite",
        "loyalty.proof",
        "loyalty.kpi.members",
        "loyalty.kpi.hotels",
        "loyalty.kpi.markets",
        "loyalty.kpi.mix",
      ],
      rowsFound: loyaltyRows.length,
      rowsMissing: Math.max(0, 4 - loyaltyRows.length),
      sourceFactSupportAvailable: pendingLoyaltyFacts ? "pending_founder_approval" : "insufficient",
      shouldRenderToday: false,
      shouldSuppressToday: true,
      proposedRowPayloadsIfSafe: [],
      exactBlockerIfNotSafe:
        "v23 loyalty facts are pending or incomplete; KPI numeric claims remain unsupported.",
      recommendedNextWriter: "v25C-4 evidence/source capture writer",
    })
  );

  sections.push(
    sectionResult({
      section: "Geographic Footprint",
      classification: geoRows.length >= 5 ? CLASSIFICATIONS.FOUNDER_REQUIRED : CLASSIFICATIONS.SOURCE_REQUIRED,
      currentRenderedIssue: geoRows.length >= 5
        ? "Region rows exist but copy appears template-thin and may include unsupported implication framing."
        : "Region coverage incomplete.",
      slotKeys: ["footprint.region.am", "footprint.region.cala", "footprint.region.eu", "footprint.region.mea", "footprint.region.apac"],
      rowsFound: geoRows.length,
      rowsMissing: Math.max(0, 5 - geoRows.length),
      sourceFactSupportAvailable: "partial",
      shouldRenderToday: geoRows.length >= 5,
      shouldSuppressToday: false,
      proposedRowPayloadsIfSafe: [],
      exactBlockerIfNotSafe:
        "Need source-backed regional improvements; do not add unsupported counts or reorder sort.",
      recommendedNextWriter: "v25C-4 evidence/source capture writer",
    })
  );

  const suppressNow = sections.filter((s) => s.shouldSuppressToday).map((s) => s.section);
  const populateNow = sections
    .filter((s) => [CLASSIFICATIONS.READY, CLASSIFICATIONS.ROW_CREATE, CLASSIFICATIONS.FIXED].includes(s.classification) && !s.shouldSuppressToday)
    .map((s) => s.section);
  const evidenceNeeded = sections.filter((s) => s.classification === CLASSIFICATIONS.SOURCE_REQUIRED).map((s) => s.section);
  const frontendNeeded = sections.filter((s) => s.classification === CLASSIFICATIONS.FRONTEND_REQUIRED).map((s) => s.section);
  const founderNeeded = sections.filter((s) => s.classification === CLASSIFICATIONS.FOUNDER_REQUIRED).map((s) => s.section);

  const report = {
    remediationVersion: REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    brand: { recordId: TRIBUTE_BRAND_ID, name: nz(tribute.name) || "Tribute Portfolio" },
    filesRead: [
      "AGENTS.md",
      "reports/brand-explorer-visual-display-defect-audit.md",
      "reports/brand-explorer-visual-display-defect-audit.json",
      "reports/brand-explorer-screenshot-seeded-remediation-review-package.md",
      "reports/brand-explorer-screenshot-seeded-remediation-review-package.json",
      "reports/brand-explorer-visual-minimums-backfill-planner.md",
      "reports/brand-explorer-visual-minimums-backfill-planner.json",
      "reports/brand-explorer-visual-minimums-backfill-writer.md",
      "reports/brand-explorer-visual-minimums-backfill-writer.json",
      "reports/brand-explorer-evidence-fact-review-package.md",
      "reports/brand-explorer-evidence-fact-review-package.json",
      "reports/tribute-portfolio-targeted-extract.md",
      "reports/tribute-portfolio-targeted-extract.json",
      "reports/brand-explorer-presentation-sort-order-audit.md",
      "docs/brand-explorer-presentation-slots.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "live Tribute Brand Explorer Presentation rows (API)",
      "live Curio Brand Explorer Presentation rows (API)",
      "live Tribute Source Library records (via prior report snapshots)",
      "live Tribute Partner Facts (via prior report snapshots)",
      "live Tribute Brand Asset Registry records (via prior report snapshots)",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-missing-content-remediation.js",
      "scripts/brand-explorer-missing-content-remediation.mjs",
      `docs/data-intelligence/${DOC_MD_NAME}`,
      `reports/${REPORT_MD_NAME}`,
      `reports/${REPORT_JSON_NAME}`,
      "package.json",
    ],
    v25CMissingContentRemediationExists: true,
    sections,
    statusBySection: Object.fromEntries(sections.map((s) => [s.section, s.classification])),
    sectionsCanBePopulatedNow: populateNow,
    sectionsMustBeSuppressedNow: suppressNow,
    sectionsNeedSourceCapture: evidenceNeeded,
    sectionsNeedFrontendMapping: frontendNeeded,
    sectionsNeedFounderReview: founderNeeded,
    exactProposedRowPayloadsIfSafe: [],
    exactSuppressionRulesRecommended: [
      "Hide Openings / Examples / Properties if no complete footprint.openings row exists (title + image + location + summary + URL).",
      "Hide Recent Momentum when there are zero dated source-backed footprint.momentum rows.",
      "Hide Portfolio Mix pills when <=1 unsupported row or when no source-backed mix evidence exists.",
      "Hide Standard Detail placeholder when standards.requirement table rows are absent or not externally safe.",
      "Hide Demand Scenario View when only one or zero commercial.demand rows exist.",
      "Hide Loyalty mechanics/KPI blocks until v23 facts are approved and KPI numbers are source-backed.",
    ],
    recommendedWriterSequence: {
      v25C_1_suppression_writer:
        "Implement conditional hide rules for Openings, Momentum, Mix, Standards placeholder, Demand Scenario, Loyalty fallback.",
      v25C_2_safe_row_creation_writer:
        "Create/update only rows that are complete and source-backed (Openings, Momentum, optional Demand expansion).",
      v25C_3_frontend_mapping_writer:
        "Add Marriott-specific portfolio ladder mapping for overview.portfolio_context rendering.",
      v25C_4_evidence_source_capture_writer:
        "Capture/approve missing facts for loyalty mechanics + KPI, standards owner table, portfolio mix, demand, and footprint refinements.",
    },
    comparisonContext: {
      curioOpeningsRowCount: curioOpeningsCount,
      curioDemandRowCount: curioDemandCount,
      tributeOpeningsRowCount: openingsRows.length,
      tributeDemandRowCount: demandRows.length,
      pendingLoyaltyFactsFromV23: pendingLoyaltyFacts,
      targetedExtractUnsupportedCount: targetedExtract?.v23EvidenceReadiness?.unsupportedOrNotExtractable ?? null,
      screenshotPackageCriticalDefects:
        screenshotPkg?.criticalVisibleDefects?.map((d) => `${d.section}:${d.defectType}`) || [],
    },
    exactNextCommand: "npm run brand-explorer-missing-content-remediation -- --brand tribute-portfolio --dry-run",
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildBrandExplorerMissingContentRemediationMarkdown(report) {
  return buildMarkdown(report);
}
