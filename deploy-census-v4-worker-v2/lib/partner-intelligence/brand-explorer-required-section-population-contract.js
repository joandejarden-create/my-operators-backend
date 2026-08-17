import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import { evaluateGeographicFootprintReadiness } from "./brand-explorer-tribute-geographic-footprint-refinement-writer.js";
import {
  evaluatePortfolioContextReadiness,
  evaluateStandardsDetailReadinessGeneralized,
  evaluateDemandScenarioReadiness,
  evaluateDemandScenarioRowComplete,
  legacyDemandScenarioRowComplete,
  blocksForSlot,
} from "./brand-explorer-required-section-contract-evaluators.js";

export const CONTRACT_VERSION = "27B";
export const REPORT_JSON_NAME = "brand-explorer-required-section-population-contract.json";
export const REPORT_MD_NAME = "brand-explorer-required-section-population-contract.md";
export const DOC_MD_NAME = "brand-explorer-required-section-population-contract-v25C-1.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const TRIBUTE_BRAND_ID = "recCvV0PuZOi8c3hC";
const REFERENCE_BRANDS = [
  { name: "Curio Collection by Hilton", id: "receQkxgjlezsc1xg" },
  { name: "Kimpton Hotels", id: "recCKuXCmGvxHPfb3" },
  { name: "Radisson Blu by Choice", id: "recWPEvxBQxVVzSq3" },
  { name: "Ascend Hotel Collection", id: "reclkgOzvAcBheUSo" },
];

const CLASSIFICATIONS = Object.freeze({
  REQUIRED_BACKFILL: "required_backfill",
  REQUIRED_REFINEMENT: "required_refinement",
  FRONTEND_MAPPING_REQUIRED: "frontend_mapping_required",
  FOUNDER_REVIEW_REQUIRED: "founder_review_required",
  FACT_APPROVAL_REQUIRED: "fact_approval_required",
  READY: "ready",
});

const REQUIRED_SECTION_MINIMUMS = Object.freeze({
  openings: {
    section: "Openings / Examples / Properties",
    minimum: 3,
    rowRules: ["title/property name", "image/imageUrl", "location or descriptor", "body/summary", "source URL or property URL"],
  },
  momentum: {
    section: "Recent Momentum",
    minimum: 3,
    rowRules: ["date or year", "title/event", "body/summary", "source URL"],
  },
  mix: {
    section: "Portfolio Mix",
    minimum: 3,
    rowRules: ["asset type, market/use case, or owner relevance", "source-backed or founder-reviewed"],
  },
  context: {
    section: "Portfolio Context",
    minimum: 1,
    rowRules: [
      "parent-company portfolio ladder context",
      "owner-facing explanation of brand position in parent or soft-brand ecosystem",
    ],
  },
  standards: {
    section: "Standard Detail / Where Available",
    minimum: 1,
    rowRules: ["structured owner-planning table or structured owner considerations", "no internal-only legal fragments directly"],
  },
  demand: {
    section: "Demand Scenario View",
    minimum: 3,
    targetParity: 6,
    rowRules: ["title", "body/summary", "owner/developer implication"],
  },
  loyalty: {
    section: "Loyalty Program",
    minimum: 5,
    rowRules: ["program affiliation summary", "earn mechanics", "redeem mechanics", "elite/member benefit mechanics", "proof/support statement"],
  },
  geo: {
    section: "Geographic Footprint",
    minimum: 5,
    rowRules: ["regional rows", "no unsupported counts", "not generic template filler"],
  },
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}

function brandRecordId(brand) {
  return nz(brand?.recordId || brand?.id);
}

function resolveBrandTarget(brandArg) {
  const normalized = nz(brandArg).toLowerCase();
  const bySlug = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === normalized);
  if (bySlug) return bySlug;
  const byId = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.recordId === brandArg);
  if (byId) return byId;
  const byName = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.name.toLowerCase() === normalized);
  if (byName) return byName;
  return { slug: normalized.replace(/\s+/g, "-"), recordId: nz(brandArg), name: nz(brandArg) };
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

function parseParagraphs(body) {
  return String(body || "")
    .split(/\n\n+/)
    .map((x) => x.trim())
    .filter(Boolean);
}

function firstHttp(paragraphs) {
  return paragraphs.find((p) => /^https?:\/\//i.test(p)) || "";
}

function openingIsComplete(row) {
  const title = nz(row?.title);
  const image = nz(row?.imageUrl);
  const paras = parseParagraphs(row?.body);
  const textParas = paras.filter((p) => !/^https?:\/\//i.test(p));
  const location = textParas[1] || "";
  const summary = textParas[3] || textParas[4] || textParas[0] || "";
  const url = nz(row?.summaryUrl) || firstHttp(paras);
  return [title, image, location, summary, url].every(hasVal);
}

function momentumIsComplete(row) {
  const title = nz(row?.title);
  const paras = parseParagraphs(row?.body);
  const date = paras[0] || "";
  const summary = paras.filter((p) => !/^https?:\/\//i.test(p)).slice(1).join(" ");
  const source = firstHttp(paras);
  return [title, date, summary, source].every(hasVal);
}

function demandIsComplete(row) {
  return evaluateDemandScenarioRowComplete(row).complete;
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

function sectionStatus(payload) {
  return {
    section: payload.section,
    currentStatus: payload.currentStatus,
    classification: payload.classification,
    requiredMinimum: payload.requiredMinimum,
    currentCount: payload.currentCount,
    missingCount: Math.max(0, payload.requiredMinimum - payload.currentCount),
    rowsFound: payload.rowsFound ?? payload.currentCount,
    rowsMissing: payload.rowsMissing ?? Math.max(0, payload.requiredMinimum - payload.currentCount),
    rendersToday: Boolean(payload.rendersToday),
    temporarySuppressBlankUiGuard: Boolean(payload.temporarySuppressBlankUiGuard),
    exactBackfillRequirements: payload.exactBackfillRequirements || [],
    sourceFactSupportAvailable: payload.sourceFactSupportAvailable || "unknown",
    proposedRowPayloadsIfSafe: payload.proposedRowPayloadsIfSafe || [],
    blockerIfNotSafe: payload.blockerIfNotSafe || "",
    recommendedNextAction: payload.recommendedNextAction || "",
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Required Section Population Contract v${CONTRACT_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`) · slug \`${report.brand.slug}\` · parent **${report.brand.parentCompany || "unknown"}**`
  );
  lines.push(`- Required Sections Ready: **${report.brandExplorerRequiredSectionsReady ? "true" : "false"}**`);
  lines.push(`- Readiness Score: **${report.readinessScore}**`);
  lines.push("");
  lines.push("## Required Minimums");
  for (const m of Object.values(report.requiredSectionMinimums)) {
    lines.push(`- **${m.section}**: minimum ${m.minimum}`);
  }
  lines.push("");
  lines.push("## Section Readiness");
  for (const s of report.sectionBySectionReadiness) {
    lines.push(`- **${s.section}**: \`${s.classification}\` · current ${s.currentCount}/${s.requiredMinimum}`);
  }
  lines.push("");
  if (!report.brandExplorerRequiredSectionsReady) {
    lines.push("## Temporary suppression guards");
    for (const g of report.temporarySuppressionGuardsRecommended) lines.push(`- ${g}`);
    if (!report.temporarySuppressionGuardsRecommended.length) lines.push("- none");
    lines.push("");
    lines.push("## Next writer sequence");
    for (const step of report.exactNextWriterSequence) lines.push(`- ${step}`);
    if (!report.exactNextWriterSequence.length) lines.push("- none");
    lines.push("");
  } else {
    lines.push("## Contract status");
    lines.push("- All required sections meet minimums — suppression guards and backfill writer sequence not applicable.");
    lines.push("");
  }
  lines.push("## Exact next command");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}

export async function buildBrandExplorerRequiredSectionPopulationContractReport(options = {}) {
  const brandArg = nz(options.brandIdOrName || "tribute-portfolio");
  const target = resolveBrandTarget(brandArg === "tribute-portfolio" ? TRIBUTE_BRAND_ID : brandArg);
  const brand = await fetchBrandApiShape(target.recordId);
  if (!brand) throw new Error(`Could not load brand: ${brandArg}`);

  const references = [];
  for (const ref of REFERENCE_BRANDS) {
    const b = await fetchBrandApiShape(ref.id);
    if (b) references.push({ ...ref, blockCount: Array.isArray(b?.brandExplorer?.blocks) ? b.brandExplorer.blocks.length : 0 });
  }

  const evidencePkg = readJsonIfExists("reports/brand-explorer-evidence-fact-review-package.json");
  const screenshotPkg = readJsonIfExists("reports/brand-explorer-screenshot-seeded-remediation-review-package.json");

  const openingRows = blocksForSlot(brand, "footprint.openings");
  const openingComplete = openingRows.filter(openingIsComplete);

  const momentumRows = blocksForSlot(brand, "footprint.momentum");
  const momentumComplete = momentumRows.filter(momentumIsComplete);

  const mixRows = blocksForSlot(brand, "footprint.portfolio_mix");

  const contextRows = blocksForSlot(brand, "overview.portfolio_context");
  const portfolioContextEval = evaluatePortfolioContextReadiness(brand, contextRows, mixRows);
  const contextReady = portfolioContextEval.ready;

  const standardsRows = blocksForSlot(brand, "standards.requirement");
  const standardsApproval = evaluateStandardsDetailReadinessGeneralized(brand, standardsRows);
  const standardsReady = standardsApproval.ready;

  const demandRows = blocksForSlot(brand, "commercial.demand");
  const demandEval = evaluateDemandScenarioReadiness(brand, demandRows);
  const demandComplete = demandRows.filter(demandIsComplete);

  const loyaltyRows = [
    ...blocksForSlot(brand, "loyalty.hero_title"),
    ...blocksForSlot(brand, "loyalty.earn"),
    ...blocksForSlot(brand, "loyalty.redeem"),
    ...blocksForSlot(brand, "loyalty.elite"),
    ...blocksForSlot(brand, "loyalty.proof"),
  ];
  const pendingLoyaltyFacts = evidencePkg?.factsByReviewBucket?.safeForFounderReview?.filter((k) => String(k).startsWith("be.loyalty.")).length || 0;

  const geoRows = [
    ...blocksForSlot(brand, "footprint.region.am"),
    ...blocksForSlot(brand, "footprint.region.cala"),
    ...blocksForSlot(brand, "footprint.region.eu"),
    ...blocksForSlot(brand, "footprint.region.mea"),
    ...blocksForSlot(brand, "footprint.region.apac"),
  ];
  const geoApproval = evaluateGeographicFootprintReadiness(brand, geoRows);
  const geoReady = geoApproval.ready;
  const genericGeoCount = geoApproval.genericRegionCount;

  const sectionBySectionReadiness = [];

  sectionBySectionReadiness.push(sectionStatus({
    section: REQUIRED_SECTION_MINIMUMS.openings.section,
    currentStatus: openingComplete.length >= 3 ? "meets_minimum" : "below_minimum",
    classification: openingComplete.length >= 3 ? CLASSIFICATIONS.READY : CLASSIFICATIONS.REQUIRED_BACKFILL,
    requiredMinimum: REQUIRED_SECTION_MINIMUMS.openings.minimum,
    currentCount: openingComplete.length,
    rowsFound: openingRows.length,
    rendersToday: openingRows.length > 0,
    temporarySuppressBlankUiGuard: openingComplete.length < 3,
    exactBackfillRequirements: [
      "Create at least 3 complete footprint.openings rows.",
      "Each row must include title, imageUrl, location/descriptor, summary body, and source/property URL.",
    ],
    sourceFactSupportAvailable: "approved source library records + founder-reviewed AI draft needed",
    blockerIfNotSafe: openingComplete.length >= 3 ? "" : "Insufficient complete openings rows.",
    recommendedNextAction: "row creation writer",
  }));

  sectionBySectionReadiness.push(sectionStatus({
    section: REQUIRED_SECTION_MINIMUMS.momentum.section,
    currentStatus: momentumComplete.length >= 3 ? "meets_minimum" : "below_minimum",
    classification: momentumComplete.length >= 3 ? CLASSIFICATIONS.READY : CLASSIFICATIONS.REQUIRED_BACKFILL,
    requiredMinimum: REQUIRED_SECTION_MINIMUMS.momentum.minimum,
    currentCount: momentumComplete.length,
    rowsFound: momentumRows.length,
    rendersToday: momentumRows.length > 0,
    temporarySuppressBlankUiGuard: momentumComplete.length < 3,
    exactBackfillRequirements: [
      "Create at least 3 footprint.momentum rows with date/year, title, summary, and source URL.",
      "Rows must be dated and source-backed.",
    ],
    sourceFactSupportAvailable: "new source capture required",
    blockerIfNotSafe: momentumComplete.length >= 3 ? "" : "No minimum set of dated/source-backed momentum rows.",
    recommendedNextAction: "source capture writer",
  }));

  sectionBySectionReadiness.push(sectionStatus({
    section: REQUIRED_SECTION_MINIMUMS.mix.section,
    currentStatus: mixRows.length >= 3 ? "meets_minimum" : "below_minimum",
    classification: mixRows.length >= 3 ? CLASSIFICATIONS.READY : CLASSIFICATIONS.REQUIRED_BACKFILL,
    requiredMinimum: REQUIRED_SECTION_MINIMUMS.mix.minimum,
    currentCount: mixRows.length,
    rowsFound: mixRows.length,
    rendersToday: mixRows.length > 0,
    temporarySuppressBlankUiGuard: mixRows.length < 3,
    exactBackfillRequirements: [
      "Create at least 3 mix rows/chips with owner-relevant framing.",
      "Rows must be source-backed or founder-reviewed.",
    ],
    sourceFactSupportAvailable: "founder-reviewed AI draft + approved source library records",
    blockerIfNotSafe: mixRows.length >= 3 ? "" : "Portfolio mix coverage below minimum.",
    recommendedNextAction: "row creation writer",
  }));

  const contextClassification = contextReady ? CLASSIFICATIONS.READY : CLASSIFICATIONS.REQUIRED_BACKFILL;
  sectionBySectionReadiness.push(sectionStatus({
    section: REQUIRED_SECTION_MINIMUMS.context.section,
    currentStatus: contextReady ? "meets_minimum" : "below_minimum",
    classification: contextClassification,
    requiredMinimum: REQUIRED_SECTION_MINIMUMS.context.minimum,
    currentCount: contextReady ? 1 : 0,
    rowsFound: contextRows.length,
    rendersToday: contextRows.length > 0,
    temporarySuppressBlankUiGuard: false,
    exactBackfillRequirements: contextReady
      ? []
      : [
          "Populate parent-company portfolio relationship context (overview.portfolio_context).",
          "Include owner-facing explanation of where the brand sits in its parent or soft-brand ecosystem.",
          portfolioContextEval.blockers.length
            ? `Blockers: ${portfolioContextEval.blockers.join(", ")}`
            : "Add portfolio mix or sibling context where available.",
        ],
    sourceFactSupportAvailable: "approved source library records + founder-reviewed narrative",
    blockerIfNotSafe: contextReady ? "" : portfolioContextEval.blockers.join("; ") || "Portfolio context incomplete.",
    recommendedNextAction: contextReady ? "" : "brand-explorer-portfolio-mix-context-normalization-writer",
  }));

  const standardsClassification = standardsReady
    ? CLASSIFICATIONS.READY
    : standardsRows.length > 0
      ? `${CLASSIFICATIONS.REQUIRED_BACKFILL} + ${CLASSIFICATIONS.FOUNDER_REVIEW_REQUIRED}`
      : `${CLASSIFICATIONS.REQUIRED_BACKFILL} + ${CLASSIFICATIONS.FOUNDER_REVIEW_REQUIRED}`;
  sectionBySectionReadiness.push(sectionStatus({
    section: REQUIRED_SECTION_MINIMUMS.standards.section,
    currentStatus: standardsReady
      ? "meets_minimum"
      : standardsRows.length > 0
        ? "partial_not_approved"
        : "below_minimum",
    classification: standardsClassification,
    requiredMinimum: REQUIRED_SECTION_MINIMUMS.standards.minimum,
    currentCount: standardsRows.length,
    rowsFound: standardsRows.length,
    rendersToday: standardsRows.length > 0,
    temporarySuppressBlankUiGuard: standardsRows.length === 0,
    exactBackfillRequirements: standardsReady
      ? []
      : [
          "Create structured owner-planning standards table rows (minimum 5+ complete columns).",
          "Ensure standards.intro includes owner-planning governance language (no company sign-off).",
          "Apply governance review state via standards.last_reviewed / standards.source_confidence or intro caveats.",
          ...(standardsApproval.blockers || []).map((b) => `Blocker: ${b}`),
        ],
    sourceFactSupportAvailable: standardsReady
      ? "founder-reviewed owner-planning governance package"
      : "pending v23 facts + new source capture required",
    blockerIfNotSafe: standardsReady
      ? ""
      : "No approved external-display-safe standards owner table package.",
    recommendedNextAction: standardsReady
      ? ""
      : brandRecordId(brand) === TRIBUTE_BRAND_ID
        ? "brand-explorer-tribute-standard-detail-review-approval-writer"
        : standardsRows.length >= 5
          ? "brand-explorer-standard-detail-governance-writer"
          : "standards founder-review package",
  }));

  sectionBySectionReadiness.push(sectionStatus({
    section: REQUIRED_SECTION_MINIMUMS.demand.section,
    currentStatus: demandEval.ready ? "meets_minimum" : "below_minimum",
    classification: demandEval.ready ? CLASSIFICATIONS.READY : CLASSIFICATIONS.REQUIRED_BACKFILL,
    requiredMinimum: REQUIRED_SECTION_MINIMUMS.demand.minimum,
    currentCount: demandComplete.length,
    rowsFound: demandRows.length,
    rendersToday: demandRows.length > 0,
    temporarySuppressBlankUiGuard: !demandEval.ready,
    exactBackfillRequirements: demandEval.ready
      ? []
      : [
          "Create at least 3 complete demand scenario rows (target 6 for parity).",
          "Each row needs title/scenario label, asset context, fit signal, and implication/body copy.",
          ...(demandEval.incompleteRows || []).slice(0, 3).map(
            (r) => `Incomplete row ${r.titlePreview || r.recordId}: missing ${r.missingFields.join(", ")}`
          ),
        ],
    sourceFactSupportAvailable: "founder-reviewed AI draft + source capture required",
    blockerIfNotSafe: demandEval.ready ? "" : demandEval.blockers.join("; ") || "Demand scenario rows incomplete vs minimum.",
    recommendedNextAction: demandEval.ready ? "" : "brand-explorer-demand-scenario-writer",
  }));

  const loyaltyCurrentCount = [
    blocksForSlot(brand, "loyalty.hero_title").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.earn").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.redeem").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.elite").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.proof").length ? 1 : 0,
  ].reduce((a, b) => a + b, 0);
  sectionBySectionReadiness.push(sectionStatus({
    section: REQUIRED_SECTION_MINIMUMS.loyalty.section,
    currentStatus: loyaltyCurrentCount >= 5 ? "meets_minimum" : "below_minimum",
    classification:
      loyaltyCurrentCount >= 5
        ? CLASSIFICATIONS.READY
        : `${CLASSIFICATIONS.REQUIRED_BACKFILL} + ${CLASSIFICATIONS.FACT_APPROVAL_REQUIRED}`,
    requiredMinimum: REQUIRED_SECTION_MINIMUMS.loyalty.minimum,
    currentCount: loyaltyCurrentCount,
    rowsFound: loyaltyRows.length,
    rendersToday: loyaltyRows.length > 0,
    temporarySuppressBlankUiGuard: loyaltyCurrentCount < 5,
    exactBackfillRequirements: [
      "Populate affiliation summary + earn + redeem + elite + proof coverage.",
      "Do not populate unsupported KPI counts.",
    ],
    sourceFactSupportAvailable:
      pendingLoyaltyFacts > 0
        ? `pending v23 facts after approval (${pendingLoyaltyFacts})`
        : "approved existing facts + source capture required",
    blockerIfNotSafe: loyaltyCurrentCount >= 5 ? "" : "Required loyalty mechanics/proof coverage incomplete.",
    recommendedNextAction: "fact approval writer",
  }));

  sectionBySectionReadiness.push(sectionStatus({
    section: REQUIRED_SECTION_MINIMUMS.geo.section,
    currentStatus: geoReady ? "meets_minimum" : geoRows.length >= 5 ? "thin_or_generic" : "below_minimum",
    classification: geoReady ? CLASSIFICATIONS.READY : CLASSIFICATIONS.REQUIRED_REFINEMENT,
    requiredMinimum: REQUIRED_SECTION_MINIMUMS.geo.minimum,
    currentCount: geoRows.length,
    rowsFound: geoRows.length,
    rendersToday: geoRows.length > 0,
    temporarySuppressBlankUiGuard: false,
    exactBackfillRequirements: geoReady
      ? []
      : [
          "Refine regional rows to remove template filler language.",
          "Keep claims source-backed; no unsupported count additions.",
          "Apply v25C-5D geographic footprint refinement writer.",
        ],
    sourceFactSupportAvailable: geoReady
      ? "founder-reviewed regional footprint refinement"
      : "approved source library + founder-reviewed refinement",
    blockerIfNotSafe: geoReady ? "" : "Regional copy still template-thin.",
    recommendedNextAction: geoReady
      ? ""
      : "brand-explorer-tribute-geographic-footprint-refinement-writer",
  }));

  const failingSections = sectionBySectionReadiness.filter((s) => !String(s.classification).startsWith(CLASSIFICATIONS.READY));
  const readinessScore = Math.round(
    (sectionBySectionReadiness.filter((s) => String(s.classification).startsWith(CLASSIFICATIONS.READY)).length /
      sectionBySectionReadiness.length) *
      100
  );

  const report = {
    contractVersion: CONTRACT_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    brand: {
      recordId: target.recordId,
      slug: target.slug,
      name: nz(brand?.name) || target.name,
      parentCompany: nz(brand?.parentCompany),
    },
    filesRead: [
      "AGENTS.md",
      "reports/brand-explorer-missing-content-remediation.md",
      "reports/brand-explorer-missing-content-remediation.json",
      "reports/brand-explorer-visual-display-defect-audit.md",
      "reports/brand-explorer-visual-display-defect-audit.json",
      "reports/brand-explorer-screenshot-seeded-remediation-review-package.md",
      "reports/brand-explorer-screenshot-seeded-remediation-review-package.json",
      "reports/brand-explorer-visual-minimums-backfill-planner.md",
      "reports/brand-explorer-visual-minimums-backfill-writer.md",
      "reports/brand-explorer-evidence-fact-review-package.md",
      "reports/brand-explorer-evidence-fact-review-package.json",
      "reports/tribute-portfolio-targeted-extract.md",
      "reports/tribute-portfolio-targeted-extract.json",
      "docs/brand-explorer-presentation-slots.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "live Tribute presentation rows (API)",
      "live Curio/Kimpton/Radisson/Ascend reference rows (API)",
      "Tribute Source Library records (via report snapshots)",
      "Tribute Partner Facts (via report snapshots)",
      "Tribute Brand Asset Registry records (via report snapshots)",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
      "scripts/brand-explorer-required-section-population-contract.mjs",
      `docs/data-intelligence/${DOC_MD_NAME}`,
      `reports/${REPORT_MD_NAME}`,
      `reports/${REPORT_JSON_NAME}`,
      "package.json",
    ],
    v25C1RequiredSectionContractExists: true,
    v27BGeneralizedEvaluatorsActive: true,
    requiredSectionMinimums: REQUIRED_SECTION_MINIMUMS,
    readinessScore,
    brandExplorerRequiredSectionsReady: failingSections.length === 0 && readinessScore === 100,
    tributeReadinessPassFail:
      target.recordId === TRIBUTE_BRAND_ID
        ? failingSections.length === 0 && readinessScore === 100
          ? "pass"
          : "fail"
        : "n/a",
    brandReadinessPassFail: failingSections.length === 0 && readinessScore === 100 ? "pass" : "fail",
    sectionBySectionReadiness,
    requiredSectionsMissing: failingSections.map((s) => s.section),
    openingsBackfillRequirements: sectionBySectionReadiness.find((s) => s.section === REQUIRED_SECTION_MINIMUMS.openings.section),
    recentMomentumBackfillRequirements: sectionBySectionReadiness.find((s) => s.section === REQUIRED_SECTION_MINIMUMS.momentum.section),
    portfolioMixBackfillRequirements: sectionBySectionReadiness.find((s) => s.section === REQUIRED_SECTION_MINIMUMS.mix.section),
    portfolioContextBackfillRequirements: sectionBySectionReadiness.find((s) => s.section === REQUIRED_SECTION_MINIMUMS.context.section),
    standardDetailBackfillRequirements: sectionBySectionReadiness.find((s) => s.section === REQUIRED_SECTION_MINIMUMS.standards.section),
    demandScenarioBackfillRequirements: sectionBySectionReadiness.find((s) => s.section === REQUIRED_SECTION_MINIMUMS.demand.section),
    loyaltyBackfillRequirements: sectionBySectionReadiness.find((s) => s.section === REQUIRED_SECTION_MINIMUMS.loyalty.section),
    geographicFootprintRefinementRequirements: sectionBySectionReadiness.find((s) => s.section === REQUIRED_SECTION_MINIMUMS.geo.section),
    sectionsNeedImmediateRowCreation: sectionBySectionReadiness
      .filter((s) => ["Openings / Examples / Properties", "Portfolio Mix", "Demand Scenario View", "Geographic Footprint"].includes(s.section))
      .map((s) => s.section),
    sectionsNeedSourceCapture: sectionBySectionReadiness
      .filter((s) => /source capture/i.test(s.sourceFactSupportAvailable) || /new source capture/i.test(s.sourceFactSupportAvailable))
      .map((s) => s.section),
    sectionsNeedFounderReview: sectionBySectionReadiness
      .filter((s) => String(s.classification).includes(CLASSIFICATIONS.FOUNDER_REVIEW_REQUIRED))
      .map((s) => s.section),
    sectionsNeedFrontendMapping: sectionBySectionReadiness
      .filter((s) => String(s.classification).includes(CLASSIFICATIONS.FRONTEND_MAPPING_REQUIRED))
      .map((s) => s.section),
    sectionsNeedFactApproval: sectionBySectionReadiness
      .filter((s) => String(s.classification).includes(CLASSIFICATIONS.FACT_APPROVAL_REQUIRED))
      .map((s) => s.section),
    temporarySuppressionGuardsRecommended:
      failingSections.length === 0
        ? []
        : sectionBySectionReadiness
            .filter(
              (s) =>
                s.temporarySuppressBlankUiGuard &&
                !String(s.classification).startsWith(CLASSIFICATIONS.READY)
            )
            .map(
              (s) =>
                `Temporarily suppress blank UI while required backfill is incomplete for ${s.section}.`
            ),
    exactNextWriterSequence:
      failingSections.length === 0
        ? []
        : [
            ...new Set(
              failingSections
                .map((s) => s.recommendedNextAction)
                .filter(Boolean)
                .map((action, index) => `${index + 1}) ${action}`)
            ),
          ],
    referenceRowsInspected: references,
    contextSignals: {
      pendingLoyaltyFacts,
      screenshotCriticalDefects: screenshotPkg?.criticalVisibleDefects?.map((d) => `${d.section}:${d.defectType}`) || [],
      evidenceNeedsSourceCapture: evidencePkg?.factsByReviewBucket?.needsSourceCapture || [],
    },
    exactNextCommand: `npm run brand-explorer-required-section-population-contract -- --brand ${target.slug} --dry-run`,
    evaluatorSummary: {
      portfolioContext: portfolioContextEval,
      standardsDetail: {
        ready: standardsApproval.ready,
        blockers: standardsApproval.blockers || [],
        evaluator: standardsApproval.evaluator || "generalized_v27B",
      },
      demandScenario: demandEval,
    },
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildBrandExplorerRequiredSectionPopulationContractMarkdown(report) {
  return buildMarkdown(report);
}
