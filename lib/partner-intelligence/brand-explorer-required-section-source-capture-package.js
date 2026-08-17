import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const PACKAGE_VERSION = "25C-2A";
export const REPORT_JSON_NAME = "brand-explorer-required-section-source-capture-package.json";
export const REPORT_MD_NAME = "brand-explorer-required-section-source-capture-package.md";
export const DOC_MD_NAME = "brand-explorer-required-section-source-capture-package-v25C-2A.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const TRIBUTE_BRAND_ID = "recCvV0PuZOi8c3hC";
const REF_BRANDS = [
  { name: "Curio Collection by Hilton", id: "receQkxgjlezsc1xg" },
  { name: "Kimpton Hotels", id: "recCKuXCmGvxHPfb3" },
  { name: "Radisson Blu by Choice", id: "recWPEvxBQxVVzSq3" },
  { name: "Ascend Hotel Collection", id: "reclkgOzvAcBheUSo" },
];

const CLASSIFICATION = Object.freeze({
  READY: "ready_for_row_creation_now",
  FACT_APPROVAL: "fact_approval_required",
  SOURCE_CAPTURE: "source_capture_required",
  FOUNDER_REVIEW: "founder_review_required",
  FRONTEND_MAPPING: "frontend_mapping_required",
  STANDARDS_REVIEW: "standards_review_required",
  NOT_SAFE: "not_safe_yet",
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}
function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
}
function parseParas(body) {
  return String(body || "")
    .split(/\n\n+/)
    .map((x) => x.trim())
    .filter(Boolean);
}
function firstHttp(paras) {
  return paras.find((p) => /^https?:\/\//i.test(p)) || "";
}
function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && nz(b.slotKey) === nz(slotKey));
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

function openingComplete(row) {
  const title = nz(row?.title);
  const image = nz(row?.imageUrl);
  const paras = parseParas(row?.body);
  const textParas = paras.filter((p) => !/^https?:\/\//i.test(p));
  const location = textParas[1] || "";
  const summary = textParas[3] || textParas[4] || textParas[0] || "";
  const url = nz(row?.summaryUrl) || firstHttp(paras);
  return [title, image, location, summary, url].every(hasVal);
}
function momentumComplete(row) {
  const title = nz(row?.title);
  const paras = parseParas(row?.body);
  const date = paras[0] || "";
  const summary = paras.filter((p) => !/^https?:\/\//i.test(p)).slice(1).join(" ");
  const url = firstHttp(paras);
  return [title, date, summary, url].every(hasVal);
}
function demandComplete(row) {
  const title = nz(row?.title);
  const body = nz(row?.body);
  const implication = nz(row?.caseSummaryOwnerObjective) || nz(row?.caseSummaryInterpretation);
  return [title, body, implication].every(hasVal);
}

function aiDraft(title, body, sourceBasis) {
  return {
    title,
    body,
    sourceBasis,
    labels: [
      "AI-drafted / pending founder review",
      "Not company-validated",
      "Not Marriott-validated",
    ],
  };
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Required Section Source Capture Package v25C-2A");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`);
  lines.push(`- Package exists: **${report.v25C2ASourceCapturePackageExists ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Sections");
  for (const s of report.sectionPlans) {
    lines.push(`- **${s.section}**: \`${s.classification}\` · ready now: ${s.readyForRowCreationNow ? "yes" : "no"}`);
  }
  lines.push("");
  lines.push("## Pending fact approvals");
  for (const f of report.pendingFactsToApprove) lines.push(`- ${f}`);
  if (!report.pendingFactsToApprove.length) lines.push("- none");
  lines.push("");
  lines.push("## New source capture tasks");
  for (const t of report.newSourceCaptureTasks) lines.push(`- ${t}`);
  lines.push("");
  lines.push("## Next writer sequence");
  for (const step of report.exactNextWriterSequence) lines.push(`- ${step}`);
  lines.push("");
  lines.push("## Exact next command");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}

export async function buildBrandExplorerRequiredSectionSourceCapturePackageReport(options = {}) {
  const brandArg = nz(options.brandIdOrName || "tribute-portfolio");
  const tribute = await fetchBrandApiShape(brandArg === "tribute-portfolio" ? TRIBUTE_BRAND_ID : brandArg);
  if (!tribute) throw new Error(`Could not load brand: ${brandArg}`);

  const contract = readJsonIfExists("reports/brand-explorer-required-section-population-contract.json");
  const evidence = readJsonIfExists("reports/brand-explorer-evidence-fact-review-package.json");
  const targeted = readJsonIfExists("reports/tribute-portfolio-targeted-extract.json");
  const missing = readJsonIfExists("reports/brand-explorer-missing-content-remediation.json");
  const defectAudit = readJsonIfExists("reports/brand-explorer-visual-display-defect-audit.json");

  const refPatterns = [];
  for (const r of REF_BRANDS) {
    const b = await fetchBrandApiShape(r.id);
    if (b) {
      refPatterns.push({
        name: r.name,
        blockCount: Array.isArray(b?.brandExplorer?.blocks) ? b.brandExplorer.blocks.length : 0,
        openings: blocksForSlot(b, "footprint.openings").length,
        demand: blocksForSlot(b, "commercial.demand").length,
      });
    }
  }

  const galleryAssets = [1, 2, 3, 4, 5, 6]
    .map((i) => blocksForSlot(tribute, `materials.gallery.${i}`)[0])
    .filter(Boolean)
    .filter((b) => hasVal(b.imageUrl))
    .map((b) => ({ slotKey: b.slotKey, title: nz(b.title), imageUrl: nz(b.imageUrl) }));

  const openingsRows = blocksForSlot(tribute, "footprint.openings");
  const openingsComplete = openingsRows.filter(openingComplete).length;
  const momentumRows = blocksForSlot(tribute, "footprint.momentum");
  const momentumCompleteCount = momentumRows.filter(momentumComplete).length;
  const mixRows = blocksForSlot(tribute, "footprint.portfolio_mix");
  const contextRows = blocksForSlot(tribute, "overview.portfolio_context");
  const standardsRows = blocksForSlot(tribute, "standards.requirement");
  const demandRows = blocksForSlot(tribute, "commercial.demand");
  const demandCompleteCount = demandRows.filter(demandComplete).length;
  const loyaltyCoverageCount = [
    blocksForSlot(tribute, "loyalty.hero_title").length ? 1 : 0,
    blocksForSlot(tribute, "loyalty.earn").length ? 1 : 0,
    blocksForSlot(tribute, "loyalty.redeem").length ? 1 : 0,
    blocksForSlot(tribute, "loyalty.elite").length ? 1 : 0,
    blocksForSlot(tribute, "loyalty.proof").length ? 1 : 0,
  ].reduce((a, b) => a + b, 0);
  const footprintRows = [
    ...blocksForSlot(tribute, "footprint.region.am"),
    ...blocksForSlot(tribute, "footprint.region.cala"),
    ...blocksForSlot(tribute, "footprint.region.eu"),
    ...blocksForSlot(tribute, "footprint.region.mea"),
    ...blocksForSlot(tribute, "footprint.region.apac"),
  ];

  const pendingFactsToApprove = [
    "be.loyalty.earnMechanics",
    "be.loyalty.redeemMechanics",
    "be.loyalty.eliteTierLadder",
    "be.loyalty.memberRatesBenefit",
    "be.loyalty.programScaleStatement",
  ];

  const sectionPlans = [
    {
      section: "Openings / Examples / Properties",
      classification: openingsComplete >= 3 ? CLASSIFICATION.READY : `${CLASSIFICATION.SOURCE_CAPTURE} + ${CLASSIFICATION.FOUNDER_REVIEW}`,
      readyForRowCreationNow: openingsComplete >= 3,
      existingApprovedSources: targeted?.sourceInventory?.filter((s) => s.approvedForExplorerUse && /brand|development|consumer/i.test(s.role)).map((s) => s.title) || [],
      existingPendingFacts: [],
      existingAssetRegistryRecords: galleryAssets.map((a) => a.slotKey),
      missingSourceEvidence: [
        "Need 3 property-specific source URLs (official Marriott property pages, newsroom, or company-controlled opening pages).",
      ],
      exactSourceCaptureTasksRequired: [
        "Capture at least 3 Tribute property pages with opening/repositioning context and source URL.",
        "Map each property to one verified image asset and one owner-relevant summary line.",
      ],
      rowCreationCanHappenNow: false,
      candidateRowPayloadsSafeForFounderReview: [
        aiDraft(
          "Candidate property card #1 (title TBD from captured property page)",
          "Location/descriptor + opening summary + source URL to be inserted from captured Marriott-controlled source.",
          "materials.gallery.* assets + captured property page URL"
        ),
      ],
    },
    {
      section: "Recent Momentum",
      classification: momentumCompleteCount >= 3 ? CLASSIFICATION.READY : CLASSIFICATION.SOURCE_CAPTURE,
      readyForRowCreationNow: false,
      existingApprovedSources: targeted?.sourceInventory?.filter((s) => s.approvedForExplorerUse).map((s) => s.title) || [],
      existingPendingFacts: [],
      existingAssetRegistryRecords: [],
      missingSourceEvidence: ["No dated/source-backed momentum rows currently captured."],
      exactSourceCaptureTasksRequired: [
        "Capture 3 dated activities from Marriott newsroom/development/property opening pages.",
        "Each capture must include date, event title, summary, and source URL.",
      ],
      rowCreationCanHappenNow: false,
      candidateRowPayloadsSafeForFounderReview: [],
    },
    {
      section: "Portfolio Mix",
      classification: mixRows.length >= 3 ? CLASSIFICATION.READY : `${CLASSIFICATION.FOUNDER_REVIEW} + ${CLASSIFICATION.SOURCE_CAPTURE}`,
      readyForRowCreationNow: false,
      existingApprovedSources: (targeted?.sourceInventory || []).map((s) => s.title),
      existingPendingFacts: ["be.positioning.independentCollectionStatement"],
      existingAssetRegistryRecords: [],
      missingSourceEvidence: ["Need 2 additional source-backed mix categories/chips."],
      exactSourceCaptureTasksRequired: [
        "Derive 2 additional mix chips from approved positioning/development sources (no portfolio statistics).",
      ],
      rowCreationCanHappenNow: false,
      candidateRowPayloadsSafeForFounderReview: [
        aiDraft("Urban Boutique Repositioning", "Owner relevance: design-led urban conversion use case.", "approved positioning + development source set"),
        aiDraft("Resort / Leisure Independent", "Owner relevance: independent leisure-led assets with Bonvoy distribution leverage.", "approved positioning + development source set"),
      ],
    },
    {
      section: "Portfolio Context",
      classification: `${CLASSIFICATION.FRONTEND_MAPPING} + ${CLASSIFICATION.FOUNDER_REVIEW}`,
      readyForRowCreationNow: false,
      existingApprovedSources: ["Tribute consumer site", "Marriott development pages"],
      existingPendingFacts: ["be.positioning.independentCollectionStatement"],
      existingAssetRegistryRecords: [],
      missingSourceEvidence: ["Marriott-specific sibling ladder mapping strategy remains unresolved."],
      exactSourceCaptureTasksRequired: [
        "Document Marriott-specific sibling/collection ladder source basis and map to frontend static ladder strategy.",
        "Produce founder-reviewed context copy backing `overview.portfolio_context` row/body.",
      ],
      rowCreationCanHappenNow: false,
      candidateRowPayloadsSafeForFounderReview: [
        aiDraft(
          "3",
          "Upper-upscale Marriott soft-collection context preserving independent identity while tied to Bonvoy/commercial systems.",
          "approved consumer + development positioning sources"
        ),
      ],
    },
    {
      section: "Standard Detail / Where Available",
      classification: `${CLASSIFICATION.STANDARDS_REVIEW} + ${CLASSIFICATION.SOURCE_CAPTURE} + ${CLASSIFICATION.NOT_SAFE}`,
      readyForRowCreationNow: false,
      existingApprovedSources: ["2026 Tribute FDD (internal/legal source)"],
      existingPendingFacts: ["be.standards.qualityAssuranceTheme", "be.meta.fddDocumentVintage"],
      existingAssetRegistryRecords: [],
      missingSourceEvidence: ["No external-display-safe owner-planning structured standards evidence set."],
      exactSourceCaptureTasksRequired: [
        "Create standards founder-review package that converts internal standards into external-display-safe owner-planning table language.",
        "Capture external-display-safe standards references before row creation.",
      ],
      rowCreationCanHappenNow: false,
      candidateRowPayloadsSafeForFounderReview: [],
    },
    {
      section: "Demand Scenario View",
      classification: demandCompleteCount >= 3 ? CLASSIFICATION.READY : `${CLASSIFICATION.SOURCE_CAPTURE} + ${CLASSIFICATION.FOUNDER_REVIEW}`,
      readyForRowCreationNow: false,
      existingApprovedSources: ["Tribute consumer site", "Marriott development pages"],
      existingPendingFacts: ["be.overview.typicalUseCase", "be.overview.whyValue"],
      existingAssetRegistryRecords: [],
      missingSourceEvidence: ["Need at least 2 more complete source-backed demand scenarios (target 5 more for parity 6)."],
      exactSourceCaptureTasksRequired: [
        "Capture source-backed demand use-cases from approved positioning/development sources.",
        "Draft 3 minimum scenarios (target 6) with owner implications for founder review.",
      ],
      rowCreationCanHappenNow: false,
      candidateRowPayloadsSafeForFounderReview: [
        aiDraft("Resort & leisure conversion", "Moderate-strong; owner implication: underwriting depends on ADR and operating complexity fit.", "existing commercial.demand row + approved positioning"),
        aiDraft("Urban boutique repositioning", "Moderate-strong; owner implication: strongest where design narrative supports premium conversion.", "approved positioning/development sources"),
      ],
    },
    {
      section: "Loyalty Program",
      classification: `${CLASSIFICATION.FACT_APPROVAL} + ${CLASSIFICATION.FOUNDER_REVIEW}`,
      readyForRowCreationNow: false,
      existingApprovedSources: ["Marriott Bonvoy loyalty page"],
      existingPendingFacts: pendingFactsToApprove,
      existingAssetRegistryRecords: [],
      missingSourceEvidence: ["KPI numeric facts still unsupported; cannot populate KPI counts."],
      exactSourceCaptureTasksRequired: [
        "Approve 5 pending loyalty facts for earn/redeem/elite/member-rate/proof coverage.",
        "Capture additional source for KPI counts if KPI cards are required later.",
      ],
      rowCreationCanHappenNow: false,
      candidateRowPayloadsSafeForFounderReview: [],
    },
    {
      section: "Geographic Footprint",
      classification: CLASSIFICATION.FOUNDER_REVIEW,
      readyForRowCreationNow: false,
      existingApprovedSources: ["Brand footprint setup rows", "approved consumer/development sources"],
      existingPendingFacts: [],
      existingAssetRegistryRecords: [],
      missingSourceEvidence: ["Regional rows are template-thin and need source-backed refinements."],
      exactSourceCaptureTasksRequired: [
        "Capture region-specific source snippets for AMER/CALA/EU/MEA/APAC wording upgrades.",
        "Avoid count claims without explicit verified source facts.",
      ],
      rowCreationCanHappenNow: false,
      candidateRowPayloadsSafeForFounderReview: [
        aiDraft(
          "CALA",
          "Regional relevance framing with corridor-specific positioning text sourced from approved brand/development references.",
          "approved source inventory only"
        ),
      ],
    },
  ];

  const reusableSources = [
    ...(evidence?.sourceRecordsReferenced || []).map((s) => `${s.id}: ${s.title}`),
    ...((targeted?.sourceInventory || []).filter((s) => s.approvedForExplorerUse).map((s) => `${s.id}: ${s.title}`)),
  ].filter((v, i, a) => a.indexOf(v) === i);

  const newSourceCaptureTasks = [
    "Openings: capture 3 property-specific Marriott-controlled pages with URL/date/location evidence.",
    "Momentum: capture 3 dated announcements from official Marriott newsroom/development/property pages.",
    "Portfolio Mix: capture 2 additional source-backed category framings (no statistics).",
    "Standards: produce external-display-safe owner-planning standards source set; legal/founder review required.",
    "Demand: capture at least 2 more source-backed scenarios (target 5 more for parity 6).",
    "Footprint: capture region-specific source-backed refinements for all template-thin region rows.",
    "Loyalty KPI: capture verified numeric facts if KPI counts are required (currently blocked).",
  ];

  const report = {
    packageVersion: PACKAGE_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    brand: { recordId: TRIBUTE_BRAND_ID, name: nz(tribute?.name) || "Tribute Portfolio" },
    filesRead: [
      "AGENTS.md",
      "reports/brand-explorer-required-section-population-contract.md",
      "reports/brand-explorer-required-section-population-contract.json",
      "reports/brand-explorer-missing-content-remediation.md",
      "reports/brand-explorer-missing-content-remediation.json",
      "reports/brand-explorer-evidence-fact-review-package.md",
      "reports/brand-explorer-evidence-fact-review-package.json",
      "reports/tribute-portfolio-targeted-extract.md",
      "reports/tribute-portfolio-targeted-extract.json",
      "reports/brand-explorer-visual-display-defect-audit.md",
      "reports/brand-explorer-visual-display-defect-audit.json",
      "docs/brand-explorer-presentation-slots.md",
      "api/brand-library.js",
      "live Tribute Source Library records (via API + snapshots)",
      "live Tribute Partner Facts (via API + snapshots)",
      "live Tribute Brand Asset Registry records (via API + snapshots)",
      "live Tribute Brand Explorer Presentation rows (API)",
      "live Curio/Kimpton/Radisson/Ascend rows for reference patterns (API)",
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-required-section-source-capture-package.js",
      "scripts/brand-explorer-required-section-source-capture-package.mjs",
      `docs/data-intelligence/${DOC_MD_NAME}`,
      `reports/${REPORT_MD_NAME}`,
      `reports/${REPORT_JSON_NAME}`,
      "package.json",
    ],
    v25C2ASourceCapturePackageExists: true,
    contractSourceOfTruth: {
      brandExplorerRequiredSectionsReady: contract?.brandExplorerRequiredSectionsReady ?? false,
      readinessScore: contract?.readinessScore ?? 0,
    },
    existingReusableSources: reusableSources,
    existingReusableAssets: galleryAssets,
    pendingFactsToApprove,
    newSourceCaptureTasks,
    sectionPlans,
    candidateRowPayloadsSafeForFounderReview: sectionPlans.flatMap((s) => s.candidateRowPayloadsSafeForFounderReview || []),
    sectionsReadyForImmediateRowCreation: sectionPlans.filter((s) => s.readyForRowCreationNow).map((s) => s.section),
    sectionsBlockedPendingSourceCaptureFactApprovalFounderReview: sectionPlans.filter((s) => !s.readyForRowCreationNow).map((s) => s.section),
    referencePatternsUsed: refPatterns,
    evidenceSignals: {
      targetedUnsupportedCount: targeted?.v23EvidenceReadiness?.unsupportedOrNotExtractable ?? null,
      screenshotCriticalDefects: missing?.comparisonContext?.screenshotPackageCriticalDefects || [],
      defectAuditRecommendedBatch: defectAudit?.recommendedNextBatch || "",
    },
    exactNextWriterSequence: [
      "v25C-2B fact approval writer",
      "v25C-2C founder-review row package",
      "v25C-2D row creation writer",
      "v25C-3 frontend mapping writer",
    ],
    exactNextCommand: "npm run brand-explorer-required-section-source-capture-package -- --brand tribute-portfolio --dry-run",
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildBrandExplorerRequiredSectionSourceCapturePackageMarkdown(report) {
  return buildMarkdown(report);
}
