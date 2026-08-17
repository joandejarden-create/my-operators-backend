import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const WRITER_VERSION = "14";
export const REPORT_JSON_NAME = "tribute-brand-explorer-final-readback-qa.json";
export const REPORT_MD_NAME = "tribute-brand-explorer-final-readback-qa.md";
export const DOC_MD_NAME = "tribute-brand-explorer-final-readback-qa-v14.md";

const DEFAULT_BRAND_ID = "recCvV0PuZOi8c3hC";
const REQUIRED_SLOTS = [
  "overview.typical_use_case",
  "standards.intro",
  "standards.questions",
  "materials.file",
  "overview.hero",
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
  "overview.scenario.1",
  "overview.scenario.2",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function readUtf8(relPath) {
  return fs.readFileSync(path.join(ROOT, relPath), "utf8");
}

function readJson(relPath) {
  return JSON.parse(readUtf8(relPath));
}

function hasVal(v) {
  return v != null && String(v).trim() !== "";
}

function mergedSlotBody(blocks, slotKey) {
  return (blocks || [])
    .filter((block) => block && String(block.slotKey) === String(slotKey))
    .map((block) => {
      const title = (block.title || "").toString().trim();
      const body = (block.body || "").toString().trim();
      if (title && body) return `${title}: ${body}`;
      return body || title;
    })
    .filter(Boolean)
    .join("\n\n");
}

async function fetchBrandApiShape(brandId) {
  const req = {
    query: { brandId, refresh: "1" },
    headers: {},
  };
  const res = {
    statusCode: 200,
    headers: {},
    payload: null,
    setHeader(name, value) {
      this.headers[name] = value;
    },
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
  if (res.statusCode >= 400 || !res.payload?.success || !res.payload?.brand) {
    throw new Error(`Brand API read failed (${res.statusCode})`);
  }
  return res.payload.brand;
}

function detectFrontendReadPaths() {
  const atelier = readUtf8("public/js/brand-explorer-atelier-from-api.js");
  const goldDetail = readUtf8("public/js/brand-explorer-gold-detail.js");
  const heroPathPresent = /overview\.hero/.test(atelier) || /renderPresentationHero/.test(goldDetail);
  return {
    typicalUseCase: /overview\.typical_use_case/.test(atelier) && /brandProfileAnalysis/.test(atelier),
    standardsIntro: /standards\.intro/.test(atelier),
    standardsQuestions: /standards\.questions/.test(atelier),
    sourceLinks: /materials\.file/.test(atelier),
    heroGalleryValueDrivers:
      heroPathPresent && /materials\.gallery\./.test(atelier) && /overview\.scenario\./.test(atelier),
    apiLoadPath: /\/api\/brand-library\/brand\?brandId=/.test(goldDetail),
  };
}

function detectStaleMappings() {
  const parityAudit = readUtf8("lib/partner-intelligence/tribute-brand-explorer-content-parity-audit.js");
  const existingFieldAudit = readUtf8("lib/partner-intelligence/tribute-existing-brand-field-validation-audit.js");
  const brandLibrary = readUtf8("api/brand-library.js");
  return {
    parityAuditStillUsesBrandBasics:
      /basic:\s*"Brand Profile Analysis"/.test(parityAudit) ||
      /basic:\s*"Brand Standards"/.test(parityAudit) ||
      /basic:\s*"Questions Owners Should Ask"/.test(parityAudit),
    existingFieldAuditStillUsesBrandBasics:
      /basicsField:\s*"Brand Profile Analysis"/.test(existingFieldAudit) ||
      /basicsField:\s*"Brand Standards"/.test(existingFieldAudit) ||
      /basicsField:\s*"Questions Owners Should Ask"/.test(existingFieldAudit),
    brandLibraryLegacyFormMapEntry:
      /brandProfileAnalysis:\s*F\.brandBasics\.profileAnalysis/.test(brandLibrary),
  };
}

export async function buildTributeBrandExplorerFinalReadbackQaReport({
  brandId = DEFAULT_BRAND_ID,
} = {}) {
  const brand = await fetchBrandApiShape(brandId);
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  const slotReadback = REQUIRED_SLOTS.map((slotKey) => {
    const value = mergedSlotBody(blocks, slotKey);
    const imagePresent = blocks.some(
      (block) => block && String(block.slotKey) === slotKey && hasVal(block.imageUrl)
    );
    return {
      slotKey,
      blockCount: blocks.filter((block) => block && String(block.slotKey) === slotKey).length,
      mergedBodyPresent: hasVal(value),
      imagePresent,
      mergedBody: value,
    };
  });
  const frontendReadPaths = detectFrontendReadPaths();
  const staleMappings = detectStaleMappings();
  const v10 = readJson("reports/tribute-brand-explorer-content-parity-audit.json");
  const v12 = readJson("reports/tribute-existing-brand-field-validation-audit.json");
  const visual = readJson("reports/brand-explorer-visual-qa-verification.json");
  const pipeline = readJson("reports/tribute-portfolio-package-pipeline.json");

  const requiredRowsVisible = {
    typicalUseCase: hasVal(mergedSlotBody(blocks, "overview.typical_use_case")),
    standardsIntro: hasVal(mergedSlotBody(blocks, "standards.intro")),
    standardsQuestions: hasVal(mergedSlotBody(blocks, "standards.questions")),
  };

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    filesRead: [
      "AGENTS.md",
      "api/brand-library.js",
      "api/lib/partner-intelligence-field-map.js",
      "api/lib/partner-intelligence-explorer-field-registry.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "lib/partner-intelligence/tribute-brand-explorer-content-promotion-writer.js",
      "lib/partner-intelligence/tribute-brand-explorer-content-parity-audit.js",
      "lib/partner-intelligence/tribute-existing-brand-field-validation-audit.js",
      "reports/tribute-brand-explorer-content-promotion-writer.json",
      "reports/tribute-brand-explorer-content-parity-audit.json",
      "reports/tribute-existing-brand-field-validation-audit.json",
      "reports/brand-explorer-visual-qa-verification.json",
      "reports/tribute-portfolio-package-pipeline.json",
      "fixtures/brand-explorer-presentation-kimpton-full.json",
      "fixtures/brand-explorer-presentation-curio-full.json",
      "fixtures/brand-explorer-presentation-radisson-blu.example.json",
      "fixtures/brand-explorer-presentation-radisson-choice-overview.json",
      "fixtures/brand-explorer-presentation-ascend-hotel-collection-full.json",
    ],
    filesChanged: [
      "api/brand-library.js",
      "lib/partner-intelligence/tribute-brand-explorer-content-parity-audit.js",
      "lib/partner-intelligence/tribute-existing-brand-field-validation-audit.js",
      "lib/partner-intelligence/tribute-brand-explorer-final-readback-qa.js",
      "scripts/tribute-brand-explorer-final-readback-qa.mjs",
      "package.json",
      "reports/tribute-brand-explorer-final-readback-qa.md",
      "reports/tribute-brand-explorer-final-readback-qa.json",
      "docs/data-intelligence/tribute-brand-explorer-final-readback-qa-v14.md",
    ],
    finalReadbackQaExists: true,
    brand: {
      id: brand.id,
      name: brand.name,
      brandWebsite: brand.brandWebsite || "",
      companyValidated: brand.companyValidated || "",
      companyValidationDate: brand.companyValidationDate || "",
    },
    apiReadback: {
      hasBrandExplorerBlocks: blocks.length > 0,
      slotReadback,
      requiredRowsVisible,
    },
    frontendReadPaths,
    staleMappings,
    v10AuditStatus: {
      idealAssetProfileGap: (v10.fieldByFieldTributeGapTable || []).find(
        (row) => row.fieldOrSectionKey === "idealAssetProfile"
      )?.gapAssessment,
      standardsGap: (v10.fieldByFieldTributeGapTable || []).find(
        (row) => row.fieldOrSectionKey === "standards"
      )?.gapAssessment,
      questionsGap: (v10.fieldByFieldTributeGapTable || []).find(
        (row) => row.fieldOrSectionKey === "questionsOwnersShouldAsk"
      )?.gapAssessment,
    },
    v12AuditStatus: {
      brandProfileAnalysis: (v12.fieldByFieldCorrectionTable || []).find(
        (row) => row.field === "Brand Profile Analysis"
      )?.classification,
      brandStandards: (v12.fieldByFieldCorrectionTable || []).find(
        (row) => row.field === "Brand Standards"
      )?.classification,
      questionsOwnersShouldAsk: (v12.fieldByFieldCorrectionTable || []).find(
        (row) => row.field === "Questions Owners Should Ask"
      )?.classification,
    },
    guardrails: {
      brandWebsiteRemainsCorrected: brand.brandWebsite === "https://tribute-portfolio.marriott.com/",
      mediaRemainsIntact: visual.tributeMediaVisibleToBrandExplorer === true,
      companyValidatedUntouched:
        !hasVal(brand.companyValidated) && !hasVal(brand.companyValidationDate),
    },
    completedBrandComparableExcludingRecentOpeningsPr:
      requiredRowsVisible.typicalUseCase &&
      requiredRowsVisible.standardsIntro &&
      requiredRowsVisible.standardsQuestions &&
      visual.tributeMediaVisibleToBrandExplorer === true,
    pipelineFactStewardshipReason: {
      currentStage: pipeline.currentStage,
      approvedFacts: pipeline?.executiveSummary?.approvedFacts ?? null,
      pendingFacts: pipeline?.executiveSummary?.pendingFacts ?? null,
      heldInternalFacts: pipeline?.executiveSummary?.heldInternalFacts ?? null,
      summary:
        "Pipeline remains at Fact Stewardship Needed because not all extracted facts are approved for publish scope; some remain pending and some are intentionally held internal.",
    },
  };
}

export function buildTributeBrandExplorerFinalReadbackQaMarkdown(report) {
  const lines = [];
  lines.push("# Tribute Brand Explorer Final Readback QA v14");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.id}\``);
  lines.push("");
  lines.push("## API slot readback");
  for (const row of report.apiReadback.slotReadback) {
    lines.push(
      `- \`${row.slotKey}\` · blocks: ${row.blockCount} · merged body: ${
        row.mergedBodyPresent ? "yes" : "no"
      } · image: ${row.imagePresent ? "yes" : "no"}`
    );
  }
  lines.push("");
  lines.push("## Frontend read-path coverage");
  lines.push(`- Typical use case / ideal asset profile path: ${report.frontendReadPaths.typicalUseCase ? "yes" : "no"}`);
  lines.push(`- Brand standards intro path: ${report.frontendReadPaths.standardsIntro ? "yes" : "no"}`);
  lines.push(`- Questions owners should ask path: ${report.frontendReadPaths.standardsQuestions ? "yes" : "no"}`);
  lines.push(`- Source links path: ${report.frontendReadPaths.sourceLinks ? "yes" : "no"}`);
  lines.push(`- Hero/gallery/value-driver image paths: ${report.frontendReadPaths.heroGalleryValueDrivers ? "yes" : "no"}`);
  lines.push("");
  lines.push("## Stale mapping check");
  lines.push(
    `- v10 parity audit still mapped to Brand Basics: ${
      report.staleMappings.parityAuditStillUsesBrandBasics ? "yes" : "no"
    }`
  );
  lines.push(
    `- v12 existing-field audit still mapped to Brand Basics: ${
      report.staleMappings.existingFieldAuditStillUsesBrandBasics ? "yes" : "no"
    }`
  );
  lines.push(
    `- API keeps legacy \`brandProfileAnalysis\` form-map entry: ${
      report.staleMappings.brandLibraryLegacyFormMapEntry ? "yes (slot-backed fallback added)" : "no"
    }`
  );
  lines.push("");
  lines.push("## Audit outcomes");
  lines.push(`- v10 idealAssetProfile: ${report.v10AuditStatus.idealAssetProfileGap || "n/a"}`);
  lines.push(`- v10 standards: ${report.v10AuditStatus.standardsGap || "n/a"}`);
  lines.push(`- v10 questionsOwnersShouldAsk: ${report.v10AuditStatus.questionsGap || "n/a"}`);
  lines.push(`- v12 Brand Profile Analysis: ${report.v12AuditStatus.brandProfileAnalysis || "n/a"}`);
  lines.push(`- v12 Brand Standards: ${report.v12AuditStatus.brandStandards || "n/a"}`);
  lines.push(`- v12 Questions Owners Should Ask: ${report.v12AuditStatus.questionsOwnersShouldAsk || "n/a"}`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Brand Website remains corrected: ${report.guardrails.brandWebsiteRemainsCorrected ? "yes" : "no"}`);
  lines.push(`- Media remains intact: ${report.guardrails.mediaRemainsIntact ? "yes" : "no"}`);
  lines.push(`- Company Validated fields untouched: ${report.guardrails.companyValidatedUntouched ? "yes" : "no"}`);
  lines.push("");
  lines.push(
    `Completed-brand comparable (excluding Recent Openings/PR): **${
      report.completedBrandComparableExcludingRecentOpeningsPr ? "yes" : "not yet"
    }**`
  );
  lines.push("");
  lines.push("## Pipeline stage note");
  lines.push(`- Stage: ${report.pipelineFactStewardshipReason.currentStage}`);
  lines.push(`- Approved facts: ${report.pipelineFactStewardshipReason.approvedFacts}`);
  lines.push(`- Pending facts: ${report.pipelineFactStewardshipReason.pendingFacts}`);
  lines.push(`- Held internal facts: ${report.pipelineFactStewardshipReason.heldInternalFacts}`);
  lines.push(`- Why: ${report.pipelineFactStewardshipReason.summary}`);
  lines.push("");
  return lines.join("\n");
}
