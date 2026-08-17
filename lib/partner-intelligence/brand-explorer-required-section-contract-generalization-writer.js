/**
 * Brand Explorer Required-Section Contract Generalization Writer v27B (read-only).
 * Compares legacy v25C-1 gates vs v27B generalized evaluators across active brands.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import { evaluateGeographicFootprintReadiness } from "./brand-explorer-tribute-geographic-footprint-refinement-writer.js";
import { evaluateStandardsDetailApprovalState } from "./brand-explorer-tribute-standard-detail-review-approval-writer.js";
import {
  buildBrandExplorerRequiredSectionPopulationContractReport,
} from "./brand-explorer-required-section-population-contract.js";
import {
  blocksForSlot,
  evaluatePortfolioContextReadiness,
  evaluateStandardsDetailReadinessGeneralized,
  evaluateDemandScenarioReadiness,
  legacyPortfolioContextReady,
  legacyDemandScenarioRowComplete,
  evaluateDemandScenarioRowComplete,
} from "./brand-explorer-required-section-contract-evaluators.js";
import { fetchLiveState } from "./tribute-portfolio-package-pipeline.js";

export const WRITER_VERSION = "27B";
export const REPORT_JSON_NAME = "brand-explorer-required-section-contract-generalization-writer.json";
export const REPORT_MD_NAME = "brand-explorer-required-section-contract-generalization-writer.md";
export const DOC_MD_NAME = "brand-explorer-required-section-contract-generalization-v27B.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const TRIBUTE_RECORD_ID = "recCvV0PuZOi8c3hC";

const SECTION_KEYS = [
  { key: "openings", section: "Openings / Examples / Properties" },
  { key: "momentum", section: "Recent Momentum" },
  { key: "mix", section: "Portfolio Mix" },
  { key: "context", section: "Portfolio Context" },
  { key: "standards", section: "Standard Detail / Where Available" },
  { key: "demand", section: "Demand Scenario View" },
  { key: "loyalty", section: "Loyalty Program" },
  { key: "geo", section: "Geographic Footprint" },
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-factory-gap-matrix-audit.md",
  "reports/brand-explorer-factory-gap-matrix-audit.json",
  "reports/brand-explorer-complete-build-batch.md",
  "reports/brand-explorer-complete-build-batch.json",
  "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
  "lib/partner-intelligence/brand-explorer-required-section-contract-evaluators.js",
  "docs/brand-explorer-presentation-slots.md",
  "live Brand Explorer Presentation rows (API)",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-required-section-contract-evaluators.js",
  "lib/partner-intelligence/brand-explorer-required-section-contract-generalization-writer.js",
  "lib/partner-intelligence/brand-explorer-required-section-population-contract.js",
  "scripts/brand-explorer-required-section-contract-generalization-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return nz(v) !== "";
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

function resolveTargets(options = {}) {
  if (options.allActive) return [...ACTIVE_BRAND_AUDIT_TARGETS];
  const slug = nz(options.brandIdOrName || "tribute-portfolio");
  const found = ACTIVE_BRAND_AUDIT_TARGETS.find((b) => b.slug === slug || b.recordId === slug);
  return found ? [found] : [{ slug, recordId: slug, name: slug }];
}

async function fetchBrandApiShape(brandId) {
  const req = { query: { brandId, refresh: "1" }, headers: {} };
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

function loyaltySlotCount(brand) {
  return [
    blocksForSlot(brand, "loyalty.hero_title").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.earn").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.redeem").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.elite").length ? 1 : 0,
    blocksForSlot(brand, "loyalty.proof").length ? 1 : 0,
  ].reduce((a, b) => a + b, 0);
}

function computeLegacySectionReadiness(brand) {
  const openingRows = blocksForSlot(brand, "footprint.openings");
  const momentumRows = blocksForSlot(brand, "footprint.momentum");
  const mixRows = blocksForSlot(brand, "footprint.portfolio_mix");
  const contextRows = blocksForSlot(brand, "overview.portfolio_context");
  const contextBody = nz(contextRows[0]?.body);
  const standardsRows = blocksForSlot(brand, "standards.requirement");
  const demandRows = blocksForSlot(brand, "commercial.demand");
  const geoRows = [
    ...blocksForSlot(brand, "footprint.region.am"),
    ...blocksForSlot(brand, "footprint.region.cala"),
    ...blocksForSlot(brand, "footprint.region.eu"),
    ...blocksForSlot(brand, "footprint.region.mea"),
    ...blocksForSlot(brand, "footprint.region.apac"),
  ];

  const legacy = {
    openings: openingRows.filter(openingIsComplete).length >= 3,
    momentum: momentumRows.filter(momentumIsComplete).length >= 3,
    mix: mixRows.length >= 3,
    context: legacyPortfolioContextReady(contextBody) && contextRows.length > 0,
    standards: evaluateStandardsDetailApprovalState(brand, standardsRows).ready,
    demand: demandRows.filter(legacyDemandScenarioRowComplete).length >= 3,
    loyalty: loyaltySlotCount(brand) >= 5,
    geo: evaluateGeographicFootprintReadiness(brand, geoRows).ready,
  };

  const readyCount = Object.values(legacy).filter(Boolean).length;
  return {
    sections: legacy,
    readinessScore: Math.round((readyCount / 8) * 100),
    brandExplorerRequiredSectionsReady: readyCount === 8,
    readyCount,
  };
}

function computeGeneralizedSectionReadiness(brand) {
  const openingRows = blocksForSlot(brand, "footprint.openings");
  const momentumRows = blocksForSlot(brand, "footprint.momentum");
  const mixRows = blocksForSlot(brand, "footprint.portfolio_mix");
  const contextRows = blocksForSlot(brand, "overview.portfolio_context");
  const standardsRows = blocksForSlot(brand, "standards.requirement");
  const demandRows = blocksForSlot(brand, "commercial.demand");
  const geoRows = [
    ...blocksForSlot(brand, "footprint.region.am"),
    ...blocksForSlot(brand, "footprint.region.cala"),
    ...blocksForSlot(brand, "footprint.region.eu"),
    ...blocksForSlot(brand, "footprint.region.mea"),
    ...blocksForSlot(brand, "footprint.region.apac"),
  ];

  const portfolio = evaluatePortfolioContextReadiness(brand, contextRows, mixRows);
  const standards = evaluateStandardsDetailReadinessGeneralized(brand, standardsRows);
  const demand = evaluateDemandScenarioReadiness(brand, demandRows);

  const generalized = {
    openings: openingRows.filter(openingIsComplete).length >= 3,
    momentum: momentumRows.filter(momentumIsComplete).length >= 3,
    mix: mixRows.length >= 3,
    context: portfolio.ready,
    standards: standards.ready,
    demand: demand.ready,
    loyalty: loyaltySlotCount(brand) >= 5,
    geo: evaluateGeographicFootprintReadiness(brand, geoRows).ready,
  };

  const readyCount = Object.values(generalized).filter(Boolean).length;
  return {
    sections: generalized,
    readinessScore: Math.round((readyCount / 8) * 100),
    brandExplorerRequiredSectionsReady: readyCount === 8,
    readyCount,
    evaluators: { portfolio, standards, demand },
  };
}

function sectionsMovedToReady(legacySections, generalizedSections) {
  const moved = [];
  for (const { key, section } of SECTION_KEYS) {
    if (!legacySections[key] && generalizedSections[key]) {
      moved.push(section);
    }
  }
  return moved;
}

function sectionsStillBlocked(generalizedSections, evaluators) {
  const blocked = [];
  for (const { key, section } of SECTION_KEYS) {
    if (!generalizedSections[key]) {
      const detail = { section, blockers: [] };
      if (key === "context") detail.blockers = evaluators.portfolio?.blockers || [];
      if (key === "standards") detail.blockers = evaluators.standards?.blockers || [];
      if (key === "demand") detail.blockers = evaluators.demand?.blockers || [];
      blocked.push(detail);
    }
  }
  return blocked;
}

function recommendedNextWriter(blockedSections, target) {
  if (!blockedSections.length) return "";
  const first = blockedSections[0];
  if (first.section === "Portfolio Context") {
    return "brand-explorer-portfolio-mix-context-normalization-writer";
  }
  if (first.section === "Standard Detail / Where Available") {
    return target.recordId === TRIBUTE_RECORD_ID
      ? "brand-explorer-tribute-standard-detail-review-approval-writer"
      : "brand-explorer-standard-detail-governance-writer";
  }
  if (first.section === "Demand Scenario View") {
    return "brand-explorer-demand-scenario-writer";
  }
  if (first.section === "Geographic Footprint") {
    return "brand-explorer-tribute-geographic-footprint-refinement-writer";
  }
  return "brand-explorer-complete-build-orchestrator";
}

function buildMarkdown(report) {
  const lines = [];
  lines.push(`# Brand Explorer Required-Section Contract Generalization Writer v${WRITER_VERSION}`);
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Brands: ${report.brandsAudited.join(", ")}`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Summary");
  lines.push(`- v27B exists: **${report.v27BExists ? "yes" : "no"}**`);
  lines.push(`- Contract identity bug fixed: **${report.contractIdentityBugFixed ? "yes" : "no"}**`);
  lines.push(`- Tribute preserved (contract 100 / 8/8): **${report.tributePreserved ? "yes" : "no"}**`);
  lines.push(`- Recommended next brand: **${report.recommendedNextBrand?.name || "none"}**`);
  lines.push("");
  lines.push("## Per-brand contract scores");
  lines.push("| Brand | Pre | Post | Moved to ready | Still blocked |");
  lines.push("| --- | ---: | ---: | --- | --- |");
  for (const b of report.brandResults) {
    lines.push(
      `| ${b.brand.name} | ${b.preGeneralization.readinessScore} | ${b.postGeneralization.readinessScore} | ${b.sectionsMovedToReady.join("; ") || "—"} | ${b.sectionsStillBlocked.map((s) => s.section).join("; ") || "—"} |`
    );
  }
  lines.push("");
  for (const b of report.brandResults) {
    lines.push(`### ${b.brand.name} (\`${b.brand.recordId}\`)`);
    lines.push(`- Parent: ${b.brand.parentCompany || "unknown"}`);
    lines.push(`- Pre: ${b.preGeneralization.readinessScore} (${b.preGeneralization.readyCount}/8)`);
    lines.push(`- Post: ${b.postGeneralization.readinessScore} (${b.postGeneralization.readyCount}/8)`);
    if (b.sectionsMovedToReady.length) {
      lines.push(`- False negatives resolved: ${b.sectionsMovedToReady.join(", ")}`);
    }
    if (b.sectionsStillBlocked.length) {
      lines.push("- Real blockers:");
      for (const s of b.sectionsStillBlocked) {
        lines.push(`  - **${s.section}**: ${s.blockers.join("; ") || "below minimum"}`);
      }
      lines.push(`- Next writer: \`${b.recommendedNextWriter}\``);
    } else {
      lines.push("- All required sections ready under v27B contract.");
    }
    lines.push("");
  }
  lines.push("## Exact next command");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  return lines.join("\n");
}

export async function buildBrandExplorerRequiredSectionContractGeneralizationWriterReport(options = {}) {
  const targets = resolveTargets(options);
  const brandResults = [];
  let companyValidatedSnapshot = null;

  for (const target of targets) {
    const brand = await fetchBrandApiShape(target.recordId);
    if (!brand) {
      brandResults.push({
        brand: target,
        error: `Could not load brand ${target.recordId}`,
      });
      continue;
    }

    const liveState = await fetchLiveState(target.recordId).catch(() => null);
    if (!companyValidatedSnapshot && liveState?.basicsFields) {
      companyValidatedSnapshot = {
        companyValidated: liveState.basicsFields["Company Validated"] ?? null,
        companyValidationDate: liveState.basicsFields["Company Validation Date"] ?? null,
      };
    }

    const pre = computeLegacySectionReadiness(brand);
    const post = computeGeneralizedSectionReadiness(brand);
    const contractReport = await buildBrandExplorerRequiredSectionPopulationContractReport({
      brandIdOrName: target.slug,
    });

    const moved = sectionsMovedToReady(pre.sections, post.sections);
    const blocked = sectionsStillBlocked(post.sections, post.evaluators);

    brandResults.push({
      brand: {
        recordId: target.recordId,
        slug: target.slug,
        name: nz(brand.name) || target.name,
        parentCompany: nz(brand.parentCompany),
      },
      preGeneralization: pre,
      postGeneralization: post,
      contractReportScore: contractReport.readinessScore,
      contractReportReady: contractReport.brandExplorerRequiredSectionsReady,
      contractBrandIdentity: contractReport.brand,
      sectionsMovedToReady: moved,
      sectionsStillBlocked: blocked,
      recommendedNextWriter: recommendedNextWriter(blocked, target),
      demandFalseNegativeCount: blocksForSlot(brand, "commercial.demand").filter(
        (r) => evaluateDemandScenarioRowComplete(r).falseNegativeVsLegacy
      ).length,
    });
  }

  const tribute = brandResults.find((b) => b.brand?.recordId === TRIBUTE_RECORD_ID);
  const nonTributeBlocked = brandResults
    .filter((b) => b.brand?.recordId !== TRIBUTE_RECORD_ID && !b.error)
    .sort((a, b) => {
      const scoreDiff = b.postGeneralization.readinessScore - a.postGeneralization.readinessScore;
      if (scoreDiff !== 0) return scoreDiff;
      return (b.sectionsStillBlocked?.length || 0) - (a.sectionsStillBlocked?.length || 0);
    });

  const recommendedNextBrand =
    nonTributeBlocked.find((b) => !b.postGeneralization.brandExplorerRequiredSectionsReady) ||
    nonTributeBlocked[0] ||
    null;

  const identityBugFixed = brandResults.every(
    (b) => !b.error && b.contractBrandIdentity?.recordId === b.brand?.recordId
  );

  const report = {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: options.apply ? "apply-blocked" : "dry-run",
    airtableModified: false,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    v27BExists: true,
    contractIdentityBugFixed: identityBugFixed,
    tributePreserved:
      Boolean(tribute) &&
      tribute.postGeneralization.readinessScore === 100 &&
      tribute.postGeneralization.brandExplorerRequiredSectionsReady &&
      tribute.contractReportScore === 100,
    brandsAudited: targets.map((t) => t.slug),
    brandResults,
    recommendedNextBrand: recommendedNextBrand
      ? {
          slug: recommendedNextBrand.brand.slug,
          name: recommendedNextBrand.brand.name,
          recordId: recommendedNextBrand.brand.recordId,
          postScore: recommendedNextBrand.postGeneralization.readinessScore,
          rationale: recommendedNextBrand.postGeneralization.brandExplorerRequiredSectionsReady
            ? "Contract-ready under v27B — next active-profile pilot"
            : recommendedNextBrand.postGeneralization.readinessScore >= 75
              ? "Highest post-v27B contract score among brands still blocked on required sections"
              : "Top remaining blocked brand after v27B generalization",
        }
      : null,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    companyValidatedSnapshot,
    exactNextCommand:
      "npm run brand-explorer-complete-build -- --all-active --dry-run --target-quality active-profile",
  };

  report.markdown = buildMarkdown(report);
  return report;
}

export function buildBrandExplorerRequiredSectionContractGeneralizationWriterMarkdown(report) {
  return buildMarkdown(report);
}
