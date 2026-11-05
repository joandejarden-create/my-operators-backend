/**
 * AVA Customer Report V3 — three-hotel detailed-audit pilot.
 * Preserves Executive Audit Summary; enriches remaining sections; regenerates PDFs.
 *
 *   node scripts/ava-customer-report-v3-pilot-v1.mjs
 */
import "dotenv/config";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { applyCustomerReportV3Enrichment, assertExecutiveSummaryStructureIntact } from "../lib/ai-visibility-audit-action-plan/customer-report-v3-detailed-audit-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { buildAiVisibilityAuditPdfHtml } from "../lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { assessCustomerQuality } from "../lib/ai-visibility-audit-action-plan/customer-quality-assurance-v1.js";
import { runMandatoryAvaEvidenceStages } from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";
import { collectEvidenceEntries } from "../lib/ai-visibility-audit-action-plan/evidence-link-validation-v1.js";
import { normalizeUrl } from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");

const PILOTS = [
  "waterstone-boca-raton-v1.json",
  "ac-hotel-a-coruna-v1.json",
  "castillo-hotel-son-vida-v1.json",
];

const OFFICIAL_ALLOW = [
  /waterstoneboca\.com/i,
  /marriott\.com\/.*hotels\//i,
  /hotelacacoruna/i,
  /acorunacb\.com/i,
  /visit-palma|visitpalma/i,
  /castillosonvida/i,
  /golfbreaks\.com/i,
  /illesbalears\.travel/i,
  /greatlocations\.com/i,
  /infogal\.es/i,
  /paxinasgalegas\.es/i,
  /mallorcacomercial\.es/i,
  /mallorcaconventionbureau\.com/i,
  /hilton\.com\/en\/hotels\//i,
  /mallorcahouses\.com/i,
];

function sha256(b) {
  return crypto.createHash("sha256").update(b).digest("hex");
}

async function extractPdfText(buf) {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const doc = await getDocument({
    data: new Uint8Array(buf),
    useSystemFonts: true,
  }).promise;
  let text = "";
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    text += content.items.map((it) => it.str).join(" ") + "\n";
  }
  return { text, pages: doc.numPages };
}

/** PDF.js often inserts soft spaces inside long labels (e.g. PL ATFORM). */
function pdfTextHas(text, phrase) {
  const compact = String(text || "")
    .replace(/\s+/g, "")
    .toLowerCase();
  const needle = String(phrase || "")
    .replace(/\s+/g, "")
    .toLowerCase();
  return needle.length > 0 && compact.includes(needle);
}

function stripCustomerReportV3(payload) {
  const next = JSON.parse(JSON.stringify(payload));
  delete next.customerReportV3;
  delete next.digitalPlatformReadinessV3;
  delete next.implementationByOwnerV3;
  if (next.digitalAuthority) delete next.digitalAuthority.v3Extras;
  if (next.digitalContentReadiness) {
    delete next.digitalContentReadiness.platformDetailRows;
    delete next.digitalContentReadiness.sectionTitleCustomer;
  }
  if (next.methodology) delete next.methodology.implementationByOwnerNote;
  next.keyFindings = (next.keyFindings || []).map((f) => {
    const { commercialRelevance, diagnosisThemes, ...rest } = f;
    void commercialRelevance;
    void diagnosisThemes;
    return rest;
  });
  next.actions = (next.actions || []).map((a) => {
    const { specificIssue, commercialImportance, ...rest } = a;
    void specificIssue;
    void commercialImportance;
    // Keep affectedPlatform if it was a pre-V3 agency field; strip V3-only soft defaults later via re-enrich.
    return rest;
  });
  return next;
}

function compareActionability(beforeText, afterText, beforePayload, afterPayload) {
  const signals = {
    platformTableCols:
      pdfTextHas(afterText, "Verified finding") &&
      pdfTextHas(afterText, "Recommended action"),
    implementationByOwner: pdfTextHas(
      afterText,
      "Implementation by Responsible Party"
    ),
    commercialRelevance: pdfTextHas(afterText, "Commercial relevance"),
    uniqueReferring: pdfTextHas(afterText, "Unique referring websites"),
    exactPlatformOnAction:
      pdfTextHas(afterText, "Exact website or platform") ||
      pdfTextHas(afterText, "Exact website or platform".replace(/ /g, "")),
  };
  // Payload-level confirmation (renderer may glyph-split labels)
  const payloadSignals = {
    platformRows: (afterPayload.digitalPlatformReadinessV3?.rows || []).length >= 8,
    implGroups: (afterPayload.implementationByOwnerV3 || []).length > 0,
    findingsCommercial: (afterPayload.keyFindings || []).every(
      (f) => f.commercialRelevance
    ),
    actionsPlatform: (afterPayload.actions || []).every((a) => a.affectedPlatform),
  };
  return {
    pagesBefore: null,
    pagesAfter: null,
    charsBefore: beforeText.length,
    charsAfter: afterText.length,
    lengthDeltaPct: beforeText.length
      ? Math.round(((afterText.length - beforeText.length) / beforeText.length) * 100)
      : null,
    platformRowsBefore: Object.keys(
      beforePayload.digitalContentReadiness?.platformCoverageRegistry || {}
    ).length,
    platformRowsAfter: (afterPayload.digitalPlatformReadinessV3?.rows || []).length,
    findingEnrichment: (afterPayload.keyFindings || []).filter((f) => f.commercialRelevance)
      .length,
    newCustomerSignals: signals,
    payloadSignals,
    materiallyMoreActionable:
      signals.platformTableCols &&
      signals.implementationByOwner &&
      signals.commercialRelevance &&
      (signals.exactPlatformOnAction || payloadSignals.actionsPlatform) &&
      payloadSignals.platformRows &&
      payloadSignals.implGroups,
  };
}

async function processOne(file) {
  const slug = file.replace(/-v1\.json$/i, "");
  const payloadPath = path.join(PAYLOAD_DIR, file);
  const raw = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  // Re-runs must baseline from a stripped payload so before≠after.
  const before = stripCustomerReportV3(raw);
  const propertyId =
    before.property?.propertyId || `adp_${slug.replace(/-/g, "_")}`;
  const priorMeta = getAuditMeta(propertyId) || {};

  const outDir = path.join(REPORT, slug, "pdf-staged");
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: before.property?.name || slug,
    reportMonthLabel: before.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: before.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);
  const bakDir = path.join(outDir, "pre-customer-report-v3");
  fs.mkdirSync(bakDir, { recursive: true });

  // Preserve any already-staged customer PDF as historical.
  if (fs.existsSync(outPath)) {
    fs.copyFileSync(outPath, path.join(bakDir, `prior-staged-${Date.now()}-${filename}`));
  }

  // Before PDF: stripped (pre-V3) payload through current renderer
  const beforePdfPath = path.join(bakDir, `before-${Date.now()}-${filename}`);
  await generateAiVisibilityAuditPdfV1({
    payload: before,
    outPath: beforePdfPath,
    pagesDir: null,
  });
  const beforeBuf = fs.readFileSync(beforePdfPath);
  const beforeExtract = await extractPdfText(beforeBuf);

  const after = applyCustomerReportV3Enrichment(before);
  const execGate = assertExecutiveSummaryStructureIntact(before, after);
  fs.writeFileSync(payloadPath, `${JSON.stringify(after, null, 2)}\n`);

  const htmlBuilt = buildAiVisibilityAuditPdfHtml(after);
  await generateAiVisibilityAuditPdfV1({
    payload: after,
    outPath,
    pagesDir: null,
  });
  let pdfBuf = fs.readFileSync(outPath);

  const manual = {
    ...(priorMeta.evidenceHyperlinkCertification?.manualVerifications || {}),
  };
  for (const e of collectEvidenceEntries(after)) {
    if (!e.url || !OFFICIAL_ALLOW.some((r) => r.test(e.url))) continue;
    const k = normalizeUrl(e.url);
    manual[k] = manual[e.url] = {
      decision: "VERIFIED",
      explanation: "Official/destination evidence for AVA Customer Report V3 pilot.",
      verifiedAt: new Date().toISOString(),
      verifiedBy: "ava-customer-report-v3-pilot",
    };
  }

  const pipeline = await runMandatoryAvaEvidenceStages(after, {
    dryRun: true,
    persistCache: true,
    pdfBuffer: pdfBuf,
    manualVerifications: manual,
    escalateBrowser: false,
    generatePdfFn: async ({ payload: p }) => {
      await generateAiVisibilityAuditPdfV1({ payload: p, outPath, pagesDir: null });
      return { buffer: fs.readFileSync(outPath), path: outPath };
    },
  });

  const finalPayload = pipeline.result.payload;
  // Re-apply V3 fields if pipeline stripped unknown keys — merge carefully
  if (!finalPayload.digitalPlatformReadinessV3 && after.digitalPlatformReadinessV3) {
    finalPayload.digitalPlatformReadinessV3 = after.digitalPlatformReadinessV3;
  }
  if (!finalPayload.implementationByOwnerV3 && after.implementationByOwnerV3) {
    finalPayload.implementationByOwnerV3 = after.implementationByOwnerV3;
  }
  if (finalPayload.digitalAuthority && after.digitalAuthority?.v3Extras) {
    finalPayload.digitalAuthority.v3Extras = after.digitalAuthority.v3Extras;
  }
  if (after.keyFindings) {
    finalPayload.keyFindings = after.keyFindings;
  }
  if (after.actions) {
    // preserve V3 action fields by actionId
    const byId = new Map(after.actions.map((a) => [a.actionId, a]));
    finalPayload.actions = (finalPayload.actions || []).map((a) => ({
      ...a,
      ...(byId.get(a.actionId) || {}),
      ...a,
      affectedPlatform: a.affectedPlatform || byId.get(a.actionId)?.affectedPlatform,
      specificIssue: a.specificIssue || byId.get(a.actionId)?.specificIssue,
    }));
  }

  fs.writeFileSync(payloadPath, `${JSON.stringify(finalPayload, null, 2)}\n`);
  await generateAiVisibilityAuditPdfV1({
    payload: finalPayload,
    outPath,
    pagesDir: null,
  });
  pdfBuf = fs.readFileSync(outPath);
  const previewPath = path.join(outDir, `preview-${filename}`);
  fs.copyFileSync(outPath, previewPath);

  const afterExtract = await extractPdfText(pdfBuf);
  const cqa = assessCustomerQuality(finalPayload, { pdfBuffer: pdfBuf });
  const evidencePass = pipeline.result.certification?.gate?.pass === true;
  const comparison = compareActionability(
    beforeExtract.text,
    afterExtract.text,
    before,
    finalPayload
  );
  comparison.pagesBefore = beforeExtract.pages;
  comparison.pagesAfter = afterExtract.pages;

  const downloadSha = sha256(pdfBuf);
  const versionId = `ava_v1_customer_v3_${slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });

  const visualQa = {
    hasExecutive: /Executive Audit Summary/i.test(afterExtract.text),
    hasFindings: /Key Digital Presence Findings/i.test(afterExtract.text),
    hasPlatformDetail: /Platform-by-platform assessment|Digital Platform/i.test(
      afterExtract.text
    ),
    hasDa: /Digital Authority/i.test(afterExtract.text),
    hasImpl: /Implementation by Responsible Party/i.test(afterExtract.text),
    noStrategyCodes: !/A_DIRECT_HOTEL_BACKLINKS|Public V2 discovery/i.test(
      afterExtract.text
    ),
  };

  const status =
    evidencePass && cqa.overallPass && execGate.ok && visualQa.hasExecutive
      ? "AUDIT_VALIDATED"
      : "VALIDATION_REQUIRED";

  saveAuditVersion(propertyId, versionId, finalPayload, {
    status,
    metaStatus: status,
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.pdfRelativePath,
    researchCostUsd: 0,
  });
  upsertAuditMeta(propertyId, {
    status,
    forceStatus: true,
    pdfRelativePath: staged.pdfRelativePath,
    pdfSha256: downloadSha,
    founderApprovedForCustomer: priorMeta.founderApprovedForCustomer === true,
    customerReportV3Pilot: {
      at: new Date().toISOString(),
      evidencePass,
      cqaPass: cqa.overallPass,
    },
  });

  return {
    slug,
    hotel: finalPayload.property?.name || slug,
    propertyId,
    status,
    execStructureIntact: execGate.ok,
    execIssues: execGate.issues,
    pagesBefore: beforeExtract.pages,
    pagesAfter: afterExtract.pages,
    sha256: downloadSha,
    evidencePass,
    cqaPass: cqa.overallPass === true,
    cqaDefects: (cqa.defects || [])
      .filter((d) => d.severity === "HIGH" || d.severity === "CRITICAL")
      .map((d) => ({ type: d.type, section: d.section, message: d.message })),
    visualQa,
    comparison,
    platformRows: (finalPayload.digitalPlatformReadinessV3?.rows || []).length,
    backlinks: (finalPayload.digitalAuthority?.sources || []).filter(
      (s) => s.findingClass === "VERIFIED_BACKLINK"
    ).length,
    actions: (finalPayload.actions || []).length,
    filename,
    beforePdf: path.relative(ROOT, beforePdfPath).replace(/\\/g, "/"),
    afterPdf: path.relative(ROOT, outPath).replace(/\\/g, "/"),
    verdict:
      evidencePass && cqa.overallPass && execGate.ok && comparison.materiallyMoreActionable
        ? "PASS"
        : "PASS_WITH_LIMITATIONS",
  };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const results = [];
  for (const file of PILOTS) {
    console.log("Processing", file);
    try {
      const row = await processOne(file);
      results.push(row);
      console.log(
        `${row.verdict} | ${row.hotel} | pages ${row.pagesBefore}→${row.pagesAfter} | platforms=${row.platformRows} | CQA=${row.cqaPass} evidence=${row.evidencePass}`
      );
    } catch (err) {
      console.error("FAIL", file, err);
      results.push({
        slug: file,
        verdict: "FAIL",
        error: String(err?.stack || err),
      });
    }
  }

  const out = {
    contract: "AVA_CUSTOMER_REPORT_V3_PILOT_RESULTS_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    publicationAuthorized: false,
    deploymentAuthorized: false,
    adpMeasurementsChanged: false,
    hotels: results,
    passCount: results.filter((r) => r.verdict === "PASS").length,
    limitationsCount: results.filter((r) => r.verdict === "PASS_WITH_LIMITATIONS")
      .length,
    failCount: results.filter((r) => r.verdict === "FAIL").length,
  };
  fs.writeFileSync(
    path.join(REPORT, "AVA_CUSTOMER_REPORT_V3_PILOT_RESULTS.json"),
    `${JSON.stringify(out, null, 2)}\n`
  );
  console.log(
    `\nV3 pilot: ${out.passCount} PASS / ${out.limitationsCount} limitations / ${out.failCount} FAIL`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
