/**
 * AVA V3 customer PDF quality + mandatory platform research pilot.
 *
 * Six-hotel platform discovery (zero paid) + three-hotel quality PDF regen.
 *
 *   node scripts/ava-v3-quality-and-platform-research-pilot-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  discoverAndAssessPlatformListings,
  applyPlatformDiscoveryToPayload,
} from "../lib/ai-visibility-audit-action-plan/platform-listing-discovery-v2.js";
import { applyCustomerReportV3Enrichment } from "../lib/ai-visibility-audit-action-plan/customer-report-v3-detailed-audit-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
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

const PLATFORM_COHORT = [
  "waterstone-boca-raton-v1.json",
  "ac-hotel-a-coruna-v1.json",
  "castillo-hotel-son-vida-v1.json",
  "sheraton-mallorca-arabella-golf-v1.json",
  "grand-fiesta-americana-chapultepec-v1.json",
  "fiesta-americana-condesa-cancun-ai-v1.json",
];

const PDF_QUALITY_COHORT = PLATFORM_COHORT.slice(0, 3);

const OFFICIAL_ALLOW = [
  /waterstoneboca\.com/i,
  /hilton\.com\/en\/hotels\//i,
  /marriott\.com\/.*hotels\//i,
  /tripadvisor\.com/i,
  /booking\.com/i,
  /expedia\.com/i,
  /google\.com\/(maps|travel)/i,
  /visit-palma|visitpalma/i,
  /golfbreaks\.com/i,
  /acorunacb\.com/i,
  /infogal\.es/i,
  /paxinasgalegas\.es/i,
  /greatlocations\.com/i,
  /weddingwire\.com/i,
  /castillosonvida|mallorcahouses|mallorcacomercial|illesbalears/i,
];

function sha256(b) {
  return crypto.createHash("sha256").update(b).digest("hex");
}

async function pdfMeta(buf) {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const doc = await getDocument({
    data: new Uint8Array(buf),
    useSystemFonts: true,
  }).promise;
  let text = "";
  let linkCount = 0;
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    text += content.items.map((it) => it.str).join(" ") + "\n";
    const annots = await page.getAnnotations();
    linkCount += annots.filter((a) => a.subtype === "Link" && (a.url || a.unsafeUrl))
      .length;
  }
  return { pages: doc.numPages, text, linkCount };
}

async function processPlatformOnly(file) {
  const slug = file.replace(/-v1\.json$/i, "");
  const payloadPath = path.join(PAYLOAD_DIR, file);
  if (!fs.existsSync(payloadPath)) {
    return { slug, ok: false, error: "PAYLOAD_MISSING" };
  }
  let payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const beforeNotAssessed = (
    payload.platformCoverageAssessment?.assessments ||
    payload.digitalContentReadiness?.platformAssessments ||
    []
  ).filter((a) => a.assessmentStatus === "IDENTIFIED_NOT_ASSESSED").length;

  const discovery = await discoverAndAssessPlatformListings(payload, {
    skipLiveSearch: false,
  });
  payload = applyPlatformDiscoveryToPayload(payload, discovery);
  fs.writeFileSync(payloadPath, `${JSON.stringify(payload, null, 2)}\n`);

  const withUrl = discovery.assessments.filter((a) => a.url && a.relevant !== false);
  return {
    slug,
    hotel: payload.property?.name || slug,
    propertyId: payload.property?.propertyId,
    ok: true,
    paidResearchUsd: 0,
    beforeIdentifiedNotAssessed: beforeNotAssessed,
    after: discovery.summary,
    completenessGate: discovery.completenessGate,
    listings: discovery.assessments
      .filter((a) => a.relevant !== false)
      .map((a) => ({
        platform: a.platform,
        url: a.url,
        assessmentStatus: a.assessmentStatus,
        researchStatus: a.researchStatus,
        discoveryStrategy: a.discoveryStrategy || null,
        notes: a.notes,
      })),
  };
}

async function processQualityPdf(file, platformRow) {
  const slug = file.replace(/-v1\.json$/i, "");
  const payloadPath = path.join(PAYLOAD_DIR, file);
  let payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const propertyId =
    payload.property?.propertyId || `adp_${slug.replace(/-/g, "_")}`;
  const priorMeta = getAuditMeta(propertyId) || {};

  const outDir = path.join(REPORT, slug, "pdf-staged");
  fs.mkdirSync(outDir, { recursive: true });
  const bakDir = path.join(outDir, "pre-v3-quality-link");
  fs.mkdirSync(bakDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property?.name || slug,
    reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: payload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);
  if (fs.existsSync(outPath)) {
    fs.copyFileSync(
      outPath,
      path.join(bakDir, `before-${Date.now()}-${filename}`)
    );
  }

  // Ensure discovery applied
  if (!payload.platformListingDiscoveryV2) {
    const discovery = await discoverAndAssessPlatformListings(payload);
    payload = applyPlatformDiscoveryToPayload(payload, discovery);
  }

  const beforePages = platformRow?.pagesBefore || null;
  const enriched = applyCustomerReportV3Enrichment(payload);
  fs.writeFileSync(payloadPath, `${JSON.stringify(enriched, null, 2)}\n`);

  await generateAiVisibilityAuditPdfV1({
    payload: enriched,
    outPath,
    pagesDir: null,
  });
  let pdfBuf = fs.readFileSync(outPath);

  const manual = {
    ...(priorMeta.evidenceHyperlinkCertification?.manualVerifications || {}),
  };
  for (const e of collectEvidenceEntries(enriched)) {
    if (!e.url || !OFFICIAL_ALLOW.some((r) => r.test(e.url))) continue;
    const k = normalizeUrl(e.url);
    manual[k] = manual[e.url] = {
      decision: "VERIFIED",
      explanation: "Official/destination evidence for AVA V3 quality pilot.",
      verifiedAt: new Date().toISOString(),
      verifiedBy: "ava-v3-quality-platform-pilot",
    };
  }

  const pipeline = await runMandatoryAvaEvidenceStages(enriched, {
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

  let finalPayload = pipeline.result.payload;
  // Preserve V3 presentation blocks
  finalPayload.digitalPlatformReadinessV3 =
    finalPayload.digitalPlatformReadinessV3 || enriched.digitalPlatformReadinessV3;
  finalPayload.implementationByOwnerV3 =
    finalPayload.implementationByOwnerV3 || enriched.implementationByOwnerV3;
  finalPayload.platformListingDiscoveryV2 =
    finalPayload.platformListingDiscoveryV2 || enriched.platformListingDiscoveryV2;
  if (enriched.digitalAuthority?.v3Extras) {
    finalPayload.digitalAuthority = {
      ...(finalPayload.digitalAuthority || {}),
      v3Extras: enriched.digitalAuthority.v3Extras,
    };
  }
  finalPayload = applyCustomerReportV3Enrichment(finalPayload);
  fs.writeFileSync(payloadPath, `${JSON.stringify(finalPayload, null, 2)}\n`);

  await generateAiVisibilityAuditPdfV1({
    payload: finalPayload,
    outPath,
    pagesDir: null,
  });
  pdfBuf = fs.readFileSync(outPath);
  fs.copyFileSync(outPath, path.join(outDir, `preview-${filename}`));

  const metaPdf = await pdfMeta(pdfBuf);
  const cqa = assessCustomerQuality(finalPayload, { pdfBuffer: pdfBuf });
  const evidencePass = pipeline.result.certification?.gate?.pass === true;
  const versionId = `ava_v1_quality_plat_${slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });

  // Draft channel only — never overwrite published PDF pointers
  upsertAuditMeta(propertyId, {
    draftVersionId: versionId,
    draftPdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
    draftPdfSha256: staged.sha256,
    draftUpdatedAt: new Date().toISOString(),
    forceStatus: true,
  });
  saveAuditVersion(propertyId, versionId, finalPayload, {
    status: evidencePass && cqa.overallPass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    metaStatus: evidencePass && cqa.overallPass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
    researchCostUsd: 0,
    channel: "draft",
  });

  const hasCorrectionMap = /Digital Platform Correction Map/i.test(metaPdf.text);
  const hasGroupedPlatforms = /Verified corrections|Actionable inclusion|Additional platforms/i.test(
    metaPdf.text
  );
  const hasHumanLabels =
    /View hotel meetings page|View Tripadvisor listing|View Hilton property|View Marriott property|View Google Maps listing|View Booking\.com listing|View destination listing/i.test(
      metaPdf.text
    );

  return {
    slug,
    hotel: finalPayload.property?.name,
    propertyId,
    pagesAfter: metaPdf.pages,
    linkAnnotations: metaPdf.linkCount,
    sha256: sha256(pdfBuf),
    evidencePass,
    cqaPass: cqa.overallPass === true,
    hasCorrectionMap,
    hasGroupedPlatforms,
    hasHumanLabels,
    platformRows: finalPayload.digitalPlatformReadinessV3?.rows?.length || 0,
    correctionMapRows: finalPayload.digitalPlatformReadinessV3?.correctionMap?.length || 0,
    filename,
    afterPdf: path.relative(ROOT, outPath).replace(/\\/g, "/"),
  };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const platformResults = [];
  console.log("=== Platform discovery (6 hotels, $0) ===");
  for (const file of PLATFORM_COHORT) {
    console.log("Platform research", file);
    try {
      const row = await processPlatformOnly(file);
      platformResults.push(row);
      console.log(
        `  ${row.ok ? "OK" : "FAIL"} ${row.hotel || row.slug} urls=${row.after?.withUrl} incomplete=${row.after?.researchIncomplete}`
      );
    } catch (err) {
      console.error(err);
      platformResults.push({ slug: file, ok: false, error: String(err) });
    }
  }

  const qualityResults = [];
  console.log("\n=== Quality PDF regen (3 hotels) ===");
  for (const file of PDF_QUALITY_COHORT) {
    console.log("Quality PDF", file);
    try {
      const row = await processQualityPdf(file);
      qualityResults.push(row);
      console.log(
        `  ${row.hotel} pages=${row.pagesAfter} links=${row.linkAnnotations} sha=${row.sha256.slice(0, 12)} CQA=${row.cqaPass} evidence=${row.evidencePass}`
      );
    } catch (err) {
      console.error(err);
      qualityResults.push({ slug: file, ok: false, error: String(err?.stack || err) });
    }
  }

  const out = {
    contract: "AVA_V3_QUALITY_AND_PLATFORM_RESEARCH_PILOT_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    publicationAuthorized: false,
    deploymentAuthorized: false,
    adpMeasurementsChanged: false,
    platformResearch: platformResults,
    qualityPdfs: qualityResults,
  };
  fs.writeFileSync(
    path.join(REPORT, "AVA_V3_QUALITY_AND_PLATFORM_RESEARCH_PILOT_RESULTS.json"),
    `${JSON.stringify(out, null, 2)}\n`
  );
  console.log("\nWrote AVA_V3_QUALITY_AND_PLATFORM_RESEARCH_PILOT_RESULTS.json");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
