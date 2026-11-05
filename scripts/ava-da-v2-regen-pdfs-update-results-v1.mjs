/**
 * Regenerate PDFs after V2 quality scrub; refresh pilot results.
 *   node scripts/ava-da-v2-regen-pdfs-update-results-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import { runCustomerQualityAssuranceLoop } from "../lib/ai-visibility-audit-action-plan/customer-quality-remediation-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { DIGITAL_AUTHORITY_FINDING_CLASSES } from "../lib/ai-visibility-audit-action-plan/digital-authority-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const PRIOR = JSON.parse(
  fs.readFileSync(path.join(REPORT, "AVA_DIGITAL_AUTHORITY_V2_PILOT_RESULTS.json"), "utf8")
);

const PILOT = [
  { propertyId: "adp_waterstone_boca_raton", slug: "waterstone-boca-raton", payload: "waterstone-boca-raton-v1.json" },
  { propertyId: "adp_castillo_hotel_son_vida", slug: "castillo-hotel-son-vida", payload: "castillo-hotel-son-vida-v1.json" },
  { propertyId: "adp_sheraton_mallorca_arabella_golf", slug: "sheraton-mallorca-arabella-golf", payload: "sheraton-mallorca-arabella-golf-v1.json" },
  { propertyId: "adp_ac_hotel_a_coruna", slug: "ac-hotel-a-coruna", payload: "ac-hotel-a-coruna-v1.json" },
  { propertyId: "adp_grand_fiesta_americana_chapultepec", slug: "grand-fiesta-americana-chapultepec", payload: "grand-fiesta-americana-chapultepec-v1.json" },
  { propertyId: "adp_fiesta_americana_condesa_cancun_ai", slug: "fiesta-americana-condesa-cancun-ai", payload: "fiesta-americana-condesa-cancun-ai-v1.json" },
];

function countClass(sources, cls) {
  return (sources || []).filter((s) => s.findingClass === cls).length;
}

const hotels = [];
for (const hotel of PILOT) {
  const payloadPath = path.join(
    ROOT,
    "lib/ai-visibility-audit-action-plan/payloads",
    hotel.payload
  );
  let payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const priorRow = PRIOR.hotels.find((h) => h.propertyId === hotel.propertyId);
  const priorMeta = getAuditMeta(hotel.propertyId) || {};
  const founder = priorMeta.founderApprovedForCustomer === true;

  const cqa = await runCustomerQualityAssuranceLoop(payload, {
    limits: { maxIterations: 2, maxPdfRegenerations: 0 },
  });
  payload = cqa.payload;
  fs.writeFileSync(payloadPath, `${JSON.stringify(payload, null, 2)}\n`);

  const outDir = path.join(REPORT, hotel.slug, "pdf-staged");
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: payload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);
  await generateAiVisibilityAuditPdfV1({ payload, outPath, pagesDir: null });
  const pdfBuf = fs.readFileSync(outPath);
  const versionId = `ava_v1_da2q_${hotel.slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId: hotel.propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });
  saveAuditVersion(hotel.propertyId, versionId, payload, {
    status: cqa.customerQualityCertified ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    metaStatus: cqa.customerQualityCertified ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.pdfRelativePath,
    researchCostUsd: 0,
  });
  upsertAuditMeta(hotel.propertyId, {
    status: cqa.customerQualityCertified ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    forceStatus: true,
    pdfRelativePath: staged.pdfRelativePath,
    pdfSha256: crypto.createHash("sha256").update(pdfBuf).digest("hex"),
    founderApprovedForCustomer: founder,
  });

  const cov = payload.digitalAuthority?.discoveryCoverage || {};
  hotels.push({
    ...priorRow,
    after: {
      ...priorRow.after,
      verifiedHotelBacklinks: countClass(
        payload.digitalAuthority?.sources,
        DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_BACKLINK
      ),
      verifiedExternalMentions: countClass(
        payload.digitalAuthority?.sources,
        DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_EXTERNAL_MENTION
      ),
      qualityFilterApplied: cov.qualityFilterApplied || null,
      researchCoverage: cov.researchCoverageStatus || priorRow.after.researchCoverage,
    },
    verifiedBacklinkExamples: (payload.digitalAuthority?.sources || [])
      .filter((s) => s.findingClass === "VERIFIED_BACKLINK")
      .map((s) => ({
        referringOrganization: s.referringOrganization,
        linkingPageUrl: s.linkingPageUrl,
        hotelDestinationUrl: s.hotelDestinationUrl,
        destinationUrlClass: s.destinationUrlClass,
      })),
    customerQualityCertified: cqa.customerQualityCertified === true,
    pdfSha256: crypto.createHash("sha256").update(pdfBuf).digest("hex"),
    filename,
  });
  console.log(
    hotel.propertyId,
    "BL",
    hotels[hotels.length - 1].after.verifiedHotelBacklinks,
    "CQA",
    cqa.customerQualityCertified
  );
}

const report = {
  ...PRIOR,
  generatedAt: new Date().toISOString(),
  qualityFilter:
    "EXCLUDE_FIRST_PARTY_BRAND_FAMILY_AND_LOW_QUALITY_REFERRERS",
  hotels,
  totals: {
    hotels: hotels.length,
    verifiedBacklinks: hotels.reduce(
      (n, r) => n + r.after.verifiedHotelBacklinks,
      0
    ),
    verifiedMentions: hotels.reduce(
      (n, r) => n + r.after.verifiedExternalMentions,
      0
    ),
    cqaPass: hotels.filter((r) => r.customerQualityCertified).length,
    searches: hotels.reduce((n, r) => n + (r.after.searchesPerformed || 0), 0),
    pagesInspected: hotels.reduce((n, r) => n + (r.after.pagesInspected || 0), 0),
  },
};
fs.writeFileSync(
  path.join(REPORT, "AVA_DIGITAL_AUTHORITY_V2_PILOT_RESULTS.json"),
  `${JSON.stringify(report, null, 2)}\n`
);
console.log(JSON.stringify(report.totals, null, 2));
