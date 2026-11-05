/**
 * Phase 2 Ambridge + Phase 3 Bethesda — DA merge + evidence gate + PDF cert
 *   node scripts/ava-phase2-ambridge-bethesda-da-certify-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { appendDigitalAuthorityActions, summarizeDigitalAuthorityClassifications } from "../lib/ai-visibility-audit-action-plan/digital-authority-v1.js";
import { runMandatoryAvaEvidenceStages } from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { extractPdfUriAnnotationsFromBuffer } from "../lib/ai-visibility-audit-action-plan/pdf-hyperlink-annotations-v1.js";
import { summarizeCertificationForAdmin } from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const ENRICHMENT = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/digital-authority/phase2-ambridge-bethesda-da-v1.json"
);

const HOTELS = [
  {
    propertyId: "adp_sheraton_guadalajara_expo",
    slug: "sheraton-guadalajara-expo",
    payload: "lib/ai-visibility-audit-action-plan/payloads/sheraton-guadalajara-expo-v1.json",
    cohort: "ambridge",
  },
  {
    propertyId: "adp_voco_cancun_zona_hotelera",
    slug: "voco-cancun-zona-hotelera",
    payload: "lib/ai-visibility-audit-action-plan/payloads/voco-cancun-zona-hotelera-v1.json",
    cohort: "ambridge",
  },
  {
    propertyId: "adp_wyndham_alltra_punta_cana",
    slug: "wyndham-alltra-punta-cana",
    payload: "lib/ai-visibility-audit-action-plan/payloads/wyndham-alltra-punta-cana-v1.json",
    cohort: "ambridge",
  },
  {
    propertyId: "adp_bethesda_marriott",
    slug: "bethesda-marriott",
    payload: "lib/ai-visibility-audit-action-plan/payloads/bethesda-marriott-v1.json",
    cohort: "bethesda",
  },
];

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function writeJson(file, data) {
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`);
}

async function processHotel(hotel, enrichmentRoot) {
  const payloadPath = path.join(ROOT, hotel.payload);
  let payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const priorMeta = getAuditMeta(hotel.propertyId) || {};
  const founder = priorMeta.founderApprovedForCustomer === true;
  const enrich = enrichmentRoot.hotels[hotel.propertyId];
  if (!enrich) throw new Error(`MISSING_ENRICHMENT ${hotel.propertyId}`);

  const daInput = {
    propertyId: hotel.propertyId,
    hotelName: payload.property?.name,
    officialWebsite: payload.property?.website,
    researchMode: "PUBLIC_PARTIAL",
    paidProviderUsed: false,
    paidResearchUsd: 0,
    inventoryCompleteness: enrich.inventoryCompleteness,
    executiveFinding: enrich.executiveFinding,
    competitiveComparison: enrich.competitiveComparison,
    sources: enrich.sources || [],
    competitorBacklinks: enrich.competitorBacklinks || [],
    competitorGaps: enrich.competitorGaps || [],
    opportunities: enrich.opportunities || [],
    actions: enrich.actions || [],
    commercialInsights: enrich.commercialInsights || [],
  };

  payload = appendDigitalAuthorityActions(payload, daInput, {
    metaPaidResearchAuthorized: false,
  });
  payload = appendDigitalAuthorityActions(payload, daInput, {
    metaPaidResearchAuthorized: false,
  });

  if (payload.executiveSummary?.closing && enrich.commercialInsights?.[0]?.insight) {
    const closing = String(payload.executiveSummary.closing);
    if (!/Digital authority:/i.test(closing)) {
      const bite = enrich.commercialInsights[0].insight;
      payload.executiveSummary.closing =
        `${closing} Digital authority: ${bite}`.length > 580
          ? `${closing.slice(0, 420).trim()}… Digital authority: ${bite}`
          : `${closing} Digital authority: ${bite}`;
    }
  }

  writeJson(payloadPath, payload);

  const outDir = path.join(REPORT, hotel.slug, "pdf-staged");
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: payload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);
  if (fs.existsSync(outPath)) {
    const bakDir = path.join(outDir, "pre-phase2-da");
    fs.mkdirSync(bakDir, { recursive: true });
    fs.copyFileSync(outPath, path.join(bakDir, `${Date.now()}-${filename}`));
  }

  const pipeline = await runMandatoryAvaEvidenceStages(payload, {
    dryRun: true,
    persistCache: true,
    forcePdfRegenerate: true,
    manualVerifications:
      priorMeta.evidenceHyperlinkCertification?.manualVerifications || {},
    generatePdfFn: async ({ payload: p }) => {
      await generateAiVisibilityAuditPdfV1({ payload: p, outPath, pagesDir: null });
      return { buffer: fs.readFileSync(outPath), path: outPath };
    },
  });

  const remediation = pipeline.result;
  payload = remediation.payload;
  writeJson(payloadPath, payload);

  const pdfBuf = fs.readFileSync(outPath);
  const pdfSha = sha256(pdfBuf);
  const annotations = extractPdfUriAnnotationsFromBuffer(pdfBuf);
  const nihBroken = annotations.uniqueUrls.some((u) =>
    /cc\.nih\.gov\/about\/visitor\/hotels/i.test(u)
  );

  const versionId = `ava_v1_phase2_${hotel.slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId: hotel.propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });

  const archiveDir = path.join(
    ROOT,
    "reports/ai-visibility-audit-action-plan/archive",
    hotel.propertyId
  );
  fs.mkdirSync(archiveDir, { recursive: true });
  fs.copyFileSync(outPath, path.join(archiveDir, "current.pdf"));
  fs.copyFileSync(outPath, path.join(archiveDir, filename));

  const gatePass = remediation.certification?.gate?.pass === true && !nihBroken;
  const status = gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED";

  saveAuditVersion(hotel.propertyId, versionId, payload, {
    status,
    metaStatus: status,
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.pdfRelativePath,
    researchCostUsd: 0,
    validation: {
      enhancement: "phase2-ambridge-bethesda-da-v1",
      evidenceHyperlinkGatePass: gatePass,
      founderApprovedForCustomer: founder,
      customerReadyBlocked: true,
      pdfSha256: staged.sha256,
      nihBrokenUrlAbsent: !nihBroken,
    },
  });

  const classes = summarizeDigitalAuthorityClassifications(payload.digitalAuthority);
  upsertAuditMeta(hotel.propertyId, {
    status,
    forceStatus: true,
    evidencePipelineStatus: remediation.pipelineStatus,
    pdfRelativePath: staged.pdfRelativePath,
    pdfSha256: pdfSha,
    founderApprovedForCustomer: founder,
    founderApprovedForCustomerAt: priorMeta.founderApprovedForCustomerAt || null,
    evidenceHyperlinkCertification: {
      ...(priorMeta.evidenceHyperlinkCertification || {}),
      lastRun: summarizeCertificationForAdmin(remediation.certification),
    },
    digitalAuthorityStatus: {
      wave: hotel.cohort,
      phase2At: new Date().toISOString(),
      researchMode: "PUBLIC_PARTIAL",
      paidResearchUsd: 0,
      classifications: classes,
    },
    autonomousEvidenceResolution: {
      ...(priorMeta.autonomousEvidenceResolution || {}),
      lastRun: {
        finishedAt: remediation.finishedAt,
        summary: remediation.summary,
        resolutionLog: remediation.resolutionLog,
        phase2: true,
      },
    },
  });

  return {
    propertyId: hotel.propertyId,
    name: payload.property.name,
    cohort: hotel.cohort,
    status,
    gatePass,
    nihBrokenUrlAbsent: !nihBroken,
    founderApprovedForCustomer: founder,
    pdfSha256: pdfSha,
    filename,
    recommendationCount: (payload.actions || []).length,
    classifications: classes,
    verifiedBacklinks: classes.VERIFIED_BACKLINK || 0,
    verifiedCompetitorBacklinks: classes.VERIFIED_COMPETITOR_BACKLINK || 0,
    remainingUnresolved: remediation.summary?.remainingUnresolved || 0,
    hotelVerdict: gatePass ? "PASS" : "FAIL",
  };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const enrichment = JSON.parse(fs.readFileSync(ENRICHMENT, "utf8"));
  const rows = [];
  for (const h of HOTELS) {
    console.log("Phase2/3", h.propertyId, "...");
    const row = await processHotel(h, enrichment);
    rows.push(row);
    console.log(" →", row.hotelVerdict, row.status, "NIH_clean", row.nihBrokenUrlAbsent);
  }
  const report = {
    contract: "AVA_PHASE2_AMBRIDGE_BETHESDA_DA_CERTIFICATION_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    publicationAuthorized: false,
    ambridgeIdentification: {
      confirmed: true,
      source:
        "reports/dealality-adp-ambridge-three-hotel-integrity-re-audit-v1/INTEGRITY_AUDIT_VERDICT.md",
      hotels: [
        "adp_sheraton_guadalajara_expo",
        "adp_voco_cancun_zona_hotelera",
        "adp_wyndham_alltra_punta_cana",
      ],
    },
    hotels: rows,
    totals: {
      hotels: rows.length,
      pass: rows.filter((r) => r.hotelVerdict === "PASS").length,
      fail: rows.filter((r) => r.hotelVerdict === "FAIL").length,
    },
  };
  fs.mkdirSync(REPORT, { recursive: true });
  writeJson(path.join(REPORT, "AVA_PHASE2_AMBRIDGE_BETHESDA_DA_CERTIFICATION.json"), report);

  const lines = [
    "# AVA Phase 2 Ambridge + Phase 3 Bethesda Certification",
    "",
    `**Generated:** ${report.generatedAt}`,
    `**Verdict:** **${report.totals.fail === 0 ? "PASS" : "FAIL"}** · Paid $0 · Publication not authorized`,
    "",
    "## Ambridge identity",
    "",
    "Canonical three-hotel set locked from ADP Ambridge integrity re-audit (not guessed):",
    "",
    "| Hotel | Property ID |",
    "|---|---|",
    "| Sheraton Guadalajara Expo | `adp_sheraton_guadalajara_expo` |",
    "| voco Cancún Zona Hotelera | `adp_voco_cancun_zona_hotelera` |",
    "| Wyndham Alltra Punta Cana | `adp_wyndham_alltra_punta_cana` |",
    "",
    "## Results",
    "",
    "| Hotel | Cohort | Status | Gate | NIH broken absent | BL | CBL | Recs | Verdict |",
    "|---|---|---|---|---|---:|---:|---:|---|",
  ];
  for (const r of rows) {
    lines.push(
      `| ${r.name} | ${r.cohort} | \`${r.status}\` | ${r.gatePass ? "PASS" : "FAIL"} | ${r.nihBrokenUrlAbsent} | ${r.verifiedBacklinks} | ${r.verifiedCompetitorBacklinks} | ${r.recommendationCount} | **${r.hotelVerdict}** |`
    );
  }
  lines.push("");
  lines.push(
    "Founder review PENDING · Customer distribution NOT_AUTHORIZED for all four."
  );
  fs.writeFileSync(
    path.join(REPORT, "AVA_PHASE2_AMBRIDGE_BETHESDA_DA_CERTIFICATION.md"),
    `${lines.join("\n")}\n`
  );
  console.log(
    JSON.stringify({ totals: report.totals, out: "AVA_PHASE2_AMBRIDGE_BETHESDA_DA_CERTIFICATION.md" }, null, 2)
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
