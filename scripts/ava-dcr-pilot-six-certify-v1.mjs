/**
 * Digital Content Readiness pilot — six priority hotels + regression trio
 *   node scripts/ava-dcr-pilot-six-certify-v1.mjs
 *   node scripts/ava-dcr-pilot-six-certify-v1.mjs --regression-only
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  appendDigitalContentReadinessActions,
  summarizeDigitalContentReadiness,
} from "../lib/ai-visibility-audit-action-plan/digital-content-readiness-v1.js";
import { runMandatoryAvaEvidenceStages } from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { summarizeCertificationForAdmin } from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";
import { extractPdfUriAnnotationsFromBuffer } from "../lib/ai-visibility-audit-action-plan/pdf-hyperlink-annotations-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const PILOT = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/digital-content-readiness/wave1-pilot-dcr-v1.json"
);

const PILOT_HOTELS = [
  {
    propertyId: "adp_waterstone_boca_raton",
    slug: "waterstone-boca-raton",
    payload: "waterstone-boca-raton-v1.json",
  },
  {
    propertyId: "adp_castillo_hotel_son_vida",
    slug: "castillo-hotel-son-vida",
    payload: "castillo-hotel-son-vida-v1.json",
  },
  {
    propertyId: "adp_sheraton_mallorca_arabella_golf",
    slug: "sheraton-mallorca-arabella-golf",
    payload: "sheraton-mallorca-arabella-golf-v1.json",
  },
  {
    propertyId: "adp_ac_hotel_a_coruna",
    slug: "ac-hotel-a-coruna",
    payload: "ac-hotel-a-coruna-v1.json",
  },
  {
    propertyId: "adp_grand_fiesta_americana_chapultepec",
    slug: "grand-fiesta-americana-chapultepec",
    payload: "grand-fiesta-americana-chapultepec-v1.json",
  },
  {
    propertyId: "adp_fiesta_americana_condesa_cancun_ai",
    slug: "fiesta-americana-condesa-cancun-ai",
    payload: "fiesta-americana-condesa-cancun-ai-v1.json",
  },
];

const REGRESSION = [
  {
    propertyId: "adp_bethesda_marriott",
    slug: "bethesda-marriott",
    payload: "bethesda-marriott-v1.json",
  },
  {
    propertyId: "adp_wyndham_san_diego_bayside",
    slug: "wyndham-san-diego-bayside",
    payload: "wyndham-san-diego-bayside-v1.json",
  },
  {
    propertyId: "adp_spice_island_beach_resort",
    slug: "spice-island-beach-resort",
    payload: "spice-island-beach-resort-v1.json",
  },
];

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function writeJson(file, data) {
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`);
}

async function processHotel(hotel, enrichmentRoot, { applyDcr }) {
  const payloadPath = path.join(
    ROOT,
    "lib/ai-visibility-audit-action-plan/payloads",
    hotel.payload
  );
  let payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const priorMeta = getAuditMeta(hotel.propertyId) || {};
  const founder = priorMeta.founderApprovedForCustomer === true;

  if (applyDcr) {
    const enrich = enrichmentRoot.hotels[hotel.propertyId];
    if (!enrich) throw new Error(`MISSING_DCR_ENRICHMENT ${hotel.propertyId}`);
    const dcrInput = {
      propertyId: hotel.propertyId,
      hotelName: payload.property?.name,
      ...enrich,
      paidProviderUsed: false,
      paidResearchUsd: 0,
      researchMode: "PUBLIC_PARTIAL",
    };
    payload = appendDigitalContentReadinessActions(payload, dcrInput);
    // Idempotency proof
    payload = appendDigitalContentReadinessActions(payload, dcrInput);

    if (
      payload.executiveSummary?.closing &&
      enrich.executiveAssessment &&
      !/Digital content readiness:/i.test(payload.executiveSummary.closing)
    ) {
      const bite = enrich.executiveAssessment.split(".")[0] + ".";
      const closing = String(payload.executiveSummary.closing);
      payload.executiveSummary.closing =
        `${closing} Digital content readiness: ${bite}`.length > 620
          ? `${closing.slice(0, 450).trim()}… Digital content readiness: ${bite}`
          : `${closing} Digital content readiness: ${bite}`;
    }

    payload.reporting = {
      ...(payload.reporting || {}),
      digitalContentReadinessContract: "AVA_DIGITAL_CONTENT_READINESS_V1",
      digitalContentReadinessGeneratedAt: new Date().toISOString(),
    };
    writeJson(payloadPath, payload);
  }

  const outDir = path.join(REPORT, hotel.slug, "pdf-staged");
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: payload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);
  if (fs.existsSync(outPath) && applyDcr) {
    const bak = path.join(outDir, "pre-dcr-pilot");
    fs.mkdirSync(bak, { recursive: true });
    fs.copyFileSync(outPath, path.join(bak, `${Date.now()}-${filename}`));
  }

  const pipeline = await runMandatoryAvaEvidenceStages(payload, {
    dryRun: true,
    persistCache: true,
    forcePdfRegenerate: applyDcr === true,
    pdfBuffer:
      !applyDcr && fs.existsSync(outPath) ? fs.readFileSync(outPath) : null,
    manualVerifications:
      priorMeta.evidenceHyperlinkCertification?.manualVerifications || {},
    generatePdfFn: async ({ payload: p }) => {
      await generateAiVisibilityAuditPdfV1({ payload: p, outPath, pagesDir: null });
      return { buffer: fs.readFileSync(outPath), path: outPath };
    },
  });

  payload = pipeline.result.payload;
  writeJson(payloadPath, payload);
  if (!fs.existsSync(outPath)) {
    await generateAiVisibilityAuditPdfV1({ payload, outPath, pagesDir: null });
  }

  const pdfBuf = fs.readFileSync(outPath);
  const pdfSha = sha256(pdfBuf);
  const annotations = extractPdfUriAnnotationsFromBuffer(pdfBuf);
  const nihBroken = annotations.uniqueUrls.some((u) =>
    /cc\.nih\.gov\/about\/visitor\/hotels/i.test(u)
  );
  const gatePass =
    pipeline.result.certification?.gate?.pass === true && !nihBroken;
  const summary = summarizeDigitalContentReadiness(
    payload.digitalContentReadiness
  );

  const dcrActionIds = (payload.digitalContentReadiness?.actions || []).map(
    (a) => a.actionId
  );
  const dupActions = (payload.actions || [])
    .map((a) => a.actionId)
    .filter((id, i, arr) => id && arr.indexOf(id) !== i);

  if (applyDcr || gatePass) {
    const versionId = `ava_v1_dcr_${hotel.slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
    const staged = stageAvaPdf({
      propertyId: hotel.propertyId,
      generatedAbsPath: outPath,
      versionId,
      filename,
    });
    saveAuditVersion(hotel.propertyId, versionId, payload, {
      status: gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
      metaStatus: gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
      forceStatus: true,
      kind: "REPORT_VERSION",
      pdfRelativePath: staged.pdfRelativePath,
      researchCostUsd: 0,
      validation: {
        enhancement: "digital-content-readiness-pilot-v1",
        evidenceHyperlinkGatePass: gatePass,
        founderApprovedForCustomer: founder,
        customerReadyBlocked: true,
        pdfSha256: staged.sha256,
        nihBrokenUrlAbsent: !nihBroken,
      },
    });
    upsertAuditMeta(hotel.propertyId, {
      status: gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
      forceStatus: true,
      evidencePipelineStatus: pipeline.result.pipelineStatus,
      pdfRelativePath: staged.pdfRelativePath,
      pdfSha256: pdfSha,
      founderApprovedForCustomer: founder,
      founderApprovedForCustomerAt: priorMeta.founderApprovedForCustomerAt || null,
      evidenceHyperlinkCertification: {
        ...(priorMeta.evidenceHyperlinkCertification || {}),
        lastRun: summarizeCertificationForAdmin(pipeline.result.certification),
      },
      digitalContentReadinessStatus: {
        contract: "AVA_DIGITAL_CONTENT_READINESS_V1",
        at: new Date().toISOString(),
        summary,
        paidResearchUsd: 0,
      },
    });
  }

  return {
    propertyId: hotel.propertyId,
    name: payload.property.name,
    mode: applyDcr ? "DCR_PILOT" : "REGRESSION",
    status: gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    gatePass,
    nihBrokenUrlAbsent: !nihBroken,
    founderApprovedForCustomer: founder,
    pdfSha256: pdfSha,
    filename,
    summary,
    idempotent:
      dupActions.length === 0 &&
      new Set(dcrActionIds).size === dcrActionIds.length,
    hotelVerdict: gatePass && (!applyDcr || summary.platformsIdentified > 0) ? "PASS" : "FAIL",
    remainingCorrections: gatePass
      ? []
      : pipeline.result.certification?.gate?.reasons || ["EVIDENCE_GATE_FAILED"],
  };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const regressionOnly = process.argv.includes("--regression-only");
  const enrichment = JSON.parse(fs.readFileSync(PILOT, "utf8"));
  const rows = [];

  if (!regressionOnly) {
    for (const h of PILOT_HOTELS) {
      console.log("DCR pilot", h.propertyId, "...");
      const row = await processHotel(h, enrichment, { applyDcr: true });
      rows.push(row);
      console.log(
        " →",
        row.hotelVerdict,
        "platforms",
        row.summary.platformsIdentified,
        "gaps",
        row.summary.materialContentGaps
      );
    }
  }

  for (const h of REGRESSION) {
    console.log("DCR regression", h.propertyId, "...");
    const row = await processHotel(h, enrichment, { applyDcr: false });
    rows.push(row);
    console.log(" →", row.hotelVerdict, row.mode);
  }

  const pilotRows = rows.filter((r) => r.mode === "DCR_PILOT");
  const report = {
    contract: "AVA_DIGITAL_CONTENT_READINESS_PILOT_RESULTS_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    publicationAuthorized: false,
    hotels: rows,
    totals: {
      pilot: pilotRows.length,
      pilotPass: pilotRows.filter((r) => r.hotelVerdict === "PASS").length,
      regression: rows.filter((r) => r.mode === "REGRESSION").length,
      regressionPass: rows.filter(
        (r) => r.mode === "REGRESSION" && r.hotelVerdict === "PASS"
      ).length,
    },
  };
  fs.mkdirSync(REPORT, { recursive: true });
  writeJson(path.join(REPORT, "AVA_DIGITAL_CONTENT_READINESS_PILOT_RESULTS.json"), report);
  console.log(JSON.stringify(report.totals, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
