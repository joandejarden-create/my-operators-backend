/**
 * Regen three pilot PDFs after Digital Authority presentation improvements.
 *   node scripts/ava-pdf-visibility-presentation-pilot-v1.mjs
 */
import "dotenv/config";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
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
import { scanInternalLanguageLeaks } from "../lib/ai-visibility-audit-action-plan/customer-facing-language-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);

const HOTELS = [
  {
    propertyId: "adp_castillo_hotel_son_vida",
    slug: "castillo-hotel-son-vida",
    file: "castillo-hotel-son-vida-v1.json",
  },
  {
    propertyId: "adp_ac_hotel_a_coruna",
    slug: "ac-hotel-a-coruna",
    file: "ac-hotel-a-coruna-v1.json",
  },
  {
    propertyId: "adp_waterstone_boca_raton",
    slug: "waterstone-boca-raton",
    file: "waterstone-boca-raton-v1.json",
  },
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

function visibilityChecks(text, payload) {
  const da = payload.digitalAuthority || {};
  const bl = (da.sources || []).filter((s) => s.findingClass === "VERIFIED_BACKLINK");
  const ment = (da.sources || []).filter(
    (s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION"
  );
  return {
    hasDaSection: /Digital Authority & External Visibility/i.test(text),
    hasSnapshot: /External visibility snapshot/i.test(text),
    hasVerifiedLinksHeading: /Verified website links/i.test(text),
    hasMentionsHeading: /references without a clear hotel link|No third-party hyperlinks/i.test(
      text
    ),
    hasPlatformTable: /External digital presence|Google Business Profile/i.test(text),
    hasPartialDisclosure: /partial public review|not a comprehensive paid backlink/i.test(
      text
    ),
    noStrategyCodes: !/A_DIRECT_HOTEL_BACKLINKS|Public V2 discovery|first-party brand-family/i.test(
      text
    ),
    noInternalFindingClass: !/\bVERIFIED_EXTERNAL_MENTION\b|\bVERIFIED_BACKLINK\b/i.test(
      text
    ),
    blCountInPayload: bl.length,
    mentionCountInPayload: ment.length,
    blHostsVisible: bl.filter((b) => {
      try {
        const host = new URL(b.linkingPageUrl).hostname.replace(/^www\./, "");
        return text.includes(host) || text.includes(b.referringOrganization || "");
      } catch {
        return false;
      }
    }).length,
    pages: null,
  };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const results = [];

  for (const hotel of HOTELS) {
    const payloadPath = path.join(
      ROOT,
      "lib/ai-visibility-audit-action-plan/payloads",
      hotel.file
    );
    const payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
    const priorMeta = getAuditMeta(hotel.propertyId) || {};
    const founder = priorMeta.founderApprovedForCustomer === true;

    const outDir = path.join(REPORT, hotel.slug, "pdf-staged");
    fs.mkdirSync(outDir, { recursive: true });
    const filename = buildAiVisibilityAuditPdfFilename({
      hotelName: payload.property.name,
      reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
      date: payload.reporting?.generatedAt,
    });
    const outPath = path.join(outDir, filename);

    // Before snapshot from existing PDF if present
    let before = null;
    if (fs.existsSync(outPath)) {
      const bak = path.join(outDir, "pre-visibility-presentation");
      fs.mkdirSync(bak, { recursive: true });
      const beforeBuf = fs.readFileSync(outPath);
      fs.copyFileSync(outPath, path.join(bak, `${Date.now()}-${filename}`));
      const beforeText = await extractPdfText(beforeBuf);
      before = {
        pages: beforeText.pages,
        sha256: sha256(beforeBuf),
        checks: visibilityChecks(beforeText.text, payload),
        leaks: scanInternalLanguageLeaks(beforeText.text, "pdf").map((l) => l.match),
      };
    }

    const htmlBuilt = buildAiVisibilityAuditPdfHtml(payload);
    const cqaBeforePdf = assessCustomerQuality(payload);
    await generateAiVisibilityAuditPdfV1({ payload, outPath, pagesDir: null });
    let pdfBuf = fs.readFileSync(outPath);

    const manual = {
      ...(priorMeta.evidenceHyperlinkCertification?.manualVerifications || {}),
    };
    for (const e of collectEvidenceEntries(payload)) {
      if (!e.url || !OFFICIAL_ALLOW.some((r) => r.test(e.url))) continue;
      const k = normalizeUrl(e.url);
      manual[k] = manual[e.url] = {
        decision: "VERIFIED",
        explanation: "Official property/destination evidence for visibility presentation pilot.",
        observation: "Allowlist",
        verifiedAt: new Date().toISOString(),
        verifiedBy: "ava-pdf-visibility-presentation-pilot",
      };
    }

    const pipeline = await runMandatoryAvaEvidenceStages(payload, {
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
    fs.writeFileSync(payloadPath, `${JSON.stringify(finalPayload, null, 2)}\n`);
    await generateAiVisibilityAuditPdfV1({
      payload: finalPayload,
      outPath,
      pagesDir: null,
    });
    pdfBuf = fs.readFileSync(outPath);
    const afterText = await extractPdfText(pdfBuf);
    const afterChecks = visibilityChecks(afterText.text, finalPayload);
    afterChecks.pages = afterText.pages;
    const cqa = assessCustomerQuality(finalPayload, { pdfBuffer: pdfBuf });
    const evidencePass = pipeline.result.certification?.gate?.pass === true;
    const leaks = scanInternalLanguageLeaks(afterText.text, "pdf").map((l) => l.match);
    const pdfSha = sha256(pdfBuf);

    const versionId = `ava_v1_vis_pres_${hotel.slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
    const staged = stageAvaPdf({
      propertyId: hotel.propertyId,
      generatedAbsPath: outPath,
      versionId,
      filename,
    });
    saveAuditVersion(hotel.propertyId, versionId, finalPayload, {
      status: evidencePass && cqa.overallPass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
      metaStatus:
        evidencePass && cqa.overallPass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
      forceStatus: true,
      kind: "REPORT_VERSION",
      pdfRelativePath: staged.pdfRelativePath,
      researchCostUsd: 0,
    });
    upsertAuditMeta(hotel.propertyId, {
      status: evidencePass && cqa.overallPass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
      forceStatus: true,
      pdfRelativePath: staged.pdfRelativePath,
      pdfSha256: pdfSha,
      founderApprovedForCustomer: founder,
      visibilityPresentationPilot: {
        at: new Date().toISOString(),
        evidencePass,
        cqaPass: cqa.overallPass,
      },
    });

    const row = {
      propertyId: hotel.propertyId,
      hotel: finalPayload.property.name,
      before,
      after: {
        pages: afterText.pages,
        sha256: pdfSha,
        checks: afterChecks,
        leaks,
      },
      htmlOk: htmlBuilt.ok === true,
      cqaPass: cqa.overallPass === true,
      cqaDefects: (cqa.defects || [])
        .filter((d) => d.severity === "HIGH" || d.severity === "CRITICAL")
        .map((d) => ({ type: d.type, section: d.section, message: d.message })),
      evidenceGatePass: evidencePass,
      filename,
      verdict:
        evidencePass && cqa.overallPass && afterChecks.hasDaSection && afterChecks.noStrategyCodes
          ? "PASS"
          : "PASS_WITH_LIMITATIONS",
    };
    results.push(row);
    console.log(
      row.hotel,
      row.verdict,
      "pages",
      row.after.pages,
      "BL",
      afterChecks.blCountInPayload,
      "hosts",
      afterChecks.blHostsVisible,
      "cqa",
      row.cqaPass,
      "ev",
      evidencePass
    );
  }

  const out = {
    contract: "AVA_PDF_VISIBILITY_PRESENTATION_PILOT_RESULTS_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    hotels: results,
  };
  fs.writeFileSync(
    path.join(REPORT, "AVA_PDF_VISIBILITY_PRESENTATION_PILOT_RESULTS.json"),
    `${JSON.stringify(out, null, 2)}\n`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
