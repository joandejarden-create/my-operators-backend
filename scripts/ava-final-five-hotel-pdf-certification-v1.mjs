/**
 * Final five-hotel AVA PDF regeneration + independent QA
 *   node scripts/ava-final-five-hotel-pdf-certification-v1.mjs
 *
 * Does NOT publish, deploy, or flip founder/customer authorization flags.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  certifyAvaEvidenceAndHyperlinks,
  summarizeCertificationForAdmin,
  AVA_REJECTED_EVIDENCE_URLS,
} from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";
import { extractPdfUriAnnotationsFromBuffer } from "../lib/ai-visibility-audit-action-plan/pdf-hyperlink-annotations-v1.js";
import { collectEvidenceEntries } from "../lib/ai-visibility-audit-action-plan/evidence-link-validation-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { buildAiVisibilityAuditPdfHtml } from "../lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT_DIR = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);

const HOTELS = [
  {
    propertyId: "adp_waterstone_boca_raton",
    slug: "waterstone-boca-raton",
    payload: "lib/ai-visibility-audit-action-plan/payloads/waterstone-boca-raton-v1.json",
  },
  {
    propertyId: "adp_castillo_hotel_son_vida",
    slug: "castillo-hotel-son-vida",
    payload: "lib/ai-visibility-audit-action-plan/payloads/castillo-hotel-son-vida-v1.json",
  },
  {
    propertyId: "adp_sheraton_mallorca_arabella_golf",
    slug: "sheraton-mallorca-arabella-golf",
    payload:
      "lib/ai-visibility-audit-action-plan/payloads/sheraton-mallorca-arabella-golf-v1.json",
  },
  {
    propertyId: "adp_wyndham_san_diego_bayside",
    slug: "wyndham-san-diego-bayside",
    payload:
      "lib/ai-visibility-audit-action-plan/payloads/wyndham-san-diego-bayside-v1.json",
  },
  {
    propertyId: "adp_spice_island_beach_resort",
    slug: "spice-island-beach-resort",
    payload:
      "lib/ai-visibility-audit-action-plan/payloads/spice-island-beach-resort-v1.json",
  },
];

/** Must never appear as clickable PDF annotations for this cohort */
const FORBIDDEN_CLICKABLE_PATTERNS = [
  /cc\.nih\.gov\/about\/visitor\/hotels/i,
  /arabellagolf\.com(?!mallorca)/i, // bare arabellagolf.com TLS-bad; mallorca sibling may differ
  /waterstoneresort\.com/i,
];

/** Approved shared footer phrase (must appear once per page; not duplicated in body) */
const FOOTER_CONF_RE =
  /Confidential\s*[·•]\s*For recipient only\s*[·•]\s*©\s*2026\s*Dealality/gi;

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function countFooterConfidentiality(text) {
  const m = String(text || "").match(FOOTER_CONF_RE);
  return m ? m.length : 0;
}

async function extractPdfPageTexts(pdfPathOrBuf) {
  const pdfjs = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const data = Buffer.isBuffer(pdfPathOrBuf)
    ? new Uint8Array(pdfPathOrBuf)
    : new Uint8Array(fs.readFileSync(pdfPathOrBuf));
  const doc = await pdfjs.getDocument({ data }).promise;
  const pages = [];
  for (let i = 1; i <= doc.numPages; i += 1) {
    const page = await doc.getPage(i);
    const c = await page.getTextContent();
    pages.push(c.items.map((x) => x.str).join(" "));
  }
  return { pageCount: doc.numPages, pages };
}

function collectRegisterUrls(payload) {
  const entries = collectEvidenceEntries(payload);
  return [...new Set(entries.map((e) => e.url).filter((u) => /^https?:\/\//i.test(u)))];
}

function actionRegisterFingerprint(payload) {
  return sha256(
    Buffer.from(
      JSON.stringify({
        actions: (payload.actions || []).map((a) => ({
          actionId: a.actionId,
          headline: a.headline,
          whatWeFound: a.whatWeFound,
          recommendedSteps: a.recommendedSteps,
          evidenceLinks: a.evidenceLinks,
        })),
        overview: (payload.actionOverview || []).map((a) => a.actionId),
        findings: (payload.keyFindings || []).map((f) => f.title || f.findingId),
      })
    )
  );
}

async function processHotel(hotel) {
  const payloadPath = path.join(ROOT, hotel.payload);
  const payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const priorMeta = getAuditMeta(hotel.propertyId) || {};
  const founder = priorMeta.founderApprovedForCustomer === true;
  const beforeStatus = priorMeta.status || payload.status;

  if (String(beforeStatus) !== "AUDIT_VALIDATED" && payload.status !== "AUDIT_VALIDATED") {
    throw new Error(`${hotel.propertyId} not AUDIT_VALIDATED (status=${beforeStatus})`);
  }

  const outDir = path.join(REPORT_DIR, hotel.slug, "pdf-staged");
  const pagesDir = path.join(outDir, "visual-qa-final-five");
  fs.mkdirSync(outDir, { recursive: true });
  fs.mkdirSync(pagesDir, { recursive: true });

  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: payload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);

  // Preserve previous version
  const preserved = [];
  if (fs.existsSync(outPath)) {
    const bakDir = path.join(outDir, "pre-final-five-cert");
    fs.mkdirSync(bakDir, { recursive: true });
    const bak = path.join(bakDir, `${Date.now()}-${filename}`);
    fs.copyFileSync(outPath, bak);
    preserved.push(path.relative(ROOT, bak).replace(/\\/g, "/"));
  }

  const built = buildAiVisibilityAuditPdfHtml(payload);
  if (!built.ok) {
    throw new Error(`HTML_BUILD_FAIL ${hotel.propertyId}: ${(built.errors || []).join(", ")}`);
  }
  // Body must not repeat the shared footer confidentiality phrase.
  // Cover chrome may include DEALALITY … CONFIDENTIAL · FOR RECIPIENT ONLY (approved).
  const bodyFooterPhraseHits = countFooterConfidentiality(built.html);
  const bodyConfidentialOk = bodyFooterPhraseHits === 0;

  const skipRegen = process.env.AVA_FINAL_FIVE_RESCORE_ONLY === "1" && fs.existsSync(outPath);
  let generated = {
    structural: {
      sectionCount: 0,
      actionCount: (payload.actions || []).length,
      hasCover: true,
    },
  };
  if (!skipRegen) {
    generated = await generateAiVisibilityAuditPdfV1({
      payload,
      outPath,
      pagesDir,
      bridgeOutPath: path.join(outDir, "proposed-monthly-action-register-bridge.json"),
    });
  }

  const pdfBuf = fs.readFileSync(outPath);
  const pdfSha = sha256(pdfBuf);
  const annotations = extractPdfUriAnnotationsFromBuffer(pdfBuf);
  const registerUrls = collectRegisterUrls(payload);
  const expectedFromHtml = built.evidenceUrls || [];

  const forbiddenHits = annotations.uniqueUrls.filter((u) =>
    FORBIDDEN_CLICKABLE_PATTERNS.some((re) => re.test(u))
  );
  if (
    annotations.uniqueUrls.some((u) =>
      /cc\.nih\.gov\/about\/visitor\/hotels/i.test(u)
    )
  ) {
    for (const rejected of Object.keys(AVA_REJECTED_EVIDENCE_URLS)) {
      if (!forbiddenHits.includes(rejected)) forbiddenHits.push(rejected);
    }
  }

  const extracted = await extractPdfPageTexts(pdfBuf);
  const confPerPage = extracted.pages.map((t) => countFooterConfidentiality(t));
  const footerOnlyPass =
    confPerPage.length > 0 && confPerPage.every((n) => n === 1);
  const lastPageText = extracted.pages[extracted.pages.length - 1] || "";
  const lastPageHasSuggestedNext = /Suggested Next Step/i.test(lastPageText);

  const certification = await certifyAvaEvidenceAndHyperlinks(payload, {
    pdfBuffer: pdfBuf,
    pdfPath: outPath,
    manualVerifications:
      priorMeta.evidenceHyperlinkCertification?.manualVerifications || {},
    persistCache: true,
    escalateBrowser: true,
  });

  const registerFp = actionRegisterFingerprint(payload);
  const actionIds = (payload.actions || []).map((a) => a.actionId).filter(Boolean);
  const fullPdfText = extracted.pages.join("\n");
  const missingActionIdsInPdf = actionIds.filter((id) => !fullPdfText.includes(id));
  const missingActionIdsInHtml = actionIds.filter((id) => !built.html.includes(id));

  const unexpectedForbidden = forbiddenHits;
  const hyperlinkPass =
    certification.gate.pass === true &&
    unexpectedForbidden.length === 0 &&
    (certification.summary?.rejectedUrlsInPdf || 0) === 0 &&
    (certification.summary?.pdfMissingRegisterLinks || 0) === 0;

  const evidencePass =
    certification.gate.pass === true &&
    (certification.summary?.brokenLinks || 0) === 0 &&
    (certification.summary?.unsupportedClaims || 0) === 0 &&
    (certification.summary?.linksRequiringManualReview || 0) === 0;

  const contentPass =
    bodyConfidentialOk &&
    missingActionIdsInPdf.length === 0 &&
    missingActionIdsInHtml.length === 0 &&
    built.ok &&
    ((generated.structural?.sectionCount || 0) >= 6 || skipRegen) &&
    (generated.structural?.hasCover === true || skipRegen);

  const visualArtifacts = fs.existsSync(pagesDir)
    ? fs
        .readdirSync(pagesDir)
        .filter((f) => f.endsWith(".png"))
        .map((f) => path.join(pagesDir, f))
    : [];
  const visualPass =
    visualArtifacts.length >= 5 &&
    footerOnlyPass &&
    bodyConfidentialOk &&
    lastPageHasSuggestedNext;

  const versionId = `ava_v1_final_five_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId: hotel.propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });

  // Legacy archive copy
  const archiveDir = path.join(
    ROOT,
    "reports/ai-visibility-audit-action-plan/archive",
    hotel.propertyId
  );
  fs.mkdirSync(archiveDir, { recursive: true });
  const archiveCurrent = path.join(archiveDir, "current.pdf");
  if (fs.existsSync(archiveCurrent)) {
    const hist = path.join(archiveDir, `pre-final-five-${Date.now()}.pdf`);
    fs.copyFileSync(archiveCurrent, hist);
    preserved.push(path.relative(ROOT, hist).replace(/\\/g, "/"));
  }
  fs.copyFileSync(outPath, archiveCurrent);
  fs.copyFileSync(outPath, path.join(archiveDir, filename));

  saveAuditVersion(hotel.propertyId, versionId, payload, {
    status: "AUDIT_VALIDATED",
    metaStatus: "AUDIT_VALIDATED",
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.pdfRelativePath,
    researchCostUsd: 0,
    validation: {
      enhancement: "final-five-hotel-pdf-certification-v1",
      evidenceHyperlinkGatePass: certification.gate.pass,
      founderApprovedForCustomer: founder,
      customerReadyBlocked: true,
      pdfSha256: staged.sha256,
      paidResearchUsd: 0,
    },
  });

  upsertAuditMeta(hotel.propertyId, {
    status: "AUDIT_VALIDATED",
    forceStatus: true,
    pdfRelativePath: staged.pdfRelativePath,
    pdfSha256: pdfSha,
    founderApprovedForCustomer: founder,
    founderApprovedForCustomerAt: priorMeta.founderApprovedForCustomerAt || null,
    evidenceHyperlinkCertification: {
      ...(priorMeta.evidenceHyperlinkCertification || {}),
      lastRun: summarizeCertificationForAdmin(certification),
      lastFullReport: {
        checkedAt: certification.checkedAt,
        gate: certification.gate,
        summary: certification.summary,
      },
      finalFiveHotelCertAt: new Date().toISOString(),
    },
    finalFiveHotelPdfCertification: {
      at: new Date().toISOString(),
      pdfSha256: pdfSha,
      filename,
      evidencePass,
      contentPass,
      hyperlinkPass,
      visualPass,
      founderReview: founder ? "APPROVED" : "PENDING",
      customerDistributionAuthorized: false,
    },
  });

  const hotelVerdict =
    evidencePass && contentPass && hyperlinkPass && visualPass ? "PASS" : "FAIL";

  return {
    propertyId: hotel.propertyId,
    name: payload.property.name,
    beforeStatus,
    status: "AUDIT_VALIDATED",
    hotelVerdict,
    matrix: {
      evidenceValidation: evidencePass ? "PASS" : "FAIL",
      contentIntegrity: contentPass ? "PASS" : "FAIL",
      hyperlinkValidation: hyperlinkPass ? "PASS" : "FAIL",
      visualPdfQuality: visualPass ? "PASS" : "FAIL",
      founderReview: founder ? "APPROVED" : "PENDING",
      customerDistributionAuthorization: "NOT_AUTHORIZED",
    },
    pdf: {
      filename,
      path: path.relative(ROOT, outPath).replace(/\\/g, "/"),
      sha256: pdfSha,
      bytes: pdfBuf.length,
      preservedVersions: preserved,
      archivePath: staged.pdfRelativePath,
    },
    annotations: {
      total: annotations.totalAnnotations,
      unique: annotations.uniqueUrls.length,
      urls: annotations.uniqueUrls,
      forbiddenHits: unexpectedForbidden,
    },
    certification: {
      gatePass: certification.gate.pass,
      reasons: certification.gate.reasons || [],
      summary: certification.summary,
    },
    contentChecks: {
      htmlBuildOk: built.ok,
      bodyFooterConfidentialityPhraseHits: bodyFooterPhraseHits,
      bodyConfidentialOk,
      pageCount: extracted.pageCount,
      confidentialityPerPage: confPerPage,
      footerOnlyPass,
      lastPageHasSuggestedNext,
      missingActionIdsInPdf,
      missingActionIdsInHtml,
      actionRegisterFingerprint: registerFp,
      sectionCount: generated.structural?.sectionCount,
      actionCount: generated.structural?.actionCount ?? actionIds.length,
      hasCover: generated.structural?.hasCover,
      expectedEvidenceUrlCount: expectedFromHtml.length,
      registerUrlCount: registerUrls.length,
      rescoredOnly: skipRegen,
    },
    visualArtifacts: visualArtifacts.map((p) =>
      path.relative(ROOT, p).replace(/\\/g, "/")
    ),
    remainingCorrections: [
      ...(!evidencePass
        ? [
            `Evidence gate: ${(certification.gate.reasons || []).join("; ") || "failed"}`,
          ]
        : []),
      ...(!contentPass
        ? [
            missingActionIdsInPdf.length
              ? `Action IDs missing from PDF text: ${missingActionIdsInPdf.join(", ")}`
              : null,
            missingActionIdsInHtml.length
              ? `Action IDs missing from HTML: ${missingActionIdsInHtml.join(", ")}`
              : null,
            !bodyConfidentialOk
              ? `Body HTML repeats footer confidentiality phrase (${bodyFooterPhraseHits})`
              : null,
            !skipRegen && !(generated.structural?.hasCover)
              ? "Cover chrome missing"
              : null,
          ].filter(Boolean)
        : []),
      ...(!hyperlinkPass
        ? [
            unexpectedForbidden.length
              ? `Forbidden clickable URLs: ${unexpectedForbidden.join(", ")}`
              : null,
            ...(certification.gate.reasons || []),
          ].filter(Boolean)
        : []),
      ...(!visualPass
        ? [
            visualArtifacts.length < 5
              ? `Insufficient visual QA screenshots (${visualArtifacts.length})`
              : null,
            !footerOnlyPass
              ? `Footer confidentiality not exactly once per page: [${confPerPage.join(",")}]`
              : null,
            !lastPageHasSuggestedNext
              ? "Last page missing Suggested Next Step"
              : null,
          ].filter(Boolean)
        : []),
    ],
    founderApprovedForCustomer: founder,
    customerDistributionAuthorized: false,
  };
}

function renderMarkdown(report) {
  const lines = [];
  lines.push("# AVA Final Five-Hotel PDF Certification");
  lines.push("");
  lines.push(`**Generated:** ${report.generatedAt}`);
  lines.push(
    "**Publication / deploy / customer distribution:** not authorized · **Founder flags:** unchanged"
  );
  lines.push("");
  lines.push(`## Overall cohort verdict: **${report.cohortVerdict}**`);
  lines.push("");
  lines.push(
    `${report.totals.pass}/${report.totals.hotels} hotels PASS technical PDF readiness. Founder review and customer distribution remain separate gates.`
  );
  lines.push("");
  lines.push("## Readiness matrix");
  lines.push("");
  lines.push(
    "| Hotel | Evidence | Content | Hyperlinks | Visual PDF | Founder review | Customer distribution | Hotel verdict |"
  );
  lines.push("|---|---|---|---|---|---|---|---|");
  for (const h of report.hotels) {
    lines.push(
      `| ${h.name} | ${h.matrix.evidenceValidation} | ${h.matrix.contentIntegrity} | ${h.matrix.hyperlinkValidation} | ${h.matrix.visualPdfQuality} | ${h.matrix.founderReview} | ${h.matrix.customerDistributionAuthorization} | **${h.hotelVerdict}** |`
    );
  }
  lines.push("");
  lines.push("## Per-hotel detail");
  lines.push("");
  for (const h of report.hotels) {
    lines.push(`### ${h.name} (\`${h.propertyId}\`) — ${h.hotelVerdict}`);
    lines.push("");
    lines.push(`- **PDF:** \`${h.pdf.filename}\``);
    lines.push(`- **SHA-256:** \`${h.pdf.sha256}\``);
    lines.push(`- **Bytes:** ${h.pdf.bytes}`);
    lines.push(`- **Staged path:** \`${h.pdf.path}\``);
    lines.push(`- **Archive:** \`${h.pdf.archivePath}\``);
    lines.push(
      `- **Preserved prior versions:** ${h.pdf.preservedVersions.length ? h.pdf.preservedVersions.map((p) => `\`${p}\``).join(", ") : "(none on disk)"}`
    );
    lines.push(
      `- **PDF annotations:** ${h.annotations.unique} unique / ${h.annotations.total} total`
    );
    lines.push(
      `- **Evidence gate:** ${h.certification.gatePass ? "PASS" : "FAIL"} · broken=${h.certification.summary?.brokenLinks ?? "?"} · unsupported=${h.certification.summary?.unsupportedClaims ?? "?"} · manual=${h.certification.summary?.linksRequiringManualReview ?? "?"} · missingPdfLinks=${h.certification.summary?.pdfMissingRegisterLinks ?? "?"}`
    );
    lines.push(
      `- **Confidentiality:** body footer-phrase hits=${h.contentChecks.bodyFooterConfidentialityPhraseHits} (must be 0) · per-page footer=[${(h.contentChecks.confidentialityPerPage || []).join(",")}] · footerOnlyPass=${h.contentChecks.footerOnlyPass}`
    );
    lines.push(
      `- **Pages:** ${h.contentChecks.pageCount} · Suggested Next Step on last page: ${h.contentChecks.lastPageHasSuggestedNext}`
    );
    lines.push(
      `- **Action register:** ${h.contentChecks.actionCount} actions · missing IDs in PDF text: ${h.contentChecks.missingActionIdsInPdf.length ? h.contentChecks.missingActionIdsInPdf.join(", ") : "none"}`
    );
    if (h.annotations.forbiddenHits?.length) {
      lines.push(`- **Forbidden clickable URLs:** ${h.annotations.forbiddenHits.join(", ")}`);
    }
    if (h.remainingCorrections?.length) {
      lines.push("- **Remaining corrections:**");
      for (const c of h.remainingCorrections) lines.push(`  - ${c}`);
    } else {
      lines.push("- **Remaining corrections:** none for technical PDF readiness");
    }
    lines.push("");
    lines.push("<details><summary>Extracted hyperlinks</summary>");
    lines.push("");
    for (const u of h.annotations.urls) lines.push(`- ${u}`);
    lines.push("");
    lines.push("</details>");
    lines.push("");
  }
  lines.push("## Authorization boundaries");
  lines.push("");
  lines.push("- Evidence validation / content / hyperlink / visual gates are technical only.");
  lines.push("- Founder review remains PENDING unless already explicitly approved.");
  lines.push("- Customer distribution is NOT_AUTHORIZED for all five.");
  lines.push("- No production deploy, Railway changes, emails, or publication performed.");
  lines.push("");
  lines.push("## Machine results");
  lines.push("");
  lines.push("`AVA_FINAL_FIVE_HOTEL_PDF_CERTIFICATION_RESULTS.json`");
  return `${lines.join("\n")}\n`;
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const rows = [];
  for (const h of HOTELS) {
    console.log("Final five PDF cert", h.propertyId, "...");
    const row = await processHotel(h);
    rows.push(row);
    console.log(
      " →",
      row.hotelVerdict,
      row.pdf.sha256.slice(0, 16),
      "E",
      row.matrix.evidenceValidation,
      "C",
      row.matrix.contentIntegrity,
      "H",
      row.matrix.hyperlinkValidation,
      "V",
      row.matrix.visualPdfQuality
    );
  }

  const passCount = rows.filter((r) => r.hotelVerdict === "PASS").length;
  const report = {
    contract: "AVA_FINAL_FIVE_HOTEL_PDF_CERTIFICATION_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    publicationAuthorized: false,
    customerDistributionAuthorized: false,
    hotels: rows,
    totals: {
      hotels: rows.length,
      pass: passCount,
      fail: rows.length - passCount,
    },
    cohortVerdict: passCount === rows.length ? "PASS" : "FAIL",
  };

  fs.mkdirSync(REPORT_DIR, { recursive: true });
  const outJson = path.join(
    REPORT_DIR,
    "AVA_FINAL_FIVE_HOTEL_PDF_CERTIFICATION_RESULTS.json"
  );
  const outMd = path.join(REPORT_DIR, "AVA_FINAL_FIVE_HOTEL_PDF_CERTIFICATION.md");
  fs.writeFileSync(outJson, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(outMd, renderMarkdown(report));
  console.log(
    JSON.stringify(
      {
        cohortVerdict: report.cohortVerdict,
        totals: report.totals,
        outMd: path.relative(ROOT, outMd).replace(/\\/g, "/"),
        outJson: path.relative(ROOT, outJson).replace(/\\/g, "/"),
        hotels: rows.map((r) => ({
          name: r.name,
          verdict: r.hotelVerdict,
          sha256: r.pdf.sha256,
        })),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
