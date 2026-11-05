/**
 * Local verification: AVA PDF filenames + secured download delivery for the six audits.
 * Does not deploy or publish.
 *
 *   node scripts/test-ava-pdf-download-filename-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildAiVisibilityAuditPdfFilename,
  buildAvaPdfFilenameFromVersion,
  contentDispositionForAvaPdf,
  formatAvaPdfIssueMonthYear,
} from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import {
  resolveAvaPdfFileForDelivery,
  assertSafeAvaPropertyId,
  assertSafeAvaVersionId,
} from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  getLatestAuditVersion,
  loadAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const COHORT = [
  {
    propertyId: "adp_bethesda_marriott",
    expectName: "Bethesda Marriott",
  },
  {
    propertyId: "adp_waterstone_boca_raton",
    expectName: "Waterstone Resort & Marina",
  },
  {
    propertyId: "adp_castillo_hotel_son_vida",
    expectName: "Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca",
  },
  {
    propertyId: "adp_sheraton_mallorca_arabella_golf",
    expectName: "Sheraton Mallorca Arabella Golf Hotel",
  },
  {
    propertyId: "adp_wyndham_san_diego_bayside",
    expectName: "Wyndham San Diego Bayside",
  },
  {
    propertyId: "adp_spice_island_beach_resort",
    expectName: "Spice Island Beach Resort",
  },
];

function sha(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

const checks = [];
function check(name, ok, detail) {
  checks.push({ name, ok: !!ok, detail: detail || null });
}

// Filename unit cases
check(
  "month_token_from_label",
  formatAvaPdfIssueMonthYear("NOVEMBER 2026") === "NOVEMBER2026"
);
check(
  "filename_bethesda",
  buildAiVisibilityAuditPdfFilename({
    hotelName: "Bethesda Marriott",
    reportMonthLabel: "NOVEMBER 2026",
  }) ===
    "Dealality - AI Visibility Audit & Action Plan - Bethesda Marriott - NOVEMBER2026.pdf"
);
check(
  "filename_waterstone_ampersand",
  buildAiVisibilityAuditPdfFilename({
    hotelName: "Waterstone Resort & Marina",
    reportMonthLabel: "NOVEMBER 2026",
  }) ===
    "Dealality - AI Visibility Audit & Action Plan - Waterstone Resort & Marina - NOVEMBER2026.pdf"
);
check(
  "filename_accented_safe",
  !/[<>:"/\\|?*]/.test(
    buildAiVisibilityAuditPdfFilename({
      hotelName: "Hôtel A Coruña — Castellón",
      reportMonthLabel: "NOVEMBER 2026",
    })
  ) &&
    buildAiVisibilityAuditPdfFilename({
      hotelName: "Hôtel A Coruña — Castellón",
      reportMonthLabel: "NOVEMBER 2026",
    }).includes("Hôtel A Coruña")
);
check(
  "filename_no_property_id",
  !buildAiVisibilityAuditPdfFilename({
    hotelName: "Bethesda Marriott",
    reportMonthLabel: "NOVEMBER 2026",
  }).includes("adp_")
);
check(
  "filename_long_stable",
  buildAiVisibilityAuditPdfFilename({
    hotelName:
      "Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca",
    reportMonthLabel: "NOVEMBER 2026",
  }).endsWith("- NOVEMBER2026.pdf")
);
const cd = contentDispositionForAvaPdf(
  "Dealality - AI Visibility Audit & Action Plan - Bethesda Marriott - NOVEMBER2026.pdf",
  { disposition: "attachment" }
);
check("content_disposition_attachment", cd.startsWith("attachment;"));
check("content_disposition_utf8", cd.includes("filename*=UTF-8''"));

try {
  assertSafeAvaPropertyId("../evil");
  check("reject_traversal_property", false);
} catch {
  check("reject_traversal_property", true);
}
try {
  assertSafeAvaVersionId("../../etc/passwd");
  check("reject_traversal_version", false);
} catch {
  check("reject_traversal_version", true);
}

const hotelResults = [];
for (const row of COHORT) {
  const meta = getAuditMeta(row.propertyId);
  const latest = getLatestAuditVersion(row.propertyId);
  const filename = buildAvaPdfFilenameFromVersion(latest, meta || {});
  const file = resolveAvaPdfFileForDelivery({
    propertyId: row.propertyId,
    versionId: latest?.versionId || null,
    pdfRelativePath: latest?.pdfRelativePath || meta?.pdfRelativePath,
  });
  const bytes = file?.abs ? fs.readFileSync(file.abs) : null;
  const isPdf = bytes && bytes.slice(0, 4).toString() === "%PDF";
  const hotelOk =
    filename.includes(row.expectName.split(",")[0]) ||
    filename.includes(row.expectName);
  hotelResults.push({
    propertyId: row.propertyId,
    hotelName: latest?.payload?.property?.name || meta?.name || row.expectName,
    filename,
    reportMonthLabel: latest?.payload?.reporting?.reportMonthLabel || null,
    pdfExists: !!file?.abs,
    pdfSha256: file?.sha256 || null,
    bytesMatchHeader: !!isPdf,
    filenameStable: filename === buildAvaPdfFilenameFromVersion(latest, meta || {}),
    noInternalIds:
      !filename.includes(row.propertyId) &&
      !(latest?.versionId && filename.includes(latest.versionId)),
  });
  check(`hotel_filename_${row.propertyId}`, hotelOk && hotelResults.at(-1).noInternalIds, filename);
  check(`hotel_pdf_${row.propertyId}`, !!file?.abs && isPdf, file?.abs);
  // Historical: re-resolve latest version id explicitly
  if (latest?.versionId) {
    const hist = resolveAvaPdfFileForDelivery({
      propertyId: row.propertyId,
      versionId: latest.versionId,
      pdfRelativePath: latest.pdfRelativePath || meta?.pdfRelativePath,
    });
    check(
      `hotel_hist_${row.propertyId}`,
      !!hist?.abs && hist.sha256 === file.sha256,
      latest.versionId
    );
  }
}

const missing = resolveAvaPdfFileForDelivery({
  propertyId: "adp_ac_hotel_a_coruna",
  pdfRelativePath: null,
});
check("missing_pdf_null", missing == null);

const out = {
  ok: checks.every((c) => c.ok),
  checks,
  hotelResults,
  failed: checks.filter((c) => !c.ok).map((c) => c.name),
};
const report = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_PDF_DOWNLOAD_VERIFICATION.json"
);
fs.mkdirSync(path.dirname(report), { recursive: true });
fs.writeFileSync(report, `${JSON.stringify(out, null, 2)}\n`);
console.log(
  JSON.stringify(
    {
      ok: out.ok,
      failed: out.failed,
      filenames: hotelResults.map((h) => h.filename),
      report,
    },
    null,
    2
  )
);
process.exit(out.ok ? 0 : 1);
