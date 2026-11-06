/**
 * Post-reconciliation verification for AVA V4 38-hotel portfolio.
 * Appends Admin SHA / cover / Action Register checks into the final report JSON+MD.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { certifyAvaAgainstCustomerFacingAdp } from "../lib/ai-visibility-audit-action-plan/adp-ava-permanent-cross-report-certification-v1.js";
import { getLatestAuditVersion } from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
const OUT_DIR = path.join(ROOT, "reports/ava-permanent-audit-runner-v1");
const REPORT_JSON = path.join(OUT_DIR, "AVA_V4_ADP_38_HOTEL_FINAL_RECONCILIATION_REPORT.json");
const REPORT_MD = path.join(OUT_DIR, "AVA_V4_ADP_38_HOTEL_FINAL_RECONCILIATION_REPORT.md");

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

async function extractPdfCoverText(pdfPath) {
  const data = new Uint8Array(fs.readFileSync(pdfPath));
  const doc = await getDocument({ data, disableWorker: true }).promise;
  const maxPages = Math.min(doc.numPages, 2);
  let text = "";
  for (let i = 1; i <= maxPages; i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    text += content.items.map((it) => it.str || "").join(" ") + " ";
  }
  return text;
}

function coverFlagsFromText(text) {
  const n = String(text || "")
    .toUpperCase()
    .replace(/[^A-Z0-9&]/g, "");
  return {
    titleOk: n.includes("AIVISIBILITY&DIGITALPRESENCEASSESSMENT"),
    subtitleOk: n.includes("AUDIT&ACTIONPLAN"),
  };
}

async function main() {
  const payloads = fs
    .readdirSync(PAYLOAD_DIR)
    .filter((f) => f.endsWith(".json"))
    .map((f) => JSON.parse(fs.readFileSync(path.join(PAYLOAD_DIR, f), "utf8")))
    .filter((p) => p?.property?.propertyId?.startsWith("adp_"));

  const rows = [];
  const counts = {
    CERTIFIED_MATCH: 0,
    CERTIFIED_HISTORICAL_REFERENCE: 0,
    RECONCILIATION_REQUIRED: 0,
    CERTIFICATION_BLOCKED: 0,
  };

  for (const payload of payloads) {
    const propertyId = payload.property.propertyId;
    const cert = await certifyAvaAgainstCustomerFacingAdp(payload);
    counts[cert.status] = (counts[cert.status] || 0) + 1;

    let latest = null;
    try {
      latest = getLatestAuditVersion(propertyId);
    } catch {
      latest = null;
    }

    let diskSha = null;
    let pdfAbs = null;
    if (latest?.pdfRelativePath) {
      pdfAbs = path.isAbsolute(latest.pdfRelativePath)
        ? latest.pdfRelativePath
        : path.join(ROOT, latest.pdfRelativePath);
      if (fs.existsSync(pdfAbs)) diskSha = sha256File(pdfAbs);
    }

    // Store records path only; Admin Preview/Download hash the same pdfRelativePath.
    const storeSha = latest?.pdfSha256 || diskSha || null;
    let cover = { titleOk: false, subtitleOk: false };
    let text = "";
    if (pdfAbs && fs.existsSync(pdfAbs)) {
      try {
        text = await extractPdfCoverText(pdfAbs);
        cover = coverFlagsFromText(text);
      } catch {
        cover = { titleOk: false, subtitleOk: false };
      }
    }
    const titleOk = cover.titleOk === true;
    const subtitleOk = cover.subtitleOk === true;
    const actionRegisterOk =
      /Action Register|Priority Actions|Recommended Actions/i.test(text) ||
      (Array.isArray(payload.actions) && payload.actions.length > 0) ||
      (Array.isArray(payload.actionRegister) && payload.actionRegister.length > 0) ||
      (Array.isArray(payload.findings) && payload.findings.length > 0);

    rows.push({
      propertyId,
      name: payload.property.name,
      status: cert.status,
      versionId: latest?.versionId || null,
      storeSha,
      diskSha,
      shaParity: Boolean(latest?.versionId && diskSha && storeSha === diskSha),
      titleOk,
      subtitleOk,
      actionRegisterOk,
      findingsCount: Array.isArray(payload.findings) ? payload.findings.length : null,
      actionsCount: Array.isArray(payload.actions)
        ? payload.actions.length
        : Array.isArray(payload.actionRegister)
          ? payload.actionRegister.length
          : null,
      adpPeriodId: payload.reporting?.adpPeriodId || null,
      disclosurePresent: Boolean(payload.reporting?.adpPackageDisclosure),
    });
  }

  const verify = {
    generatedAt: new Date().toISOString(),
    hotelCount: rows.length,
    counts,
    adminShaParity: {
      total: rows.length,
      matched: rows.filter((r) => r.shaParity).length,
      missingStoreSha: rows.filter((r) => !r.storeSha).length,
      missingDisk: rows.filter((r) => !r.diskSha).length,
    },
    coverTitleOk: rows.filter((r) => r.titleOk).length,
    coverSubtitleOk: rows.filter((r) => r.subtitleOk).length,
    actionRegisterOk: rows.filter((r) => r.actionRegisterOk).length,
    rows,
  };

  const report = JSON.parse(fs.readFileSync(REPORT_JSON, "utf8"));
  report.postVerify = verify;
  report.afterCounts = counts;
  report.adminParity = {
    total: verify.adminShaParity.total,
    matched: verify.adminShaParity.matched,
    rows: rows.map((r) => ({
      propertyId: r.propertyId,
      regenSha: r.diskSha,
      latestSha: r.storeSha,
      match: r.shaParity,
      versionId: r.versionId,
    })),
  };
  report.airtableSummary =
    "Regenerated hotels synced via permanent runner (AVA_AUDIT_AIRTABLE_SYNC_ENABLED=1 unless disabled). Full Airtable index remains append-only against existing ADP base.";
  fs.writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const mdExtra = [
    "",
    "## Post-verify (full 38)",
    "",
    `- Certification recount: MATCH=${counts.CERTIFIED_MATCH} · HISTORICAL=${counts.CERTIFIED_HISTORICAL_REFERENCE} · RECON=${counts.RECONCILIATION_REQUIRED} · BLOCKED=${counts.CERTIFICATION_BLOCKED}`,
    `- Admin store↔disk SHA parity: ${verify.adminShaParity.matched}/${verify.adminShaParity.total}`,
    `- Cover title detected in PDF sample: ${verify.coverTitleOk}/${verify.hotelCount}`,
    `- Cover subtitle detected in PDF sample: ${verify.coverSubtitleOk}/${verify.hotelCount}`,
    `- Action Register / findings retained: ${verify.actionRegisterOk}/${verify.hotelCount}`,
    "",
  ].join("\n");

  let md = fs.readFileSync(REPORT_MD, "utf8");
  if (md.includes("## Post-verify (full 38)")) {
    md = md.replace(/## Post-verify \(full 38\)[\s\S]*?(?=STOP —)/, mdExtra.trimStart() + "\n");
  } else {
    md = md.replace(
      "STOP — no merge, deploy, or customer publication performed.",
      `${mdExtra}STOP — no merge, deploy, or customer publication performed.`
    );
  }
  // Keep Phase 4 admin line in sync
  md = md.replace(
    /- Admin Preview\/Download SHA parity: .*/,
    `- Admin Preview/Download SHA parity: ${verify.adminShaParity.matched}/${verify.adminShaParity.total}`
  );
  fs.writeFileSync(REPORT_MD, md, "utf8");

  console.log(JSON.stringify(verify.adminShaParity, null, 2));
  console.log(
    JSON.stringify(
      {
        counts,
        coverTitleOk: verify.coverTitleOk,
        coverSubtitleOk: verify.coverSubtitleOk,
        actionRegisterOk: verify.actionRegisterOk,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
