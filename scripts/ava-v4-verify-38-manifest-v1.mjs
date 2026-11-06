/**
 * Verify all 38 certified V4 PDFs against AVA_38_HOTEL_FINAL_MANIFEST.json.
 * Does not regenerate PDFs.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";
import { buildAiVisibilityAuditPdfHtmlV4 } from "../lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v4.js";
import { detectPdfPresentationDefects } from "../lib/ai-visibility-audit-action-plan/customer-quality-assurance-v1.js";
import { applyCanonicalActionRegisterToPayload } from "../lib/ai-visibility-audit-action-plan/canonical-action-register-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const MANIFEST = path.join(
  REPORT,
  process.env.AVA_MANIFEST_FILE || "AVA_38_HOTEL_FINAL_MANIFEST.json"
);
const PAYLOAD_DIR = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/payloads"
);

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

const manifest = JSON.parse(fs.readFileSync(MANIFEST, "utf8"));
const results = [];
let mismatches = 0;

for (const h of manifest.hotels || []) {
  const abs = path.join(ROOT, h.pdfPath);
  const row = {
    hotelName: h.hotelName,
    propertyId: h.propertyId,
    pdfPath: h.pdfPath,
    ok: true,
    issues: [],
  };
  if (!fs.existsSync(abs)) {
    row.ok = false;
    row.issues.push("PDF_MISSING");
    mismatches += 1;
    results.push(row);
    continue;
  }
  const buf = fs.readFileSync(abs);
  const hash = sha256(buf);
  row.sha256 = hash;
  row.manifestSha256 = h.pdfSha256;
  if (hash !== h.pdfSha256) {
    row.ok = false;
    row.issues.push("SHA_MISMATCH");
    mismatches += 1;
  }
  const doc = await getDocument({
    data: new Uint8Array(buf),
    useSystemFonts: true,
  }).promise;
  row.pageCount = doc.numPages;
  if (h.pageCount && doc.numPages !== h.pageCount) {
    row.ok = false;
    row.issues.push(`PAGE_COUNT_MISMATCH:${doc.numPages}!=${h.pageCount}`);
    mismatches += 1;
  }
  let text = "";
  let links = 0;
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const tc = await page.getTextContent();
    text += tc.items.map((x) => x.str).join(" ");
    const annots = await page.getAnnotations();
    for (const a of annots) {
      if (a.subtype === "Link" && (a.url || a.unsafeUrl)) links += 1;
    }
  }
  if (!text.includes(String(h.hotelName).slice(0, 18))) {
    row.ok = false;
    row.issues.push("HOTEL_NAME_NOT_IN_PDF");
    mismatches += 1;
  }
  if (/\[object Object\]/i.test(text)) {
    row.ok = false;
    row.issues.push("OBJECT_OBJECT");
    mismatches += 1;
  }
  if (
    /CURRENT_CANONICAL_OR_OFFICIAL|HISTORICAL_OR_LEGACY_DOMAIN|THIRD_PARTY_OR_UNRESOLVED/.test(
      text
    )
  ) {
    row.ok = false;
    row.issues.push("INTERNAL_DESTINATION_ENUM");
    mismatches += 1;
  }
  const lastPage = await doc.getPage(doc.numPages);
  const lastText = (await lastPage.getTextContent()).items
    .map((x) => x.str)
    .join(" ");
  if (lastText.length < 400) {
    row.ok = false;
    row.issues.push("NEAR_EMPTY_FINAL_PAGE");
    mismatches += 1;
  }
  row.linkCount = links;

  if (h.file && fs.existsSync(path.join(PAYLOAD_DIR, h.file))) {
    const raw = JSON.parse(
      fs.readFileSync(path.join(PAYLOAD_DIR, h.file), "utf8")
    );
    delete raw.customerActionRegister;
    const { register } = applyCanonicalActionRegisterToPayload(raw);
    const gate = detectPdfPresentationDefects(raw, {
      buildHtml: buildAiVisibilityAuditPdfHtmlV4,
    });
    const crit = gate.defects.filter((d) => d.severity === "CRITICAL");
    if (crit.length) {
      row.ok = false;
      row.issues.push(`PDF_GATE_CRITICAL:${crit.length}`);
      mismatches += 1;
    }
    for (const a of register.actions.filter((x) =>
      /HIGH|CRITICAL/i.test(x.priority)
    )) {
      const needle = String(a.actionTitle || "").slice(0, 22);
      if (needle && !text.includes(needle)) {
        row.ok = false;
        row.issues.push(`MISSING_HIGH:${a.actionId}`);
        mismatches += 1;
      }
    }
  }
  results.push(row);
  console.log(
    `${row.ok ? "OK" : "FAIL"} ${h.hotelName} ${row.issues.join(",") || ""}`
  );
}

const out = {
  contract: "AVA_V4_MANIFEST_VERIFY_V1",
  generatedAt: new Date().toISOString(),
  total: results.length,
  matched: results.filter((r) => r.ok).length,
  mismatches,
  reusedUnchanged: mismatches === 0,
  results,
};
fs.writeFileSync(
  path.join(REPORT, "AVA_V4_MANIFEST_VERIFY_RESULTS.json"),
  `${JSON.stringify(out, null, 2)}\n`
);
console.log(
  JSON.stringify(
    {
      total: out.total,
      matched: out.matched,
      mismatches: out.mismatches,
      reusedUnchanged: out.reusedUnchanged,
    },
    null,
    2
  )
);
if (mismatches) process.exit(2);
