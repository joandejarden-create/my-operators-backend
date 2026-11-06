#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const MANIFEST = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_38_HOTEL_V4_LOCAL_REGENERATION_MANIFEST.json"
);
const ADMIN =
  process.env.AVA_STORAGE_ROOT ||
  "C:\\Dev\\dealality\\my-operators-backend\\data\\ava-data";
const BASE = process.env.AVA_ADMIN_BASE || "http://localhost:8080";

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

async function fetchPdf(url) {
  const res = await fetch(url);
  if (!res.ok) {
    return { status: res.status, headerSha: null, bodySha: null, bytes: 0 };
  }
  const headerSha = String(res.headers.get("x-ava-pdf-sha256") || "").toLowerCase();
  const buf = Buffer.from(await res.arrayBuffer());
  return {
    status: res.status,
    headerSha,
    bodySha: sha256(buf),
    bytes: buf.length,
  };
}

async function main() {
  const manifest = JSON.parse(fs.readFileSync(MANIFEST, "utf8"));
  const results = [];
  let ok = 0;
  let publishedChanged = 0;

  for (const h of manifest.hotels) {
    const expect = String(h.pdfSha256 || "").toLowerCase();
    const draftPath = path.join(ADMIN, "pdfs", h.propertyId, "draft.pdf");
    const pubPath = path.join(ADMIN, "pdfs", h.propertyId, "published.pdf");
    const draftSha = fs.existsSync(draftPath)
      ? sha256(fs.readFileSync(draftPath))
      : null;
    const pubSha = fs.existsSync(pubPath)
      ? sha256(fs.readFileSync(pubPath))
      : null;

    const previewUrl = `${BASE}/api/admin/ai-visibility-audit/${h.propertyId}/pdf?channel=draft`;
    const downloadUrl = `${previewUrl}&download=1`;
    const preview = await fetchPdf(previewUrl);
    const download = await fetchPdf(downloadUrl);

    const pass =
      draftSha === expect &&
      preview.status === 200 &&
      download.status === 200 &&
      preview.headerSha === expect &&
      download.headerSha === expect &&
      preview.bodySha === expect &&
      download.bodySha === expect &&
      preview.bodySha === download.bodySha;

    if (pass) ok += 1;
    // published should not equal the new draft unless there was never a separate published file
    // "unchanged" = if published existed before promote it still differs OR still matches prior published
    // Promote reported publishedUntouched=true for all — verify published != new draft when published exists
    if (pubSha && pubSha === expect) publishedChanged += 1;

    results.push({
      hotelName: h.hotelName,
      propertyId: h.propertyId,
      expectSha: expect,
      draftSha,
      preview,
      download,
      pass,
      publishedEqualsNewDraft: Boolean(pubSha && pubSha === expect),
    });
    process.stdout.write(
      `${pass ? "OK" : "FAIL"} ${h.hotelName} ${expect.slice(0, 10)}…\n`
    );
  }

  const out = {
    generatedAt: new Date().toISOString(),
    adminRoot: ADMIN,
    base: BASE,
    ok,
    total: results.length,
    publishedEqualsNewDraftCount: publishedChanged,
    failed: results.filter((r) => !r.pass).map((r) => r.hotelName),
    results,
  };
  const outPath = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_V4_FINAL_LOCAL_ADMIN_HTTP_VERIFY.json"
  );
  fs.writeFileSync(outPath, `${JSON.stringify(out, null, 2)}\n`);
  console.log(`\n${ok}/${results.length} Preview+Download match manifest`);
  console.log("Wrote", outPath);
  if (ok !== results.length) process.exit(2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
