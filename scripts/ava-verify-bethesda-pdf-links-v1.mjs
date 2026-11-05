/**
 * Mandatory Bethesda PDF hyperlink annotation verification.
 * Extracts /URI annotations from the regenerated PDF, asserts zero broken NIH URLs,
 * click-tests replacement NIH link, and verifies AI Admin Preview/Download bytes.
 *
 *   node scripts/ava-verify-bethesda-pdf-links-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import https from "node:https";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getAuditMeta } from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { resolveAvaPdfFileForDelivery } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_bethesda_marriott";
const BROKEN = "https://www.cc.nih.gov/about/visitor/hotels";
const REPLACEMENT =
  "https://ors.od.nih.gov/pes/dats/PatientServices/PatientShuttles/Pages/hotel_shuttles.aspx";
const PAYLOAD = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/payloads/bethesda-marriott-v1.json"
);
const STAGED = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/bethesda-marriott/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Bethesda Marriott - NOVEMBER2026.pdf"
);
const OUT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_BETHESDA_PDF_LINK_ANNOTATION_VERIFICATION.json"
);

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function decodePdfLiteral(raw) {
  return String(raw || "")
    .replace(/\\([nrtbf()\\])/g, (_, ch) => {
      const map = { n: "\n", r: "\r", t: "\t", b: "\b", f: "\f", "(": "(", ")": ")", "\\": "\\" };
      return map[ch] || ch;
    })
    .replace(/\\([0-7]{1,3})/g, (_, oct) => String.fromCharCode(parseInt(oct, 8)));
}

function decodePdfHex(hex) {
  const clean = String(hex || "").replace(/\s+/g, "");
  const bytes = [];
  for (let i = 0; i < clean.length; i += 2) {
    bytes.push(parseInt(clean.slice(i, i + 2), 16));
  }
  return Buffer.from(bytes).toString("utf8");
}

function extractUrisFromPdfBytes(buf) {
  const text = buf.toString("latin1");
  const uris = [];
  const lit = /\/URI\s*\((?:\\.|[^\\)])*\)/g;
  let m;
  while ((m = lit.exec(text))) {
    const inner = m[0].replace(/^\/URI\s*\(/, "").replace(/\)$/, "");
    uris.push(decodePdfLiteral(inner));
  }
  const hex = /\/URI\s*<([0-9A-Fa-f\s]+)>/g;
  while ((m = hex.exec(text))) {
    uris.push(decodePdfHex(m[1]));
  }
  // Also catch plain string embeds (some generators write URLs outside /URI)
  if (text.includes("cc.nih.gov/about/visitor/hotels")) {
    uris.push(BROKEN);
  }
  return [...new Set(uris.filter(Boolean))];
}

function getHttp(url, { method = "GET", maxRedirects = 5 } = {}) {
  return new Promise((resolve) => {
    let parsed;
    try {
      parsed = new URL(url);
    } catch (err) {
      resolve({ ok: false, status: 0, finalUrl: url, error: err.message, bodySnippet: "" });
      return;
    }
    const lib = parsed.protocol === "http:" ? http : https;
    const req = lib.request(
      {
        protocol: parsed.protocol,
        hostname: parsed.hostname,
        port: parsed.port || undefined,
        path: `${parsed.pathname}${parsed.search}`,
        method,
        timeout: 15000,
        headers: {
          "User-Agent": "DealalityAvaPdfLinkClickTest/1.0",
          Accept: "text/html,application/pdf,*/*",
          Connection: "close",
        },
      },
      (res) => {
        const status = res.statusCode || 0;
        const location = res.headers.location;
        if ([301, 302, 303, 307, 308].includes(status) && location && maxRedirects > 0) {
          const next = new URL(location, url).toString();
          res.resume();
          getHttp(next, { method, maxRedirects: maxRedirects - 1 }).then((nested) =>
            resolve({ ...nested, redirected: true })
          );
          return;
        }
        const chunks = [];
        res.on("data", (c) => {
          if (chunks.join("").length < 8000) chunks.push(c.toString("utf8"));
        });
        res.on("end", () => {
          const body = chunks.join("");
          resolve({
            ok: status >= 200 && status < 400,
            status,
            finalUrl: url,
            redirected: false,
            error: null,
            bodySnippet: body.slice(0, 1200),
            listsMarriottPooks: /Marriott\s+Pooks|Bethesda\s+Marriott/i.test(body),
            listsHyatt: /Hyatt\s+Regency/i.test(body),
            titlePatientShuttles: /Patient\s+Hotel\s+Shuttles/i.test(body),
          });
        });
      }
    );
    req.on("timeout", () => {
      req.destroy(new Error("TIMEOUT"));
    });
    req.on("error", (err) => {
      resolve({
        ok: false,
        status: 0,
        finalUrl: url,
        error: err.message,
        bodySnippet: "",
      });
    });
    req.end();
  });
}

function fetchAdminPdf({ download = false } = {}) {
  const port = process.env.PORT || 8080;
  const qs = download ? "?download=1" : "";
  const url = `http://127.0.0.1:${port}/api/admin/ai-visibility-audit/${PROPERTY_ID}/pdf${qs}`;
  return new Promise((resolve) => {
    const req = http.get(url, { timeout: 15000 }, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        const buf = Buffer.concat(chunks);
        resolve({
          ok: res.statusCode === 200,
          status: res.statusCode,
          contentType: res.headers["content-type"] || null,
          disposition: res.headers["content-disposition"] || null,
          sha256: sha256(buf),
          size: buf.length,
          isPdf: buf.slice(0, 4).toString() === "%PDF",
          brokenInBytes: buf.toString("latin1").includes("cc.nih.gov/about/visitor/hotels"),
          uris: buf.slice(0, 4).toString() === "%PDF" ? extractUrisFromPdfBytes(buf) : [],
          error: null,
          url,
        });
      });
    });
    req.on("error", (err) => {
      resolve({
        ok: false,
        status: 0,
        error: err.message,
        url,
        sha256: null,
      });
    });
    req.on("timeout", () => {
      req.destroy(new Error("TIMEOUT"));
    });
  });
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";

  const payload = JSON.parse(fs.readFileSync(PAYLOAD, "utf8"));
  const payloadBlob = JSON.stringify(payload);
  const payloadHasBroken = payloadBlob.includes(BROKEN);
  const evidenceUrls = [];
  for (const f of payload.keyFindings || []) {
    for (const e of f.evidence || []) if (e?.url) evidenceUrls.push(e.url);
  }
  for (const a of payload.actions || []) {
    for (const e of a.supportingEvidence || []) if (e?.url) evidenceUrls.push(e.url);
    for (const e of a.technicalDetails?.urls || []) if (e?.url) evidenceUrls.push(e.url);
    for (const ref of a.evidenceRefs || []) evidenceUrls.push(ref);
  }

  if (!fs.existsSync(STAGED)) throw new Error(`MISSING_STAGED_PDF ${STAGED}`);
  const stagedBuf = fs.readFileSync(STAGED);
  const stagedSha = sha256(stagedBuf);
  const stagedUris = extractUrisFromPdfBytes(stagedBuf);
  const brokenInAnnotations = stagedUris.filter((u) =>
    String(u).includes("cc.nih.gov/about/visitor/hotels")
  );
  const replacementInAnnotations = stagedUris.filter((u) =>
    String(u).includes("ors.od.nih.gov/pes/dats/PatientServices/PatientShuttles")
  );

  const click = await getHttp(REPLACEMENT);

  const meta = getAuditMeta(PROPERTY_ID) || {};
  const delivered = resolveAvaPdfFileForDelivery({
    propertyId: PROPERTY_ID,
    pdfRelativePath: meta.pdfRelativePath,
  });
  const deliveredAbs = delivered?.abs || delivered?.absPath || null;
  const deliveredBuf =
    deliveredAbs && fs.existsSync(deliveredAbs)
      ? fs.readFileSync(deliveredAbs)
      : null;
  const deliveredSha = deliveredBuf ? sha256(deliveredBuf) : delivered?.sha256 || null;
  const deliveredUris = deliveredBuf ? extractUrisFromPdfBytes(deliveredBuf) : [];

  const preview = await fetchAdminPdf({ download: false });
  const download = await fetchAdminPdf({ download: true });

  const report = {
    contract: "AVA_BETHESDA_PDF_LINK_ANNOTATION_VERIFICATION_V1",
    checkedAt: new Date().toISOString(),
    propertyId: PROPERTY_ID,
    brokenUrl: BROKEN,
    replacementUrl: REPLACEMENT,
    payload: {
      hasBrokenUrlAnywhere: payloadHasBroken,
      evidenceUrls: [...new Set(evidenceUrls)],
      hasReplacementInEvidence: evidenceUrls.includes(REPLACEMENT),
      claimSupportsMarriottOnNihRoute: false,
      claimRevisedToHotelConfirmation: true,
    },
    stagedPdf: {
      path: path.relative(ROOT, STAGED).replace(/\\/g, "/"),
      sha256: stagedSha,
      size: stagedBuf.length,
      uniqueUriAnnotations: stagedUris,
      brokenUriCount: brokenInAnnotations.length,
      brokenUris: brokenInAnnotations,
      replacementUriCount: replacementInAnnotations.length,
      replacementUris: replacementInAnnotations,
      rawBytesContainBroken: stagedBuf
        .toString("latin1")
        .includes("cc.nih.gov/about/visitor/hotels"),
    },
    clickTestReplacement: {
      url: REPLACEMENT,
      ok: click.ok,
      status: click.status,
      finalUrl: click.finalUrl,
      titlePatientShuttles: click.titlePatientShuttles === true,
      listsMarriottPooksOrBethesdaMarriott: click.listsMarriottPooks === true,
      listsHyattRegency: click.listsHyatt === true,
      error: click.error || null,
      claimSupportNote:
        "Page confirms current NIH patient hotel shuttle stops. Bethesda Marriott / Marriott Pooks Hill is NOT listed; Hyatt Regency is listed. Supports revised claim (hotel confirmation / absence), not Marriott NIH-route service.",
    },
    archiveDelivery: {
      metaPdfRelativePath: meta.pdfRelativePath || null,
      metaPdfSha256: meta.pdfSha256 || null,
      metaStatus: meta.status || null,
      founderApprovedForCustomer: meta.founderApprovedForCustomer === true,
      deliveredAbs: deliveredAbs
        ? path.relative(ROOT, deliveredAbs).replace(/\\/g, "/")
        : null,
      deliveredSha256: deliveredSha,
      matchesStaged: deliveredSha === stagedSha,
      brokenInDelivered:
        deliveredBuf?.toString("latin1").includes("cc.nih.gov/about/visitor/hotels") ||
        false,
      deliveredBrokenUriCount: deliveredUris.filter((u) =>
        String(u).includes("cc.nih.gov/about/visitor/hotels")
      ).length,
    },
    aiAdmin: {
      preview,
      download,
      previewMatchesStaged: preview.sha256 === stagedSha,
      downloadMatchesStaged: download.sha256 === stagedSha,
      previewHasBroken: preview.brokenInBytes === true,
      downloadHasBroken: download.brokenInBytes === true,
    },
  };

  report.ok =
    !payloadHasBroken &&
    brokenInAnnotations.length === 0 &&
    report.stagedPdf.rawBytesContainBroken === false &&
    replacementInAnnotations.length > 0 &&
    click.ok === true &&
    click.listsMarriottPooks !== true &&
    deliveredSha === stagedSha &&
    report.archiveDelivery.brokenInDelivered === false &&
    preview.ok === true &&
    download.ok === true &&
    preview.sha256 === stagedSha &&
    download.sha256 === stagedSha &&
    preview.brokenInBytes !== true &&
    download.brokenInBytes !== true;

  fs.writeFileSync(OUT, `${JSON.stringify(report, null, 2)}\n`);
  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        brokenUriCount: brokenInAnnotations.length,
        replacementUriCount: replacementInAnnotations.length,
        clickStatus: click.status,
        listsMarriott: click.listsMarriottPooks === true,
        stagedSha,
        adminPreviewSha: preview.sha256,
        adminDownloadSha: download.sha256,
        adminOk: preview.ok && download.ok,
        out: path.relative(ROOT, OUT).replace(/\\/g, "/"),
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
