/**
 * Re-capture IHG development/ folder pages with fixed HTML + MHTML + PDF.
 *   node scripts/recapture-ihg-development-html.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import puppeteer from "puppeteer";
import { captureHtmlWithBrowser } from "../lib/partner-intelligence/harvest-browser-capture.js";
import { resolveReferenceRoot } from "../lib/partner-intelligence/reference-material-paths.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const APPLY = process.argv.includes("--apply");
const COMPANY = "IHG Hotels & Resorts";
const BASE = "https://development.ihg.com";

const PAGES = [
  { url: `${BASE}/`, title: "IHG development home" },
  { url: `${BASE}/resources`, title: "Development resources library" },
  { url: `${BASE}/hotel-development/getting-started`, title: "Getting started owner lifecycle" },
  { url: `${BASE}/hotel-development/owner-value`, title: "Owner value hub" },
  { url: `${BASE}/hotel-development/owner-value/hotel-lifecycle-management`, title: "Hotel lifecycle management" },
  { url: `${BASE}/hotel-development/owner-value/powerful-loyalty-programme`, title: "IHG One Rewards owner value" },
  { url: `${BASE}/hotel-development/owner-value/effective-revenue-delivery`, title: "Effective revenue delivery" },
  { url: `${BASE}/hotel-development/owner-value/strong-and-distinct-brands`, title: "Strong and distinct brands" },
  { url: `${BASE}/hotel-development/owner-value/powerful-procurement`, title: "Powerful procurement" },
  { url: `${BASE}/hotel-development/owner-value/digital-advantage`, title: "Digital advantage" },
  { url: `${BASE}/hotel-development/owner-value/sustainable-business`, title: "Sustainable business" },
  { url: `${BASE}/regions/americas`, title: "Americas development region CALA" },
  { url: `${BASE}/regions/emeaa`, title: "EMEAA development region" },
  { url: `${BASE}/regions/greater-china`, title: "Greater China development region" },
  { url: `${BASE}/contact-us`, title: "Contact development team" },
  { url: `${BASE}/hotel-brands`, title: "Hotel brands portfolio" },
  { url: "https://www.ihgplc.com/en/investors/annual-report", title: "IHG investor annual report hub" },
];

const OBSOLETE = [
  "Compare our brands.html",
  "Americas development region.html",
  "Getting started — owner lifecycle support.html",
  "Getting started owner lifecycle support.html",
  "a-sustainable-business.html",
];

function writeDevelopmentFolderIndex(devDir, captures) {
  const rows = captures
    .map((c) => {
      const base = path.basename(c.relativePath || "", ".html");
      const mhtml = c.mhtmlRelativePath ? path.basename(c.mhtmlRelativePath) : `${base} (archive).mhtml`;
      const pdf = c.pdfRelativePath ? path.basename(c.pdfRelativePath) : `${base} (snapshot).pdf`;
      const html = path.basename(c.relativePath || `${base}.html`);
      return `<tr>
        <td>${escapeHtml(c.title || base)}</td>
        <td><a href="${escapeAttr(mhtml)}">MHTML (offline)</a></td>
        <td><a href="${escapeAttr(pdf)}">PDF</a></td>
        <td><a href="${escapeAttr(html)}">HTML</a></td>
      </tr>`;
    })
    .join("\n");

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>IHG Development — Reference Index</title>
  <style>
    body { font-family: Inter, system-ui, sans-serif; max-width: 960px; margin: 2rem auto; padding: 0 1.25rem; color: #1a1a2e; line-height: 1.5; }
    h1 { font-size: 1.5rem; margin-bottom: 0.25rem; }
    .note { background: #f0f4ff; border: 1px solid #c5d0f0; border-radius: 8px; padding: 12px 16px; margin: 1rem 0 1.5rem; font-size: 0.95rem; }
    table { width: 100%; border-collapse: collapse; font-size: 0.92rem; }
    th, td { text-align: left; padding: 10px 8px; border-bottom: 1px solid #e2e8f0; vertical-align: top; }
    th { font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.04em; color: #64748b; }
    a { color: #4338ca; }
    code { background: #f1f5f9; padding: 2px 6px; border-radius: 4px; font-size: 0.85em; }
  </style>
</head>
<body>
  <h1>IHG Development — captured reference pages</h1>
  <p>Captured ${new Date().toISOString().slice(0, 10)} from development.ihg.com</p>
  <div class="note">
    <strong>How to view:</strong>
    <ul style="margin: 0.5rem 0 0; padding-left: 1.25rem;">
      <li><strong>Best offline:</strong> open the <code>(archive).mhtml</code> file in Chrome or Edge (double-click from this synced folder).</li>
      <li><strong>Print / share:</strong> use the <code>(snapshot).pdf</code> file.</li>
      <li><strong>HTML:</strong> open in Chrome with internet; styles are inlined but images load from ihg.com. Google Drive web preview may not render HTML correctly — download or open locally instead.</li>
    </ul>
  </div>
  <table>
    <thead><tr><th>Page</th><th>Offline archive</th><th>PDF</th><th>HTML</th></tr></thead>
    <tbody>
${rows}
    </tbody>
  </table>
</body>
</html>`;

  fs.writeFileSync(path.join(devDir, "INDEX.html"), html, "utf8");
  fs.writeFileSync(
    path.join(devDir, "HOW-TO-VIEW.txt"),
    `IHG Development reference captures — viewing guide

Best offline: open the (archive).mhtml file in Chrome or Edge.
Print/share: use the (snapshot).pdf file.
HTML: open INDEX.html or individual .html files in Chrome (not Google Drive web preview).

Google Drive's in-browser preview blocks external assets; use local files from:
G:\\My Drive\\Dealality™\\Platform Design & Build\\Brand Reference Material\\IHG Hotels & Resorts\\development\\
`,
    "utf8"
  );
  console.log("Wrote INDEX.html + HOW-TO-VIEW.txt");
}

function escapeHtml(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function escapeAttr(s) {
  return String(s).replace(/&/g, "&amp;").replace(/"/g, "&quot;");
}

async function main() {
  const devDir = path.join(resolveReferenceRoot(), COMPANY, "development");
  for (const name of OBSOLETE) {
    const p = path.join(devDir, name);
    if (fs.existsSync(p)) {
      console.log("Remove obsolete:", name);
      if (APPLY) fs.unlinkSync(p);
    }
  }

  const browser = await puppeteer.launch({ headless: "new" });
  const report = { ok: [], errors: [] };

  for (let i = 0; i < PAGES.length; i++) {
    const page = PAGES[i];
    console.log(`[${i + 1}/${PAGES.length}]`, page.title);
    if (!APPLY) {
      console.log("  WOULD", page.url);
      continue;
    }
    try {
      const result = await captureHtmlWithBrowser(browser, {
        url: page.url,
        title: page.title,
        companyFolder: COMPANY,
        typeKey: "development-brochure",
        category: "development",
        alsoPdf: true,
        alsoMhtml: true,
        gotoTimeout: page.url.includes("/owner-value") ? 180000 : 120000,
      });
      report.ok.push({ ...page, ...result });
      console.log("  OK", result.relativePath);
      if (result.mhtmlRelativePath) console.log("  MHTML", result.mhtmlRelativePath);
    } catch (err) {
      const msg = err?.message || String(err);
      report.errors.push({ ...page, error: msg });
      console.warn("  FAIL", msg);
    }
  }

  await browser.close();

  if (APPLY && report.ok.length) {
    writeDevelopmentFolderIndex(devDir, report.ok);
  }

  const out = path.join(__dirname, "..", "reports", "ihg-development-recapture.json");
  fs.writeFileSync(out, JSON.stringify(report, null, 2));
  console.log("\nWrote", out, "| OK:", report.ok.length, "| Errors:", report.errors.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
