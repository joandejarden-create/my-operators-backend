/**
 * PDF/HTML export copy check — renders snapshot like print path and asserts legacy strings absent.
 * Usage: node scripts/test-ocs-pdf-export-copy.mjs
 */
import puppeteer from "puppeteer";
import { writeFileSync, mkdirSync, rmSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { buildOperatorCapabilitySnapshotV1 } from "../lib/operator-capability-snapshot-build.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const outDir = join(root, "reports", "ocs-pdf-export-check");

const fields = {
  "Project Type": "New Build",
  "Property Name": "Hilton Garden Inn Medellin",
  "Current Operating Model": "Owner-operated (unbranded)",
  "Opening / Transition Phase": "Construction",
  "Primary Market Region": "CALA",
  "Preferred Future Operating Model": "Third-party management only",
  "Operator Capability Priorities": [
    "Pre-opening / opening support",
    "Revenue management & distribution",
    "Accounting & owner reporting",
    "Design / renovation PM",
    "Local market / CALA execution",
  ],
  "Owner Reporting Frequency": "Monthly",
  "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
  "Stage of Development": "Under construction",
};

const snap = buildOperatorCapabilitySnapshotV1(fields, "recPDFcopyCheck");
mkdirSync(outDir, { recursive: true });
const htmlPath = join(outDir, "snapshot.html");
const pdfPath = join(outDir, "snapshot.pdf");

const dataJson = JSON.stringify(snap).replace(/</g, "\\u003c");
writeFileSync(
  htmlPath,
  `<!DOCTYPE html><html><head>
<link rel="stylesheet" href="${pathToFileURL(join(root, "public/css/brand-alignment-snapshot.css")).href}">
<link rel="stylesheet" href="${pathToFileURL(join(root, "public/css/operator-capability-snapshot.css")).href}">
</head><body>
<div id="root"></div>
<script src="${pathToFileURL(join(root, "public/js/operator-capability-snapshot.js")).href}"></script>
<script>OperatorCapabilitySnapshot.render(document.getElementById("root"), ${dataJson}, { fullPage: true });</script>
</body></html>`
);

const browser = await puppeteer.launch({ headless: "new" });
const page = await browser.newPage();
await page.goto(pathToFileURL(htmlPath).href, { waitUntil: "networkidle0", timeout: 60000 });
await page.waitForFunction(() => document.querySelectorAll(".bas-book-page").length >= 2, { timeout: 15000 });

const htmlText = await page.evaluate(() => {
  const root = document.querySelector(".operator-capability-snapshot");
  return root ? root.textContent || "" : document.body.textContent || "";
});
const pdfBuffer = await page.pdf({
  format: "A4",
  printBackground: true,
  margin: { top: "12mm", right: "12mm", bottom: "12mm", left: "12mm" },
});
writeFileSync(pdfPath, pdfBuffer);
await browser.close();

const pdfText = pdfBuffer.toString("latin1");
const pageCount = (pdfText.match(/\/Type\s*\/Page\b/g) || []).length;

const checks = [
  {
    label: "legacy owner-operated→third-party sentence",
    bad: /Current model is owner-operated while the preferred future model is third-party management/gi,
    text: htmlText + pdfText,
    want: 0,
  },
  {
    label: "Operating model transition to validate phrase",
    bad: /Operating model transition to validate/gi,
    text: htmlText + pdfText,
    want: 0,
  },
  {
    label: "Operating model transitions to validate phrase",
    bad: /Operating model transitions to validate/gi,
    text: htmlText + pdfText,
    want: 0,
  },
  {
    label: "operating model conflict",
    bad: /operating model conflict/gi,
    text: htmlText + pdfText,
    want: 0,
  },
  {
    label: "clarification count draft line",
    bad: /1 clarification may be resolved to strengthen this internal draft/gi,
    text: htmlText + pdfText,
    want: 0,
  },
  {
    label: "limited review status body",
    good: /Limited internal draft\. Resolve the clarification below before external sharing\./,
    text: htmlText,
    want: 1,
  },
  {
    label: "operating model transition section body",
    good: /Operating model transition[\s\S]*Preferred future model: third[\u2011-]party management/i,
    text: htmlText,
    want: 1,
  },
];

let failed = 0;
for (const c of checks) {
  if (c.bad) {
    const n = (c.text.match(c.bad) || []).length;
    if (n !== c.want) {
      console.error(`FAIL: ${c.label} — found ${n}, expected ${c.want}`);
      failed++;
    } else {
      console.log(`ok: ${c.label} (${n})`);
    }
  } else if (c.good) {
    const ok = c.good.test(c.text);
    if (!ok) {
      console.error(`FAIL: ${c.label} — expected match`);
      failed++;
    } else {
      console.log(`ok: ${c.label}`);
    }
  }
}

console.log(JSON.stringify({ pdfPages: pageCount, htmlPath, pdfPath }, null, 2));
if (failed) process.exit(1);
