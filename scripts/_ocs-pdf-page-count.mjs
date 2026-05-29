/**
 * Estimate Operator Capability Snapshot PDF page count (Medellin-style fixture).
 * Usage: node scripts/_ocs-pdf-page-count.mjs
 */
import puppeteer from "puppeteer";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { buildOperatorCapabilitySnapshotV1 } from "../lib/operator-capability-snapshot-build.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const fields = {
  "Project Type": "New Build",
  "Property Name": "Hilton Garden Inn Medellin",
  "Current Operating Model": "Owner-operated (unbranded)",
  "Opening / Transition Phase": "Construction",
  "Primary Market Region": "CALA",
  "Preferred Future Operating Model": "Third-party management only",
  "Operator Capability Priorities": [
    "Pre-opening support",
    "Revenue management & distribution",
    "Owner reporting",
    "Development / complex project coordination",
    "Local market execution",
  ],
  "Owner Reporting Frequency": "Monthly",
  "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
  "Stage of Development": "Under construction",
};

const snap = buildOperatorCapabilitySnapshotV1(fields, "recPDFcount");
const htmlPath = join(root, "reports", "ocs-pdf-count-temp.html");
mkdirSync(join(root, "reports"), { recursive: true });

const dataJson = JSON.stringify(snap).replace(/</g, "\\u003c");
const htmlPathUrl = pathToFileURL(htmlPath).href;

writeFileSync(
  htmlPath,
  `<!DOCTYPE html><html><head>
<link rel="stylesheet" href="${pathToFileURL(join(root, "public/css/brand-alignment-snapshot.css")).href}">
<link rel="stylesheet" href="${pathToFileURL(join(root, "public/css/operator-capability-snapshot.css")).href}">
<link rel="stylesheet" href="${pathToFileURL(join(root, "public/css/snapshot-page-shell.css")).href}">
</head><body class="bas-page">
<div id="root"></div>
<script src="${pathToFileURL(join(root, "public/js/operator-capability-snapshot.js")).href}"></script>
<script>
const data = ${dataJson};
OperatorCapabilitySnapshot.render(document.getElementById("root"), data, { fullPage: true });
</script></body></html>`
);

const browser = await puppeteer.launch({ headless: "new" });
const page = await browser.newPage();
await page.goto(htmlPathUrl, { waitUntil: "networkidle0", timeout: 60000 });
await page.waitForFunction(() => document.querySelectorAll(".bas-book-page").length >= 2, { timeout: 15000 });
const pdf = await page.pdf({
  format: "A4",
  printBackground: true,
  margin: { top: "12mm", right: "12mm", bottom: "12mm", left: "12mm" },
});
await browser.close();

const count = (pdf.toString("latin1").match(/\/Type\s*\/Page\b/g) || []).length;
console.log(JSON.stringify({ deal: fields["Property Name"], pdfPages: count }, null, 2));
