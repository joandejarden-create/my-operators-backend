/**
 * Copy latest mockup assets from the engagement folder into railway/static/.
 * Run before commit/deploy: npm run sync
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const srcDir = path.join(__dirname, "..");
const destDir = path.join(__dirname, "static");

const files = [
  { from: "commercial-performance-hub-mockup.html", to: "index.html" },
  { from: "commercial-performance-hub-mockup.js", to: "commercial-performance-hub-mockup.js" },
  { from: "lonrp-dashboard-views.js", to: "lonrp-dashboard-views.js" },
  { from: "lonrp-executive-view.js", to: "lonrp-executive-view.js" },
  { from: "lonrp-tableau-data.js", to: "lonrp-tableau-data.js" },
];

fs.mkdirSync(destDir, { recursive: true });

for (const { from, to } of files) {
  const src = path.join(srcDir, from);
  const dest = path.join(destDir, to);
  if (!fs.existsSync(src)) {
    console.error(`Missing source: ${src}`);
    process.exit(1);
  }
  fs.copyFileSync(src, dest);
  console.log(`Copied ${from} → static/${to}`);
}

console.log("Sync complete.");
