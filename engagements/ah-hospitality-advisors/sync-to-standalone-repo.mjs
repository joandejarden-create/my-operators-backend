/**
 * Push latest mockup from deal-capture-proxy engagement into the standalone
 * GitHub repo folder (sibling directory). Run from deal-capture-proxy root:
 *
 *   node engagements/ah-hospitality-advisors/sync-to-standalone-repo.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const srcDir = __dirname;
const files = [
  { from: "commercial-performance-hub-mockup.html", to: "index.html" },
  { from: "commercial-performance-hub-mockup.js", to: "commercial-performance-hub-mockup.js" },
  { from: "lonrp-dashboard-views.js", to: "lonrp-dashboard-views.js" },
  { from: "lonrp-executive-view.js", to: "lonrp-executive-view.js" },
  { from: "lonrp-tableau-data.js", to: "lonrp-tableau-data.js" },
];

const destDirs = [
  path.resolve(__dirname, "../../../ah-commercial-performance-hub/static"),
  path.resolve(__dirname, "../../../ah-commercial-performance-hub/docs"),
];

for (const destDir of destDirs) {
  if (!fs.existsSync(destDir)) {
    console.error(`Standalone repo folder not found: ${destDir}`);
    process.exit(1);
  }
}

for (const destDir of destDirs) {
  for (const { from, to } of files) {
    const src = path.join(srcDir, from);
    const dest = path.join(destDir, to);
    if (!fs.existsSync(src)) {
      console.error(`Missing source: ${src}`);
      process.exit(1);
    }
    fs.copyFileSync(src, dest);
    const label = destDir.includes("docs") ? "docs" : "static";
    console.log(`Copied ${from} → ah-commercial-performance-hub/${label}/${to}`);
  }
}

console.log("Done. Commit and push ah-commercial-performance-hub (GitHub Pages uses docs/).");
