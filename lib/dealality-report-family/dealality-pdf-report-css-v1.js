/**
 * Shared CSS bundle for Dealality PDF report family (cover + body chrome).
 * ADP monthly review and GDI must load the same cover rules.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function readCss(rel) {
  const full = path.join(ROOT, rel);
  if (!fs.existsSync(full)) {
    throw new Error(`CSS_NOT_LOADED: missing ${rel}`);
  }
  return `/* ${rel} */\n${fs.readFileSync(full, "utf8")}`;
}

/**
 * Canonical cover + report-system CSS shared by ADP PDF and GDI PDF.
 * @param {string} [extraCss] — report-body-only overrides (no cover geometry)
 */
export function loadDealalityPdfReportCssBundle(extraCss = "") {
  const parts = [
    readCss("public/css/dealality-report-system-v1.css"),
    readCss("public/css/dealality-report-print-chrome.css"),
    readCss("public/css/brand-alignment-snapshot.css"),
    readCss("public/css/adp-monthly-review-report-v1.css"),
  ];
  if (extraCss) parts.push(extraCss);
  return parts.join("\n\n");
}
