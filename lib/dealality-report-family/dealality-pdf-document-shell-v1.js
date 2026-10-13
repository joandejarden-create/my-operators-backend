/**
 * Shared Dealality executive PDF document shell (ADP + GDI).
 * Same root HTML, body classes, host wrapper, and Playwright chrome options.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadDealalityPdfReportCssBundle } from "./dealality-pdf-report-css-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function loadPrintChrome() {
  const code = fs.readFileSync(
    path.join(ROOT, "public/js/dealality-report-print-chrome.js"),
    "utf8"
  );
  const sandbox = { window: {}, module: { exports: {} }, globalThis: {} };
  const fn = new Function(
    "window",
    "module",
    "globalThis",
    code + "\nreturn window.DealalityReportPrintChrome || module.exports;"
  );
  return fn(sandbox.window, sandbox.module, sandbox.window);
}

/**
 * Canonical Playwright page.pdf options for the executive report family.
 * @param {string} footerReportTitle
 */
export function getDealalityPdfPlaywrightOptions(footerReportTitle) {
  const Chrome = loadPrintChrome();
  return {
    format: "A4",
    printBackground: true,
    preferCSSPageSize: false,
    displayHeaderFooter: true,
    headerTemplate: Chrome.playwrightHeaderTemplate(),
    footerTemplate: Chrome.playwrightFooterTemplate(footerReportTitle),
    margin: Chrome.pdfMargins(),
  };
}

/**
 * Wrap report host HTML in the shared document shell.
 * @param {object} opts
 * @param {string} opts.title
 * @param {string} opts.hostHtml — already includes #adp-mr-pdf-host / report body
 * @param {string} [opts.extraCss]
 * @param {string} [opts.cssText] — prebuilt bundle; if omitted, load default + extra
 */
export function renderDealalityPdfDocumentHtml(opts = {}) {
  const cssText =
    opts.cssText ||
    loadDealalityPdfReportCssBundle(opts.extraCss || "");
  const title = String(opts.title || "Dealality Report")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>${title}</title>
  <style>
${cssText}
  </style>
</head>
<body class="hid-print-pdf-export" style="margin:0;padding:0;background:#fff">
${opts.hostHtml || ""}
</body>
</html>`;
}

export { loadPrintChrome, loadDealalityPdfReportCssBundle };
