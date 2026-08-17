/**
 * Build updated site head + footer with marketing nav 1120 width lock.
 */
import fs from "fs";

const NAV_CSS = `
<style id="dc-mkt-nav-width-01a">
  /* Marketing Symbol navbar → same 1120 content band as Home #nav */
  html.dl-public-no-loader .navbar-2,
  html.dl-public-no-loader .navbar-2.w-nav {
    width: 100% !important;
    max-width: none !important;
  }
  html.dl-public-no-loader .navbar-2 > .navbar_content {
    width: 100% !important;
    max-width: none !important;
    margin-left: 0 !important;
    margin-right: 0 !important;
    box-sizing: border-box !important;
    padding-left: calc((100% - 1120px) / 2 + clamp(1.5rem, 4vw, 3rem)) !important;
    padding-right: calc((100% - 1120px) / 2 + clamp(1.5rem, 4vw, 3rem)) !important;
  }
  html.dl-public-no-loader .navbar-2 > .navbar_content > .navbar_content {
    width: 100% !important;
    max-width: none !important;
    box-sizing: border-box !important;
    padding-left: 0 !important;
    padding-right: 0 !important;
    margin: 0 !important;
  }
  @media (max-width: 1200px) {
    html.dl-public-no-loader .navbar-2 > .navbar_content {
      padding-left: clamp(1.25rem, 4vw, 2.5rem) !important;
      padding-right: clamp(1.25rem, 4vw, 2.5rem) !important;
    }
  }
</style>
`;

const headPath = "C:/Dev/deal-capture-proxy/data/_tmp-site-head-nav-width.html";
const footerPath = "C:/Dev/deal-capture-proxy/data/_tmp-site-footer-nav-width.html";

// Head will be written by caller from MCP; this script patches a saved file.
const headIn = process.argv[2];
const footerIn = process.argv[3];
if (!headIn || !footerIn) {
  console.error("Usage: node script.mjs <head.html> <footer.html>");
  process.exit(1);
}

let head = fs.readFileSync(headIn, "utf8");
let footer = fs.readFileSync(footerIn, "utf8");

if (!head.includes("dc-mkt-nav-width-01a")) {
  if (head.includes("</style>\n\n<!-- Dealality: Old Home FOUC gate")) {
    head = head.replace(
      "</style>\n\n<!-- Dealality: Old Home FOUC gate",
      "</style>\n" + NAV_CSS.trim() + "\n\n<!-- Dealality: Old Home FOUC gate"
    );
  } else {
    head = head.trimEnd() + "\n" + NAV_CSS.trim() + "\n";
  }
}

const SCRIPT_TAG =
  '<!-- dcMktNavWidth01a: Symbol navbar 1120 band = Home #nav -->\n<script src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/PLACEHOLDER_dealality-marketing-nav-width.v20260801a.js" integrity="PLACEHOLDER_SRI" crossorigin="anonymous"></script>\n';

if (!footer.includes("marketing-nav-width")) {
  footer = footer.trimEnd() + "\n\n" + SCRIPT_TAG;
}

fs.writeFileSync(headPath, head);
fs.writeFileSync(footerPath, footer);
console.log(
  JSON.stringify({
    headLen: head.length,
    footerLen: footer.length,
    hasCss: head.includes("dc-mkt-nav-width-01a"),
    hasScriptSlot: footer.includes("marketing-nav-width"),
  })
);
