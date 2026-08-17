/**
 * Pull site freeform from a saved MCP JSON dump if present, else fail.
 * Prefer: pass paths via env.
 *
 * Alternative: reconstruct by reading live site HTML won't work for freeform.
 *
 * This script writes patched freeform from:
 *   data/_wf-site-freeform.json  { head, footer }
 */
import fs from "fs";

const SRC = "C:/Dev/deal-capture-proxy/data/_wf-site-freeform.json";
if (!fs.existsSync(SRC)) {
  console.error("missing", SRC);
  process.exit(1);
}
const data = JSON.parse(fs.readFileSync(SRC, "utf8"));
let head = data.head || data.headContent || "";
let footer = data.footer || data.footerContent || "";

const NAV_CSS = `<style id="dc-mkt-nav-width-01a">
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
</style>`;

const SCRIPT =
  `<!-- dcMktNavWidth01a: Symbol navbar 1120 band = Home #nav -->\n` +
  `<script src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e4ecc93bf55bda98a5a1f_dealality-marketing-nav-width.v20260801a.js" integrity="sha256-UCpwXYZCx9rIT5g4D0LngJpFk4iqXh+v7BmPMjxCIzI=" crossorigin="anonymous"></script>`;

if (!head.includes("dc-mkt-nav-width-01a")) {
  const marker = "</style>\n\n<!-- Dealality: Old Home FOUC gate";
  if (head.includes(marker)) {
    head = head.replace(
      marker,
      "</style>\n" + NAV_CSS + "\n\n<!-- Dealality: Old Home FOUC gate"
    );
  } else {
    head = head.trimEnd() + "\n" + NAV_CSS + "\n";
  }
}
if (!footer.includes("marketing-nav-width")) {
  footer = footer.trimEnd() + "\n\n" + SCRIPT + "\n";
}

fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-site-head-nav-width.html",
  head
);
fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-site-footer-nav-width.html",
  footer
);
fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_mcp-set-nav-width.json",
  JSON.stringify({ head, footer })
);
console.log(
  JSON.stringify({
    headLen: head.length,
    footerLen: footer.length,
    hasCss: head.includes("dc-mkt-nav-width-01a"),
    hasJs: footer.includes("6a6e4ecc93bf55bda98a5a1f"),
  })
);
