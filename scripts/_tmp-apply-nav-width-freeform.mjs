/**
 * Fetch live freeform via files we write, patch, emit MCP-ready JSON.
 * Inputs: data/_tmp-site-head-raw.html, data/_tmp-site-footer-raw.html
 */
import fs from "fs";

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
  `<!-- dcMktNavWidth01a: Symbol navbar 1120 band = Home #nav (JS backup; CSS is in site head) -->\n` +
  `<script src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e4ecc93bf55bda98a5a1f_dealality-marketing-nav-width.v20260801a.js" integrity="sha256-UCpwXYZCx9rIT5g4D0LngJpFk4iqXh+v7BmPMjxCIzI=" crossorigin="anonymous"></script>`;

const head = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-site-head-raw.html",
  "utf8"
);
const footer = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-site-footer-raw.html",
  "utf8"
);

let nextHead = head;
if (!nextHead.includes("dc-mkt-nav-width-01a")) {
  const marker = "</style>\n\n<!-- Dealality: Old Home FOUC gate";
  if (nextHead.includes(marker)) {
    nextHead = nextHead.replace(
      marker,
      "</style>\n" + NAV_CSS + "\n\n<!-- Dealality: Old Home FOUC gate"
    );
  } else {
    nextHead = nextHead.trimEnd() + "\n" + NAV_CSS + "\n";
  }
}

let nextFooter = footer;
if (!nextFooter.includes("marketing-nav-width")) {
  nextFooter = nextFooter.trimEnd() + "\n\n" + SCRIPT + "\n";
}

fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-site-head-nav-width.html",
  nextHead
);
fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-site-footer-nav-width.html",
  nextFooter
);
fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_mcp-set-nav-width.json",
  JSON.stringify({
    head: nextHead,
    footer: nextFooter,
  })
);
console.log(
  JSON.stringify({
    headLen: nextHead.length,
    footerLen: nextFooter.length,
    hasCss: nextHead.includes("dc-mkt-nav-width-01a"),
    hasJs: nextFooter.includes("6a6e4ecc93bf55bda98a5a1f"),
  })
);
