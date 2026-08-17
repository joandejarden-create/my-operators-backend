/**
 * Build Home page freeform payloads for footer top-align + motion 01g.
 */
import fs from "fs";

const PAGE = "68108c2a063eeb5d1bd7ae90";
const MOTION_JS =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2be2eb6dc0e334533fbc_old-home-motion.prod.v20260801g.js";
const MOTION_SRI =
  "sha384-EKByvF9YKtbgBkJTUZFlP+PBJlFl6GPabBlWAJ52BYFxuZSilOaxvVuGYELR8np2";

// Head from MCP get_page_freeform_code — append footer top-align override before closing.
const headBase = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-home-page-freeform-head.html",
  "utf8"
);
const footerBase = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-home-page-freeform-footer.html",
  "utf8"
);

const footerCss = `
<style id="oh-footer-top-align">
/* Top-align Platform / Learn / Company columns (was align-items:end stair-step) */
#footer-grid,#footer-grid-new{align-items:start!important}
#footer-col-products,#footer-col-resources,#footer-col-links{align-self:start!important}
</style>
`;

let head = headBase.trimEnd();
if (!head.includes("oh-footer-top-align")) {
  head = head + "\n" + footerCss.trim() + "\n";
}

let footer = footerBase
  .replace(
    /old-home-motion\.prod\.v20260801f\.js[^"]*/g,
    "PLACEHOLDER"
  )
  .replace(
    /https:\/\/cdn\.prod\.website-files\.com\/68108c29063eeb5d1bd7ae4a\/[0-9a-f]+_old-home-motion\.prod\.v20260801f\.js/g,
    MOTION_JS
  )
  .replace(
    /integrity="sha384-[^"]+"(\s+crossorigin="anonymous"\s+defer\s+id="oh-motion-prod-js")/,
    `integrity="${MOTION_SRI}"$1`
  )
  .replace(
    /Old Home motion prod v20260801f[^\n]*/,
    "Old Home motion prod v20260801g — unlock `/` + `/old-home`; unify enter contract"
  );

if (!footer.includes("old-home-motion.prod.v20260801g.js")) {
  footer = `<!-- Old Home footer scripts loaded by OldHomeBootGuard site header script -->
<!-- Manual Process is the approved #about package (HtmlEmbed oh-manual-process-embed). -->
<!-- Deal Desk Phase B cinematic script REMOVED 2026-07-31: superseded by Manual Process; do not restore. -->
<!-- Old Home motion prod v20260801g — unlock \`/\` + \`/old-home\`; unify enter contract -->
<script src="${MOTION_JS}" integrity="${MOTION_SRI}" crossorigin="anonymous" defer id="oh-motion-prod-js"></script>
`;
}

fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-home-freeform-set.json",
  JSON.stringify({ page_id: PAGE, head, footer }, null, 2)
);
console.log(
  JSON.stringify({
    headLen: head.length,
    footerLen: footer.length,
    hasFooterCss: head.includes("oh-footer-top-align"),
    hasMotionG: footer.includes("v20260801g.js"),
    footerPreview: footer.slice(0, 500),
  })
);
