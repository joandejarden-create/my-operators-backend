import fs from "fs";

const p = "public/marketing/dealality-old-home-freeform-head.v20260729w16.css";
let css = fs.readFileSync(p, "utf8");

if (css.includes("Modules dual-tab panels")) {
  console.log("already patched", css.length);
  process.exit(0);
}

const addition = `
/* Modules dual-tab panels: hidden attr is source of truth (fixes aria-hidden/style.display stuck hide). */
#modules-dots button.is-active,#modules-dot-1.is-active,#modules-dot-2.is-active,.modules-dot.is-active,#modules-dots button[aria-selected="true"],#modules-dot-1[aria-selected="true"],#modules-dot-2[aria-selected="true"]{background:#fff!important;transform:scale(1.15)!important}
#modules-panel-outcomes[hidden],#modules-panel-platform[hidden]{display:none!important}
#modules-panel-outcomes:not([hidden]),#modules-panel-platform:not([hidden]){display:block!important}
#modules-panel-platform[aria-hidden="true"]:not([hidden]),#modules-panel-outcomes[aria-hidden="true"]:not([hidden]){display:block!important}
#modules-tab-outcomes,#modules-tab-platform{display:inline-flex!important;visibility:visible!important;opacity:1!important;max-width:none!important;overflow:visible!important;white-space:nowrap!important}
#modules-badge{overflow:visible!important;max-width:100%!important;flex-wrap:nowrap!important;gap:0!important;padding:5px!important}
`;

css = css.trimEnd() + "\n" + addition;
fs.writeFileSync(p, css);
console.log("patched", css.length);
