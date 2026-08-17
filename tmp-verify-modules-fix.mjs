import https from "https";
import fs from "fs";

function get(url) {
  return new Promise((res, rej) => {
    https
      .get(url, { headers: { "cache-control": "no-cache" } }, (r) => {
        let d = "";
        r.on("data", (c) => (d += c));
        r.on("end", () => res(d));
      })
      .on("error", rej);
  });
}

const html = await get(
  "https://www.dealality.com/old-home?cb=" + Date.now()
);
fs.writeFileSync("tmp-old-home-verify.html", html);

const checks = {
  ohmodulestabfixw16: html.includes("ohmodulestabfixw16"),
  w16css: html.includes("freeform-head.v20260729w16"),
  w15css: html.includes("freeform-head.v20260729w15"),
  panel2: html.includes("modules-panel-platform"),
  how: (html.match(/How Dealality Works/g) || []).length,
  owners: (html.match(/What Owners Gain/g) || []).length,
  dots: html.includes("modules-dot-2"),
  benefitsTabs: html.includes("benefits-tabs"),
};

console.log(JSON.stringify(checks, null, 2));
console.log("len", html.length);

// Fetch the registered script content
const scriptUrl =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a%2F689e5ba67671442434f3ca35%2F6a6a22cdba5c2e3ed95d0b00%2Fohmodulestabfixw16-1.0.0.js";
const js = await get(scriptUrl);
console.log("script has setPanel/sp", js.includes("function sp"));
console.log("script has w16 css", js.includes("w16.css"));
console.log("script len", js.length);

// Fetch w16 css and confirm panel rules
const css = await get(
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a18e9e0ff4ffc45c68246_dealality-old-home-freeform-head.v20260729w16.css"
);
console.log(
  "css has not([hidden])",
  css.includes(":not([hidden]){display:block!important}")
);
console.log(
  "css has dual-tab comment",
  css.includes("Modules dual-tab panels")
);
