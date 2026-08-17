import fs from "fs";
import https from "https";

const h = fs.readFileSync("tmp-old-home-live2.html", "utf8");

// All script srcs and freeform footer markers
const scripts = [...h.matchAll(/<script[^>]*src="([^"]+)"/g)].map((m) => m[1]);
console.log("ALL SCRIPTS:");
scripts.forEach((s) => console.log(" ", s));

const inlineFoot = h.includes("modules-dot-1") && h.includes("function activate");
console.log("\ninline activate in page?", h.includes("function activate(which)"));
console.log("footer freeform comment?", h.includes("ohOpenOpportunityReview"));
console.log("footer-oh string?", h.includes("footer-oh"));
console.log("6a69fe92?", h.includes("6a69fe92"));

// Extract end of body
const end = h.slice(-8000);
fs.writeFileSync("tmp-old-home-tail.html", end);
console.log("\n--- BODY TAIL ---");
console.log(end);

function get(url) {
  return new Promise((res, rej) => {
    https
      .get(url, (r) => {
        let d = "";
        r.on("data", (c) => (d += c));
        r.on("end", () => res(d));
      })
      .on("error", rej);
  });
}

const tabsJs =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6906bb27597c4a7d4f1433_dealality-old-home-benefits-tabs.v20260728b.js";
const tabsCss =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css";

const [js, css] = await Promise.all([get(tabsJs), get(tabsCss)]);
fs.writeFileSync("tmp-live-benefits-tabs.js", js);
fs.writeFileSync("tmp-live-benefits-tabs.css", css);
console.log("\nbenefits-tabs.js len", js.length);
console.log(js.slice(0, 2500));
console.log("\nbenefits-tabs.css:");
console.log(css);
