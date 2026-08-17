import fs from "fs";

const h = fs.readFileSync("tmp-home-live-nav.html", "utf8");
console.log("title", (h.match(/<title>[^<]+/) || [])[0]);
console.log("has oh-nav", h.includes("oh-nav"));
console.log("has old-home markers", {
  insightsSection: /id="insights"/.test(h),
  faqSection: /id="faq"/.test(h),
  dcPremium: h.includes("dc-premium"),
  process: h.includes(">Process<"),
  capabilities: h.includes(">Capabilities<"),
});

const re = /<a[^>]+href="([^"]*)"[^>]*>\s*(About|Process|Capabilities|Insights|FAQ|Sign In)\s*<\/a>/gi;
let m;
while ((m = re.exec(h))) {
  console.log(`${m[2]} => ${m[1]}`);
}

// Also look for nav-like blocks
const i = h.search(/About[\s\S]{0,40}Process|Insights[\s\S]{0,40}FAQ/);
console.log("snippet", h.slice(Math.max(0, i - 80), i + 350).replace(/\s+/g, " ").slice(0, 450));
