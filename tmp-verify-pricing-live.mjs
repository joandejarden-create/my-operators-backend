import https from "https";
import fs from "fs";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve(d));
      })
      .on("error", reject);
  });
}

const body = await get("https://www.dealality.com/old-home?v=" + Date.now());
fs.writeFileSync("tmp-old-home-verify-pricing.html", body);
const checks = [
  "Choose Your Starting Point",
  "Start with the role that fits you.",
  "Owners & Developers",
  "Success-Based",
  "Explore Your Opportunity",
  "Request Brand Access",
  "Request Operator Access",
  "pricing-terms",
  "dealality-old-home-pricing.v20260729b.css",
  "oh-pricing-inview",
  "Confidential opportunity review",
  "Management-term submission workflow",
  "Subscription access does not guarantee",
  "Pricing varies by opportunity size",
  "site footer left unchanged intentionally",
  "Memberstack webflow package",
];
for (const c of checks) {
  console.log(body.includes(c) ? "YES" : "NO ", c);
}
