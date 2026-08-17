import fs from "fs";
import crypto from "crypto";

function md5(path) {
  const b = fs.readFileSync(path);
  return { hash: crypto.createHash("md5").update(b).digest("hex"), len: b.length };
}

const css = md5("public/marketing/dealality-old-home-pricing.v20260729b.css");
const js = md5("public/marketing/old-home-footer-oh.v20260729f.js");
fs.writeFileSync(
  "tmp-pricing-hashes.json",
  JSON.stringify({ css, js }, null, 2)
);
console.log(JSON.stringify({ css, js }, null, 2));
