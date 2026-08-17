import crypto from "crypto";
import fs from "fs";

const files = [
  "public/marketing/dealality-old-home-freeform-head.v20260729w16.css",
  "public/marketing/old-home-footer-oh.v20260729e.js",
];
for (const f of files) {
  const b = fs.readFileSync(f);
  const md5 = crypto.createHash("md5").update(b).digest("hex");
  console.log(JSON.stringify({ file: f, md5, length: b.length }));
}
