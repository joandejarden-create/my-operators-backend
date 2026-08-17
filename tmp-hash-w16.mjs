import crypto from "crypto";
import fs from "fs";

for (const f of [
  "public/marketing/dealality-old-home-freeform-head.v20260729w16.css",
  "public/marketing/old-home-footer-oh.v20260729e.js",
]) {
  const b = fs.readFileSync(f);
  console.log(f);
  console.log(crypto.createHash("md5").update(b).digest("hex"), b.length);
}
