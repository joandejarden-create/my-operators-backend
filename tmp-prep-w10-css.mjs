import fs from "fs";
import crypto from "crypto";
import path from "path";

const SITE = "68108c29063eeb5d1bd7ae4a";
const file = path.resolve("public/marketing/dealality-old-home-freeform-head.v20260729w10.css");
const buf = fs.readFileSync(file);
const hash = crypto.createHash("md5").update(buf).digest("hex");
const fileName = "dealality-old-home-freeform-head.v20260729w10.css";

console.log(JSON.stringify({ file, bytes: buf.length, hash, fileName, siteId: SITE }, null, 2));
