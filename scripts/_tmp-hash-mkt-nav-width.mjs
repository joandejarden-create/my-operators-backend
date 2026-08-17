import fs from "fs";
import crypto from "crypto";
import path from "path";

const file = path.resolve(
  "C:/Dev/deal-capture-proxy/public/marketing/dealality-marketing-nav-width.v20260801a.js"
);
const buf = fs.readFileSync(file);
console.log(
  JSON.stringify({
    md5: crypto.createHash("md5").update(buf).digest("hex"),
    sha256:
      "sha256-" + crypto.createHash("sha256").update(buf).digest("base64"),
    size: buf.length,
  })
);
