import fs from "fs";
import crypto from "crypto";
import path from "path";

const file = path.resolve(
  "C:/Dev/deal-capture-proxy/public/marketing/dealality-old-home-freeform-head.v20260801f.css"
);
const buf = fs.readFileSync(file);
console.log(
  JSON.stringify({
    file: path.basename(file),
    md5: crypto.createHash("md5").update(buf).digest("hex"),
    sha384:
      "sha384-" + crypto.createHash("sha384").update(buf).digest("base64"),
    size: buf.length,
    hasStart: buf.toString().includes("align-items:start!important"),
    hasEndDesktop: /align-items:end!important/.test(
      buf
        .toString()
        .split("@media")[0]
        .split("Footer columns")[1]
        ?.slice(0, 400) || ""
    ),
  })
);
