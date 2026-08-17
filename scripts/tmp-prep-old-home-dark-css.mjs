import fs from "fs";
import crypto from "crypto";

let css = fs.readFileSync("public/marketing/dealality-old-home-premium.css", "utf8");
css = css
  .replace(/\/\*[\s\S]*?\*\//g, "")
  .replace(/\s+/g, " ")
  .replace(/\s*([{}:;,])\s*/g, "$1")
  .trim();
const extra =
  "html,body,#dc-page{height:auto!important;min-height:0!important;margin:0;padding:0;background:#080F25!important}";
const out = css + extra;
const file = "tmp-old-home-dark.css";
fs.writeFileSync(file, out);
const hash = crypto.createHash("md5").update(Buffer.from(out)).digest("hex");
console.log(JSON.stringify({ bytes: Buffer.byteLength(out), hash, file }));
