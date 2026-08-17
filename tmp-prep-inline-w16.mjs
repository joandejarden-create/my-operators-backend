import fs from "fs";
import crypto from "crypto";

const src = fs.readFileSync("tmp-oh-modules-w16-fix.js", "utf8");
console.log("len", src.length);
if (src.length > 2000) {
  console.error("too long for inline");
  process.exit(2);
}
const md5 = crypto.createHash("md5").update(src).digest("hex");
const sri =
  "sha256-" + crypto.createHash("sha256").update(src).digest("base64");
console.log("md5", md5);
console.log("sri", sri);
fs.writeFileSync(
  "tmp-oh-modules-w16-meta.json",
  JSON.stringify({ len: src.length, md5, sri, src }, null, 2)
);
