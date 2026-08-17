import fs from "fs";
import crypto from "crypto";

const head = fs.readFileSync("tmp-old-home-head-globe.txt", "utf8");
const start = head.indexOf("/* Hero globe");
const end = head.indexOf("/* Hero — copy left");
if (start < 0 || end < 0) throw new Error("markers missing");
const css = head.slice(start, end).trim() + "\n";
fs.writeFileSync("public/marketing/dealality-old-home-hero-globe-bg.css", css);
console.log({
  bytes: css.length,
  md5: crypto.createHash("md5").update(css).digest("hex"),
  sha256: crypto.createHash("sha256").update(css).digest("base64"),
});
