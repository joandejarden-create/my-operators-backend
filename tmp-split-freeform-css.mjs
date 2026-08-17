import fs from "fs";
import crypto from "crypto";
import axios from "axios";
import FormData from "form-data";

const head = fs.readFileSync("tmp-old-home-head-globe.txt", "utf8");
const m = head.match(/<style>([\s\S]*)<\/style>/);
if (!m) throw new Error("no style block");
const styleCss = m[1].trim() + "\n";
const links = head
  .replace(/<style>[\s\S]*<\/style>/, "")
  .trim();

// Ensure globe css link is present once; main freeform css will include globe rules too
const freeformCssName = "dealality-old-home-freeform-head.v20260729.css";
fs.writeFileSync(`public/marketing/${freeformCssName}`, styleCss);
const md5 = crypto.createHash("md5").update(styleCss).digest("hex");
const sha = crypto.createHash("sha256").update(styleCss).digest("base64");
console.log({ bytes: styleCss.length, md5, sha, linksPreview: links.slice(0, 200) });
fs.writeFileSync(
  "tmp-freeform-links-only.txt",
  links +
    `\n<link rel="stylesheet" href="HOSTED_PLACEHOLDER">\n`
);
fs.writeFileSync("tmp-freeform-style-md5.txt", md5);
fs.writeFileSync("tmp-freeform-style-sha.txt", sha);
