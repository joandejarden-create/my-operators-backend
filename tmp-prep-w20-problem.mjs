import fs from "fs";
import crypto from "crypto";

fs.copyFileSync(
  "public/marketing/dealality-old-home-freeform-head.v20260729w19.css",
  "public/marketing/dealality-old-home-freeform-head.v20260729w20.css"
);

const css = fs.readFileSync(
  "public/marketing/dealality-old-home-freeform-head.v20260729w20.css"
);
console.log("css md5", crypto.createHash("md5").update(css).digest("hex"), css.length);

let js = fs.readFileSync("public/marketing/old-home-problem-v2.v20260729b.js", "utf8");
js = js
  .replace(
    /https:\/\/cdn\.prod\.website-files\.com\/68108c29063eeb5d1bd7ae4a\/[^"']+freeform-head\.v20260729w\d+\.css/g,
    "PLACEHOLDER_CDN_w20.css"
  )
  .replace(
    /dealality-old-home-freeform-head\.v20260729w\d+\.css/g,
    "dealality-old-home-freeform-head.v20260729w20.css"
  );
fs.writeFileSync("public/marketing/old-home-problem-v2.v20260729c.js", js);
console.log("has placeholder", js.includes("PLACEHOLDER_CDN_w20.css"));
console.log("has w20 name", js.includes("v20260729w20.css"));
