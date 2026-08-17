import fs from "fs";
import crypto from "crypto";

fs.copyFileSync(
  "public/marketing/dealality-old-home-testimonials.v20260731ab.js",
  "public/marketing/dealality-old-home-testimonials.v20260731ac.js"
);
const b = fs.readFileSync(
  "public/marketing/dealality-old-home-testimonials.v20260731ac.js"
);
console.log({
  md5: crypto.createHash("md5").update(b).digest("hex"),
  len: b.length,
  hasNew: b.toString().includes("perfect tool to centralize"),
  hasVer: b.toString().includes("202607328"),
});

const bootUrl =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cbc801086b3663915d2e0_old-home-boot-guard.v20260731w.js";
const boot = await (await fetch(bootUrl)).text();
fs.writeFileSync("public/marketing/old-home-boot-guard.v20260731w.js", boot);
const i = boot.indexOf("testimonials");
console.log(boot.slice(Math.max(0, i - 80), i + 220));
