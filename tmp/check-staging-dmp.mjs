import { execSync } from "child_process";
const html = execSync(
  'curl.exe -sL "https://mvp-deal-capture.webflow.io/old-home?cb=' +
    Date.now() +
    '"',
  { encoding: "utf8", maxBuffer: 20 * 1024 * 1024 }
);
const keys = [
  "boot.v20260731j",
  "boot.v20260731i",
  "manual-process.v20260731k.css",
  "manual-process.v20260731j.css",
  "manual-process.v20260731j.html",
];
for (const k of keys) console.log(k, html.includes(k));
