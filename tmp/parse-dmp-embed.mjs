import fs from "fs";
const raw = fs.readFileSync(
  "C:/Users/joand/.cursor/browser-logs/cdp-response-Runtime.evaluate-2026-08-01T21-48-55-415Z.json",
  "utf8"
);
const j = JSON.parse(raw);
const html = j?.result?.result?.value || j?.result?.value || "";
console.log("len", html.length);
console.log((html.match(/manual-process[^\"'\s>]+/g) || []).slice(0, 20));
console.log(html.slice(0, 900));
