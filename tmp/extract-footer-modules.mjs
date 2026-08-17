import fs from "fs";

const raw = fs.readFileSync(
  "C:/Users/joand/.cursor/projects/c-Dev-deal-capture-proxy/agent-tools/3a764595-f812-475e-bc44-1c217ebc8e4a.txt",
  "utf8"
);
const line = raw.trim().split(/\n/)[0];
const j = JSON.parse(line);
const c = j.result.content;
const idx = c.indexOf("modules-copy");
console.log(c.slice(Math.max(0, idx - 200), idx + 250));
fs.writeFileSync("tmp/site-footer-freeform.html", c);
console.log("wrote footer len", c.length);
