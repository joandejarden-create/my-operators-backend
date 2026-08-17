import fs from "fs";
const c = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-site-footer-nav-width.html",
  "utf8"
);
fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_mcp-footer-nav-width-only.json",
  JSON.stringify({ content: c })
);
console.log(/v20260801b\.js/.test(c), c.length);
