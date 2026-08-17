import fs from "fs";
const head = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-freeform-head-cutover.html",
  "utf8"
);
const footer = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-freeform-footer-cutover.html",
  "utf8"
);
fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-freeform-payload.json",
  JSON.stringify({ head, footer })
);
console.log({
  head: head.length,
  footer: footer.length,
  gate: head.includes("path !== '/' && path !== '/old-home'"),
  nav: footer.includes("nav-cleanup.v20260801b"),
});
