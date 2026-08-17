const fs = require("fs");

// Paste from last get_site_freeform_code footer result, then strip f20 pin.
const raw = fs.readFileSync("tmp/site-footer-from-api.txt", "utf8");
const cleaned = raw
  .replace(
    /\n<!-- dmp f20:[\s\S]*?v20260801f20\.css\" \/>\n?/,
    "\n<!-- Manual Process CSS pin lives on Home page footer freeform (f21) — do not pin DMP CSS at site level -->\n"
  )
  .replace(/\n+$/, "\n");

fs.writeFileSync("tmp/site-footer-nof20.txt", cleaned);
console.log({
  len: cleaned.length,
  hasF20: cleaned.includes("f20.css"),
  ends: cleaned.slice(-180),
});
