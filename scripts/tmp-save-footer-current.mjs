/**
 * Saves Old Home footer freeform from stdin argv file, or expects
 * tmp-old-home-footer-from-mcp.html to exist.
 * Prefer: node scripts that write MCP content.
 */
import fs from "fs";

// Content captured from Webflow get_page_freeform_code footer (2026-07-28)
// Keep scripts 1+2 intact; only reader script will be replaced by patcher.
const content = fs.existsSync("tmp-old-home-footer-live.html")
  ? fs.readFileSync("tmp-old-home-footer-live.html", "utf8")
  : null;

if (!content) {
  console.error("missing tmp-old-home-footer-live.html — write MCP footer first");
  process.exit(1);
}
fs.writeFileSync("tmp-old-home-footer-current.html", content);
console.log("ok", content.length);
