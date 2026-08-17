import fs from "fs";
import crypto from "crypto";

const t = fs.readFileSync("tmp-restore-site-footer.html", "utf8");
const marker = "<!-- INLINE: pass JWT";
const start = t.indexOf(marker);
// Find the end of the second big auth script (closes before the old-home nmenu IIFE)
const nmenu = t.indexOf('<script>(function(){\nvar b=document.getElementById("nmenu")');
if (start < 0 || nmenu < 0) {
  console.error("markers missing", start, nmenu);
  process.exit(1);
}
// Keep HTML comments + both script tags as HTML fragment for site footer injection
const htmlChunk = t.slice(start, nmenu).trimEnd();
fs.writeFileSync("tmp-site-footer-auth-chunk.html", htmlChunk + "\n");
console.log("auth html chunk chars", htmlChunk.length);

// Also build a pure JS file of both IIFEs concatenated for hosted use
const scripts = [...htmlChunk.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);
const js = scripts.join("\n;\n");
const jsPath = "public/marketing/dealality-site-footer-auth.v20260729a.js";
fs.writeFileSync(jsPath, js);
const buf = fs.readFileSync(jsPath);
console.log("js bytes", buf.length, "md5", crypto.createHash("md5").update(buf).digest("hex"));
