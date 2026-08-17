/**
 * Extract plain text from a Scribd HTML capture (contentUrl JSONP pages embedded in page).
 *
 *   node scripts/extract-scribd-text-from-capture.mjs --html "G:/…/Hilton Develop APAC Brochure.html" --out "G:/…/Hilton Develop APAC Brochure.txt"
 */
import fs from "fs";
import path from "path";

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}

const htmlPath = arg("--html");
const outPath = arg("--out");

function stripHtml(s) {
  return s
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, " ")
    .trim();
}

async function fetchJsonp(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0 (compatible; DealalityReferenceCapture/1.0)" },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
  const raw = await res.text();
  const jsonStart = raw.indexOf("(");
  const jsonEnd = raw.lastIndexOf(")");
  if (jsonStart < 0 || jsonEnd <= jsonStart) return "";
  const payload = JSON.parse(raw.slice(jsonStart + 1, jsonEnd));
  const html = Array.isArray(payload) ? payload[0] : payload?.content || payload?.html || "";
  return stripHtml(String(html || ""));
}

async function main() {
  if (!htmlPath || !outPath) {
    console.error("Usage: --html path/to/capture.html --out path/to/output.txt");
    process.exit(1);
  }
  const html = fs.readFileSync(htmlPath, "utf8");
  const urls = [...html.matchAll(/contentUrl:\s*"(https:\/\/html\.scribdassets\.com\/[^"]+)"/g)].map((m) => m[1]);
  const unique = [...new Set(urls)];
  if (!unique.length) {
    console.error("No contentUrl entries found — Scribd capture may be a login wall.");
    process.exit(1);
  }
  console.log("Pages:", unique.length);
  const parts = [];
  for (let i = 0; i < unique.length; i++) {
    const text = await fetchJsonp(unique[i]);
    if (text) parts.push(text);
    if ((i + 1) % 10 === 0) console.log(`  fetched ${i + 1}/${unique.length}`);
  }
  const body = parts.join("\n\n");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, body, "utf8");
  console.log("Wrote", outPath, `(${body.length} chars, ${parts.length} pages)`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
