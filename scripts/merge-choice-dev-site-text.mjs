/**
 * Merge fixtures/choice-dev-site-text/*.txt into one markdown file.
 * node scripts/merge-choice-dev-site-text.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const IN_DIR = path.join(ROOT, "fixtures", "choice-dev-site-text");
const OUT = path.join(ROOT, "fixtures", "choice-dev-site-all-text.md");

const files = fs
  .readdirSync(IN_DIR)
  .filter((f) => f.endsWith(".txt"))
  .sort();

const parts = [
  "# Choice Hotels Development — site text export",
  "",
  `Generated: ${new Date().toISOString()}`,
  `Pages: ${files.length}`,
  "",
];

for (const f of files) {
  const body = fs.readFileSync(path.join(IN_DIR, f), "utf8");
  const title = body.split("\n")[0].replace(/^# /, "") || f;
  parts.push(`---`, "", `## ${title}`, "", "```", body, "```", "");
}

fs.writeFileSync(OUT, parts.join("\n"), "utf8");
console.log(`Wrote ${OUT} (${parts.length} lines)`);
