#!/usr/bin/env node
import { readdirSync } from "fs";
import { execSync } from "child_process";

const fixtures = readdirSync("fixtures")
  .filter(
    (f) =>
      f.startsWith("travel-infrastructure-") &&
      f.endsWith(".json") &&
      !f.includes("sample") &&
      !f.includes("additional-types")
  )
  .sort();

const pending = [];
const done = [];
const errors = [];

for (const f of fixtures) {
  const rel = `fixtures/${f}`;
  try {
    const out = execSync(
      `node scripts/import-travel-infrastructure-commit.mjs --file "${rel}" --require-verified-fixture`,
      { encoding: "utf8" }
    );
    const m = out.match(/Ready to import: (\d+) valid rows/);
    const n = m ? parseInt(m[1], 10) : -1;
    if (n > 0) pending.push({ file: f, n });
    else done.push(f);
  } catch (e) {
    errors.push({ file: f, err: (e.stdout || e.message || "").split("\n").slice(-3).join(" ") });
  }
}

console.log(
  `PENDING: ${pending.length} files, ${pending.reduce((s, x) => s + x.n, 0)} rows`
);
for (const p of pending) console.log(`  ${p.n}\t${p.file}`);
console.log(`\nDONE (0 new rows): ${done.length} files`);
if (errors.length) {
  console.log(`\nERRORS: ${errors.length}`);
  for (const e of errors) console.log(`  ${e.file}: ${e.err}`);
}
