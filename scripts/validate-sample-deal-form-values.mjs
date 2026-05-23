#!/usr/bin/env node
/**
 * Report sample-deal fixture fields that still do not match Deal Setup select options.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import FORM_OPTIONS from "../lib/deal-setup-form-options.json" with { type: "json" };

const dir = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "fixtures", "sample-deals");

const files = fs.readdirSync(dir).filter((f) => f.endsWith(".example.json"));
let issues = 0;

function checkValue(field, value) {
  const opts = FORM_OPTIONS[field];
  if (!opts) return [];
  const problems = [];
  const values = Array.isArray(value) ? value : [value];
  for (const v of values) {
    const s = String(v).trim();
    if (!s) continue;
    if (!opts.includes(s)) problems.push({ field, value: s });
  }
  return problems;
}

for (const file of files) {
  const record = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
  const problems = [];
  for (const layer of ["referenceProperty", "fictionalDeal"]) {
    const fields = record[layer]?.fields;
    if (!fields) continue;
    for (const [field, value] of Object.entries(fields)) {
      if (Array.isArray(value)) {
        for (const p of checkValue(field, value)) problems.push(p);
      } else if (typeof value === "string" && FORM_OPTIONS[field]) {
        const opts = FORM_OPTIONS[field];
        const s = value.trim();
        if (!s) continue;
        if (opts.length && !opts.includes(s)) {
          problems.push({ field, value: s });
        }
      }
    }
  }
  if (problems.length) {
    console.log("\n", file, problems.length, "issues");
    for (const p of problems.slice(0, 15)) {
      console.log("  -", p.field, "→", JSON.stringify(p.value));
    }
    if (problems.length > 15) console.log("  ...", problems.length - 15, "more");
    issues += problems.length;
  }
}

if (!issues) console.log("All checked select fields match form options.");
else {
  console.log("\nTotal issues:", issues);
  process.exitCode = 1;
}
