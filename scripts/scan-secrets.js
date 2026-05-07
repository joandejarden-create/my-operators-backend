#!/usr/bin/env node
import fs from "fs";
import path from "path";

const ROOT = process.cwd();
const SKIP_DIRS = new Set([
  ".git",
  "node_modules",
  ".next",
  "dist",
  "build",
  ".cache",
  ".cursor",
]);

const SCAN_EXTENSIONS = new Set([
  ".js",
  ".mjs",
  ".cjs",
  ".ts",
  ".tsx",
  ".jsx",
  ".json",
  ".html",
  ".md",
  ".txt",
  ".env",
  ".example",
  ".py",
  ".ps1",
  ".sh",
  ".yml",
  ".yaml",
]);

const SECRET_PATTERNS = [
  { name: "Airtable PAT", regex: /\bpat[A-Za-z0-9]{10,}\.[a-f0-9]{40,}\b/g },
  { name: "Bearer Airtable PAT", regex: /Bearer\s+pat[A-Za-z0-9]{10,}\.[a-f0-9]{40,}/gi },
  {
    name: "Hardcoded Airtable API key assignment",
    regex: /AIRTABLE_API_KEY\s*[:=]\s*["']pat[A-Za-z0-9]{10,}\.[a-f0-9]{40,}["']/g,
  },
  {
    name: "Authorization Bearer PAT literal",
    regex: /Authorization\s*[:=]\s*["']Bearer\s+pat[A-Za-z0-9]{10,}\.[a-f0-9]{40,}["']/g,
  },
];

function shouldScan(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (SCAN_EXTENSIONS.has(ext)) return true;
  return filePath.endsWith(".env.example") || filePath.endsWith(".local.example");
}

function walk(dir, out) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (!SKIP_DIRS.has(entry.name)) {
        walk(full, out);
      }
      continue;
    }
    if (shouldScan(full)) {
      out.push(full);
    }
  }
}

function lineNumberForIndex(content, index) {
  return content.slice(0, index).split("\n").length;
}

const files = [];
walk(ROOT, files);

const findings = [];
for (const file of files) {
  let content = "";
  try {
    content = fs.readFileSync(file, "utf8");
  } catch {
    continue;
  }

  for (const pattern of SECRET_PATTERNS) {
    pattern.regex.lastIndex = 0;
    let match;
    while ((match = pattern.regex.exec(content)) !== null) {
      findings.push({
        file: path.relative(ROOT, file),
        line: lineNumberForIndex(content, match.index),
        type: pattern.name,
        snippet: match[0].slice(0, 120),
      });
    }
  }
}

if (findings.length === 0) {
  console.log("Secret scan passed: no hardcoded credential patterns found.");
  process.exit(0);
}

console.log(`Secret scan found ${findings.length} potential issue(s):`);
for (const finding of findings) {
  console.log(`- ${finding.file}:${finding.line} [${finding.type}] ${finding.snippet}`);
}
process.exit(1);

