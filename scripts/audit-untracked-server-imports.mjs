#!/usr/bin/env node
import { readFileSync, existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { execSync } from "node:child_process";

const root = process.cwd();
const tracked = new Set(
  execSync("git ls-files", { encoding: "utf8" })
    .trim()
    .split(/\r?\n/)
    .filter(Boolean)
);
const queue = ["server.js"];
const seen = new Set();
const untracked = new Set();
const missing = new Set();

function resolveImport(fromFile, spec) {
  if (!spec.startsWith(".")) return null;
  const base = resolve(dirname(fromFile), spec);
  const candidates = [base, `${base}.js`, `${base}.mjs`, join(base, "index.js")];
  for (const c of candidates) {
    if (existsSync(c)) return c.replace(/\\/g, "/");
  }
  return base.replace(/\\/g, "/");
}

while (queue.length) {
  const rel = queue.shift();
  if (seen.has(rel)) continue;
  seen.add(rel);
  const abs = join(root, rel);
  if (!existsSync(abs)) {
    missing.add(rel);
    continue;
  }
  const text = readFileSync(abs, "utf8");
  const re = /from\s+['"](\.[^'"]+)['"]/g;
  let m;
  while ((m = re.exec(text))) {
    const target = resolveImport(abs, m[1]);
    if (!target) continue;
    const normalizedRoot = root.replace(/\\/g, "/");
    const normalizedTarget = target.replace(/\\/g, "/");
    const relTarget = normalizedTarget.startsWith(`${normalizedRoot}/`)
      ? normalizedTarget.slice(normalizedRoot.length + 1)
      : normalizedTarget;
    if (!tracked.has(relTarget)) untracked.add(relTarget);
    if (!existsSync(join(root, relTarget)) && !existsSync(target)) missing.add(relTarget);
    else queue.push(relTarget);
  }
}

console.log(`UNTRACKED (${untracked.size}):`);
[...untracked].sort().forEach((f) => console.log(f));
console.log(`MISSING (${missing.size}):`);
[...missing].sort().forEach((f) => console.log(f));
