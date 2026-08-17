import fs from "fs";
import path from "path";
import { execSync } from "child_process";
import { pathToFileURL } from "url";

const ROOT = process.cwd();
const tracked = new Set(
  execSync("git ls-files", { encoding: "utf8" })
    .trim()
    .split(/\r?\n/)
    .map((p) => p.replace(/\\/g, "/"))
);

const queue = ["server.js"];
const seen = new Set();
const missing = [];

function resolveImport(fromFile, spec) {
  if (!spec.startsWith("./") && !spec.startsWith("../")) return null;
  const base = path.dirname(fromFile);
  const raw = path.normalize(path.join(base, spec)).replace(/\\/g, "/");
  const candidates = [
    raw,
    raw + ".js",
    raw + ".mjs",
    path.join(raw, "index.js").replace(/\\/g, "/"),
  ];
  for (const c of candidates) {
    if (fs.existsSync(c) && fs.statSync(c).isFile()) return c;
  }
  return null;
}

function extractImports(filePath) {
  const src = fs.readFileSync(filePath, "utf8");
  return [...src.matchAll(/from\s+["']([^"']+)["']/g)].map((m) => m[1]);
}

while (queue.length) {
  const file = queue.shift();
  if (!file || seen.has(file)) continue;
  seen.add(file);
  if (!fs.existsSync(file)) continue;
  if (!tracked.has(file.replace(/\\/g, "/"))) {
    missing.push(file.replace(/\\/g, "/"));
  }
  for (const spec of extractImports(file)) {
    const resolved = resolveImport(file, spec);
    if (resolved) queue.push(resolved);
  }
}

console.log(JSON.stringify([...new Set(missing)].sort(), null, 2));
