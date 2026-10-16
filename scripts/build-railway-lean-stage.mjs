#!/usr/bin/env node
/**
 * Build a filesystem staging tree that contains only paths NOT matched by
 * .railwayignore. Use for Railway CLI uploads when ignore-crate behavior
 * diverges from expected pack size (Cloudflare 413).
 *
 *   node scripts/build-railway-lean-stage.mjs --out <abs-dir>
 */
import fs from "node:fs";
import path from "node:path";
import ignore from "ignore";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outIdx = process.argv.indexOf("--out");
const stage =
  outIdx >= 0
    ? path.resolve(process.argv[outIdx + 1])
    : path.resolve(root, "..", "..", "_railway-lean-stage");

const ig = ignore().add(
  fs.readFileSync(path.join(root, ".railwayignore"), "utf8")
);

function toPosix(p) {
  return p.split(path.sep).join("/");
}

function walk(relDir, acc = []) {
  const absDir = relDir ? path.join(root, relDir) : root;
  for (const ent of fs.readdirSync(absDir, { withFileTypes: true })) {
    if (ent.name === ".git" || ent.name === "node_modules") continue;
    const rel = relDir ? toPosix(path.join(relDir, ent.name)) : ent.name;
    if (ent.isDirectory()) walk(rel, acc);
    else if (!ig.ignores(rel)) acc.push(rel);
  }
  return acc;
}

if (fs.existsSync(stage)) {
  fs.rmSync(stage, { recursive: true, force: true });
}
fs.mkdirSync(stage, { recursive: true });

const files = walk("");
let total = 0;
for (const rel of files) {
  const src = path.join(root, rel);
  const dst = path.join(stage, rel);
  fs.mkdirSync(path.dirname(dst), { recursive: true });
  fs.copyFileSync(src, dst);
  total += fs.statSync(src).size;
}

// Stage is already filtered; keep CLI from re-expanding parent trees.
fs.writeFileSync(
  path.join(stage, ".railwayignore"),
  ["node_modules/", ".git/", ""].join("\n")
);

const linkSrc = path.join(root, ".railway");
if (fs.existsSync(linkSrc)) {
  fs.cpSync(linkSrc, path.join(stage, ".railway"), { recursive: true });
}

console.log(
  JSON.stringify(
    {
      root,
      stage,
      files: files.length,
      bytes: total,
      mb: Number((total / 1e6).toFixed(1)),
    },
    null,
    2
  )
);
