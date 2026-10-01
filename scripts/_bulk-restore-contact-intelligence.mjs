#!/usr/bin/env node
/**
 * Bulk-restore all lib/hotel-intelligence/contact-intelligence/* Writes from transcripts,
 * then fill gaps from artifacts astra source snapshot if present.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const transcriptsRoot = path.join(
  process.env.USERPROFILE || "",
  ".cursor/projects/c-Dev-deal-capture-proxy/agent-transcripts"
);
const needle = "lib/hotel-intelligence/contact-intelligence/";

function walk(d, acc = []) {
  for (const e of fs.readdirSync(d, { withFileTypes: true })) {
    const p = path.join(d, e.name);
    if (e.isDirectory()) walk(p, acc);
    else if (e.name.endsWith(".jsonl")) acc.push(p);
  }
  return acc;
}

function normalize(p) {
  const s = String(p || "").replace(/\\/g, "/");
  const idx = s.indexOf(needle);
  if (idx < 0) return null;
  return s.slice(idx);
}

const byPath = new Map();

for (const f of walk(transcriptsRoot)) {
  const lines = fs.readFileSync(f, "utf8").split(/\n/);
  for (const line of lines) {
    if (!line.includes("contact-intelligence") || !line.includes("tool_use")) continue;
    let o;
    try {
      o = JSON.parse(line);
    } catch {
      continue;
    }
    const content = o?.message?.content;
    if (!Array.isArray(content)) continue;
    for (const part of content) {
      if (part?.type !== "tool_use") continue;
      const norm = normalize(part.input?.path);
      if (!norm) continue;
      if (part.name === "Write" && typeof part.input?.contents === "string") {
        byPath.set(norm, part.input.contents);
      } else if (
        part.name === "StrReplace" &&
        byPath.has(norm) &&
        part.input?.old_string != null
      ) {
        let cur = byPath.get(norm);
        if (cur.includes(part.input.old_string)) {
          cur = part.input.replace_all
            ? cur.split(part.input.old_string).join(part.input.new_string)
            : cur.replace(part.input.old_string, part.input.new_string);
          byPath.set(norm, cur);
        }
      }
    }
  }
}

console.log("transcript_files", byPath.size);
let wrote = 0;
for (const [rel, contents] of byPath) {
  const out = path.join(ROOT, rel);
  // Don't overwrite existing non-empty files that are longer (prefer disk if newer/complete)
  if (fs.existsSync(out)) {
    const existing = fs.readFileSync(out, "utf8");
    if (existing.length >= contents.length) continue;
  }
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, contents);
  wrote++;
  console.log("wrote", rel, contents.length);
}
console.log("wrote_count", wrote);

// Fill from astra artifact source
const astraSrc = path.join(
  ROOT,
  "artifacts/astra-hpc-ownership-contact-review-20260918/source/lib/hotel-intelligence/contact-intelligence"
);
if (fs.existsSync(astraSrc)) {
  function copyMissing(srcDir, destDir) {
    for (const e of fs.readdirSync(srcDir, { withFileTypes: true })) {
      const s = path.join(srcDir, e.name);
      const d = path.join(destDir, e.name);
      if (e.isDirectory()) {
        fs.mkdirSync(d, { recursive: true });
        copyMissing(s, d);
      } else if (!fs.existsSync(d)) {
        fs.copyFileSync(s, d);
        console.log("astra_copy", path.relative(ROOT, d));
      }
    }
  }
  copyMissing(astraSrc, path.join(ROOT, needle));
}
