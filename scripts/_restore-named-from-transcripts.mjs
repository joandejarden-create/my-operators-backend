#!/usr/bin/env node
/** Restore specific relative paths from any agent transcript Write/StrReplace. */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const transcriptsRoot = path.join(
  process.env.USERPROFILE || "",
  ".cursor/projects/c-Dev-deal-capture-proxy/agent-transcripts"
);

const targets = process.argv.slice(2);
if (!targets.length) {
  console.error("need target relative paths");
  process.exit(1);
}

function walk(d, acc = []) {
  for (const e of fs.readdirSync(d, { withFileTypes: true })) {
    const p = path.join(d, e.name);
    if (e.isDirectory()) walk(p, acc);
    else if (e.name.endsWith(".jsonl")) acc.push(p);
  }
  return acc;
}

function matchTarget(filePath) {
  const s = String(filePath || "").replace(/\\/g, "/");
  for (const t of targets) {
    const tt = t.replace(/\\/g, "/");
    if (s.endsWith(tt) || s.includes("/" + tt)) return tt;
  }
  return null;
}

const byTarget = new Map();

for (const f of walk(transcriptsRoot)) {
  const lines = fs.readFileSync(f, "utf8").split(/\n/);
  for (const line of lines) {
    if (!line.includes("tool_use")) continue;
    if (!targets.some((t) => line.includes(path.basename(t)))) continue;
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
      const t = matchTarget(part.input?.path);
      if (!t) continue;
      if (part.name === "Write" && typeof part.input?.contents === "string") {
        byTarget.set(t, part.input.contents);
        console.log("Write", t, "from", path.basename(path.dirname(f)), "len", part.input.contents.length);
      } else if (
        part.name === "StrReplace" &&
        byTarget.has(t) &&
        part.input?.old_string != null
      ) {
        let cur = byTarget.get(t);
        if (cur.includes(part.input.old_string)) {
          cur = part.input.replace_all
            ? cur.split(part.input.old_string).join(part.input.new_string)
            : cur.replace(part.input.old_string, part.input.new_string);
          byTarget.set(t, cur);
          console.log("StrReplace", t);
        } else {
          console.warn("StrReplace miss", t);
        }
      }
    }
  }
}

for (const t of targets) {
  const tt = t.replace(/\\/g, "/");
  if (!byTarget.has(tt)) {
    console.error("MISSING", tt);
    continue;
  }
  const out = path.join(ROOT, tt);
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, byTarget.get(tt));
  console.log("wrote", out, byTarget.get(tt).length);
}
