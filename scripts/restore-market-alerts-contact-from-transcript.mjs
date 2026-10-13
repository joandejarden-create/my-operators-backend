#!/usr/bin/env node
/**
 * Restore lib/market-alerts-contact/* from agent transcript Write/StrReplace history.
 * These files were built but never committed (clean-tree upload omitted untracked deps).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const transcript = path.join(
  process.env.USERPROFILE || "",
  ".cursor/projects/c-Dev-deal-capture-proxy/agent-transcripts/851ef301-ba07-4b6a-97eb-c3b3cd14113d/851ef301-ba07-4b6a-97eb-c3b3cd14113d.jsonl"
);

const lines = fs.readFileSync(transcript, "utf8").split(/\n/).filter(Boolean);
const byPath = new Map();

function normalizePath(p) {
  const s = String(p || "").replace(/\\/g, "/");
  const idx = s.indexOf("lib/market-alerts-contact/");
  if (idx < 0) return null;
  return s.slice(idx);
}

for (const line of lines) {
  let obj;
  try {
    obj = JSON.parse(line);
  } catch {
    continue;
  }
  const content = obj?.message?.content;
  if (!Array.isArray(content)) continue;
  for (const part of content) {
    if (part?.type !== "tool_use") continue;
    const name = part.name;
    const input = part.input || {};
    const norm = normalizePath(input.path);
    if (!norm) continue;
    if (name === "Write" && typeof input.contents === "string") {
      byPath.set(norm, input.contents);
    } else if (
      name === "StrReplace" &&
      byPath.has(norm) &&
      input.old_string != null &&
      input.new_string != null
    ) {
      let cur = byPath.get(norm);
      if (cur.includes(input.old_string)) {
        cur = input.replace_all
          ? cur.split(input.old_string).join(input.new_string)
          : cur.replace(input.old_string, input.new_string);
        byPath.set(norm, cur);
      } else {
        console.warn("StrReplace miss:", norm);
      }
    }
  }
}

console.log("restored_count", byPath.size);
for (const [rel, contents] of byPath) {
  const out = path.join(ROOT, rel);
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, contents);
  console.log("wrote", rel, contents.length);
}
