import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const payload = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/ai-visibility-audit-action-plan/payloads/radisson-blu-toronto-downtown-v1.json"
    ),
    "utf8"
  )
);

function walk(node, trail = []) {
  if (!node || typeof node !== "object") return;
  if (Array.isArray(node)) {
    for (let i = 0; i < node.length; i++) walk(node[i], trail.concat(i));
    return;
  }
  for (const [k, v] of Object.entries(node)) {
    if (typeof v === "string") {
      if (/^SOURCE EVIDENCE$/i.test(v.trim()) || (/^Source$/i.test(v.trim()) && /evidence/i.test(trail.join(".")))) {
        console.error("FAIL placeholder", trail.concat(k).join("."), v);
        process.exit(1);
      }
    } else walk(v, trail.concat(k));
  }
}
walk(payload);

for (const a of payload.actions || []) {
  const ev = a.supportingEvidence || [];
  if (!ev.length) {
    console.error("FAIL action missing evidence", a.actionId);
    process.exit(1);
  }
  for (const e of ev) {
    if (!e.label || (!e.url && !e.note)) {
      console.error("FAIL thin evidence", a.actionId, e);
      process.exit(1);
    }
  }
}

console.log(JSON.stringify({ ok: true, gate: "test:ava-no-placeholder-evidence-v1" }, null, 2));
