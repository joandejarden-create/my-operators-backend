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

const banned = [
  /opportunity owner/i,
  /outreach or listing update request/i,
  /destination\/partner page/i,
  /partner contact/i,
];

for (const a of payload.actions || []) {
  const blob = JSON.stringify(a.completionCriteria || {});
  for (const re of banned) {
    if (re.test(blob)) {
      console.error("FAIL mismatched completion criteria", a.actionId, re);
      process.exit(1);
    }
  }
  if (!a.completionCriteria || typeof a.completionCriteria !== "object") {
    console.error("FAIL missing completionCriteria", a.actionId);
    process.exit(1);
  }
}

console.log(JSON.stringify({ ok: true, gate: "test:ava-action-completion-criteria-match-v1" }, null, 2));
