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
const a1 = (payload.actions || []).find((a) => a.actionId === "AVA-RBTD-001");
const a2 = (payload.actions || []).find((a) => a.actionId === "AVA-RBTD-005");
if (!a1 || !a2) {
  console.error("FAIL missing actions");
  process.exit(1);
}
const c1 = JSON.stringify(a1.completionCriteria || {});
const c2 = JSON.stringify(a2.completionCriteria || {});
if (/pets\/EV|family\/harbourfront|meetings inventory/i.test(c1)) {
  console.error("FAIL action1 criteria not dining-specific", c1);
  process.exit(1);
}
if (!/restaurant\/lounge|dining|Booking\.com/i.test(c1)) {
  console.error("FAIL action1 missing dining criteria", c1);
  process.exit(1);
}
if (/pet|EV|family|harbourfront|dining status/i.test(c2) && !/meeting/i.test(c2)) {
  console.error("FAIL action2 unrelated");
  process.exit(1);
}
if (!/venue count|meeting space|Cvent|Choice listing aligned/i.test(c2)) {
  console.error("FAIL action2 missing meetings criteria", c2);
  process.exit(1);
}
console.log(
  JSON.stringify(
    { ok: true, gate: "test:ava-action-specific-completion-criteria-v1" },
    null,
    2
  )
);
