import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PR_ID = "adp_mr_radisson_blu_toronto_dow_2026-11_v4";
const pr = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/ai-demand-positioning/monthly-review/archive/adp_radisson_blu_toronto_downtown/2026-11",
      PR_ID,
      "review.json"
    ),
    "utf8"
  )
);

const blob = JSON.stringify(pr);
const banned = [
  /concentrated competitive displacement by Bisha/i,
  /helps explain why/i,
  /closing this supports inclusion/i,
  /substitution pattern/i,
  /management-response program/i,
  /reliable lat/i,
  /a upper upscale/i,
];
// Allow explicit negation of management-response program
const cleaned = blob.replace(
  /does not justify a management-response program/gi,
  "DOES_NOT_JUSTIFY_TA_PROGRAM"
);
for (const re of banned) {
  if (re.test(cleaned)) {
    console.error("FAIL unsupported causality/copy", String(re));
    process.exit(1);
  }
}
for (const row of pr.realityGapPrioritization || []) {
  if (/ACTIONABLE_NOW/i.test(row.priority) && /supports inclusion/i.test(row.why || "")) {
    console.error("FAIL actionable causality", row.attribute);
    process.exit(1);
  }
}
console.log(JSON.stringify({ ok: true, gate: "test:performance-review-no-unsupported-causality-v1" }, null, 2));
