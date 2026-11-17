import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const pr = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/ai-demand-positioning/monthly-review/archive/adp_radisson_blu_toronto_downtown/2026-11/adp_mr_radisson_blu_toronto_dow_2026-11_v4/review.json"
    ),
    "utf8"
  )
);
const blob = JSON.stringify(pr);
const banned = [
  /help explain why/i,
  /helps explain why/i,
  /may improve representation if source defects are confirmed/i,
  /substitution pattern/i,
  /Actionable now:\s*Family-Friendly/i,
];
for (const re of banned) {
  if (re.test(blob)) {
    console.error("FAIL residual causality/copy", String(re));
    process.exit(1);
  }
}
if (!/does not establish that this attribute caused the absence/i.test(blob)) {
  console.error("FAIL missing outdoor safe language");
  process.exit(1);
}
console.log(
  JSON.stringify(
    { ok: true, gate: "test:performance-review-no-residual-causality-v1" },
    null,
    2
  )
);
