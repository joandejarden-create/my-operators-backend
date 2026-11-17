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
if (/Actionable now:\s*Family-Friendly;\s*City Views/i.test(blob)) {
  console.error("FAIL actionable-now contradiction");
  process.exit(1);
}
const family = (pr.realityGapPrioritization || []).find(
  (r) => r.attribute === "family_friendly"
);
const city = (pr.realityGapPrioritization || []).find((r) => r.attribute === "city_views");
for (const row of [family, city]) {
  if (!row) {
    console.error("FAIL missing row");
    process.exit(1);
  }
  if (/ACTIONABLE_NOW/i.test(row.priority)) {
    console.error("FAIL row still ACTIONABLE_NOW", row.attribute, row.priority);
    process.exit(1);
  }
}
if (!/Monitor \/ validate representation first/i.test(blob)) {
  console.error("FAIL missing monitor/validate summary");
  process.exit(1);
}
console.log(
  JSON.stringify(
    { ok: true, gate: "test:performance-review-priority-summary-consistency-v1" },
    null,
    2
  )
);
