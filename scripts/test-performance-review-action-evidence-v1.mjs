import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PR_ID = "adp_mr_radisson_blu_toronto_dow_2026-11_v3";
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

for (const a of pr.actionAgenda || []) {
  if (!Array.isArray(a.evidenceRefs) || !a.evidenceRefs.length) {
    console.error("FAIL orphan action", a.actionId);
    process.exit(1);
  }
  if (!Array.isArray(a.definitionOfDone) || !a.definitionOfDone.length) {
    console.error("FAIL definitionOfDone", a.actionId);
    process.exit(1);
  }
  if (!a.managementPriority) {
    console.error("FAIL managementPriority", a.actionId);
    process.exit(1);
  }
}
console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:performance-review-action-evidence-v1",
      actions: (pr.actionAgenda || []).length,
    },
    null,
    2
  )
);
