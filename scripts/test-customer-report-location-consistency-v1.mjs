import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const PR_ID = "adp_mr_radisson_blu_toronto_dow_2026-11_v4";

const ava = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/ai-visibility-audit-action-plan/payloads/radisson-blu-toronto-downtown-v1.json"
    ),
    "utf8"
  )
);
const pr = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/ai-demand-positioning/monthly-review/archive",
      PROPERTY_ID,
      "2026-11",
      PR_ID,
      "review.json"
    ),
    "utf8"
  )
);

const want = /Toronto,\s*Ontario,\s*Canada/;
if (!want.test(ava.property?.coverLocation || "")) {
  console.error("FAIL ava coverLocation", ava.property?.coverLocation);
  process.exit(1);
}
if (!want.test(pr.property?.market || "") && !want.test(pr.property?.coverLocation || "")) {
  console.error("FAIL pr location", pr.property);
  process.exit(1);
}
console.log(JSON.stringify({ ok: true, gate: "test:customer-report-location-consistency-v1" }, null, 2));
