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

const actions = payload.actions || [];
const decisions = payload.hotelDecisions?.items || [];
if (actions.length !== decisions.length) {
  console.error("FAIL count mismatch", actions.length, decisions.length);
  process.exit(1);
}
if (!/4 hotel-specific actions/i.test(payload.actionOverviewIntro || "")) {
  console.error("FAIL overview intro", payload.actionOverviewIntro);
  process.exit(1);
}
if ((payload.actionOverview || []).length !== actions.length) {
  console.error("FAIL overview length");
  process.exit(1);
}
console.log(
  JSON.stringify(
    { ok: true, gate: "test:ava-action-count-consistency-v1", count: actions.length },
    null,
    2
  )
);
