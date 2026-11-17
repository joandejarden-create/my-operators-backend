/**
 * Gate: Toronto Performance Review + AVA Audit cross-consistency V2.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE = "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const PR_ID = "adp_mr_radisson_blu_toronto_dow_2026-11_v3";

function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

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

if (ava.property?.propertyId !== PROPERTY_ID) fail("ava propertyId");
if (pr.property?.propertyId !== PROPERTY_ID) fail("pr propertyId");
if (!/Toronto,\s*Ontario,\s*Canada/i.test(ava.property?.coverLocation || "")) {
  fail("ava coverLocation");
}
if (!/Toronto,\s*Ontario,\s*Canada/i.test(pr.property?.market || pr.property?.coverLocation || "")) {
  fail("pr location");
}
if (pr.reporting?.currentPeriodId !== CORE) fail("pr core period");

const need = [
  ["AI Consideration", "33.2%"],
  ["Scenario Presence", "38.1%"],
  ["#1 Appearance", "44.7%"],
  ["Top-3 Appearance", "85.1%"],
  ["Reality Coverage", "19.0%"],
  ["Reality Gap", "66.7%"],
];
for (const [label, display] of need) {
  const hit = (pr.kpis || []).some(
    (k) => k.label === label && String(k.currentDisplay) === display
  );
  if (!hit) fail(`pr kpi missing ${label}=${display}`);
}

const exec = JSON.stringify(pr.executiveAssessment || "");
if (/concentrated competitive displacement by Bisha/i.test(exec)) {
  fail("bisha framed as primary");
}
if (!/38\.1%|Scenario Presence/i.test(exec)) fail("primary constraint missing");

if ((ava.actions || []).length !== (ava.hotelDecisions?.items || []).length) {
  fail("ava action/decision count mismatch");
}

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:toronto-report-cross-consistency-v2",
      avaActions: ava.actions.length,
      prKpis: pr.kpis.length,
    },
    null,
    2
  )
);
