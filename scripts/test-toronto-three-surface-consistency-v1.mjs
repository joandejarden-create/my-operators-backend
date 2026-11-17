import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";

const published = loadPublishedReport(PROPERTY_ID);
const pr = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/ai-demand-positioning/monthly-review/archive",
      PROPERTY_ID,
      "2026-11/adp_mr_radisson_blu_toronto_dow_2026-11_v4/review.json"
    ),
    "utf8"
  )
);
const ava = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/ai-visibility-audit-action-plan/payloads/radisson-blu-toronto-downtown-v1.json"
    ),
    "utf8"
  )
);

const need = {
  consideration: 33.2,
  scenario: 38.1,
  first: 44.7,
  top3: 85.1,
  coverage: 19,
  gap: 66.7,
};

const pub = {
  consideration: published.executiveMetrics?.considerationRate?.rate,
  scenario: published.executiveMetrics?.scenarioPresence?.rate,
  first: published.executiveMetrics?.rankMetrics?.numberOneAppearanceRate,
  top3: published.executiveMetrics?.rankMetrics?.topThreeAppearanceRate,
  coverage: published.realityGap?.coveragePercent,
  gap: published.realityGap?.gapScore,
};

function kpi(label) {
  return (pr.kpis || []).find((k) => k.label === label)?.currentValue;
}
const prVals = {
  consideration: kpi("AI Consideration"),
  scenario: kpi("Scenario Presence"),
  first: kpi("#1 Appearance"),
  top3: kpi("Top-3 Appearance"),
  coverage: kpi("Reality Coverage"),
  gap: kpi("Reality Gap"),
};

for (const [k, v] of Object.entries(need)) {
  if (pub[k] !== v && !(k === "coverage" && pub[k] === 19.0)) {
    // coverage may be 19 or 19.0
    if (Number(pub[k]) !== Number(v)) {
      console.error("FAIL published", k, pub[k], "want", v);
      process.exit(1);
    }
  }
  if (Number(prVals[k]) !== Number(v)) {
    console.error("FAIL pr", k, prVals[k], "want", v);
    process.exit(1);
  }
}

const avaBlob = JSON.stringify(ava);
if (!avaBlob.includes("33.2%") || !avaBlob.includes("38.1%")) {
  console.error("FAIL ava missing locked metrics");
  process.exit(1);
}
if (/AI Consideration is 29%/.test(JSON.stringify(published.executiveRead || {}))) {
  console.error("FAIL published executiveRead 29%");
  process.exit(1);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "test:toronto-three-surface-consistency-v1",
      pub,
      prVals,
      avaActions: (ava.actions || []).length,
    },
    null,
    2
  )
);
