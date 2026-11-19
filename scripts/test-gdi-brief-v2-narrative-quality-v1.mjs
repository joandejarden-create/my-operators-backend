#!/usr/bin/env node
/**
 * Brief v2 — before/after narrative quality on frozen gold/weak fixtures ($0).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { scoreCommercialIntelligenceBriefV2 } from "../lib/group-demand-intelligence/presentation/gdi-commercial-intelligence-brief-quality-score-v1.js";
import { composeCommercialIntelligenceBriefV2 } from "../lib/group-demand-intelligence/presentation/gdi-commercial-intelligence-brief-v2-composer-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIX = path.join(ROOT, "fixtures/gdi-brief-v2-narrative-quality-v1");
const REPORT = path.join(ROOT, "reports/gdi-brief-v2-narrative-quality-v1");
const NOW = new Date("2026-11-19T12:00:00.000Z");

const index = JSON.parse(fs.readFileSync(path.join(FIX, "index.json"), "utf8"));
const rows = [];

for (const sample of index.samples) {
  const beforeOpp = JSON.parse(
    fs.readFileSync(path.join(FIX, `${sample.label}.json`), "utf8"),
  );
  const before = scoreCommercialIntelligenceBriefV2(beforeOpp, { now: NOW });
  const composed = composeCommercialIntelligenceBriefV2(beforeOpp, { now: NOW });
  const after = composed.score;
  const afterOpp = composed.opportunity;

  assert.ok(afterOpp.whatIsHappening);
  assert.ok(afterOpp.whyThisHotel);
  assert.ok(afterOpp.potentialHotelBusiness);
  assert.ok(afterOpp.recommendedNextStep);
  assert.ok(!/research verification date/i.test(afterOpp.whatIsHappening));
  assert.ok(!/research verification date/i.test(afterOpp.potentialHotelBusiness || ""));

  if (sample.label === "nrc-ric") {
    assert.equal(afterOpp.briefV2.commercialPosture, "PAST_CYCLE_DO_NOT_PURSUE");
    assert.ok(/do not pursue|past/i.test(afterOpp.recommendedNextStep));
    assert.ok(!before.defects.includes("PAST_EVENT_ACTIVE_PURSUE") || after.total >= before.total);
  }
  if (sample.label === "imc-construction") {
    assert.ok(!/Research verification date/i.test(afterOpp.whatIsHappening));
    assert.ok(after.total >= before.total);
  }
  if (sample.label === "owwa") {
    assert.ok(
      /overflow|secondary/i.test(afterOpp.whyThisHotel + afterOpp.recommendedNextStep),
    );
  }
  if (sample.label === "nice" || sample.label === "pdac") {
    assert.ok(after.total >= 18, `${sample.label} gold total ${after.total}`);
  }

  rows.push({
    label: sample.label,
    class: sample.class,
    id: sample.id,
    title: sample.title,
    before: { total: before.total, dimensions: before.dimensions, defects: before.defects },
    after: { total: after.total, dimensions: after.dimensions, defects: after.defects },
    delta: after.total - before.total,
    changes: composed.changes,
    afterBrief: {
      whatIsHappening: afterOpp.whatIsHappening,
      whyThisHotel: afterOpp.whyThisHotel,
      potentialHotelBusiness: afterOpp.potentialHotelBusiness,
      whyActNow: afterOpp.whyActNow,
      recommendedNextStep: afterOpp.recommendedNextStep,
      commercialPosture: afterOpp.briefV2?.commercialPosture,
      uncertainties: afterOpp.uncertainties,
    },
    beforeBrief: {
      whatIsHappening: beforeOpp.whatIsHappening,
      whyThisHotel: beforeOpp.whyThisHotel,
      potentialHotelBusiness: beforeOpp.potentialHotelBusiness,
      whyActNow: beforeOpp.whyActNow,
      recommendedNextStep: beforeOpp.recommendedNextStep,
    },
  });
}

fs.mkdirSync(REPORT, { recursive: true });
const summary = {
  version: "gdi-brief-v2-narrative-quality-v1",
  at: new Date().toISOString(),
  scoringAsOf: NOW.toISOString(),
  paidCalls: 0,
  airtableWrites: 0,
  samples: rows.map((r) => ({
    label: r.label,
    class: r.class,
    beforeTotal: r.before.total,
    afterTotal: r.after.total,
    delta: r.delta,
    beforeDefects: r.before.defects,
    afterDefects: r.after.defects,
  })),
};
fs.writeFileSync(path.join(REPORT, "RESULTS.json"), `${JSON.stringify({ ...summary, detail: rows }, null, 2)}\n`);
fs.writeFileSync(
  path.join(REPORT, "FOUNDER_RESULTS.md"),
  `# GDI Brief v2 — before/after narrative quality

Scored as of ${NOW.toISOString().slice(0, 10)} (fixed clock).

| Sample | Class | Before | After | Δ | Key improvement |
|---|---|---:|---:|---:|---|
${rows
  .map((r) => {
    const key =
      r.label === "nrc-ric"
        ? "Past cycle → do-not-pursue"
        : r.label === "imc-construction"
          ? "Truncation/methodology cleaned"
          : r.label === "owwa"
            ? "Overflow posture clarified"
            : r.label === "nice"
              ? "Hotel fit + pursuit composed"
              : "Gold narrative preserved/polished";
    return `| ${r.label} | ${r.class} | ${r.before.total}/30 | ${r.after.total}/30 | ${r.delta >= 0 ? "+" : ""}${r.delta} | ${key} |`;
  })
  .join("\n")}

## Example — IMC Construction (weak → improved)

**Before whatIsHappening:**  
> ${String(rows.find((r) => r.label === "imc-construction")?.beforeBrief.whatIsHappening || "").slice(0, 220)}

**After whatIsHappening:**  
> ${String(rows.find((r) => r.label === "imc-construction")?.afterBrief.whatIsHappening || "").slice(0, 220)}

## Example — NRC RIC (past cycle)

**After posture:** \`${rows.find((r) => r.label === "nrc-ric")?.afterBrief.commercialPosture}\`  
**After next step:** ${rows.find((r) => r.label === "nrc-ric")?.afterBrief.recommendedNextStep}

## Safety
Paid/Airtable/publication: 0. Single drawer contract retained for hotel GDI + PDI-3.
`,
);

console.log(JSON.stringify({ ok: true, samples: summary.samples, reportDir: REPORT }, null, 2));
