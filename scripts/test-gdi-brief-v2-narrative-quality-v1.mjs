#!/usr/bin/env node
/**
 * Brief v2 — before/after narrative quality + evidence-fidelity on frozen fixtures ($0).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { scoreCommercialIntelligenceBriefV2 } from "../lib/group-demand-intelligence/presentation/gdi-commercial-intelligence-brief-quality-score-v1.js";
import {
  composeCommercialIntelligenceBriefV2,
  competitorPlacedOtherHotels,
  thisHotelListedPartner,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-intelligence-brief-v2-composer-v1.js";
import { evaluateBriefEvidenceFidelity } from "../lib/group-demand-intelligence/presentation/gdi-commercial-intelligence-brief-evidence-fidelity-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIX = path.join(ROOT, "fixtures/gdi-brief-v2-narrative-quality-v1");
const CATALOGS = path.join(FIX, "evidence-catalogs");
const REPORT = path.join(ROOT, "reports/gdi-brief-v2-narrative-quality-v1");
const FIDELITY_REPORT = path.join(ROOT, "reports/gdi-brief-v2-evidence-fidelity-audit-v1");
const NOW = new Date("2026-11-19T12:00:00.000Z");

const index = JSON.parse(fs.readFileSync(path.join(FIX, "index.json"), "utf8"));
const rows = [];
const fidelityRows = [];

for (const sample of index.samples) {
  const beforeOpp = JSON.parse(
    fs.readFileSync(path.join(FIX, `${sample.label}.json`), "utf8"),
  );
  const catalog = JSON.parse(
    fs.readFileSync(path.join(CATALOGS, `${sample.label}.json`), "utf8"),
  );
  const before = scoreCommercialIntelligenceBriefV2(beforeOpp, { now: NOW });
  const composed = composeCommercialIntelligenceBriefV2(beforeOpp, { now: NOW });
  const after = composed.score;
  const afterOpp = composed.opportunity;
  const fidelity = evaluateBriefEvidenceFidelity(afterOpp, catalog);

  assert.ok(afterOpp.whatIsHappening);
  assert.ok(afterOpp.whyThisHotel);
  assert.ok(afterOpp.potentialHotelBusiness);
  assert.ok(afterOpp.recommendedNextStep);
  assert.ok(!/research verification date/i.test(afterOpp.whatIsHappening));
  assert.ok(!/research verification date/i.test(afterOpp.potentialHotelBusiness || ""));
  assert.ok(
    !/market with visiting-staff and meeting demand still to be qualified/i.test(
      afterOpp.whatIsHappening + afterOpp.potentialHotelBusiness,
    ),
    `${sample.label}: generic truncation filler must not appear`,
  );
  assert.ok(
    !/credible host candidate/i.test(afterOpp.whyThisHotel || ""),
    `${sample.label}: must not claim credible host from missing venue evidence`,
  );
  assert.equal(
    fidelity.unsupportedIntroduced.length,
    0,
    `${sample.label}: unsupported patterns ${JSON.stringify(fidelity.unsupportedIntroduced)}`,
  );

  if (sample.label === "pdac") {
    assert.equal(thisHotelListedPartner(beforeOpp), true);
    assert.equal(competitorPlacedOtherHotels(beforeOpp), false);
    assert.equal(afterOpp.briefV2.commercialPosture, "LISTED_PARTNER_QUALIFY");
    assert.ok(after.total >= 18);
  }
  if (sample.label === "nrc-ric") {
    assert.equal(afterOpp.briefV2.commercialPosture, "PAST_CYCLE_DO_NOT_PURSUE");
    assert.ok(/do not pursue|past/i.test(afterOpp.recommendedNextStep));
    assert.ok(!/lodging demand path open/i.test(afterOpp.whyActNow || ""));
  }
  if (sample.label === "imc-construction") {
    assert.ok(!/Research verification date/i.test(afterOpp.whatIsHappening));
    assert.ok(!/request RFP or site-visit inclusion/i.test(afterOpp.recommendedNextStep));
    assert.ok(/Bethesda regional hub|Bethesda/i.test(afterOpp.whatIsHappening));
    assert.ok(after.total >= before.total);
  }
  if (sample.label === "owwa") {
    assert.ok(
      /overflow|secondary/i.test(afterOpp.whyThisHotel + afterOpp.recommendedNextStep),
    );
    assert.equal(afterOpp.briefV2.commercialPosture, "COMPETITOR_PLACED_OVERFLOW_ONLY");
  }
  if (sample.label === "nice") {
    assert.ok(after.total >= 18, `${sample.label} gold total ${after.total}`);
    assert.equal(afterOpp.briefV2.commercialPosture, "FUTURE_VENUE_OPEN");
    assert.ok(/unverified/i.test(afterOpp.whyThisHotel));
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
    fidelity,
  });

  fidelityRows.push({
    label: sample.label,
    class: sample.class,
    opportunityId: sample.id,
    archivedSources: catalog.archivedSources || [],
    notes: catalog.notes || null,
    fidelityScore: fidelity.fidelityScore,
    judgment: fidelity.judgment,
    wordCount: fidelity.wordCount,
    retained: fidelity.retained,
    omitted: fidelity.omitted,
    omittedMaterialBrief: fidelity.omittedMaterialBrief,
    researchAbsent: fidelity.researchAbsent,
    unsupportedIntroduced: fidelity.unsupportedIntroduced,
    defects: fidelity.defects,
    afterPosture: afterOpp.briefV2?.commercialPosture,
    compositionChanges: composed.changes,
  });
}

fs.mkdirSync(REPORT, { recursive: true });
fs.mkdirSync(FIDELITY_REPORT, { recursive: true });

const summary = {
  version: "gdi-brief-v2-narrative-quality-v1",
  amendment: "commercial-fact-retention-honesty-v1",
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
    fidelityScore: r.fidelity.fidelityScore,
    fidelityJudgment: r.fidelity.judgment,
    unsupportedCount: r.fidelity.unsupportedIntroduced.length,
  })),
};

fs.writeFileSync(
  path.join(REPORT, "RESULTS.json"),
  `${JSON.stringify({ ...summary, detail: rows }, null, 2)}\n`,
);

const fidelityDoc = `# GDI Brief v2 — Evidence-fidelity audit (Phase A)

Fixed clock: ${NOW.toISOString().slice(0, 10)}. Paid Exa calls: **0**. No invented source facts.

## Method

For each frozen fixture, compare composed Level-1 Brief fields against a curated **evidence catalog** built only from archived Exa / Webhound / opportunity JSON / contact findings. Facts marked \`ABSENT_FROM_RESEARCH\` are research gaps, not processing losses.

## Trace summary

| Sample | Fidelity | Judgment | Retained | Omitted | Research-absent | Unsupported |
|---|---:|---|---:|---:|---:|---:|
${fidelityRows
  .map(
    (r) =>
      `| ${r.label} | ${r.fidelityScore} | ${r.judgment} | ${r.retained.length} | ${r.omitted.length} | ${r.researchAbsent.length} | ${r.unsupportedIntroduced.length} |`,
  )
  .join("\n")}

## Composer honesty fixes applied

- Partner listing ≠ competitor overflow (PDAC → \`LISTED_PARTNER_QUALIFY\`)
- No “credible host” from venue TBA (NICE)
- Past cycle / selected venue does not get open-host template (NRC)
- Truncation repair never invents visiting-staff demand filler (IMC)
- Scaling accounts do not get conference RFP/site-visit bleed (IMC)

## Per-opportunity omitted material (still Level-2 / next-slice)

${fidelityRows
  .map((r) => {
    const omitted = r.omitted.length
      ? r.omitted.map((f) => `  - [${f.destination}] ${f.claim}`).join("\n")
      : "  - (none in catalog vs Level-1 blob)";
    return `### ${r.label}\n\n**Posture:** \`${r.afterPosture}\`\n\n**Omitted from Level-1 (may belong in Supporting Research):**\n${omitted}\n`;
  })
  .join("\n")}

## Two-level presentation

- **Level 1** — Commercial Intelligence Brief (existing drawer fields)
- **Level 2** — Supporting Research (lodging evidence, contacts, claims, sources, gaps)

Contract: \`docs/data-intelligence/gdi-commercial-fact-selection-contract-v1.md\`
`;

fs.writeFileSync(path.join(FIDELITY_REPORT, "FOUNDER_AUDIT.md"), fidelityDoc);
fs.writeFileSync(
  path.join(FIDELITY_REPORT, "AUDIT.json"),
  `${JSON.stringify(
    {
      version: "gdi-brief-v2-evidence-fidelity-audit-v1",
      at: new Date().toISOString(),
      scoringAsOf: NOW.toISOString(),
      paidCalls: 0,
      opportunities: fidelityRows,
    },
    null,
    2,
  )}\n`,
);

fs.writeFileSync(
  path.join(REPORT, "FOUNDER_RESULTS.md"),
  `# GDI Brief v2 — before/after narrative quality (+ evidence fidelity)

Scored as of ${NOW.toISOString().slice(0, 10)} (fixed clock). Amendment: commercial fact retention & composer honesty.

| Sample | Class | Before | After | Δ | Fidelity | Judgment | Key fix |
|---|---|---:|---:|---:|---:|---|---|
${rows
  .map((r) => {
    const key =
      r.label === "nrc-ric"
        ? "Past cycle; no open-host template"
        : r.label === "imc-construction"
          ? "No filler / no RFP bleed"
          : r.label === "owwa"
            ? "Overflow posture retained"
            : r.label === "nice"
              ? "Unverified open-venue fit (honest)"
              : "Partner posture (not competitor overflow)";
    return `| ${r.label} | ${r.class} | ${r.before.total}/30 | ${r.after.total}/30 | ${r.delta >= 0 ? "+" : ""}${r.delta} | ${r.fidelity.fidelityScore} | ${r.fidelity.judgment} | ${key} |`;
  })
  .join("\n")}

## Example — NICE (honest hotel fit)

**Before whyThisHotel:** _(null)_  

**After whyThisHotel:**  
> ${rows.find((r) => r.label === "nice")?.afterBrief.whyThisHotel || ""}

## Example — PDAC posture

**After commercialPosture:** \`${rows.find((r) => r.label === "pdac")?.afterBrief.commercialPosture}\`  
(was incorrectly \`COMPETITOR_PLACED_OVERFLOW_ONLY\` when Radisson is a listed partner)

## Example — IMC Construction

**After whatIsHappening:**  
> ${rows.find((r) => r.label === "imc-construction")?.afterBrief.whatIsHappening || ""}

**After next step:**  
> ${rows.find((r) => r.label === "imc-construction")?.afterBrief.recommendedNextStep || ""}

## Safety
Paid/Airtable/publication: 0. Single drawer contract retained for hotel GDI + PDI-3.  
Evidence audit: \`reports/gdi-brief-v2-evidence-fidelity-audit-v1/FOUNDER_AUDIT.md\`  
Fact-selection contract: \`docs/data-intelligence/gdi-commercial-fact-selection-contract-v1.md\`
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      samples: summary.samples,
      fidelityReport: "reports/gdi-brief-v2-evidence-fidelity-audit-v1/FOUNDER_AUDIT.md",
    },
    null,
    2,
  ),
);
