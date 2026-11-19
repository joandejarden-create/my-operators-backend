#!/usr/bin/env node
/**
 * Commercial Fact Selection V1 — regressions on five archived evidence catalogs ($0).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { composeCommercialIntelligenceBriefV2 } from "../lib/group-demand-intelligence/presentation/gdi-commercial-intelligence-brief-v2-composer-v1.js";
import {
  reevaluateFactDestination,
  selectAndApplyCommercialFacts,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-fact-selection-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIX = path.join(ROOT, "fixtures/gdi-brief-v2-narrative-quality-v1");
const CATALOGS = path.join(FIX, "evidence-catalogs");
const REPORT = path.join(ROOT, "reports/gdi-commercial-fact-selection-v1");
const NOW = new Date("2026-11-19T12:00:00.000Z");

const index = JSON.parse(fs.readFileSync(path.join(FIX, "index.json"), "utf8"));
const rows = [];

for (const sample of index.samples) {
  const beforeOpp = JSON.parse(
    fs.readFileSync(path.join(FIX, `${sample.label}.json`), "utf8"),
  );
  const catalog = JSON.parse(
    fs.readFileSync(path.join(CATALOGS, `${sample.label}.json`), "utf8"),
  );
  const composed = composeCommercialIntelligenceBriefV2(beforeOpp, { now: NOW });
  const beforeBrief = {
    whatIsHappening: composed.opportunity.whatIsHappening,
    whyThisHotel: composed.opportunity.whyThisHotel,
    potentialHotelBusiness: composed.opportunity.potentialHotelBusiness,
    whyActNow: composed.opportunity.whyActNow,
    recommendedNextStep: composed.opportunity.recommendedNextStep,
    commercialPosture: composed.opportunity.briefV2?.commercialPosture,
  };

  const selected = selectAndApplyCommercialFacts(composed.opportunity, {
    now: NOW,
    catalog,
    durableRef: `fixtures/gdi-brief-v2-narrative-quality-v1/evidence-catalogs/${sample.label}.json`,
  });
  const after = selected.opportunity;
  const q = after.factSelectionV1.quality;

  assert.ok(after.commercialFactCatalog?.facts?.length >= 1);
  assert.ok(Array.isArray(after.supportingResearch));
  assert.equal(after.commercialFactCatalog.durableRef.includes(sample.label), true);

  if (sample.label === "pdac") {
    const bureau = after.commercialFactCatalog.facts.find(
      (f) => f.id === "pdac-no-housing-bureau",
    );
    assert.ok(bureau);
    assert.equal(bureau.selectedDestination, "BRIEF", "housing bureau must promote to L1");
    assert.ok(
      /housing bureau|Book directly/i.test(after.whatIsHappening),
      "PDAC L1 must retain direct-book / no-bureau fact",
    );
  }

  if (sample.label === "nice") {
    const fiu = after.commercialFactCatalog.facts.find((f) => f.id === "nice-fiu-ops");
    assert.equal(fiu.selectedDestination, "BRIEF");
    assert.ok(
      /FIU|New America/i.test(
        after.whatIsHappening +
          after.potentialHotelBusiness +
          JSON.stringify(after.supportingResearch),
      ),
      "FIU/New America must surface in L1 or L2",
    );
    const priv = after.commercialFactCatalog.facts.find((f) => f.id === "nice-customer-sourced");
    assert.equal(priv.selectedDestination, "SUPPORTING_HOTEL_PRIVATE");
    assert.ok(
      !/added to sourced properties/i.test(after.whatIsHappening),
      "hotel-private sourcing must not leak into Level 1 shared brief",
    );
  }

  if (sample.label === "owwa") {
    const oct = after.commercialFactCatalog.facts.find((f) => f.id === "owwa-oct20");
    const venue = after.commercialFactCatalog.facts.find((f) => f.id === "owwa-enercare");
    assert.equal(oct.selectedDestination, "BRIEF");
    assert.equal(venue.selectedDestination, "BRIEF");
    assert.ok(/October 20|Enercare/i.test(after.whatIsHappening));
    assert.ok(/Sam Petryna/i.test(after.recommendedNextStep + JSON.stringify(after.primaryContact)));
  }

  if (sample.label === "nrc-ric") {
    assert.equal(after.briefV2?.commercialPosture || composed.opportunity.briefV2?.commercialPosture, "PAST_CYCLE_DO_NOT_PURSUE");
    assert.ok(/do not pursue|past/i.test(after.recommendedNextStep + after.whyActNow));
    assert.ok(!/credible host candidate/i.test(after.whyThisHotel || ""));
  }

  if (sample.label === "imc-construction") {
    assert.ok(/Bethesda regional hub/i.test(after.whatIsHappening));
    assert.ok(!/confirmed room block/i.test(after.whatIsHappening + after.potentialHotelBusiness));
    const absent = after.commercialFactCatalog.facts.find((f) => f.id === "imc-room-block");
    assert.equal(absent.selectedDestination, "OMIT_MISSING");
  }

  // Historical date must never become future deadline
  const hist = reevaluateFactDestination(
    {
      claim: "Event was 2024",
      activityDate: "2024-05-01",
      treatAsFutureDeadline: true,
      verificationStatus: "CONFIRMED",
      commercialImportance: "HIGH",
      changesPursuit: true,
    },
    { now: NOW },
  );
  assert.equal(hist.destination, "OMIT_UNSUPPORTED");

  rows.push({
    label: sample.label,
    class: sample.class,
    id: sample.id,
    beforeBrief,
    afterBrief: {
      whatIsHappening: after.whatIsHappening,
      whyThisHotel: after.whyThisHotel,
      potentialHotelBusiness: after.potentialHotelBusiness,
      whyActNow: after.whyActNow,
      recommendedNextStep: after.recommendedNextStep,
      commercialPosture: after.briefV2?.commercialPosture || beforeBrief.commercialPosture,
    },
    supportingResearch: after.supportingResearch,
    catalogFacts: after.commercialFactCatalog.facts.map((f) => ({
      id: f.id,
      claim: f.claim,
      priorDestination: f.priorDestination,
      selectedDestination: f.selectedDestination,
      selectionReason: f.selectionReason,
      sourceUrl: f.sourceUrl || null,
      hotelPrivate: f.hotelPrivate,
    })),
    promotedIntoBrief: after.factSelectionV1.promotedIntoBrief,
    defects: after.factSelectionV1.defects,
    quality: q,
    archivedSources: catalog.archivedSources || [],
  });
}

fs.mkdirSync(REPORT, { recursive: true });
const summary = {
  version: "gdi-commercial-fact-selection-v1",
  at: new Date().toISOString(),
  scoringAsOf: NOW.toISOString(),
  paidCalls: 0,
  airtableWrites: 0,
  samples: rows.map((r) => ({
    label: r.label,
    level1Accuracy: r.quality.level1SelectionAccuracy,
    retention: r.quality.verifiedFactRetentionL1L2,
    unsupported: r.quality.unsupportedAssertionCount,
    promoted: r.promotedIntoBrief,
    defects: r.defects,
  })),
};

const md = `# GDI Commercial Fact Selection V1 — founder before/after

Fixed clock: ${NOW.toISOString().slice(0, 10)}. Paid Exa: **0**.

## Objective

Commercially important verified findings must not disappear between archived research and the customer opportunity detail.

## Quality (separate measures)

| Sample | L1 selection accuracy | L1+L2 retention | Unsupported |
|---|---:|---:|---:|
${rows
  .map(
    (r) =>
      `| ${r.label} | ${r.quality.level1SelectionAccuracy} | ${r.quality.verifiedFactRetentionL1L2} | ${r.quality.unsupportedAssertionCount} |`,
  )
  .join("\n")}

## Rendered before → after (by opportunity)

${rows
  .map((r) => {
    const promoted = r.catalogFacts.filter((f) => f.selectedDestination === "BRIEF");
    const support = r.catalogFacts.filter((f) =>
      String(f.selectedDestination).startsWith("SUPPORTING"),
    );
    const missing = r.catalogFacts.filter((f) => f.selectedDestination === "OMIT_MISSING");
    return `### ${r.label} — ${r.beforeBrief.whatIsHappening?.slice(0, 60) || r.id}…

**1. Opportunity brief (after)**  
> ${r.afterBrief.whatIsHappening}

**Why this hotel:** ${r.afterBrief.whyThisHotel || "_(none)_"}  
**Potential business:** ${r.afterBrief.potentialHotelBusiness || "_(none)_"}  
**Why act now:** ${r.afterBrief.whyActNow || "_(none)_"}  
**Next step:** ${r.afterBrief.recommendedNextStep || "_(none)_"}  
**Posture:** \`${r.afterBrief.commercialPosture || ""}\`

**2. Supporting Research (Level 2)**  
${
  r.supportingResearch.length
    ? r.supportingResearch
        .map(
          (s) =>
            `- ${s.claim}${s.sourceUrl ? ` — ${s.sourceUrl}` : ""}${s.hotelPrivate ? " _(hotel-private)_" : ""}`,
        )
        .join("\n")
    : "_None selected for Level 2 surface (catalog may still retain OMIT_MISSING)._"
}

**3. Source evidence / citations (selected facts)**  
${r.catalogFacts
  .filter((f) => f.sourceUrl)
  .map((f) => `- ${f.id}: ${f.sourceUrl}`)
  .join("\n") || "_See archivedSources in AUDIT.json_"}

**4. Contacts / buyer routes**  
Preserved on opportunity \`primaryContact\` / promoted contact lines in next step when material.

**5. Promoted into Level 1**  
${promoted.map((f) => `- **${f.id}**: ${f.claim} _(was ${f.priorDestination || "n/a"} → ${f.selectedDestination})_`).join("\n") || "_None newly required_"}

**6. Retained supporting only**  
${support.map((f) => `- **${f.id}**: ${f.claim}`).join("\n") || "_None_"}

**7. Missing / unsupported**  
${missing.map((f) => `- **${f.id}**: ${f.claim} — absent from research`).join("\n") || "_None_"}
${r.defects.length ? r.defects.map((d) => `- DEFECT \`${d.code}\`: ${d.factId}`).join("\n") : ""}

**Archived research paths:** ${r.archivedSources.join("; ") || "_(fixture catalog)_"}
`;
  })
  .join("\n")}

## Regression highlights
- **PDAC:** no-housing-bureau / direct book promoted to Level 1 (reevaluated from prior SUPPORTING).
- **NICE:** FIU/New America material; customer sourced-properties stays hotel-private (not L1 shared).
- **OWWA:** Enercare + October 20 promoted; Sam Petryna retained.
- **NRC RIC:** past-cycle posture; no open pursuit / no credible-host invention.
- **IMC:** expansion hub retained; confirmed room block remains ABSENT (not invented).

Paid/Airtable/publication: 0
`;

fs.writeFileSync(path.join(REPORT, "RESULTS.json"), `${JSON.stringify({ ...summary, detail: rows }, null, 2)}\n`);
fs.writeFileSync(path.join(REPORT, "FOUNDER_BEFORE_AFTER.md"), md);

console.log(JSON.stringify(summary, null, 2));
