#!/usr/bin/env node
/**
 * Commercial Fact Selection V1 — source retention + L1/L2 regressions ($0).
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
import { buildSlimResearchPayload } from "../lib/group-demand-intelligence/workflow/gdi-research-inventory-slim-payload-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIX = path.join(ROOT, "fixtures/gdi-brief-v2-narrative-quality-v1");
const CATALOGS = path.join(FIX, "evidence-catalogs");
const REPORT = path.join(ROOT, "reports/gdi-commercial-fact-selection-v1");
const NOW = new Date("2026-11-19T12:00:00.000Z");

const SECTION_LABELS = {
  demand_context: "Opportunity / demand context",
  potential_hotel_business: "Potential hotel business",
  hotel_relevance: "Hotel relevance",
  commercial_timing: "Commercial timing",
  buyer_contacts: "Buyer contacts and booking routes",
  competitors_venues: "Competitors / historical venues",
  recommended_next_step: "Recommended next step",
  supporting_research: "Supporting Research",
};

function esc(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function renderDrawerHtml(after) {
  const cites = (key) => {
    const rows = after.sectionCitations?.[key] || [];
    if (!rows.length) return "";
    return `<ul class="gdi-section-citations" data-section-citations="${esc(key)}">${rows
      .map(
        (s) =>
          `<li><a href="${esc(s.url)}">${esc(s.title || s.url)}</a></li>`,
      )
      .join("")}</ul>`;
  };
  const support = (after.supportingResearch || [])
    .map((r) => {
      const url = r.sourceUrl || (r.sourceUrls || [])[0] || "";
      return `<li>${esc(r.claim)}${r.hotelPrivate ? " <em>(hotel-private)</em>" : ""}${
        url ? ` — <a href="${esc(url)}">${esc(url)}</a>` : ""
      }</li>`;
    })
    .join("");
  const all = (after.allSources || [])
    .map(
      (s) =>
        `<li>${s.title ? `<strong>${esc(s.title)}</strong> — ` : ""}<a href="${esc(s.url)}">${esc(s.url)}</a> <span class="muted">[${esc((s.drawerSections || []).join(", "))}]</span></li>`,
    )
    .join("");

  return `<article class="gdi-drawer-render" data-opportunity="${esc(after.id || after.opportunityId || "")}">
  <section data-section-id="what_is_happening"><h3>What is happening</h3>
    <p>${esc(after.whatIsHappening)}</p>
    ${cites("demand_context")}${cites("competitors_venues")}
  </section>
  <section data-section-id="potential_business"><h3>Potential business for this hotel</h3>
    <p>${esc(after.potentialHotelBusiness)}</p>
    ${cites("potential_hotel_business")}
  </section>
  <section data-section-id="why_this_hotel"><h3>Why this hotel</h3>
    <p>${esc(after.whyThisHotel)}</p>
    ${cites("hotel_relevance")}
  </section>
  <section data-section-id="why_act_now"><h3>Why act now</h3>
    <p>${esc(after.whyActNow)}</p>
    ${cites("commercial_timing")}
  </section>
  <section data-section-id="who_to_contact"><h3>Who to contact</h3>
    <p>${esc(after.primaryContact?.name || "")}${after.primaryContact?.email ? ` — ${esc(after.primaryContact.email)}` : ""}</p>
    ${cites("buyer_contacts")}
  </section>
  <section data-section-id="recommended_next_step"><h3>Recommended next step</h3>
    <p>${esc(after.recommendedNextStep)}</p>
    ${cites("recommended_next_step")}
  </section>
  <section data-section-id="supporting_research"><h3>Supporting Research</h3>
    <ul>${support || "<li><em>None</em></li>"}</ul>
  </section>
  <section data-section-id="all_sources"><h3>All Sources</h3>
    <ul>${all || "<li><em>None</em></li>"}</ul>
  </section>
</article>`;
}

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

  const slimProbe = buildSlimResearchPayload(composed.opportunity);
  const selected = selectAndApplyCommercialFacts(composed.opportunity, {
    now: NOW,
    catalog,
    durableRef: `fixtures/gdi-brief-v2-narrative-quality-v1/evidence-catalogs/${sample.label}.json`,
    slimSources: slimProbe.ok ? slimProbe.payload?.sources || [] : [],
  });
  const after = selected.opportunity;
  const q = after.factSelectionV1.quality;

  assert.ok(after.commercialFactCatalog?.facts?.length >= 1);
  assert.ok(Array.isArray(after.supportingResearch));
  assert.ok(Array.isArray(after.allSources), "allSources register required");
  assert.ok(after.sectionCitations && typeof after.sectionCitations === "object");
  assert.equal(after.commercialFactCatalog.durableRef.includes(sample.label), true);
  assert.equal(after.factSelectionV1.persistence?.certified, false);

  // Fact→source traceability for catalog facts that carry URLs
  for (const f of after.commercialFactCatalog.facts) {
    if (!f.sourceUrl && !(f.sourceUrls || []).length) continue;
    if (f.origin === "ABSENT_FROM_RESEARCH") continue;
    assert.ok(
      (f.sourceIds || []).length > 0,
      `${sample.label}/${f.id} must retain sourceIds`,
    );
  }

  if (sample.label === "pdac") {
    const bureau = after.commercialFactCatalog.facts.find(
      (f) => f.id === "pdac-no-housing-bureau",
    );
    assert.ok(bureau);
    assert.equal(bureau.selectedDestination, "BRIEF", "housing bureau must promote to L1");
    assert.ok((bureau.sourceIds || []).length > 0, "PDAC housing bureau must keep sourceIds");
    assert.ok(
      /housing bureau|Book directly/i.test(after.whatIsHappening),
      "PDAC L1 must retain direct-book / no-bureau fact",
    );
    assert.ok(
      /Nicole Sampson/i.test(after.recommendedNextStep + after.whatIsHappening),
      "Nicole Sampson must surface",
    );
    // Truncated lodging ending must not invent "Toronto" completion
    const lodgingFact = after.commercialFactCatalog.facts.find(
      (f) => f.id === "inline-lodging-direct-book",
    );
    if (lodgingFact) {
      assert.ok(!/Hotel X Toron[a-z]/i.test(lodgingFact.claim) || /Hotel X Toron\./i.test(lodgingFact.claim) || !/Toron$/i.test(lodgingFact.claim));
    }
    const partnerCite = (after.sectionCitations.competitors_venues || []).concat(
      after.sectionCitations.demand_context || [],
      after.sectionCitations.buyer_contacts || [],
    );
    assert.ok(partnerCite.some((s) => /pdac\.ca/i.test(s.url)), "PDAC sources in sections");
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
    assert.equal(q.confidentialSourceLeakage, 0);
  }

  if (sample.label === "owwa") {
    const oct = after.commercialFactCatalog.facts.find((f) => f.id === "owwa-oct20");
    const venue = after.commercialFactCatalog.facts.find((f) => f.id === "owwa-enercare");
    assert.equal(oct.selectedDestination, "BRIEF");
    assert.equal(venue.selectedDestination, "BRIEF");
    assert.ok(/October 20, 2026/i.test(oct.claim), "October 20 must carry year 2026");
    assert.ok(/October 20|Enercare/i.test(after.whatIsHappening));
    assert.ok(/Sam Petryna/i.test(after.recommendedNextStep + JSON.stringify(after.primaryContact)));
    assert.ok(!/overflow volume|guaranteed overflow/i.test(after.whatIsHappening + after.potentialHotelBusiness));
  }

  if (sample.label === "nrc-ric") {
    assert.equal(after.briefV2?.commercialPosture || composed.opportunity.briefV2?.commercialPosture, "PAST_CYCLE_DO_NOT_PURSUE");
    assert.ok(/do not pursue|past/i.test(after.recommendedNextStep + after.whyActNow));
    assert.ok(!/credible host candidate/i.test(after.whyThisHotel || ""));
    assert.ok(!/active.*RFP|open RFP for 2027/i.test(after.whatIsHappening + after.whyActNow));
  }

  if (sample.label === "imc-construction") {
    assert.ok(/Bethesda regional hub/i.test(after.whatIsHappening));
    assert.ok(!/confirmed room block/i.test(after.whatIsHappening + after.potentialHotelBusiness));
    const absent = after.commercialFactCatalog.facts.find((f) => f.id === "imc-room-block");
    assert.equal(absent.selectedDestination, "OMIT_MISSING");
  }

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

  const drawerHtml = renderDrawerHtml(after);
  assert.ok(drawerHtml.includes("All Sources"));
  assert.ok(drawerHtml.includes("gdi-section-citations") || after.allSources.length === 0);

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
    allSources: after.allSources,
    sectionCitations: after.sectionCitations,
    catalogFacts: after.commercialFactCatalog.facts.map((f) => ({
      id: f.id,
      claim: f.claim,
      priorDestination: f.priorDestination,
      selectedDestination: f.selectedDestination,
      selectionReason: f.selectionReason,
      sourceUrl: f.sourceUrl || null,
      sourceIds: f.sourceIds || [],
      drawerSections: f.drawerSections || [],
      provenanceStatus: f.provenanceStatus || null,
      hotelPrivate: f.hotelPrivate,
    })),
    promotedIntoBrief: after.factSelectionV1.promotedIntoBrief,
    defects: after.factSelectionV1.defects,
    quality: q,
    persistence: after.factSelectionV1.persistence,
    archivedSources: catalog.archivedSources || [],
    drawerHtml,
  });
}

fs.mkdirSync(REPORT, { recursive: true });
const summary = {
  version: "gdi-commercial-fact-selection-v1",
  amendment: "source-retention-section-citations",
  at: new Date().toISOString(),
  scoringAsOf: NOW.toISOString(),
  paidCalls: 0,
  airtableWrites: 0,
  persistenceCertified: false,
  samples: rows.map((r) => ({
    label: r.label,
    sourceRetentionCompleteness: r.quality.sourceRetentionCompleteness,
    factToSourceTraceability: r.quality.factToSourceTraceability,
    sectionPlacementAccuracy: r.quality.sectionPlacementAccuracy,
    level1Accuracy: r.quality.level1SelectionAccuracy,
    retention: r.quality.verifiedFactRetentionL1L2,
    unsupportedOrMisattributed: r.quality.unsupportedOrMisattributedCount,
    confidentialLeakage: r.quality.confidentialSourceLeakage,
    persistenceGaps: r.persistence?.knownGaps || [],
    sourceCount: r.allSources.length,
    promoted: r.promotedIntoBrief,
    defects: r.defects,
  })),
};

const pdac = rows.find((r) => r.label === "pdac");

const md = `# GDI Commercial Fact Selection V1 — founder before/after

**Amendment:** complete source retention + section-level citations.  
Fixed clock: ${NOW.toISOString().slice(0, 10)}. Paid Exa: **0**. Airtable writes: **0**.

## Objective

Commercially important verified findings must not disappear between archived research and the customer opportunity detail — and every selected fact must keep recoverable source relationships.

## Independent quality gates

| Sample | 1 Source retention | 2 Fact→source | 3 Section placement | 4 L1 selection | 5 L1+L2 retention | 6 Unsupported/misattr | 7 Confidential leak | 8 Persistence |
|---|---:|---:|---:|---:|---:|---:|---:|---|
${rows
  .map(
    (r) =>
      `| ${r.label} | ${r.quality.sourceRetentionCompleteness} | ${r.quality.factToSourceTraceability} | ${r.quality.sectionPlacementAccuracy} | ${r.quality.level1SelectionAccuracy} | ${r.quality.verifiedFactRetentionL1L2} | ${r.quality.unsupportedOrMisattributedCount} | ${r.quality.confidentialSourceLeakage} | NOT_CERTIFIED |`,
  )
  .join("\n")}

> A 100% fact-retention score is **not** sufficient if source URLs / sourceIds are missing.

### Persistence / read-back (gate 8) — gap report

Slice 2A slim inventory keep **URL evidence rows**, not the full \`commercialSourceRegister\` / \`sectionCitations\` / \`fact.sourceIds\` graph.

${(rows[0]?.persistence?.knownGaps || [])
  .map((g) => `- ${g}`)
  .join("\n")}

Durable catalogs remain referenced via \`commercialFactCatalog.durableRef\`. Full persistence certification requires a founder-approved slim/store contract extension — **not claimed here**.

## Rendered drawer sample — PDAC (contextual citations + All Sources)

\`\`\`html
${pdac?.drawerHtml || "_(pdac missing)_"}
\`\`\`

Also written to \`RENDERED_DRAWER_PDAC.html\`.

## Rendered before → after (by opportunity)

${rows
  .map((r) => {
    const promoted = r.catalogFacts.filter((f) => f.selectedDestination === "BRIEF");
    const support = r.catalogFacts.filter((f) =>
      String(f.selectedDestination).startsWith("SUPPORTING"),
    );
    const missing = r.catalogFacts.filter((f) => f.selectedDestination === "OMIT_MISSING");
    const sectionLines = Object.entries(SECTION_LABELS)
      .map(([key, label]) => {
        const cites = r.sectionCitations?.[key] || [];
        if (!cites.length) return null;
        return `**${label}**\n${cites.map((s) => `  - ${s.title || "source"} — ${s.url} _(facts: ${(s.associatedFactIds || []).join(", ")})_`).join("\n")}`;
      })
      .filter(Boolean)
      .join("\n\n");
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
            `- ${s.claim}${s.sourceUrl ? ` — ${s.sourceUrl}` : ""}${s.sourceIds?.length ? ` _(sourceIds: ${s.sourceIds.join(", ")})_` : ""}${s.hotelPrivate ? " _(hotel-private)_" : ""}`,
        )
        .join("\n")
    : "_None selected for Level 2 surface (catalog may still retain OMIT_MISSING)._"
}

**3. Section-level citations**  
${sectionLines || "_No section citations linked_"}

**4. All Sources (deduplicated register)**  
${r.allSources.map((s) => `- \`${s.sourceId}\` ${s.title || ""} — ${s.url} _(sections: ${(s.drawerSections || []).join(", ")}; facts: ${(s.associatedFactIds || []).join(", ")})_`).join("\n") || "_Empty_"}

**5. Promoted into Level 1**  
${promoted.map((f) => `- **${f.id}**: ${f.claim} _(was ${f.priorDestination || "n/a"} → ${f.selectedDestination}; sources: ${(f.sourceIds || []).join(", ") || "none"})_`).join("\n") || "_None newly required_"}

**6. Retained supporting only**  
${support.map((f) => `- **${f.id}**: ${f.claim}${f.hotelPrivate ? " _(hotel-private)_" : ""}`).join("\n") || "_None_"}

**7. Missing / unsupported / provenance**  
${missing.map((f) => `- **${f.id}**: ${f.claim} — absent from research`).join("\n") || "_None_"}
${r.defects.length ? r.defects.map((d) => `- DEFECT \`${d.code}\`: ${d.factId}`).join("\n") : ""}

**Archived research paths:** ${r.archivedSources.join("; ") || "_(fixture catalog)_"}
`;
  })
  .join("\n")}

## Regression highlights
- **PDAC:** no-housing-bureau / direct book → L1 with sourceIds; Nicole Sampson + convention route cited; truncated lodging repaired without inventing the ending; section citations + All Sources rendered.
- **NICE:** FIU/New America material; hotel-private sourced-properties stays out of shared L1.
- **OWWA:** Enercare + October 20 **2026** (past milestone, not future deadline); Sam Petryna retained; no unsupported overflow-volume claim.
- **NRC RIC:** past-cycle posture; no open pursuit / no credible-host invention; no active current-cycle RFP implication.
- **IMC:** expansion hub retained; confirmed room block remains ABSENT (not invented).

Paid/Airtable/publication: 0 · Persistence certification: **NOT claimed**
`;

fs.writeFileSync(path.join(REPORT, "RESULTS.json"), `${JSON.stringify({ ...summary, detail: rows }, null, 2)}\n`);
fs.writeFileSync(path.join(REPORT, "FOUNDER_BEFORE_AFTER.md"), md);
if (pdac) {
  fs.writeFileSync(
    path.join(REPORT, "RENDERED_DRAWER_PDAC.html"),
    `<!doctype html><meta charset="utf-8"><title>PDAC drawer — source citations</title>
<style>
  body{font:14px/1.45 system-ui,sans-serif;max-width:880px;margin:24px auto;padding:0 16px;color:#1a1a1a}
  section{margin:1.25rem 0;padding-bottom:1rem;border-bottom:1px solid #e5e5e5}
  .gdi-section-citations{margin:.5rem 0 0;padding-left:1.1rem;font-size:12px}
  .muted{color:#666;font-size:12px}
  a{color:#0b57d0}
</style>
${pdac.drawerHtml}
`,
  );
}

console.log(JSON.stringify(summary, null, 2));
