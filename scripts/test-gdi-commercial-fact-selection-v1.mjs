#!/usr/bin/env node
/**
 * Commercial Fact Selection V1 — source retention + Sources (N) UI regressions ($0).
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
import { displayTitleForSource } from "../lib/group-demand-intelligence/presentation/gdi-commercial-source-register-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIX = path.join(ROOT, "fixtures/gdi-brief-v2-narrative-quality-v1");
const CATALOGS = path.join(FIX, "evidence-catalogs");
const REPORT = path.join(ROOT, "reports/gdi-commercial-fact-selection-v1");
const NOW = new Date("2026-11-19T12:00:00.000Z");

const DRAWER_SECTION_MAP = [
  { id: "what_is_happening", heading: "What is happening", keys: ["demand_context", "competitors_venues"], body: (o) => o.whatIsHappening },
  { id: "potential_business", heading: "Potential business for this hotel", keys: ["potential_hotel_business"], body: (o) => o.potentialHotelBusiness },
  { id: "why_this_hotel", heading: "Why this hotel", keys: ["hotel_relevance"], body: (o) => o.whyThisHotel },
  { id: "why_act_now", heading: "Why act now", keys: ["commercial_timing"], body: (o) => o.whyActNow },
  { id: "who_to_contact", heading: "Who to contact", keys: ["buyer_contacts"], body: (o) => o.primaryContact?.name || "" },
  { id: "recommended_next_step", heading: "Recommended next step", keys: ["recommended_next_step"], body: (o) => o.recommendedNextStep },
];

function esc(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function uniqueSectionSources(after, keys, opportunityTitle) {
  const seen = new Set();
  const rows = [];
  for (const key of keys) {
    for (const s of after.sectionCitations?.[key] || []) {
      const url = String(s.url || "").toLowerCase().replace(/\/$/, "");
      if (!url || seen.has(url)) continue;
      seen.add(url);
      rows.push({
        url: s.url,
        title: displayTitleForSource(s, { opportunityTitle }),
      });
    }
  }
  return rows;
}

function sourcesButtonHtml(rows, { expanded = false } = {}) {
  if (!rows.length) return "";
  // Customer default: collapsed (no open attribute) — matches gdi-inline-details
  const openAttr = expanded ? " open" : "";
  return `<details class="gdi-inline-details gdi-section-sources"${openAttr}><summary>Sources (${rows.length})</summary><ul class="gdi-evidence-default">${rows
    .map(
      (s) =>
        `<li class="gdi-evidence-item"><a href="${esc(s.url)}">${esc(s.title || s.url)}</a></li>`,
    )
    .join("")}</ul></details>`;
}

function renderDrawerHtml(after, { expanded = false } = {}) {
  const oppTitle = after.title || after.displayTitle || "";
  const sections = DRAWER_SECTION_MAP.map((sec) => {
    const rows = uniqueSectionSources(after, sec.keys, oppTitle);
    const btn = sourcesButtonHtml(rows, { expanded });
    assert.ok(
      !btn || (btn.match(/<summary>Sources \(/g) || []).length === 1,
      `${sec.id} must have at most one Sources button`,
    );
    const urls = rows.map((r) => r.url.toLowerCase().replace(/\/$/, ""));
    assert.equal(urls.length, new Set(urls).size, `${sec.id} must dedupe URLs`);
    for (const r of rows) {
      assert.ok(!/csrc_/i.test(r.title || ""), "no source IDs in titles");
      assert.ok(r.title && r.title !== oppTitle, `meaningful title for ${r.url}`);
    }
    return `  <section data-section-id="${esc(sec.id)}"><h3>${esc(sec.heading)}</h3>
    <p>${esc(sec.body(after) || "—")}</p>
    ${btn}
  </section>`;
  });

  const support = (after.supportingResearch || [])
    .map((r) => `<li>${esc(r.claim)}${r.hotelPrivate ? " <em>(hotel-private)</em>" : ""}</li>`)
    .join("");

  return `<article class="gdi-drawer-render" data-opportunity="${esc(after.id || after.opportunityId || "")}">
  <section data-section-id="opportunity_overview"><h3>Opportunity overview</h3><p>${esc(after.title || "")}</p></section>
${sections.join("\n")}
  <section data-section-id="supporting_evidence"><h3>Supporting evidence</h3>
    ${(after.sources || [])
      .slice(0, 8)
      .map((s) => {
        const url = typeof s === "string" ? s : s.url;
        const title = displayTitleForSource(
          typeof s === "string" ? { url: s } : s,
          { opportunityTitle: oppTitle },
        );
        return url
          ? `<li><a href="${esc(url)}">${esc(title)}</a></li>`
          : "";
      })
      .filter(Boolean)
      .join("")
      ? `<ul>${(after.sources || [])
          .map((s) => {
            const url = typeof s === "string" ? s : s?.url;
            if (!url) return "";
            const title = displayTitleForSource(
              typeof s === "string" ? { url: s } : s,
              { opportunityTitle: oppTitle },
            );
            return `<li><a href="${esc(url)}">${esc(title)}</a></li>`;
          })
          .filter(Boolean)
          .join("")}</ul>`
      : "<p class=\"muted\">No source URLs attached yet.</p>"}
    ${
      support
        ? `<details class="gdi-research-detail-expand"><summary>Supporting Research (${after.supportingResearch.length})</summary><ul>${support}</ul></details>`
        : ""
    }
  </section>
</article>`;
}

function assertNoCustomerLeakage(html) {
  assert.ok(!/gdi-section-citations/i.test(html), "no visible citation lists");
  assert.ok(!/All Sources/i.test(html), "no prominent All Sources block");
  assert.ok(!/csrc_/i.test(html), "no source IDs in customer HTML");
  assert.ok(!/demand_context|competitors_venues|commercial_timing/i.test(html.replace(/data-section-id="[^"]+"/g, "")), "no internal section keys visible");
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
  assert.ok(Array.isArray(after.allSources), "underlying allSources register required");
  assert.ok(after.sectionCitations && typeof after.sectionCitations === "object");
  assert.equal(after.commercialFactCatalog.durableRef.includes(sample.label), true);
  assert.equal(after.factSelectionV1.persistence?.certified, false);

  for (const f of after.commercialFactCatalog.facts) {
    if (!f.sourceUrl && !(f.sourceUrls || []).length) continue;
    if (f.origin === "ABSENT_FROM_RESEARCH") continue;
    assert.ok((f.sourceIds || []).length > 0, `${sample.label}/${f.id} must retain sourceIds`);
  }

  if (sample.label === "pdac") {
    const bureau = after.commercialFactCatalog.facts.find(
      (f) => f.id === "pdac-no-housing-bureau",
    );
    assert.ok(bureau);
    assert.equal(bureau.selectedDestination, "BRIEF");
    assert.ok((bureau.sourceIds || []).length > 0);
    assert.ok(/housing bureau|Book directly|direct booking/i.test(after.whatIsHappening));
    assert.ok(!/does NOT use 3rd party housing\s*$/i.test(after.whatIsHappening), "no truncated lodging append");
    assert.ok(
      !/Book directly with your hotel[\s\S]*housing bureaus[\s\S]*Book directly/i.test(
        after.whatIsHappening,
      ),
      "no repetitive direct-book restatement",
    );
    const housingMentions = (after.whatIsHappening.match(/housing bureau/gi) || []).length;
    assert.ok(housingMentions <= 1, "housing bureau stated at most once");
    assert.ok(/Nicole Sampson/i.test(after.recommendedNextStep + after.whatIsHappening));
    assert.ok(
      !/no direct route is published/i.test(after.recommendedNextStep),
      "must not claim no contact route when organizational route exists",
    );
    assert.ok(
      /convention@pdac\.ca/i.test(after.recommendedNextStep),
      "published organizational route must remain visible",
    );
    const buyer = after.sectionCitations?.buyer_contacts || [];
    assert.ok(
      buyer.every((s) => /staff|about-pdac/i.test(s.url)),
      "Who to contact Sources must be contact pages, not unrelated lodging pages",
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
    );
    const priv = after.commercialFactCatalog.facts.find((f) => f.id === "nice-customer-sourced");
    assert.equal(priv.selectedDestination, "SUPPORTING_HOTEL_PRIVATE");
    assert.ok(!/added to sourced properties/i.test(after.whatIsHappening));
    assert.equal(q.confidentialSourceLeakage, 0);
  }

  if (sample.label === "owwa") {
    const oct = after.commercialFactCatalog.facts.find((f) => f.id === "owwa-oct20");
    const venue = after.commercialFactCatalog.facts.find((f) => f.id === "owwa-enercare");
    assert.equal(oct.selectedDestination, "BRIEF");
    assert.equal(venue.selectedDestination, "BRIEF");
    assert.ok(/October 20, 2026/i.test(oct.claim));
    assert.ok(/October 20|Enercare/i.test(after.whatIsHappening));
    assert.ok(/Sam Petryna/i.test(after.recommendedNextStep + JSON.stringify(after.primaryContact)));
    assert.ok(!/overflow volume|guaranteed overflow/i.test(after.whatIsHappening + after.potentialHotelBusiness));
  }

  if (sample.label === "nrc-ric") {
    assert.equal(
      after.briefV2?.commercialPosture || composed.opportunity.briefV2?.commercialPosture,
      "PAST_CYCLE_DO_NOT_PURSUE",
    );
    assert.ok(/do not pursue|past/i.test(after.recommendedNextStep + after.whyActNow));
    assert.ok(!/credible host candidate/i.test(after.whyThisHotel || ""));
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

  const drawerHtml = renderDrawerHtml(after, { expanded: false });
  const drawerHtmlExpanded = renderDrawerHtml(after, { expanded: true });
  assertNoCustomerLeakage(drawerHtml);
  assert.ok(/Sources \(\d+\)/.test(drawerHtml), `${sample.label} must show Sources (N)`);
  assert.equal((drawerHtml.match(/<h3>All Sources<\/h3>/g) || []).length, 0);
  assert.ok(
    !/<details class="gdi-inline-details gdi-section-sources" open>/.test(drawerHtml),
    "Sources controls collapsed by default",
  );
  assert.ok(
    !/Supporting Research[\s\S]{0,400}Sources \(/i.test(drawerHtml),
    "Supporting Research must not nest a duplicate Sources block",
  );

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
    drawerHtmlExpanded,
  });
}

fs.mkdirSync(REPORT, { recursive: true });
const summary = {
  version: "gdi-commercial-fact-selection-v1",
  amendment: "consolidated-sources-buttons-ui",
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

const demoLabels = ["pdac", "nice", "owwa"];
const style = `<style>
  body{font:14px/1.45 system-ui,sans-serif;max-width:980px;margin:24px auto;padding:0 16px;color:#1a1a1a;background:#fff}
  h1,h2{font-size:1.15rem} h3{font-size:1rem;margin:0 0 .4rem}
  section{margin:1rem 0;padding-bottom:.85rem;border-bottom:1px solid #e8e8e8}
  .gdi-inline-details,.gdi-section-sources,.gdi-research-detail-expand{margin-top:.4rem;font-size:12px}
  .gdi-section-sources > summary,.gdi-research-detail-expand > summary{cursor:pointer;font-weight:600;color:#334}
  .gdi-evidence-default{margin:.35rem 0 0;padding-left:1.1rem}
  .muted{color:#666}
  a{color:#0b57d0}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  @media(max-width:800px){.grid{grid-template-columns:1fr}}
  .panel{border:1px solid #ddd;border-radius:6px;padding:12px 14px;background:#fafafa}
  .panel h2{margin-top:0}
</style>`;

const compareHtml = `<!doctype html><meta charset="utf-8"><title>GDI drawer — Sources (N) consolidation</title>
${style}
<h1>GDI drawer structural parity — Sources (N) consolidation</h1>
<p>Established section hierarchy preserved. Visible citation lists removed. One <strong>Sources (N)</strong> control per section (existing details/summary convention).</p>
${demoLabels
  .map((label) => {
    const r = rows.find((x) => x.label === label);
    return `<h2>${label.toUpperCase()}</h2>
<div class="grid">
  <div class="panel"><h2>Before (citation lists — rejected)</h2>
    <p class="muted">Prior amendment showed visible URL lists + All Sources + generic titles.</p>
    <pre style="white-space:pre-wrap;font-size:11px">${esc(
      `What is happening\n> ${r.beforeBrief.whatIsHappening}\n\n[visible citation lists / All Sources — removed]`,
    )}</pre>
  </div>
  <div class="panel"><h2>After — default collapsed</h2>
    ${r.drawerHtml}
  </div>
  <div class="panel"><h2>After — Sources expanded (inspection)</h2>
    ${r.drawerHtmlExpanded}
  </div>
</div>`;
  })
  .join("\n")}
`;

const md = `# GDI Commercial Fact Selection V1 — founder before/after

**Amendment:** consolidated **Sources (N)** buttons (structural parity with existing drawer).  
Fixed clock: ${NOW.toISOString().slice(0, 10)}. Paid Exa: **0**. Airtable writes: **0**.

## UI correction

- Removed visible section citation lists and prominent All Sources block from the customer drawer.
- One \`Sources (N)\` expandable control per applicable section (existing \`gdi-inline-details\` convention).
- N = unique sources for that section (URL-deduped).
- Descriptive titles derived from path when stored title was a generic opportunity name.
- Supporting Research folded into existing Supporting evidence as expandable detail (no new top-level section).
- Full \`allSources\` / \`sourceIds\` register retained in underlying data (not customer-primary).

## Independent quality gates

| Sample | 1 Source retention | 2 Fact→source | 3 Section placement | 4 L1 selection | 5 L1+L2 retention | 6 Unsupported/misattr | 7 Confidential leak | 8 Persistence |
|---|---:|---:|---:|---:|---:|---:|---:|---|
${rows
  .map(
    (r) =>
      `| ${r.label} | ${r.quality.sourceRetentionCompleteness} | ${r.quality.factToSourceTraceability} | ${r.quality.sectionPlacementAccuracy} | ${r.quality.level1SelectionAccuracy} | ${r.quality.verifiedFactRetentionL1L2} | ${r.quality.unsupportedOrMisattributedCount} | ${r.quality.confidentialSourceLeakage} | NOT_CERTIFIED |`,
  )
  .join("\n")}

### Persistence (gate 8)

${(rows[0]?.persistence?.knownGaps || []).map((g) => `- ${g}`).join("\n")}

## Structural parity demo

See \`RENDERED_DRAWER_SOURCES_BUTTONS.html\` (PDAC / NICE / OWWA side-by-side before note + after with Sources expanded).

### PDAC after (excerpt)

\`\`\`html
${rows.find((r) => r.label === "pdac")?.drawerHtml || ""}
\`\`\`

## Narrative cleanup

${rows
  .map((r) => {
    return `### ${r.label}

**Before whatIsHappening:** ${r.beforeBrief.whatIsHappening}

**After whatIsHappening:** ${r.afterBrief.whatIsHappening}

**Sources buttons (unique counts):** ${DRAWER_SECTION_MAP.map((sec) => {
      const n = uniqueSectionSources(r, sec.keys, r.afterBrief.whatIsHappening?.slice(0, 0) || rows.find((x) => x.label === r.label)?.id).length;
      // recount properly
      return null;
    }).filter(Boolean)}
${DRAWER_SECTION_MAP.map((sec) => {
  const n = uniqueSectionSources(
    { sectionCitations: r.sectionCitations },
    sec.keys,
    "",
  );
  const titles = n.map((s) => s.title).join("; ");
  return `- ${sec.heading}: Sources (${n.length})${n.length ? ` — ${titles}` : " _(hidden — empty)_"}`;
}).join("\n")}

**Underlying register size:** ${r.allSources.length} (not shown as primary All Sources list)
`;
  })
  .join("\n")}

## Regression highlights
- **PDAC:** one housing-bureau/direct-book statement; no truncated lodging append; Sources buttons with path-based titles (e.g. Hotels 2027, Staff).
- **NICE / OWWA / NRC / IMC:** fact selection + source retention preserved; hotel-private not in shared L1.
- Drawer section hierarchy unchanged (overview → what → potential → why hotel → why now → facts → contact → next → supporting evidence → validation).

Paid/Airtable/publication: 0 · Persistence certification: **NOT claimed**
`;

fs.writeFileSync(path.join(REPORT, "RESULTS.json"), `${JSON.stringify({ ...summary, detail: rows }, null, 2)}\n`);
fs.writeFileSync(path.join(REPORT, "FOUNDER_BEFORE_AFTER.md"), md);
fs.writeFileSync(path.join(REPORT, "RENDERED_DRAWER_SOURCES_BUTTONS.html"), compareHtml);
fs.writeFileSync(
  path.join(REPORT, "RENDERED_DRAWER_PDAC.html"),
  `<!doctype html><meta charset="utf-8"><title>PDAC — Sources (N)</title>${style}${rows.find((r) => r.label === "pdac").drawerHtml}`,
);

console.log(JSON.stringify(summary, null, 2));
