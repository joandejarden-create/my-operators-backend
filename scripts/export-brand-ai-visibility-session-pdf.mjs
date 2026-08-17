/**
 * One-shot full-section Brand AI Visibility PDF export from measured baseline.
 * No provider calls. No measurement changes. Auth not required (direct store read).
 *
 * Usage: node scripts/export-brand-ai-visibility-session-pdf.mjs
 */
import fs from "fs";
import path from "path";
import { spawnSync } from "child_process";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  getBrandOverviewPayload,
  getBrandQuestionsPayload,
  getBrandSourcesPayload,
  getBrandCompetitorsPayload,
  getBrandPortfolioPayload,
} from "../lib/ai-visibility/brand-read-service.js";
import { getBrandExecutiveSummaryPayload } from "../lib/ai-visibility/brand-executive-summary.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import {
  loadShowcaseCompaniesConfig,
  getShowcaseCompany,
} from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import {
  peerSetBrandNamesById,
  PEER_SET_ID_V2,
} from "../lib/ai-visibility/peer-sets.js";
import { normalizeAiVisibilityViewerContext } from "../lib/ai-visibility/viewer-context.js";
import { loadObservationsByProviderForCohort } from "../lib/ai-visibility/cross-provider-questions.js";
import { buildDiscoverabilityProductPayload } from "../lib/ai-visibility/brand-website-wiring.js";

const AUTOGRAPH = "recEJCTDj1zrsjPM6";
const CHROME =
  process.env.CHROME_PATH ||
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

function esc(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}
function d(v) {
  if (v == null) return "—";
  if (typeof v === "object") return esc(v.display ?? v.value ?? "—");
  return esc(v);
}
function pct(n) {
  if (n == null || Number.isNaN(Number(n))) return "—";
  return (Math.round(Number(n) * 1000) / 10).toFixed(1) + "%";
}
function kpiCards(items) {
  return items
    .map(
      ([label, val, help]) =>
        `<div class="kpi"><div class="kpi-l">${esc(label)}</div><div class="kpi-v">${esc(
          val
        )}</div>${help ? `<div class="kpi-h">${esc(help)}</div>` : ""}</div>`
    )
    .join("");
}
function insightCards(boxes) {
  return (boxes || [])
    .map(
      (b) => `<div class="insight"><div class="insight-t">${esc(
        b.title
      )}</div><div class="insight-f">${esc(b.finding)}</div><div class="insight-e">${esc(
        b.evidence || ""
      )}</div><div class="insight-s">${esc(
        b.soWhat || b.takeaway || ""
      )}</div></div>`
    )
    .join("");
}

const showcase = loadShowcaseCompaniesConfig();
const marriott = getShowcaseCompany("marriott", showcase);
const names = { ...peerSetBrandNamesById(PEER_SET_ID_V2) };
for (const b of marriott.brands || []) names[b.brandId] = b.brandName;
try {
  const uni = JSON.parse(
    fs.readFileSync("fixtures/ai-visibility/golden-set-v2-entity-universe.json", "utf8")
  );
  for (const e of uni.entities || []) {
    if (e.id && e.name) names[e.id] = e.name;
  }
} catch {
  /* optional */
}

const brandIds = marriott.brandIds;
const store = createBrandAiVisibilityReadStore({});
const dealalityUser = { id: "pdf-export", role: "admin" };
const viewerContext = normalizeAiVisibilityViewerContext(dealalityUser);
viewerContext.roles = ["admin"];
const entitlementGraph = buildFixtureEntitlementGraph({
  entitledBrandIds: brandIds,
  peerBrandIds: brandIds,
  source: "demo_showcase_portfolio",
});
const auth = {
  dealalityUser,
  viewerContext,
  entitlementGraph,
  brandNamesById: names,
  store,
};

const exec = await getBrandExecutiveSummaryPayload({
  ...auth,
  geography: "CALA",
  language: "en",
  provider: "openai",
});
const ov = await getBrandOverviewPayload({
  ...auth,
  brandId: AUTOGRAPH,
  geography: "CALA",
  language: "en",
  provider: "openai",
});
const q = await getBrandQuestionsPayload({
  ...auth,
  brandId: AUTOGRAPH,
  geography: "CALA",
  language: "en",
  provider: "openai",
  limit: 50,
  offset: 0,
});
const qAll = await getBrandQuestionsPayload({
  ...auth,
  brandId: AUTOGRAPH,
  geography: "CALA",
  language: "en",
  provider: "all",
  limit: 50,
  offset: 0,
});
const src = await getBrandSourcesPayload({
  ...auth,
  brandId: AUTOGRAPH,
  geography: "CALA",
  language: "en",
  provider: "openai",
});
const peers = await getBrandCompetitorsPayload({
  ...auth,
  brandId: AUTOGRAPH,
  geography: "CALA",
  language: "en",
  provider: "openai",
});
const portAll = await getBrandPortfolioPayload({
  ...auth,
  geography: "CALA",
  language: "en",
  provider: "all",
});
const byP = await loadObservationsByProviderForCohort({
  store,
  geoFilter: {
    geographyScope: "Region",
    commercialRegion: "CALA",
    country: null,
    key: "CALA",
  },
  language: "en",
  providers: ["openai", "gemini", "perplexity", "claude"],
});

function peersOnPrompt(promptId, provider = "openai") {
  const o = (byP[provider]?.observations || []).find((x) => x.promptId === promptId);
  return (o?.presentEntityIds || []).map((id) => names[id] || id);
}

const discByBrand = {};
for (const b of marriott.brands) {
  discByBrand[b.brandName] = buildDiscoverabilityProductPayload(b.brandId, {
    brandNamesById: names,
  });
}

const freshness = exec.monitoringFreshness || ov.monitoringFreshness || {};
const pos = exec.currentPosition || {};
const brands = exec.portfolioOverview?.brands || [];
const insights = exec.executiveInsights?.boxes || [];
const detailInsights = ov.detailExecutiveInsights?.boxes || [];
const providerRows = ov.providerPresencePanel?.rows || [];
const intentRows = ov.decisionPatterns?.ownerIntentCoverage?.rows || [];
const questions = q.questions || [];
const questionsAll = qAll.questions || [];
const missingQs = questions.filter((x) => x.presenceObserved === false);
const lang = exec.languageComparison || {};
const secondary = ov.secondary || {};
const ownedSec = ov.ownedCitationSecondary || {};
const disc = ov.publicDiscoverability || {};
const srcIntel = src.sourceIntelligence || src.citedSourceIntelligence || {};
const freq = srcIntel.DOMAIN_FREQUENCY || [];
const peerRows = (peers.peers || peers.rows || peers.competitors || []).slice(0, 15);

const html = `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<title>Brand AI Visibility — Marriott · Autograph · CALA · EN · OpenAI</title>
<style>
  @page { size: A4; margin: 14mm 12mm; }
  * { box-sizing: border-box; }
  body { font-family: "Segoe UI", system-ui, sans-serif; color: #1a1f2c; font-size: 11px; line-height: 1.45; margin: 0; }
  h1 { font-size: 22px; margin: 0 0 4px; }
  h2 { font-size: 15px; margin: 22px 0 6px; border-bottom: 2px solid #1a1f2c; padding-bottom: 4px; page-break-after: avoid; }
  h3 { font-size: 12px; margin: 12px 0 6px; color: #334; }
  .sub { color: #556; margin: 0 0 12px; }
  .meta { display: flex; flex-wrap: wrap; gap: 8px 16px; background: #f4f6f9; padding: 10px 12px; border-radius: 8px; margin-bottom: 14px; }
  .meta span { white-space: nowrap; }
  .disclaimer { border: 1px solid #d7dbe3; border-radius: 8px; padding: 8px 10px; margin: 10px 0 16px; background: #fafbfc; font-size: 10px; color: #445; }
  .kpi-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin: 8px 0 12px; }
  .kpi { border: 1px solid #e2e6ee; border-radius: 8px; padding: 8px 10px; background: #fff; page-break-inside: avoid; }
  .kpi-l { font-size: 9px; text-transform: uppercase; letter-spacing: .04em; color: #667; }
  .kpi-v { font-size: 16px; font-weight: 700; margin-top: 2px; word-break: break-word; }
  .kpi-h { font-size: 9px; color: #667; margin-top: 4px; }
  .insight-row { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
  .insight { border: 1px solid #e2e6ee; border-radius: 8px; padding: 8px 10px; page-break-inside: avoid; }
  .insight-t { font-weight: 700; font-size: 11px; }
  .insight-f { margin-top: 4px; }
  .insight-e { color: #2563eb; font-weight: 600; margin-top: 4px; }
  .insight-s { color: #556; margin-top: 4px; font-size: 10px; }
  table { width: 100%; border-collapse: collapse; margin: 6px 0 12px; font-size: 10px; }
  th, td { border-bottom: 1px solid #e5e8ef; padding: 5px 6px; text-align: left; vertical-align: top; }
  th { background: #f4f6f9; font-size: 9px; text-transform: uppercase; letter-spacing: .03em; color: #445; }
  tr:nth-child(even) td { background: #fafbfc; }
  .part { page-break-before: always; }
  .part:first-of-type { page-break-before: auto; }
  .tag { display: inline-block; background: #eef2ff; color: #3730a3; padding: 1px 6px; border-radius: 999px; font-size: 9px; font-weight: 600; }
  .miss { color: #b45309; font-weight: 600; }
  .ok { color: #047857; font-weight: 600; }
  .note { font-size: 10px; color: #556; margin: 4px 0 10px; }
  ul, ol { margin: 4px 0 10px 16px; padding: 0; }
  .footer { margin-top: 18px; font-size: 9px; color: #778; border-top: 1px solid #dde; padding-top: 8px; }
</style></head><body>

<div class="part">
  <h1>Brand AI Visibility</h1>
  <p class="sub">See how your brands appear when hotel owners ask AI platforms about development, conversion, and positioning.</p>
  <div class="meta">
    <span><b>Portfolio:</b> Marriott</span>
    <span><b>Hero brand:</b> Autograph Collection</span>
    <span><b>Geography:</b> CALA</span>
    <span><b>Language:</b> English</span>
    <span><b>Provider:</b> OpenAI (+ multi-provider panels)</span>
    <span><b>Monitored:</b> ${esc(freshness.LAST_MONITORED_DISPLAY || "Aug 14, 2026")}</span>
    <span><b>Providers completed:</b> ${esc(freshness.PROVIDERS_COMPLETED_DISPLAY || "4 of 4")}</span>
    <span><b>Mode:</b> Latest monitored results · MANUAL_GOVERNED</span>
  </div>
  <div class="disclaimer">
    <b>Disclaimer.</b> AI model outputs are probabilistic and may vary between monitoring runs.
    Dealality validates calculation accuracy, evidence traceability, and monitoring consistency.
    Interpret AI answers as observed model behavior—not guaranteed fact or advice.
    All Providers is a derived view, not a combined AI run. Citations are associated, not causal.
    Presence does not mean recommendation. Discoverability is a separate diagnostic.
  </div>

  <h2>Executive Summary</h2>
  <div class="insight-row">${insightCards(insights)}</div>

  <h2>Portfolio Snapshot</h2>
  <div class="kpi-row">
    ${kpiCards([
      ["Portfolio AI Presence", d(pos.portfolioAiPresence), pos.portfolioAiPresence?.helper],
      ["Brands Monitored", d(pos.brandsMonitored), null],
      [
        "Top Brand by AI Presence",
        (pos.topBrandByAiPresence?.brandName || "—") +
          " · " +
          pct(pos.topBrandByAiPresence?.presence),
        null,
      ],
      ["Questions Missing", d(pos.questionsMissing), pos.questionsMissing?.helper],
      ["Best Competitive Position", d(pos.bestCompetitivePosition), null],
      ["Decision Visibility Coverage", d(pos.decisionVisibilityCoverage), null],
      ["Top Decision Territory", pos.topDecisionTerritory?.intentTerritory || "—", null],
      ["Evidence-Backed Review Items", d(pos.evidenceBackedReviewItemCount), null],
    ])}
  </div>

  <h2>Your Brands</h2>
  <table><thead><tr><th>Brand</th><th>AI Presence</th><th>Competitive Position</th><th>Questions Missing</th><th>Top Decision Territory</th></tr></thead>
  <tbody>
  ${brands
    .map(
      (b) => `<tr>
    <td>${esc(b.brandName)}</td>
    <td>${d(b.aiPresence)}</td>
    <td>${d(b.competitivePosition)}</td>
    <td>${d(b.questionsMissing)}</td>
    <td>${esc(b.topDecisionTerritory || "—")}</td>
  </tr>`
    )
    .join("")}
  </tbody></table>

  <h2>Visibility Signals</h2>
  <h3>Top Strengths</h3>
  <ul>
    <li>Autograph Collection leads AI Presence in CALA at 91.7% (#1 of 15).</li>
    <li>Tribute Portfolio follows at 83.3% (#3 of 15).</li>
  </ul>
  <h3>Gaps / Risks</h3>
  <ul>
    <li>AC Hotels by Marriott at 8.3% Presence (11 of 12 questions missing).</li>
    <li>Westin at 25.0%; Design Hotels at 41.7%.</li>
  </ul>

  <h2>Peer Context &amp; Next Review</h2>
  <div class="kpi-row">
    ${kpiCards([
      ["Best Competitive Position", d(pos.bestCompetitivePosition), "Autograph among governed peer set"],
      ["Priority Review Focus", "Branded Residences Missing", "Autograph miss across providers"],
      ["Provider Spread (Autograph)", "OpenAI 91.7% → Perplexity 58.3%", "See Provider Presence"],
      ["Review Item Count", d(pos.evidenceBackedReviewItemCount), null],
    ])}
  </div>
</div>

<div class="part">
  <h2>Evidence, Sources &amp; Discovery (Executive)</h2>
  <h3>Citation Intelligence (Autograph · OpenAI · CALA · EN)</h3>
  <div class="kpi-row">
    ${kpiCards([
      ["Citation Rate", d(secondary.citationRate), "Share of responses with ≥1 citation"],
      [
        "Owned Source Citation Rate",
        d(secondary.ownedSourceCitationRate || ownedSec.ownedSourceCitationRate),
        "Configured owned domains",
      ],
      ["Third-Party Citation Rate", d(secondary.thirdPartyCitationRate), null],
      ["Owned Domains Cited", (ownedSec.ownedDomainsCited || []).join(", ") || "—", null],
    ])}
  </div>
  <h3>Source Landscape — Citation Frequency</h3>
  <p class="note">Frequency = share of successful responses citing the domain. Not influence or ranking power.</p>
  <table><thead><tr><th>Domain</th><th>Responses citing</th><th>Frequency</th><th>Prompt families</th></tr></thead>
  <tbody>
  ${freq
    .slice(0, 12)
    .map(
      (f) => `<tr>
    <td>${esc(f.domain)}</td>
    <td>${esc(f.RESPONSES_CITING_SOURCE)} / ${esc(f.COMPARABLE_RESPONSES)}</td>
    <td>${esc(f.SOURCE_CITATION_FREQUENCY_DISPLAY || pct(f.SOURCE_CITATION_FREQUENCY))}</td>
    <td>${esc((f.PROMPT_FAMILIES_CITING_SOURCE || []).join(", "))}</td>
  </tr>`
    )
    .join("")}
  </tbody></table>

  <h3>Provider Visibility (Autograph)</h3>
  <table><thead><tr><th>Provider</th><th>Presence</th><th>Present / Monitored</th><th>Questions Missing</th></tr></thead>
  <tbody>
  ${providerRows
    .map(
      (r) => `<tr>
    <td>${esc(r.PROVIDER_LABEL || r.PROVIDER)}</td>
    <td>${esc(r.PRESENCE_RATE_DISPLAY)}</td>
    <td>${esc(r.PRESENT_N)} / ${esc(r.MONITORED_N)}</td>
    <td>${esc(r.QUESTIONS_MISSING_N)}</td>
  </tr>`
    )
    .join("")}
  </tbody></table>

  <h3>Public Discoverability (Marriott portfolio)</h3>
  <table><thead><tr><th>Brand</th><th>State</th><th>Last checked</th></tr></thead>
  <tbody>
  ${Object.entries(discByBrand)
    .map(
      ([name, p]) => `<tr>
    <td>${esc(name)}</td>
    <td>${esc(p.DISCOVERABILITY)}</td>
    <td>${esc(p.LAST_CHECKED_AT || "—")}</td>
  </tr>`
    )
    .join("")}
  </tbody></table>

  <h3>Language Comparison (Autograph)</h3>
  <div class="kpi-row">
    ${kpiCards([
      ["EN AI Presence", pct(lang.EN_AI_PRESENCE ?? lang.EN_PRESENCE), null],
      ["ES AI Presence", pct(lang.ES_AI_PRESENCE ?? lang.ES_PRESENCE), null],
      ["EN Questions Missing", String(lang.EN_QUESTIONS_MISSING ?? "—"), null],
      ["ES Questions Missing", String(lang.ES_QUESTIONS_MISSING ?? "—"), null],
    ])}
  </div>
  <p class="note">${esc(lang.presenceNote || "")}</p>
</div>

<div class="part">
  <h1>Detailed View — Autograph Collection</h1>
  <p class="sub">CALA · English · OpenAI (with All Providers / multi-provider diagnostics)</p>

  <h2>Brand Overview</h2>
  <div class="insight-row">${insightCards(detailInsights)}</div>

  <h2>Brand Detail</h2>
  <div class="kpi-row">
    ${kpiCards([
      ["AI Presence", d(ov.kpis?.aiPresence), null],
      ["Competitive Position", d(ov.kpis?.competitivePosition), ov.kpis?.competitivePosition?.helper],
      ["Questions Missing", d(ov.kpis?.questionsMissing), ov.kpis?.questionsMissing?.helper],
      ["Δ vs prior run", ov.kpis?.aiPresence?.delta?.display || "—", null],
    ])}
  </div>

  <h2>Coverage Diagnostics</h2>
  <h3>Provider Presence</h3>
  <table><thead><tr><th>Provider</th><th>Presence</th><th>Status</th><th>Missing N</th><th>Δ</th></tr></thead>
  <tbody>
  ${providerRows
    .map(
      (r) => `<tr>
    <td>${esc(r.PROVIDER_LABEL)}</td>
    <td>${esc(r.PRESENCE_RATE_DISPLAY)}</td>
    <td>${esc(r.MONITORING_STATUS_DISPLAY)}</td>
    <td>${esc(r.QUESTIONS_MISSING_N)}</td>
    <td>${esc(r.DELTA_DISPLAY)}</td>
  </tr>`
    )
    .join("")}
  </tbody></table>
  <h3>Owner-Intent Coverage</h3>
  <table><thead><tr><th>Intent Territory</th><th>Coverage</th><th>Present / Monitored</th></tr></thead>
  <tbody>
  ${intentRows
    .map(
      (r) => `<tr>
    <td>${esc(r.intentTerritory)}</td>
    <td>${esc(r.display)}</td>
    <td>${esc(r.numerator)} / ${esc(r.denominator)}</td>
  </tr>`
    )
    .join("")}
  </tbody></table>

  <h2>Questions Missing Watchlist</h2>
  <p class="note">OpenAI · CALA · EN — questions where Autograph was not observed.</p>
  <table><thead><tr><th>Question</th><th>Prompt Family</th><th>Status</th><th>Peers Present (OpenAI)</th><th>Evidence ID</th></tr></thead>
  <tbody>
  ${(missingQs.length
    ? missingQs
    : [
        {
          question: "(none)",
          intentTerritory: "—",
          presenceLabel: "—",
          evidenceId: "—",
        },
      ]
  )
    .map((row) => {
      const peersPresent = row.promptId
        ? peersOnPrompt(row.promptId, "openai").join(", ")
        : "—";
      return `<tr>
      <td>${esc(row.question)}</td>
      <td>${esc(row.intentTerritory)}</td>
      <td class="miss">${esc(row.presenceLabel || "Missing")}</td>
      <td>${esc(peersPresent || "—")}</td>
      <td>${esc(row.evidenceId || "—")}</td>
    </tr>`;
    })
    .join("")}
  </tbody></table>

  <h3>All Providers — Cross-Provider Question States</h3>
  <table><thead><tr><th>Question / Prompt</th><th>State</th><th>Present on</th><th>Missing on</th></tr></thead>
  <tbody>
  ${questionsAll
    .map(
      (row) => `<tr>
    <td>${esc(row.question || row.promptId)}</td>
    <td>${esc(row.CROSS_PROVIDER_STATE || row.presenceLabel)}</td>
    <td>${esc((row.PROVIDERS_PRESENT || []).join(", ") || "—")}</td>
    <td>${esc((row.PROVIDERS_MISSING || []).join(", ") || "—")}</td>
  </tr>`
    )
    .join("")}
  </tbody></table>

  <h2>Competitive / Peer Analysis</h2>
  <p class="note">Rank by AI Presence within the governed peer set (provider-specific). Recommendation metrics omitted (not V1 product claims).</p>
  <table><thead><tr><th>Brand</th><th>AI Presence</th><th>Rank</th></tr></thead>
  <tbody>
  ${peerRows
    .map((r) => {
      const rate = r.aiPresenceRate ?? r.presenceRate ?? r.aiPresence?.value;
      const rank =
        r.competitivePosition != null
          ? "#" + r.competitivePosition
          : r.rank != null
            ? "#" + r.rank
            : "—";
      return `<tr>
    <td>${esc(r.entityName || r.brandName || r.name)}${
        r.isSubject ? ' <span class="tag">Subject</span>' : ""
      }</td>
    <td>${pct(rate)}</td>
    <td>${esc(rank)}</td>
  </tr>`;
    })
    .join("")}
  </tbody></table>
</div>

<div class="part">
  <h2>Owner Questions (full OpenAI cohort)</h2>
  <table><thead><tr><th>Question</th><th>Family</th><th>Status</th><th>Evidence</th></tr></thead>
  <tbody>
  ${questions
    .map(
      (row) => `<tr>
    <td>${esc(row.question)}</td>
    <td>${esc(row.intentTerritory)}</td>
    <td class="${row.presenceObserved ? "ok" : "miss"}">${esc(row.presenceLabel)}</td>
    <td>${esc(row.evidenceId || "—")}</td>
  </tr>`
    )
    .join("")}
  </tbody></table>

  <h2>Citation &amp; Source Intelligence (Detail)</h2>
  <div class="kpi-row">
    ${kpiCards([
      ["Citation Rate", d(secondary.citationRate), null],
      ["Owned Source Coverage", d(ownedSec.ownedSourceCitationRate), "Configured owned domains only"],
      ["Third-Party Citations", d(ownedSec.thirdPartyCitationRate), null],
      ["Owned Domain Status", ownedSec.OWNED_DOMAIN_STATUS || disc.ownedDomainStatus || "—", null],
    ])}
  </div>
  <h3>Top Cited Domains</h3>
  <ol>
  ${freq
    .slice(0, 10)
    .map(
      (f) =>
        `<li><b>${esc(f.domain)}</b> — ${esc(f.SOURCE_CITATION_FREQUENCY_DISPLAY)} (${esc(
          f.RESPONSES_CITING_SOURCE
        )}/${esc(f.COMPARABLE_RESPONSES)} responses)</li>`
    )
    .join("")}
  </ol>

  <h2>Public Discoverability (Autograph)</h2>
  <div class="kpi-row">
    ${kpiCards([
      ["State", disc.DISCOVERABILITY || disc.status || "—", null],
      ["Last Checked", disc.LAST_CHECKED_AT || "—", null],
      ["Official Sources Configured", String(disc.OFFICIAL_SOURCES_CONFIGURED), null],
      ["Owner Development Content Found", String(disc.OWNER_DEVELOPMENT_CONTENT_FOUND), null],
    ])}
  </div>
  <p class="note">Owner-intent content gaps: ${esc(
    (disc.OWNER_INTENT_CONTENT_GAPS || []).join(", ") || "none listed"
  )}</p>
  <ul>
    <li>Final URL: ${esc(disc.baseline?.FINAL_URL || "—")}</li>
    <li>HTTP: ${esc(disc.baseline?.HTTP_STATUS)} · Indexability: ${esc(
      disc.baseline?.INDEXABILITY_STATE
    )}</li>
    <li>Brand website: ${esc(disc.brandRow?.brandWebsite || "—")}</li>
    <li>Development URL: ${esc(disc.brandRow?.brandDevelopmentUrl || "—")}</li>
    <li>Residences URL: ${esc(disc.brandRow?.brandedResidencesSourceUrl || "—")}</li>
  </ul>

  <h2>All Providers Portfolio Snapshot</h2>
  <p class="note">Derived averages — not a combined AI run. Peer rank requires a specific provider.</p>
  <table><thead><tr><th>Brand</th><th>All Providers Presence</th><th>Questions Missing</th><th>Provider Breakdown</th></tr></thead>
  <tbody>
  ${(portAll.brands || [])
    .map((b) => {
      const br = (b.crossProviderPresence?.PROVIDER_PRESENCE_BREAKDOWN || [])
        .map((p) => `${p.label || p.provider}: ${pct(p.presenceRate)}`)
        .join(" · ");
      return `<tr>
      <td>${esc(b.brandName)}</td>
      <td>${d(b.aiPresence)}</td>
      <td>${d(b.questionsMissing)}</td>
      <td>${esc(br)}</td>
    </tr>`;
    })
    .join("")}
  </tbody></table>

  <div class="footer">
    Export generated ${new Date().toISOString()} from federated measured baseline (no new provider runs).
    Filters mirrored: Marriott · Autograph Collection · CALA · English · OpenAI (+ All Providers diagnostics).
    Full-section data export for pilot/session use. Pixel-perfect live UI PDF requires an authenticated browser session.
  </div>
</div>
</body></html>`;

const outDir = path.resolve("data/ai-visibility/exports");
fs.mkdirSync(outDir, { recursive: true });
const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const htmlPath = path.join(
  outDir,
  `brand-ai-visibility-marriott-autograph-cala-en-${stamp}.html`
);
const pdfPath = path.join(
  outDir,
  `brand-ai-visibility-marriott-autograph-cala-en-${stamp}.pdf`
);
fs.writeFileSync(htmlPath, html, "utf8");

if (!fs.existsSync(CHROME)) {
  console.error(
    JSON.stringify({ ok: false, error: "CHROME_NOT_FOUND", htmlPath, CHROME }, null, 2)
  );
  process.exit(1);
}

const userData = path.join(outDir, "_chrome-print-profile");
fs.mkdirSync(userData, { recursive: true });
const fileUrl = "file:///" + htmlPath.replace(/\\/g, "/");
const args = [
  "--headless=new",
  "--disable-gpu",
  "--no-pdf-header-footer",
  `--user-data-dir=${userData}`,
  `--print-to-pdf=${pdfPath}`,
  "--print-to-pdf-no-header",
  fileUrl,
];
const r = spawnSync(CHROME, args, { encoding: "utf8", timeout: 90000 });
const ok = fs.existsSync(pdfPath) && fs.statSync(pdfPath).size > 1000;
console.log(
  JSON.stringify(
    {
      ok,
      htmlPath,
      pdfPath,
      pdfBytes: ok ? fs.statSync(pdfPath).size : 0,
      status: r.status,
      stderr: (r.stderr || "").slice(-800),
    },
    null,
    2
  )
);
if (!ok) process.exit(1);
