#!/usr/bin/env node
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const OUT = join(dirname(fileURLToPath(import.meta.url)), "../reports/brand-explorer/portfolio-content-quality-audit-v1");
const summary = JSON.parse(readFileSync(join(OUT, "00-summary.json"), "utf8"));
const inv = JSON.parse(readFileSync(join(OUT, "01-master-inventory.json"), "utf8"));
const rows = inv.rows || [];

function esc(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

const tr = rows
  .map((r) => {
    const fails = (r.quality?.criticalFails || []).slice(0, 2).join("; ");
    return `<tr data-status="${esc(r.processingStatus)}" data-gate="${esc(r.quality?.gate)}"><td>${esc(r.brandName)}</td><td><code>${esc(r.brandId)}</code></td><td>${esc((r.batches || []).join(","))}</td><td>${esc(r.processingStatus)}</td><td>${r.tabsPresent?.count ?? 0}/10</td><td>${r.quality?.total ?? ""}</td><td>${esc(r.quality?.gate)}</td><td>${r.loyaltySixLikely ? "Y" : "N"}</td><td>${r.demandSixLikely ? "Y" : "N"}</td><td>${r.inRegistry ? "Y" : "N"}</td><td><code>${esc((r.candidateHash || "").slice(0, 12))}</code></td><td><a href="${esc(r.compareStudioUrl)}">Compare</a></td><td>${esc(fails)}</td></tr>`;
  })
  .join("\n");

const statusOpts = Object.keys(summary.processingStatusCounts || {})
  .map((k) => `<option value="${esc(k)}">${esc(k)}</option>`)
  .join("");
const gateOpts = Object.keys(summary.qualityGateCounts || {})
  .map((k) => `<option value="${esc(k)}">${esc(k)}</option>`)
  .join("");

const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BE Portfolio Audit Dashboard</title>
  <style>
    body{font-family:Segoe UI,system-ui,sans-serif;margin:24px;background:#0f1419;color:#e7ecf1}
    h1{font-size:1.4rem;margin:0 0 8px}
    .meta{color:#9aa7b5;margin-bottom:16px}
    .cards{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:18px}
    .card{background:#1a222c;border:1px solid #2a3542;border-radius:10px;padding:12px 14px;min-width:140px}
    .card b{display:block;font-size:1.35rem}
    input,select{background:#121820;color:#e7ecf1;border:1px solid #2a3542;border-radius:8px;padding:6px 8px;margin-right:8px}
    table{width:100%;border-collapse:collapse;font-size:0.82rem}
    th,td{border-bottom:1px solid #243040;padding:6px 8px;text-align:left;vertical-align:top}
    th{position:sticky;top:0;background:#151b22}
    a{color:#8ec7ff}
    code{font-size:0.78rem}
  </style>
</head>
<body>
  <h1>Brand Explorer — Portfolio Content &amp; Quality Audit</h1>
  <p class="meta">Generated ${esc(summary.generatedAt)} · Contract BE_PORTFOLIO_CONTENT_QUALITY_AUDIT_V1 · PORTFOLIO_AUDIT_COMPLETE: NO · Comparison Studio inventory: ALIGNED</p>
  <div class="cards">
    <div class="card"><span>Universe</span><b>${summary.universeProcessedOrQueued}</b></div>
    <div class="card"><span>In Studio registry</span><b>${summary.inStagedRegistry}</b></div>
    <div class="card"><span>Staged candidates</span><b>${summary.withCandidateArtifact}</b></div>
    <div class="card"><span>Needs remediation</span><b>${summary.processingStatusCounts?.NEEDS_REMEDIATION || 0}</b></div>
    <div class="card"><span>Airtable persisted</span><b>${summary.processingStatusCounts?.AIRTABLE_PERSISTED || 0}</b></div>
    <div class="card"><span>Studio gaps</span><b>${summary.missingFromComparisonStudio}</b></div>
    <div class="card"><span>Fee unverified</span><b>${summary.feeUnverified}</b></div>
    <div class="card"><span>Gallery gaps</span><b>${summary.galleryGaps}</b></div>
  </div>
  <p>
    <input id="q" type="search" placeholder="Filter brands…" />
    <select id="status"><option value="">All statuses</option>${statusOpts}</select>
    <select id="gate"><option value="">All gates</option>${gateOpts}</select>
    <span id="count"></span>
  </p>
  <table>
    <thead><tr><th>Brand</th><th>ID</th><th>Batch</th><th>Status</th><th>Tabs</th><th>Score</th><th>Gate</th><th>L6</th><th>D6</th><th>Studio</th><th>Hash</th><th>Compare</th><th>Critical</th></tr></thead>
    <tbody id="tb">${tr}</tbody>
  </table>
  <script>
    const q=document.getElementById('q'), st=document.getElementById('status'), g=document.getElementById('gate'), tb=document.getElementById('tb'), count=document.getElementById('count');
    function filt(){const qq=q.value.toLowerCase(); let n=0; [...tb.rows].forEach(r=>{const hay=r.innerText.toLowerCase(); const ok=(!qq||hay.includes(qq))&&(!st.value||r.dataset.status===st.value)&&(!g.value||r.dataset.gate===g.value); r.style.display=ok?'':'none'; if(ok)n++;}); count.textContent=n+' shown';}
    q.oninput=st.onchange=g.onchange=filt; filt();
  </script>
</body>
</html>
`;

writeFileSync(join(OUT, "12-founder-dashboard.html"), html);
writeFileSync(
  join(OUT, "README.md"),
  readFileSync(join(OUT, "README.md"), "utf8").replace(
    "| `11-remaining-blockers.md` | Blockers / next actions |\n",
    "| `11-remaining-blockers.md` | Blockers / next actions |\n| `12-founder-dashboard.html` | Interactive founder dashboard |\n"
  )
);
console.log(JSON.stringify({ ok: true, rows: rows.length, out: join(OUT, "12-founder-dashboard.html") }));
