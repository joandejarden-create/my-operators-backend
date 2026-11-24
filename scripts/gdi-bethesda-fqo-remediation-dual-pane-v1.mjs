#!/usr/bin/env node
/**
 * Isolated dual-pane HTML: gold vs remediation candidate ($0). Port 8080 untouched.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recLuxvwwxID7U2B8";
const GOLD = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const CAND = "gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261124_5e4d5630";
const OUT = path.join(ROOT, "reports/gdi-bethesda-fqo-remediation-v1");

function esc(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function card(o, label) {
  if (!o) return `<div class="miss">${esc(label)}: MISSING</div>`;
  const pc = o.primaryContact || {};
  const how =
    pc.email ||
    pc.phone ||
    pc.linkedinUrl ||
    pc.askToBeDirectedTo ||
    "(no channel)";
  const act = String(o.recommendedNextStep || o.cardWhyNowLine || "").slice(0, 160);
  return `<div class="card"><div class="h">${esc(o.title)}</div>
    <div class="meta">${esc(o.opportunityType || o.opportunityTypeLabel || "")} · ${esc(o.salesActionPillLabel || o.tileBadgePlan?.salesActionPillLabel || "")}</div>
    <div class="who">${esc(pc.name || "(no name)")} — ${esc(pc.role || "")}</div>
    <div class="how">${esc(how)}</div>
    <div class="act">${esc(act)}</div></div>`;
}

const gold = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      HOTEL,
      GOLD,
      "package.json",
    ),
    "utf8",
  ),
);
const cand = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      HOTEL,
      CAND,
      "package.json",
    ),
    "utf8",
  ),
);
const disp = JSON.parse(
  fs.readFileSync(path.join(OUT, "GOLD_67_DISPOSITIONS.json"), "utf8"),
);
const candById = new Map(cand.opportunities.map((o) => [o.id, o]));

const rows = gold.opportunities
  .map((g) => {
    const c = candById.get(g.id);
    const d = disp.rows.find((r) => r.canonicalId === g.id);
    return `<tr class="${c ? "ok" : "gap"}"><td class="id">${esc(g.id)}<div class="disp">${esc(d?.disposition)} / ${esc(d?.processingDisposition)}</div></td><td>${card(g, "GOLD")}</td><td>${card(c, "CAND")}</td></tr>`;
  })
  .join("\n");

const html = `<!doctype html>
<html><head><meta charset="utf-8"/>
<title>Bethesda FQO Remediation Dual-Pane</title>
<style>
body{font:13px/1.4 system-ui;margin:16px;background:#f7f5f1;color:#1a1a1a}
h1{font-size:20px;margin:0 0 8px} .sum{margin-bottom:16px}
table{width:100%;border-collapse:collapse} td{vertical-align:top;border-top:1px solid #ddd;padding:10px 8px}
.card{background:#fff;border:1px solid #e2ddd4;padding:10px;border-radius:6px}
.h{font-weight:600;margin-bottom:4px}.meta{color:#666;font-size:12px;margin-bottom:6px}
.who,.how,.act{font-size:12px;margin-top:3px}.miss{color:#a30;font-weight:600}
.gap{background:#fff4f0}.ok .disp{color:#176}.id{width:220px;font-size:11px;word-break:break-all}
.disp{margin-top:4px;font-weight:600}
</style></head><body>
<h1>Bethesda Marriott — Gold vs Remediation Candidate</h1>
<div class="sum">Gold 67 · Candidate ${cand.opportunityCount} · Gold-in-candidate ${disp.rows.filter((r) => r.inCandidate).length} · Genuine review gaps ${disp.rows.filter((r) => !r.inCandidate).length} · Pointers unchanged · Isolated dual-pane (8080 untouched)</div>
<table><thead><tr><th>ID / Disposition</th><th>Original gold (approved)</th><th>New isolated candidate</th></tr></thead><tbody>
${rows}
</tbody></table></body></html>
`;

const visualDir = path.join(OUT, "visual");
fs.mkdirSync(visualDir, { recursive: true });
const htmlPath = path.join(visualDir, "dual-pane-gold-vs-candidate.html");
fs.writeFileSync(htmlPath, html);
console.log("wrote", path.relative(ROOT, htmlPath));
