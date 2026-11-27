#!/usr/bin/env node
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildWhoHowDrawerPresentation } from "../../lib/group-demand-intelligence/workflow/gdi-who-how-agent-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "../..");
const livePath = path.join(
  root,
  "reports/gdi-wyndham-final-commercial-continuation-prep-v1/checkpoint-a/wave2/CHECKPOINT_A_LIVE_RESULT.json",
);
const live = JSON.parse(fs.readFileSync(livePath, "utf8"));
const byStatus = {};
for (const r of live.tranche.results) {
  const s = r.whoHow?.whoHowStatus;
  if (s && !byStatus[s]) byStatus[s] = r;
}
const picks = [
  byStatus.VERIFIED_DIRECT_BUYER,
  byStatus.VERIFIED_BUYER_INFLUENCER,
  byStatus.VERIFIED_ENTRY_POINTS_2,
  live.tranche.results.find(
    (r) =>
      (r.whoHow?.entryPoints || []).filter(
        (e) => e.fullName && e.status !== "NOT_FOUND",
      ).length === 1,
  ),
].filter(Boolean);

const cases = picks.map((r) => ({
  id: r.opportunityId,
  org: r.opportunity?.organizationName,
  title: r.opportunity?.title,
  status: r.whoHow?.whoHowStatus,
  buyerVsEntry: r.opportunity?.buyerVsEntry,
  drawer:
    r.opportunity?.whoHowDrawer ||
    buildWhoHowDrawerPresentation(r.whoHow || {}, r.opportunity || {}),
  primary: r.opportunity?.primaryContact,
  whoHow: r.whoHow,
}));

fs.mkdirSync(
  path.join(
    root,
    "reports/gdi-wyndham-final-commercial-continuation-prep-v1/checkpoint-a",
  ),
  { recursive: true },
);
fs.writeFileSync(
  path.join(
    root,
    "reports/gdi-wyndham-final-commercial-continuation-prep-v1/checkpoint-a/17-BROWSER_PREVIEW_CASES.json",
  ),
  `${JSON.stringify(cases, null, 2)}\n`,
);

function esc(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

const sections = cases
  .map((c) => {
    const people = [];
    if (c.whoHow?.directBuyer?.fullName) {
      people.push(["Primary Lodging Decision-Maker", c.whoHow.directBuyer]);
    }
    if (c.whoHow?.buyerInfluencer?.fullName) {
      people.push(["Verified Buyer Influencer", c.whoHow.buyerInfluencer]);
    }
    (c.whoHow?.entryPoints || []).forEach((ep, idx) => {
      if (ep?.fullName && ep.status !== "NOT_FOUND") {
        people.push([`Alternative Entry Point ${idx + 1}`, ep]);
      }
    });
    const unresolved = !people.length;
    const peopleHtml = unresolved
      ? `<div class="warn">Unresolved WHO/HOW / Contact Research Required — no credible named contact structured.</div>`
      : people
          .map(
            ([label, p]) => `
      <div class="person">
        <strong>${esc(label)}</strong>
        <div>${esc(p.fullName)} — ${esc(p.currentTitle || p.title || "")}</div>
        <div class="muted">${esc(p.organization || "")}</div>
        <div><em>Why:</em> ${esc(p.whyWeAreShowingThisContact || p.whyCredibleRoute || "")}</div>
        <div><em>Approach:</em> ${esc(p.recommendedSalesApproach || "")}</div>
        <div class="muted">Identity verified: ${Boolean(p.identityVerified)} · Buying authority verified: ${Boolean(p.hotelBuyingAuthorityVerified || p.buyerAuthorityVerified)}</div>
        <div class="muted">Source: ${esc(p.sourceUrl || "")}</div>
      </div>`,
          )
          .join("");
    return `<section class="card" data-case="${esc(c.status)}">
    <div class="badge">${esc(c.status)}</div>
    <div class="badge">${esc(c.buyerVsEntry || "n/a")}</div>
    <h2>${esc(c.org || "")}</h2>
    <div class="muted">${esc(c.title || "")} · ${esc(c.id)}</div>
    <h3>Who to contact</h3>
    ${peopleHtml}
    <div class="warn">Surfe: explicit-click only — not auto-invoked on this preview.</div>
  </section>`;
  })
  .join("\n");

const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>GDI Checkpoint A WHO/HOW Preview (non-publishing)</title>
  <style>
    body{font-family:Georgia,serif;margin:0;background:#f6f3ee;color:#1c1917}
    header{padding:24px 28px;background:#1c1917;color:#f6f3ee}
    header p{opacity:.8;max-width:70ch}
    main{padding:24px;display:grid;gap:20px}
    .card{background:#fff;border:1px solid #ddd2c4;padding:20px 22px}
    .badge{display:inline-block;font:12px/1.2 system-ui,sans-serif;padding:4px 8px;border:1px solid #c4b5a0;margin:0 8px 8px 0}
    h2{margin:8px 0 4px;font-size:22px}
    h3{margin:16px 0 8px;font-size:15px;letter-spacing:.04em;text-transform:uppercase;color:#6b5c4c}
    .person{border-top:1px solid #eee;padding:10px 0}
    .muted{color:#6b5c4c;font-size:14px}
    .warn{background:#fff7ed;border:1px solid #fdba74;padding:10px;margin-top:10px}
  </style>
</head>
<body>
  <header>
    <div class="badge">INTERNAL NON-PUBLISHING PREVIEW</div>
    <div class="badge">NO_STAGED · NO_CURRENT · NO_AIRTABLE</div>
    <h1>Checkpoint A WHO/HOW — Wyndham San Diego Bayside</h1>
    <p>Representative live Wave-2 results for drawer validation on integrated staging (port 8080). Surfe remains explicit-click only.</p>
  </header>
  <main>
${sections}
  </main>
</body>
</html>
`;

fs.mkdirSync(path.join(root, "public/internal"), { recursive: true });
const out = path.join(
  root,
  "public/internal/gdi-checkpoint-a-who-how-preview.html",
);
fs.writeFileSync(out, html);
console.log(
  JSON.stringify(
    {
      ok: true,
      out,
      url: "http://localhost:8080/internal/gdi-checkpoint-a-who-how-preview.html",
      cases: cases.map((c) => ({
        id: c.id,
        org: c.org,
        status: c.status,
        buyerVsEntry: c.buyerVsEntry,
      })),
    },
    null,
    2,
  ),
);
