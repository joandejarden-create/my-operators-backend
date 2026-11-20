#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildScalingTwoContactPresentation } from "../lib/group-demand-intelligence/scaling-two-contact-standard-v1.js";
import { buildContactChannelDisplayState } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OPP =
  "data/group-demand-intelligence/hotels/recCAGqkERSXgiku4/opportunities.json";
const OUT = path.join(ROOT, "reports/gdi-public-contact-discovery-2-v1");

const d = JSON.parse(fs.readFileSync(path.join(ROOT, OPP), "utf8"));
const row = d.opportunities.find((o) => o.id === "paid3_0a6b19883953");
if (!row) throw new Error("Ecomondo opportunity missing");

const pres = buildScalingTwoContactPresentation(row);
const disp = buildContactChannelDisplayState(row);
const p = pres.primary;
const s = pres.secondary;
const esc = (v) =>
  String(v == null ? "" : v)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

const secondaryHtml =
  s && s.customerFacingNamed
    ? `<div class="sec"><h2>Secondary: ${esc(s.name)}</h2>
       <div class="label">Email</div><div>${esc(s.email || "—")}</div></div>`
    : "";

const html = `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<title>Alejandra public contact card — Surfe-last</title>
<style>
  body{font-family:Georgia,serif;background:#0f1419;color:#e8e6e3;margin:0;padding:32px}
  .card{max-width:560px;margin:0 auto;padding:28px 32px;border:1px solid #3a4550;border-radius:12px;background:#1a222c}
  h1{font-size:1.15rem;margin:0 0 4px}
  h2{font-size:1rem;margin:0 0 8px}
  .role{opacity:.85;margin:0 0 16px}
  .label{opacity:.7;font-size:.78rem;text-transform:uppercase;letter-spacing:.04em}
  .row{margin:10px 0}
  .badge{display:inline-block;margin-top:18px;padding:6px 10px;border-radius:6px;background:#1e3a2f;color:#8fd6b0;font-size:.8rem}
  .sec{margin-top:22px;padding-top:16px;border-top:1px solid #3a4550}
  .note{margin-top:20px;opacity:.65;font-size:.8rem;line-height:1.4}
</style></head><body>
<div class="card">
  <h1>${esc(p.name)}</h1>
  <p class="role">${esc(p.title || "")} · ${esc(p.organization || "")}</p>
  <div class="row"><div class="label">${esc(p.emailLabel || "Direct Business Email")}</div><div>${esc(p.email || "—")}</div></div>
  <div class="row"><div class="label">Phone</div><div>${esc(p.phone || "— (unresolved; Find Phone only)")}</div></div>
  <div class="badge">Surfe CTA: ${esc(disp.surfeCtaLabel || "NONE")}</div>
  ${secondaryHtml}
  <p class="note">Source: ecomondomexico.com.mx/en/contact/ · PUBLIC_SOURCE · hotel-booking authority unverified · Surfe spend: 0</p>
</div>
</body></html>
`;

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "ALEJANDRA_CONTACT_CARD.html"), html, "utf8");
console.log(
  JSON.stringify(
    {
      ok: true,
      email: p.email,
      surfeCtaLabel: disp.surfeCtaLabel,
      secondaryName: s?.name || null,
      secondaryEmail: s?.email || null,
      html: path.join(OUT, "ALEJANDRA_CONTACT_CARD.html"),
    },
    null,
    2,
  ),
);
