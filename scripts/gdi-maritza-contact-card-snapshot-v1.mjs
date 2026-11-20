#!/usr/bin/env node
/**
 * Render Maritza Surfe-last contact-card HTML + slot JSON for founder evidence.
 */
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
const row = d.opportunities.find((o) => o.id === "disc_b4661c17bd92");
if (!row) throw new Error("Maritza opportunity missing");

const pres = buildScalingTwoContactPresentation(row);
const disp = buildContactChannelDisplayState(row);
const p = pres.primary;
const ext = (p.phoneExtensions || []).join(" / ");
const esc = (s) =>
  String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");

const html = `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<title>Maritza public contact card — Surfe-last</title>
<style>
  body{font-family:Georgia,serif;background:#0f1419;color:#e8e6e3;margin:0;padding:32px}
  .card{max-width:520px;margin:0 auto;padding:28px 32px;border:1px solid #3a4550;border-radius:12px;background:#1a222c}
  h1{font-size:1.15rem;margin:0 0 4px;font-weight:600}
  .role{opacity:.85;margin:0 0 16px;font-size:.95rem}
  .row{margin:10px 0;font-size:.98rem}
  .label{opacity:.7;font-size:.78rem;text-transform:uppercase;letter-spacing:.04em}
  .val{margin-top:2px}
  .badge{display:inline-block;margin-top:18px;padding:6px 10px;border-radius:6px;background:#1e3a2f;color:#8fd6b0;font-size:.8rem}
  .note{margin-top:20px;opacity:.65;font-size:.8rem;line-height:1.4}
</style></head><body>
<div class="card">
  <h1>${esc(p.name)}</h1>
  <p class="role">${esc(p.title || "")} · ${esc(p.organization || "")}</p>
  <div class="row"><div class="label">${esc(p.emailLabel || "Direct Business Email")}</div><div class="val">${esc(p.email || "—")}</div></div>
  <div class="row"><div class="label">${esc(p.phoneLabel || "Phone")}</div><div class="val">${esc(p.phone || "—")}${ext ? ` · Ext. ${esc(ext)}` : ""}</div></div>
  <div class="badge">Surfe CTA: ${disp.surfeCtaLabel == null ? "NONE (public channels verified)" : esc(disp.surfeCtaLabel)}</div>
  <p class="note">Source: ${esc(row.primaryContact?.channelSourceUrl || "—")}<br/>
  Origin: PUBLIC_SOURCE · Authority: RELEVANT_COMMERCIAL_ENTRY_POINT (not hotel-booking buyer)</p>
</div>
</body></html>
`;

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "MARITZA_CONTACT_CARD.html"), html, "utf8");
fs.writeFileSync(
  path.join(OUT, "MARITZA_PRESENTATION_SLOT.json"),
  `${JSON.stringify({ primary: p, display: disp, surfeCtaLabel: disp.surfeCtaLabel }, null, 2)}\n`,
  "utf8",
);
console.log(
  JSON.stringify(
    {
      ok: true,
      email: p.email,
      phone: p.phone,
      extensions: p.phoneExtensions || [],
      surfeCtaLabel: disp.surfeCtaLabel,
      html: path.join(OUT, "MARITZA_CONTACT_CARD.html"),
    },
    null,
    2,
  ),
);
