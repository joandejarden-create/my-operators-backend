#!/usr/bin/env node
/**
 * Parse Bethesda/Mayflower sales-review markdown into a drawer enrichment overlay.
 * Recover hotel-specific thesis / next actions that the CSV commercialStatus discarded.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REVIEW_DIR = path.join(
  ROOT,
  "reports/gdi-bethesda-process-replication/hotel-review"
);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function extractCards(md, hotelKey) {
  const parts = md.split(/\n### /).slice(1);
  const out = [];
  for (const p of parts) {
    const accountLine = p.split("\n")[0].trim();
    const cardId = (p.match(/\*\*Card ID:\*\*\s*`([^`]+)`/) || [])[1];
    if (!cardId) continue;
    const get = (lab) => {
      const m = p.match(new RegExp(`\\*\\*${lab}:\\*\\*\\s*([^\\n]+)`));
      return m ? text(m[1]) : null;
    };
    const sourcesLine = get("Sources");
    const sourceUrls = [];
    if (sourcesLine) {
      for (const m of sourcesLine.matchAll(/https?:\/\/[^\s)]+/gi)) {
        sourceUrls.push(m[0].replace(/[.,;]+$/, ""));
      }
    }
    out.push({
      hotelKey,
      cardId,
      account: get("Account") || accountLine,
      travelers: get("Relevant travelers / business occasion"),
      timing: get("Timing"),
      whyFits: get("Why it fits this hotel"),
      whatWeKnow: get("What we know"),
      nextAction: get("Recommended next action"),
      sourcesLine,
      sourceUrls,
      contactLine: (p.match(/\*\*Contact package:\*\*[^\n]+/) || [null])[0],
    });
  }
  return out;
}

const may = fs.readFileSync(path.join(REVIEW_DIR, "mayflower-sales-review.md"), "utf8");
const beth = fs.readFileSync(path.join(REVIEW_DIR, "bethesda-sales-review.md"), "utf8");
const cards = [
  ...extractCards(may, "mayflower"),
  ...extractCards(beth, "bethesda"),
];

const outPath = path.join(
  ROOT,
  "reports/gdi-drawer-completeness/sales-review-overlay.json"
);
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(
  outPath,
  JSON.stringify({ asOf: new Date().toISOString(), cards }, null, 2) + "\n"
);
console.log(JSON.stringify({ ok: true, count: cards.length, outPath }, null, 2));
