/**
 * AC Hotel A Coruña — HPC Phase 1 duplicate / identity search (read-only).
 * Target: AIRTABLE_BASE_ID_ALT / Hotel Property Census only.
 *
 *   node scripts/ac-hotel-a-coruna-hpc-phase1-search.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "hotel-census", "ac-hotel-a-coruna-onboarding-v1");

const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
if (!apiKey || !baseId) {
  console.error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT");
  process.exit(1);
}
if (baseId === "appvtnDurnMSjINP6") {
  console.error("FORBIDDEN: legacy base blocked for HPC search");
  process.exit(1);
}

const HPC = "Hotel Property Census";
const base = new Airtable({ apiKey }).base(baseId);

const formulas = [
  { label: "name_ac_coruna_accents", f: `OR(FIND("AC Hotel A Coruña", {Property Name} & ""), FIND("AC Hotel A Coruna", {Property Name} & ""), FIND("ac hotel a coruña", LOWER({Canonical Property Name} & "")), FIND("ac hotel a coruna", LOWER({Canonical Property Name} & "")))` },
  { label: "lcgco", f: `OR(FIND("lcgco", LOWER({Property Identity Key} & "")), FIND("lcgco", LOWER({Official Property URL} & "")), FIND("LCGCO", {Property Name} & ""), FIND("LCGCO", {Brand Property Code} & ""), FIND("LCGCO", {Property Code} & ""))` },
  { label: "enrique_marinas", f: `OR(FIND("enrique mariñas", LOWER({Address} & "")), FIND("enrique marinas", LOWER({Address} & "")), FIND("Enrique Mariñas", {Address} & ""))` },
  { label: "postal_15009", f: `AND(OR(FIND("15009", {Address} & ""), FIND("15009", {Postal Code} & "")), OR(FIND("Coru", {City} & ""), FIND("Coruña", {City} & ""), FIND("Coruna", {City} & "")))` },
  { label: "city_ac_marriott", f: `AND(OR(FIND("Coruña", {City} & ""), FIND("Coruna", {City} & ""), FIND("A Coruña", {City} & "")), OR(FIND("AC Hotel", {Property Name} & ""), FIND("AC Hotels", {Current Brand} & ""), FIND("Marriott", {Current Brand} & ""), FIND("Marriott", {Brand Family} & "")))` },
  { label: "spain_ac_coruna_loose", f: `AND(OR(FIND("Spain", {Country} & ""), FIND("España", {Country} & ""), FIND("ES", {Country} & "")), OR(FIND("Coruña", {Property Name} & ""), FIND("Coruna", {Property Name} & ""), FIND("Coruña", {City} & ""), FIND("Coruna", {City} & "")))` },
];

function rowFrom(r, labels) {
  const f = r.fields || {};
  return {
    id: r.id,
    matchLabels: labels,
    name: f["Property Name"] || null,
    canon: f["Canonical Property Name"] || null,
    identityKey: f["Property Identity Key"] || null,
    brandCode: f["Brand Property Code"] || f["Property Code"] || null,
    brand: f["Current Brand"] || null,
    brandFamily: f["Brand Family"] || null,
    address: f["Address"] || null,
    city: f["City"] || null,
    state: f["State / Region"] || null,
    postal: f["Postal Code"] || null,
    country: f["Country"] || null,
    rooms: f["Rooms / Keys"] ?? null,
    lat: f["Latitude"] ?? null,
    lng: f["Longitude"] ?? null,
    url: f["Official Property URL"] || null,
    status: f["Production Use Status"] || null,
    phone: f["Phone"] || null,
  };
}

function scoreHit(h) {
  let score = 0;
  const reasons = [];
  const blob = `${h.name} ${h.canon} ${h.identityKey} ${h.brandCode} ${h.address} ${h.url}`.toLowerCase();
  if (/lcgco/.test(blob)) {
    score += 50;
    reasons.push("marriott_code_lcgco");
  }
  if (/ac hotel a coru[nñ]a|ac hotels?.*coru/.test(blob)) {
    score += 30;
    reasons.push("name_match");
  }
  if (/enrique mari[nñ]as/.test(blob)) {
    score += 25;
    reasons.push("address_street");
  }
  if (/15009/.test(blob)) {
    score += 15;
    reasons.push("postal_15009");
  }
  if (/coru[nñ]a/.test(`${h.city || ""}`.toLowerCase())) {
    score += 10;
    reasons.push("city");
  }
  if (/ac hotel/i.test(h.brand || "") || /ac hotel/i.test(h.name || "")) {
    score += 8;
    reasons.push("ac_brand");
  }
  return { score, reasons };
}

function classify(best) {
  if (!best) return "NO_EXISTING_CANONICAL_MATCH";
  if (best.score >= 70 && best.reasons.includes("marriott_code_lcgco")) {
    return "EXACT_CANONICAL_MATCH";
  }
  if (best.score >= 50) return "LIKELY_DUPLICATE";
  if (best.score >= 25) return "AMBIGUOUS_MATCH";
  return "NO_EXISTING_CANONICAL_MATCH";
}

const byId = new Map();
for (const { label, f } of formulas) {
  try {
    await base(HPC)
      .select({ filterByFormula: f, maxRecords: 50 })
      .eachPage((records, next) => {
        for (const r of records) {
          const prev = byId.get(r.id);
          if (prev) prev.matchLabels.push(label);
          else byId.set(r.id, rowFrom(r, [label]));
        }
        next();
      });
  } catch (err) {
    console.warn(`formula ${label} failed:`, err.message || err);
  }
}

const hits = [...byId.values()].map((h) => {
  const { score, reasons } = scoreHit(h);
  return { ...h, score, reasons };
});
hits.sort((a, b) => b.score - a.score);
const best = hits[0] || null;
const decision = classify(best);

const report = {
  generatedAt: new Date().toISOString(),
  phase: "HPC_PHASE1_DUPLICATE_SEARCH",
  hotel: "AC Hotel A Coruña",
  marriottCode: "LCGCO",
  baseId,
  table: HPC,
  forbiddenBaseBlocked: baseId !== "appvtnDurnMSjINP6",
  formulasRun: formulas.map((x) => x.label),
  hitCount: hits.length,
  hits,
  best,
  classification: decision,
  createAllowed: decision === "NO_EXISTING_CANONICAL_MATCH",
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(
  path.join(OUT_DIR, "PHASE1_HPC_DUPLICATE_SEARCH.json"),
  JSON.stringify(report, null, 2) + "\n"
);

const md = [];
md.push("# AC Hotel A Coruña — HPC Phase 1 Duplicate Search");
md.push("");
md.push(`Classification: **${decision}**`);
md.push(`Hits: ${hits.length}`);
md.push(`Create allowed: ${report.createAllowed}`);
md.push("");
if (hits.length) {
  md.push("| Score | ID | Name | Code | City | Reasons |");
  md.push("|---|---|---|---|---|---|");
  for (const h of hits.slice(0, 20)) {
    md.push(
      `| ${h.score} | ${h.id} | ${h.name} | ${h.brandCode || "—"} | ${h.city || "—"} | ${(h.reasons || []).join(", ")} |`
    );
  }
} else {
  md.push("_No HPC hits._");
}
fs.writeFileSync(path.join(OUT_DIR, "PHASE1_HPC_DUPLICATE_SEARCH.md"), md.join("\n"));

console.log(
  JSON.stringify(
    {
      classification: decision,
      hitCount: hits.length,
      best: best
        ? { id: best.id, name: best.name, score: best.score, reasons: best.reasons }
        : null,
      createAllowed: report.createAllowed,
      out: path.join(OUT_DIR, "PHASE1_HPC_DUPLICATE_SEARCH.json"),
    },
    null,
    2
  )
);
