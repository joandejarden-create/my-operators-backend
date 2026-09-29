/**
 * Spice Island Beach Resort — HPC Phase 1 duplicate / identity search (read-only).
 * Target: AIRTABLE_BASE_ID_ALT / Hotel Property Census only.
 *
 *   node scripts/spice-island-beach-resort-hpc-phase1-search.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(
  __dirname,
  "../reports/hotel-census/spice-island-beach-resort-onboarding-v1"
);

const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
if (!apiKey || !baseId) {
  console.error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT");
  process.exit(1);
}
if (baseId === "appvtnDurnMSjINP6") {
  console.error("FORBIDDEN: legacy base blocked");
  process.exit(1);
}

const HPC = "Hotel Property Census";
const base = new Airtable({ apiKey }).base(baseId);

const formulas = [
  {
    label: "name_spice_island",
    f: `OR(FIND("Spice Island Beach Resort", {Property Name} & ""), FIND("Spice Island Beach", {Property Name} & ""), FIND("spice island", LOWER({Canonical Property Name} & "")), FIND("spice island", LOWER({Property Name} & "")))`,
  },
  {
    label: "domain_spiceislandbeachresort",
    f: `OR(FIND("spiceislandbeachresort.com", LOWER({Official Property URL} & "")), FIND("spiceislandbeachresort.com", LOWER({Source URL} & "")))`,
  },
  {
    label: "grand_anse_spice",
    f: `AND(OR(FIND("Grand Anse", {Address} & ""), FIND("grand anse", LOWER({Address} & "")), FIND("Grand Anse", {City} & "")), OR(FIND("Spice", {Property Name} & ""), FIND("Grenada", {Country} & ""), FIND("Grenada", {City} & "")))`,
  },
  {
    label: "grenada_spice",
    f: `AND(OR(FIND("Grenada", {Country} & ""), FIND("Grenada", {City} & ""), FIND("Grenada", {State / Region} & "")), OR(FIND("Spice", {Property Name} & ""), FIND("spice", LOWER({Canonical Property Name} & "")))`,
  },
  {
    label: "grenada_beach_resort_loose",
    f: `AND(OR(FIND("Grenada", {Country} & ""), FIND("Grenada", {City} & "")), OR(FIND("Beach Resort", {Property Name} & ""), FIND("Grand Anse", {Address} & ""), FIND("Grand Anse", {Property Name} & "")))`,
  },
  {
    label: "st_georges_grenada",
    f: `AND(OR(FIND("Grenada", {Country} & ""), FIND("Grenada", {City} & "")), OR(FIND("St. George", {City} & ""), FIND("St George", {City} & ""), FIND("Saint George", {City} & ""), FIND("St. George's", {City} & "")))`,
  },
];

function rowFrom(r, labels) {
  const f = r.fields || {};
  return {
    id: r.id,
    matchLabels: labels,
    name: f["Property Name"] || null,
    canon: f["Canonical Property Name"] || null,
    identityKey: f["Property Identity Key"] || null,
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
    phone: f["Phone"] || null,
    status: f["Production Use Status"] || null,
    owner: f["Owner Name"] || null,
    operator: f["Operator / Management Company"] || null,
  };
}

function scoreHit(h) {
  let score = 0;
  const reasons = [];
  const blob = `${h.name} ${h.canon} ${h.identityKey} ${h.address} ${h.url} ${h.city} ${h.country}`.toLowerCase();
  if (/spiceislandbeachresort\.com/.test(blob)) {
    score += 50;
    reasons.push("official_domain");
  }
  if (/spice island beach resort/.test(blob)) {
    score += 40;
    reasons.push("exact_name");
  } else if (/spice island/.test(blob)) {
    score += 25;
    reasons.push("name_spice_island");
  }
  if (/grand anse/.test(blob)) {
    score += 15;
    reasons.push("grand_anse");
  }
  if (/grenada/.test(blob)) {
    score += 15;
    reasons.push("grenada");
  }
  if (h.rooms === 64 || h.rooms === "64") {
    score += 10;
    reasons.push("rooms_64");
  }
  return { score, reasons };
}

function classify(best) {
  if (!best) return "NO_EXISTING_CANONICAL_MATCH";
  if (best.score >= 70 && (best.reasons.includes("official_domain") || best.reasons.includes("exact_name"))) {
    return "EXACT_CANONICAL_MATCH";
  }
  if (best.score >= 50) return "LIKELY_DUPLICATE";
  if (best.score >= 25) return "AMBIGUOUS_MATCH";
  return "NO_EXISTING_CANONICAL_MATCH";
}

const byId = new Map();
const formulaErrors = [];
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
    formulaErrors.push({ label, error: err.message || String(err) });
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
  hotel: "Spice Island Beach Resort",
  baseId,
  table: HPC,
  forbiddenBaseBlocked: baseId !== "appvtnDurnMSjINP6",
  formulasRun: formulas.map((x) => x.label),
  formulaErrors,
  hitCount: hits.length,
  hits,
  best,
  classification: decision,
  createAllowed: decision === "NO_EXISTING_CANONICAL_MATCH",
  bindExisting: decision === "EXACT_CANONICAL_MATCH" || decision === "LIKELY_DUPLICATE",
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(
  path.join(OUT_DIR, "PHASE1_HPC_DUPLICATE_SEARCH.json"),
  JSON.stringify(report, null, 2) + "\n"
);
fs.writeFileSync(
  path.join(OUT_DIR, "PHASE1_HPC_DUPLICATE_SEARCH.md"),
  [
    "# Spice Island Beach Resort — HPC Phase 1",
    "",
    `Classification: **${decision}**`,
    `Hits: ${hits.length}`,
    `Create allowed: ${report.createAllowed}`,
    `Bind existing: ${report.bindExisting}`,
    "",
    hits.length
      ? [
          "| Score | ID | Name | Rooms | City | Country | Reasons |",
          "|---|---|---|---|---|---|---|",
          ...hits.slice(0, 20).map(
            (h) =>
              `| ${h.score} | ${h.id} | ${h.name} | ${h.rooms ?? "—"} | ${h.city || "—"} | ${h.country || "—"} | ${(h.reasons || []).join(", ")} |`
          ),
        ].join("\n")
      : "_No HPC hits._",
    "",
  ].join("\n")
);

console.log(
  JSON.stringify(
    {
      classification: decision,
      hitCount: hits.length,
      best: best
        ? {
            id: best.id,
            name: best.name,
            rooms: best.rooms,
            score: best.score,
            reasons: best.reasons,
            url: best.url,
          }
        : null,
      createAllowed: report.createAllowed,
      bindExisting: report.bindExisting,
      formulaErrors,
    },
    null,
    2
  )
);
