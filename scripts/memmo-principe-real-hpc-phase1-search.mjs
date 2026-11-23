/**
 * Memmo Príncipe Real — HPC Phase 1 duplicate / identity search (read-only).
 * Target: AIRTABLE_BASE_ID_ALT / Hotel Property Census only.
 *
 *   node scripts/memmo-principe-real-hpc-phase1-search.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-design-hotels-operational-pilot-v1/memmo-hpc-enrollment-v1"
);

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
  {
    label: "name_memmo_principe",
    f: `OR(FIND("Memmo Príncipe Real", {Property Name} & ""), FIND("Memmo Principe Real", {Property Name} & ""), FIND("memmo príncipe real", LOWER({Canonical Property Name} & "")), FIND("memmo principe real", LOWER({Canonical Property Name} & "")), FIND("memmo principe", LOWER({Property Name} & "")))`,
  },
  {
    label: "lisdm",
    f: `OR(FIND("lisdm", LOWER({Property Identity Key} & "")), FIND("lisdm", LOWER({Official Property URL} & "")), FIND("LISDM", {Property Name} & ""), FIND("LISDM", {Brand Property Code} & ""), FIND("LISDM", {Property Code} & ""), FIND("lisdm", LOWER({Source URL} & "")))`,
  },
  {
    label: "address_pedro_v",
    f: `OR(FIND("Pedro V", {Address} & ""), FIND("pedro v", LOWER({Address} & "")), FIND("D. Pedro V", {Address} & ""), FIND("Rua D. Pedro", {Address} & ""))`,
  },
  {
    label: "postal_1250",
    f: `AND(OR(FIND("1250-094", {Address} & ""), FIND("1250-094", {Postal Code} & ""), FIND("1250", {Postal Code} & "")), OR(FIND("Lisbon", {City} & ""), FIND("Lisboa", {City} & ""), FIND("Portugal", {Country} & "")))`,
  },
  {
    label: "lisbon_design_hotels_memmo",
    f: `AND(OR(FIND("Lisbon", {City} & ""), FIND("Lisboa", {City} & "")), OR(FIND("Portugal", {Country} & ""), FIND("PT", {Country} & "")), OR(FIND("Memmo", {Property Name} & ""), FIND("Design Hotels", {Current Brand} & ""), FIND("Design Hotels", {Affiliation} & ""), FIND("Design Hotels", {Brand Family} & "")))`,
  },
  {
    label: "portugal_memmo",
    f: `AND(OR(FIND("Portugal", {Country} & ""), FIND("PT", {Country} & "")), OR(FIND("Memmo", {Property Name} & ""), FIND("Memmo", {Canonical Property Name} & "")))`,
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
    brandCode: f["Brand Property Code"] || f["Property Code"] || null,
    brand: f["Current Brand"] || null,
    brandFamily: f["Brand Family"] || null,
    affiliation: f["Affiliation"] || f["Current Brand"] || null,
    address: f["Address"] || null,
    city: f["City"] || null,
    state: f["State / Region"] || null,
    postal: f["Postal Code"] || null,
    country: f["Country"] || null,
    market: f["Market"] || null,
    submarket: f["Submarket"] || null,
    rooms: f["Rooms / Keys"] ?? null,
    lat: f["Latitude"] ?? null,
    lng: f["Longitude"] ?? null,
    url: f["Official Property URL"] || null,
    status: f["Production Use Status"] || null,
    meetingFlag: f["Meeting Space Flag"] ?? null,
  };
}

function scoreHit(h) {
  let score = 0;
  const reasons = [];
  const blob = `${h.name} ${h.canon} ${h.identityKey} ${h.brandCode} ${h.address} ${h.url}`.toLowerCase();
  if (/lisdm/.test(blob)) {
    score += 50;
    reasons.push("marriott_code_lisdm");
  }
  if (/memmo\s+pr[ií]ncipe\s+real|memmo\s+principe\s+real/.test(blob)) {
    score += 35;
    reasons.push("name_match");
  } else if (/memmo/.test(blob)) {
    score += 15;
    reasons.push("memmo_token");
  }
  if (/pedro\s+v|d\.\s*pedro/.test(blob)) {
    score += 20;
    reasons.push("address_pedro_v");
  }
  if (/1250-094|1250/.test(blob)) {
    score += 10;
    reasons.push("postal");
  }
  if (/lisbon|lisboa/.test(`${h.city || ""}`.toLowerCase())) {
    score += 8;
    reasons.push("city_lisbon");
  }
  if (/portugal|pt/.test(`${h.country || ""}`.toLowerCase())) {
    score += 5;
    reasons.push("country_portugal");
  }
  if (/design hotels/i.test(h.brand || "") || /design hotels/i.test(h.affiliation || "")) {
    score += 5;
    reasons.push("design_hotels_brand");
  }
  return { score, reasons };
}

function classify(best) {
  if (!best) return "NO_EXISTING_CANONICAL_MATCH";
  if (best.score >= 70 && best.reasons.includes("marriott_code_lisdm")) {
    return "EXACT_CANONICAL_MATCH";
  }
  if (best.score >= 60 && best.reasons.includes("name_match")) {
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
    formulaErrors.push({ label, message: err.message || String(err), status: err.statusCode || err.status });
    console.warn(`formula ${label} failed:`, err.message || err);
  }
}

const scored = [...byId.values()]
  .map((h) => {
    const { score, reasons } = scoreHit(h);
    return { ...h, score, reasons };
  })
  .sort((a, b) => b.score - a.score);

const best = scored[0] || null;
const decision = classify(best);

const report = {
  version: "memmo-principe-real-hpc-phase1-search-v1",
  generatedAt: new Date().toISOString(),
  mode: "READ_ONLY",
  targetBaseId: baseId,
  table: HPC,
  queryTerms: [
    "Memmo Príncipe Real",
    "Lisbon, Portugal",
    "LISDM",
    "Rua D. Pedro V 56 J",
    "1250-094",
    "Design Hotels",
  ],
  formulaErrors,
  hitCount: scored.length,
  hits: scored,
  best,
  duplicateDecision: decision,
  nextStep:
    decision === "EXACT_CANONICAL_MATCH" || decision === "LIKELY_DUPLICATE"
      ? "REUSE_EXISTING_HPC_ID"
      : decision === "AMBIGUOUS_MATCH"
        ? "STOP_IDENTITY_UNCERTAIN"
        : "STEWARD_ENROLL_IF_AUTHORIZED",
};

fs.mkdirSync(OUT_DIR, { recursive: true });
const outPath = path.join(OUT_DIR, "phase1-search.json");
fs.writeFileSync(outPath, `${JSON.stringify(report, null, 2)}\n`);
console.log(
  JSON.stringify(
    {
      outPath,
      duplicateDecision: decision,
      hitCount: scored.length,
      bestId: best?.id || null,
      bestName: best?.name || null,
      bestScore: best?.score ?? null,
      formulaErrorCount: formulaErrors.length,
      nextStep: report.nextStep,
    },
    null,
    2
  )
);
