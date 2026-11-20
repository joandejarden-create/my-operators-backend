#!/usr/bin/env node
/**
 * Factory 1.0 — read-only next-brand selection from live Airtable.
 * Lists Active/Live Brand Basics with presentation row counts, scores pilot suitability,
 * writes reports/brand-explorer/factory-1-0/01-next-brand-selection.json
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_JSON = path.join(ROOT, "reports/brand-explorer/factory-1-0/01-next-brand-selection.json");

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const EXCLUDE_IDS = new Set([
  "receQkxgjlezsc1xg", // Curio Collection by Hilton
  "reclkgOzvAcBheUSo", // Ascend Hotel Collection
]);

const PARENT_LINK_FIELD_CANDIDATES = [
  "Brand",
  "Brand_Basic_ID",
  "Brand Setup - Brand Basics",
  "Brand Basics",
];

const SOFT_COLLECTION_NAME_RE =
  /\b(collection|portfolio|tapestry|autograph|tribute|kimpton|canopy|design hotels|vignette|voco|unbound|lxr|handwritten|motto|hotel indigo|crowne plaza|even hotels|avid hotels|aloft|element|moxy|ac hotels|protea|regent|conrad|waldorf|w hotels|edition|delano|mondrian|so\/|mgallery|pullman|swissotel|mgm collection)\b/i;

const NOTABLE_SOFT_BRANDS = [
  "Tapestry",
  "Autograph",
  "Tribute",
  "Design Hotels",
  "Kimpton",
  "Canopy",
  "Tapestry Collection",
  "Autograph Collection",
  "Tribute Portfolio",
  "Canopy by Hilton",
  "Design Hotels",
  "Kimpton Hotels",
];

const PREFERRED_PARENT_RE =
  /(hilton|marriott|ihg|intercontinental hotels group|hyatt)/i;

const SLEEP_MS = 180;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normName(s) {
  return nz(s)
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[’']/g, "'")
    .trim();
}

async function atFetch(baseId, token, table, { offset, filterByFormula, fields } = {}) {
  const params = new URLSearchParams({ pageSize: "100" });
  if (offset) params.set("offset", offset);
  if (filterByFormula) params.set("filterByFormula", filterByFormula);
  for (const f of fields || []) params.append("fields[]", f);
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}?${params}`;
  let attempt = 0;
  while (attempt < 6) {
    attempt += 1;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json();
    if (res.status === 429) {
      await sleep(SLEEP_MS * attempt * 2);
      continue;
    }
    if (!res.ok) {
      throw new Error(`${table} ${res.status}: ${json?.error?.message || JSON.stringify(json)}`);
    }
    return json;
  }
  throw new Error(`${table}: exhausted retries`);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function listAll(baseId, token, table, opts = {}) {
  const out = [];
  let offset;
  do {
    const json = await atFetch(baseId, token, table, { ...opts, offset });
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(SLEEP_MS);
  } while (offset);
  return out;
}

function linkedBasicsIds(fields, knownBasicsIds) {
  const ids = new Set();
  const ingest = (val) => {
    if (Array.isArray(val)) {
      for (const x of val) {
        if (typeof x === "string" && x.startsWith("rec")) ids.add(x);
      }
    } else if (typeof val === "string" && val.startsWith("rec")) {
      ids.add(val);
    }
  };
  for (const key of PARENT_LINK_FIELD_CANDIDATES) {
    ingest(fields?.[key]);
  }
  for (const [key, val] of Object.entries(fields || {})) {
    if (PARENT_LINK_FIELD_CANDIDATES.includes(key)) continue;
    if (!/brand/i.test(key)) continue;
    ingest(val);
  }
  if (knownBasicsIds) {
    for (const id of [...ids]) {
      if (!knownBasicsIds.has(id)) ids.delete(id);
    }
  }
  return ids;
}

function parentCompanyLabel(fields) {
  const raw = fields?.["Parent Company"];
  if (Array.isArray(raw)) return raw.map((x) => nz(x)).filter(Boolean).join("; ");
  return nz(raw);
}

function scoreCandidate(brand, presentationCount) {
  const reasons = [];
  let score = 0;

  const status = nz(brand.brandStatus);
  if (status === "Live") {
    score += 8;
    reasons.push("Brand Status Live (+8)");
  } else if (status === "Active") {
    score += 6;
    reasons.push("Brand Status Active (+6)");
  }

  const parent = nz(brand.parentCompany);
  if (PREFERRED_PARENT_RE.test(parent)) {
    score += 40;
    reasons.push(`Preferred parent company (${parent}) (+40)`);
  }

  const name = nz(brand.name);
  if (SOFT_COLLECTION_NAME_RE.test(name)) {
    score += 22;
    reasons.push("Soft-collection / lifestyle naming signal (+22)");
  }

  if (presentationCount === 0) {
    score -= 80;
    reasons.push("No presentation rows (-80)");
  } else if (presentationCount < 15) {
    score += 5;
    reasons.push(`Thin presentation (${presentationCount} rows) (+5)`);
  } else if (presentationCount >= 15 && presentationCount <= 140) {
    score += 35;
    reasons.push(`Presentation sweet spot for uplift (${presentationCount} rows) (+35)`);
  } else if (presentationCount > 140 && presentationCount <= 220) {
    score += 18;
    reasons.push(`Substantial presentation; room for Curio/Ascend bar (${presentationCount} rows) (+18)`);
  } else {
    score += 5;
    reasons.push(`Heavy presentation inventory (${presentationCount} rows) (+5)`);
  }

  // Slight preference for Marriott soft collections already in factory adjacent workstreams
  if (/marriott/i.test(parent) && /tribute|autograph|edition|moxy|ac hotels|protea/i.test(name)) {
    score += 12;
    reasons.push("Marriott soft-collection factory peer (+12)");
  }
  if (/hilton/i.test(parent) && /tapestry|canopy|curio|embassy|tempo|livsmart/i.test(name)) {
    score += 10;
    reasons.push("Hilton soft-collection factory peer (+10)");
  }
  if (/ihg|intercontinental/i.test(parent) && /kimpton|vignette|voco|hotel indigo|even hotels/i.test(name)) {
    score += 10;
    reasons.push("IHG soft-collection factory peer (+10)");
  }

  // De-prioritize Choice/Radisson universe for this Hilton/Marriott/IHG pilot lane
  if (/choice hotels|radisson/i.test(parent) && !PREFERRED_PARENT_RE.test(parent)) {
    score -= 25;
    reasons.push("Outside Hilton/Marriott/IHG pilot lane (-25)");
  }

  return { score, reasons };
}

function buildPresentationCounts(basicsById, basicsByNormName, presentationRecords) {
  const byBrandId = Object.fromEntries([...basicsById.keys()].map((id) => [id, 0]));
  const unmatchedNames = new Map();

  for (const rec of presentationRecords) {
    const f = rec.fields || {};
    const linked = linkedBasicsIds(f, basicsById);
    let matched = false;
    for (const id of linked) {
      if (byBrandId[id] != null) {
        byBrandId[id] += 1;
        matched = true;
      }
    }
    if (matched) continue;

    const brandName = nz(f["Brand Name"]);
    if (!brandName) continue;
    const hit = basicsByNormName.get(normName(brandName));
    if (hit?.id && byBrandId[hit.id] != null) {
      byBrandId[hit.id] += 1;
      continue;
    }
    unmatchedNames.set(brandName, (unmatchedNames.get(brandName) || 0) + 1);
  }

  return { byBrandId, unmatchedNames };
}

async function main() {
  const token = nz(process.env.AIRTABLE_API_KEY);
  const baseId = nz(process.env.AIRTABLE_BASE_ID);
  if (!token || !baseId) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID required (load-env.js).");
    process.exit(1);
  }

  const basicsFilter = "OR({Brand Status}='Active',{Brand Status}='Live')";
  const basicsFields = ["Brand Name", "Parent Company", "Brand Status", "Brand Architecture", "Brand Model"];

  const [basicsRecords, presentationRecords] = await Promise.all([
    listAll(baseId, token, BASICS_TABLE, { filterByFormula: basicsFilter, fields: basicsFields }),
    listAll(baseId, token, PRESENTATION_TABLE),
  ]);

  const basicsById = new Map();
  const basicsByNormName = new Map();

  for (const rec of basicsRecords) {
    const name = nz(rec.fields?.["Brand Name"]);
    if (!name) continue;
    const row = {
      id: rec.id,
      name,
      parentCompany: parentCompanyLabel(rec.fields),
      brandStatus: nz(rec.fields?.["Brand Status"]),
      brandArchitecture: nz(rec.fields?.["Brand Architecture"]),
      brandModel: nz(rec.fields?.["Brand Model"]),
    };
    basicsById.set(rec.id, row);
    const n = normName(name);
    if (!basicsByNormName.has(n)) basicsByNormName.set(n, row);
  }

  const { byBrandId, unmatchedNames } = buildPresentationCounts(
    basicsById,
    basicsByNormName,
    presentationRecords
  );

  const candidates = [];
  for (const [id, brand] of basicsById) {
    if (EXCLUDE_IDS.has(id)) continue;
    const presentationCount = byBrandId[id] || 0;
    const { score, reasons } = scoreCandidate(brand, presentationCount);
    candidates.push({
      id,
      name: brand.name,
      parentCompany: brand.parentCompany,
      brandStatus: brand.brandStatus,
      brandArchitecture: brand.brandArchitecture,
      brandModel: brand.brandModel,
      presentationCount,
      factoryPilotScore: score,
      scoreRationale: reasons,
    });
  }

  candidates.sort((a, b) => {
    if (b.factoryPilotScore !== a.factoryPilotScore) return b.factoryPilotScore - a.factoryPilotScore;
    if (b.presentationCount !== a.presentationCount) return b.presentationCount - a.presentationCount;
    return a.name.localeCompare(b.name);
  });

  const withPresentation = candidates.filter((c) => c.presentationCount > 0);
  const ranked = withPresentation.length ? withPresentation : candidates;
  const recommended = ranked[0] || null;
  const top5Alternatives = ranked.slice(1, 6);

  const notableMatches = {};
  for (const label of NOTABLE_SOFT_BRANDS) {
    const re = new RegExp(label.replace(/\//g, "\\/"), "i");
    const hits = candidates
      .filter((c) => re.test(c.name))
      .map((c) => ({
        id: c.id,
        name: c.name,
        parentCompany: c.parentCompany,
        brandStatus: c.brandStatus,
        presentationCount: c.presentationCount,
        factoryPilotScore: c.factoryPilotScore,
      }));
    if (hits.length) notableMatches[label] = hits;
  }

  const report = {
    generatedAt: new Date().toISOString(),
    version: "factory-1-0-next-brand-select-v1",
    universe: {
      basicsTable: BASICS_TABLE,
      presentationTable: PRESENTATION_TABLE,
      activeLiveBasicsCount: basicsRecords.length,
      presentationRowCount: presentationRecords.length,
      excludedBrandIds: [...EXCLUDE_IDS],
    },
    recommended: recommended
      ? {
          id: recommended.id,
          name: recommended.name,
          parentCompany: recommended.parentCompany,
          brandStatus: recommended.brandStatus,
          presentationCount: recommended.presentationCount,
          factoryPilotScore: recommended.factoryPilotScore,
          rationale: recommended.scoreRationale,
        }
      : null,
    top5Alternatives: top5Alternatives.map((c) => ({
      id: c.id,
      name: c.name,
      parentCompany: c.parentCompany,
      brandStatus: c.brandStatus,
      presentationCount: c.presentationCount,
      factoryPilotScore: c.factoryPilotScore,
    })),
    notableSoftCollectionBrands: notableMatches,
    allActiveLiveWithCounts: candidates.map((c) => ({
      id: c.id,
      name: c.name,
      parentCompany: c.parentCompany,
      brandStatus: c.brandStatus,
      presentationCount: c.presentationCount,
      factoryPilotScore: c.factoryPilotScore,
    })),
    presentationNameUnmatchedBasics: Object.fromEntries([...unmatchedNames.entries()].slice(0, 50)),
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2), "utf8");

  console.log(`Wrote ${OUT_JSON}`);
  if (recommended) {
    console.log("\n=== RECOMMENDED NEXT BRAND ===");
    console.log(`Name: ${recommended.name}`);
    console.log(`ID: ${recommended.id}`);
    console.log(`Parent: ${recommended.parentCompany || "(none)"}`);
    console.log(`Presentation rows: ${recommended.presentationCount}`);
    console.log(`Score: ${recommended.factoryPilotScore}`);
    console.log(`Rationale: ${recommended.scoreRationale.join("; ")}`);
  }
  console.log("\n=== TOP 5 ALTERNATIVES ===");
  for (const alt of top5Alternatives) {
    console.log(
      `- ${alt.name} (${alt.id}) | ${alt.parentCompany || "—"} | rows=${alt.presentationCount} | score=${alt.factoryPilotScore}`
    );
  }
  console.log("\n=== NOTABLE SOFT BRANDS (counts) ===");
  for (const [label, hits] of Object.entries(notableMatches)) {
    for (const h of hits) {
      console.log(`  ${label}: ${h.name} (${h.id}) rows=${h.presentationCount} score=${h.factoryPilotScore}`);
    }
  }
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});