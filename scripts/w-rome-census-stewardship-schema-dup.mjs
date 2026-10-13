/**
 * W Rome census stewardship — Part A/B: live HPC schema + duplicate search.
 * Read-only. Dry inventory only.
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const OUT = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "../reports/group-demand-intelligence/w-rome-census-stewardship-v1"
);

const CANDIDATE_BASES = [
  { label: "AIRTABLE_BASE_ID_ALT", baseId: process.env.AIRTABLE_BASE_ID_ALT },
  { label: "AIRTABLE_BASE_ID", baseId: process.env.AIRTABLE_BASE_ID },
  {
    label: "AIRTABLE_INTELLIGENCE_BASE_ID",
    baseId: process.env.AIRTABLE_INTELLIGENCE_BASE_ID || process.env.AIRTABLE_GDI_BASE_ID,
  },
  { label: "explicit_appa2cE7", baseId: "appa2cE7FTRmIbB32" },
  { label: "explicit_appCCUsu", baseId: "appCCUsuGsE1ifoLk" },
];

const TABLE = "Hotel Property Census";
const TABLE_ID = "tbl9aY5ijiuIzzWam";

async function metaTables(baseId) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}/tables`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  if (!res.ok) {
    return { ok: false, status: res.status, error: await res.text() };
  }
  const data = await res.json();
  return { ok: true, tables: data.tables || [] };
}

function slimFields(f) {
  const keep = {};
  for (const [k, v] of Object.entries(f || {})) {
    if (
      /name|brand|city|country|address|room|web|lat|lng|long|affili|parent|market|status|code|marsha|phone|postal|zip|open/i.test(
        k
      )
    ) {
      keep[k] = v;
    }
  }
  return keep;
}

async function searchDuplicates(baseId) {
  const base = new Airtable({ apiKey }).base(baseId);
  const hits = [];
  const formulas = [
    `OR(FIND("W Rome", {Canonical Property Name}&""), FIND("W Rome", {Property Name}&""), FIND("w rome", LOWER({Canonical Property Name}&"")), FIND("ROMWV", {Canonical Property Name}&""), FIND("ROMWV", {Property Name}&""))`,
    `AND(OR(FIND("Rome", {City}&""), FIND("Roma", {City}&""), FIND("rome", LOWER({City}&""))), OR(FIND("W ", {Canonical Property Name}&""), FIND("W Hotel", {Canonical Property Name}&""), FIND("W Hotels", {Current Brand}&""), FIND("W Hotels", {Brand Family}&"")))`,
  ];

  // Broad scan for name/code tokens — formula may fail if fields missing
  try {
    await base(TABLE)
      .select({
        filterByFormula: `OR(
          FIND("ROMWV", LOWER({Canonical Property Name}&"")),
          FIND("romwv", LOWER({Property Name}&"")),
          FIND("w rome", LOWER({Canonical Property Name}&"")),
          FIND("w roma", LOWER({Canonical Property Name}&"")),
          FIND("w rome", LOWER({Property Name}&""))
        )`,
        pageSize: 100,
        maxRecords: 50,
      })
      .eachPage((records, next) => {
        for (const r of records) {
          hits.push({ id: r.id, fields: slimFields(r.fields), via: "formula_name_code" });
        }
        next();
      });
  } catch (e) {
    hits.push({ formulaError: String(e.message || e) });
  }

  // Full-page scan fallback for Rome+W (bounded)
  let scanned = 0;
  const scanHits = [];
  try {
    await base(TABLE)
      .select({ pageSize: 100, fields: undefined })
      .eachPage((records, next) => {
        for (const r of records) {
          scanned += 1;
          const blob = JSON.stringify(r.fields || {}).toLowerCase();
          const isWRome =
            /\bw\s*rome\b|\bw\s*roma\b|romwv/.test(blob) ||
            (/\brome\b|\broma\b/.test(blob) &&
              /\bw hotels\b|\bw\s+hotel\b/.test(blob) &&
              /italy|italia|00187|via liguria/.test(blob));
          if (isWRome) {
            scanHits.push({ id: r.id, fields: slimFields(r.fields), via: "scan" });
          }
        }
        // Stop early after deep scan if we already have many pages — still need Italy coverage
        if (scanned >= 20000) return;
        next();
      });
  } catch (e) {
    scanHits.push({ scanError: String(e.message || e), scanned });
  }

  return { formulaHits: hits, scanHits, scanned };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const schemaReport = [];
  let hpcBase = null;
  let hpcTable = null;

  for (const c of CANDIDATE_BASES) {
    if (!c.baseId) {
      schemaReport.push({ ...c, ok: false, error: "env_missing" });
      continue;
    }
    const meta = await metaTables(c.baseId);
    if (!meta.ok) {
      schemaReport.push({ ...c, ok: false, status: meta.status, error: String(meta.error).slice(0, 200) });
      continue;
    }
    const table =
      meta.tables.find((t) => t.id === TABLE_ID) ||
      meta.tables.find((t) => t.name === TABLE);
    schemaReport.push({
      ...c,
      ok: true,
      tableCount: meta.tables.length,
      hasHpcById: Boolean(meta.tables.find((t) => t.id === TABLE_ID)),
      hasHpcByName: Boolean(meta.tables.find((t) => t.name === TABLE)),
      hpcTableId: table?.id || null,
      hpcTableName: table?.name || null,
      fieldCount: table?.fields?.length || 0,
      fields: (table?.fields || []).map((f) => ({
        id: f.id,
        name: f.name,
        type: f.type,
        options: f.options?.choices
          ? f.options.choices.map((x) => x.name).slice(0, 30)
          : undefined,
      })),
    });
    if (table && !hpcBase) {
      hpcBase = c;
      hpcTable = table;
    }
  }

  fs.writeFileSync(
    path.join(OUT, "PART_A_HPC_SCHEMA.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), schemaReport, selected: hpcBase }, null, 2)
  );

  let dup = null;
  if (hpcBase?.baseId) {
    dup = await searchDuplicates(hpcBase.baseId);
    fs.writeFileSync(path.join(OUT, "PART_B_DUPLICATE_SEARCH.json"), JSON.stringify(dup, null, 2));
  }

  const decision =
    (dup?.scanHits || []).filter((h) => h.id).length > 0 ||
    (dup?.formulaHits || []).filter((h) => h.id).length > 0
      ? "LIKELY_EXISTING_MATCH_REQUIRES_REVIEW"
      : "NO_EXISTING_CANONICAL_MATCH";

  console.log(
    JSON.stringify(
      {
        selectedBase: hpcBase?.label,
        selectedBaseId: hpcBase?.baseId,
        hpcTableId: hpcTable?.id,
        fieldCount: hpcTable?.fields?.length,
        sampleFieldNames: (hpcTable?.fields || []).slice(0, 40).map((f) => f.name),
        formulaHitCount: (dup?.formulaHits || []).filter((h) => h.id).length,
        scanHitCount: (dup?.scanHits || []).filter((h) => h.id).length,
        scanned: dup?.scanned,
        decision,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
