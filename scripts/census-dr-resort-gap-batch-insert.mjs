#!/usr/bin/env node
/**
 * Batch coverage-insert DR resort gaps — census re-check before EVERY insert.
 *
 * Uses curated official inventory + alias/rebrand tokens.
 * Coverage allowlist only (no address/phone/rooms/lat/lng).
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DR_OFFICIAL_RESORT_INVENTORIES,
  WEAK_MATCH_TOKENS,
} from "../lib/independent-census/dr-official-resort-inventory-control.js";
import {
  buildCoverageInsertFields,
  COVERAGE_INSERT_ALLOWED_FIELDS,
  COVERAGE_INSERT_NEVER_FIELDS,
} from "../lib/research-engine-v2/census-autopilot-coverage-reconciliation.js";
import { createHotelPropertyCensusRecords } from "../lib/research-engine-v2/census-autopilot-discovery-insert-apply.js";
import { INTAKE_APPLY_CONFIRMS } from "../lib/independent-census/intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "../lib/independent-census/intake-autopilot-apply.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import { buildCanonicalBrandDictionary } from "../lib/research-engine-v2/census-brand-canonical-dictionary.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const COUNTRY = "Dominican Republic";

const BATCH_GROUPS = [
  "Bahia_Principe",
  "Barcelo_Occidental",
  "Hyatt_Inclusive",
  "Hodelpa",
];

const STATE_BY_CITY = {
  "Punta Cana": "La Altagracia",
  Bávaro: "La Altagracia",
  Bavaro: "La Altagracia",
  "Cap Cana": "La Altagracia",
  "Uvero Alto": "La Altagracia",
  Miches: "El Seibo",
  "La Romana": "La Romana",
  Bayahíbe: "La Romana",
  Samaná: "Samaná",
  Samana: "Samaná",
  "Santo Domingo": "Distrito Nacional",
  Santiago: "Santiago",
  "Juan Dolio": "San Pedro de Macorís",
  "Puerto Plata": "Puerto Plata",
};

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function slugify(s) {
  return norm(s).replace(/\s+/g, "_").slice(0, 48);
}

function identityKey(row, group) {
  const prefix =
    {
      Bahia_Principe: "ind_bahia_do",
      Barcelo_Occidental: "ind_barcelo_do",
      Hyatt_Inclusive: "ind_hyatt_do",
      Hodelpa: "ind_hodelpa_do",
    }[group] || "ind_resort_do";
  const tip =
    (row.distinctive_tokens || []).find((t) => norm(t).length >= 5) || row.name;
  return `${prefix}_${slugify(tip)}`;
}

function isPropertyLevelOfficialUrl(url) {
  const u = String(url || "").trim().toLowerCase();
  if (!/^https?:\/\//.test(u)) return false;
  if (/\/destinations\/dominican-republic\/?$/.test(u)) return false;
  if (/\/en\/dominican-republic\/?$/.test(u)) return false;
  if (/our-hotels\.html/.test(u)) return false;
  if (/world of hyatt/.test(u)) return false;
  if (/\/hotels\/america\/dominican-republic\/?$/.test(u)) return false;
  return true;
}

function parseArgs(argv = process.argv.slice(2)) {
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function listDrCensus(baseId, token) {
  const fields = [
    "Property Name",
    "Canonical Property Name",
    "Current Brand",
    "City",
    "Official Property URL",
    "Property Identity Key",
  ];
  const formula = `AND({Country}='${COUNTRY}')`;
  const out = [];
  let offset;
  do {
    const p = new URLSearchParams({ filterByFormula: formula, pageSize: "100" });
    for (const f of fields) p.append("fields[]", f);
    if (offset) p.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${CENSUS_TABLE_ID}?${p}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

function matchRow(row, records) {
  const gapUrl = norm(row.url || "");
  const gapName = norm(row.name);
  const hits = [];
  for (const r of records) {
    const f = r.fields || {};
    const name = norm(f["Property Name"]);
    const canon = norm(f["Canonical Property Name"]);
    const url = norm(f["Official Property URL"]);
    const key = norm(f["Property Identity Key"]);
    const hay = `${name} ${canon} ${url} ${key}`;
    const reasons = [];
    let score = 0;
    if (gapUrl && url && gapUrl === url) {
      reasons.push("exact_url");
      score += 100;
    }
    if (gapName && (name === gapName || canon === gapName)) {
      reasons.push("exact_name");
      score += 90;
    }
    for (const tok of [...(row.distinctive_tokens || []), ...(row.alias_tokens || [])]) {
      const t = norm(tok);
      if (!t || t.length < 4) continue;
      if (WEAK_MATCH_TOKENS.has(t) && !(row.alias_tokens || []).includes(tok)) continue;
      if (hay.includes(t)) {
        reasons.push(`token:${t}`);
        score += (row.alias_tokens || []).map(norm).includes(t) ? 55 : 40;
      }
    }
    if (key && key === norm(identityKey(row, row._group))) {
      reasons.push("exact_identity_key");
      score += 100;
    }
    if (!reasons.length) continue;
    hits.push({
      score,
      reasons,
      id: r.id,
      property_name: f["Property Name"],
      city: f.City,
      key: f["Property Identity Key"],
    });
  }
  hits.sort((a, b) => b.score - a.score);
  const best = hits[0] || null;
  if (best && best.score >= 40) return { decision: "already_in_census", best };
  if (best && best.score >= 20) return { decision: "probable_match", best };
  return { decision: "true_missing", best: null };
}

function extractFields(built) {
  return built?.fields && typeof built.fields === "object" ? built.fields : built;
}

async function main() {
  const args = parseArgs();
  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    console.error(JSON.stringify({ ok: false, blocked: "wrong_write_target" }));
    process.exit(1);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  const brandDictionary = buildCanonicalBrandDictionary({});
  const envCheck = checkIntakeApplyEnv();

  let records = await listDrCensus(baseId, token);
  const existingKeys = new Set(
    records
      .map((r) => norm(r.fields?.["Property Identity Key"]))
      .filter(Boolean)
  );

  const results = [];
  const toInsert = [];

  for (const group of BATCH_GROUPS) {
    for (const row of DR_OFFICIAL_RESORT_INVENTORIES[group] || []) {
      const tagged = { ...row, _group: group };
      const match = matchRow(tagged, records);
      if (match.decision !== "true_missing") {
        results.push({
          group,
          name: row.name,
          decision: match.decision,
          matched: match.best?.property_name || null,
          action: "skip_already_or_probable",
        });
        continue;
      }

      const idKey = identityKey(row, group);
      if (existingKeys.has(norm(idKey))) {
        results.push({
          group,
          name: row.name,
          decision: "identity_key_collision",
          action: "skip",
          identity_key: idKey,
        });
        continue;
      }

      const officialUrl = row.url || null;
      if (!officialUrl || !row.city || !isPropertyLevelOfficialUrl(officialUrl)) {
        results.push({
          group,
          name: row.name,
          decision: "blocked_missing_property_level_url_or_city",
          action: "skip",
          url: officialUrl,
        });
        continue;
      }

      const discovered = {
        property_name: row.name,
        brand: row.brand,
        parent_company: row.parent_company || row.brand,
        source_family: row.source_family || group,
        city: row.city,
        state_region: STATE_BY_CITY[row.city] || undefined,
        country: COUNTRY,
        official_property_url: officialUrl,
        official_directory_url: row.source,
        identity_key: idKey,
        identity_confidence: "High",
        source_confidence: "High",
      };

      const built = buildCoverageInsertFields(discovered, {
        brandDictionary,
        human_review_required: false,
      });
      const fields = extractFields(built) || {};
      for (const k of COVERAGE_INSERT_NEVER_FIELDS) delete fields[k];
      for (const k of Object.keys(fields)) {
        if (!COVERAGE_INSERT_ALLOWED_FIELDS.includes(k)) delete fields[k];
      }

      const validation = { pass: true, failed: [] };
      for (const req of [
        "Property Name",
        "Property Identity Key",
        "Current Brand",
        "Country",
        "City",
        "Official Property URL",
      ]) {
        if (!fields[req]) {
          validation.pass = false;
          validation.failed.push(`missing_${req}`);
        }
      }
      if (norm(fields.City) === "unknown") {
        validation.pass = false;
        validation.failed.push("city_unknown");
      }

      if (!validation.pass) {
        results.push({
          group,
          name: row.name,
          decision: "validation_failed",
          action: "skip",
          validation,
        });
        continue;
      }

      toInsert.push({ group, name: row.name, identity_key: idKey, fields });
      results.push({
        group,
        name: row.name,
        decision: "true_missing_queued",
        action: "insert_candidate",
        identity_key: idKey,
        city: fields.City,
        url: fields["Official Property URL"],
      });
    }
  }

  const doWrite = Boolean(args.apply && args.allConfirmsOk && envCheck.allOk);
  const created = [];
  const skippedLive = [];

  if (doWrite) {
    for (const item of toInsert) {
      // Live re-check immediately before each write
      records = await listDrCensus(baseId, token);
      const liveRow = {
        name: item.name,
        distinctive_tokens: [item.name],
        alias_tokens: [],
        url: item.fields["Official Property URL"],
        _group: item.group,
      };
      // Prefer identity key + name token re-check
      const byKey = records.find(
        (r) => norm(r.fields?.["Property Identity Key"]) === norm(item.identity_key)
      );
      const byName = records.find(
        (r) => norm(r.fields?.["Property Name"]) === norm(item.name)
      );
      if (byKey || byName) {
        skippedLive.push({
          name: item.name,
          reason: "appeared_during_batch_or_already_present",
          matched: (byKey || byName).fields["Property Name"],
        });
        continue;
      }

      const createResult = await createHotelPropertyCensusRecords(baseId, token, [
        { fields: item.fields },
      ]);
      const rows = Array.isArray(createResult)
        ? createResult
        : createResult?.created || [];
      created.push({
        name: item.name,
        identity_key: item.identity_key,
        id: rows[0]?.id || null,
      });
      existingKeys.add(norm(item.identity_key));
    }
  }

  const report = {
    status: doWrite ? "applied" : "dry_run",
    airtable_writes: doWrite,
    hard_rule: "Census re-check before every insert; skip rebrands via alias tokens",
    generated_at: new Date().toISOString(),
    dr_census_before: records.length,
    queued_inserts: toInsert.length,
    created_count: created.length,
    skipped_live_count: skippedLive.length,
    created,
    skipped_live: skippedLive,
    results,
    field_mapping_note:
      "buildCoverageInsertFields → Hotel Property Census coverage allowlist",
  };

  mkdirSync(join(root, "reports"), { recursive: true });
  const out = doWrite
    ? "reports/census-dr-resort-gap-batch-insert-applied.json"
    : "reports/census-dr-resort-gap-batch-insert-dry-run.json";
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));

  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: out,
        airtable_writes: doWrite,
        queued_inserts: toInsert.length,
        created_count: created.length,
        skipped_already: results.filter((r) =>
          ["already_in_census", "probable_match"].includes(r.decision)
        ).length,
        sample_queued: toInsert.slice(0, 15).map((t) => t.name),
        created_names: created.map((c) => c.name),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
