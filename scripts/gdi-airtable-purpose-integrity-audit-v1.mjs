#!/usr/bin/env node
/**
 * Read-only GDI Airtable Purpose, Data Integrity & Full E2E Persistence Audit V1.
 * No schema changes, no record mutations, no paid research, no publication changes.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_AIRTABLE_BASE,
  GDI_AIRTABLE_TABLES,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-airtable-purpose-integrity-audit-v1");
const TOKEN = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
const BASE = GDI_AIRTABLE_BASE.baseId;

const EXTRA_TABLES = Object.freeze({
  DECISIONS: {
    name: "Decisions",
    tableId: "tblulPWvEmd3iiDhJ",
    identityField: "decisionId",
  },
  DECISION_EVENTS: {
    name: "Decision Events",
    tableId: "tblLL1CuKpvI43DtB",
    identityField: "eventId",
  },
  HOTEL_COMMERCIAL_PROFILES_FACILITY: {
    name: "Hotel Commercial Profiles",
    tableId: "tblBYJtKj6oi4J0Ow",
    identityField: "Profile Key",
  },
  HOTEL_EVENT_SPACES: {
    name: "Hotel Event Spaces",
    tableId: "tblDwW2jpbKBBtpko",
    identityField: "Space Key",
  },
  HOTEL_DEMAND_NODES: {
    name: "Hotel Demand Nodes",
    tableId: "tbl5xEOk7Hnr5Iebq",
    identityField: "Node Key",
  },
  HOTEL_SEASONALITY_NEED_PERIODS: {
    name: "Hotel Seasonality & Need Periods",
    tableId: "tbllCSH31bxsXM3C0",
    identityField: "Period Key",
  },
  HOTEL_INTELLIGENCE_EVIDENCE: {
    name: "Hotel Intelligence Evidence",
    tableId: "tblHxS8x0niDEGVsh",
    identityField: "Evidence ID",
  },
  GDI_NEED_PERIOD_CHANGE_HISTORY: {
    name: "GDI Need Period Change History",
    tableId: "tblMTSB0bkzFO2zzh",
    identityField: "Change ID",
  },
  GDI_COMMERCIAL_PROFILE_CHANGE_HISTORY: {
    name: "GDI Commercial Profile Change History",
    tableId: "tblrFnRPOoUz6oNCn",
    identityField: "Change ID",
  },
});

const HOTELS = Object.freeze([
  {
    name: "Bethesda Marriott",
    hotelId: "recLuxvwwxID7U2B8",
    runId: "gdi_e2e_14seg_mwladic1_ff9898",
  },
  { name: "Sheraton Guadalajara Expo", hotelId: "recCAGqkERSXgiku4" },
  { name: "Wyndham San Diego Bayside", hotelId: "recnPB3Q02NO8Ma1o" },
  { name: "Castillo Hotel Son Vida", hotelId: "rec82D9zpB8fede1I" },
  { name: "YOTEL Geneva Lake", hotelId: "recrPQcZg7SFARRb2" },
  { name: "Radisson Blu Toronto Downtown", hotelId: "recH9h3McgMNFQEla" },
  { name: "Waterstone Resort & Marina", hotelId: "recgMYovrrZDJMqzX" },
]);

const SYNTHETIC_FIELD_HINTS = [
  "Scenario",
  "scenario",
  "Source",
  "source",
  "Provenance",
  "provenance",
  "Status",
  "status",
  "Notes",
  "notes",
  "Profile Notes",
  "Need Period Notes",
  "Research Status",
  "Verification Status",
  "Data Quality",
  "Seed Type",
  "Record Type",
];

const SYNTHETIC_PATTERNS =
  /SYNTHETIC|TEST_SCENARIO|FIXTURE|FOUNDER_TEST|ONBOARD(ING)?_SEED|GENERIC_SEED|WATERSTONE.?TEST|PROVISIONAL|MARKET_ESTIMATED|DEMO_|EXAMPLE\.INVALID/i;

function ensureOut() {
  fs.mkdirSync(OUT, { recursive: true });
}

function allTables() {
  return [
    ...Object.values(GDI_AIRTABLE_TABLES),
    ...Object.values(EXTRA_TABLES),
  ];
}

async function airtableFetch(urlPath, query = {}) {
  if (!TOKEN) throw new Error("AIRTABLE_TOKEN_MISSING");
  const u = new URL(`https://api.airtable.com/v0/${BASE}/${urlPath}`);
  for (const [k, v] of Object.entries(query)) {
    if (v == null || v === "") continue;
    u.searchParams.set(k, String(v));
  }
  const res = await fetch(u, {
    headers: { Authorization: `Bearer ${TOKEN}` },
  });
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text.slice(0, 500) };
  }
  if (!res.ok) {
    const err = new Error(
      `Airtable ${res.status} ${urlPath}: ${json?.error?.message || text.slice(0, 200)}`,
    );
    err.status = res.status;
    err.body = json;
    throw err;
  }
  return json;
}

async function listAllRecords(tableId, { fields, filterByFormula, max = 5000 } = {}) {
  const records = [];
  let offset;
  do {
    const q = {
      pageSize: 100,
      offset,
    };
    if (filterByFormula) q.filterByFormula = filterByFormula;
    if (fields?.length) {
      // Airtable expects repeated fields[] — URLSearchParams handles one; use manual
    }
    const u = new URL(`https://api.airtable.com/v0/${BASE}/${tableId}`);
    u.searchParams.set("pageSize", "100");
    if (offset) u.searchParams.set("offset", offset);
    if (filterByFormula) u.searchParams.set("filterByFormula", filterByFormula);
    if (fields?.length) {
      for (const f of fields) u.searchParams.append("fields[]", f);
    }
    const res = await fetch(u, {
      headers: { Authorization: `Bearer ${TOKEN}` },
    });
    const json = await res.json();
    if (!res.ok) {
      throw new Error(
        `list ${tableId} ${res.status}: ${json?.error?.message || JSON.stringify(json).slice(0, 200)}`,
      );
    }
    records.push(...(json.records || []));
    offset = json.offset;
    if (records.length >= max) break;
  } while (offset);
  return records.slice(0, max);
}

async function metaTables() {
  const res = await fetch(
    `https://api.airtable.com/v0/meta/bases/${BASE}/tables`,
    { headers: { Authorization: `Bearer ${TOKEN}` } },
  );
  const json = await res.json();
  if (!res.ok) {
    throw new Error(`meta ${res.status}: ${json?.error?.message || "fail"}`);
  }
  return json.tables || [];
}

function loadFsOpportunityCount(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) return { present: false, count: 0 };
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const arr = Array.isArray(j) ? j : j.opportunities || [];
  return { present: true, count: arr.length, path: p };
}

function publicationPointer(hotelId) {
  const dir = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages",
    hotelId,
  );
  const out = { staged: null, current: null };
  for (const name of ["STAGED.json", "CURRENT.json"]) {
    const p = path.join(dir, name);
    if (!fs.existsSync(p)) continue;
    try {
      const j = JSON.parse(fs.readFileSync(p, "utf8"));
      out[name === "STAGED.json" ? "staged" : "current"] = {
        version: j.version || j.packageVersion || j.publicationVersion || null,
        opportunityCount:
          j.opportunityCount ??
          j.opportunities?.length ??
          j.inventory?.opportunities?.length ??
          null,
      };
    } catch {
      out[name === "STAGED.json" ? "staged" : "current"] = { parseError: true };
    }
  }
  return out;
}

function scanSynthetic(records) {
  const hits = [];
  for (const rec of records) {
    const fields = rec.fields || {};
    const blob = JSON.stringify(fields);
    if (!SYNTHETIC_PATTERNS.test(blob)) continue;
    const matchedFields = {};
    for (const [k, v] of Object.entries(fields)) {
      const s = typeof v === "string" ? v : JSON.stringify(v);
      if (SYNTHETIC_PATTERNS.test(s)) matchedFields[k] = String(s).slice(0, 180);
    }
    hits.push({
      recordId: rec.id,
      matchedFields,
      createdTime: rec.createdTime || null,
    });
  }
  return hits;
}

function identityDupes(records, identityField) {
  const map = new Map();
  for (const rec of records) {
    const id = rec.fields?.[identityField];
    if (id == null || id === "") continue;
    const key = String(id);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(rec.id);
  }
  const dups = [];
  for (const [key, ids] of map) {
    if (ids.length > 1) dups.push({ identity: key, recordIds: ids });
  }
  return dups;
}

function brokenLinks(records, linkFieldNames, knownIdsByTable) {
  // knownIdsByTable unused for now — report empty/orphan link counts only
  const stats = {};
  for (const name of linkFieldNames) {
    let nonempty = 0;
    let empty = 0;
    for (const rec of records) {
      const v = rec.fields?.[name];
      if (Array.isArray(v) && v.length) nonempty += 1;
      else empty += 1;
    }
    stats[name] = { nonempty, empty };
  }
  return stats;
}

async function countByHotelFormula(tableId, hotelFieldCandidates, hotelId) {
  for (const field of hotelFieldCandidates) {
    try {
      const formula = `{${field}}='${hotelId}'`;
      const records = await listAllRecords(tableId, {
        filterByFormula: formula,
        max: 5000,
        fields: [field],
      });
      return { field, count: records.length, ok: true };
    } catch {
      // try next field name
    }
  }
  return { field: null, count: null, ok: false };
}

async function main() {
  ensureOut();
  const startedAt = new Date().toISOString();
  const evidence = {
    version: "gdi-airtable-purpose-integrity-audit-v1",
    baseId: BASE,
    baseName: GDI_AIRTABLE_BASE.name,
    startedAt,
    mutation: false,
    paidResearch: false,
    schemaChanges: false,
    publicationChanges: false,
    tokenPresent: Boolean(TOKEN),
  };

  if (!TOKEN) {
    evidence.error = "AIRTABLE_TOKEN_MISSING";
    fs.writeFileSync(
      path.join(OUT, "EVIDENCE_REGISTER.json"),
      JSON.stringify(evidence, null, 2),
    );
    console.error("AIRTABLE_TOKEN_MISSING");
    process.exit(2);
  }

  const meta = await metaTables();
  const metaById = new Map(meta.map((t) => [t.id, t]));
  const metaByName = new Map(meta.map((t) => [t.name, t]));

  const tableInventory = [];
  for (const t of allTables()) {
    const metaT = metaById.get(t.tableId) || metaByName.get(t.name);
    let records = [];
    let listError = null;
    try {
      records = await listAllRecords(t.tableId, { max: 5000 });
    } catch (e) {
      listError = String(e.message || e);
    }
    const identityField = t.identityField;
    const dups = identityField ? identityDupes(records, identityField) : [];
    const syntheticHits = scanSynthetic(records);
    const linkFields = (metaT?.fields || [])
      .filter((f) => f.type === "multipleRecordLinks")
      .map((f) => f.name);
    const linkStats = brokenLinks(records, linkFields, null);
    const lastCreated = records
      .map((r) => r.createdTime)
      .filter(Boolean)
      .sort()
      .slice(-1)[0] || null;

    tableInventory.push({
      name: t.name,
      tableId: t.tableId,
      identityField: identityField || null,
      adapter: t.adapter || null,
      existsInMeta: Boolean(metaT),
      fieldCount: metaT?.fields?.length || null,
      recordCount: listError ? null : records.length,
      listError,
      duplicateIdentityCount: dups.length,
      duplicateIdentitiesSample: dups.slice(0, 10),
      syntheticOrProvisionalHitCount: syntheticHits.length,
      syntheticSample: syntheticHits.slice(0, 15).map((h) => ({
        recordId: h.recordId,
        matchedFields: h.matchedFields,
      })),
      linkFieldStats: linkStats,
      lastCreatedTimeObserved: lastCreated,
      primaryFieldName: metaT?.fields?.[0]?.name || null,
    });
  }

  // Multi-hotel GDO + research target + fit counts
  const hotelFieldCandidates = [
    "hotelId",
    "Hotel ID",
    "HPC Hotel ID",
    "Hotel Record ID",
  ];
  const multiHotel = [];
  for (const h of HOTELS) {
    const fsOps = loadFsOpportunityCount(h.hotelId);
    const pubs = publicationPointer(h.hotelId);
    const gdo = await countByHotelFormula(
      GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.tableId,
      hotelFieldCandidates,
      h.hotelId,
    );
    const targets = await countByHotelFormula(
      GDI_AIRTABLE_TABLES.GDI_RESEARCH_TARGETS.tableId,
      hotelFieldCandidates,
      h.hotelId,
    );
    const fit = await countByHotelFormula(
      GDI_AIRTABLE_TABLES.HOTEL_DEMAND_GENERATOR_FIT.tableId,
      hotelFieldCandidates,
      h.hotelId,
    );
    const needPeriods = await countByHotelFormula(
      GDI_AIRTABLE_TABLES.GDI_HOTEL_NEED_PERIODS.tableId,
      hotelFieldCandidates,
      h.hotelId,
    );
    multiHotel.push({
      ...h,
      fsOpportunityCount: fsOps.count,
      fsOpportunitiesPresent: fsOps.present,
      publication: pubs,
      airtable: {
        groupDemandOpportunities: gdo,
        researchTargets: targets,
        hotelDemandGeneratorFit: fit,
        gdiHotelNeedPeriods: needPeriods,
      },
    });
  }

  // Deep sample: GDI Hotel Commercial Profiles + Need Periods + DG + Fit
  const deepSamples = {};
  for (const key of [
    "GDI_HOTEL_COMMERCIAL_PROFILES",
    "GDI_HOTEL_NEED_PERIODS",
    "DEMAND_GENERATORS",
    "HOTEL_DEMAND_GENERATOR_FIT",
    "DEMAND_PROGRAMS",
    "DEMAND_GENERATOR_SIGNALS",
  ]) {
    const t = GDI_AIRTABLE_TABLES[key];
    const records = await listAllRecords(t.tableId, { max: 500 });
    deepSamples[t.name] = {
      count: records.length,
      synthetic: scanSynthetic(records),
      identityDupes: identityDupes(records, t.identityField).slice(0, 20),
      sampleRecords: records.slice(0, 8).map((r) => {
        const f = r.fields || {};
        const pick = {};
        for (const name of [
          t.identityField,
          ...SYNTHETIC_FIELD_HINTS,
          "hotelId",
          "Hotel ID",
          "HPC Hotel ID",
          "Organization Name",
          "Generator Name",
          "Name",
          "Status",
          "Source",
          "Scenario Type",
          "Confirmation Status",
          "Hotel Confirmed",
        ]) {
          if (f[name] != null) {
            const v = f[name];
            pick[name] =
              typeof v === "string"
                ? v.slice(0, 240)
                : JSON.stringify(v).slice(0, 240);
          }
        }
        return { recordId: r.id, createdTime: r.createdTime, fields: pick };
      }),
    };
  }

  // Bethesda opportunity identity overlap vs FS
  const bethesda = HOTELS[0];
  const gdoAll = await listAllRecords(
    GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.tableId,
    {
      filterByFormula: `{hotelId}='${bethesda.hotelId}'`,
      max: 5000,
      fields: ["opportunityId", "hotelId"],
    },
  );
  const fsPath = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    bethesda.hotelId,
    "opportunities.json",
  );
  let fsIds = new Set();
  if (fs.existsSync(fsPath)) {
    const j = JSON.parse(fs.readFileSync(fsPath, "utf8"));
    const arr = Array.isArray(j) ? j : j.opportunities || [];
    fsIds = new Set(arr.map((o) => o.id || o.opportunityId).filter(Boolean));
  }
  const atIds = new Set(
    gdoAll.map((r) => r.fields?.opportunityId).filter(Boolean),
  );
  const onlyAt = [...atIds].filter((id) => !fsIds.has(id));
  const onlyFs = [...fsIds].filter((id) => !atIds.has(id));
  const both = [...fsIds].filter((id) => atIds.has(id));

  // Re-load dry-run plan for Bethesda if available
  let bethesdaDryRun = null;
  try {
    const prior = JSON.parse(
      fs.readFileSync(
        path.join(
          ROOT,
          "reports/gdi-full-e2e-airtable-persistence-reconciliation-v1/BETHESDA_RECONCILIATION.json",
        ),
        "utf8",
      ),
    );
    bethesdaDryRun = prior.dryRunPlanForProcessed20;
  } catch {
    bethesdaDryRun = null;
  }

  evidence.completedAt = new Date().toISOString();
  evidence.metaTableCount = meta.length;
  evidence.auditedTableCount = tableInventory.length;
  evidence.tableInventory = tableInventory;
  evidence.multiHotelReconciliation = multiHotel;
  evidence.deepSamples = deepSamples;
  evidence.bethesdaIdentityOverlap = {
    hotelId: bethesda.hotelId,
    airtableGdoCount: gdoAll.length,
    fsOpportunityCount: fsIds.size,
    inBoth: both.length,
    onlyAirtableSample: onlyAt.slice(0, 25),
    onlyFsSample: onlyFs.slice(0, 25),
    onlyAirtableCount: onlyAt.length,
    onlyFsCount: onlyFs.length,
  };
  evidence.bethesdaPriorDryRunPlan = bethesdaDryRun;
  evidence.priorAuditReference =
    "reports/gdi-airtable-complete-data-model-audit-v1/";
  evidence.pr356Reference =
    "reports/gdi-full-e2e-airtable-persistence-reconciliation-v1/";

  fs.writeFileSync(
    path.join(OUT, "EVIDENCE_REGISTER.json"),
    JSON.stringify(evidence, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT, "_live_table_inventory.json"),
    JSON.stringify(tableInventory, null, 2),
  );
  fs.writeFileSync(
    path.join(OUT, "_live_multi_hotel.json"),
    JSON.stringify(multiHotel, null, 2),
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        out: OUT,
        tables: tableInventory.map((t) => ({
          name: t.name,
          count: t.recordCount,
          dups: t.duplicateIdentityCount,
          synthetic: t.syntheticOrProvisionalHitCount,
        })),
        hotels: multiHotel.map((h) => ({
          name: h.name,
          fs: h.fsOpportunityCount,
          gdo: h.airtable.groupDemandOpportunities.count,
        })),
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
