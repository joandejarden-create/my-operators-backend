/**
 * Airtable Canonicalization + Duplicate Audit V1
 *
 * Read-only by default. Optional --deactivate-duplicate-attrs after report.
 *
 *   node scripts/audit-airtable-canonicalization-v1.mjs
 *   node scripts/audit-airtable-canonicalization-v1.mjs --deactivate-duplicate-attrs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
  getGdiOpportunitiesAirtableBaseId,
  isLegacyMvpBase,
} from "../lib/decision-outcomes/airtable-base.js";
import { MAP_HOTEL_ADP_ATTRIBUTE as ATTR } from "../lib/hotel-intelligence/adp-attributes/field-map.js";
import { MAP_HOTEL_GENERATOR_FIT as FIT } from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";
import { MAP_RESEARCH_TARGET as TGT } from "../lib/group-demand-intelligence/research-coverage/airtable-field-map.js";
import { MAP_RESEARCH_RUN as RUN } from "../lib/group-demand-intelligence/research-coverage/airtable-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "airtable-canonicalization-v1");
const DEACTIVATE = process.argv.includes("--deactivate-duplicate-attrs");

const BASES = {
  platform: process.env.AIRTABLE_INTELLIGENCE_BASE_ID || process.env.ADP_AIRTABLE_BASE_ID || CANONICAL_INTELLIGENCE_BASE_ID,
  hpc: process.env.AIRTABLE_BASE_ID_ALT || "appCCUsuGsE1ifoLk",
  legacyMvp: process.env.AIRTABLE_BASE_ID || LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
};

const EXPECTED_HI = {
  "Hotel Commercial Profiles": "tblBYJtKj6oi4J0Ow",
  "Hotel Event Spaces": "tblDwW2jpbKBBtpko",
  "Hotel Demand Nodes": "tbl5xEOk7Hnr5Iebq",
  "Hotel Seasonality & Need Periods": "tbllCSH31bxsXM3C0",
  "Hotel Intelligence Evidence": "tblHxS8x0niDEGVsh",
  "Hotel ADP Attributes": "tblMA6v0HAmsY9ImW",
};

const EXPECTED_GDI = {
  "GDI Opportunities": "tblRuReslJMwsfRQj",
  "Demand Generators": "tblykUVOjGVkawvdD",
  "Demand Programs": "tblxm7wqfFNG37Z2m",
  "Hotel Demand Generator Fit": "tblqamXbIk9n6gXfV",
  "Demand Generator Signals": "tblZ0cOM78uYMCTXx",
  "Private Event Venues": "tblE5p4HjVHfcMDna",
  "Hotel Venue Fit": "tbluXqX7ecocCrdE1",
  "Private Event Signals": "tbl5rSZxKv218GuOP",
  "Decisions": "tblulPWvEmd3iiDhJ",
  "Decision Events": "tblLL1CuKpvI43DtB",
  "GDI Research Targets": "tblVyuEf5vjooWDKX",
  "GDI Research Runs": "tblzNMUIo2T6onaKH",
  "GDI Research Target Runs": "tblSA0cFVplNfWMjp",
};

const HOTELS = [
  { label: "Bethesda Marriott", hpcId: "recLuxvwwxID7U2B8" },
  { label: "AC Hotel A Coruña", hpcId: "rec2PVBDavppGpenm" },
  { label: "Spice Island Beach Resort", hpcId: "recKRJjcPnb4tVDDS" },
];

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) throw new Error("missing_airtable_token");
assertNotLegacyMvpCanonicalBase(BASES.platform, { surface: "canonicalization-audit" });

const INTEREST_RE =
  /attribute|attributes|hotel|intelligence|adp|gdi|demand|generator|fit|profile|property|opportunity|research/i;

async function metaTables(baseId) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}/tables`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  if (!res.ok) {
    const body = await res.text();
    return { ok: false, baseId, status: res.status, error: body.slice(0, 500), tables: [] };
  }
  const json = await res.json();
  return {
    ok: true,
    baseId,
    tables: (json.tables || []).map((t) => ({
      id: t.id,
      name: t.name,
      primaryFieldId: t.primaryFieldId,
      fieldCount: (t.fields || []).length,
      fields: (t.fields || []).map((f) => ({ id: f.id, name: f.name, type: f.type })),
    })),
  };
}

async function countRecords(baseId, tableNameOrId) {
  const base = new Airtable({ apiKey: token }).base(baseId);
  let count = 0;
  try {
    await base(tableNameOrId)
      .select({ pageSize: 100, fields: [] })
      .eachPage((recs, next) => {
        count += recs.length;
        next();
      });
    return { ok: true, count };
  } catch (e) {
    return { ok: false, count: null, error: String(e.message || e) };
  }
}

async function listAll(baseId, tableName, fields = null) {
  const base = new Airtable({ apiKey: token }).base(baseId);
  const rows = [];
  const opts = { pageSize: 100 };
  if (fields) opts.fields = fields;
  await base(tableName)
    .select(opts)
    .eachPage((recs, next) => {
      for (const r of recs) rows.push({ id: r.id, createdTime: r._rawJson?.createdTime, fields: r.fields || {} });
      next();
    });
  return rows;
}

function esc(s) {
  return String(s || "").replace(/"/g, '\\"');
}

async function listByHpc(baseId, tableName, hpcField, hpcId, fields = null) {
  const base = new Airtable({ apiKey: token }).base(baseId);
  const rows = [];
  const opts = {
    pageSize: 100,
    filterByFormula: `{${hpcField}} = "${esc(hpcId)}"`,
  };
  if (fields) opts.fields = fields;
  await base(tableName)
    .select(opts)
    .eachPage((recs, next) => {
      for (const r of recs) rows.push({ id: r.id, createdTime: r._rawJson?.createdTime, fields: r.fields || {} });
      next();
    });
  return rows;
}

// --- Phase 1: table inventory ---
console.error("[audit] meta inventory…");
const meta = {
  platform: await metaTables(BASES.platform),
  hpc: await metaTables(BASES.hpc),
  legacyMvp: await metaTables(BASES.legacyMvp),
};

const keywordHits = [];
for (const [role, pack] of Object.entries(meta)) {
  for (const t of pack.tables || []) {
    if (INTEREST_RE.test(t.name)) {
      keywordHits.push({ role, baseId: pack.baseId, id: t.id, name: t.name, fieldCount: t.fieldCount });
    }
  }
}

// Count only interesting / expected tables on platform (avoid counting entire base)
const expectedNames = new Set([...Object.keys(EXPECTED_HI), ...Object.keys(EXPECTED_GDI)]);
const attributeLike = (meta.platform.tables || []).filter((t) =>
  /attribute/i.test(t.name)
);

const tableInventory = [];
for (const t of keywordHits.filter((h) => h.role === "platform" || expectedNames.has(h.name))) {
  // dedupe later
}
const seenInv = new Set();
for (const hit of keywordHits) {
  const key = `${hit.baseId}:${hit.id}`;
  if (seenInv.has(key)) continue;
  seenInv.add(key);
  let count = null;
  let countErr = null;
  // Only count expected + attribute-like + fits/targets/opps on platform; skip huge unrelated
  const shouldCount =
    hit.baseId === BASES.platform &&
    (expectedNames.has(hit.name) || /attribute|adp|gdi|demand generator|research target|research run|hotel commercial|hotel event|hotel demand|hotel intelligence|hotel seasonality|hotel venue|private event|decision/i.test(hit.name));
  if (shouldCount) {
    const c = await countRecords(hit.baseId, hit.name);
    count = c.count;
    countErr = c.error || null;
  }
  const expectedId = EXPECTED_HI[hit.name] || EXPECTED_GDI[hit.name] || null;
  tableInventory.push({
    name: hit.name,
    id: hit.id,
    baseId: hit.baseId,
    baseRole: hit.role,
    recordCount: count,
    countError: countErr,
    expectedId,
    idMatchesExpected: expectedId ? expectedId === hit.id : null,
  });
}

// --- Phase 2: attribute table duplication ---
const attributeTables = (meta.platform.tables || [])
  .filter((t) => /attribute/i.test(t.name))
  .map((t) => {
    const expected = t.name === "Hotel ADP Attributes" && t.id === "tblMA6v0HAmsY9ImW";
    let classification = "UNKNOWN";
    if (t.id === "tblMA6v0HAmsY9ImW" && t.name === "Hotel ADP Attributes") classification = "CANONICAL";
    else if (/attribute/i.test(t.name) && t.id !== "tblMA6v0HAmsY9ImW") classification = "DIFFERENT_CONCEPT_OR_LEGACY";
    return {
      name: t.name,
      id: t.id,
      fieldNames: t.fields.map((f) => f.name),
      classification,
      expectedCanonical: expected,
    };
  });

// Also check legacy MVP for attribute-like tables
const legacyAttrTables = (meta.legacyMvp.tables || [])
  .filter((t) => /attribute|adp|gdi|hotel intelligence|demand generator/i.test(t.name))
  .map((t) => ({
    name: t.name,
    id: t.id,
    baseId: BASES.legacyMvp,
    classification: "WRONG_BASE_OR_LEGACY_MVP",
  }));

// --- Phase 3: ADP attribute record audit ---
console.error("[audit] ADP attribute records…");
const attrAudit = {};
const duplicateActiveToDeactivate = [];

for (const hotel of HOTELS) {
  const rows = await listByHpc(
    BASES.platform,
    "Hotel ADP Attributes",
    ATTR.hpcHotelId,
    hotel.hpcId,
    [
      ATTR.attributeKey,
      ATTR.dedupeKey,
      ATTR.hpcHotelId,
      ATTR.attributeCategory,
      ATTR.attributeName,
      ATTR.attributeVersion,
      ATTR.active,
      ATTR.usedInAdp,
      ATTR.lastVerifiedAt,
    ]
  );
  const active = rows.filter((r) => r.fields[ATTR.active] === true);
  const inactive = rows.filter((r) => r.fields[ATTR.active] !== true);

  // Exact dupes by dedupe key among ALL rows
  const byDedupe = new Map();
  for (const r of rows) {
    const k = r.fields[ATTR.dedupeKey] || r.fields[ATTR.attributeKey] || "";
    if (!byDedupe.has(k)) byDedupe.set(k, []);
    byDedupe.get(k).push(r);
  }
  const exactDupGroups = [...byDedupe.entries()].filter(([k, arr]) => k && arr.length > 1);

  // Multiple ACTIVE same key
  const byActiveKey = new Map();
  for (const r of active) {
    const k = r.fields[ATTR.dedupeKey] || r.fields[ATTR.attributeKey] || "";
    if (!byActiveKey.has(k)) byActiveKey.set(k, []);
    byActiveKey.get(k).push(r);
  }
  const multiActive = [...byActiveKey.entries()].filter(([k, arr]) => k && arr.length > 1);

  // Semantic: same category+name active, different version/key
  const byCatName = new Map();
  for (const r of active) {
    const k = `${r.fields[ATTR.attributeCategory]}::${r.fields[ATTR.attributeName]}`;
    if (!byCatName.has(k)) byCatName.set(k, []);
    byCatName.get(k).push(r);
  }
  const semanticDupes = [...byCatName.entries()].filter(([, arr]) => arr.length > 1);

  // For multi-active same key: keep newest createdTime, deactivate others
  for (const [key, arr] of multiActive) {
    const sorted = [...arr].sort((a, b) => String(b.createdTime || "").localeCompare(String(a.createdTime || "")));
    const keep = sorted[0];
    for (const dup of sorted.slice(1)) {
      duplicateActiveToDeactivate.push({
        hotel: hotel.label,
        hpcId: hotel.hpcId,
        dedupeKey: key,
        keepId: keep.id,
        deactivateId: dup.id,
      });
    }
  }

  attrAudit[hotel.label] = {
    hpcId: hotel.hpcId,
    total: rows.length,
    active: active.length,
    inactive: inactive.length,
    exactDuplicateGroups: exactDupGroups.length,
    exactDuplicateExtraRows: exactDupGroups.reduce((n, [, a]) => n + a.length - 1, 0),
    multipleActiveSameKey: multiActive.length,
    multipleActiveExtraRows: multiActive.reduce((n, [, a]) => n + a.length - 1, 0),
    semanticActiveDupes: semanticDupes.length,
    legacyV1Keys: active.filter((r) => /v1$|_v1::|adp_attr_v0/i.test(String(r.fields[ATTR.attributeVersion] || r.fields[ATTR.dedupeKey] || ""))).length,
    clean: multiActive.length === 0 && semanticDupes.length === 0,
    sampleMultiActive: multiActive.slice(0, 5).map(([k, arr]) => ({ key: k, ids: arr.map((r) => r.id) })),
    sampleSemantic: semanticDupes.slice(0, 5).map(([k, arr]) => ({ key: k, ids: arr.map((r) => r.id) })),
  };
}

// --- Phase 4: GDI fit/target duplication ---
console.error("[audit] GDI fits/targets…");
const gdiAudit = {};
for (const hotel of HOTELS) {
  const fits = await listByHpc(BASES.platform, "Hotel Demand Generator Fit", FIT.hotelId, hotel.hpcId, [
    FIT.fitId,
    FIT.hotelId,
    FIT.demandGeneratorId,
    FIT.organizationName,
    FIT.schemaVersion,
  ]);
  const targets = await listByHpc(BASES.platform, "GDI Research Targets", TGT.hotelId, hotel.hpcId, [
    TGT.targetId,
    TGT.hotelId,
    TGT.canonicalName,
    TGT.entityId,
    TGT.targetType,
  ]);
  let runs = [];
  try {
    runs = await listByHpc(BASES.platform, "GDI Research Runs", RUN.hotelId, hotel.hpcId, [RUN.runId, RUN.hotelId, RUN.status]);
  } catch (e) {
    runs = [];
  }

  function uniqBy(rows, keyFn) {
    const m = new Map();
    for (const r of rows) {
      const k = keyFn(r);
      if (!m.has(k)) m.set(k, []);
      m.get(k).push(r);
    }
    return m;
  }

  const fitById = uniqBy(fits, (r) => r.fields[FIT.fitId] || r.id);
  const fitByHotelGen = uniqBy(
    fits,
    (r) => `${r.fields[FIT.hotelId]}::${r.fields[FIT.demandGeneratorId] || r.fields[FIT.organizationName] || r.id}`
  );
  const tgtById = uniqBy(targets, (r) => r.fields[TGT.targetId] || r.id);
  const runById = uniqBy(runs, (r) => r.fields[RUN.runId] || r.id);

  gdiAudit[hotel.label] = {
    hpcId: hotel.hpcId,
    fitsReported: fits.length,
    uniqueFitIds: fitById.size,
    duplicateFitIdGroups: [...fitById.values()].filter((a) => a.length > 1).length,
    uniqueHotelGeneratorKeys: fitByHotelGen.size,
    duplicateHotelGeneratorGroups: [...fitByHotelGen.values()].filter((a) => a.length > 1).length,
    targetsReported: targets.length,
    uniqueTargetIds: tgtById.size,
    duplicateTargetIdGroups: [...tgtById.values()].filter((a) => a.length > 1).length,
    runs: runs.length,
    uniqueRunIds: runById.size,
    fitIds: fits.map((r) => r.id),
    targetIds: targets.map((r) => r.id),
    runIds: runs.map((r) => r.id),
    clean:
      [...fitById.values()].every((a) => a.length === 1) &&
      [...fitByHotelGen.values()].every((a) => a.length === 1) &&
      [...tgtById.values()].every((a) => a.length === 1),
  };
}

// GDI Opportunities count by hotel (may fail on field name)
async function countOpps(hotelId) {
  const fieldCandidates = ["Hotel ID", "HPC Hotel ID", "Canonical Hotel ID", "Dealality Hotel ID"];
  for (const f of fieldCandidates) {
    try {
      const rows = await listByHpc(BASES.platform, "GDI Opportunities", f, hotelId);
      return { field: f, count: rows.length, ids: rows.map((r) => r.id) };
    } catch {
      /* try next */
    }
  }
  return { field: null, count: 0, ids: [], note: "no_hotel_field_or_empty" };
}

const oppAudit = {};
for (const hotel of HOTELS) {
  oppAudit[hotel.label] = await countOpps(hotel.hpcId);
}

// --- Optional deactivate ---
let remediation = {
  tablesDeleted: 0,
  rowsDeleted: 0,
  rowsDeactivated: 0,
  deactivated: [],
  dryRunOnly: !DEACTIVATE,
};
if (DEACTIVATE && duplicateActiveToDeactivate.length) {
  const base = new Airtable({ apiKey: token }).base(BASES.platform);
  const table = base("Hotel ADP Attributes");
  for (const d of duplicateActiveToDeactivate) {
    await table.update(d.deactivateId, {
      [ATTR.active]: false,
      [ATTR.notes]: `Deactivated by airtable-canonicalization-v1; duplicate active of ${d.keepId} key=${d.dedupeKey}`,
    });
    remediation.rowsDeactivated += 1;
    remediation.deactivated.push(d);
  }
} else {
  remediation.pendingDeactivations = duplicateActiveToDeactivate;
}

const report = {
  generatedAt: new Date().toISOString(),
  bases: {
    platform: BASES.platform,
    hpc: BASES.hpc,
    legacyMvp: BASES.legacyMvp,
    platformIsCanonical: BASES.platform === CANONICAL_INTELLIGENCE_BASE_ID,
    legacyIsForbidden: isLegacyMvpBase(BASES.legacyMvp),
  },
  metaOk: {
    platform: meta.platform.ok,
    hpc: meta.hpc.ok,
    legacyMvp: meta.legacyMvp.ok,
    legacyError: meta.legacyMvp.error || null,
  },
  tableCounts: {
    platform: (meta.platform.tables || []).length,
    hpc: (meta.hpc.tables || []).length,
    legacyMvp: (meta.legacyMvp.tables || []).length,
  },
  allPlatformTables: (meta.platform.tables || []).map((t) => ({ id: t.id, name: t.name })),
  allHpcTables: (meta.hpc.tables || []).map((t) => ({ id: t.id, name: t.name })),
  legacyInteresting: legacyAttrTables,
  keywordHits,
  tableInventory,
  attributeTables,
  expectedHiIdCheck: Object.entries(EXPECTED_HI).map(([name, id]) => {
    const found = (meta.platform.tables || []).find((t) => t.name === name);
    return { name, expectedId: id, foundId: found?.id || null, match: found?.id === id };
  }),
  expectedGdiIdCheck: Object.entries(EXPECTED_GDI).map(([name, id]) => {
    const found = (meta.platform.tables || []).find((t) => t.name === name);
    return { name, expectedId: id, foundId: found?.id || null, match: found?.id === id };
  }),
  attrAudit,
  gdiAudit,
  oppAudit,
  remediation,
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(path.join(OUT_DIR, "AUDIT.json"), JSON.stringify(report, null, 2) + "\n");
// Also dump full meta for schema inspection
fs.writeFileSync(
  path.join(OUT_DIR, "META_TABLES.json"),
  JSON.stringify(
    {
      platform: meta.platform.tables?.map((t) => ({ id: t.id, name: t.name, fields: t.fields })),
      hpc: meta.hpc.tables?.map((t) => ({ id: t.id, name: t.name, fields: t.fields?.map((f) => f.name) })),
      legacyMvp: meta.legacyMvp.ok
        ? meta.legacyMvp.tables?.map((t) => ({ id: t.id, name: t.name }))
        : { error: meta.legacyMvp.error },
    },
    null,
    2
  ) + "\n"
);

console.log(
  JSON.stringify(
    {
      outDir: OUT_DIR,
      bases: report.bases,
      tableCounts: report.tableCounts,
      attributeTables: report.attributeTables.map((t) => ({
        name: t.name,
        id: t.id,
        classification: t.classification,
      })),
      legacyInteresting: report.legacyInteresting,
      hiIdMatch: report.expectedHiIdCheck.every((x) => x.match),
      gdiIdMatch: report.expectedGdiIdCheck.every((x) => x.match),
      hiMismatches: report.expectedHiIdCheck.filter((x) => !x.match),
      gdiMismatches: report.expectedGdiIdCheck.filter((x) => !x.match),
      attrAudit: report.attrAudit,
      gdiAudit: Object.fromEntries(
        Object.entries(report.gdiAudit).map(([k, v]) => [
          k,
          {
            fits: v.fitsReported,
            uniqueFits: v.uniqueFitIds,
            uniqueHotelGen: v.uniqueHotelGeneratorKeys,
            targets: v.targetsReported,
            uniqueTargets: v.uniqueTargetIds,
            runs: v.runs,
            clean: v.clean,
          },
        ])
      ),
      pendingDeactivations: duplicateActiveToDeactivate.length,
      remediation,
    },
    null,
    2
  )
);
