/**
 * Static write-path registry for Airtable canonicalization V1.
 * Avoids shell ripgrep escaping issues on Windows.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "airtable-canonicalization-v1");

const writePaths = [
  {
    logicalObject: "Hotel ADP Attributes",
    writer: "lib/hotel-intelligence/adp-attributes/airtable-store.js#syncHotelAdpAttributesToAirtable",
    callers: [
      "lib/hotel-intelligence/research/research-hotel-intelligence.js",
      "scripts/sync-hotel-adp-attributes.mjs",
      "scripts/apply-hotel-intelligence-ac-spice-reconciliation.mjs",
      "scripts/apply-hotel-intelligence-bethesda.mjs",
    ],
    status: "CURRENT_WRITE",
    upsertSafe: true,
    note: "IDEMPOTENT_UPSERT by Dedupe Key; deactivates stale actives",
  },
  {
    logicalObject: "HI Commercial/Event/Demand/Evidence/Seasonality",
    writer: "lib/hotel-intelligence/schema/hi-airtable-store.js#applyHotelIntelligencePacket",
    callers: [
      "lib/hotel-intelligence/research/research-hotel-intelligence.js",
      "scripts/apply-hotel-intelligence-ac-spice-reconciliation.mjs",
      "scripts/apply-hotel-intelligence-bethesda.mjs",
    ],
    status: "CURRENT_WRITE",
    upsertSafe: true,
    note: "IDEMPOTENT_UPSERT by profileKey/spaceKey/nodeKey/evidenceId",
  },
  {
    logicalObject: "Hotel Demand Generator Fit",
    writer: "lib/group-demand-intelligence/demand-generators/airtable-stores.js#upsertHotelGeneratorFit",
    callers: ["lib/group-demand-intelligence/research-coverage/onboard-hotel-research-graph.js"],
    status: "CURRENT_WRITE",
    upsertSafe: true,
    note: "IDEMPOTENT_UPSERT by Fit ID",
  },
  {
    logicalObject: "Demand Generators / Programs / Signals",
    writer: "lib/group-demand-intelligence/demand-generators/airtable-stores.js",
    callers: ["onboard-hotel-research-graph.js", "weekly/coverage engines"],
    status: "CURRENT_WRITE",
    upsertSafe: true,
  },
  {
    logicalObject: "GDI Research Targets",
    writer: "lib/group-demand-intelligence/research-coverage/airtable-stores.js#upsertResearchTarget",
    callers: ["onboard-hotel-research-graph.js", "coverage engine"],
    status: "CURRENT_WRITE",
    upsertSafe: true,
  },
  {
    logicalObject: "GDI Research Runs / Target Runs",
    writer: "lib/group-demand-intelligence/research-coverage/airtable-stores.js#upsertResearchRun|upsertTargetRun",
    callers: [
      "coverage engine",
      "scripts/persist-gdi-research-runs-ac-spice.mjs",
      "scripts/gdi-ac-hotel-a-coruna-market-cycle-2.mjs",
    ],
    status: "CURRENT_WRITE",
    upsertSafe: true,
  },
  {
    logicalObject: "Group Demand Opportunities (founder label: GDI Opportunities)",
    writer: "lib/group-demand-intelligence/airtable-opportunity-store.js via opportunity-persistence.js",
    callers: ["promote-qualified-opportunity.js", "gdi first-cycle scripts"],
    status: "CURRENT_WRITE",
    upsertSafe: true,
  },
  {
    logicalObject: "HPC Hotel Property Census",
    writer: "census stewardship scripts → AIRTABLE_BASE_ID_ALT",
    callers: ["hotel census onboarding / stewardship"],
    status: "CURRENT_WRITE",
    upsertSafe: true,
    note: "Intentionally targets appCCUsuGsE1ifoLk — not intelligence base",
  },
];

const report = {
  generatedAt: new Date().toISOString(),
  namingNote: {
    founderLabel: "GDI Opportunities",
    airtableTableName: "Group Demand Opportunities",
    tableId: "tblRuReslJMwsfRQj",
    classification: "DIFFERENT_LABEL_SAME_TABLE",
  },
  attributeTableDupes: {
    countOnPlatform: 1,
    canonical: { name: "Hotel ADP Attributes", id: "tblMA6v0HAmsY9ImW" },
  },
  writePaths,
  doubleWriteRisk: "NONE_ACTIVE — multiple callers share one idempotent upsert per object; no parallel attribute tables",
  wrongBase: {
    hiGdiAdpGuard: "assertNotLegacyMvpCanonicalBase on GDI/HI/decision/opportunity stores",
    forbiddenBase: "appvtnDurnMSjINP6",
    productMainStillMvp: "AIRTABLE_BASE_ID may still be MVP for non-intelligence product CRM — HI/GDI/ADP must not fall back to it (fail-closed)",
    wrongBaseWritesFoundThisAudit: 0,
    legacyBaseWritersStillActiveForHiGdiAdp: 0,
  },
  conceptsThatLookDuplicativeButAreNot: [
    {
      a: "Hotel Demand Nodes (platform HI)",
      b: "Hotel Demand Generator Fit (platform GDI)",
      why: "Nodes = market geography/anchors for hotel context; Fits = hotel↔organization commercial fit scoring",
    },
    {
      a: "Hotel Demand Nodes (platform)",
      b: "Demand Anchors / Demand Centers (HPC base)",
      why: "Census geography demand ontology vs hotel-intelligence demand nodes — different bases and purposes",
    },
    {
      a: "Hotel ADP Attributes",
      b: "HI Commercial Profile fields",
      why: "Attributes = derived ADP consumption audit trail; Commercial Profile = hotel fact store",
    },
    {
      a: "GDI Opportunities (docs)",
      b: "Group Demand Opportunities (Airtable)",
      why: "Same table tblRuReslJMwsfRQj — naming alias only",
    },
  ],
};

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(path.join(OUT_DIR, "WRITE_PATHS.json"), JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify({ ok: true, writePaths: writePaths.length, out: path.join(OUT_DIR, "WRITE_PATHS.json") }, null, 2));
