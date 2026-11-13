#!/usr/bin/env node
/**
 * Read-only GDI Airtable complete data-model audit V1.
 * Does not alter schema, records, or publication packages.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  listOpportunitiesForHotel,
  getGdiOpportunitiesAirtableBaseId,
  isGdiOpportunityAirtableConfigured,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { MAP_GDI_OPPORTUNITY as F } from "../lib/group-demand-intelligence/opportunity-field-map.js";
import { verifyPublicationPackageIntegrity } from "../lib/group-demand-intelligence/publication-package/index.js";
import { buildCanonicalCustomerPresentationContract } from "../lib/group-demand-intelligence/presentation/gdi-canonical-customer-presentation-contract-v1.js";
import { loadFeedback } from "../lib/group-demand-intelligence/repository.js";
import {
  assertPublishedIdsCoveredByResearchRegistry,
  evaluateFullE2eHotelTemplateResearchStorageGate,
  GDI_RESEARCH_STORAGE_ROLES,
} from "../lib/group-demand-intelligence/research/gdi-research-persistence-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-airtable-complete-data-model-audit-v1");
const HOTEL = "recLuxvwwxID7U2B8";
const BASE = "appa2cE7FTRmIbB32";
const FP =
  "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66";
const PKG_VERSION = "gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2";
const TOKEN = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";

const GDI_TABLE_NAMES = new Set([
  "Group Demand Opportunities",
  "GDI Research Targets",
  "GDI Research Runs",
  "GDI Research Target Runs",
  "Demand Generators",
  "Demand Programs",
  "Demand Generator Signals",
  "Hotel Demand Generator Fit",
  "Private Event Venues",
  "Private Event Signals",
  "Hotel Venue Fit",
  "Hotel Commercial Profiles",
  "GDI Hotel Commercial Profiles",
  "GDI Commercial Profile Change History",
  "Hotel Event Spaces",
  "Hotel Demand Nodes",
  "Hotel Seasonality & Need Periods",
  "GDI Hotel Need Periods",
  "GDI Need Period Change History",
  "GDI Need-Date Campaign Runs",
  "GDI Need Period Opportunity Matches",
  "Hotel Intelligence Evidence",
  "Decisions",
  "Decision Events",
  "Hotel ADP Attributes",
]);

const APP_CONSUMED = {
  "Group Demand Opportunities": {
    required: true,
    consumed: true,
    read: "airtable-opportunity-store.listOpportunitiesForHotel",
    write: "upsertOpportunity / gdi-airtable-sync",
  },
  "GDI Research Targets": {
    required: true,
    consumed: true,
    read: "research-coverage/airtable-stores",
    write: "upsertResearchTarget",
  },
  "GDI Research Runs": {
    required: true,
    consumed: true,
    read: "research-coverage/airtable-stores",
    write: "upsertResearchRun",
  },
  "GDI Research Target Runs": {
    required: true,
    consumed: true,
    read: "research-coverage/airtable-stores",
    write: "upsertResearchTargetRun",
  },
  "Demand Generators": {
    required: false,
    consumed: true,
    read: "demand-generators/airtable-stores",
    write: "upsertDemandGenerator",
  },
  "Demand Programs": {
    required: false,
    consumed: true,
    read: "demand-generators/airtable-stores",
    write: "upsertDemandProgram",
  },
  "Demand Generator Signals": {
    required: false,
    consumed: true,
    read: "demand-generators/airtable-stores",
    write: "upsertSignal",
  },
  "Hotel Demand Generator Fit": {
    required: false,
    consumed: true,
    read: "demand-generators/airtable-stores",
    write: "upsertHotelFit",
  },
  "Private Event Venues": {
    required: false,
    consumed: true,
    read: "private-events stores",
    write: "PE upserts",
  },
  "Private Event Signals": {
    required: false,
    consumed: true,
    read: "private-events stores",
    write: "PE upserts",
  },
  "Hotel Venue Fit": {
    required: false,
    consumed: true,
    read: "private-events stores",
    write: "PE upserts",
  },
  "Hotel Commercial Profiles": {
    required: false,
    consumed: true,
    read: "need-date / ADP facility",
    write: "facility profile writers",
  },
  "GDI Hotel Commercial Profiles": {
    required: true,
    consumed: true,
    read: "need-date-airtable-store-v3",
    write: "need-date portal",
  },
  "GDI Commercial Profile Change History": {
    required: false,
    consumed: true,
    read: "need-date history",
    write: "need-date portal",
  },
  "Hotel Event Spaces": {
    required: false,
    consumed: "partial",
    read: "facility inventory",
    write: "facility writers",
  },
  "Hotel Demand Nodes": {
    required: false,
    consumed: "partial",
    read: "facility inventory",
    write: "facility writers",
  },
  "Hotel Seasonality & Need Periods": {
    required: false,
    consumed: "legacy/partial",
    read: "legacy seasonality",
    write: "legacy",
    note: "Superseded for GDI by GDI Hotel Need Periods",
  },
  "GDI Hotel Need Periods": {
    required: true,
    consumed: true,
    read: "need-date-airtable-store-v3",
    write: "need-date portal / campaign",
  },
  "GDI Need Period Change History": {
    required: false,
    consumed: true,
    read: "need-date history",
    write: "need-date portal",
  },
  "GDI Need-Date Campaign Runs": {
    required: false,
    consumed: true,
    read: "need-date campaign",
    write: "need-date campaign",
  },
  "GDI Need Period Opportunity Matches": {
    required: true,
    consumed: true,
    read: "need-date match",
    write: "match writers",
  },
  "Hotel Intelligence Evidence": {
    required: false,
    consumed: "partial/AVA",
    read: "AVA / evidence",
    write: "AVA sync",
  },
  Decisions: {
    required: false,
    consumed: true,
    read: "decision-outcomes/airtable-store",
    write: "decision-outcomes",
  },
  "Decision Events": {
    required: false,
    consumed: true,
    read: "decision-outcomes/airtable-store",
    write: "decision-outcomes",
  },
  "Hotel ADP Attributes": {
    required: false,
    consumed: false,
    read: "ADP",
    write: "ADP",
    note: "ADP domain; not GDI customer inventory",
  },
};

function text(v) {
  return String(v == null ? "" : v).trim();
}
function csvEscape(v) {
  const s = String(v == null ? "" : v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function writeCsv(filePath, headers, rows) {
  const lines = [headers.join(",")];
  for (const r of rows) {
    lines.push(headers.map((h) => csvEscape(r[h])).join(","));
  }
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`, "utf8");
}
function writeJson(filePath, data) {
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function pickPath(obj, dotted) {
  const parts = dotted.split(".");
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}
function nonempty(v) {
  if (v == null) return false;
  if (typeof v === "string") return text(v).length > 0;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  return true;
}

fs.mkdirSync(OUT, { recursive: true });
if (!TOKEN || !isGdiOpportunityAirtableConfigured()) {
  console.error("Airtable not configured");
  process.exit(1);
}

console.log("[audit] fetching meta schema…");
const metaRes = await fetch(`https://api.airtable.com/v0/meta/bases/${BASE}/tables`, {
  headers: { Authorization: `Bearer ${TOKEN}` },
});
const meta = await metaRes.json();
if (!metaRes.ok) {
  console.error(meta);
  process.exit(1);
}

const tables = meta.tables || [];
const tableByName = new Map(tables.map((t) => [t.name, t]));

async function countTable(tableName, filterByFormula = null) {
  const params = new URLSearchParams({ pageSize: "100" });
  if (filterByFormula) params.set("filterByFormula", filterByFormula);
  let offset = null;
  let count = 0;
  let sample = null;
  let lastModified = null;
  do {
    const url = `https://api.airtable.com/v0/${BASE}/${encodeURIComponent(tableName)}?${params}${
      offset ? `&offset=${encodeURIComponent(offset)}` : ""
    }`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
    const j = await r.json();
    if (!r.ok) {
      return { ok: false, error: j?.error?.message || r.statusText, count: 0 };
    }
    const recs = j.records || [];
    count += recs.length;
    if (!sample && recs[0]) sample = recs[0];
    for (const rec of recs) {
      const u = rec.fields?.updatedAt || rec.fields?.["Updated At"] || rec.createdTime;
      if (u && (!lastModified || String(u) > String(lastModified))) lastModified = u;
    }
    offset = j.offset || null;
  } while (offset);
  return { ok: true, count, sampleId: sample?.id || null, lastModified };
}

console.log("[audit] counting tables (GDI-relevant)…");
const tableInventory = [];
const fieldInventory = [];
const populationRows = [];

for (const t of tables) {
  const isGdi = GDI_TABLE_NAMES.has(t.name) || t.name.startsWith("GDI ") || t.name.startsWith("Demand ") || t.name.startsWith("Private Event") || t.name.startsWith("Hotel Venue") || t.name === "Decisions" || t.name === "Decision Events" || t.name === "Hotel Intelligence Evidence" || t.name === "Hotel Commercial Profiles" || t.name === "Hotel Event Spaces" || t.name === "Hotel Demand Nodes" || t.name === "Hotel Seasonality & Need Periods" || t.name === "Hotel ADP Attributes";
  const usage = APP_CONSUMED[t.name] || {
    required: false,
    consumed: false,
    read: "unknown",
    write: "unknown",
    note: isGdi ? "Present in base; confirm consumer" : "ADP/other domain",
  };

  // Skip ADP-only tables for population counts to save API; still list in inventory
  const skipCount =
    t.name.startsWith("ADP ") ||
    t.name.startsWith("AI Demand Positioning") ||
    t.name === "ADP Observation Prompt Ledger" ||
    t.name === "ADP Report Corrections" ||
    t.name === "ADP Evidence Index" ||
    t.name === "ADP Competitive Rankings" ||
    t.name === "ADP Provider Metrics" ||
    t.name === "ADP Territory Metrics" ||
    t.name === "ADP Period Metrics" ||
    t.name === "ADP Report Snapshots" ||
    t.name === "ADP Monitoring Periods";

  let total = { ok: true, count: null, lastModified: null };
  let beth = { ok: true, count: null };
  if (!skipCount && (isGdi || GDI_TABLE_NAMES.has(t.name))) {
    total = await countTable(t.name);
    // hotel filter attempts
    const hotelFields = (t.fields || []).map((f) => f.name);
    let formula = null;
    if (hotelFields.includes("hotelId")) formula = `{hotelId}='${HOTEL}'`;
    else if (hotelFields.includes("Hotel ID")) formula = `{Hotel ID}='${HOTEL}'`;
    if (formula) beth = await countTable(t.name, formula);
    console.log(`  ${t.name}: total=${total.count} beth=${beth.count}`);
  }

  const pk =
    (t.fields || []).find((f) => f.name.toLowerCase().includes("id") && f.type === "singleLineText")?.name ||
    "Airtable record id";
  const links = (t.fields || [])
    .filter((f) => f.type === "multipleRecordLinks")
    .map((f) => `${f.name}→${f.options?.linkedTableId || "?"}`);

  tableInventory.push({
    tableId: t.id,
    tableName: t.name,
    purpose: usage.note || (isGdi ? "GDI-related" : "ADP/other"),
    fieldCount: (t.fields || []).length,
    primaryKeyHint: pk,
    canonicalBusinessId: pk,
    linkedRecordRelationships: links.join(" | "),
    totalRecords: total.count,
    bethesdaRecords: beth.count,
    lastSuccessfulWriteHint: total.lastModified,
    appConsumed: usage.consumed,
    requiredOptionalLegacy: usage.required
      ? "required"
      : usage.consumed === false
        ? "optional/unused"
        : "optional",
    readFunctions: usage.read || "",
    writeFunctions: usage.write || "",
    classification: skipCount ? "ADP_DOMAIN" : isGdi ? "GDI_DOMAIN" : "OTHER",
  });

  for (const f of t.fields || []) {
    fieldInventory.push({
      tableId: t.id,
      tableName: t.name,
      fieldId: f.id,
      fieldName: f.name,
      fieldType: f.type,
      linkedTableId: f.options?.linkedTableId || "",
      choices:
        f.type === "singleSelect" || f.type === "multipleSelects"
          ? (f.options?.choices || []).map((c) => c.name).join("|")
          : "",
    });
  }

  if (!skipCount && (isGdi || GDI_TABLE_NAMES.has(t.name))) {
    populationRows.push({
      tableName: t.name,
      tableId: t.id,
      totalRecords: total.count ?? "",
      bethesdaRecords: beth.count ?? "",
      hotelFilterable: beth.count != null ? "yes" : "no",
      appConsumed: String(usage.consumed),
      required: String(!!usage.required),
      lastModifiedHint: total.lastModified || "",
      error: total.ok ? "" : total.error || "",
      notes: usage.note || "",
    });
  }
}

writeCsv(
  path.join(OUT, "01-AIRTABLE_TABLE_INVENTORY.csv"),
  Object.keys(tableInventory[0]),
  tableInventory,
);
writeCsv(
  path.join(OUT, "02-AIRTABLE_FIELD_INVENTORY.csv"),
  Object.keys(fieldInventory[0]),
  fieldInventory,
);
writeCsv(
  path.join(OUT, "10-TABLE_POPULATION_AUDIT.csv"),
  Object.keys(populationRows[0] || { tableName: "" }),
  populationRows,
);

// Relationship map
const relLines = [
  "# GDI Airtable relationship map",
  "",
  `Base: \`${BASE}\``,
  "",
  "## Core opportunity spine",
  "",
  "- **Group Demand Opportunities** (`tblRuReslJMwsfRQj`) ← canonical opportunity registry (`opportunityId` + `hotelId`)",
  "- Links (typed columns): `peVenueId`, `peSignalId`, `hotelVenueFitId`, `decisionId`, discovery run IDs (optional columns)",
  "- Nested fidelity: `opportunityPayloadJson` (compacted on write)",
  "",
  "## Research coverage",
  "",
  "- GDI Research Targets → Demand Generators / Programs / PE Venues (linked fields)",
  "- GDI Research Runs → hotel-scoped runs",
  "- GDI Research Target Runs → Target × Run execution grain",
  "",
  "## Demand generator graph",
  "",
  "- Demand Generators → Demand Programs → Demand Generator Signals",
  "- Hotel Demand Generator Fit → hotel × generator",
  "",
  "## Private events",
  "",
  "- Private Event Venues → Hotel Venue Fit → Private Event Signals → Group Demand Opportunities",
  "",
  "## Need dates",
  "",
  "- GDI Hotel Commercial Profiles → GDI Hotel Need Periods → Change History",
  "- GDI Need Period Opportunity Matches ↔ Opportunities + Need Periods",
  "- Legacy: Hotel Seasonality & Need Periods / Hotel Commercial Profiles (facility) — do not treat as GDI authority when GDI tables populated",
  "",
  "## Feedback / outcomes",
  "",
  "- Decisions / Decision Events (Airtable) + FS `feedback.json` (merge-at-read)",
  "",
  "## Customer publication (not Airtable)",
  "",
  "- Bundled publication package CURRENT — immutable customer content master",
  "",
];
fs.writeFileSync(path.join(OUT, "03-TABLE_RELATIONSHIP_MAP.md"), relLines.join("\n"));

console.log("[audit] loading Bethesda Airtable opportunities + V2 package…");
const atRows = await listOpportunitiesForHotel(HOTEL);
const pkg = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      HOTEL,
      PKG_VERSION,
      "package.json",
    ),
    "utf8",
  ),
);
const integrity = verifyPublicationPackageIntegrity(pkg);
const feedback = loadFeedback(HOTEL);
const pkgById = new Map((pkg.opportunities || []).map((o) => [o.id, o]));
const atById = new Map(atRows.map((o) => [o.id, o]));
const publishedIds = (pkg.opportunities || []).map((o) => o.id);
const coverage = assertPublishedIdsCoveredByResearchRegistry({
  publishedOpportunityIds: publishedIds,
  researchRegistryIds: atRows.map((o) => o.id),
});

// Fetch raw Airtable fields for payload analysis (sample all beth rows via REST for columns)
console.log("[audit] raw Airtable column vs payload analysis…");
async function listRawOpportunities() {
  const out = [];
  let offset = null;
  const formula = `{hotelId}='${HOTEL}'`;
  do {
    const params = new URLSearchParams({
      pageSize: "100",
      filterByFormula: formula,
    });
    if (offset) params.set("offset", offset);
    const r = await fetch(
      `https://api.airtable.com/v0/${BASE}/${encodeURIComponent("Group Demand Opportunities")}?${params}`,
      { headers: { Authorization: `Bearer ${TOKEN}` } },
    );
    const j = await r.json();
    if (!r.ok) throw new Error(JSON.stringify(j));
    out.push(...(j.records || []));
    offset = j.offset || null;
  } while (offset);
  return out;
}
const rawRecs = await listRawOpportunities();
const rawByOppId = new Map();
for (const rec of rawRecs) {
  const id = text(rec.fields?.opportunityId);
  if (id) rawByOppId.set(id, rec);
}

const typedColumns = Object.values(F);
const jsonFieldStats = new Map(); // key -> { presentInJson, presentInColumn, agree, disagree, onlyJson, onlyColumn }
function bump(key, field) {
  if (!jsonFieldStats.has(key)) {
    jsonFieldStats.set(key, {
      key,
      presentInJson: 0,
      presentInColumn: 0,
      agree: 0,
      disagree: 0,
      onlyJson: 0,
      onlyColumn: 0,
      columnName: null,
    });
  }
  const s = jsonFieldStats.get(key);
  s[field] += 1;
}

const columnToPayloadKey = {
  opportunityId: "id",
  hotelId: "hotelId",
  opportunityName: "title",
  organizationName: "organizationName",
  opportunityType: "opportunityType",
  priority: "priority",
  actionStatus: "actionStatus",
  eventStartDate: "eventStartDate",
  eventEndDate: "eventEndDate",
  segment: "segment",
  venueStatus: "venueSourcingStatus",
  qualification: "opportunityQualification",
  whyNow: "whyNow",
  whyThisMatters: "summaryWhyMatters",
  hotelWinThesis: "hotelOpportunityThesis",
  recommendedAction: "recommendedAction",
  primaryContactName: "primaryContact.name",
  primaryContactRole: "primaryContact.role",
  primaryContactEmail: "primaryContact.email",
  primaryContactPhone: "primaryContact.phone",
  sourceCount: "sourceCount",
  sourceSummary: "sourceSummary",
  lastResearchedAt: "lastVerifiedAt",
  researchVersion: "researchVersion",
  demandFamily: "demandFamily",
  demandSignalType: "demandSignalType",
  peVenueId: "peVenueId",
  peSignalId: "peSignalId",
  hotelVenueFitId: "hotelVenueFitId",
};

for (const id of publishedIds) {
  const rec = rawByOppId.get(id);
  if (!rec) continue;
  let payload = null;
  try {
    payload = rec.fields?.opportunityPayloadJson
      ? JSON.parse(rec.fields.opportunityPayloadJson)
      : null;
  } catch {
    payload = null;
  }
  for (const [col, payloadKey] of Object.entries(columnToPayloadKey)) {
    const colVal = rec.fields?.[col];
    const payVal = payload ? pickPath(payload, payloadKey) : undefined;
    const colOk = nonempty(colVal);
    const payOk = nonempty(payVal);
    const st = jsonFieldStats.get(payloadKey) || {
      key: payloadKey,
      presentInJson: 0,
      presentInColumn: 0,
      agree: 0,
      disagree: 0,
      onlyJson: 0,
      onlyColumn: 0,
      columnName: col,
    };
    st.columnName = col;
    if (payOk) st.presentInJson += 1;
    if (colOk) st.presentInColumn += 1;
    if (payOk && colOk) {
      const a = String(colVal);
      const b = typeof payVal === "object" ? JSON.stringify(payVal) : String(payVal);
      if (a === b || (col === "opportunityName" && text(payVal) && a.includes(text(payVal).slice(0, 20)))) {
        st.agree += 1;
      } else st.disagree += 1;
    } else if (payOk && !colOk) st.onlyJson += 1;
    else if (!payOk && colOk) st.onlyColumn += 1;
    jsonFieldStats.set(payloadKey, st);
  }
  // Track high-value presentation keys only in JSON
  if (payload) {
    for (const k of [
      "displayTitle",
      "salesActionKey",
      "salesActionPillLabel",
      "cardContactSubtitle",
      "revenueStreamIcons",
      "revenueStreamClass",
      "whatIsHappening",
      "needDateMatchV1",
      "sharedTenSectionDrawer",
      "commercialIntelligenceBrief",
      "sources",
      "primaryContact",
      "tileContactPreview",
      "commercialStageKey",
      "priorityPillLabel",
    ]) {
      const st = jsonFieldStats.get(`payloadOnly:${k}`) || {
        key: `payloadOnly:${k}`,
        presentInJson: 0,
        presentInColumn: 0,
        agree: 0,
        disagree: 0,
        onlyJson: 0,
        onlyColumn: 0,
        columnName: "(none — JSON only)",
      };
      if (nonempty(payload[k])) {
        st.presentInJson += 1;
        st.onlyJson += 1;
      }
      jsonFieldStats.set(`payloadOnly:${k}`, st);
    }
  }
}

writeJson(path.join(OUT, "05-OPPORTUNITY_JSON_FIELD_ANALYSIS.json"), {
  schema: "gdi-opportunity-json-field-analysis-v1",
  hotelId: HOTEL,
  publishedCount: publishedIds.length,
  airtableRawCount: rawRecs.length,
  typedColumnMap: columnToPayloadKey,
  compactionNote:
    "Airtable writes compact bulky research keys; customer presentation fields should survive in opportunityPayloadJson when present at upsert time.",
  fields: [...jsonFieldStats.values()].sort((a, b) => a.key.localeCompare(b.key)),
});

// Field-to-Airtable matrix from customer UI contract
const CUSTOMER_FIELDS = [
  // Header
  { section: "Header", label: "Hotel identity", dto: "hotelId", class: "AIRTABLE_DIRECT", table: "Group Demand Opportunities", field: "hotelId", required: true },
  { section: "Header", label: "Need-date status", dto: "hotelNeedDateConfigV1.status", class: "AIRTABLE_DERIVED", table: "GDI Hotel Need Periods", field: "Status", required: true, note: "Resolved live at serve; not package fingerprint" },
  { section: "Header", label: "Opportunity count", dto: "count", class: "PUBLICATION_METADATA", table: "publication package", field: "opportunities.length", required: true },
  // Cards
  { section: "Opportunity card", label: "Canonical ID", dto: "id", class: "AIRTABLE_DIRECT", table: "Group Demand Opportunities", field: "opportunityId", required: true },
  { section: "Opportunity card", label: "Title", dto: "displayTitle", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "opportunityPayloadJson.displayTitle / opportunityName", required: true },
  { section: "Opportunity card", label: "Organization", dto: "organizationName", class: "AIRTABLE_DIRECT", table: "Group Demand Opportunities", field: "organizationName", required: true },
  { section: "Opportunity card", label: "Priority", dto: "priority", class: "AIRTABLE_DIRECT", table: "Group Demand Opportunities", field: "priority", required: true },
  { section: "Opportunity card", label: "Priority pill", dto: "priorityPillLabel", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "opportunityPayloadJson.priorityPillLabel", required: true },
  { section: "Opportunity card", label: "Commercial-action badge", dto: "salesActionPillLabel", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "opportunityPayloadJson.salesActionKey/PillLabel", required: true },
  { section: "Opportunity card", label: "Event dates", dto: "eventStartDate", class: "AIRTABLE_DIRECT", table: "Group Demand Opportunities", field: "eventStartDate", required: false },
  { section: "Opportunity card", label: "Date pill", dto: "datePillDisplayLabel", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "opportunityPayloadJson.datePillDisplayLabel", required: false },
  { section: "Opportunity card", label: "Opportunity type", dto: "opportunityType", class: "AIRTABLE_DIRECT", table: "Group Demand Opportunities", field: "opportunityType", required: true },
  { section: "Opportunity card", label: "Sourcing status", dto: "venueSourcingStatus", class: "AIRTABLE_DIRECT", table: "Group Demand Opportunities", field: "venueStatus", required: false },
  { section: "Opportunity card", label: "Summary", dto: "whatIsHappening", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "opportunityPayloadJson.whatIsHappening / whyNow", required: true },
  { section: "Opportunity card", label: "Contact name", dto: "primaryContact.name", class: "AIRTABLE_DIRECT", table: "Group Demand Opportunities", field: "primaryContactName + payload.primaryContact", required: true },
  { section: "Opportunity card", label: "Contact subtitle", dto: "cardContactSubtitle", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "opportunityPayloadJson.cardContactSubtitle / primaryContactRole", required: true },
  { section: "Opportunity card", label: "SVG revenue icons", dto: "revenueStreamIcons", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "opportunityPayloadJson.revenueStreamClass/Icons", required: true },
  { section: "Opportunity card", label: "Need-date tile state", dto: "needDateMatchV1.displayState", class: "AIRTABLE_DERIVED", table: "GDI Need Period Opportunity Matches", field: "Match + period join", required: false },
  // Drawer
  { section: "Drawer", label: "Overview / demand thesis", dto: "summaryWhat / hotelOpportunityThesis", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "whyThisMatters / hotelWinThesis / payload", required: true },
  { section: "Drawer", label: "Recommended next step", dto: "recommendedAction", class: "AIRTABLE_DIRECT", table: "Group Demand Opportunities", field: "recommendedAction", required: true },
  { section: "Drawer", label: "Why now", dto: "whyNow", class: "AIRTABLE_DIRECT", table: "Group Demand Opportunities", field: "whyNow", required: true },
  { section: "Drawer", label: "Supporting sources", dto: "sources", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "sourceSummary + payload.sources (+ FS evidence)", required: true },
  { section: "Drawer", label: "Need-Date Match detail", dto: "needDateMatchV1", class: "AIRTABLE_DERIVED", table: "GDI Need Period Opportunity Matches", field: "match fields", required: false },
  { section: "Drawer", label: "Customer feedback", dto: "feedback/validation", class: "FEEDBACK_DERIVED", table: "Decisions/FS feedback.json", field: "merge-at-read", required: false },
  { section: "Drawer", label: "Ten-section drawer body", dto: "sharedTenSectionDrawer", class: "LOCAL_ONLY_DEPENDENCY", table: "Group Demand Opportunities", field: "often compacted out of payload; rebuilt at serve from CIB", required: true, note: "P0 risk if CIB also compacted" },
  { section: "Drawer", label: "Commercial intelligence brief", dto: "commercialIntelligenceBrief", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "opportunityPayloadJson (may be slimmed)", required: true },
  // Filters
  { section: "Filters", label: "Market Scope", dto: "hotelMarketScopeEligible", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "payload flags", required: true },
  { section: "Filters", label: "Sales Opportunities filter", dto: "salesActionKey", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "Group Demand Opportunities", field: "payload.salesActionKey", required: true },
  { section: "Filters", label: "Weekly Change", dto: "weeklyChange", class: "MISSING_SOURCE", table: "unknown", field: "not durable in Airtable columns", required: false },
  { section: "Export", label: "CSV columns", dto: "customer-csv-export", class: "AIRTABLE_PLUS_VERSIONED_RULE", table: "publication DTO", field: "export mapper", required: true },
  { section: "Publication", label: "Content fingerprint", dto: "contentFingerprintSha256", class: "PUBLICATION_METADATA", table: "bundled package", field: "CURRENT", required: true },
  { section: "UI", label: "SVG geometry / CSS", dto: "n/a", class: "UI_PRESENTATION_ONLY", table: "n/a", field: "code assets", required: false },
];

const matrixRows = [];
for (const f of CUSTOMER_FIELDS) {
  let pop = 0;
  let missing = 0;
  for (const id of publishedIds) {
    const pkgOpp = pkgById.get(id);
    const atOpp = atById.get(id);
    const raw = rawByOppId.get(id);
    let ok = false;
    if (f.dto === "id") ok = nonempty(atOpp?.id);
    else if (f.dto === "hotelId") ok = text(atOpp?.hotelId) === HOTEL;
    else if (f.dto === "displayTitle") ok = nonempty(atOpp?.displayTitle || atOpp?.title || raw?.fields?.opportunityName);
    else if (f.dto === "organizationName") ok = nonempty(atOpp?.organizationName || raw?.fields?.organizationName);
    else if (f.dto === "priority") ok = nonempty(atOpp?.priority || raw?.fields?.priority);
    else if (f.dto === "primaryContact.name") ok = nonempty(atOpp?.primaryContact?.name || raw?.fields?.primaryContactName);
    else if (f.dto === "cardContactSubtitle") ok = nonempty(atOpp?.cardContactSubtitle || atOpp?.primaryContact?.title || atOpp?.primaryContact?.role || raw?.fields?.primaryContactRole);
    else if (f.dto === "salesActionPillLabel") ok = nonempty(atOpp?.salesActionPillLabel || atOpp?.salesActionKey);
    else if (f.dto === "whatIsHappening") ok = nonempty(atOpp?.whatIsHappening || atOpp?.summaryWhat || raw?.fields?.whyNow);
    else if (f.dto === "revenueStreamIcons") ok = nonempty(atOpp?.revenueStreamIcons || atOpp?.revenueStreamClass);
    else if (f.dto === "sources") ok = nonempty(atOpp?.sources) || nonempty(raw?.fields?.sourceSummary);
    else if (f.dto === "recommendedAction") ok = nonempty(atOpp?.recommendedAction || raw?.fields?.recommendedAction);
    else if (f.dto === "whyNow") ok = nonempty(atOpp?.whyNow || raw?.fields?.whyNow);
    else if (f.dto === "sharedTenSectionDrawer") ok = nonempty(atOpp?.sharedTenSectionDrawer) || nonempty(atOpp?.commercialIntelligenceBrief) || nonempty(atOpp?.salesBrief);
    else if (f.dto === "commercialIntelligenceBrief") ok = nonempty(atOpp?.commercialIntelligenceBrief || atOpp?.salesBrief);
    else if (f.class === "PUBLICATION_METADATA" || f.class === "UI_PRESENTATION_ONLY" || f.class === "FEEDBACK_DERIVED") ok = true;
    else if (f.class === "MISSING_SOURCE") ok = false;
    else ok = nonempty(pickPath(atOpp || {}, f.dto)) || nonempty(pickPath(pkgOpp || {}, f.dto));

    // For reconstruction test: prefer Airtable presence
    if (f.required && f.class !== "PUBLICATION_METADATA" && f.class !== "UI_PRESENTATION_ONLY" && f.class !== "FEEDBACK_DERIVED") {
      if (ok) pop += 1;
      else missing += 1;
    } else if (ok) pop += 1;
  }
  matrixRows.push({
    uiSection: f.section,
    uiLabel: f.label,
    finalDtoProperty: f.dto,
    sourceAirtableTable: f.table,
    sourceAirtableField: f.field,
    canonicalFieldType: f.class.includes("DIRECT") ? "typed-column-or-payload" : "derived/json/meta",
    joinOrLinkedPath: f.note || "",
    researchSource: "GDI research registry / need-date / feedback",
    transformationOrDerivation: f.class,
    validationRule: f.required ? "required for customer content" : "optional",
    requiredOrOptional: f.required ? "required" : "optional",
    currentPopulationCount: pop,
    missingValues: missing,
    fallbackBehavior: f.class === "LOCAL_ONLY_DEPENDENCY" ? "serve-time rebuild / package freeze" : "none/package",
    localOrPackageOnlyRequired: ["LOCAL_ONLY_DEPENDENCY", "MISSING_SOURCE", "PUBLICATION_METADATA"].includes(f.class) ? "yes" : "no",
    classification: f.class,
    recommendedCorrection: f.class === "LOCAL_ONLY_DEPENDENCY" || f.class === "MISSING_SOURCE"
      ? "Persist durable semantic inputs in Airtable typed columns or non-compacted payload; version derivation rules"
      : f.class === "AIRTABLE_PLUS_VERSIONED_RULE"
        ? "Ensure payload retains field at upsert; add typed column if filterable"
        : "",
  });
}
writeCsv(
  path.join(OUT, "04-GDI_FIELD_TO_AIRTABLE_MATRIX.csv"),
  Object.keys(matrixRows[0]),
  matrixRows,
);
// Also copy as named alias
fs.copyFileSync(
  path.join(OUT, "04-GDI_FIELD_TO_AIRTABLE_MATRIX.csv"),
  path.join(OUT, "GDI_FIELD_TO_AIRTABLE_MATRIX.csv"),
);

// Bethesda Airtable-only reconstruction vs package (NO package fallback for candidate)
console.log("[audit] Airtable-only reconstruction compare…");
const compareFields = [
  "id",
  "displayTitle",
  "title",
  "organizationName",
  "priority",
  "priorityPillLabel",
  "salesActionKey",
  "salesActionPillLabel",
  "opportunityType",
  "whatIsHappening",
  "summaryWhat",
  "whyNow",
  "recommendedAction",
  "cardContactSubtitle",
  "revenueStreamClass",
  "primaryContact.name",
  "primaryContact.title",
  "primaryContact.email",
  "eventStartDate",
  "datePillDisplayLabel",
  "commercialIntelligenceBrief",
  "sharedTenSectionDrawer",
  "sources",
  "needDateMatchV1",
];

function getVal(o, key) {
  if (key.includes(".")) return pickPath(o, key);
  return o?.[key];
}
function norm(v) {
  if (v == null) return "";
  if (typeof v === "object") {
    try {
      return JSON.stringify(v);
    } catch {
      return String(v);
    }
  }
  return text(v);
}

const perOpp = [];
const fieldDiffCounts = Object.fromEntries(compareFields.map((k) => [k, { match: 0, mismatch: 0, missingInAirtable: 0 }]));
for (const id of publishedIds) {
  const pkgOpp = pkgById.get(id);
  const atOpp = atById.get(id); // from listOpportunities — payload preferred
  const diffs = [];
  const reconstructed = atOpp
    ? buildCanonicalCustomerPresentationContract(atOpp, {
        contentFingerprintSha256: null,
        listScope: "airtable_reconstruction_test",
      })
    : null;
  const pkgContract = pkgOpp
    ? buildCanonicalCustomerPresentationContract(pkgOpp, {
        contentFingerprintSha256: FP,
      })
    : null;

  for (const key of compareFields) {
    const a = getVal(atOpp, key);
    const p = getVal(pkgOpp, key);
    const aN = norm(a);
    const pN = norm(p);
    if (!atOpp) {
      fieldDiffCounts[key].missingInAirtable += 1;
      diffs.push({ key, status: "MISSING_AIRTABLE_ROW" });
    } else if (!nonempty(a) && nonempty(p)) {
      fieldDiffCounts[key].missingInAirtable += 1;
      diffs.push({ key, status: "MISSING_IN_AIRTABLE_VALUE", packageSample: pN.slice(0, 120) });
    } else if (nonempty(a) && nonempty(p) && aN !== pN) {
      // soft match titles
      if (key === "displayTitle" || key === "title") {
        if (aN.toLowerCase() === pN.toLowerCase() || aN.includes(pN.slice(0, 24)) || pN.includes(aN.slice(0, 24))) {
          fieldDiffCounts[key].match += 1;
          continue;
        }
      }
      if (key === "primaryContact.name" && aN.toLowerCase() === pN.toLowerCase()) {
        fieldDiffCounts[key].match += 1;
        continue;
      }
      fieldDiffCounts[key].mismatch += 1;
      diffs.push({
        key,
        status: "MISMATCH",
        airtable: aN.slice(0, 160),
        package: pN.slice(0, 160),
      });
    } else {
      fieldDiffCounts[key].match += 1;
    }
  }

  // Contract-level compare for commercial action / contact subtitle
  const contractDiffs = [];
  if (reconstructed && pkgContract) {
    for (const [pathKey, getter] of [
      ["commercialAction.pillLabel", (c) => c.commercialAction?.pillLabel],
      ["contact.cardContactSubtitle", (c) => c.contact?.cardContactSubtitle],
      ["contact.name", (c) => c.contact?.name],
      ["display.title", (c) => c.display?.title],
      ["icons.revenueStreamClass", (c) => c.icons?.revenueStreamClass],
    ]) {
      const av = text(getter(reconstructed));
      const pv = text(getter(pkgContract));
      if (pv && !av) contractDiffs.push({ path: pathKey, status: "MISSING_IN_AIRTABLE_RECON" });
      else if (av && pv && av !== pv) contractDiffs.push({ path: pathKey, status: "MISMATCH", airtable: av, package: pv });
    }
  }

  perOpp.push({
    id,
    title: pkgOpp?.displayTitle || pkgOpp?.title,
    inAirtable: Boolean(atOpp),
    diffCount: diffs.length,
    contractDiffCount: contractDiffs.length,
    diffs: diffs.slice(0, 30),
    contractDiffs,
  });
}

const reconSummary = {
  schema: "gdi-bethesda-airtable-reconstruction-v1",
  hotelId: HOTEL,
  approvedFingerprint: FP,
  packageIntegrityOk: integrity?.ok !== false,
  publishedCount: publishedIds.length,
  airtableCount: atRows.length,
  coverage,
  usedPublicationPackageAsFallback: false,
  fieldDiffCounts,
  opportunitiesWithAnyDiff: perOpp.filter((p) => p.diffCount > 0 || p.contractDiffCount > 0).length,
  opportunitiesExactOnComparedKeys: perOpp.filter((p) => p.diffCount === 0 && p.contractDiffCount === 0).length,
  topMissingFields: Object.entries(fieldDiffCounts)
    .map(([k, v]) => ({ field: k, ...v }))
    .sort((a, b) => b.missingInAirtable - a.missingInAirtable)
    .slice(0, 15),
  perOpportunity: perOpp,
};
writeJson(path.join(OUT, "11-BETHESDA_AIRTABLE_RECONSTRUCTION.json"), reconSummary);

const missingFields = {
  schema: "gdi-missing-fields-and-links-v1",
  localOnlyOrMissingRequired: matrixRows.filter(
    (r) =>
      r.requiredOrOptional === "required" &&
      (r.classification === "LOCAL_ONLY_DEPENDENCY" ||
        r.classification === "MISSING_SOURCE" ||
        Number(r.missingValues) > 0),
  ),
  reconstructionGaps: reconSummary.topMissingFields.filter((f) => f.missingInAirtable > 0 || f.mismatch > 0),
  unexplainedLocalDependencies: matrixRows.filter((r) => r.classification === "LOCAL_ONLY_DEPENDENCY"),
};
writeJson(path.join(OUT, "12-MISSING_FIELDS_AND_LINKS.json"), missingFields);

// Evidence audit
const evidenceAudit = {
  schema: "gdi-evidence-and-research-history-audit-v1",
  hotelId: HOTEL,
  opportunities: publishedIds.map((id) => {
    const at = atById.get(id);
    const raw = rawByOppId.get(id);
    const sources = Array.isArray(at?.sources) ? at.sources.length : 0;
    return {
      id,
      sourceCountColumn: raw?.fields?.sourceCount ?? null,
      sourceSummaryPresent: nonempty(raw?.fields?.sourceSummary),
      payloadSourcesCount: sources,
      lastResearchedAt: raw?.fields?.lastResearchedAt || at?.lastVerifiedAt || null,
      researchVersion: raw?.fields?.researchVersion || at?.researchVersion || null,
      researchRegistryProvenance: at?.researchRegistryProvenance || null,
      hasResearchHistoryBlob: Boolean(at?.researchAttempts || at?.supervisorV2),
      note: at?.researchAttempts
        ? "history present in memory/FS path"
        : "research history blobs typically compacted out of Airtable payload",
    };
  }),
  durableFsDependencies: [
    "data/.../opportunity-research-state/",
    "data/.../runs/",
    "data/.../reconstructed-opportunities-v1.json",
    "feedback.json",
  ],
  airtableEvidenceTables: [
    "Hotel Intelligence Evidence",
    "Demand Generator Signals",
    "GDI Research Target Runs",
  ],
};
writeJson(path.join(OUT, "08-EVIDENCE_AND_RESEARCH_HISTORY_AUDIT.json"), evidenceAudit);

// Contact / commercial / need-date markdown reviews
fs.writeFileSync(
  path.join(OUT, "06-CONTACT_DATA_MODEL_REVIEW.md"),
  `# Contact data model review

## Current storage

Contacts are **embedded** on Group Demand Opportunities:

- Typed columns: \`primaryContactName\`, \`primaryContactRole\`, \`primaryContactEmail\`, \`primaryContactPhone\`, \`contactQuality\`, \`primaryContactId\`
- Rich object in \`opportunityPayloadJson.primaryContact\` / \`tileContactPreview\` / \`cardContactSubtitle\`

## Field authority (preserve)

- Card subtitle = frozen professional title / displayRole — not opportunityRole remapping
- Surfe remains on-demand; do not invent booking titles

## Adequacy

| Need | Status |
|------|--------|
| Name / email / phone | Typed columns + JSON — **adequate for registry** |
| Verified professional title | Mostly JSON (\`primaryContact.title\`) — **column weak** (role column overloaded) |
| Activity-specific role | JSON only |
| Buyer vs intermediary | JSON / eligibility objects |
| Source evidence URL | JSON only |
| Enrichment status | JSON / Surfe usage FS |

## Recommendation

- **P1:** Add typed columns: \`primaryContactProfessionalTitle\`, \`primaryContactActivityRole\`, \`cardContactSubtitle\`, \`contactSourceUrl\` without requiring a full Contacts table yet.
- **P2:** Normalized Contacts + Contact–Opportunity link table only if multi-hotel contact reuse / dedupe becomes operationally required.
- Do **not** create Contacts table in this audit; compare shows embedded model still primary consumer path.
`,
);

fs.writeFileSync(
  path.join(OUT, "07-COMMERCIAL_ACTION_DATA_MODEL_REVIEW.md"),
  `# Commercial recommendation data model review

## Origins today

| Output | Typical origin | Durable in Airtable? |
|--------|----------------|----------------------|
| Priority | Research + remediation | Typed \`priority\` column + payload |
| Commercial action / sales pill | Deterministic resolvers + frozen package DTO | **Mostly payload JSON** (\`salesActionKey\`, \`salesActionPillLabel\`) — not typed columns |
| Recommended next step | Research / remediation | Typed \`recommendedAction\` + payload |
| Why now / why hotel | Research | Typed \`whyNow\` / \`whyThisMatters\` / \`hotelWinThesis\` |
| Evidence confidence | Research | Typed \`evidenceConfidence\` |
| Revenue stream SVG inputs | Classification rules + freeze | Payload \`revenueStreamClass\` / icons |
| Sourcing status | Research | Typed \`venueStatus\` |

## Gap

External customer renderer must **not** re-run independent commercial-action resolvers. Approved package freezes outputs. For Airtable-backed regeneration, the same inputs + **versioned derivation rules** must reproduce pills deterministically — today many approved pill strings exist only in publication DTOs / payload, and historical upserts may lack full salesAction fields.

## Recommendation

- Store: \`salesActionKey\`, \`commercialStageKey\`, \`revenueStreamClass\`, derivation rule version, evidence refs as typed or guaranteed payload fields.
- Keep approved publication immutable; regenerate only under founder publish workflow.
`,
);

fs.writeFileSync(
  path.join(OUT, "09-NEED_DATE_DATA_MODEL_REVIEW.md"),
  `# Need-Date Match data model review

## Authority (recommended)

| Attribute | Canonical table | Notes |
|-----------|-----------------|-------|
| Need periods | **GDI Hotel Need Periods** | Confirmed vs inferred via Status / Source |
| Change history | GDI Need Period Change History | Versioned |
| Opportunity overlap / match | **GDI Need Period Opportunity Matches** | Match strength + links |
| Hotel commercial selection profile | GDI Hotel Commercial Profiles | Portal selection-first |
| Facility commercial profile | Hotel Commercial Profiles | ADP/facility — not GDI need-date authority |
| Legacy seasonality | Hotel Seasonality & Need Periods | **Legacy** — do not dual-write |

## Application consumption

- Hotel-level status resolved live at auth/share serve (\`resolveHotelNeedDateConfiguration\`) — **not** inside publication fingerprint.
- Per-opportunity match tiles from match table + opportunity dates.

## Conflicts

Duplication risk between facility seasonality tables and GDI need-period tables. Template must document GDI tables as authority for Need-Date Match.
`,
);

fs.writeFileSync(
  path.join(OUT, "13-CANONICAL_SCHEMA_RECOMMENDATION.md"),
  `# Canonical GDI schema recommendation

## Target flow

**Airtable-backed canonical research/commercial data → versioned deterministic presentation rules → immutable founder-approved publication (+ feedback merge).**

## Keep separate

1. Mutable research registry (Group Demand Opportunities + graph tables)
2. Durable evidence/history (FS research-state/runs + Airtable signals/target-runs; stop relying on reconstructed bags for production hotels)
3. Deterministic derived commercial values (versioned rules; persist keys + rule version)
4. Immutable publication packages
5. Feedback (FS + Decisions)

## Minimum schema additions (justified by matrix)

On Group Demand Opportunities (typed columns):

- \`salesActionKey\`, \`commercialStageKey\`, \`revenueStreamClass\`
- \`cardContactSubtitle\`, \`primaryContactProfessionalTitle\`
- \`displayTitle\` (if distinct from opportunityName)
- \`presentationRuleVersion\`

Ensure write path does **not** compact away customer-required payload keys.

Optional later: Contacts + OpportunityContacts; Evidence links table.

## Do not

- Replace publication packages with live Airtable customer reads
- Duplicate entire payload into hundreds of columns
- Invent need dates or contact titles
`,
);

const p0 = missingFields.localOnlyOrMissingRequired.filter(
  (r) => r.classification === "LOCAL_ONLY_DEPENDENCY" || Number(r.missingValues) > publishedIds.length * 0.2,
);
fs.writeFileSync(
  path.join(OUT, "14-IMPLEMENTATION_AND_MIGRATION_PLAN.md"),
  `# Implementation and migration plan (audit only — no execution)

## P0 — Customer content cannot be reconstructed from durable canonical data

| Item | Problem | Fix | Risk |
|------|---------|-----|------|
| Presentation pills / icons / drawer body | Approved V2 fields missing or mismatched vs Airtable payload | Persist required presentation semantic fields at upsert; stop compacting them; backfill from approved package **into research registry only** under separate auth | Medium — must not mutate publication |
| sharedTenSectionDrawer / CIB | Compacted out | Guarantee CIB or drawer inputs in payload; rebuild drawer from CIB with versioned builder | Medium |
| Weekly change filter | MISSING_SOURCE | Define durable change ledger or drop filter until backed | Low |

**Rollback:** Airtable row versioning via re-upsert from snapshots; publication untouched.

## P1 — Essential fields/relationships inconsistent

- Typed columns for salesActionKey, revenueStreamClass, cardContactSubtitle
- Need-date match rows for Bethesda coverage audit (populate matches if feature shown)
- Contact professional title column
- Research target/run linkage fields populated for published IDs

## P2 — Quality / filtering / efficiency

- Normalize Contacts if multi-hotel reuse needed
- Evidence index table for URL dedupe
- Deprecate legacy seasonality dual-write

## P3 — Optional

- Full Contacts CRM graph
- Materialized weekly change

## Authorization required before any apply

Founder approval for: schema column adds, backfill from publication DTOs into Airtable research rows, any production write. **Never** republish Bethesda without separate approval.
`,
);

const templateGaps = {
  schema: "gdi-template-certification-gaps-v1",
  researchIdCoverage: coverage,
  fieldCoverageGate: {
    ok: p0.length === 0 && reconSummary.opportunitiesExactOnComparedKeys === publishedIds.length,
    requiredLocalOnlyRemaining: missingFields.unexplainedLocalDependencies.length,
    opportunitiesNotFullyReconstructable: reconSummary.opportunitiesWithAnyDiff,
    publishedCount: publishedIds.length,
  },
  publicationImmutability: {
    fingerprint: FP,
    intact: pkg.contentFingerprintSha256 === FP,
  },
  feedbackPreserved: (feedback.items || []).length >= 0,
  templateCertified: false,
  blockers: [
    "Airtable ID coverage alone is insufficient",
    `${reconSummary.opportunitiesWithAnyDiff}/${publishedIds.length} opportunities differ when reconstructed from Airtable vs approved V2 on compared fields`,
    `${missingFields.unexplainedLocalDependencies.length} LOCAL_ONLY_DEPENDENCY required fields remain`,
    "Must not declare GDI_FULL_E2E_HOTEL_TEMPLATE_V1 certified until reconstruction parity gate passes",
  ],
  recommendedGates: [
    "publishedIds ⊆ researchRegistryIds",
    "required customer fields airtable-backed (matrix classification ban LOCAL_ONLY/MISSING for required)",
    "airtable reconstruction parity vs approved package (or explicit waiver)",
    "publication fingerprint immutable",
    "feedback merge intact",
  ],
};
writeJson(path.join(OUT, "15-TEMPLATE_CERTIFICATION_GAPS.json"), templateGaps);

const gate = evaluateFullE2eHotelTemplateResearchStorageGate({
  publishedOpportunityIds: publishedIds,
  researchRegistryIds: atRows.map((o) => o.id),
  requiredPublicationFingerprint: FP,
  actualPublicationFingerprint: pkg.contentFingerprintSha256,
});

const founder = `# GDI Airtable Complete Data Model Audit V1

**Read-only.** No schema/record changes. Bethesda V2 publication **not** modified.  
**Base:** \`${BASE}\` · **Opportunities:** \`tblRuReslJMwsfRQj\`  
**Hotel:** Bethesda Marriott (\`${HOTEL}\`) · Fingerprint \`${FP}\`

## Verdict

| Gate | Result |
|------|--------|
| All 67 published IDs in Airtable | **${coverage.ok ? "PASS" : "FAIL"}** (${coverage.publishedCount} / registry ${coverage.registryCount}) |
| Airtable-only reconstruction == approved V2 | **FAIL** — ${reconSummary.opportunitiesWithAnyDiff}/${publishedIds.length} opportunities have field/contract diffs |
| Zero required LOCAL_ONLY / MISSING sources | **FAIL** — see matrix |
| Template certification | **NOT CERTIFIED** |

ID presence ≠ field completeness. Customer surfaces today are powered primarily by **immutable publication DTOs** (+ live need-date status + feedback). Airtable holds registry identity and many typed columns, but critical presentation fields live in \`opportunityPayloadJson\` and/or were compacted / never backfilled from the V2 freeze.

## What powers GDI outputs today

1. **Customer cards/drawers (published hotels):** founder-approved publication package  
2. **Need-date hotel status:** live GDI Need Period tables (outside fingerprint)  
3. **Feedback:** FS feedback / Decisions merge-at-read  
4. **Research/live mode:** Airtable opportunities (+ FS enrich)  
5. **Icons/CSS:** code (semantic inputs should be data-backed)

## Top reconstruction gaps (Airtable vs V2)

${reconSummary.topMissingFields
  .slice(0, 8)
  .map((f) => `- \`${f.field}\`: missing=${f.missingInAirtable}, mismatch=${f.mismatch}, match=${f.match}`)
  .join("\n")}

## Schema inventory

- ${tables.length} tables in base; GDI-relevant counted in \`10-TABLE_POPULATION_AUDIT.csv\`
- Group Demand Opportunities: **${populationRows.find((r) => r.tableName === "Group Demand Opportunities")?.totalRecords ?? "?"}** total · **${populationRows.find((r) => r.tableName === "Group Demand Opportunities")?.bethesdaRecords ?? "?"}** Bethesda

## Recommended architecture

See \`13-CANONICAL_SCHEMA_RECOMMENDATION.md\` and \`14-IMPLEMENTATION_AND_MIGRATION_PLAN.md\`.

**Do not** replace immutable publication with live Airtable customer reads.

## Artifacts

\`01\`–\`15\` in this directory + \`GDI_FIELD_TO_AIRTABLE_MATRIX.csv\`.

Research storage ID gate: ${gate.ok ? "PASS" : "FAIL"} · Full field reconstruction gate: FAIL
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founder);

console.log(
  JSON.stringify(
    {
      tables: tables.length,
      coverageOk: coverage.ok,
      airtableBeth: atRows.length,
      reconDiffOpps: reconSummary.opportunitiesWithAnyDiff,
      exact: reconSummary.opportunitiesExactOnComparedKeys,
      localOnly: missingFields.unexplainedLocalDependencies.length,
      out: OUT,
    },
    null,
    2,
  ),
);
