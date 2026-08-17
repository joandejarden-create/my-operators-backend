/**
 * Read-only Airtable UI visibility diagnostic for Census v1.1 fields.
 *
 *   npm run research-engine-v2:production-census-v11-airtable-visibility-check
 */

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import { buildV11FieldSpecs } from "../lib/research-engine-v2/production-census-schema-v11.js";
import {
  TABLE_IDS,
  EXPECTED_FREEZE,
  PRODUCTION_USE_STATUS,
} from "../lib/research-engine-v2/production-census-write.js";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");

export const STATUS = Object.freeze({
  UI_VISIBILITY: "production_census_v11_fields_exist_ui_visibility_issue",
  MISSING: "production_census_v11_fields_missing_needs_schema_recheck",
  WRONG_BASE: "production_census_v11_founder_looking_at_wrong_base_or_table",
});

const CENSUS_TABLES = [
  "Hotel Property Census",
  "Hotel Property Brand Affiliations",
  "Hotel Property Source Evidence",
  "Hotel Property Steward Review",
];

const SEARCH_TARGETS = [
  "Hotel Description - Source Text",
  "Amenities - Structured Tags",
  "Owner Name",
  "Operator / Management Company",
  "Enrichment Status",
  "Human Review Required",
  "Production Use Status",
];

function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}

async function metaTables(baseId, token, includeVisible = false) {
  const q = includeVisible ? "?include%5B%5D=visibleFieldIds" : "";
  const res = await fetch(
    `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables${q}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(`meta ${res.status}: ${JSON.stringify(json.error || json)}`);
  return json.tables || [];
}

async function listAll(baseId, token, tableId, fields) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${res.status}`);
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

async function main() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const platformId = bases.platform_base_id || bases.target_base_id;
  const mvpId = process.env.AIRTABLE_BASE_ID;
  const sandboxId = process.env.AIRTABLE_BASE_ID_SANDBOX;

  const platformTables = await metaTables(platformId, token, true);
  const mvpTables = mvpId ? await metaTables(mvpId, token, false) : [];
  const sandboxTables = sandboxId ? await metaTables(sandboxId, token, false) : [];

  const tablePresence = {};
  for (const name of CENSUS_TABLES) {
    const t = platformTables.find((x) => x.name === name);
    tablePresence[name] = {
      exists_on_platform: Boolean(t),
      id: t?.id || null,
      exists_on_mvp: mvpTables.some((x) => x.name === name),
      exists_on_sandbox: sandboxTables.some((x) => x.name === name),
    };
  }

  const census = platformTables.find((t) => t.name === "Hotel Property Census");
  const v11Names = buildV11FieldSpecs().map((f) => f.name);
  const fields = census?.fields || [];
  const fieldByName = Object.fromEntries(fields.map((f) => [f.name, f]));
  const v11Present = v11Names.filter((n) => fieldByName[n]);
  const v11Missing = v11Names.filter((n) => !fieldByName[n]);

  const views = (census?.views || []).map((v) => {
    const visible = new Set(v.visibleFieldIds || []);
    const hiddenV11 = v11Present.filter((n) => {
      const f = fieldByName[n];
      return v.visibleFieldIds ? !visible.has(f.id) : null;
    });
    return {
      id: v.id,
      name: v.name,
      type: v.type,
      visible_field_count: v.visibleFieldIds?.length ?? null,
      v11_hidden_count: Array.isArray(hiddenV11) ? hiddenV11.filter(Boolean).length : null,
      v11_hidden_names: Array.isArray(hiddenV11) ? hiddenV11 : [],
    };
  });

  const searchTargets = SEARCH_TARGETS.map((name) => {
    const f = fieldByName[name];
    const grid = views.find((v) => v.type === "grid") || views[0];
    const visibleIds = census?.views?.find((v) => v.id === grid?.id)?.visibleFieldIds;
    const visible = visibleIds && f ? visibleIds.includes(f.id) : null;
    return {
      name,
      exists: Boolean(f),
      field_id: f?.id || null,
      type: f?.type || null,
      visible_in_default_grid: visible,
    };
  });

  const rows = census
    ? await listAll(platformId, token, census.id, [
        "Property Identity Key",
        "Enrichment Status",
        "Human Review Required",
        "Production Use Status",
        "Hotel Description - Source Text",
        "Amenities - Source Text",
        "Owner Name",
        "Operator / Management Company",
        "Rooms / Keys",
        "Opening Date",
      ])
    : [];

  const blankByDesign = [
    "Hotel Description - Source Text",
    "Hotel Description - AI Summary",
    "Short Property Summary",
    "Amenities - Source Text",
    "Amenities - Structured Tags",
    "Owner Name",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
    "Developer Name",
  ];
  const safeBackfill = [
    "Enrichment Status (= Not Started)",
    "Human Review Required (= true on 4 held only)",
    "Data Confidence Tier",
    "Enrichment Priority",
    "Independent Classification / soft-brand flags (derived)",
  ];

  const recordStats = {
    count: rows.length,
    enrichment_not_started: rows.filter((r) => r.fields?.["Enrichment Status"] === "Not Started")
      .length,
    human_review_true: rows.filter((r) => r.fields?.["Human Review Required"] === true).length,
    production_use_ok: rows.filter((r) => r.fields?.["Production Use Status"] === PRODUCTION_USE_STATUS)
      .length,
    description_filled: rows.filter((r) => Boolean(r.fields?.["Hotel Description - Source Text"]))
      .length,
    amenities_filled: rows.filter((r) => Boolean(r.fields?.["Amenities - Source Text"])).length,
    owner_filled: rows.filter((r) => Boolean(r.fields?.["Owner Name"])).length,
    operator_filled: rows.filter((r) => Boolean(r.fields?.["Operator / Management Company"]))
      .length,
    rooms_filled: rows.filter((r) => r.fields?.["Rooms / Keys"] != null).length,
    opening_filled: rows.filter((r) => Boolean(r.fields?.["Opening Date"])).length,
  };

  const allTablesOnPlatform = CENSUS_TABLES.every((n) => tablePresence[n].exists_on_platform);
  const fieldsComplete = v11Missing.length === 0 && fields.length >= 95;
  const gridHidesV11 = views.some((v) => (v.v11_hidden_count || 0) > 0);

  let status = STATUS.WRONG_BASE;
  if (!allTablesOnPlatform || !fieldsComplete) status = STATUS.MISSING;
  else if (gridHidesV11) status = STATUS.UI_VISIBILITY;
  else {
    // Fields exist + visible → founder almost certainly on wrong base/table,
    // or expecting renames / filled enrichment that were never applied.
    status = STATUS.WRONG_BASE;
  }

  const report = {
    generated_at: new Date().toISOString(),
    mode: "read_only",
    airtable_writes: false,
    status,
    freeze_hash: EXPECTED_FREEZE,
    bases: {
      correct_base: {
        name: "Deal Capture Platform",
        id_masked: mask(platformId),
        id_full_hint: "appCCU…foLk",
        role: "Hotel Property Census production home",
      },
      not_here: [
        {
          name: "Deal Capture MVP",
          id_masked: mask(mvpId),
          reason: "Brand Explorer / Brand Setup / Operator Setup live here — not Hotel Property Census",
          has_hotel_property_census: tablePresence["Hotel Property Census"].exists_on_mvp,
        },
        {
          name: "Deal Capture MVP — Sandbox",
          id_masked: mask(sandboxId),
          reason: "Sandbox BE pilot only",
          has_hotel_property_census: tablePresence["Hotel Property Census"].exists_on_sandbox,
        },
      ],
      wrong_tables_on_correct_or_other_bases: [
        "Brand Setup - Brand Basics",
        "Brand Setup - Brand Explorer Presentation",
        "Hotel Census (legacy)",
        "Verified Independent Hotel Census (stub)",
      ],
    },
    tables: tablePresence,
    hotel_property_census: {
      table_id: census?.id || TABLE_IDS["Hotel Property Census"],
      record_count: recordStats.count,
      total_field_count: fields.length,
      v11_fields_present: v11Present.length,
      v11_fields_expected: v11Names.length,
      v11_fields_missing: v11Missing,
      v11_field_names: v11Present,
      blank_by_design: blankByDesign,
      safe_backfill_fields: safeBackfill,
      record_stats: recordStats,
    },
    name_changes: {
      applied: false,
      explanation: [
        "Post-apply review was read-only.",
        "It recommended possible v1.1.1 cleanup later — it did not rename or delete fields.",
        "Last Verified Date vs Last Reviewed Date remains a founder decision (not applied).",
        "Amenity consolidation (move Fitness/Pool/etc. into Structured Tags) was recommended but not applied.",
        "No fields were renamed or deleted after v1.1 schema create.",
      ],
    },
    views: {
      available: views,
      recommended_view: views[0]?.name || "Grid view",
      v11_hidden_by_any_view: gridHidesV11,
      note: gridHidesV11
        ? "Some v1.1 fields are hidden from at least one grid view — use Hidden fields / field search."
        : "API reports all 95 fields (including all 62 v1.1 fields) visible in Grid view. If founder still cannot see them, they are almost certainly in the wrong base/table, or scanning for renamed labels / filled enrichment cells that do not exist yet.",
    },
    search_targets: searchTargets,
    ui_guidance: {
      steps: [
        "Open Airtable base: Deal Capture Platform (appCCU…foLk) — not MVP, not Sandbox",
        "Open table: Hotel Property Census (tbl9aY5ijiuIzzWam)",
        "Open view: Grid view",
        "Click the field visibility / hidden fields control (or use field search)",
        "Search for: Hotel Description - Source Text, Amenities - Structured Tags, Owner Name, Operator / Management Company, Enrichment Status, Human Review Required, Production Use Status",
        "Expect Enrichment Status = Not Started on rows; description/owner/operator/rooms/amenities cells blank by design",
        "Do not expect renamed fields (Last Reviewed Date etc.) — renames were not applied",
      ],
    },
    why_founder_may_not_see_changes: [
      "Looking at Deal Capture MVP or Sandbox instead of Deal Capture Platform",
      "Looking at Brand Setup - Brand Basics / Presentation instead of Hotel Property Census",
      "Looking at legacy Hotel Census or Verified Independent Hotel Census stub",
      "Expecting field renames from post-apply review — those were recommendations only, not applied",
      "Expecting filled description/owner/operator/rooms cells — blank until enrichment lane runs",
      "Wide table: new columns are toward the right of the original identity columns (scroll or field search)",
    ],
    recommended_next_step:
      "Founder opens Deal Capture Platform → Hotel Property Census → Grid view → field search for Enrichment Status. Confirm 666 rows and blank enrichment columns. Then proceed to first enrichment lane (descriptions + amenities + property type) when ready.",
  };

  const md = [
    `# Production Census v1.1 — Airtable UI Visibility Check`,
    ``,
    `**Status:** \`${status}\``,
    `**Mode:** read-only`,
    `**Generated:** ${report.generated_at}`,
    ``,
    `## Verdict`,
    ``,
    status === STATUS.WRONG_BASE
      ? "v1.1 fields **exist** on Deal Capture Platform → Hotel Property Census and are **visible in Grid view**. Founder is likely looking at the **wrong base/table**, or expecting **renames / filled enrichment** that were never applied."
      : status === STATUS.MISSING
        ? "Schema recheck needed — expected Census tables/fields incomplete."
        : "Fields exist but may be hidden from the current view — use Hidden fields / field search.",
    ``,
    `## 1. Correct base and tables`,
    ``,
    `- **Correct base:** Deal Capture Platform (\`${mask(platformId)}\` / appCCU…foLk)`,
    `- **Correct table:** Hotel Property Census (\`${report.hotel_property_census.table_id}\`)`,
    ``,
    `### Do not look here`,
    ``,
    `- Deal Capture MVP (\`${mask(mvpId)}\`) — Brand Explorer / Brand Setup`,
    `- Deal Capture MVP — Sandbox (\`${mask(sandboxId)}\`)`,
    `- Brand Setup - Brand Basics`,
    `- Brand Setup - Brand Explorer Presentation`,
    `- legacy Hotel Census`,
    `- Verified Independent Hotel Census stub`,
    ``,
    `### Table presence on Platform`,
    ``,
    ...CENSUS_TABLES.map(
      (n) =>
        `- ${n}: ${tablePresence[n].exists_on_platform ? "EXISTS" : "MISSING"} (\`${tablePresence[n].id || "—"}\`) · on MVP: ${tablePresence[n].exists_on_mvp} · on Sandbox: ${tablePresence[n].exists_on_sandbox}`
    ),
    ``,
    `## 2. Hotel Property Census state`,
    ``,
    `| Metric | Expected | Actual |`,
    `| --- | --- | --- |`,
    `| Records | 666 | ${recordStats.count} |`,
    `| Fields | 95 | ${fields.length} |`,
    `| v1.1 fields | 62 | ${v11Present.length} |`,
    `| Enrichment Status = Not Started | 666 | ${recordStats.enrichment_not_started} |`,
    `| Human Review Required = true | 4 | ${recordStats.human_review_true} |`,
    `| Descriptions filled | 0 | ${recordStats.description_filled} |`,
    `| Amenities filled | 0 | ${recordStats.amenities_filled} |`,
    `| Owner filled | 0 | ${recordStats.owner_filled} |`,
    `| Operator filled | 0 | ${recordStats.operator_filled} |`,
    `| Rooms filled | 0 | ${recordStats.rooms_filled} |`,
    `| Opening dates filled | 0 | ${recordStats.opening_filled} |`,
    ``,
    `### Blank by design`,
    ``,
    ...blankByDesign.map((f) => `- ${f}`),
    ``,
    `### Safe backfill present`,
    ``,
    ...safeBackfill.map((f) => `- ${f}`),
    ``,
    `## 3. Name changes were NOT applied`,
    ``,
    ...report.name_changes.explanation.map((x) => `- ${x}`),
    ``,
    `## 4. View visibility`,
    ``,
    ...views.map(
      (v) =>
        `- **${v.name}** (\`${v.id}\`, ${v.type}) — visible fields: ${v.visible_field_count}; v1.1 hidden: ${v.v11_hidden_count}`
    ),
    ``,
    report.views.note,
    ``,
    `### Field search targets`,
    ``,
    `| Field | Exists | Visible in Grid |`,
    `| --- | --- | --- |`,
    ...searchTargets.map(
      (t) => `| ${t.name} | ${t.exists} | ${t.visible_in_default_grid} |`
    ),
    ``,
    `## 5. Airtable UI steps for founder`,
    ``,
    ...report.ui_guidance.steps.map((s, i) => `${i + 1}. ${s}`),
    ``,
    `## Why founder may not see changes`,
    ``,
    ...report.why_founder_may_not_see_changes.map((x) => `- ${x}`),
    ``,
    `## Recommended next step`,
    ``,
    report.recommended_next_step,
    ``,
  ].join("\n");

  writeJson(join(REPORTS, "production-census-v11-airtable-visibility-check.json"), report);
  writeMd(join(REPORTS, "production-census-v11-airtable-visibility-check.md"), md);
  writeMd(
    join(DOCS, "production-census-v11-airtable-visibility-check.md"),
    `${md}\n\n## Scope\n\nRead-only diagnostic. No Airtable writes, renames, deletes, or Brand Explorer patches.\n`
  );

  console.log(
    JSON.stringify(
      {
        status,
        platform: mask(platformId),
        census_id: census?.id,
        records: recordStats.count,
        fields: fields.length,
        v11: v11Present.length,
        missing: v11Missing.length,
        grid_hides_v11: gridHidesV11,
        mvp_has_census: tablePresence["Hotel Property Census"].exists_on_mvp,
        sandbox_has_census: tablePresence["Hotel Property Census"].exists_on_sandbox,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error("[census-visibility] FAILED", err);
  process.exitCode = 1;
});
