/**
 * Production Census view cleanup — field visibility/order only.
 * Meta API can READ visibleFieldIds but cannot UPDATE existing views (PATCH/PUT → 404).
 */

import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { PRODUCTION_USE_STATUS } from "./production-census-write.js";

export const VIEW_CLEANUP_VERSION = "production-census-view-cleanup-v1";

export const STATUS = Object.freeze({
  CLEANED: "production_census_views_cleaned_ready_for_field_contract_freeze",
  MANUAL: "production_census_views_manual_ui_steps_needed",
  BLOCKED: "production_census_views_cleanup_blocked",
});

/** Requested label → live Airtable field name on Hotel Property Census */
export const FIELD_ALIASES = Object.freeze({
  Brand: "Current Brand",
  "Source Family": "Family / Source Family",
});

export const OVERMODELED_AMENITY_FLAGS = Object.freeze([
  "Fitness Flag",
  "Pool Flag",
  "Parking Flag",
  "Airport Shuttle Flag",
  "Spa Flag",
  "Beach / Waterfront Flag",
]);

export const VIEW_SPECS = Object.freeze([
  {
    name: "Census - Core Identity",
    purpose: "Daily review of the core property record.",
    requested_show: [
      "Property Name",
      "Brand",
      "Affiliation Status",
      "City",
      "State / Region",
      "Country",
      "Source URL",
      "Source Family",
      "Data Eligible",
      "Data Confidence Tier",
      "Production Use Status",
      "Enrichment Status",
      "Human Review Required",
    ],
    filter: null,
  },
  {
    name: "Census - Enrichment",
    purpose: "Description, amenities, property type, and asset-context enrichment.",
    requested_show: [
      "Property Name",
      "Brand",
      "City",
      "State / Region",
      "Country",
      "Hotel Description - Source Text",
      "Hotel Description - AI Summary",
      "Amenities - Source Text",
      "Amenities - Structured Tags",
      "Property Type",
      "Asset Context",
      "Market / Submarket",
      "F&B Flag",
      "Meeting Space Flag",
      "Resort / Leisure Flag",
      "Extended Stay Flag",
      "Mixed-Use Flag",
      "Branded Residences Flag",
      "Source URL",
      "Enrichment Status",
      "Enrichment Priority",
      "Data Confidence Tier",
      "Human Review Required",
    ],
    explicitly_hide: [...OVERMODELED_AMENITY_FLAGS],
    filter: null,
  },
  {
    name: "Census - Owner Operator",
    purpose: "Later owner/operator sourcing. Keep clean and mostly empty for now.",
    requested_show: [
      "Property Name",
      "Brand",
      "City",
      "State / Region",
      "Country",
      "Owner Name",
      "Owner Confidence",
      "Operator / Management Company",
      "Operator Confidence",
      "Ownership Review Status",
      "Operator Review Status",
      "Source URL",
      "Data Confidence Tier",
      "Enrichment Status",
      "Human Review Required",
    ],
    filter: null,
  },
  {
    name: "Census - Steward Review",
    purpose: "Human review queue.",
    requested_show: [
      "Property Name",
      "Brand",
      "Affiliation Status",
      "City",
      "State / Region",
      "Country",
      "Human Review Required",
      "Brand-Unassigned Reason",
      "Notes for Steward",
      "Enrichment Priority",
      "Data Confidence Tier",
      "Source URL",
      "Production Use Status",
      "Enrichment Status",
    ],
    filter: {
      field: "Human Review Required",
      op: "is checked",
      manual_instruction: "Filter Census - Steward Review where Human Review Required is checked.",
      expected_record_count: 4,
    },
  },
]);

function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function resolveFieldName(requested) {
  return FIELD_ALIASES[requested] || requested;
}

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

async function listTables(baseId, token, includeVisible = false) {
  const q = includeVisible ? "?include%5B%5D=visibleFieldIds" : "";
  const { res, json } = await metaFetch(baseId, token, `/tables${q}`);
  if (!res.ok) throw new Error(`meta tables ${res.status}`);
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
    if (!res.ok) throw new Error(`list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(100);
  } while (offset);
  return out;
}

export async function probeViewUpdateSupport(baseId, token, tableId, viewId, sampleFieldIds) {
  const probes = [];
  for (const method of ["PATCH", "PUT"]) {
    const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/views/${viewId}`, {
      method,
      body: JSON.stringify({ visibleFieldIds: sampleFieldIds.slice(0, 2) }),
    });
    probes.push({
      method,
      path: `/tables/${tableId}/views/${viewId}`,
      status: res.status,
      error: json.error || json,
    });
  }
  const supported = probes.some((p) => p.status >= 200 && p.status < 300);
  return {
    supported,
    probes,
    reason: supported
      ? null
      : "Airtable Meta API has no supported endpoint to update existing view visibleFieldIds / field order / filters (PATCH/PUT return 404).",
  };
}

function buildViewPlan(spec, fieldByName, allFieldNames) {
  const show = [];
  const missing = [];
  const aliases_applied = [];
  for (const requested of spec.requested_show) {
    const resolved = resolveFieldName(requested);
    if (requested !== resolved) aliases_applied.push({ requested, resolved });
    const field = fieldByName[resolved];
    if (!field) missing.push({ requested, resolved });
    else show.push({ requested, resolved, field_id: field.id });
  }
  const showNames = new Set(show.map((s) => s.resolved));
  const hide = allFieldNames.filter((n) => !showNames.has(n));
  return {
    name: spec.name,
    purpose: spec.purpose,
    show_ordered: show.map((s) => s.resolved),
    show_detail: show,
    hide,
    hide_count: hide.length,
    explicitly_hide: spec.explicitly_hide || [],
    aliases_applied,
    missing_fields: missing,
    filter: spec.filter,
  };
}

function manualUiStepsForView(plan) {
  const steps = [
    `Open Deal Capture Platform → Hotel Property Census → view “${plan.name}”.`,
    `Open the Hide fields control (or right-click column headers).`,
    `Hide every field except the ${plan.show_ordered.length} listed below (or Hide all, then unhide only these).`,
    `Drag columns left-to-right into this exact order:`,
    ...plan.show_ordered.map((n, i) => `  ${i + 1}. ${n}`),
  ];
  if (plan.explicitly_hide?.length) {
    steps.push(
      `Confirm these over-modeled amenity flags stay hidden: ${plan.explicitly_hide.join(", ")}.`
    );
  }
  if (plan.filter) {
    steps.push(`Filter: ${plan.filter.manual_instruction}`);
    steps.push(
      `Expected after filter: ${plan.filter.expected_record_count} held records (Human Review Required checked).`
    );
  }
  return steps;
}

export async function runProductionCensusViewCleanup() {
  const started = Date.now();
  const token = resolvePat();
  const bases = resolveTargetBase();

  const tables = await listTables(bases.target_base_id, token, true);
  const census = tables.find((t) => t.name === "Hotel Property Census");
  if (!census) {
    return {
      version: VIEW_CLEANUP_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      blocked_reason: "Hotel Property Census table not found on Platform base",
    };
  }

  const fieldByName = Object.fromEntries((census.fields || []).map((f) => [f.name, f]));
  const allFieldNames = (census.fields || []).map((f) => f.name);
  const viewsByName = Object.fromEntries((census.views || []).map((v) => [v.name, v]));

  const viewsFound = [];
  const viewsMissing = [];
  for (const spec of VIEW_SPECS) {
    const v = viewsByName[spec.name];
    if (!v) viewsMissing.push(spec.name);
    else {
      viewsFound.push({
        id: v.id,
        name: v.name,
        type: v.type,
        visible_field_count: v.visibleFieldIds?.length ?? null,
      });
    }
  }

  if (viewsMissing.length) {
    return {
      version: VIEW_CLEANUP_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      blocked_reason: "One or more required views missing",
      views_missing: viewsMissing,
      views_found: viewsFound,
      base_id_masked: mask(bases.target_base_id),
      table_id: census.id,
    };
  }

  const sampleView = viewsByName[VIEW_SPECS[0].name];
  const sampleFieldIds = [fieldByName["Property Name"]?.id, fieldByName["Current Brand"]?.id].filter(
    Boolean
  );
  const apiProbe = await probeViewUpdateSupport(
    bases.target_base_id,
    token,
    census.id,
    sampleView.id,
    sampleFieldIds
  );

  const viewPlans = VIEW_SPECS.map((spec) => buildViewPlan(spec, fieldByName, allFieldNames));
  const anyMissingFields = viewPlans.some((p) => p.missing_fields.length > 0);
  if (anyMissingFields) {
    return {
      version: VIEW_CLEANUP_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      blocked_reason: "One or more required show-fields missing from schema",
      view_plans: viewPlans,
      api_probe: apiProbe,
    };
  }

  // Census validation (read-only)
  const validationFields = [
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
    "Latitude",
    "Longitude",
    ...OVERMODELED_AMENITY_FLAGS,
  ];
  const rows = await listAll(bases.target_base_id, token, census.id, validationFields);

  const amenityFilled = {};
  for (const n of OVERMODELED_AMENITY_FLAGS) amenityFilled[n] = 0;
  for (const row of rows) {
    for (const n of OVERMODELED_AMENITY_FLAGS) {
      if (row.fields?.[n] === true) amenityFilled[n] += 1;
    }
  }

  const names = new Set(allFieldNames);
  const validation = {
    record_count: rows.length,
    field_count: census.fields.length,
    duplicates: (() => {
      const m = new Map();
      for (const r of rows) {
        const k = r.fields?.["Property Identity Key"];
        m.set(k, (m.get(k) || 0) + 1);
      }
      return [...m.values()].filter((n) => n > 1).length;
    })(),
    v111_renames_present: {
      "Last Reviewed Date": names.has("Last Reviewed Date"),
      "Resort / Leisure Flag": names.has("Resort / Leisure Flag"),
      "Extended Stay Flag": names.has("Extended Stay Flag"),
    },
    old_names_absent: {
      "Last Verified Date": !names.has("Last Verified Date"),
      "Resort Amenities Flag": !names.has("Resort Amenities Flag"),
      "Extended Stay Amenity Flag": !names.has("Extended Stay Amenity Flag"),
    },
    overmodeled_still_exist: OVERMODELED_AMENITY_FLAGS.every((n) => names.has(n)),
    amenity_filled: amenityFilled,
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
    zero_zero: rows.filter((r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0).length,
    no_schema_writes: true,
    no_record_writes: true,
  };

  validation.pass =
    validation.record_count === 666 &&
    validation.field_count === 95 &&
    validation.duplicates === 0 &&
    Object.values(validation.v111_renames_present).every(Boolean) &&
    Object.values(validation.old_names_absent).every(Boolean) &&
    validation.overmodeled_still_exist &&
    validation.enrichment_not_started === 666 &&
    validation.human_review_true === 4 &&
    validation.production_use_ok === 666 &&
    validation.description_filled === 0 &&
    validation.amenities_filled === 0 &&
    validation.owner_filled === 0 &&
    validation.operator_filled === 0 &&
    validation.rooms_filled === 0 &&
    validation.opening_filled === 0 &&
    validation.zero_zero === 0 &&
    Object.values(amenityFilled).every((n) => n === 0);

  const appliedViaApi = false;
  const viewOrderApplied = false;
  const stewardFilterApplied = false;

  const manual_ui_steps = viewPlans.map((plan) => ({
    view: plan.name,
    steps: manualUiStepsForView(plan),
  }));

  // General UI tip once
  const general_manual_howto = [
    "In Airtable: open the view → click Hide fields (eye icon / field visibility).",
    "Prefer: Hide all → then toggle on only the listed fields in order (drag to reorder after unhiding).",
    "Do not delete fields from the table. Hiding is view-local only.",
    "Do not change filters on Core Identity / Enrichment / Owner Operator unless a filter already exists.",
    "For Steward Review only: Filter where Human Review Required is checked (expect 4 records).",
  ];

  let status = STATUS.MANUAL;
  if (!validation.pass) status = STATUS.BLOCKED;
  else if (appliedViaApi && viewOrderApplied) status = STATUS.CLEANED;

  return {
    version: VIEW_CLEANUP_VERSION,
    generated_at: new Date().toISOString(),
    status,
    duration_ms: Date.now() - started,
    token_masked: mask(token),
    base_id_masked: mask(bases.target_base_id),
    table_id: census.id,
    table_name: "Hotel Property Census",
    field_aliases: FIELD_ALIASES,
    api_view_update: {
      supported: apiProbe.supported,
      applied: appliedViaApi,
      view_order_applied: viewOrderApplied,
      steward_filter_applied: stewardFilterApplied,
      probe: apiProbe,
    },
    views_found: viewsFound,
    views_missing: viewsMissing,
    views: viewPlans.map((p) => ({
      name: p.name,
      purpose: p.purpose,
      fields_shown_ordered: p.show_ordered,
      fields_hidden: p.hide,
      fields_hidden_count: p.hide_count,
      explicitly_hide_overmodeled: p.explicitly_hide,
      aliases_applied: p.aliases_applied,
      filter: p.filter,
      current_visible_count: viewsByName[p.name]?.visibleFieldIds?.length ?? null,
      currently_matches_target:
        (() => {
          const visible = viewsByName[p.name]?.visibleFieldIds || [];
          const targetIds = p.show_detail.map((s) => s.field_id);
          if (visible.length !== targetIds.length) return false;
          return targetIds.every((id, i) => visible[i] === id);
        })(),
    })),
    general_manual_howto,
    manual_ui_steps,
    validation,
    final_recommendation:
      "Complete the manual Hide fields + column order steps for all four views (and Steward Review filter). Then freeze the Census field contract and begin descriptions + amenities enrichment.",
  };
}

export function renderViewCleanupMarkdown(r) {
  const lines = [
    `# Production Census View Cleanup`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Generated:** ${r.generated_at}`,
    `**Base:** Deal Capture Platform (\`${r.base_id_masked}\`)`,
    `**Table:** Hotel Property Census (\`${r.table_id}\`)`,
    ``,
    `## 1. Executive summary`,
    ``,
    `- Views found: ${(r.views_found || []).length}`,
    `- Views missing: ${(r.views_missing || []).length}`,
    `- API view visibility/order update supported: ${r.api_view_update?.supported}`,
    `- View order applied via API: ${r.api_view_update?.view_order_applied}`,
    `- Steward filter applied via API: ${r.api_view_update?.steward_filter_applied}`,
    `- Census validation pass: ${r.validation?.pass}`,
    `- Field aliases: Brand → Current Brand; Source Family → Family / Source Family`,
    ``,
    `## 2. Views found`,
    ``,
    "```json",
    JSON.stringify(r.views_found || [], null, 2),
    "```",
    ``,
  ];

  if (r.views_missing?.length) {
    lines.push(`**Missing:** ${(r.views_missing || []).join(", ")}`, ``);
  }

  lines.push(`## 3–4. Fields shown / hidden per view`, ``);
  for (const v of r.views || []) {
    lines.push(
      `### ${v.name}`,
      ``,
      `Purpose: ${v.purpose}`,
      ``,
      `**Show (${v.fields_shown_ordered?.length}), ordered:**`,
      ...(v.fields_shown_ordered || []).map((n, i) => `${i + 1}. ${n}`),
      ``,
      `**Hidden:** ${v.fields_hidden_count} fields (all others on the table).`,
      v.explicitly_hide_overmodeled?.length
        ? `**Explicitly keep hidden (over-modeled):** ${v.explicitly_hide_overmodeled.join(", ")}`
        : "",
      v.aliases_applied?.length
        ? `**Aliases:** ${v.aliases_applied.map((a) => `${a.requested} → ${a.resolved}`).join("; ")}`
        : "",
      `**Currently matches target visibility/order:** ${v.currently_matches_target}`,
      ``
    );
  }

  lines.push(
    `## 5. Whether view order was applied`,
    ``,
    `- Applied via API: **${r.api_view_update?.view_order_applied}**`,
    `- Reason: ${r.api_view_update?.probe?.reason || "n/a"}`,
    ``,
    `## 6. Whether Steward Review filter was applied`,
    ``,
    `- Applied via API: **${r.api_view_update?.steward_filter_applied}**`,
    `- Manual: Filter Census - Steward Review where Human Review Required is checked.`,
    `- Expected records after filter: **4**`,
    ``,
    `## 7. Manual UI steps needed`,
    ``,
    `### General`,
    ``,
    ...(r.general_manual_howto || []).map((s) => `- ${s}`),
    ``
  );

  for (const block of r.manual_ui_steps || []) {
    lines.push(`### ${block.view}`, ``, ...(block.steps || []).map((s) => `- ${s}`), ``);
  }

  lines.push(
    `## 8. Census validation`,
    ``,
    "```json",
    JSON.stringify(r.validation || {}, null, 2),
    "```",
    ``,
    `## 9. Brand Explorer safety result`,
    ``,
    "```json",
    JSON.stringify(r.brand_explorer_safety || { pending: true }, null, 2),
    "```",
    ``,
    `## 10. Final recommendation`,
    ``,
    r.final_recommendation || "",
    ``
  );

  return lines.filter((x) => x !== "").join("\n");
}
