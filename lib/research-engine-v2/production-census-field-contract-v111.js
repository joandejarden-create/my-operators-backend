/**
 * Freeze Production Census field contract v1.1.1 (read-only).
 * No Airtable writes. No Brand Explorer writes.
 */

import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { PRODUCTION_USE_STATUS } from "./production-census-write.js";

export const CONTRACT_VERSION = "production-census-field-contract-v111-v1";
export const CONTRACT_ID = "production_census_field_contract_v1.1.1";

export const STATUS = Object.freeze({
  FROZEN_READY: "production_census_field_contract_v111_frozen_ready_for_enrichment",
  FROZEN_RADAR_MISSING: "production_census_field_contract_v111_frozen_radar_fields_missing_v112_needed",
  HOLD: "production_census_field_contract_v111_hold_before_enrichment",
});

export const RADAR_PUBLIC_FIELDS = Object.freeze([
  "Radar Display Status",
  "Radar Display Reason",
  "Radar Geography Status",
  "Public Census Eligibility",
  "Public Display Confidence",
  "Public Display Review Status",
]);

export const FIRST_ENRICHMENT_ALLOWED = Object.freeze([
  "Hotel Description - Source Text",
  "Hotel Description - AI Summary",
  "Amenities - Source Text",
  "Amenities - Structured Tags",
  "Property Type",
  "Asset Context",
  "Market / Submarket",
  "Resort / Leisure Flag",
  "Extended Stay Flag",
  "F&B Flag",
  "Meeting Space Flag",
  "Mixed-Use Flag",
  "Branded Residences Flag",
]);

export const FIRST_ENRICHMENT_BLOCKED = Object.freeze([
  "Owner Name",
  "Developer Name",
  "Operator / Management Company",
  "Rooms / Keys",
  "Opening Date",
  "Renovation / Conversion Date",
  "Affiliation Start Date",
  // Brand Explorer / public product writes
  "Brand Explorer public fields",
  "Recent Momentum",
  "Company Validated",
  "Brand Verified",
]);

export const OVERMODELED_AMENITY_FLAGS = Object.freeze([
  "Fitness Flag",
  "Pool Flag",
  "Parking Flag",
  "Airport Shuttle Flag",
  "Spa Flag",
  "Beach / Waterfront Flag",
]);

/**
 * Contract classification tags (multi-tag allowed via primary + tags).
 * primary is the main contract role; tags add usage constraints.
 */
function entry(name, group, primary, tags = [], notes = "") {
  return { name, group, primary, tags, notes };
}

/**
 * Frozen field contract for all Hotel Property Census fields (excl. inventing Radar fields).
 * Link-inverse fields included as internal_only.
 */
export function buildFieldContractEntries() {
  return [
    // A. Core Identity
    entry("Property Name", "A. Core Identity", "contract_required", ["public_safe_if_approved"]),
    entry("Canonical Property Name", "A. Core Identity", "contract_optional", ["public_safe_if_approved"]),
    entry("Property Identity Key", "A. Core Identity", "contract_required", ["internal_only", "source_only"]),
    entry("Phone", "A. Core Identity", "contract_optional", ["do_not_use_publicly_yet"]),
    entry("Official Property URL", "A. Core Identity", "contract_optional", ["public_safe_if_approved"]),
    entry("Future Opening Flag", "A. Core Identity", "contract_optional", ["do_not_use_publicly_yet"]),

    // B. Geography
    entry("Country", "B. Geography", "contract_required", ["public_safe_if_approved"]),
    entry("State / Region", "B. Geography", "contract_required", ["public_safe_if_approved"]),
    entry("City", "B. Geography", "contract_required", ["public_safe_if_approved"]),
    entry("Address", "B. Geography", "contract_optional", ["do_not_use_publicly_yet"]),
    entry("Latitude", "B. Geography", "contract_optional", [
      "public_safe_if_approved",
      "do_not_use_publicly_yet",
    ], "Present for many rows; never invent 0,0"),
    entry("Longitude", "B. Geography", "contract_optional", [
      "public_safe_if_approved",
      "do_not_use_publicly_yet",
    ], "Present for many rows; never invent 0,0"),
    entry("Market / Submarket", "B. Geography", "enrichment_target", [
      "public_safe_if_approved",
    ], "First enrichment lane — Dealality geography, not STR"),

    // C. Brand / Affiliation
    entry("Current Brand", "C. Brand / Affiliation", "contract_required", ["public_safe_if_approved"]),
    entry("Brand Family", "C. Brand / Affiliation", "contract_optional", ["public_safe_if_approved"]),
    entry("Brand Explorer Slug if mapped", "C. Brand / Affiliation", "internal_only", [
      "do_not_use_publicly_yet",
    ]),
    entry("Affiliation Status", "C. Brand / Affiliation", "contract_required", ["public_safe_if_approved"]),
    entry("Affiliation As-Of Date", "C. Brand / Affiliation", "contract_optional", ["source_only"]),
    entry("Affiliation Start Date", "C. Brand / Affiliation", "contract_optional", [
      "source_only",
      "do_not_use_publicly_yet",
    ], "Blocked in first enrichment lane"),
    entry("Prior Brand", "C. Brand / Affiliation", "contract_optional", ["do_not_use_publicly_yet"]),
    entry("Brand Confidence", "C. Brand / Affiliation", "governance_field", ["internal_only"]),

    // D. Source Evidence
    entry("Family / Source Family", "D. Source Evidence", "contract_required", ["source_only", "internal_only"]),
    entry("Source URL", "D. Source Evidence", "contract_required", ["source_only"]),
    entry("Source Type", "D. Source Evidence", "contract_optional", ["source_only", "internal_only"]),
    entry("Source Confidence", "D. Source Evidence", "governance_field", ["source_only", "internal_only"]),
    entry("Discovery Date", "D. Source Evidence", "contract_optional", ["source_only", "internal_only"]),
    entry("VIC Freeze Hash", "D. Source Evidence", "contract_required", ["source_only", "internal_only"]),
    entry("Hotel Property Source Evidence", "D. Source Evidence", "internal_only", [], "Link inverse"),

    // E. Radar / Public Display (v1.1.2 placeholders — not on table yet)
    // Documented separately when missing; not invented here.

    // F. Description / Amenities
    entry("Hotel Description - Source Text", "F. Description / Amenities", "enrichment_target", [
      "source_only",
      "public_safe_if_approved",
    ], "First enrichment lane"),
    entry("Hotel Description - AI Summary", "F. Description / Amenities", "enrichment_target", [
      "public_safe_if_approved",
    ], "First enrichment lane — governed AI only"),
    entry("Short Property Summary", "F. Description / Amenities", "enrichment_target", [
      "do_not_use_publicly_yet",
    ], "Not in first lane"),
    entry("Property Positioning", "F. Description / Amenities", "enrichment_target", [
      "do_not_use_publicly_yet",
    ], "Not in first lane"),
    entry("Amenities - Source Text", "F. Description / Amenities", "enrichment_target", [
      "source_only",
      "public_safe_if_approved",
    ], "First enrichment lane"),
    entry("Amenities - Structured Tags", "F. Description / Amenities", "enrichment_target", [
      "public_safe_if_approved",
    ], "First enrichment lane"),
    entry("F&B Flag", "F. Description / Amenities", "enrichment_target", ["public_safe_if_approved"], "First enrichment lane"),
    entry("Meeting Space Flag", "F. Description / Amenities", "enrichment_target", [
      "public_safe_if_approved",
    ], "First enrichment lane"),
    entry("Resort / Leisure Flag", "F. Description / Amenities", "enrichment_target", [
      "public_safe_if_approved",
    ], "First enrichment lane"),
    entry("Extended Stay Flag", "F. Description / Amenities", "enrichment_target", [
      "public_safe_if_approved",
    ], "First enrichment lane"),
    entry("Branded Residences Flag", "F. Description / Amenities", "enrichment_target", [
      "public_safe_if_approved",
    ], "First enrichment lane"),
    entry("Mixed-Use Flag", "F. Description / Amenities", "enrichment_target", [
      "public_safe_if_approved",
    ], "First enrichment lane"),
    entry("Fitness Flag", "F. Description / Amenities", "contract_optional", [
      "do_not_use_publicly_yet",
      "internal_only",
    ], "Over-modeled — keep hidden; do not populate in first lane"),
    entry("Pool Flag", "F. Description / Amenities", "contract_optional", [
      "do_not_use_publicly_yet",
      "internal_only",
    ], "Over-modeled — keep hidden; do not populate in first lane"),
    entry("Parking Flag", "F. Description / Amenities", "contract_optional", [
      "do_not_use_publicly_yet",
      "internal_only",
    ], "Over-modeled — keep hidden; do not populate in first lane"),
    entry("Airport Shuttle Flag", "F. Description / Amenities", "contract_optional", [
      "do_not_use_publicly_yet",
      "internal_only",
    ], "Over-modeled — keep hidden; do not populate in first lane"),
    entry("Spa Flag", "F. Description / Amenities", "contract_optional", [
      "do_not_use_publicly_yet",
      "internal_only",
    ], "Over-modeled — keep hidden; do not populate in first lane"),
    entry("Beach / Waterfront Flag", "F. Description / Amenities", "contract_optional", [
      "do_not_use_publicly_yet",
      "internal_only",
    ], "Over-modeled — keep hidden; do not populate in first lane"),

    // G. Asset Context
    entry("Hotel Class / Segment", "G. Asset Context", "enrichment_target", [
      "do_not_use_publicly_yet",
    ], "Not in first lane"),
    entry("Property Type", "G. Asset Context", "enrichment_target", ["public_safe_if_approved"], "First enrichment lane"),
    entry("Asset Context", "G. Asset Context", "enrichment_target", ["public_safe_if_approved"], "First enrichment lane"),
    entry("Rooms / Keys", "G. Asset Context", "enrichment_target", [
      "source_only",
      "do_not_use_publicly_yet",
    ], "Blocked in first enrichment lane"),
    entry("Rooms Source URL", "G. Asset Context", "source_only", ["do_not_use_publicly_yet"]),
    entry("Rooms Confidence", "G. Asset Context", "governance_field", ["internal_only"]),
    entry("Building / Asset Notes", "G. Asset Context", "enrichment_target", ["do_not_use_publicly_yet"]),
    entry("Opening Date", "G. Asset Context", "enrichment_target", [
      "source_only",
      "do_not_use_publicly_yet",
    ], "Blocked in first enrichment lane"),
    entry("Opening Date Source URL", "G. Asset Context", "source_only", ["do_not_use_publicly_yet"]),
    entry("Renovation / Conversion Status", "G. Asset Context", "enrichment_target", [
      "do_not_use_publicly_yet",
    ]),
    entry("Renovation / Conversion Date", "G. Asset Context", "enrichment_target", [
      "source_only",
      "do_not_use_publicly_yet",
    ], "Blocked in first enrichment lane"),
    entry("Renovation / Conversion Source URL", "G. Asset Context", "source_only", [
      "do_not_use_publicly_yet",
    ]),

    // H. Owner / Developer
    entry("Owner Name", "H. Owner / Developer", "enrichment_target", [
      "source_only",
      "do_not_use_publicly_yet",
    ], "Blocked in first enrichment lane"),
    entry("Owner Type", "H. Owner / Developer", "enrichment_target", ["do_not_use_publicly_yet"]),
    entry("Owner Source URL", "H. Owner / Developer", "source_only", ["do_not_use_publicly_yet"]),
    entry("Owner Confidence", "H. Owner / Developer", "governance_field", ["internal_only"]),
    entry("Developer Name", "H. Owner / Developer", "enrichment_target", [
      "source_only",
      "do_not_use_publicly_yet",
    ], "Blocked in first enrichment lane"),
    entry("Developer Source URL", "H. Owner / Developer", "source_only", ["do_not_use_publicly_yet"]),
    entry("Developer Confidence", "H. Owner / Developer", "governance_field", ["internal_only"]),
    entry("Ownership Review Status", "H. Owner / Developer", "governance_field", ["internal_only"]),

    // I. Operator / Management
    entry("Operator / Management Company", "I. Operator / Management", "enrichment_target", [
      "source_only",
      "do_not_use_publicly_yet",
    ], "Blocked in first enrichment lane"),
    entry("Operator Type", "I. Operator / Management", "enrichment_target", ["do_not_use_publicly_yet"]),
    entry("Management Model", "I. Operator / Management", "enrichment_target", ["do_not_use_publicly_yet"]),
    entry("Operator Source URL", "I. Operator / Management", "source_only", ["do_not_use_publicly_yet"]),
    entry("Operator Confidence", "I. Operator / Management", "governance_field", ["internal_only"]),
    entry("Operator Review Status", "I. Operator / Management", "governance_field", ["internal_only"]),
    entry("Possible Operator Target", "I. Operator / Management", "internal_only", [
      "do_not_use_publicly_yet",
    ]),

    // J. Independent / Brand-Unconfirmed Handling
    entry("Independent Hotel Flag", "J. Independent / Brand-Unconfirmed Handling", "contract_optional", [
      "do_not_use_publicly_yet",
    ]),
    entry("Independent Classification", "J. Independent / Brand-Unconfirmed Handling", "contract_optional", [
      "do_not_use_publicly_yet",
    ]),
    entry("Brand-Unassigned Reason", "J. Independent / Brand-Unconfirmed Handling", "governance_field", [
      "internal_only",
    ]),
    entry("Possible Soft-Brand Candidate", "J. Independent / Brand-Unconfirmed Handling", "internal_only", [
      "do_not_use_publicly_yet",
    ]),
    entry("Possible Brand Conversion Candidate", "J. Independent / Brand-Unconfirmed Handling", "internal_only", [
      "do_not_use_publicly_yet",
    ]),
    entry("Possible Owner Outreach Target", "J. Independent / Brand-Unconfirmed Handling", "internal_only", [
      "do_not_use_publicly_yet",
    ]),
    entry("Possible Financing Target", "J. Independent / Brand-Unconfirmed Handling", "internal_only", [
      "do_not_use_publicly_yet",
    ]),
    entry("Possible Dealality Opportunity", "J. Independent / Brand-Unconfirmed Handling", "internal_only", [
      "do_not_use_publicly_yet",
    ]),

    // K. Enrichment Governance
    entry("Data Eligible", "K. Enrichment Governance", "governance_field", ["internal_only"]),
    entry("Identity Confidence", "K. Enrichment Governance", "governance_field", ["internal_only"]),
    entry("Production Use Status", "K. Enrichment Governance", "governance_field", [
      "contract_required",
      "internal_only",
    ], "Must remain Census Only / Not Owner-Facing until product approval"),
    entry("Data Confidence Tier", "K. Enrichment Governance", "governance_field", ["internal_only"]),
    entry("Relationship Confidence", "K. Enrichment Governance", "governance_field", ["internal_only"]),
    entry("Last Reviewed Date", "K. Enrichment Governance", "governance_field", ["internal_only"]),
    entry("Next Review Needed", "K. Enrichment Governance", "governance_field", ["internal_only"]),
    entry("Enrichment Status", "K. Enrichment Governance", "governance_field", ["contract_required"]),
    entry("Enrichment Priority", "K. Enrichment Governance", "governance_field", ["internal_only"]),

    // L. Steward Review
    entry("Steward Review Status", "L. Steward Review", "governance_field", ["internal_only"]),
    entry("Human Review Required", "L. Steward Review", "governance_field", ["contract_required"]),
    entry("Notes for Steward", "L. Steward Review", "governance_field", ["internal_only"]),
    entry("Hotel Property Brand Affiliations", "L. Steward Review", "internal_only", [], "Link inverse"),
    entry("Hotel Property Steward Review", "L. Steward Review", "internal_only", [], "Link inverse"),
  ];
}

function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function listTables(baseId, token) {
  const res = await fetch(
    `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}/tables?include%5B%5D=visibleFieldIds`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json();
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

export async function runFieldContractFreeze() {
  const started = Date.now();
  const token = resolvePat();
  const bases = resolveTargetBase();
  const tables = await listTables(bases.target_base_id, token);
  const census = tables.find((t) => t.name === "Hotel Property Census");
  if (!census) {
    return {
      version: CONTRACT_VERSION,
      contract_id: CONTRACT_ID,
      generated_at: new Date().toISOString(),
      status: STATUS.HOLD,
      hold_reason: "Hotel Property Census not found",
    };
  }

  const liveNames = (census.fields || []).map((f) => f.name);
  const liveSet = new Set(liveNames);
  const contractEntries = buildFieldContractEntries();
  const contractNames = new Set(contractEntries.map((e) => e.name));

  const unlistedLive = liveNames.filter((n) => !contractNames.has(n));
  const missingFromLive = contractEntries.filter((e) => !liveSet.has(e.name)).map((e) => e.name);

  const radar = {
    required_for_radar_integration: [...RADAR_PUBLIC_FIELDS],
    present: RADAR_PUBLIC_FIELDS.filter((n) => liveSet.has(n)),
    missing: RADAR_PUBLIC_FIELDS.filter((n) => !liveSet.has(n)),
    all_present: RADAR_PUBLIC_FIELDS.every((n) => liveSet.has(n)),
    v112_needed: !RADAR_PUBLIC_FIELDS.every((n) => liveSet.has(n)),
    blocks_first_enrichment: false,
    note:
      "Radar/public display fields are absent. Do not invent them. Schedule schema v1.1.2 before Radar integration. Description/amenity enrichment may proceed under this freeze.",
  };

  const rows = await listAll(bases.target_base_id, token, census.id, [
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
    "Renovation / Conversion Date",
    "Affiliation Start Date",
    "Latitude",
    "Longitude",
  ]);

  const keyCounts = new Map();
  for (const r of rows) {
    const k = r.fields?.["Property Identity Key"];
    keyCounts.set(k, (keyCounts.get(k) || 0) + 1);
  }

  const validation = {
    base: "Deal Capture Platform",
    table: "Hotel Property Census",
    table_id: census.id,
    record_count: rows.length,
    field_count: census.fields.length,
    duplicates: [...keyCounts.values()].filter((n) => n > 1).length,
    production_use_ok: rows.filter((r) => r.fields?.["Production Use Status"] === PRODUCTION_USE_STATUS)
      .length,
    enrichment_not_started: rows.filter((r) => r.fields?.["Enrichment Status"] === "Not Started").length,
    human_review_true: rows.filter((r) => r.fields?.["Human Review Required"] === true).length,
    description_filled: rows.filter((r) => Boolean(r.fields?.["Hotel Description - Source Text"]))
      .length,
    amenities_filled: rows.filter((r) => Boolean(r.fields?.["Amenities - Source Text"])).length,
    owner_filled: rows.filter((r) => Boolean(r.fields?.["Owner Name"])).length,
    operator_filled: rows.filter((r) => Boolean(r.fields?.["Operator / Management Company"]))
      .length,
    rooms_filled: rows.filter((r) => r.fields?.["Rooms / Keys"] != null).length,
    opening_filled: rows.filter((r) => Boolean(r.fields?.["Opening Date"])).length,
    renovation_filled: rows.filter((r) => Boolean(r.fields?.["Renovation / Conversion Date"]))
      .length,
    affiliation_start_filled: rows.filter((r) => Boolean(r.fields?.["Affiliation Start Date"]))
      .length,
    zero_zero: rows.filter((r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0).length,
    renames_present: {
      "Last Reviewed Date": liveSet.has("Last Reviewed Date"),
      "Resort / Leisure Flag": liveSet.has("Resort / Leisure Flag"),
      "Extended Stay Flag": liveSet.has("Extended Stay Flag"),
    },
    old_names_absent: {
      "Last Verified Date": !liveSet.has("Last Verified Date"),
      "Resort Amenities Flag": !liveSet.has("Resort Amenities Flag"),
      "Extended Stay Amenity Flag": !liveSet.has("Extended Stay Amenity Flag"),
    },
    views: (census.views || []).map((v) => ({
      name: v.name,
      visible_field_count: v.visibleFieldIds?.length ?? null,
    })),
  };

  validation.pass =
    validation.record_count === 666 &&
    validation.field_count === 95 &&
    validation.duplicates === 0 &&
    validation.production_use_ok === 666 &&
    validation.enrichment_not_started === 666 &&
    validation.human_review_true === 4 &&
    validation.description_filled === 0 &&
    validation.amenities_filled === 0 &&
    validation.owner_filled === 0 &&
    validation.operator_filled === 0 &&
    validation.rooms_filled === 0 &&
    validation.opening_filled === 0 &&
    validation.renovation_filled === 0 &&
    validation.affiliation_start_filled === 0 &&
    validation.zero_zero === 0 &&
    Object.values(validation.renames_present).every(Boolean) &&
    Object.values(validation.old_names_absent).every(Boolean) &&
    missingFromLive.length === 0 &&
    unlistedLive.length === 0;

  const groups = {};
  for (const e of contractEntries) {
    if (!groups[e.group]) groups[e.group] = [];
    groups[e.group].push(e);
  }

  const byPrimary = (primary) => contractEntries.filter((e) => e.primary === primary).map((e) => e.name);
  const withTag = (tag) =>
    contractEntries.filter((e) => e.tags.includes(tag) || e.primary === tag).map((e) => e.name);

  const steward_rules = {
    human_review_required_true_count_expected: 4,
    human_review_required_true_count_actual: validation.human_review_true,
    steward_view: "Census - Steward Review",
    steward_filter: "Human Review Required is checked",
    rules: [
      "Do not clear Human Review Required on held records without steward decision.",
      "Use Notes for Steward and Brand-Unassigned Reason for hold rationale.",
      "Enrichment may run on Data Eligible rows with Enrichment Status = Not Started, but held records stay in Steward Review queue.",
      "Production Use Status must remain Census Only / Not Owner-Facing until explicit product approval.",
      "Never write Brand Explorer Company Validated / Brand Verified / Recent Momentum from Census enrichment.",
    ],
  };

  let status = STATUS.FROZEN_READY;
  if (!validation.pass) status = STATUS.HOLD;
  else if (radar.v112_needed) status = STATUS.FROZEN_RADAR_MISSING;

  return {
    version: CONTRACT_VERSION,
    contract_id: CONTRACT_ID,
    generated_at: new Date().toISOString(),
    status,
    duration_ms: Date.now() - started,
    read_only: true,
    airtable_writes: false,
    brand_explorer_writes: false,
    token_masked: mask(token),
    base_id_masked: mask(bases.target_base_id),
    table_id: census.id,
    validation,
    contract_coverage: {
      live_field_count: liveNames.length,
      contract_entry_count: contractEntries.length,
      unlisted_live_fields: unlistedLive,
      missing_from_live: missingFromLive,
    },
    field_groups: groups,
    fields_flat: contractEntries,
    required_fields: byPrimary("contract_required").concat(
      contractEntries.filter((e) => e.tags.includes("contract_required")).map((e) => e.name)
    ).filter((v, i, a) => a.indexOf(v) === i),
    optional_fields: byPrimary("contract_optional"),
    enrichment_target_fields: byPrimary("enrichment_target"),
    governance_fields: byPrimary("governance_field"),
    source_only_fields: withTag("source_only"),
    internal_only_fields: withTag("internal_only"),
    public_safe_if_approved_fields: withTag("public_safe_if_approved"),
    do_not_use_publicly_yet_fields: withTag("do_not_use_publicly_yet"),
    radar_public_display: radar,
    first_enrichment_lane: {
      allowed: [...FIRST_ENRICHMENT_ALLOWED],
      blocked: [...FIRST_ENRICHMENT_BLOCKED],
      overmodeled_amenity_flags_not_in_first_lane: [...OVERMODELED_AMENITY_FLAGS],
      rules: [
        "Source-backed writes only — never invent descriptions, amenities, or flags.",
        "AI Summary only after Source Text exists and governance rules are followed.",
        "Do not populate over-modeled amenity flags in first lane.",
        "Do not touch owner / developer / operator / rooms / dates in first lane.",
        "Do not patch Brand Explorer or change Brand Status.",
        "Radar fields missing do not block this lane; Radar integration waits for v1.1.2.",
      ],
    },
    steward_rules,
    applied_renames_frozen: {
      "Last Verified Date": "Last Reviewed Date",
      "Resort Amenities Flag": "Resort / Leisure Flag",
      "Extended Stay Amenity Flag": "Extended Stay Flag",
    },
    kept_unchanged_frozen: [
      "Rooms / Keys",
      "Operator / Management Company",
      "Owner Name",
      "Source URL",
      "State / Region",
    ],
    final_recommendation:
      status === STATUS.HOLD
        ? "Hold enrichment until Census validation failures are resolved."
        : status === STATUS.FROZEN_RADAR_MISSING
          ? "Proceed with first enrichment lane (descriptions + amenities + property type/asset context). Schedule schema v1.1.2 for Radar/public display fields before any Radar integration or public Census surfacing."
          : "Proceed with first enrichment lane under this frozen contract.",
  };
}

export function renderFieldContractMarkdown(r) {
  const groupLines = [];
  for (const [group, fields] of Object.entries(r.field_groups || {})) {
    groupLines.push(`### ${group}`, ``);
    for (const f of fields) {
      groupLines.push(
        `- **${f.name}** — \`${f.primary}\`${f.tags?.length ? ` · tags: ${f.tags.join(", ")}` : ""}${f.notes ? ` — ${f.notes}` : ""}`
      );
    }
    groupLines.push(``);
  }

  return [
    `# Production Census Field Contract v1.1.1`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Contract ID:** \`${r.contract_id}\``,
    `**Generated:** ${r.generated_at}`,
    `**Read-only freeze:** true (no Airtable / Brand Explorer writes)`,
    ``,
    `## 1. Executive summary`,
    ``,
    `- Census: ${r.validation?.record_count} records / ${r.validation?.field_count} fields`,
    `- Validation pass: ${r.validation?.pass}`,
    `- Radar fields present: ${(r.radar_public_display?.present || []).length}/${(r.radar_public_display?.required_for_radar_integration || []).length}`,
    `- First enrichment may proceed: ${r.status !== STATUS.HOLD}`,
    `- v1.1.2 needed for Radar: ${r.radar_public_display?.v112_needed}`,
    ``,
    `## 2. Frozen Census field groups`,
    ``,
    ...groupLines,
    `## 3. Required fields`,
    ``,
    ...(r.required_fields || []).map((n) => `- ${n}`),
    ``,
    `## 4. Optional fields`,
    ``,
    ...(r.optional_fields || []).map((n) => `- ${n}`),
    ``,
    `## 5. Enrichment target fields`,
    ``,
    ...(r.enrichment_target_fields || []).map((n) => `- ${n}`),
    ``,
    `## 6. Public / Radar display fields`,
    ``,
    "```json",
    JSON.stringify(r.radar_public_display || {}, null, 2),
    "```",
    ``,
    `## 7. Fields safe for first enrichment`,
    ``,
    ...(r.first_enrichment_lane?.allowed || []).map((n) => `- ${n}`),
    ``,
    `## 8. Fields still blocked`,
    ``,
    ...(r.first_enrichment_lane?.blocked || []).map((n) => `- ${n}`),
    ``,
    `Over-modeled amenity flags (not in first lane):`,
    ...(r.first_enrichment_lane?.overmodeled_amenity_flags_not_in_first_lane || []).map(
      (n) => `- ${n}`
    ),
    ``,
    `## 9. Steward review rules`,
    ``,
    "```json",
    JSON.stringify(r.steward_rules || {}, null, 2),
    "```",
    ``,
    `## 10. Brand Explorer safety result`,
    ``,
    "```json",
    JSON.stringify(r.brand_explorer_safety || { pending: true }, null, 2),
    "```",
    ``,
    `## 11. Final recommendation`,
    ``,
    r.final_recommendation || "",
    ``,
    `## Appendix — Census validation`,
    ``,
    "```json",
    JSON.stringify(r.validation || {}, null, 2),
    "```",
    ``,
  ].join("\n");
}
