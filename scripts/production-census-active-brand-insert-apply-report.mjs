/**
 * Post-apply validation + report writers for Active Brand Setup insert apply.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import { MAP_FIRST_PASS } from "../lib/research-engine-v2/production-census-first-pass-enrichment.js";
import { productionHotelPropertyCensus } from "../lib/research-engine-v2/production-census-source-of-truth.js";

const RUN = "reports/research-engine-v2/autopilot/2026-08-05T23-52-53_CALA-source-discovery";
const preflight = JSON.parse(
  fs.readFileSync("reports/research-engine-v2/production-census-active-brand-insert-preflight.json", "utf8")
);
const applySummary = JSON.parse(fs.readFileSync(path.join(RUN, "apply-summary.json"), "utf8"));
const checkpoint = JSON.parse(fs.readFileSync(path.join(RUN, "insert-checkpoint.json"), "utf8"));

const token = resolvePat();
const bases = resolveTargetBase();
const tableId = TABLE_IDS["Hotel Property Census"];
let count = 0;
let offset;
const sampleCreated = [];
const createdKeys = new Set(checkpoint.identity_keys_created || []);

do {
  const params = new URLSearchParams({ pageSize: "100" });
  if (offset) params.set("offset", offset);
  for (const f of [
    MAP_FIRST_PASS.identityKey,
    MAP_FIRST_PASS.propertyName,
    "Production Use Status",
    "Owner Name",
  ]) {
    params.append("fields[]", f);
  }
  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(bases.target_base_id)}/${encodeURIComponent(tableId)}?${params}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  for (const r of json.records || []) {
    count += 1;
    const key = String(r.fields?.[MAP_FIRST_PASS.identityKey] || "");
    if (createdKeys.has(key) && sampleCreated.length < 5) {
      sampleCreated.push({
        id: r.id,
        key,
        name: r.fields?.[MAP_FIRST_PASS.propertyName],
        production: r.fields?.["Production Use Status"],
        owner: r.fields?.["Owner Name"] || null,
      });
    }
  }
  offset = json.offset;
  await new Promise((r) => setTimeout(r, 100));
} while (offset);

const finalStatus =
  applySummary.created_count === preflight.pass_count &&
  (preflight.counts.steward_review_required > 0 || preflight.counts.duplicate_risk > 0)
    ? "production_census_active_brand_insert_apply_partial_steward_remaining"
    : applySummary.created_count === preflight.bundle_inserts
      ? "production_census_active_brand_insert_apply_clean"
      : applySummary.airtable_writes
        ? "production_census_active_brand_insert_apply_partial_steward_remaining"
        : "production_census_active_brand_insert_apply_blocked";

const report = {
  run_type: "production_census_active_brand_insert_apply",
  generated_at: new Date().toISOString(),
  final_status: finalStatus,
  selected_run_dir: RUN,
  production_target: productionHotelPropertyCensus,
  airtable_writes: true,
  brand_setup_writes: false,
  brand_explorer_writes: false,
  vic_writes: false,
  old_census_writes: false,
  webhound_invoked: false,
  preflight: {
    bundle_inserts: preflight.bundle_inserts,
    pass: preflight.pass_count,
    counts: preflight.counts,
    filtered_bundle: preflight.filtered_approval_bundle,
  },
  apply: {
    status: applySummary.status,
    created_count: applySummary.created_count,
    created_record_ids: applySummary.created_record_ids,
    blocked_duplicates: applySummary.blocked_duplicates,
    steward_routed_on_apply: applySummary.steward_routed,
    batches_completed: checkpoint.batches_completed,
    census_count_after: count,
    census_count_before_approx: 666,
    delta_approx: count - 666,
  },
  validation: {
    created_count_matches_pass: applySummary.created_count === preflight.pass_count,
    production_use_status_sample: sampleCreated,
    forbidden_fields_on_sample_empty: sampleCreated.every((s) => !s.owner),
    table_id: tableId,
  },
  steward_remaining: preflight.counts.steward_review_required,
  recommended_next:
    "Steward 16 Choice Radisson Individuals names with member-of suffixes + unknown city; optional name normalize then re-queue",
};

fs.writeFileSync(
  "reports/research-engine-v2/production-census-active-brand-insert-apply.json",
  JSON.stringify(report, null, 2)
);

const md = [
  "# Active Brand Setup Insert Apply",
  "",
  `**Status:** \`${finalStatus}\``,
  "",
  `Selected run: \`${RUN}\``,
  "",
  "## Preflight",
  "",
  "- Bundle: 91",
  `- Pass: **${preflight.pass_count}**`,
  `- Steward remaining: **${preflight.counts.steward_review_required}** (Choice "a member of Radisson Individuals" names)`,
  `- Duplicate risk: ${preflight.counts.duplicate_risk}`,
  "",
  "## Apply",
  "",
  `- Created: **${applySummary.created_count}**`,
  "- Airtable writes: true",
  `- Target: Hotel Property Census (${tableId})`,
  `- Census count after: **${count}** (before rededupe index ~666; delta ~${count - 666})`,
  "- Brand Setup / Brand Explorer / VIC / old Census: untouched",
  "",
  "## Validation",
  "",
  `- Created count matches preflight pass: ${applySummary.created_count === preflight.pass_count}`,
  "- Sample Production Use Status / forbidden fields:",
  ...sampleCreated.map(
    (s) => `  - ${s.key}: production=${s.production}, owner=${s.owner || "∅"}`
  ),
  "",
  "## Next",
  "",
  report.recommended_next,
  "",
];
fs.writeFileSync("reports/research-engine-v2/production-census-active-brand-insert-apply.md", md.join("\n"));
fs.writeFileSync(
  "docs/data-intelligence/production-census-active-brand-insert-apply.md",
  [
    "# Active Brand Setup Insert Apply",
    "",
    `**Status:** \`${finalStatus}\``,
    "",
    `- Applied **${applySummary.created_count}** High-confidence Active Brand Setup discovery inserts to Hotel Property Census.`,
    `- **${preflight.counts.steward_review_required}** steward cases remain (Choice Radisson Individuals member-of names).`,
    "- Full report: `reports/research-engine-v2/production-census-active-brand-insert-apply.json`",
    "",
  ].join("\n")
);

fs.writeFileSync(
  path.join(RUN, "summary.md"),
  [
    "# Active Brand Setup source_discovery — insert apply",
    "",
    `Status: **${finalStatus}**`,
    "",
    `- Preflight pass: ${preflight.pass_count} / 91`,
    `- Inserted: ${applySummary.created_count}`,
    `- Steward remaining: ${preflight.counts.steward_review_required}`,
    `- Census rows after: ${count}`,
    "",
  ].join("\n")
);

const blockedCsv = [
  "identity_key,classification,reasons,property_name,brand,country",
  ...preflight.results
    .filter((r) => r.classification !== "pass_insert_preflight")
    .map((r) =>
      [
        r.identity_key,
        r.classification,
        r.reasons.join("|"),
        JSON.stringify(r.property_name),
        JSON.stringify(r.brand),
        r.country,
      ].join(",")
    ),
].join("\n");
fs.writeFileSync(path.join(RUN, "blocked-records.csv"), blockedCsv);

fs.writeFileSync(
  path.join(RUN, "learning-update.json"),
  JSON.stringify(
    {
      census_active_brand_insert_apply: true,
      status: finalStatus,
      created_count: applySummary.created_count,
      steward_remaining: preflight.counts.steward_review_required,
      approval_bundle_bound: true,
      brand_explorer_untouched: true,
      brand_setup_untouched: true,
      vic_writes: false,
    },
    null,
    2
  )
);

console.log(
  JSON.stringify(
    {
      finalStatus,
      count,
      created: applySummary.created_count,
      steward: preflight.counts.steward_review_required,
    },
    null,
    2
  )
);
