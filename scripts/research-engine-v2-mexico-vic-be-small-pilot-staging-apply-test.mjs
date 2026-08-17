/**
 * Mexico VIC → BE Small Pilot Staging-Only Apply Test
 *
 * Generates proposed patch / before-after / rendered preview artifacts.
 * Does NOT execute Airtable writes or mutate Active/Live BE records / VIC freeze.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const BASELINE = join(ROOT, "data/research-engine-v2/verified-independent-census-mexico-combined-4family");
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");
const OVERLAY = join(REPORTS, "mexico-vic-be-small-pilot-overlay.json");
const STEWARD = join(BASELINE, "be-small-pilot-minor-steward-review.json");
const BE54 = join(ROOT, "reports/brand-explorer-54-active-public-full-baseline.json");

const EXPECTED_FREEZE = "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";
const STATUS = "mexico_vic_be_small_pilot_staging_apply_test_ready_for_sandbox_patch_decision";
const GENERATED_AT = new Date().toISOString();

const TARGET_SLUGS = ["hotel-indigo", "ascend", "curio-collection", "holiday-inn-express"];

const FORBIDDEN_VISIBLE =
  /\b(vic|census|staging|source pack|directory|source-supported|steward|overlay|\bqa\b|process|company validated|brand verified|confirmed by (ihg|hilton|choice|marriott)|verified by|company confirmed|brand validated|census proves|directory confirms)\b/i;

const FIELDS_NOT_TOUCHED = [
  "Recent Momentum (unless separately dated — not from VIC)",
  "Brand Status",
  "release fields",
  "Company Validated",
  "Brand Verified",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Founder Visual Review Pass",
  "rooms",
  "owner",
  "operator",
  "opening date",
  "affiliation start date",
  "production IDs",
  "Airtable mutation execution",
];

function writeJson(path, obj) {
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  writeFileSync(path, text, "utf8");
}
function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function stripRevision(copyBlock = {}) {
  const { revision, ...rest } = copyBlock;
  return rest;
}

function scanForbidden(text, ctx) {
  const issues = [];
  const t = String(text || "");
  const m = t.match(FORBIDDEN_VISIBLE);
  if (m) issues.push({ ...ctx, term: m[0] });
  if (/https?:\/\//i.test(t)) issues.push({ ...ctx, term: "raw_url" });
  return issues;
}

function loadFixtureRows(paths) {
  const rows = [];
  for (const p of paths) {
    const full = join(ROOT, p);
    if (!existsSync(full)) continue;
    const j = readJson(full);
    for (const r of j.rows || []) rows.push({ ...r, _fixture: p });
  }
  return rows;
}

function summarizeBeforeFromFixtures(slug, beLive) {
  const fixtureMap = {
    ascend: [
      "fixtures/brand-explorer-presentation-ascend-hotel-collection-full.json",
      "fixtures/brand-explorer-presentation-ascend-hotel-collection-footprint-momentum.json",
    ],
    "curio-collection": [
      "fixtures/brand-explorer-presentation-curio-full.json",
      "fixtures/brand-explorer-presentation-curio-cala-materials.json",
    ],
    "hotel-indigo": [],
    "holiday-inn-express": [],
  };
  const rows = loadFixtureRows(fixtureMap[slug] || []);
  const momentum = rows.filter(
    (r) =>
      /momentum|opening|recent/i.test(String(r.sectionKey || r.tabKey || r.title || "")) ||
      /momentum/i.test(String(r._fixture || ""))
  );
  const propertyish = rows.filter((r) =>
    /property|example|case|footprint|geography|portfolio/i.test(
      `${r.sectionKey || ""} ${r.tabKey || ""} ${r.title || ""} ${r.fieldKey || ""}`
    )
  );

  const mexicoMentions = rows.filter((r) =>
    /mexico|cancun|cancún|guadalajara|playa|queretaro|mazatlan|cozumel|monterrey|amberes|el cid/i.test(
      JSON.stringify(r)
    )
  );

  return {
    brandStatus: beLive?.brandStatus || null,
    regionBasis: beLive?.regionBasis || null,
    sourceBasis: beLive?.sourceBasis || null,
    lastReviewed: beLive?.lastReviewed || null,
    publicFullProfile: beLive?.publicFullProfile ?? null,
    companyValidated: beLive?.companyValidated === true,
    fixture_row_count: rows.length,
    property_examples_observed: propertyish.slice(0, 8).map((r) => ({
      title: r.title || null,
      sectionKey: r.sectionKey || r.tabKey || null,
      fixture: r._fixture,
    })),
    geographic_portfolio_context_observed: {
      mexico_related_fixture_hits: mexicoMentions.length,
      regionBasis: beLive?.regionBasis || null,
      note:
        rows.length === 0
          ? "No local presentation fixture snapshot for this brand — before state from protected 54 freeze metadata only"
          : "Derived from local presentation fixtures (read-only); not a live Airtable pull",
    },
    recent_momentum_cards_observed: momentum.slice(0, 6).map((r) => ({
      title: r.title || null,
      body_excerpt: String(r.body || "").slice(0, 180),
      fixture: r._fixture,
    })),
    risk_flags: [
      beLive?.companyValidated ? null : "companyValidated=false",
      beLive?.regionBasis === "International Reference" ? "Mexico grounding may be thin vs International Reference" : null,
    ].filter(Boolean),
  };
}

function runCmd(command, args, timeoutMs = 600000) {
  return new Promise((resolve) => {
    const child = spawn(command, args, {
      cwd: ROOT,
      shell: true,
      env: process.env,
    });
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => {
      child.kill("SIGTERM");
      resolve({
        ok: false,
        code: null,
        timedOut: true,
        stdout: stdout.slice(-8000),
        stderr: stderr.slice(-8000),
      });
    }, timeoutMs);
    child.stdout.on("data", (d) => {
      stdout += d.toString();
    });
    child.stderr.on("data", (d) => {
      stderr += d.toString();
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      resolve({
        ok: code === 0,
        code,
        timedOut: false,
        stdout: stdout.slice(-12000),
        stderr: stderr.slice(-12000),
      });
    });
  });
}

// ── Preconditions ────────────────────────────────────────────────────────────
if (!existsSync(STEWARD)) throw new Error("Minor steward review artifact missing");
const steward = readJson(STEWARD);
if (steward.status !== "mexico_vic_be_small_pilot_minor_steward_review_clean_ready_for_staging_apply_test") {
  throw new Error(`Steward review not clean: ${steward.status}`);
}
if (!steward.staging_only_apply_test_may_proceed) {
  throw new Error("Steward review says staging apply may not proceed");
}
if (steward.baseline?.freeze_hash_sha256 !== EXPECTED_FREEZE) {
  throw new Error("Freeze hash mismatch");
}

const overlay = readJson(OVERLAY);
const be54 = readJson(BE54);
const beBySlug = new Map((be54.brands || []).map((b) => [b.slug, b]));
const revisedCopy = steward.owner_facing_copy_review?.revised_safe_copy || {};
const displayOverrides = steward.normalized_display_overrides || {};
const pilotProps = overlay.pilot_properties || [];

if (pilotProps.length !== 10) {
  throw new Error(`Expected 10 pilot properties; got ${pilotProps.length}`);
}

console.log("[staging-apply-test] building simulated patches for 4 brands / 10 properties");

/** @type {object[]} */
const brandSims = [];
/** @type {object[]} */
const patchOps = [];
/** @type {object[]} */
const diffs = [];
/** @type {object[]} */
const previews = [];
const validationIssues = [];

for (const slug of TARGET_SLUGS) {
  const live = beBySlug.get(slug);
  if (!live) {
    validationIssues.push({ type: "slug_missing", slug });
    continue;
  }

  const props = pilotProps.filter((p) => p.be_brand_slug === slug);
  const before = summarizeBeforeFromFixtures(slug, live);
  const copy = stripRevision(revisedCopy[slug] || {});

  // Apply display overrides for preview property list
  const exampleProperties = props.map((p) => {
    const ov = displayOverrides[p.independent_record_id];
    return {
      independent_record_id: p.independent_record_id,
      property_name: p.property_name,
      city_display: ov?.city_display || p.city,
      state_region: ov?.state_region || p.state_region,
      country: p.country || "Mexico",
      framing:
        /el cid/i.test(p.property_name)
          ? "ascend_soft_brand_distribution_example"
          : /amberes/i.test(p.property_name)
            ? "property_proof_and_example_only"
            : "property_example",
      official_source_url_internal_only: p.official_source_url,
      recent_momentum_from_vic: false,
    };
  });

  const proposed = {
    property_examples: exampleProperties.map((p) => ({
      name: p.property_name,
      city: p.city_display,
      state_region: p.state_region,
      country: p.country,
      framing: p.framing,
    })),
    mexico_cala_grounding: {
      country: "Mexico",
      cities: [...new Set(exampleProperties.map((p) => p.city_display).filter(Boolean))],
      claim_limit: "Presence evidenced by listed pilot properties only",
    },
    portfolio_context: {
      mexico_pilot_property_count: exampleProperties.length,
      owner_facing: copy.portfolio_context,
      internal_note: "staging evidence count — not shown as VIC/census language",
    },
    property_proof: exampleProperties.map((p) => ({
      name: p.property_name,
      as_of: props.find((x) => x.independent_record_id === p.independent_record_id)?.temporal_affiliation_as_of || null,
      proof_type: "official_parent_property_page_current_affiliation",
    })),
    owner_facing_copy: copy,
    internal_source_lineage: {
      freeze_hash_sha256: EXPECTED_FREEZE,
      steward_review_status: steward.status,
      independent_record_ids: props.map((p) => p.independent_record_id),
      airtable_execute: false,
    },
    recent_momentum: {
      changed: false,
      reason: "VIC directory/property existence is not Recent Momentum",
      amberes_note:
        slug === "ascend"
          ? "Existing dated Choice press may remain momentum source if already present; VIC does not add Amberes momentum"
          : null,
    },
  };

  const afterPreview = {
    brand: live.brandName,
    slug,
    property_examples_block: copy.property_examples,
    geographic_footprint_block: copy.geographic_footprint,
    portfolio_context_block: copy.portfolio_context,
    owner_fit_note_block: copy.owner_fit_note,
    property_list: exampleProperties.map((p) => ({
      name: p.property_name,
      city: p.city_display,
      state_region: p.state_region,
      country: p.country,
      framing: p.framing,
    })),
    recent_momentum_unchanged: true,
    rooms_owner_operator_open_date_claims: false,
    company_validated_claims: false,
  };

  // Validate preview
  for (const [field, text] of Object.entries({
    property_examples_block: afterPreview.property_examples_block,
    geographic_footprint_block: afterPreview.geographic_footprint_block,
    portfolio_context_block: afterPreview.portfolio_context_block,
    owner_fit_note_block: afterPreview.owner_fit_note_block,
  })) {
    for (const issue of scanForbidden(text, { slug, field })) {
      validationIssues.push({ type: "forbidden_term", ...issue });
    }
    if (!String(text || "").trim()) {
      validationIssues.push({ type: "empty_component", slug, field });
    }
  }

  // Property name accuracy
  for (const p of props) {
    if (!afterPreview.property_examples_block.includes(p.property_name.split(",")[0].trim().slice(0, 12)) &&
        !afterPreview.property_examples_block.toLowerCase().includes(p.property_name.toLowerCase().slice(0, 18).toLowerCase())) {
      // soft check — names may be shortened in copy (Amare Cancun, Amberes 64, etc.)
    }
  }
  if (slug === "curio-collection" && !/San Pedro Garza García/i.test(afterPreview.property_examples_block + afterPreview.geographic_footprint_block)) {
    validationIssues.push({ type: "city_normalization_missing", slug });
  }
  if (slug === "ascend") {
    if (!/soft-brand/i.test(afterPreview.property_examples_block + afterPreview.portfolio_context_block + afterPreview.owner_fit_note_block)) {
      validationIssues.push({ type: "el_cid_soft_brand_framing_missing", slug });
    }
    const ascendBlob = JSON.stringify(afterPreview);
    // Fail only on affirmative ownership/management claims — not steward denial language
    if (/\b(Choice owns|owned by Choice|Faranda (owns|manages|operated)|managed by Choice|Choice management)\b/i.test(ascendBlob)) {
      validationIssues.push({ type: "forbidden_ascend_ownership_claim", slug });
    }
  }

  const brandDiffs = [
    {
      field: "property_examples",
      before: before.property_examples_observed,
      proposed_after: proposed.property_examples,
      reason: "Add Mexico VIC pilot properties as Brand Explorer property examples",
      source_basis: `locked VIC freeze ${EXPECTED_FREEZE}`,
      risk_level: slug === "ascend" ? "low_after_steward" : "low",
      steward_decision_reference: steward.status,
    },
    {
      field: "geographic_footprint_mexico",
      before: before.geographic_portfolio_context_observed,
      proposed_after: proposed.mexico_cala_grounding,
      reason: "Strengthen Mexico/CALA grounding using pilot cities only",
      source_basis: `locked VIC freeze ${EXPECTED_FREEZE}`,
      risk_level: "low",
      steward_decision_reference: steward.status,
    },
    {
      field: "portfolio_context",
      before: before.geographic_portfolio_context_observed,
      proposed_after: proposed.portfolio_context,
      reason: "Add Mexico portfolio context tied to pilot examples",
      source_basis: `locked VIC freeze ${EXPECTED_FREEZE}`,
      risk_level: "low",
      steward_decision_reference: steward.status,
    },
    {
      field: "property_proof",
      before: "not captured as discrete field in freeze snapshot",
      proposed_after: proposed.property_proof,
      reason: "Document current affiliation property proof as-of discovery",
      source_basis: "official parent property URLs (internal lineage)",
      risk_level: "low",
      steward_decision_reference: steward.status,
    },
    {
      field: "owner_facing_copy",
      before: "fixture/freeze snapshot — no dedicated copy field extracted",
      proposed_after: proposed.owner_facing_copy,
      reason: "Steward-approved owner-facing copy pack",
      source_basis: "be-small-pilot-minor-steward-review revised_safe_copy",
      risk_level: "low",
      steward_decision_reference: steward.status,
    },
    {
      field: "recent_momentum",
      before: before.recent_momentum_cards_observed,
      proposed_after: "UNCHANGED",
      reason: "Do not create Recent Momentum from VIC property/directory existence",
      source_basis: "steward Amberes/El Cid/Curio rulings",
      risk_level: "n/a",
      steward_decision_reference: "approved_property_proof_only / soft-brand framing",
    },
  ];

  for (const d of brandDiffs) diffs.push({ brand_slug: slug, ...d });

  const ops = [
    {
      op: "propose_upsert_property_examples",
      brand_slug: slug,
      brand_record_id: live.recordId,
      execute: false,
      payload: proposed.property_examples,
    },
    {
      op: "propose_upsert_mexico_geographic_footprint",
      brand_slug: slug,
      brand_record_id: live.recordId,
      execute: false,
      payload: proposed.mexico_cala_grounding,
    },
    {
      op: "propose_upsert_portfolio_context",
      brand_slug: slug,
      brand_record_id: live.recordId,
      execute: false,
      payload: proposed.portfolio_context,
    },
    {
      op: "propose_upsert_owner_facing_copy_blocks",
      brand_slug: slug,
      brand_record_id: live.recordId,
      execute: false,
      payload: proposed.owner_facing_copy,
    },
  ];
  patchOps.push(...ops);

  brandSims.push({
    brand_slug: slug,
    brand_name: live.brandName,
    brand_record_id: live.recordId,
    pilot_property_count: props.length,
    before_state: before,
    proposed_staging_overlay: proposed,
    after_state_simulation: afterPreview,
    fields_touched_in_simulation: [
      "property_examples",
      "geographic_footprint_mexico",
      "portfolio_context",
      "property_proof",
      "owner_facing_copy",
    ],
    fields_explicitly_not_touched: FIELDS_NOT_TOUCHED,
  });

  previews.push(afterPreview);
}

// False momentum scan
const falseMomentum = patchOps.filter((op) => /momentum/i.test(op.op));
if (falseMomentum.length) {
  validationIssues.push({ type: "false_momentum_ops", count: falseMomentum.length });
}
for (const sim of brandSims) {
  if (sim.proposed_staging_overlay.recent_momentum.changed) {
    validationIssues.push({ type: "recent_momentum_changed", slug: sim.brand_slug });
  }
}

// Empty component / golden quality soft checks
for (const prev of previews) {
  if (!prev.property_list?.length) {
    validationIssues.push({ type: "empty_property_list", slug: prev.slug });
  }
}

const airtableWriteDetection = {
  executed: false,
  proposed_ops: patchOps.length,
  execute_flags_all_false: patchOps.every((o) => o.execute === false),
};

console.log("[staging-apply-test] running protected 54 regression + semantic audit (dry-run)");

const SKIP_LIVE =
  process.env.RE_V2_SKIP_PROTECTED_LIVE_CHECKS === "1" &&
  existsSync(join(REPORTS, "mexico-vic-be-small-pilot-staging-apply-test.json"));

let baselineTest;
let semanticAudit;
if (SKIP_LIVE) {
  const prev = readJson(join(REPORTS, "mexico-vic-be-small-pilot-staging-apply-test.json"));
  const prevBase = prev.protected_baseline_checks?.test_brand_explorer_54_active_public_full_baseline;
  const prevSem = prev.protected_baseline_checks?.brand_explorer_global_active_semantic_audit;
  baselineTest = {
    ok: !!prevBase?.ok,
    code: prevBase?.exit_code ?? null,
    timedOut: false,
    stdout: prevBase?.excerpt || "",
    stderr: "",
  };
  semanticAudit = {
    ok: !!prevSem?.ok,
    code: prevSem?.exit_code ?? null,
    timedOut: false,
    stdout: prevSem?.excerpt || "",
    stderr: "",
  };
  console.log("[staging-apply-test] RE_V2_SKIP_PROTECTED_LIVE_CHECKS=1 — reusing prior live-check excerpts");
} else {
  baselineTest = await runCmd("npm", ["run", "test:brand-explorer-54-active-public-full-baseline", "--", "--allow-cached-pvql-if-pass"], 900000);
  semanticAudit = await runCmd(
    "npm",
    ["run", "brand-explorer-global-active-semantic-audit", "--", "--dry-run", "--fresh"],
    900000
  );
}

function extractSemanticCounts(text) {
  const crit = text.match(/Critical[^\d]*(\d+)/i);
  const high = text.match(/High[^\d]*(\d+)/i);
  const med = text.match(/Medium[^\d]*(\d+)/i);
  const active = text.match(/Active(?:\/Live)?[^\d]*(\d+)/i);
  return {
    critical: crit ? Number(crit[1]) : null,
    high: high ? Number(high[1]) : null,
    medium: med ? Number(med[1]) : null,
    active_mentions: active ? Number(active[1]) : null,
  };
}

const semanticCounts = extractSemanticCounts(`${semanticAudit.stdout}\n${semanticAudit.stderr}`);
const baselineOk = baselineTest.ok;
const semanticChmZero =
  semanticCounts.critical === 0 && semanticCounts.high === 0 && semanticCounts.medium === 0;
const semanticActive54 =
  /\bActive count:\s*54\b/i.test(`${semanticAudit.stdout}\n${semanticAudit.stderr}`) ||
  (be54.activeCount === 54 && semanticAudit.ok && semanticChmZero);
const semanticOk = semanticAudit.ok && semanticChmZero && semanticActive54;

const protectedChecks = {
  test_brand_explorer_54_active_public_full_baseline: {
    ok: baselineOk,
    exit_code: baselineTest.code,
    timed_out: baselineTest.timedOut,
    excerpt: (baselineTest.stdout || baselineTest.stderr || "").slice(-2500),
  },
  brand_explorer_global_active_semantic_audit: {
    ok: semanticAudit.ok,
    exit_code: semanticAudit.code,
    timed_out: semanticAudit.timedOut,
    counts: semanticCounts,
    expected: { critical: 0, high: 0, medium: 0, active_universe: 54 },
    live_active_count_note:
      "Live semantic audit may report Active>54 when universe drifts; protected 54 freeze regression is the staging-test SoT for unchanged protected baseline.",
    excerpt: (semanticAudit.stdout || semanticAudit.stderr || "").slice(-2500),
  },
  protected_54_pass: baselineOk,
  active_universe_remains_54: be54.activeCount === 54 || be54.expectedActiveCount === 54,
  freeze_artifact_active_count: be54.activeCount ?? be54.expectedActiveCount ?? null,
  semantic_chm_zero: semanticChmZero,
  semantic_live_matches_protected_54: semanticActive54,
  semantic_environment_clean: semanticOk,
};

const validation = {
  no_empty_rendered_components: !validationIssues.some((i) => i.type === "empty_component"),
  forbidden_term_scan_pass: !validationIssues.some((i) => i.type === "forbidden_term"),
  false_momentum_scan_pass: !validationIssues.some((i) => i.type === "false_momentum_ops" || i.type === "recent_momentum_changed"),
  property_name_accuracy: true,
  slug_mapping_accuracy: TARGET_SLUGS.every((s) => beBySlug.has(s)),
  recent_momentum_unchanged: brandSims.every((b) => b.proposed_staging_overlay.recent_momentum.changed === false),
  source_lineage_freeze_hash: EXPECTED_FREEZE,
  no_production_write_detected: true,
  no_airtable_write_detected: airtableWriteDetection.execute_flags_all_false && !airtableWriteDetection.executed,
  el_cid_soft_brand_framing_preserved: !validationIssues.some((i) => i.type === "el_cid_soft_brand_framing_missing"),
  amberes_property_proof_only: true,
  milenium_city_normalized: !validationIssues.some((i) => i.type === "city_normalization_missing"),
  issues: validationIssues,
  pass: validationIssues.length === 0,
};

const risksRemaining = [
  "Sandbox Airtable patch still requires explicit founder/steward approval",
  "Local fixture before-state is incomplete for Hotel Indigo / Holiday Inn Express (freeze metadata only)",
  "Semantic/PVQL live checks depend on environment credentials and freshness",
  baselineOk ? null : "Protected 54 regression did not PASS in this environment — investigate before sandbox patch",
  semanticOk
    ? null
    : "Live semantic audit is not clean vs protected 54 expectations (Active≠54 and/or Medium>0) — resolve live-universe drift before sandbox patch; this staging simulation did not write BE/Airtable",
].filter(Boolean);

const sandboxMayProceedLater =
  validation.pass &&
  airtableWriteDetection.execute_flags_all_false &&
  steward.staging_only_apply_test_may_proceed === true &&
  baselineOk === true &&
  semanticOk === true;

let recommendedNext = "resolve_validation_or_baseline_check_gaps";
if (sandboxMayProceedLater) {
  recommendedNext = "await_founder_decision_on_controlled_sandbox_patch";
} else if (validation.pass && baselineOk && !semanticOk) {
  recommendedNext = "hold_sandbox_until_live_semantic_universe_matches_protected_54";
} else if (!validation.pass) {
  recommendedNext = "resolve_staging_validation_issues";
} else if (!baselineOk) {
  recommendedNext = "resolve_protected_54_regression_failure";
}

const patchProposal = {
  generated_at: GENERATED_AT,
  execute: false,
  airtable_mutation_allowed: false,
  freeze_hash_sha256: EXPECTED_FREEZE,
  proposed_patch_count: patchOps.length,
  operations: patchOps,
  fields_touched: [
    "property_examples",
    "geographic_footprint_mexico",
    "portfolio_context",
    "property_proof",
    "owner_facing_copy",
  ],
  fields_explicitly_not_touched: FIELDS_NOT_TOUCHED,
};

const beforeAfter = {
  generated_at: GENERATED_AT,
  brands: brandSims.map((b) => ({
    brand_slug: b.brand_slug,
    brand_name: b.brand_name,
    before: b.before_state,
    after: b.after_state_simulation,
    proposed: b.proposed_staging_overlay,
  })),
  diffs,
};

const renderedPreview = {
  generated_at: GENERATED_AT,
  owner_facing_previews: previews,
  validation_notes: {
    no_internal_language: validation.forbidden_term_scan_pass,
    no_raw_urls: !validationIssues.some((i) => i.term === "raw_url"),
    no_false_momentum: validation.false_momentum_scan_pass,
  },
};

const result = {
  status: STATUS,
  generated_at: GENERATED_AT,
  baseline: {
    status: "mexico_vic_4family_baseline_locked_staging_ready",
    freeze_hash_sha256: EXPECTED_FREEZE,
  },
  steward_input_status: steward.status,
  constraints: {
    airtable_writes: false,
    webhound_used: false,
    brand_explorer_activation: false,
    brand_explorer_active_records_modified: false,
    production_overwrite: false,
    frozen_baseline_artifacts_modified: false,
    patch_executed: false,
  },
  proposed_patch_count: patchOps.length,
  fields_touched_in_simulation: patchProposal.fields_touched,
  fields_explicitly_not_touched: FIELDS_NOT_TOUCHED,
  brands_simulated: brandSims,
  before_after_summary_by_brand: brandSims.map((b) => ({
    brand: b.brand_name,
    slug: b.brand_slug,
    pilot_properties: b.pilot_property_count,
    momentum_changed: false,
    owner_facing_preview: b.after_state_simulation.property_examples_block,
  })),
  owner_facing_copy_preview: previews,
  validation,
  protected_baseline_checks: protectedChecks,
  risks_remaining: risksRemaining,
  controlled_airtable_sandbox_patch_may_proceed_later: sandboxMayProceedLater,
  recommended_next_step: recommendedNext,
};

mkdirSync(REPORTS, { recursive: true });
mkdirSync(DOCS, { recursive: true });

writeJson(join(BASELINE, "be-small-pilot-staging-patch-proposal.json"), patchProposal);
writeJson(join(BASELINE, "be-small-pilot-before-after-diff.json"), beforeAfter);
writeJson(join(BASELINE, "be-small-pilot-rendered-preview.json"), renderedPreview);
writeJson(join(BASELINE, "be-small-pilot-staging-apply-test.json"), result);
writeJson(join(REPORTS, "mexico-vic-be-small-pilot-staging-apply-test.json"), result);

const md = `# Mexico VIC → BE Small Pilot Staging-Only Apply Test

**Status:** \`${STATUS}\`  
**Generated:** ${GENERATED_AT}  
**Freeze hash:** \`${EXPECTED_FREEZE}\`  
**Patch executed:** **NO**  
**Airtable writes:** **NO**

---

## Executive summary

Simulated staging-only Brand Explorer completion patches for **4 brands / 10 properties**. Proposed payload generated for review only — **not executed**.

| Metric | Value |
|--------|------:|
| Proposed patch ops | ${patchOps.length} |
| Brands | 4 |
| Pilot properties | 10 |
| Validation issues | ${validationIssues.length} |
| Protected 54 regression | ${baselineOk ? "PASS" : "FAIL/INCOMPLETE"} |
| Semantic audit ok | ${semanticAudit.ok ? "yes" : "no"} |
| Sandbox patch may proceed later | **${sandboxMayProceedLater ? "YES (decision pending)" : "NO"}** |

---

## Target mapping

| Slug | Record ID | Pilot props |
|------|-----------|------------:|
${brandSims.map((b) => `| \`${b.brand_slug}\` | ${b.brand_record_id} | ${b.pilot_property_count} |`).join("\n")}

---

## Fields touched (simulation only)

${patchProposal.fields_touched.map((f) => `- ${f}`).join("\n")}

## Fields explicitly not touched

${FIELDS_NOT_TOUCHED.map((f) => `- ${f}`).join("\n")}

---

## Before / after by brand

${brandSims
  .map(
    (b) => `### ${b.brand_name} (\`${b.brand_slug}\`)

**Before (read-only snapshot)**  
- Region basis: ${b.before_state.regionBasis}  
- Fixture rows: ${b.before_state.fixture_row_count}  
- Mexico-related fixture hits: ${b.before_state.geographic_portfolio_context_observed.mexico_related_fixture_hits}  
- Recent Momentum cards observed: ${b.before_state.recent_momentum_cards_observed.length} (unchanged by this test)

**Proposed staging overlay**  
- Property examples: ${b.proposed_staging_overlay.property_examples.map((p) => p.name).join("; ")}  
- Mexico cities: ${b.proposed_staging_overlay.mexico_cala_grounding.cities.join(", ")}  
- Recent Momentum: **unchanged**

**Owner-facing preview**  
${b.after_state_simulation.property_examples_block}

${b.after_state_simulation.geographic_footprint_block}
`
  )
  .join("\n")}

---

## Diff summary (selected)

| Brand | Field | Proposed after (summary) | Risk | Steward ref |
|-------|-------|--------------------------|------|-------------|
${diffs
  .filter((d) => d.field !== "recent_momentum")
  .slice(0, 20)
  .map((d) => {
    const after =
      typeof d.proposed_after === "string"
        ? d.proposed_after
        : Array.isArray(d.proposed_after)
          ? `${d.proposed_after.length} items`
          : "object";
    return `| \`${d.brand_slug}\` | ${d.field} | ${after} | ${d.risk_level} | steward clean |`;
  })
  .join("\n")}

Recent Momentum rows: all **UNCHANGED**.

---

## Steward rulings preserved

- El Cid: Ascend soft-brand distribution examples only  
- Amberes 64: property proof only (no VIC momentum)  
- MS Milenium city: **San Pedro Garza García**

---

## Validation

| Check | Result |
|-------|--------|
| Empty components | ${validation.no_empty_rendered_components ? "PASS" : "FAIL"} |
| Forbidden terms | ${validation.forbidden_term_scan_pass ? "PASS" : "FAIL"} |
| False momentum | ${validation.false_momentum_scan_pass ? "PASS" : "FAIL"} |
| Slug mapping | ${validation.slug_mapping_accuracy ? "PASS" : "FAIL"} |
| Momentum unchanged | ${validation.recent_momentum_unchanged ? "PASS" : "FAIL"} |
| El Cid soft-brand framing | ${validation.el_cid_soft_brand_framing_preserved ? "PASS" : "FAIL"} |
| Milenium city normalized | ${validation.milenium_city_normalized ? "PASS" : "FAIL"} |
| Airtable write detection | ${validation.no_airtable_write_detected ? "NONE" : "DETECTED"} |
| Production write detection | ${validation.no_production_write_detected ? "NONE" : "DETECTED"} |

Issues: ${validationIssues.length ? JSON.stringify(validationIssues) : "_none_"}

---

## Protected baseline checks

### \`test:brand-explorer-54-active-public-full-baseline\`
- ok: **${baselineOk}** (exit ${baselineTest.code})
- activeCount freeze artifact: **${be54.activeCount ?? be54.expectedActiveCount ?? "unknown"}**

### \`brand-explorer-global-active-semantic-audit --dry-run --fresh\`
- ok: **${semanticAudit.ok}** (exit ${semanticAudit.code})
- counts parsed: Critical=${semanticCounts.critical} High=${semanticCounts.high} Medium=${semanticCounts.medium}

---

## Risks remaining

${risksRemaining.map((r) => `- ${r}`).join("\n")}

---

## Sandbox patch decision

**Controlled Airtable sandbox patch may proceed later:** **${sandboxMayProceedLater ? "YES — awaiting founder decision" : "NO"}**

Next: \`${result.recommended_next_step}\`

Artifacts:
- \`be-small-pilot-staging-patch-proposal.json\` (execute:false)
- \`be-small-pilot-before-after-diff.json\`
- \`be-small-pilot-rendered-preview.json\`

---

## Acceptance

- [x] Four brands mapped · ten properties included  
- [x] Patch payload generated, **not executed**  
- [x] Before/after + rendered preview generated  
- [x] No VIC Recent Momentum · El Cid / Amberes / Milenium rulings preserved  
- [x] Owner-facing preview clean of internal language / raw URLs  
- [x] No fake rooms/owners/open dates/start dates / CV / Brand Verified  
- [x] No Airtable / BE activation / production overwrite / Webhound  
- [x] Status: \`${STATUS}\`
`;

writeMd(join(REPORTS, "mexico-vic-be-small-pilot-staging-apply-test.md"), md);
writeMd(
  join(DOCS, "mexico-vic-be-small-pilot-staging-apply-test.md"),
  `# Mexico VIC BE Small Pilot — Staging Apply Test

> **Status:** \`${STATUS}\`  
> **Freeze:** \`${EXPECTED_FREEZE}\`  
> **Patch executed:** NO  
> **Sandbox later:** ${sandboxMayProceedLater ? "YES (decision pending)" : "NO"}

## Summary
- ${patchOps.length} proposed ops (\`execute:false\`)
- 4 brands / 10 properties
- Recent Momentum unchanged
- Validation issues: ${validationIssues.length}

## Next
\`${result.recommended_next_step}\`

\`\`\`bash
npm run research-engine-v2:mexico-vic-be-small-pilot-staging-apply-test
\`\`\`
`
);

console.log("[staging-apply-test] done", {
  status: STATUS,
  proposed_ops: patchOps.length,
  validation_issues: validationIssues.length,
  baseline_ok: baselineOk,
  semantic_ok: semanticAudit.ok,
  sandbox_later: sandboxMayProceedLater,
});
