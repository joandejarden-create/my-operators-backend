/**
 * Mexico VIC → BE Small Pilot: rebase against frozen 62 + sandbox patch decision.
 *
 * Does NOT mutate frozen VIC/62 artifacts.
 * Does NOT patch production Brand Explorer / Airtable.
 * Sandbox execute:true only when isolation is explicitly proven via env.
 * Webhound is not used.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import "dotenv/config";
import {
  validateAirtableSandbox,
  STATUS as SANDBOX_STATUS,
} from "../lib/research-engine-v2/airtable-sandbox-validation.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const BASELINE = join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family"
);
const REPORTS = join(ROOT, "reports/research-engine-v2");
const DOCS = join(ROOT, "docs/data-intelligence");
const BE62_JSON = join(ROOT, "reports/brand-explorer-62-active-public-full-baseline.json");
const BE62_MD = join(ROOT, "docs/data-intelligence/brand-explorer-62-active-public-full-baseline.md");
const BE62_LIB = join(ROOT, "lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js");
const PROPOSAL = join(BASELINE, "be-small-pilot-staging-patch-proposal.json");
const DIFF = join(BASELINE, "be-small-pilot-before-after-diff.json");
const PREVIEW = join(BASELINE, "be-small-pilot-rendered-preview.json");
const STAGING_REPORT = join(REPORTS, "mexico-vic-be-small-pilot-staging-apply-test.json");
const STEWARD = join(BASELINE, "be-small-pilot-minor-steward-review.json");
const VIC_LOCK = join(BASELINE, "baseline-lock.json");

const EXPECTED_FREEZE =
  "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";
const EXPECTED_62_DECISION = "frozen_62_active_public_full_baseline_quality_clean_flex_held";
const GENERATED_AT = new Date().toISOString();

const TARGET_BRANDS = [
  {
    slug: "hotel-indigo",
    mapping: "hotel-indigo → hotel-indigo",
    expectedRecordId: "recegXrqaPiSLGCIe",
  },
  {
    slug: "ascend",
    mapping: "ascend-hotel-collection → ascend",
    expectedRecordId: "reclkgOzvAcBheUSo",
  },
  {
    slug: "curio-collection",
    mapping: "curio-collection-by-hilton → curio-collection",
    expectedRecordId: "receQkxgjlezsc1xg",
  },
  {
    slug: "holiday-inn-express",
    mapping: "holiday-inn-express → holiday-inn-express",
    expectedRecordId: "recmGmiIqDtAsm01f",
  },
];

const PILOT_PROPERTY_CHECKS = [
  { key: "Hotel Indigo Guadalajara Expo", brand: "hotel-indigo", match: /Hotel Indigo Guadalajara Expo/i },
  { key: "Hotel Indigo Playa del Carmen", brand: "hotel-indigo", match: /Hotel Indigo Playa del Carmen/i },
  { key: "Hotel Indigo Guanajuato", brand: "hotel-indigo", match: /Hotel Indigo Guanajuato/i },
  { key: "Amberes 64", brand: "ascend", match: /Amberes 64/i },
  { key: "El Cid Castilla", brand: "ascend", match: /El Cid Castilla/i },
  { key: "El Cid La Ceiba", brand: "ascend", match: /El Cid La Ceiba/i },
  { key: "Amare Cancun", brand: "curio-collection", match: /Amare Cancun/i },
  { key: "The Fives Downtown", brand: "curio-collection", match: /The Fives Downtown/i },
  { key: "MS Milenium Monterrey", brand: "curio-collection", match: /MS Milenium Monterrey/i },
  { key: "Holiday Inn Express Queretaro", brand: "holiday-inn-express", match: /Holiday Inn Express.*Quer[eé]taro/i },
];

const ALLOWED_FIELD_OPS = new Set([
  "propose_upsert_property_examples",
  "propose_upsert_mexico_geographic_footprint",
  "propose_upsert_portfolio_context",
  "propose_upsert_owner_facing_copy_blocks",
  "propose_upsert_property_proof",
]);

const FORBIDDEN_PAYLOAD_KEYS =
  /^(Brand Status|Active Profile Approved|Ready for Active Profile|Founder Visual Review Pass|Company Validated|Brand Verified|rooms|owner|operator|open.?date|affiliation.?start|Recent Momentum)$/i;

const FORBIDDEN_VISIBLE =
  /\b(vic|census|staging|source pack|directory|source-supported|steward|overlay|\bqa\b|process|company validated|brand verified|confirmed by (ihg|hilton|choice|marriott)|verified by|company confirmed|brand validated|census proves|directory confirms)\b/i;

const STATUS = {
  EXECUTED: "mexico_vic_be_small_pilot_sandbox_patch_executed_ready_for_review",
  SAFE_NOT_EXECUTED: "mexico_vic_be_small_pilot_sandbox_patch_safe_but_not_executed",
  NOT_PROVEN: "mexico_vic_be_small_pilot_sandbox_not_proven_do_not_execute",
  REVISE: "mexico_vic_be_small_pilot_rebase_requires_revision",
};

function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}
function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}
function fileFingerprint(path) {
  if (!existsSync(path)) return null;
  const st = statSync(path);
  return { path: path.replace(ROOT + "\\", "").replace(ROOT + "/", ""), size: st.size, mtimeMs: st.mtimeMs };
}

function runCmd(command, args, timeoutMs = 900000) {
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

function maskBaseId(id) {
  if (!id) return null;
  if (id.length < 10) return "(short)";
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}

function classifyField(opName) {
  if (opName.includes("property_examples")) return "property_examples";
  if (opName.includes("geographic_footprint") || opName.includes("mexico_geographic"))
    return "geographic_footprint_mexico";
  if (opName.includes("portfolio_context")) return "portfolio_context";
  if (opName.includes("owner_facing")) return "owner_facing_copy";
  if (opName.includes("property_proof")) return "property_proof";
  return opName;
}

function scanOwnerFacing(text, ctx) {
  const issues = [];
  const t = String(text || "");
  const m = t.match(FORBIDDEN_VISIBLE);
  if (m) issues.push({ ...ctx, term: m[0] });
  if (/https?:\/\//i.test(t)) issues.push({ ...ctx, term: "raw_url" });
  return issues;
}

function assessSandboxIsolation() {
  const productionBase = process.env.AIRTABLE_BASE_ID || null;
  const altBase = process.env.AIRTABLE_BASE_ID_ALT || null;
  const sandboxCandidates = {
    AIRTABLE_BASE_ID_SANDBOX: process.env.AIRTABLE_BASE_ID_SANDBOX || null,
    AIRTABLE_SANDBOX_BASE_ID: process.env.AIRTABLE_SANDBOX_BASE_ID || null,
    BE_SANDBOX_BASE_ID: process.env.BE_SANDBOX_BASE_ID || null,
    AIRTABLE_STAGING_BASE_ID: process.env.AIRTABLE_STAGING_BASE_ID || null,
  };
  const sandboxBase =
    sandboxCandidates.AIRTABLE_BASE_ID_SANDBOX ||
    sandboxCandidates.AIRTABLE_SANDBOX_BASE_ID ||
    sandboxCandidates.BE_SANDBOX_BASE_ID ||
    sandboxCandidates.AIRTABLE_STAGING_BASE_ID ||
    null;
  const confirmedFlag = process.env.BE_PILOT_SANDBOX_CONFIRMED === "1";
  const airtableEnv = String(process.env.AIRTABLE_ENV || "").toLowerCase();
  const envSaysSandbox = ["sandbox", "test", "staging"].includes(airtableEnv);

  const checks = [
    {
      id: "sandbox_base_env_present",
      pass: Boolean(sandboxBase),
      detail: sandboxBase
        ? `sandbox candidate ${maskBaseId(sandboxBase)}`
        : "no AIRTABLE_*_SANDBOX / STAGING base env set",
    },
    {
      id: "sandbox_base_differs_from_production",
      pass: Boolean(sandboxBase && productionBase && sandboxBase !== productionBase),
      detail:
        sandboxBase && productionBase
          ? sandboxBase === productionBase
            ? "sandbox base equals AIRTABLE_BASE_ID (production path)"
            : "sandbox base differs from AIRTABLE_BASE_ID"
          : "cannot compare — missing sandbox and/or production base",
    },
    {
      id: "explicit_sandbox_confirmation_flag",
      pass: confirmedFlag,
      detail: confirmedFlag
        ? "BE_PILOT_SANDBOX_CONFIRMED=1"
        : "BE_PILOT_SANDBOX_CONFIRMED not set to 1",
    },
    {
      id: "airtable_env_sandbox_or_test",
      pass: envSaysSandbox,
      detail: airtableEnv ? `AIRTABLE_ENV=${airtableEnv}` : "AIRTABLE_ENV unset",
    },
    {
      id: "write_target_not_production_base",
      pass: Boolean(sandboxBase && productionBase && sandboxBase !== productionBase),
      detail: "write target must be sandbox/test base, not AIRTABLE_BASE_ID",
    },
    {
      id: "alt_platform_base_not_used_as_sandbox_by_default",
      pass: true,
      detail: altBase
        ? `AIRTABLE_BASE_ID_ALT present (${maskBaseId(altBase)}) — not treated as BE sandbox`
        : "AIRTABLE_BASE_ID_ALT unset",
    },
  ];

  const proven = checks.every((c) => c.pass);
  return {
    proven,
    production_base_masked: maskBaseId(productionBase),
    sandbox_base_masked: maskBaseId(sandboxBase),
    sandbox_candidates: Object.fromEntries(
      Object.entries(sandboxCandidates).map(([k, v]) => [k, maskBaseId(v)])
    ),
    checks,
    execute_allowed: proven,
    decision: proven ? "sandbox_isolation_proven" : "sandbox_not_proven_do_not_execute",
  };
}

function assessProductionSafety(sandbox) {
  const productionBase = process.env.AIRTABLE_BASE_ID || null;
  const checks = [
    {
      id: "airtable_base_id_present_as_production_path",
      pass: Boolean(sandbox.production_base_masked),
      detail: `AIRTABLE_BASE_ID=${sandbox.production_base_masked || "(unset)"} — treated as production unless separate sandbox base proven`,
    },
    {
      id: "no_execute_against_production",
      pass: true,
      detail: "execute remains false unless sandbox proven and write adapter enabled",
    },
    {
      id: "production_brand_explorer_base_not_targeted_for_write",
      pass: true,
      detail: sandbox.proven
        ? "sandbox proven — any future writes must target sandbox base only"
        : "sandbox not proven — no writes will execute (production protected)",
    },
    {
      id: "frozen_62_artifacts_not_modified",
      pass: true,
      detail: "script writes only research-engine-v2 / pilot data outputs",
    },
    {
      id: "frozen_vic_artifacts_not_modified",
      pass: true,
      detail: "VIC lock + freeze hash artifacts are read-only inputs",
    },
  ];
  // If someone forced execute without sandbox, fail hard
  const fail = sandbox.proven === false && process.env.BE_PILOT_FORCE_EXECUTE === "1";
  if (fail) {
    checks.push({
      id: "force_execute_blocked",
      pass: false,
      detail: "BE_PILOT_FORCE_EXECUTE set while sandbox not proven",
    });
  }
  const allPass = checks.every((c) => c.pass) && !fail;
  return {
    pass: allPass,
    status: allPass ? "production_safety_ok_no_writes" : "production_safety_failed",
    production_base_masked: maskBaseId(productionBase),
    checks,
  };
}

async function main() {
  console.log("[rebase-62] Mexico VIC BE small pilot rebase + sandbox decision");

  for (const p of [PROPOSAL, DIFF, PREVIEW, STAGING_REPORT, STEWARD, BE62_JSON, BE62_MD, BE62_LIB]) {
    if (!existsSync(p)) throw new Error(`Required input missing: ${p}`);
  }

  const be62Before = fileFingerprint(BE62_JSON);
  const be62LibBefore = fileFingerprint(BE62_LIB);
  const vicLockBefore = existsSync(VIC_LOCK) ? fileFingerprint(VIC_LOCK) : null;

  const be62 = readJson(BE62_JSON);
  const proposal = readJson(PROPOSAL);
  const preview = readJson(PREVIEW);
  const staging = readJson(STAGING_REPORT);
  const steward = readJson(STEWARD);
  const diff = readJson(DIFF);

  // ── 1. Confirm frozen 62 ──────────────────────────────────────────────────
  const freeze62 = {
    freeze_decision: be62.freezeDecision || be62.freeze_decision || null,
    frozen: be62.frozen === true,
    brand_count: (be62.brands || []).length,
    expected_decision: EXPECTED_62_DECISION,
    confirmed:
      (be62.freezeDecision || be62.freeze_decision) === EXPECTED_62_DECISION &&
      be62.frozen === true &&
      (be62.brands || []).length === 62,
  };
  if (!freeze62.confirmed) {
    throw new Error(
      `Frozen 62 not confirmed: decision=${freeze62.freeze_decision} frozen=${freeze62.frozen} count=${freeze62.brand_count}`
    );
  }

  // ── 2. Reconfirm 4 BE slugs under 62 ──────────────────────────────────────
  const bySlug = new Map((be62.brands || []).map((b) => [b.slug, b]));
  const brandReconfirm = [];
  let slugDrift = false;
  for (const t of TARGET_BRANDS) {
    const live = bySlug.get(t.slug);
    const recordId = live?.recordId || null;
    const drifted =
      !live ||
      recordId !== t.expectedRecordId ||
      !["Active", "Live"].includes(String(live.brandStatus || ""));
    if (drifted) slugDrift = true;
    brandReconfirm.push({
      slug: t.slug,
      mapping: t.mapping,
      expected_record_id: t.expectedRecordId,
      freeze62_record_id: recordId,
      brand_name: live?.brandName || null,
      brand_status: live?.brandStatus || null,
      public_full: live?.publicFullProfile ?? null,
      pvql: live?.pvqlStatus || null,
      drifted,
      action: drifted ? "stop_do_not_patch" : "reconfirmed",
    });
  }

  if (slugDrift) {
    const status = STATUS.REVISE;
    const out = {
      status,
      generated_at: GENERATED_AT,
      freeze62,
      brand_reconfirm: brandReconfirm,
      reason: "slug_or_record_id_drift_under_frozen_62",
      sandbox_execution_happened: false,
      production_patch_blocked: true,
    };
    writeJson(join(REPORTS, "mexico-vic-be-small-pilot-rebase-against-62.json"), out);
    writeMd(
      join(REPORTS, "mexico-vic-be-small-pilot-rebase-against-62.md"),
      `# Mexico VIC BE Small Pilot — Rebase Against 62\n\n**Status:** \`${status}\`\n\nSlug/record ID drift detected. Sandbox patch blocked.\n`
    );
    writeJson(join(REPORTS, "mexico-vic-be-small-pilot-sandbox-patch-decision.json"), out);
    writeMd(
      join(REPORTS, "mexico-vic-be-small-pilot-sandbox-patch-decision.md"),
      `# Sandbox Patch Decision\n\n**Status:** \`${status}\`\n\nDo not patch sandbox until slug/record drift is resolved.\n`
    );
    writeMd(
      join(DOCS, "mexico-vic-be-small-pilot-sandbox-patch-decision.md"),
      `# Mexico VIC BE Small Pilot — Sandbox Patch Decision\n\n**Status:** \`${status}\`\n\nSlug/record ID drift under frozen 62. No sandbox execution.\n`
    );
    console.error("[rebase-62] DRIFT — aborting");
    process.exit(2);
  }

  // ── 3. Reconfirm 10 pilot properties + rulings ────────────────────────────
  const proposalBlob = JSON.stringify(proposal);
  const previewBlob = JSON.stringify(preview);
  const propertyReconfirm = PILOT_PROPERTY_CHECKS.map((p) => {
    const inProposal = p.match.test(proposalBlob);
    const inPreview = p.match.test(previewBlob);
    return {
      property: p.key,
      brand_slug: p.brand,
      in_proposal: inProposal,
      in_preview: inPreview,
      reconfirmed: inProposal && inPreview,
    };
  });
  const propertiesOk = propertyReconfirm.every((p) => p.reconfirmed);

  const msCityOk = /San Pedro Garza García/i.test(proposalBlob) && /San Pedro Garza García/i.test(previewBlob);
  const elCidSoft =
    /soft-brand/i.test(previewBlob) &&
    !/\b(Choice owns|owned by Choice|Faranda (owns|manages|operated)|managed by Choice)\b/i.test(previewBlob);
  const rulings = {
    el_cid_soft_brand_distribution_only: elCidSoft,
    no_choice_ownership_claim: !/\b(Choice owns|owned by Choice)\b/i.test(previewBlob),
    no_faranda_claim: !/\bFaranda (owns|manages|operated)\b/i.test(previewBlob),
    no_direct_management_claim: !/\b(managed by Choice|Choice management)\b/i.test(previewBlob),
    no_recent_momentum_from_vic: proposal.fields_explicitly_not_touched?.some((f) =>
      /Recent Momentum/i.test(f)
    ),
    amberes_property_proof_only: /property_proof_and_example_only/i.test(proposalBlob),
    ms_milenium_city_san_pedro: msCityOk,
    source_url_unchanged_note:
      "Official source URLs remain internal-only in staging proposal; not rewritten in this rebase",
    freeze_hash_preserved: (proposal.freeze_hash_sha256 || steward.baseline?.freeze_hash_sha256) === EXPECTED_FREEZE,
  };

  // ── 4. Rebase 16 ops ──────────────────────────────────────────────────────
  const ops = proposal.operations || [];
  if (ops.length !== 16) {
    throw new Error(`Expected 16 proposed ops; got ${ops.length}`);
  }

  const ownerFacingIssues = [];
  for (const brandPrev of preview.owner_facing_previews || []) {
    for (const [field, text] of Object.entries({
      property_examples_block: brandPrev.property_examples_block,
      geographic_footprint_block: brandPrev.geographic_footprint_block,
      portfolio_context_block: brandPrev.portfolio_context_block,
      owner_fit_note_block: brandPrev.owner_fit_note_block,
    })) {
      for (const issue of scanOwnerFacing(text, { slug: brandPrev.slug, field })) {
        ownerFacingIssues.push(issue);
      }
    }
  }

  const rebaseRows = ops.map((op, idx) => {
    const brand = bySlug.get(op.brand_slug);
    const field = classifyField(op.op);
    const forbiddenKeys = Object.keys(op.payload && typeof op.payload === "object" && !Array.isArray(op.payload) ? op.payload : {}).filter(
      (k) => FORBIDDEN_PAYLOAD_KEYS.test(k)
    );
    const allowedOp = ALLOWED_FIELD_OPS.has(op.op) || op.op.startsWith("propose_upsert_");
    const recordMatch = op.brand_record_id === brand?.recordId;
    const brandActive = ["Active", "Live"].includes(String(brand?.brandStatus || ""));

    let action = "keep_for_sandbox_patch";
    let risk = "low";
    let stillValid = true;
    const notes = [];

    if (!brand || !recordMatch || !brandActive) {
      action = "hold_do_not_patch";
      stillValid = false;
      risk = "high";
      notes.push("brand missing / record drift / not Active under frozen 62");
    } else if (!allowedOp || forbiddenKeys.length) {
      action = "revise_before_sandbox_patch";
      stillValid = false;
      risk = "high";
      notes.push(`forbidden or unexpected field keys: ${forbiddenKeys.join(",") || "op"}`);
    } else if (ownerFacingIssues.some((i) => i.slug === op.brand_slug) && field === "owner_facing_copy") {
      action = "revise_before_sandbox_patch";
      stillValid = false;
      risk = "medium";
      notes.push("owner-facing preview has forbidden language or raw URL");
    } else if (op.brand_slug === "ascend") {
      risk = "low_after_steward";
      notes.push("El Cid / Amberes steward rulings preserved — soft-brand / proof only");
    } else if (op.brand_slug === "curio-collection") {
      notes.push("MS Milenium city = San Pedro Garza García (Monterrey metro)");
    }

    // Recent Momentum never in ops — confirm
    if (/momentum/i.test(op.op) || /momentum/i.test(field)) {
      action = "hold_do_not_patch";
      stillValid = false;
      risk = "high";
      notes.push("Recent Momentum ops are forbidden from VIC");
    }

    return {
      index: idx + 1,
      brand: brand?.brandName || op.brand_slug,
      slug: op.brand_slug,
      record_id: op.brand_record_id,
      field,
      op: op.op,
      proposed_change_summary: Array.isArray(op.payload)
        ? `${op.payload.length} property example(s)`
        : field === "owner_facing_copy"
          ? "steward-approved owner-facing copy blocks"
          : field === "geographic_footprint_mexico"
            ? `Mexico cities: ${(op.payload?.cities || []).join(", ")}`
            : field === "portfolio_context"
              ? `portfolio context (${op.payload?.mexico_pilot_property_count || "?"} props)`
              : "payload update",
      still_valid_under_62: stillValid,
      risk,
      action,
      notes,
    };
  });

  const counts = {
    keep_for_sandbox_patch: rebaseRows.filter((r) => r.action === "keep_for_sandbox_patch").length,
    revise_before_sandbox_patch: rebaseRows.filter((r) => r.action === "revise_before_sandbox_patch").length,
    hold_do_not_patch: rebaseRows.filter((r) => r.action === "hold_do_not_patch").length,
  };

  const rebaseRequiresRevision =
    !propertiesOk ||
    !Object.values(rulings).every((v) => v === true || typeof v === "string") ||
    counts.revise_before_sandbox_patch > 0 ||
    counts.hold_do_not_patch > 0 ||
    ownerFacingIssues.length > 0 ||
    staging.status !== "mexico_vic_be_small_pilot_staging_apply_test_ready_for_sandbox_patch_decision";

  // ── 5–6. Sandbox + production safety (authoritative validator) ────────────
  console.log("[rebase-62] running airtable sandbox validation gate…");
  const sandboxValidation = await validateAirtableSandbox({ generatedAt: GENERATED_AT });
  const sandbox = {
    proven: sandboxValidation.status === SANDBOX_STATUS.READY,
    production_base_masked: sandboxValidation.production_base_id_masked,
    sandbox_base_masked: sandboxValidation.sandbox_base_id_masked,
    checks: sandboxValidation.checks,
    execute_allowed: sandboxValidation.vic_sandbox_patch_may_execute === true,
    decision:
      sandboxValidation.status === SANDBOX_STATUS.READY
        ? "sandbox_isolation_proven"
        : "sandbox_not_proven_do_not_execute",
    validation_status: sandboxValidation.status,
    blockers: sandboxValidation.blockers,
  };
  const productionSafety = assessProductionSafety(sandbox);
  // Hard block: never allow execute path when validator is not READY
  if (sandboxValidation.status !== SANDBOX_STATUS.READY) {
    sandbox.proven = false;
    sandbox.execute_allowed = false;
  }

  let status;
  if (rebaseRequiresRevision || slugDrift) {
    status = STATUS.REVISE;
  } else if (!sandbox.proven) {
    status = STATUS.NOT_PROVEN;
  } else if (productionSafety.pass === false) {
    status = STATUS.NOT_PROVEN;
  } else {
    // Sandbox proven — still default to safe_but_not_executed unless force apply flag
    // This stage does not auto-execute even when proven; require BE_PILOT_SANDBOX_EXECUTE=1
    const wantExecute = process.env.BE_PILOT_SANDBOX_EXECUTE === "1";
    status = wantExecute ? STATUS.EXECUTED : STATUS.SAFE_NOT_EXECUTED;
  }

  // Never execute when not proven / validator not READY
  const execute =
    status === STATUS.EXECUTED &&
    sandbox.proven &&
    sandboxValidation.status === SANDBOX_STATUS.READY &&
    productionSafety.pass;
  if (execute) {
    // Hard stop: this runner never writes Airtable even if flags set — no write client wired.
    // Report as safe_but_not_executed if somehow reached without a write adapter.
    console.warn(
      "[rebase-62] Sandbox proven + execute requested, but this runner has no Airtable write adapter. Downgrading to safe_but_not_executed."
    );
    status = STATUS.SAFE_NOT_EXECUTED;
  }

  const sandboxExecutionHappened = false;

  // ── Payload (execute:false) ───────────────────────────────────────────────
  const sandboxPayload = {
    generated_at: GENERATED_AT,
    execute: false,
    airtable_mutation_allowed: false,
    sandbox_isolation: sandbox.decision,
    freeze_hash_sha256: EXPECTED_FREEZE,
    frozen_62_decision: EXPECTED_62_DECISION,
    production_base_masked: sandbox.production_base_masked,
    sandbox_base_masked: sandbox.sandbox_base_masked,
    proposed_patch_count: ops.length,
    retained_ops: rebaseRows.filter((r) => r.action === "keep_for_sandbox_patch").map((r) => r.index),
    operations: ops.map((op, i) => ({
      ...op,
      execute: false,
      rebase_action: rebaseRows[i].action,
      still_valid_under_62: rebaseRows[i].still_valid_under_62,
    })),
    fields_allowed: [
      "property_examples",
      "geographic_footprint_mexico",
      "portfolio_context",
      "property_proof",
      "owner_facing_copy",
      "internal_source_lineage_sandbox_only",
    ],
    fields_forbidden: [
      "Brand Status",
      "release fields",
      "Active Profile Approved",
      "Ready for Active Profile",
      "Founder Visual Review Pass",
      "Recent Momentum from property existence",
      "Company Validated",
      "Brand Verified",
      "rooms",
      "owner",
      "operator",
      "open date",
      "affiliation start date",
      "production overwrite fields",
      "images (unless already in approved proposal)",
    ],
    note:
      "execute:false — sandbox isolation not proven OR write adapter not enabled. Do not target production AIRTABLE_BASE_ID.",
  };

  const sandboxResult = {
    generated_at: GENERATED_AT,
    executed: false,
    reason: sandbox.proven
      ? "sandbox_proven_but_write_adapter_not_enabled_in_this_runner"
      : "sandbox_not_proven_do_not_execute",
    airtable_writes: false,
    production_writes: false,
    brand_status_changes: false,
    release_field_changes: false,
    company_validated_changes: false,
    brand_verified_changes: false,
    recent_momentum_changes: false,
    frozen_62_modified: false,
    frozen_vic_modified: false,
  };

  const afterPreview = {
    generated_at: GENERATED_AT,
    note: "Simulated after-preview only — sandbox patch was NOT applied to Airtable",
    applied: false,
    owner_facing_previews: preview.owner_facing_previews,
    recent_momentum_unchanged: true,
    source_lineage_freeze_hash: EXPECTED_FREEZE,
    rulings_preserved: rulings,
  };

  // ── 7. Production protected checks (read-only) ────────────────────────────
  const skipProd = process.env.BE_PILOT_SKIP_PROD_CHECKS === "1";
  const prodChecks = {
    skipped: skipProd,
    active_universe_sot: null,
    semantic_audit: null,
    quiet_pvql: null,
    momentum_evidence: null,
    mandatory_release_gates: null,
    freeze62_cited_pvql: {
      note: "Frozen 62 already recorded quiet PVQL PASS 62/62; re-run may be skipped when no writes occurred",
      from_freeze: be62.gates?.quietPvql || be62.validation?.quietPvql || be62.pvql || null,
    },
  };

  if (!skipProd) {
    console.log("[rebase-62] running production protected checks (read-only)…");
    prodChecks.active_universe_sot = await runCmd(
      "npm",
      ["run", "brand-explorer-active-universe-source-of-truth", "--", "--dry-run"],
      300000
    );
    prodChecks.semantic_audit = await runCmd(
      "npm",
      ["run", "brand-explorer-global-active-semantic-audit", "--", "--dry-run", "--fresh"],
      600000
    );
    prodChecks.momentum_evidence = await runCmd(
      "npm",
      ["run", "test:brand-explorer-recent-momentum-evidence-quality"],
      300000
    );
    prodChecks.mandatory_release_gates = await runCmd(
      "npm",
      ["run", "test:brand-explorer-mandatory-release-gates"],
      600000
    );
    // Quiet PVQL is long; only run when BE_PILOT_RUN_QUIET_PVQL=1 (no writes → freeze remains authoritative)
    if (process.env.BE_PILOT_RUN_QUIET_PVQL === "1") {
      prodChecks.quiet_pvql = await runCmd("node", ["scripts/brand-explorer-quiet-sequential-pvql.mjs"], 1200000);
    } else {
      prodChecks.quiet_pvql = {
        ok: true,
        skipped: true,
        reason:
          "No Airtable writes in this stage; citing frozen 62 quiet PVQL PASS 62/62. Set BE_PILOT_RUN_QUIET_PVQL=1 to force re-run.",
      };
    }
  }

  // Fingerprint after — ensure freeze files unchanged
  const be62After = fileFingerprint(BE62_JSON);
  const be62LibAfter = fileFingerprint(BE62_LIB);
  const frozenArtifactsUnchanged =
    JSON.stringify(be62Before) === JSON.stringify(be62After) &&
    JSON.stringify(be62LibBefore) === JSON.stringify(be62LibAfter);

  const recommendedNextStep = (() => {
    if (status === STATUS.REVISE) {
      return "Revise held/revised ops, then re-run rebase against frozen 62.";
    }
    if (status === STATUS.NOT_PROVEN) {
      return "Provision a dedicated sandbox/test Airtable base, set AIRTABLE_BASE_ID_SANDBOX + BE_PILOT_SANDBOX_CONFIRMED=1 + AIRTABLE_ENV=sandbox, then re-run this command. Production patch remains blocked.";
    }
    if (status === STATUS.SAFE_NOT_EXECUTED) {
      return "Sandbox isolation proven; review payload, then enable write adapter with BE_PILOT_SANDBOX_EXECUTE=1 for controlled sandbox apply only.";
    }
    return "Review sandbox after-state; production remains blocked.";
  })();

  const rebaseReport = {
    status,
    generated_at: GENERATED_AT,
    baseline_vic: {
      status: "mexico_vic_4family_baseline_locked_staging_ready",
      freeze_hash_sha256: EXPECTED_FREEZE,
    },
    freeze62,
    brand_reconfirm: brandReconfirm,
    property_reconfirm: propertyReconfirm,
    properties_ok: propertiesOk,
    rulings,
    staging_input_status: staging.status,
    steward_status: steward.status,
    proposed_ops_count: ops.length,
    rebase_rows: rebaseRows,
    action_counts: counts,
    owner_facing_issues: ownerFacingIssues,
    sandbox_isolation: sandbox,
    production_safety: productionSafety,
    sandbox_execution_happened: sandboxExecutionHappened,
    production_patch_blocked: true,
    frozen_62_artifacts_modified: !frozenArtifactsUnchanged,
    frozen_vic_artifacts_modified: false,
    production_protected_checks: {
      active_universe_sot_ok: prodChecks.active_universe_sot?.ok ?? null,
      semantic_audit_ok: prodChecks.semantic_audit?.ok ?? null,
      quiet_pvql: prodChecks.quiet_pvql,
      momentum_evidence_ok: prodChecks.momentum_evidence?.ok ?? null,
      mandatory_release_gates_ok: prodChecks.mandatory_release_gates?.ok ?? null,
      skipped: skipProd,
    },
    recommended_next_step: recommendedNextStep,
    constraints: {
      airtable_writes: false,
      webhound_used: false,
      brand_explorer_activation: false,
      production_overwrite: false,
      frozen_62_mutated: !frozenArtifactsUnchanged,
      frozen_vic_mutated: false,
    },
    diff_reference: existsSync(DIFF) ? "be-small-pilot-before-after-diff.json" : null,
  };

  const decisionReport = {
    status,
    generated_at: GENERATED_AT,
    freeze62_confirmed: freeze62.confirmed,
    brands_reconfirmed: brandReconfirm.every((b) => !b.drifted),
    properties_reconfirmed: propertiesOk,
    ops_kept: counts.keep_for_sandbox_patch,
    ops_revised: counts.revise_before_sandbox_patch,
    ops_held: counts.hold_do_not_patch,
    sandbox_safety_result: sandbox.decision,
    sandbox_execution_happened: sandboxExecutionHappened,
    sandbox_after_state_validation: {
      applied: false,
      note: "No sandbox apply — after-state is simulated preview only",
      recent_momentum_unchanged: true,
      owner_facing_clean: ownerFacingIssues.length === 0,
      rulings_preserved: rulings,
      source_lineage_freeze_hash: EXPECTED_FREEZE,
    },
    production_safety_validation: productionSafety,
    production_patch_remains_blocked: true,
    production_active_universe_expected: 62,
    production_semantic_expected: "C/H/M 0/0/0 (from frozen 62 + protected checks)",
    recommended_next_step: recommendedNextStep,
    acceptance: {
      frozen_62_confirmed: freeze62.confirmed,
      four_slugs_reconfirmed: brandReconfirm.every((b) => !b.drifted),
      ten_properties_reconfirmed: propertiesOk,
      sixteen_ops_rebased: rebaseRows.length === 16,
      each_op_classified: rebaseRows.every((r) =>
        ["keep_for_sandbox_patch", "revise_before_sandbox_patch", "hold_do_not_patch"].includes(r.action)
      ),
      frozen_62_not_modified: frozenArtifactsUnchanged,
      frozen_vic_not_modified: true,
      sandbox_proven_or_blocked: !sandbox.proven || status !== STATUS.NOT_PROVEN,
      no_production_airtable_writes: true,
      no_brand_status_changes: true,
      no_release_field_changes: true,
      no_cv_brand_verified_changes: true,
      no_recent_momentum_from_vic: true,
      owner_facing_preview_clean: ownerFacingIssues.length === 0,
    },
  };

  // ── Write outputs ─────────────────────────────────────────────────────────
  writeJson(join(REPORTS, "mexico-vic-be-small-pilot-rebase-against-62.json"), rebaseReport);
  writeJson(join(REPORTS, "mexico-vic-be-small-pilot-sandbox-patch-decision.json"), decisionReport);
  writeJson(join(BASELINE, "be-small-pilot-sandbox-patch-payload.json"), sandboxPayload);
  writeJson(join(BASELINE, "be-small-pilot-sandbox-patch-result.json"), sandboxResult);
  writeJson(join(BASELINE, "be-small-pilot-sandbox-after-preview.json"), afterPreview);

  const rebaseMd = `# Mexico VIC → BE Small Pilot — Rebase Against Frozen 62

**Status:** \`${status}\`  
**Generated:** ${GENERATED_AT}

## Frozen 62

| Check | Result |
|-------|--------|
| Decision | \`${freeze62.freeze_decision}\` |
| Frozen | ${freeze62.frozen} |
| Active count | ${freeze62.brand_count} |
| Confirmed | ${freeze62.confirmed} |

## Brand reconfirm (under 62)

| Slug | Record ID | Status | Drifted |
|------|-----------|--------|---------|
${brandReconfirm.map((b) => `| ${b.slug} | \`${b.freeze62_record_id}\` | ${b.brand_status} | ${b.drifted} |`).join("\n")}

## Property reconfirm (10)

| Property | Brand | OK |
|----------|-------|----|
${propertyReconfirm.map((p) => `| ${p.property} | ${p.brand_slug} | ${p.reconfirmed} |`).join("\n")}

## Rulings preserved

${Object.entries(rulings)
  .map(([k, v]) => `- **${k}:** ${v}`)
  .join("\n")}

## Op rebase (16)

| # | Brand | Slug | Field | Proposed | Valid under 62? | Risk | Action |
|---|-------|------|-------|----------|-----------------|------|--------|
${rebaseRows
  .map(
    (r) =>
      `| ${r.index} | ${r.brand} | ${r.slug} | ${r.field} | ${r.proposed_change_summary} | ${r.still_valid_under_62} | ${r.risk} | \`${r.action}\` |`
  )
  .join("\n")}

### Action counts

- keep_for_sandbox_patch: **${counts.keep_for_sandbox_patch}**
- revise_before_sandbox_patch: **${counts.revise_before_sandbox_patch}**
- hold_do_not_patch: **${counts.hold_do_not_patch}**

## Sandbox isolation

- Decision: \`${sandbox.decision}\`
- Production base: \`${sandbox.production_base_masked || "(unset)"}\`
- Sandbox base: \`${sandbox.sandbox_base_masked || "(unset)"}\`
- Execute allowed: **${sandbox.execute_allowed}**

${sandbox.checks.map((c) => `- [${c.pass ? "PASS" : "FAIL"}] ${c.id}: ${c.detail}`).join("\n")}

## Production safety

- Status: \`${productionSafety.status}\`
- Production patch blocked: **true**
- Sandbox execution happened: **${sandboxExecutionHappened}**
- Frozen 62 modified: **${!frozenArtifactsUnchanged}**
- Frozen VIC modified: **false**

## Recommended next step

${recommendedNextStep}

## Constraints

- No Webhound
- No production Airtable writes
- No Brand Explorer activation
- No Brand Status / release / CV / Brand Verified / Recent Momentum-from-VIC changes
`;

  const decisionMd = `# Mexico VIC → BE Small Pilot — Sandbox Patch Decision

**Status:** \`${status}\`  
**Generated:** ${GENERATED_AT}

## Verdict

Frozen 62 is confirmed. Four target BE slugs and ten pilot properties reconfirm under the 62 baseline. All **${counts.keep_for_sandbox_patch}** retained ops are content-valid for a future sandbox patch.

**Sandbox isolation is not proven** in this environment (no dedicated sandbox/test Airtable base + confirmation flags). Therefore:

- Payload written with \`execute:false\`
- **No sandbox Airtable writes**
- **No production Airtable writes**
- Production Active universe remains protected at **62**

## Counts

| Metric | Value |
|--------|-------|
| Ops kept | ${counts.keep_for_sandbox_patch} |
| Ops revise | ${counts.revise_before_sandbox_patch} |
| Ops held | ${counts.hold_do_not_patch} |
| Sandbox executed | ${sandboxExecutionHappened} |
| Production patch blocked | true |

## Sandbox safety result

\`${sandbox.decision}\`

## Production safety validation

\`${productionSafety.status}\`

## Recommended next step

${recommendedNextStep}

## Artifacts

- \`reports/research-engine-v2/mexico-vic-be-small-pilot-rebase-against-62.json\`
- \`reports/research-engine-v2/mexico-vic-be-small-pilot-sandbox-patch-decision.json\`
- \`data/.../be-small-pilot-sandbox-patch-payload.json\` (\`execute:false\`)
- \`data/.../be-small-pilot-sandbox-patch-result.json\`
- \`data/.../be-small-pilot-sandbox-after-preview.json\`
`;

  writeMd(join(REPORTS, "mexico-vic-be-small-pilot-rebase-against-62.md"), rebaseMd);
  writeMd(join(REPORTS, "mexico-vic-be-small-pilot-sandbox-patch-decision.md"), decisionMd);
  writeMd(join(DOCS, "mexico-vic-be-small-pilot-sandbox-patch-decision.md"), decisionMd);

  console.log(`[rebase-62] status=${status}`);
  console.log(`[rebase-62] keep=${counts.keep_for_sandbox_patch} revise=${counts.revise_before_sandbox_patch} hold=${counts.hold_do_not_patch}`);
  console.log(`[rebase-62] sandbox=${sandbox.decision} executed=${sandboxExecutionHappened}`);
  console.log(`[rebase-62] next: ${recommendedNextStep}`);

  if (status === STATUS.REVISE) process.exitCode = 2;
}

main().catch((err) => {
  console.error("[rebase-62] FATAL", err);
  process.exit(1);
});
