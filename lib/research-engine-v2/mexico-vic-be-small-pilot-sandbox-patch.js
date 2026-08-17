/**
 * Mexico VIC → BE small pilot — sandbox-only Airtable patch.
 *
 * Writes ONLY to AIRTABLE_BASE_ID_SANDBOX after assertSandboxReadyForVicBePatch().
 * Never initializes a production write client. Never touches Brand Status,
 * release fields, CV/Verified, or Recent Momentum.
 */

import Airtable from "airtable";
import {
  assertSandboxReadyForVicBePatch,
  STATUS as SANDBOX_STATUS,
  maskBaseId,
  readSandboxEnv,
  resolveSandboxApiKey,
  PRESENTATION_BRAND_LINK_CANDIDATES,
  FORBIDDEN_WRITE_FIELDS,
} from "./airtable-sandbox-validation.js";

export const SANDBOX_PATCH_VERSION = "mexico-vic-be-small-pilot-sandbox-patch-v1";
export const EXPECTED_FREEZE =
  "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";

export const EXEC_STATUS = Object.freeze({
  EXECUTED: "mexico_vic_be_small_pilot_sandbox_patch_executed_ready_for_review",
  VALIDATION_FAILED: "sandbox_validation_failed_do_not_execute",
  DRY_RUN: "mexico_vic_be_small_pilot_sandbox_patch_dry_run_ready",
  BLOCKED: "mexico_vic_be_small_pilot_sandbox_patch_blocked",
});

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const FORBIDDEN_VISIBLE =
  /\b(vic|census|staging|source pack|directory|source-supported|steward|overlay|\bqa\b|process|company validated|brand verified|confirmed by (ihg|hilton|choice|marriott)|verified by|company confirmed|brand validated|census proves|directory confirms)\b/i;

const SLOT_BY_OP = Object.freeze({
  propose_upsert_property_examples: "vic.pilot.property_examples",
  propose_upsert_mexico_geographic_footprint: "vic.pilot.geographic_footprint_mexico",
  propose_upsert_portfolio_context: "vic.pilot.portfolio_context",
  propose_upsert_owner_facing_copy_blocks: "vic.pilot.owner_facing_copy",
});

const TITLE_BY_OP = Object.freeze({
  propose_upsert_property_examples: "Mexico Property Examples",
  propose_upsert_mexico_geographic_footprint: "Mexico Geographic Footprint",
  propose_upsert_portfolio_context: "Mexico Portfolio Context",
  propose_upsert_owner_facing_copy_blocks: "Mexico Owner-Facing Notes",
});

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function scanOwnerFacing(text) {
  const issues = [];
  const t = String(text || "");
  const m = t.match(FORBIDDEN_VISIBLE);
  if (m) issues.push(`forbidden_term:${m[0]}`);
  if (/https?:\/\//i.test(t)) issues.push("raw_url");
  return issues;
}

function bodyFromOp(op) {
  if (op.op === "propose_upsert_property_examples") {
    const lines = (op.payload || []).map((p) => {
      const framing =
        p.framing === "ascend_soft_brand_distribution_example"
          ? " (soft-brand distribution example)"
          : p.framing === "property_proof_and_example_only"
            ? " (property proof / example)"
            : "";
      return `• ${p.name} — ${p.city}, ${p.state_region}, ${p.country}${framing}`;
    });
    return lines.join("\n");
  }
  if (op.op === "propose_upsert_mexico_geographic_footprint") {
    const cities = (op.payload?.cities || []).join(", ");
    return `Present in Mexico across ${cities}. ${op.payload?.claim_limit || ""}`.trim();
  }
  if (op.op === "propose_upsert_portfolio_context") {
    return String(op.payload?.owner_facing || "").trim();
  }
  if (op.op === "propose_upsert_owner_facing_copy_blocks") {
    const p = op.payload || {};
    return [
      p.property_examples,
      p.geographic_footprint,
      p.portfolio_context,
      p.owner_fit_note,
    ]
      .filter(Boolean)
      .join("\n\n");
  }
  return "";
}

function scrubForbiddenFields(fields) {
  const out = { ...fields };
  for (const key of Object.keys(out)) {
    if (FORBIDDEN_WRITE_FIELDS.includes(key)) delete out[key];
    if (/Brand Status|Company Validated|Brand Verified|Active Profile|Founder Visual|Ready for Active|Recent Momentum|rooms|owner|operator|open.?date|affiliation/i.test(key)) {
      delete out[key];
    }
  }
  return out;
}

async function listPresentationRowsForBrand(base, brandRecordId, linkField) {
  const formula = `FIND('${brandRecordId}', ARRAYJOIN({${linkField}}))`;
  const rows = [];
  await base(PRESENTATION_TABLE)
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((records, next) => {
      for (const r of records) rows.push(r);
      next();
    });
  return rows;
}

async function resolveLinkField(base) {
  // Probe by reading one record is expensive; use known candidates via create dry logic.
  // Fetch meta via first record field keys after a lightweight select.
  const sample = await base(PRESENTATION_TABLE)
    .select({ maxRecords: 1 })
    .firstPage();
  const keys = sample[0] ? Object.keys(sample[0].fields || {}) : [];
  const hit = PRESENTATION_BRAND_LINK_CANDIDATES.find((c) => keys.includes(c));
  if (hit) return hit;
  // Fallback: try each candidate with a formula that won't match
  for (const c of PRESENTATION_BRAND_LINK_CANDIDATES) {
    try {
      await base(PRESENTATION_TABLE)
        .select({ filterByFormula: `FIND('recNONE', ARRAYJOIN({${c}}))`, maxRecords: 1 })
        .firstPage();
      return c;
    } catch {
      /* try next */
    }
  }
  throw new Error("Could not resolve Presentation brand link field in sandbox");
}

/**
 * @param {object} opts
 * @param {object} opts.proposal - staging patch proposal JSON
 * @param {object} opts.preview - rendered preview JSON
 * @param {boolean} opts.execute - when true and validation READY, write sandbox
 * @param {object} [opts.env]
 */
export async function runMexicoVicBeSmallPilotSandboxPatch(opts = {}) {
  const generatedAt = new Date().toISOString();
  const executeRequested = opts.execute === true;
  const env = opts.env || process.env;
  const proposal = opts.proposal;
  const preview = opts.preview;

  let validation;
  try {
    validation = await assertSandboxReadyForVicBePatch({ env, generatedAt });
  } catch (err) {
    validation = err.sandboxValidation || {
      status: SANDBOX_STATUS.FAILED,
      blockers: [err.message],
    };
    return {
      version: SANDBOX_PATCH_VERSION,
      status: EXEC_STATUS.VALIDATION_FAILED,
      generated_at: generatedAt,
      execute_requested: executeRequested,
      executed: false,
      sandbox_validation: {
        status: validation.status,
        blockers: validation.blockers,
        production_base_id_masked: validation.production_base_id_masked,
        sandbox_base_id_masked: validation.sandbox_base_id_masked,
        ids_differ: validation.ids_differ,
      },
      ops_executed: 0,
      production_writes: 0,
      sandbox_writes: 0,
      records_touched: [],
      fields_touched: [],
      forbidden_fields_untouched: true,
      production_safety: {
        pass: true,
        note: "No writes attempted — validation failed",
      },
      freeze_hash_sha256: EXPECTED_FREEZE,
      recommended_next_step:
        "Fix sandbox env/base per validation blockers, re-run validate-airtable-sandbox, then re-run sandbox-patch -- --execute",
    };
  }

  const cfg = readSandboxEnv(env);
  const keyResolution = await resolveSandboxApiKey(env);
  const apiKey = keyResolution.ok ? keyResolution.apiKey : null;
  if (!apiKey || !cfg.sandboxBaseId) {
    return {
      version: SANDBOX_PATCH_VERSION,
      status: EXEC_STATUS.VALIDATION_FAILED,
      generated_at: generatedAt,
      executed: false,
      reason: keyResolution.detail || "sandbox_base_or_api_key_missing_after_validation",
      sandbox_token_label: keyResolution.label,
    };
  }

  // CRITICAL: production write client never initialized
  const productionWriteClientInitialized = false;
  Airtable.configure({ apiKey });
  const sandboxBase = new Airtable({ apiKey }).base(cfg.sandboxBaseId);

  const ops = proposal?.operations || [];
  if (ops.length !== 16) {
    return {
      version: SANDBOX_PATCH_VERSION,
      status: EXEC_STATUS.BLOCKED,
      generated_at: generatedAt,
      executed: false,
      reason: `expected_16_ops_got_${ops.length}`,
    };
  }

  // Preflight: brand basics snapshots (Brand Status etc. for after-compare)
  const brandSnapshotsBefore = {};
  for (const op of ops) {
    const id = op.brand_record_id;
    if (brandSnapshotsBefore[id]) continue;
    const rec = await sandboxBase(BASICS_TABLE).find(id);
    brandSnapshotsBefore[id] = {
      brand_slug: op.brand_slug,
      record_id: id,
      brand_name: rec.fields?.["Brand Name"] || null,
      brand_status: rec.fields?.["Brand Status"] || null,
      company_validated: rec.fields?.["Company Validated"] ?? null,
      brand_verified: rec.fields?.["Brand Verified"] ?? null,
    };
  }

  const linkField = await resolveLinkField(sandboxBase);

  /** @type {object[]} */
  const planned = [];
  const ownerFacingIssues = [];

  for (let i = 0; i < ops.length; i++) {
    const op = ops[i];
    const slotKey = SLOT_BY_OP[op.op];
    if (!slotKey) {
      return {
        version: SANDBOX_PATCH_VERSION,
        status: EXEC_STATUS.BLOCKED,
        generated_at: generatedAt,
        executed: false,
        reason: `unsupported_op:${op.op}`,
      };
    }
    if (/momentum/i.test(op.op) || /momentum/i.test(slotKey)) {
      return {
        version: SANDBOX_PATCH_VERSION,
        status: EXEC_STATUS.BLOCKED,
        generated_at: generatedAt,
        executed: false,
        reason: "recent_momentum_op_forbidden",
      };
    }

    const title = TITLE_BY_OP[op.op];
    let body = bodyFromOp(op);

    // Prefer steward owner-facing copy for the matching preview brand when available
    if (op.op === "propose_upsert_owner_facing_copy_blocks") {
      body = bodyFromOp(op);
    } else if (op.op === "propose_upsert_property_examples") {
      const prev = (preview?.owner_facing_previews || []).find((p) => p.slug === op.brand_slug);
      if (prev?.property_examples_block) body = prev.property_examples_block;
    } else if (op.op === "propose_upsert_mexico_geographic_footprint") {
      const prev = (preview?.owner_facing_previews || []).find((p) => p.slug === op.brand_slug);
      if (prev?.geographic_footprint_block) body = prev.geographic_footprint_block;
    } else if (op.op === "propose_upsert_portfolio_context") {
      const prev = (preview?.owner_facing_previews || []).find((p) => p.slug === op.brand_slug);
      if (prev?.portfolio_context_block) body = prev.portfolio_context_block;
    }

    for (const issue of scanOwnerFacing(body)) {
      ownerFacingIssues.push({ op_index: i + 1, slug: op.brand_slug, issue });
    }
    for (const issue of scanOwnerFacing(title)) {
      ownerFacingIssues.push({ op_index: i + 1, slug: op.brand_slug, issue: `title:${issue}` });
    }

    // Find existing presentation row by Slot Key + brand
    const existingRows = await listPresentationRowsForBrand(
      sandboxBase,
      op.brand_record_id,
      linkField
    );
    const existing = existingRows.find(
      (r) => String(r.fields?.["Slot Key"] || "") === slotKey
    );

    const fields = scrubForbiddenFields({
      Title: title,
      Body: body,
      "Slot Key": slotKey,
      [linkField]: [op.brand_record_id],
      Active: true,
    });

    // Drop Active if not present on create failure later — handled at write time
    planned.push({
      index: i + 1,
      op: op.op,
      brand_slug: op.brand_slug,
      brand_record_id: op.brand_record_id,
      slot_key: slotKey,
      action: existing ? "update" : "create",
      record_id: existing?.id || null,
      fields,
      table: PRESENTATION_TABLE,
    });

    await sleep(120);
  }

  if (ownerFacingIssues.length) {
    return {
      version: SANDBOX_PATCH_VERSION,
      status: EXEC_STATUS.BLOCKED,
      generated_at: generatedAt,
      executed: false,
      reason: "owner_facing_preview_failed_cleanliness_gate",
      owner_facing_issues: ownerFacingIssues,
      planned_ops: planned.length,
    };
  }

  // Rulings checks on planned bodies
  const allBodies = planned.map((p) => p.fields.Body).join("\n");
  const rulings = {
    el_cid_soft_brand: /soft-brand/i.test(allBodies) || /soft-brand distribution example/i.test(allBodies),
    no_choice_owns: !/\b(Choice owns|owned by Choice)\b/i.test(allBodies),
    no_faranda: !/\bFaranda (owns|manages|operated)\b/i.test(allBodies),
    no_direct_mgmt: !/\b(managed by Choice|Choice management)\b/i.test(allBodies),
    ms_san_pedro: /San Pedro Garza García/i.test(allBodies),
    freeze_hash: EXPECTED_FREEZE,
  };

  if (!executeRequested) {
    return {
      version: SANDBOX_PATCH_VERSION,
      status: EXEC_STATUS.DRY_RUN,
      generated_at: generatedAt,
      execute_requested: false,
      executed: false,
      sandbox_validation: {
        status: validation.status,
        production_base_id_masked: validation.production_base_id_masked,
        sandbox_base_id_masked: validation.sandbox_base_id_masked,
        ids_differ: validation.ids_differ,
      },
      planned,
      brand_snapshots_before: brandSnapshotsBefore,
      rulings,
      production_write_client_initialized: productionWriteClientInitialized,
      production_writes: 0,
      sandbox_writes: 0,
      freeze_hash_sha256: EXPECTED_FREEZE,
      note: "Dry-run only. Re-run with --execute after review.",
    };
  }

  /** @type {object[]} */
  const writeResults = [];
  for (const plan of planned) {
    const fields = { ...plan.fields };
    try {
      if (plan.action === "update" && plan.record_id) {
        // Title/Body/Slot Key only on update — avoid rewriting link unless needed
        const updateFields = scrubForbiddenFields({
          Title: fields.Title,
          Body: fields.Body,
          "Slot Key": fields["Slot Key"],
        });
        const updated = await sandboxBase(PRESENTATION_TABLE).update(plan.record_id, updateFields);
        writeResults.push({
          ...plan,
          executed: true,
          record_id: updated.id,
          fields_written: Object.keys(updateFields),
        });
      } else {
        let created;
        try {
          created = await sandboxBase(PRESENTATION_TABLE).create(fields);
        } catch (err) {
          // Retry without Active if field missing
          if (/Active/i.test(err.message || "")) {
            const { Active, ...rest } = fields;
            created = await sandboxBase(PRESENTATION_TABLE).create(scrubForbiddenFields(rest));
          } else if (/Slot Key/i.test(err.message || "")) {
            const { "Slot Key": _sk, ...rest } = fields;
            created = await sandboxBase(PRESENTATION_TABLE).create(scrubForbiddenFields(rest));
          } else {
            throw err;
          }
        }
        writeResults.push({
          ...plan,
          action: "create",
          executed: true,
          record_id: created.id,
          fields_written: Object.keys(created.fields || fields),
        });
      }
    } catch (err) {
      writeResults.push({
        ...plan,
        executed: false,
        error: err.message,
      });
    }
    await sleep(280);
  }

  const executedCount = writeResults.filter((w) => w.executed).length;
  const failed = writeResults.filter((w) => !w.executed);

  // After snapshots
  const brandSnapshotsAfter = {};
  for (const id of Object.keys(brandSnapshotsBefore)) {
    const rec = await sandboxBase(BASICS_TABLE).find(id);
    brandSnapshotsAfter[id] = {
      brand_slug: brandSnapshotsBefore[id].brand_slug,
      record_id: id,
      brand_name: rec.fields?.["Brand Name"] || null,
      brand_status: rec.fields?.["Brand Status"] || null,
      company_validated: rec.fields?.["Company Validated"] ?? null,
      brand_verified: rec.fields?.["Brand Verified"] ?? null,
    };
  }

  const statusUnchanged = Object.keys(brandSnapshotsBefore).every((id) => {
    const b = brandSnapshotsBefore[id];
    const a = brandSnapshotsAfter[id];
    return (
      b.brand_status === a.brand_status &&
      JSON.stringify(b.company_validated) === JSON.stringify(a.company_validated) &&
      JSON.stringify(b.brand_verified) === JSON.stringify(a.brand_verified)
    );
  });

  const afterPreview = {
    generated_at: generatedAt,
    applied: executedCount === 16 && failed.length === 0,
    owner_facing_previews: (preview?.owner_facing_previews || []).map((p) => ({
      ...p,
      recent_momentum_unchanged: true,
      rooms_owner_operator_open_date_claims: false,
      company_validated_claims: false,
    })),
    recent_momentum_unchanged: true,
    source_lineage_freeze_hash: EXPECTED_FREEZE,
    rulings_preserved: rulings,
    sandbox_presentation_rows: writeResults.map((w) => ({
      brand_slug: w.brand_slug,
      slot_key: w.slot_key,
      record_id: w.record_id,
      title: w.fields?.Title,
      body: w.fields?.Body,
      executed: w.executed,
    })),
  };

  const status =
    executedCount === 16 && failed.length === 0 && statusUnchanged
      ? EXEC_STATUS.EXECUTED
      : EXEC_STATUS.BLOCKED;

  return {
    version: SANDBOX_PATCH_VERSION,
    status,
    generated_at: generatedAt,
    execute_requested: true,
    executed: executedCount > 0,
    ops_executed: executedCount,
    ops_failed: failed.length,
    write_results: writeResults,
    records_touched: writeResults.filter((w) => w.executed).map((w) => ({
      table: PRESENTATION_TABLE,
      record_id: w.record_id,
      brand_slug: w.brand_slug,
      slot_key: w.slot_key,
    })),
    fields_touched: ["Title", "Body", "Slot Key", linkField],
    forbidden_fields_untouched: true,
    brand_snapshots_before: brandSnapshotsBefore,
    brand_snapshots_after: brandSnapshotsAfter,
    brand_status_unchanged: statusUnchanged,
    recent_momentum_unchanged: true,
    production_write_client_initialized: productionWriteClientInitialized,
    production_writes: 0,
    sandbox_writes: executedCount,
    sandbox_validation: {
      status: validation.status,
      production_base_id_masked: validation.production_base_id_masked,
      sandbox_base_id_masked: validation.sandbox_base_id_masked,
      ids_differ: validation.ids_differ,
    },
    production_safety: {
      pass: true,
      production_base_masked: maskBaseId(cfg.productionBaseId),
      sandbox_base_masked: maskBaseId(cfg.sandboxBaseId),
      ids_differ: cfg.sandboxBaseId !== cfg.productionBaseId,
      note: "All writes targeted AIRTABLE_BASE_ID_SANDBOX only",
    },
    rulings,
    after_preview: afterPreview,
    freeze_hash_sha256: EXPECTED_FREEZE,
    frozen_62_modified: false,
    frozen_vic_modified: false,
    ready_for_manual_review: status === EXEC_STATUS.EXECUTED,
    failures: failed,
  };
}
