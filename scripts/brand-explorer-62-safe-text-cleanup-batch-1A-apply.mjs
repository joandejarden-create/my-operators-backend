#!/usr/bin/env node
/**
 * Apply Brand Explorer Safe Text Cleanup Batch 1A only.
 *
 * Usage:
 *   node scripts/brand-explorer-62-safe-text-cleanup-batch-1A-apply.mjs --dry-run
 *   node scripts/brand-explorer-62-safe-text-cleanup-batch-1A-apply.mjs --apply \
 *     --confirm-batch-1A-only \
 *     --confirm-no-batch-1B \
 *     --confirm-no-census-writes \
 *     --confirm-no-protected-fields \
 *     --confirm-no-recent-momentum \
 *     --confirm-founder-approved-1A
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const VERSION = "brand-explorer-62-safe-text-cleanup-batch-1A-apply-v1";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const HELD_FLEX_SLUG = "four-points-flex-by-sheraton";
const PLAN_JSON = "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1.json";
const ALLOWED_FIELDS = new Set([
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Brand Relevance",
  "Case Summary Relevance",
  "Case Summary Interpretation",
  "Case Summary Tags",
]);
const PROTECTED_FIELD_NAMES = [
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
  "Founder Visual Review Pass",
  "External Display Status",
  "Release",
  "Recent Momentum",
];
const REQUIRED_APPLY_FLAGS = [
  "--confirm-batch-1A-only",
  "--confirm-no-batch-1B",
  "--confirm-no-census-writes",
  "--confirm-no-protected-fields",
  "--confirm-no-recent-momentum",
  "--confirm-founder-approved-1A",
];

const STATUS = Object.freeze({
  APPLIED: "brand_explorer_62_safe_text_cleanup_batch_1A_applied_ready_for_1B_review",
  PARTIAL: "brand_explorer_62_safe_text_cleanup_batch_1A_partial_apply_needs_review",
  BLOCKED: "brand_explorer_62_safe_text_cleanup_batch_1A_blocked_before_apply",
});

function nz(v) {
  return v == null ? "" : String(v);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function readJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(abs, data) {
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, `${JSON.stringify(data, null, 2)}\n`);
}

function writeMd(abs, text) {
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, text.endsWith("\n") ? text : `${text}\n`);
}

function selectBatch1A(plan) {
  const source = plan.batch1A?.length ? plan.batch1A : plan.patches || [];
  return source.filter(
    (p) =>
      p.batchLane === "1A" &&
      p.riskLevel === "Low" &&
      p.applyRecommendation === "include_in_batch_1" &&
      p.patchCategory === "safe_text_cleanup" &&
      p.airtableRecordId &&
      ALLOWED_FIELDS.has(p.fieldName)
  );
}

function preflight(patches, universe) {
  const issues = [];
  const activeCount = universe?.activeSourceOfTruth?.totalCount ?? universe?.inventory?.length;
  const inventory = universe?.inventory || [];
  const flexInActive = inventory.some((b) => b.slug === HELD_FLEX_SLUG);

  if (activeCount !== 62) issues.push(`active_universe_count_${activeCount}_expected_62`);
  if (flexInActive) issues.push("four_points_flex_in_active_universe");

  for (const p of patches) {
    if (!ALLOWED_FIELDS.has(p.fieldName)) issues.push(`disallowed_field:${p.fieldName}:${p.airtableRecordId}`);
    if (PROTECTED_FIELD_NAMES.includes(p.fieldName)) issues.push(`protected_field:${p.fieldName}`);
    if (/momentum|company validated|brand verified|brand status|release/i.test(p.fieldName || "")) {
      issues.push(`protected_or_momentum_field:${p.fieldName}`);
    }
    if (p.batchLane !== "1A") issues.push(`not_1A:${p.airtableRecordId}`);
    if (p.patchCategory !== "safe_text_cleanup") issues.push(`bad_category:${p.patchCategory}`);
    if (p.riskLevel !== "Low") issues.push(`not_low_risk:${p.airtableRecordId}`);
  }

  return {
    ok: issues.length === 0,
    issues,
    activeCount,
    flexHeld: !flexInActive,
    patchCount: patches.length,
    expectedPatchCount: 36,
    censusWritesPlanned: false,
    protectedFieldsIncluded: false,
    recentMomentumIncluded: false,
    brandStatusIncluded: false,
    companyValidatedIncluded: false,
    releaseFieldsIncluded: false,
  };
}

async function fetchRecord(baseId, token, recordId) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `GET ${recordId} ${res.status}`);
  return json;
}

async function patchRecord(baseId, token, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH ${recordId} ${res.status}`);
  return json;
}

function groupByRecord(patches) {
  /** @type {Map<string, typeof patches>} */
  const map = new Map();
  for (const p of patches) {
    if (!map.has(p.airtableRecordId)) map.set(p.airtableRecordId, []);
    map.get(p.airtableRecordId).push(p);
  }
  return map;
}

async function main() {
  const argv = process.argv.slice(2);
  const apply = argv.includes("--apply");
  const dryRun = argv.includes("--dry-run") || !apply;

  if (apply && dryRun && argv.includes("--dry-run")) {
    // explicit dry-run wins if both present
  }

  const plan = readJson(PLAN_JSON);
  if (!plan) throw new Error(`Missing ${PLAN_JSON}`);
  const universe = readJson("reports/brand-explorer-active-universe-source-of-truth.json");
  const patches = selectBatch1A(plan);
  const pre = preflight(patches, universe);

  const flagCheck = {
    apply,
    required: REQUIRED_APPLY_FLAGS,
    missing: REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f)),
  };
  flagCheck.ok = flagCheck.missing.length === 0;

  const report = {
    version: VERSION,
    generatedAt: new Date().toISOString(),
    mode: apply && !argv.includes("--dry-run") ? "apply" : "dry-run",
    status: STATUS.BLOCKED,
    preflight: pre,
    flagCheck,
    selectedPatchCount: patches.length,
    brandsAffected: [...new Set(patches.map((p) => p.brandSlug))].sort(),
    fieldsChanged: [...new Set(patches.map((p) => p.fieldName))].sort(),
    forbiddenTermsRemoved: {},
    patchesPlanned: patches.map((p) => ({
      brandSlug: p.brandSlug,
      airtableRecordId: p.airtableRecordId,
      fieldName: p.fieldName,
      slotKey: p.slotKey,
      termsRemoved: p.termsRemoved || [],
      currentTextPreview: nz(p.currentText).slice(0, 120),
      proposedTextPreview: nz(p.proposedText).slice(0, 120),
    })),
    applyResults: [],
    protectedFieldsUntouched: true,
    censusUntouched: true,
    batch1BUntouched: true,
    kimptonRefreshUntouched: true,
    mgalleryUntouched: true,
    recentMomentumUntouched: true,
    validationGateResults: null,
    learningLedgerUpdate: null,
    recommendationForBatch1B: null,
    hardRulesHonored: [
      "Batch 1A only",
      "No Batch 1B",
      "No Kimpton property refresh",
      "No MGallery",
      "No Recent Momentum",
      "No Census writes",
      "No protected Brand Basics fields",
    ],
  };

  for (const p of patches) {
    for (const t of p.termsRemoved || []) {
      report.forbiddenTermsRemoved[t] = (report.forbiddenTermsRemoved[t] || 0) + 1;
    }
  }

  if (!pre.ok) {
    report.status = STATUS.BLOCKED;
    report.blockedReason = pre.issues;
    writeOutputs(report);
    console.error("BLOCKED preflight", pre.issues);
    process.exit(3);
  }

  if (patches.length !== 36) {
    console.warn(`WARN: expected 36 Batch 1A patches, selected ${patches.length}`);
  }

  const doApply = apply && !argv.includes("--dry-run");
  if (doApply && !flagCheck.ok) {
    report.status = STATUS.BLOCKED;
    report.blockedReason = [`missing_flags:${flagCheck.missing.join(",")}`];
    writeOutputs(report);
    console.error("BLOCKED missing flags", flagCheck.missing);
    process.exit(3);
  }

  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE_API_KEY / AIRTABLE_BASE_ID");

  // Verify current values still match plan (or already applied)
  const byRec = groupByRecord(patches);
  console.log(`[${VERSION}] mode=${report.mode} records=${byRec.size} fieldPatches=${patches.length}`);

  for (const [recordId, recPatches] of byRec.entries()) {
    process.stdout.write(`  ${recordId} (${recPatches[0].brandSlug})... `);
    let live;
    try {
      live = await fetchRecord(baseId, token, recordId);
      await sleep(150);
    } catch (e) {
      console.log(`GET_ERR ${e.message}`);
      report.applyResults.push({
        airtableRecordId: recordId,
        brandSlug: recPatches[0].brandSlug,
        ok: false,
        error: e.message,
        stage: "fetch",
      });
      continue;
    }

    const fieldsLive = live.fields || {};
    const fieldsToWrite = {};
    const fieldResults = [];
    let skipRecord = false;

    for (const p of recPatches) {
      const liveVal = nz(fieldsLive[p.fieldName]);
      const expectedCurrent = nz(p.currentText);
      const proposed = nz(p.proposedText);
      if (liveVal === proposed) {
        fieldResults.push({
          fieldName: p.fieldName,
          status: "already_applied",
          termsRemoved: p.termsRemoved || [],
        });
        continue;
      }
      if (liveVal !== expectedCurrent) {
        fieldResults.push({
          fieldName: p.fieldName,
          status: "drift_hold",
          error: "live_text_does_not_match_plan_currentText",
          livePreview: liveVal.slice(0, 100),
          expectedPreview: expectedCurrent.slice(0, 100),
        });
        skipRecord = true;
        continue;
      }
      fieldsToWrite[p.fieldName] = proposed;
      fieldResults.push({
        fieldName: p.fieldName,
        status: doApply ? "pending_write" : "dry_run_would_write",
        termsRemoved: p.termsRemoved || [],
      });
    }

    if (skipRecord && Object.keys(fieldsToWrite).length === 0) {
      console.log("DRIFT");
      report.applyResults.push({
        airtableRecordId: recordId,
        brandSlug: recPatches[0].brandSlug,
        ok: false,
        stage: "drift",
        fieldResults,
      });
      continue;
    }

    if (!Object.keys(fieldsToWrite).length) {
      console.log("already_clean");
      report.applyResults.push({
        airtableRecordId: recordId,
        brandSlug: recPatches[0].brandSlug,
        ok: true,
        stage: "already_applied",
        fieldResults,
      });
      continue;
    }

    if (!doApply) {
      console.log(`dry-run write ${Object.keys(fieldsToWrite).join(",")}`);
      report.applyResults.push({
        airtableRecordId: recordId,
        brandSlug: recPatches[0].brandSlug,
        ok: true,
        stage: "dry_run",
        fieldsWouldWrite: Object.keys(fieldsToWrite),
        fieldResults,
      });
      continue;
    }

    try {
      await patchRecord(baseId, token, recordId, fieldsToWrite);
      await sleep(220);
      console.log(`APPLIED ${Object.keys(fieldsToWrite).join(",")}`);
      report.applyResults.push({
        airtableRecordId: recordId,
        brandSlug: recPatches[0].brandSlug,
        ok: true,
        stage: "applied",
        fieldsWritten: Object.keys(fieldsToWrite),
        fieldResults: fieldResults.map((f) =>
          f.status === "pending_write" ? { ...f, status: "applied" } : f
        ),
      });
    } catch (e) {
      console.log(`PATCH_ERR ${e.message}`);
      report.applyResults.push({
        airtableRecordId: recordId,
        brandSlug: recPatches[0].brandSlug,
        ok: false,
        stage: "patch",
        error: e.message,
        fieldResults,
      });
    }
  }

  const appliedRecords = report.applyResults.filter((r) => r.ok && r.stage === "applied").length;
  const already = report.applyResults.filter((r) => r.ok && r.stage === "already_applied").length;
  const failed = report.applyResults.filter((r) => !r.ok).length;
  const dryOk = report.applyResults.filter((r) => r.ok && r.stage === "dry_run").length;

  report.summary = {
    recordsTouchedOrVerified: report.applyResults.length,
    appliedRecords,
    alreadyAppliedRecords: already,
    dryRunRecords: dryOk,
    failedRecords: failed,
    fieldPatchesSelected: patches.length,
  };

  if (!doApply) {
    report.status = failed ? STATUS.PARTIAL : STATUS.APPLIED; // dry-run ready posture uses applied status name only after real apply
    report.status = STATUS.BLOCKED;
    report.blockedReason = ["dry_run_only_no_writes"];
    // Actually for dry-run we shouldn't use blocked - but this script is for apply. Keep dry-run reporting clear.
    report.status = failed ? STATUS.PARTIAL : "brand_explorer_62_safe_text_cleanup_batch_1A_dry_run_ready";
  } else if (failed === 0 && (appliedRecords > 0 || already === report.applyResults.length)) {
    report.status = STATUS.APPLIED;
  } else if (appliedRecords > 0 && failed > 0) {
    report.status = STATUS.PARTIAL;
  } else {
    report.status = STATUS.BLOCKED;
  }

  report.recommendationForBatch1B =
    "After post-apply gates PASS, founder-review Batch 1B (census wording cleanups + Kimpton location refresh) separately. Do not auto-apply 1B.";

  writeOutputs(report);
  console.log(`Status: ${report.status}`);
  console.log(JSON.stringify(report.summary, null, 2));
  if (report.status === STATUS.BLOCKED && doApply) process.exitCode = 3;
  if (report.status === STATUS.PARTIAL) process.exitCode = 3;
}

function writeOutputs(report) {
  const outDir = path.join(ROOT, "reports", "brand-explorer");
  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  writeJson(path.join(outDir, "brand-explorer-62-safe-text-cleanup-batch-1A-apply.json"), report);
  writeMd(path.join(outDir, "brand-explorer-62-safe-text-cleanup-batch-1A-apply.md"), renderMd(report));
  writeMd(path.join(docsDir, "brand-explorer-62-safe-text-cleanup-batch-1A-apply.md"), renderDocs(report));
}

function esc(s) {
  return String(s || "").replace(/\|/g, "\\|").replace(/\n/g, " ");
}

function renderMd(r) {
  const lines = [];
  lines.push("# Brand Explorer 62 — Safe Text Cleanup Batch 1A Apply");
  lines.push("");
  lines.push(`**Status:** \`${r.status}\``);
  lines.push(`**Generated:** ${r.generatedAt}`);
  lines.push(`**Mode:** ${r.mode}`);
  lines.push("");
  lines.push("## 1. Executive summary");
  lines.push("");
  lines.push(`- Selected Batch 1A patches: **${r.selectedPatchCount}**`);
  lines.push(`- Brands affected: **${r.brandsAffected.length}**`);
  lines.push(`- Fields: ${r.fieldsChanged.map((f) => `\`${f}\``).join(", ")}`);
  lines.push(`- Apply summary: ${JSON.stringify(r.summary)}`);
  lines.push(`- Census untouched: **${r.censusUntouched}** · Protected fields untouched: **${r.protectedFieldsUntouched}**`);
  lines.push("");
  lines.push("## 2. Patches applied");
  lines.push("");
  for (const a of r.applyResults) {
    lines.push(
      `- \`${a.airtableRecordId}\` · ${a.brandSlug} · ${a.stage}${a.ok ? "" : " · FAIL"} · ${(a.fieldsWritten || a.fieldsWouldWrite || []).join(", ") || "—"}`
    );
  }
  lines.push("");
  lines.push("## 3. Brands affected");
  lines.push("");
  lines.push(r.brandsAffected.map((s) => `\`${s}\``).join(", "));
  lines.push("");
  lines.push("## 4. Fields changed");
  lines.push("");
  for (const f of r.fieldsChanged) lines.push(`- \`${f}\``);
  lines.push("");
  lines.push("## 5. Forbidden terms removed");
  lines.push("");
  lines.push("| Term | Count |");
  lines.push("| --- | ---: |");
  for (const [k, v] of Object.entries(r.forbiddenTermsRemoved || {})) lines.push(`| \`${k}\` | ${v} |`);
  lines.push("");
  lines.push("## 6. Protected fields untouched");
  lines.push("");
  lines.push("- Company Validated / Company Validation Date / Brand Verified / Brand Status / release fields / Recent Momentum / Founder Visual Review Pass");
  lines.push(`- Confirmed: **${r.protectedFieldsUntouched}**`);
  lines.push("");
  lines.push("## 7. Census untouched confirmation");
  lines.push("");
  lines.push(`- **${r.censusUntouched}** — no Hotel Property Census writes in this lane`);
  lines.push("");
  lines.push("## 8. Validation gate results");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.validationGateResults, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## 9. Learning ledger update");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.learningLedgerUpdate, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## 10. Recommendation for Batch 1B");
  lines.push("");
  lines.push(r.recommendationForBatch1B || "—");
  lines.push("");
  lines.push(`**Final status:** \`${r.status}\``);
  lines.push("");
  return lines.join("\n");
}

function renderDocs(r) {
  return `# Brand Explorer 62 — Safe Text Cleanup Batch 1A Apply

> **Status:** \`${r.status}\`  
> **Generated:** ${r.generatedAt}  
> **Mode:** ${r.mode}

## Summary

Applied founder-approved **Batch 1A** safe_text_cleanup only (${r.selectedPatchCount} field patches). Batch 1B, Kimpton refresh, MGallery, Recent Momentum, and Census were not touched.

## Artifacts

- \`reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1A-apply.md\`
- \`reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1A-apply.json\`

## Next

${r.recommendationForBatch1B || "Founder review Batch 1B separately."}
`;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
