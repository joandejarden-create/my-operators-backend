#!/usr/bin/env node
/**
 * Apply Brand Explorer Safe Text Cleanup Batch 1B only.
 *
 * Usage:
 *   node scripts/brand-explorer-62-safe-text-cleanup-batch-1B-apply.mjs --dry-run
 *   node scripts/brand-explorer-62-safe-text-cleanup-batch-1B-apply.mjs --apply \
 *     --confirm-batch-1B-only \
 *     --confirm-no-batch-1A-reapply \
 *     --confirm-no-census-writes \
 *     --confirm-no-protected-fields \
 *     --confirm-no-recent-momentum \
 *     --confirm-no-mgallery \
 *     --confirm-founder-approved-1B
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const VERSION = "brand-explorer-62-safe-text-cleanup-batch-1B-apply-v1";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const HELD_FLEX_SLUG = "four-points-flex-by-sheraton";
const PLAN_JSON = "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1.json";
const RECONCILE_1A_JSON =
  "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1A-reconciliation.json";
const EXPECTED_PATCH_COUNT = 32;

const ALLOWED_FIELDS = new Set(["Title", "Body", "Case Summary Overview"]);
const ALLOWED_CATEGORIES = new Set(["safe_text_cleanup", "property_example_update"]);

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
  "--confirm-batch-1B-only",
  "--confirm-no-batch-1A-reapply",
  "--confirm-no-census-writes",
  "--confirm-no-protected-fields",
  "--confirm-no-recent-momentum",
  "--confirm-no-mgallery",
  "--confirm-founder-approved-1B",
];

const STATUS = Object.freeze({
  APPLIED: "brand_explorer_62_safe_text_cleanup_batch_1B_applied_ready_for_mgallery_or_child_table_validation",
  PARTIAL: "brand_explorer_62_safe_text_cleanup_batch_1B_partial_apply_needs_review",
  BLOCKED: "brand_explorer_62_safe_text_cleanup_batch_1B_blocked_before_apply",
  DRY_RUN: "brand_explorer_62_safe_text_cleanup_batch_1B_dry_run_ready",
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

function selectBatch1B(plan) {
  const source = plan.batch1B?.length ? plan.batch1B : plan.patches || [];
  return source.filter(
    (p) =>
      p.batchLane === "1B" &&
      p.riskLevel === "Low" &&
      p.applyRecommendation === "include_in_batch_1" &&
      ALLOWED_CATEGORIES.has(p.patchCategory) &&
      p.airtableRecordId &&
      ALLOWED_FIELDS.has(p.fieldName)
  );
}

function preflight(patches, universe, reconcile1A) {
  const issues = [];
  const activeCount = universe?.activeSourceOfTruth?.totalCount ?? universe?.inventory?.length;
  const inventory = universe?.inventory || [];
  const flexInActive = inventory.some((b) => b.slug === HELD_FLEX_SLUG);

  if (activeCount !== 62) issues.push(`active_universe_count_${activeCount}_expected_62`);
  if (flexInActive) issues.push("four_points_flex_in_active_universe");
  if (patches.length !== EXPECTED_PATCH_COUNT) {
    issues.push(`patch_count_${patches.length}_expected_${EXPECTED_PATCH_COUNT}`);
  }

  const reconcileOk =
    reconcile1A?.status === "brand_explorer_batch_1A_confirmed_applied_ready_for_1B" ||
    reconcile1A?.executiveSummary?.appliedInProduction === 36;
  if (!reconcileOk) issues.push("batch_1A_not_confirmed_applied_in_reconciliation");

  for (const p of patches) {
    if (!ALLOWED_FIELDS.has(p.fieldName)) issues.push(`disallowed_field:${p.fieldName}:${p.airtableRecordId}`);
    if (PROTECTED_FIELD_NAMES.includes(p.fieldName)) issues.push(`protected_field:${p.fieldName}`);
    if (/momentum|company validated|brand verified|brand status|release/i.test(p.fieldName || "")) {
      issues.push(`protected_or_momentum_field:${p.fieldName}`);
    }
    if (/momentum/i.test(p.slotKey || "") && p.patchCategory !== "safe_text_cleanup") {
      // openings is OK; momentum slots blocked unless separately approved
    }
    if (/^footprint\.momentum/i.test(p.slotKey || "")) {
      issues.push(`recent_momentum_slot:${p.slotKey}:${p.airtableRecordId}`);
    }
    if (p.batchLane !== "1B") issues.push(`not_1B:${p.airtableRecordId}`);
    if (!ALLOWED_CATEGORIES.has(p.patchCategory)) issues.push(`bad_category:${p.patchCategory}`);
    if (p.riskLevel !== "Low") issues.push(`not_low_risk:${p.airtableRecordId}`);
    if (/mgallery/i.test(p.brandSlug || "")) issues.push(`mgallery_included:${p.brandSlug}`);
  }

  const kimpton = patches.filter(
    (p) => p.patchCategory === "property_example_update" || /kimpton/i.test(p.brandSlug || "")
  );
  for (const k of kimpton) {
    if (k.riskLevel !== "Low") issues.push(`kimpton_not_low:${k.airtableRecordId}`);
    if (!k.censusSupport?.censusRecordId) issues.push(`kimpton_missing_census_support:${k.airtableRecordId}`);
  }

  return {
    ok: issues.length === 0,
    issues,
    activeCount,
    flexHeld: !flexInActive,
    patchCount: patches.length,
    expectedPatchCount: EXPECTED_PATCH_COUNT,
    batch1AConfirmedApplied: Boolean(reconcileOk),
    censusWritesPlanned: false,
    childBrandSetupWritesPlanned: false,
    protectedFieldsIncluded: false,
    recentMomentumIncluded: false,
    brandStatusIncluded: false,
    companyValidatedIncluded: false,
    releaseFieldsIncluded: false,
    mgalleryIncluded: false,
    kimptonPatchCount: kimpton.length,
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
  const doApply = apply && !argv.includes("--dry-run");

  const plan = readJson(PLAN_JSON);
  if (!plan) throw new Error(`Missing ${PLAN_JSON}`);
  const universe = readJson("reports/brand-explorer-active-universe-source-of-truth.json");
  const reconcile1A = readJson(RECONCILE_1A_JSON);
  const patches = selectBatch1B(plan);
  const pre = preflight(patches, universe, reconcile1A);

  const flagCheck = {
    apply: doApply,
    required: REQUIRED_APPLY_FLAGS,
    missing: REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f)),
  };
  flagCheck.ok = flagCheck.missing.length === 0;

  const kimptonPlanned = patches.filter(
    (p) => p.patchCategory === "property_example_update" || p.brandSlug === "kimpton"
  );

  const report = {
    version: VERSION,
    generatedAt: new Date().toISOString(),
    mode: doApply ? "apply" : "dry-run",
    status: STATUS.BLOCKED,
    preflight: pre,
    flagCheck,
    selectedPatchCount: patches.length,
    brandsAffected: [...new Set(patches.map((p) => p.brandSlug))].sort(),
    fieldsChanged: [...new Set(patches.map((p) => p.fieldName))].sort(),
    forbiddenTermsRemoved: {},
    kimptonLocationRefresh: {
      planned: kimptonPlanned.map((p) => ({
        airtableRecordId: p.airtableRecordId,
        fieldName: p.fieldName,
        slotKey: p.slotKey,
        currentText: p.currentText,
        proposedText: p.proposedText,
        censusSupport: p.censusSupport || null,
        riskLevel: p.riskLevel,
      })),
      result: null,
    },
    patchesPlanned: patches.map((p) => ({
      brandSlug: p.brandSlug,
      airtableRecordId: p.airtableRecordId,
      fieldName: p.fieldName,
      slotKey: p.slotKey,
      patchCategory: p.patchCategory,
      termsRemoved: p.termsRemoved || [],
      currentTextPreview: nz(p.currentText).slice(0, 120),
      proposedTextPreview: nz(p.proposedText).slice(0, 120),
    })),
    applyResults: [],
    protectedFieldsUntouched: true,
    censusUntouched: true,
    childBrandSetupUntouched: true,
    batch1AUntouched: true,
    mgalleryUntouched: true,
    recentMomentumUntouched: true,
    validationGateResults: null,
    learningLedgerUpdate: null,
    remainingCleanupItems: [
      "MGallery quality minor (held — not Batch 1)",
      "Wrong Census fuzzy property swaps (held)",
      "Child Brand Setup table validation (separate program)",
      "Any Medium/High risk text or property claims",
    ],
    recommendationForNextLane: null,
    hardRulesHonored: [
      "Batch 1B only",
      "No Batch 1A reapply",
      "No MGallery",
      "No Recent Momentum",
      "No Census writes",
      "No child Brand Setup writes",
      "No protected Brand Basics fields",
      "Kimpton Mas Olas same-property Census support only",
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
          patchCategory: p.patchCategory,
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
        patchCategory: p.patchCategory,
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

  // Kimpton result
  const kimptonResult = [];
  for (const k of kimptonPlanned) {
    const row = report.applyResults.find((r) => r.airtableRecordId === k.airtableRecordId);
    const fr = row?.fieldResults?.find((f) => f.fieldName === k.fieldName);
    kimptonResult.push({
      airtableRecordId: k.airtableRecordId,
      fieldName: k.fieldName,
      stage: row?.stage || null,
      fieldStatus: fr?.status || null,
      ok: Boolean(row?.ok && (fr?.status === "applied" || fr?.status === "already_applied" || fr?.status === "dry_run_would_write")),
      censusSupport: k.censusSupport || null,
    });
  }
  report.kimptonLocationRefresh.result = kimptonResult;

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
    report.status = failed ? STATUS.PARTIAL : STATUS.DRY_RUN;
  } else if (failed === 0 && (appliedRecords > 0 || already === report.applyResults.length)) {
    report.status = STATUS.APPLIED;
  } else if (appliedRecords > 0 && failed > 0) {
    report.status = STATUS.PARTIAL;
  } else {
    report.status = STATUS.BLOCKED;
  }

  report.recommendationForNextLane =
    "Next founder lanes: (1) MGallery quality minor cleanup (held), or (2) separate Brand Setup child-table validation program. Do not expand Batch 1 into child tables or Medium/High risk property swaps.";

  writeOutputs(report);
  console.log(`Status: ${report.status}`);
  console.log(JSON.stringify(report.summary, null, 2));
  if (report.status === STATUS.BLOCKED && doApply) process.exitCode = 3;
  if (report.status === STATUS.PARTIAL) process.exitCode = 3;
}

function writeOutputs(report) {
  const outDir = path.join(ROOT, "reports", "brand-explorer");
  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  writeJson(path.join(outDir, "brand-explorer-62-safe-text-cleanup-batch-1B-apply.json"), report);
  writeMd(path.join(outDir, "brand-explorer-62-safe-text-cleanup-batch-1B-apply.md"), renderMd(report));
  writeMd(path.join(docsDir, "brand-explorer-62-safe-text-cleanup-batch-1B-apply.md"), renderDocs(report));
}

function renderMd(r) {
  const lines = [];
  lines.push("# Brand Explorer 62 — Safe Text Cleanup Batch 1B Apply");
  lines.push("");
  lines.push(`**Status:** \`${r.status}\``);
  lines.push(`**Generated:** ${r.generatedAt}`);
  lines.push(`**Mode:** ${r.mode}`);
  lines.push("");
  lines.push("## 1. Executive summary");
  lines.push("");
  lines.push(`- Selected Batch 1B patches: **${r.selectedPatchCount}**`);
  lines.push(`- Brands affected: **${r.brandsAffected.length}**`);
  lines.push(`- Fields: ${r.fieldsChanged.map((f) => `\`${f}\``).join(", ")}`);
  lines.push(`- Apply summary: ${JSON.stringify(r.summary)}`);
  lines.push(
    `- Census untouched: **${r.censusUntouched}** · Protected fields untouched: **${r.protectedFieldsUntouched}** · Child Brand Setup untouched: **${r.childBrandSetupUntouched}**`
  );
  lines.push(`- Batch 1A confirmed preflight: **${r.preflight?.batch1AConfirmedApplied}**`);
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
  lines.push("## 5. Forbidden/internal terms removed");
  lines.push("");
  lines.push("| Term | Count |");
  lines.push("| --- | ---: |");
  for (const [k, v] of Object.entries(r.forbiddenTermsRemoved || {})) lines.push(`| \`${k}\` | ${v} |`);
  lines.push("");
  lines.push("## 6. Kimpton location refresh result");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.kimptonLocationRefresh, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## 7. Protected fields untouched");
  lines.push("");
  lines.push(
    "- Company Validated / Company Validation Date / Brand Verified / Brand Status / release fields / Recent Momentum / Founder Visual Review Pass"
  );
  lines.push(`- Confirmed: **${r.protectedFieldsUntouched}**`);
  lines.push("");
  lines.push("## 8. Census untouched confirmation");
  lines.push("");
  lines.push(`- **${r.censusUntouched}** — no Hotel Property Census writes (read-support for Kimpton only)`);
  lines.push("");
  lines.push("## 9. Validation gate results");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.validationGateResults, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## 10. Learning ledger update");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.learningLedgerUpdate, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## 11. Remaining Brand Explorer cleanup items");
  lines.push("");
  for (const x of r.remainingCleanupItems || []) lines.push(`- ${x}`);
  lines.push("");
  lines.push("## 12. Recommendation for next lane");
  lines.push("");
  lines.push(r.recommendationForNextLane || "—");
  lines.push("");
  lines.push(`**Final status:** \`${r.status}\``);
  lines.push("");
  return lines.join("\n");
}

function renderDocs(r) {
  return `# Brand Explorer 62 — Safe Text Cleanup Batch 1B Apply

> **Status:** \`${r.status}\`  
> **Generated:** ${r.generatedAt}  
> **Mode:** ${r.mode}

## Summary

Founder-approved **Batch 1B** only (${r.selectedPatchCount} field patches): census/census-URL wording cleanup + Kimpton Mas Olas same-property location refresh. MGallery, Recent Momentum, Census writes, child Brand Setup tables, and protected fields were not touched.

## Artifacts

- \`reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1B-apply.md\`
- \`reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1B-apply.json\`

## Next

${r.recommendationForNextLane || "MGallery or child-table validation."}
`;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
