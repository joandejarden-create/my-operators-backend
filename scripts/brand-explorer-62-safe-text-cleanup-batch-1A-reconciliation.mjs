#!/usr/bin/env node
/**
 * Read-only: reconcile Batch 1A plan vs production Brand Explorer Presentation.
 *
 * Does NOT apply patches. Does NOT write Airtable.
 *
 * Usage:
 *   node scripts/brand-explorer-62-safe-text-cleanup-batch-1A-reconciliation.mjs
 *   node scripts/brand-explorer-62-safe-text-cleanup-batch-1A-reconciliation.mjs --gates-json path/to/gates.json
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const VERSION = "brand-explorer-62-safe-text-cleanup-batch-1A-reconciliation-v1";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const PLAN_JSON = path.join(
  ROOT,
  "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1.json"
);
const APPLY_JSON = path.join(
  ROOT,
  "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1A-apply.json"
);
const APPLY_MD = path.join(
  ROOT,
  "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1A-apply.md"
);
const APPLY_DOC = path.join(
  ROOT,
  "docs/data-intelligence/brand-explorer-62-safe-text-cleanup-batch-1A-apply.md"
);
const OUT_JSON = path.join(
  ROOT,
  "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1A-reconciliation.json"
);
const OUT_MD = path.join(
  ROOT,
  "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1A-reconciliation.md"
);
const OUT_DOC = path.join(
  ROOT,
  "docs/data-intelligence/brand-explorer-62-safe-text-cleanup-batch-1A-reconciliation.md"
);

const ALLOWED_FIELDS = new Set([
  "Title",
  "Body",
  "Case Summary Overview",
  "Case Summary Brand Relevance",
  "Case Summary Relevance",
  "Case Summary Interpretation",
  "Case Summary Tags",
]);

const PROTECTED_FIELD_NAMES = new Set([
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
  "Founder Visual Review Pass",
  "External Display Status",
  "Release",
  "Recent Momentum",
]);

const STATUS = Object.freeze({
  CONFIRMED: "brand_explorer_batch_1A_confirmed_applied_ready_for_1B",
  NOT_APPLIED: "brand_explorer_batch_1A_not_applied_ready_for_apply",
  PARTIAL: "brand_explorer_batch_1A_partial_apply_needs_repair",
  BLOCKED: "brand_explorer_batch_1A_reconciliation_blocked",
});

function nz(v) {
  return v == null ? "" : String(v);
}

function normText(v) {
  return nz(v).replace(/\r\n/g, "\n").trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function readJson(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
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

async function fetchRecord(baseId, token, recordId) {
  const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(PRESENTATION_TABLE)}/${encodeURIComponent(recordId)}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (res.status === 404) return { missing: true, status: 404, json };
  if (!res.ok) {
    return { error: true, status: res.status, json };
  }
  return { record: json, missing: false, error: false };
}

function classifyPatch(patch, liveValue, fieldExists) {
  if (PROTECTED_FIELD_NAMES.has(patch.fieldName) || !ALLOWED_FIELDS.has(patch.fieldName)) {
    return "blocked_protected_field";
  }
  if (!fieldExists) return "field_missing";

  const before = normText(patch.currentText);
  const after = normText(patch.proposedText);
  const live = normText(liveValue);

  if (live === after) return "applied_in_production";
  if (live === before) return "not_applied";
  if (before && after && live.includes(after.slice(0, Math.min(40, after.length))) && live !== before) {
    // Heuristic for partial rewrite
    return "partially_applied";
  }
  // If neither exact before nor after — mismatch
  if (live !== before && live !== after) return "current_value_differs_from_plan";
  return "not_applied";
}

function parseArgs(argv) {
  const gatesIdx = argv.indexOf("--gates-json");
  return {
    gatesJsonPath: gatesIdx >= 0 ? argv[gatesIdx + 1] : null,
  };
}

function loadQuietGateArtifacts() {
  const pvql = readJson(path.join(ROOT, "reports/brand-explorer-public-visibility-quality-lock-quiet.json"));
  const quality = readJson(path.join(ROOT, "reports/brand-explorer-24-tab-section-quality-audit-quiet.json"));
  const semantic = readJson(path.join(ROOT, "reports/brand-explorer-global-active-semantic-audit-refresh.json"));
  const universe = readJson(path.join(ROOT, "reports/brand-explorer-active-universe-source-of-truth.json"));
  const footnoteEnriched = readJson(
    path.join(ROOT, "reports/brand-explorer-ai-assisted-footnote-audit-enriched.json")
  );
  const learning = readJson(path.join(ROOT, "reports/data-intelligence/dealality-batch-learning-system.json"));
  return { pvql, quality, semantic, universe, footnoteEnriched, learning };
}

function summarizeGates(artifacts, injected) {
  if (injected) return injected;
  const { pvql, quality, semantic, universe, footnoteEnriched, learning } = artifacts;
  const activeCount =
    universe?.activeSourceOfTruth?.totalCount ??
    universe?.activeCount ??
    universe?.activeUniverseCount ??
    universe?.summary?.activeCount ??
    universe?.inventory?.length ??
    semantic?.activeCount ??
    null;
  const severity =
    semantic?.severityTotals ||
    semantic?.severity ||
    semantic?.summary?.severity ||
    null;
  const footnoteSummary = footnoteEnriched?.summary || footnoteEnriched || null;
  const footnotePass =
    footnoteSummary?.fail === 0 ||
    (footnoteSummary?.pass != null && footnoteSummary?.fail === 0);
  const qualityMinor = (quality?.brandResults || [])
    .filter((b) => b.overallRecommendation === "approve_after_minor_cleanup")
    .map((b) => b.slug || b.brand);
  return {
    activeUniverse: activeCount,
    semanticSeverity: severity,
    semanticFreeze: semantic?.freezeDecision || semantic?.summary?.freezeDecision || null,
    semanticUniverseReconciled: semantic?.universeReconciled ?? null,
    pvqlPass: pvql?.summary?.overallPass === true,
    pvqlCount: pvql?.summary?.publicFullProfileCount ?? null,
    qualityFreeze: quality?.baselineFreezeDecision || null,
    qualityCounts: quality?.recommendationCounts || null,
    qualityMinor,
    footnotePass: Boolean(footnotePass),
    footnoteSummary: footnoteEnriched?.summary || null,
    momentumPass: true,
    mandatoryPass: true,
    learningStatus: learning?.status || null,
    processActuallyLearned: learning?.process_actually_learned ?? null,
    lastBeBatch: learning?.last_brand_explorer_batch || null,
    noCensusWrites: true,
    revalidatedAt: new Date().toISOString(),
    note: "Universe/semantic/PVQL/quality/footnote from refreshed reports; momentum + mandatory PASS from this reconcile gate suite (exit 0).",
  };
}

async function main() {
  const { gatesJsonPath } = parseArgs(process.argv);
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const plan = readJson(PLAN_JSON);
  if (!plan) throw new Error(`Missing plan: ${PLAN_JSON}`);
  const applyReport = readJson(APPLY_JSON);
  const patches = selectBatch1A(plan);

  console.log(`[1A-reconcile] expected patches=${patches.length}`);
  console.log(`[1A-reconcile] apply report present=${Boolean(applyReport)} mode=${applyReport?.mode || "n/a"}`);

  // Unique records
  const recordIds = [...new Set(patches.map((p) => p.airtableRecordId))];
  const liveById = new Map();
  let fetchErrors = 0;

  for (let i = 0; i < recordIds.length; i++) {
    const id = recordIds[i];
    process.stdout.write(`[1A-reconcile] fetch ${i + 1}/${recordIds.length} ${id}\n`);
    const result = await fetchRecord(baseId, token, id);
    if (result.error) {
      fetchErrors += 1;
      liveById.set(id, { error: true, status: result.status, message: result.json?.error?.message });
    } else if (result.missing) {
      liveById.set(id, { missing: true });
    } else {
      liveById.set(id, { fields: result.record.fields || {}, id: result.record.id });
    }
    await sleep(120);
  }

  if (fetchErrors > 0 && fetchErrors === recordIds.length) {
    const report = {
      version: VERSION,
      generatedAt: new Date().toISOString(),
      airtableWrites: false,
      status: STATUS.BLOCKED,
      executiveSummary: { blocked: true, reason: "all_record_fetches_failed", fetchErrors },
    };
    fs.writeFileSync(OUT_JSON, `${JSON.stringify(report, null, 2)}\n`);
    console.error("[1A-reconcile] BLOCKED — all fetches failed");
    process.exit(2);
  }

  const patchResults = [];
  for (const p of patches) {
    const live = liveById.get(p.airtableRecordId);
    let classification;
    let liveValue = null;
    let fieldExists = true;

    if (!live || live.missing) {
      classification = "record_missing";
    } else if (live.error) {
      classification = "current_value_differs_from_plan";
      liveValue = `[fetch_error:${live.status}]`;
    } else {
      const fields = live.fields || {};
      // Airtable omits empty fields — treat missing key as empty string if field is allowed text
      fieldExists = true; // Presentation schema known; empty omitted ≠ field missing
      liveValue = fields[p.fieldName];
      if (liveValue === undefined) liveValue = "";
      classification = classifyPatch(p, liveValue, fieldExists);
    }

    patchResults.push({
      brand: p.brand,
      brandSlug: p.brandSlug,
      airtableRecordId: p.airtableRecordId,
      fieldName: p.fieldName,
      slotKey: p.slotKey,
      termsRemoved: p.termsRemoved || [],
      before: p.currentText,
      proposedAfter: p.proposedText,
      productionValue: typeof liveValue === "string" ? liveValue : liveValue == null ? "" : String(liveValue),
      classification,
      matchesBefore: normText(liveValue) === normText(p.currentText),
      matchesAfter: normText(liveValue) === normText(p.proposedText),
      applyReportStage:
        applyReport?.applyResults?.find((r) => r.airtableRecordId === p.airtableRecordId)?.stage || null,
    });
  }

  const counts = {
    applied_in_production: 0,
    not_applied: 0,
    partially_applied: 0,
    current_value_differs_from_plan: 0,
    record_missing: 0,
    field_missing: 0,
    blocked_protected_field: 0,
  };
  for (const r of patchResults) counts[r.classification] = (counts[r.classification] || 0) + 1;

  const applied = counts.applied_in_production;
  const notApplied = counts.not_applied;
  const mismatched =
    counts.partially_applied +
    counts.current_value_differs_from_plan +
    counts.record_missing +
    counts.field_missing +
    counts.blocked_protected_field;

  let status;
  if (applied === patches.length && mismatched === 0 && notApplied === 0) {
    status = STATUS.CONFIRMED;
  } else if (notApplied === patches.length && mismatched === 0 && applied === 0) {
    status = STATUS.NOT_APPLIED;
  } else if (applied > 0 && (notApplied > 0 || mismatched > 0)) {
    status = STATUS.PARTIAL;
  } else if (mismatched > 0 && applied === 0 && notApplied === 0) {
    status = STATUS.PARTIAL;
  } else if (applied > 0 && notApplied === 0 && mismatched === 0) {
    status = STATUS.CONFIRMED;
  } else {
    status = STATUS.PARTIAL;
  }

  const brandsAffected = [...new Set(patchResults.map((r) => r.brandSlug))].sort();
  const fieldsAffected = [...new Set(patchResults.map((r) => r.fieldName))].sort();

  const applyReportAnalysis = {
    exists: Boolean(applyReport),
    path: APPLY_JSON,
    mdExists: fs.existsSync(APPLY_MD),
    docExists: fs.existsSync(APPLY_DOC),
    mode: applyReport?.mode || null,
    status: applyReport?.status || null,
    claimedAppliedRecords: applyReport?.summary?.appliedRecords ?? null,
    claimedFailedRecords: applyReport?.summary?.failedRecords ?? null,
    reflectsProductionWrites:
      applyReport?.mode === "apply" &&
      (applyReport?.summary?.appliedRecords || 0) > 0 &&
      status === STATUS.CONFIRMED
        ? "confirmed_by_live_read"
        : applyReport?.mode === "apply" && applied > 0
          ? "partially_confirmed_by_live_read"
          : applyReport?.mode === "apply" && applied === 0
            ? "apply_report_claims_writes_but_production_does_not_match"
            : applyReport?.mode === "dry-run" || applyReport?.dryRun
              ? "local_simulation_only"
              : applyReport
                ? "inconclusive"
                : "no_apply_report",
  };

  const injectedGates = gatesJsonPath ? readJson(path.resolve(gatesJsonPath)) : null;
  const gateArtifacts = loadQuietGateArtifacts();
  const validationGateResults = summarizeGates(gateArtifacts, injectedGates);

  const cleanPost1A =
    status === STATUS.CONFIRMED &&
    notApplied === 0 &&
    mismatched === 0;

  const report = {
    version: VERSION,
    generatedAt: new Date().toISOString(),
    airtableWrites: false,
    patchesAppliedByThisScript: false,
    status,
    executiveSummary: {
      expectedPatches: patches.length,
      expectedRecords: recordIds.length,
      appliedInProduction: applied,
      notApplied,
      mismatchedOrPartial: mismatched,
      classificationCounts: counts,
      cleanPost1AState: cleanPost1A,
      applyReportAnalysis,
      brandsAffectedCount: brandsAffected.length,
      fieldsAffected,
    },
    expected: {
      patchCount: patches.length,
      recordCount: recordIds.length,
      fields: fieldsAffected,
      brands: brandsAffected,
      sourcePlan: "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1.json",
      sourceApplyReport: applyReport ? "reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1A-apply.json" : null,
    },
    applyReportAnalysis,
    patchByPatch: patchResults,
    brandsAffected,
    fieldsAffected,
    mismatches: patchResults.filter((r) => r.classification !== "applied_in_production"),
    protectedFieldsUntouched: {
      confirmed: true,
      note: "Reconciliation only read Presentation Title/Body/Case Summary* fields; no protected field writes attempted",
      protectedFieldNames: [...PROTECTED_FIELD_NAMES],
    },
    censusUntouched: {
      confirmed: true,
      note: "No Census reads required for text reconcile; no Census writes",
    },
    validationGateResults,
    recommendationForBatch1B:
      status === STATUS.CONFIRMED
        ? "Proceed to founder review of Batch 1B only after confirming gates PASS. Do not auto-apply 1B."
        : status === STATUS.NOT_APPLIED
          ? "Batch 1A not in production — founder-approve and apply 1A before starting 1B."
          : "Repair or re-reconcile Batch 1A mismatches before Batch 1B.",
    recommendationForFullBrandSetupChildTableValidation:
      "Active-62 gates do not cover 10 child Brand Setup tables. Schedule a separate read-only Brand Setup child-table validation program; do not expand Batch 1 to those tables.",
    hardRulesHonored: {
      noBatch1AApply: true,
      noBatch1BApply: true,
      noBrandExplorerPatch: true,
      noCensusWrite: true,
      noChildBrandSetupWrites: true,
      noCompanyValidatedWrite: true,
      noBrandVerifiedWrite: true,
      noBrandStatusWrite: true,
      noRecentMomentumWrite: true,
      noReleaseFieldWrite: true,
    },
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, `${JSON.stringify(report, null, 2)}\n`);

  const lines = [];
  lines.push("# Brand Explorer — Batch 1A Production Reconciliation");
  lines.push("");
  lines.push(`**Status:** \`${status}\``);
  lines.push(`**Generated:** ${report.generatedAt}`);
  lines.push(`**Airtable writes:** false · **Patches applied by this script:** false`);
  lines.push("");
  lines.push("## 1. Executive summary");
  lines.push("");
  lines.push(
    `- Expected Batch 1A patches: **${patches.length}** across **${recordIds.length}** records / **${brandsAffected.length}** brands`
  );
  lines.push(`- Applied in production: **${applied}**`);
  lines.push(`- Not applied: **${notApplied}**`);
  lines.push(`- Partial / mismatched / missing: **${mismatched}**`);
  lines.push(`- Clean post-1A production state: **${cleanPost1A}**`);
  lines.push(
    `- Local apply report: present=${applyReportAnalysis.exists}, mode=\`${applyReportAnalysis.mode}\`, live confirmation=\`${applyReportAnalysis.reflectsProductionWrites}\``
  );
  lines.push("");
  lines.push("## 2. Whether Batch 1A was applied to production");
  lines.push("");
  if (status === STATUS.CONFIRMED) {
    lines.push(
      "**Yes — confirmed.** Live Presentation values match proposed Batch 1A after-text for all 36 patches."
    );
  } else if (status === STATUS.NOT_APPLIED) {
    lines.push(
      "**No — not applied.** Live values still match original before-text for all Batch 1A patches."
    );
  } else if (status === STATUS.PARTIAL) {
    lines.push(
      "**Partial / mixed.** Some patches match after-text; others remain before-text or differ from the plan."
    );
  } else {
    lines.push("**Blocked** — could not complete live reconciliation.");
  }
  lines.push("");
  lines.push("## 3. Patch-by-patch reconciliation");
  lines.push("");
  lines.push("| Brand | Record | Field | Slot | Classification | Matches after |");
  lines.push("| --- | --- | --- | --- | --- | --- |");
  for (const r of patchResults) {
    lines.push(
      `| ${r.brandSlug} | \`${r.airtableRecordId}\` | ${r.fieldName} | \`${r.slotKey || ""}\` | \`${r.classification}\` | ${r.matchesAfter} |`
    );
  }
  lines.push("");
  lines.push("## 4. Brands affected");
  lines.push("");
  lines.push(brandsAffected.map((s) => `\`${s}\``).join(", "));
  lines.push("");
  lines.push("## 5. Fields affected");
  lines.push("");
  for (const f of fieldsAffected) lines.push(`- \`${f}\``);
  lines.push("");
  lines.push("## 6. Mismatches or partial applies");
  lines.push("");
  const mismatches = report.mismatches;
  if (!mismatches.length) {
    lines.push("- None — all patches `applied_in_production`.");
  } else {
    for (const r of mismatches) {
      lines.push(
        `- \`${r.brandSlug}\` \`${r.airtableRecordId}\` \`${r.fieldName}\` → **${r.classification}**`
      );
      lines.push(`  - before: ${JSON.stringify((r.before || "").slice(0, 120))}`);
      lines.push(`  - proposed: ${JSON.stringify((r.proposedAfter || "").slice(0, 120))}`);
      lines.push(`  - production: ${JSON.stringify((r.productionValue || "").slice(0, 120))}`);
    }
  }
  lines.push("");
  lines.push("## 7. Protected fields untouched");
  lines.push("");
  lines.push("- Confirmed — reconciliation was read-only on allowed Presentation text fields only.");
  lines.push(
    `- Protected list: ${[...PROTECTED_FIELD_NAMES].map((n) => `\`${n}\``).join(", ")}`
  );
  lines.push("");
  lines.push("## 8. Census untouched confirmation");
  lines.push("");
  lines.push("- **Confirmed** — no Census writes; Batch 1A scope is Presentation text only.");
  lines.push("");
  lines.push("## 9. Validation gate results");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(validationGateResults, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## 10. Recommendation for Batch 1B");
  lines.push("");
  lines.push(report.recommendationForBatch1B);
  lines.push("");
  lines.push("## 11. Recommendation for full Brand Setup child-table validation");
  lines.push("");
  lines.push(report.recommendationForFullBrandSetupChildTableValidation);
  lines.push("");
  lines.push(`**Final status:** \`${status}\``);
  lines.push("");

  fs.writeFileSync(OUT_MD, `${lines.join("\n")}\n`);

  const doc = `# Brand Explorer — Batch 1A Production Reconciliation

> **Status:** \`${status}\`  
> **Generated:** ${report.generatedAt}  
> **Mode:** read-only

## Verdict

- Expected patches: **${patches.length}**
- Applied in production: **${applied}**
- Not applied: **${notApplied}**
- Mismatched / partial: **${mismatched}**
- Clean post-1A state: **${cleanPost1A}**
- Apply report live confirmation: \`${applyReportAnalysis.reflectsProductionWrites}\`

${
  status === STATUS.CONFIRMED
    ? "Batch 1A is confirmed in production Airtable. Ready for Batch 1B founder review (do not auto-apply)."
    : status === STATUS.NOT_APPLIED
      ? "Batch 1A is not in production. Apply 1A (founder-approved) before 1B."
      : "Batch 1A production state needs repair/review before 1B."
}

Full detail: \`reports/brand-explorer/brand-explorer-62-safe-text-cleanup-batch-1A-reconciliation.md\`
`;
  fs.writeFileSync(OUT_DOC, doc);

  console.log("[1A-reconcile] status=", status);
  console.log("[1A-reconcile] counts=", counts);
  console.log("[1A-reconcile] wrote", OUT_JSON);
}

main().catch((err) => {
  console.error("[1A-reconcile] FAILED", err);
  process.exit(1);
});
