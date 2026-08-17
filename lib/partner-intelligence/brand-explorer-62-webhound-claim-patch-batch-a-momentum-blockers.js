/**
 * Brand Explorer 62 — Webhound Claim Patch Batch A (Recent Momentum blockers).
 *
 * Scope: A_recent_momentum_blockers only from reconciliation v1.
 * Action: hide exact Presentation footprint.momentum rows from public
 * (Active:false + External Display Status: Do Not Display).
 *
 * Never writes Brand Setup child tables, Census, Brand Status, release fields,
 * Company Validated, Brand Verified, or Batches B–F.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  EXPECTED_ACTIVE_COUNT_62,
  FREEZE_DECISION_62,
  ROOT,
} from "./brand-explorer-62-active-public-full-baseline.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import { PRESENTATION_TABLE } from "./brand-explorer-62-webhound-claim-validation.js";

export const BATCH_A_VERSION =
  "brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers-v1";
export const BATCH_A_STATUS_COMPLETE =
  "brand_explorer_62_webhound_claim_patch_batch_a_momentum_blockers_complete_ready_for_batch_b_review";
export const BATCH_A_STATUS_DRY_RUN =
  "brand_explorer_62_webhound_claim_patch_batch_a_momentum_blockers_dry_run_ready";
export const BATCH_A_STATUS_BLOCKED =
  "brand_explorer_62_webhound_claim_patch_batch_a_momentum_blockers_blocked";
export const BATCH_A_STATUS_PARTIAL =
  "brand_explorer_62_webhound_claim_patch_batch_a_momentum_blockers_partial_apply_needs_review";

export const RECON_JSON =
  "reports/brand-explorer/brand-explorer-62-webhound-airtable-reconciliation-v1.json";
export const REPORT_JSON =
  "brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.json";
export const REPORT_MD =
  "brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.md";
export const DOCS_MD =
  "brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.md";

export const BATCH_A_NAME = "A_recent_momentum_blockers";
export const EXPECTED_ITEM_COUNT = 16;
export const EXPECTED_UNIQUE_RECORDS = 13;

export const APPLY_FLAGS = Object.freeze([
  "--confirm-batch-a-only",
  "--confirm-no-batch-b-f",
  "--confirm-recent-momentum-presentation-only",
  "--confirm-no-brand-setup-writes",
  "--confirm-no-census-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validated-writes",
  "--confirm-no-brand-verified-writes",
  "--confirm-founder-approved-batch-a",
]);

const ALLOWED_WRITE_FIELDS = new Set(["Active", "External Display Status"]);
const FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
  "Release",
  "Ready for Active Profile",
  "Active Profile Approved",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
]);

const SNAPSHOT_FIELDS = [
  "Slot Key",
  "Brand Name",
  "Title",
  "Body",
  "Active",
  "External Display Status",
  "Case Summary Overview",
  "Case Summary Brand Relevance",
  "Case Summary Interpretation",
  "Case Summary Tags",
  "Sort Order",
];

const WRITE_THROTTLE_MS = 320;
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = path.join(ROOT, "reports", "brand-explorer");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return v.length ? String(v[0] ?? "").trim() : "";
  return String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function readJson(rel) {
  const p = path.isAbsolute(rel) ? rel : path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function snapshotFields(fields = {}) {
  const out = {};
  for (const k of SNAPSHOT_FIELDS) out[k] = fields[k] ?? null;
  return out;
}

function isAlreadyHidden(fields = {}) {
  const eds = nz(fields["External Display Status"]);
  if (/^do not display$/i.test(eds) || /^internal only$/i.test(eds)) return true;
  if (fields.Active === false) return true;
  return false;
}

function classifyOutcome(momentumAction) {
  if (momentumAction === "stale_hold") return "stale_held_from_public";
  if (momentumAction === "remove_unsupported") return "removed_from_public";
  if (momentumAction === "steward_review") return "steward_review_held";
  return "held_from_public";
}

/**
 * Resolve Batch A queue items from reconciliation report.
 */
export function loadBatchAItems(recon = readJson(RECON_JSON)) {
  if (!recon) throw new Error(`Missing ${RECON_JSON}`);
  const batch = (recon.proposedRemediationBatches || []).find((b) => b.batch === BATCH_A_NAME);
  if (!batch) throw new Error(`Missing batch ${BATCH_A_NAME} in reconciliation report`);
  const idSet = new Set(batch.itemIds || []);
  const items = (recon.remediationQueue || []).filter((q) => idSet.has(q.id));
  return { batch, items };
}

/**
 * Consolidate rem items → one patch per Presentation record.
 */
export function planBatchAPatches(items) {
  const byRecord = new Map();
  for (const item of items) {
    if (item.airtableTable !== PRESENTATION_TABLE) {
      throw new Error(`${item.id}: refused non-Presentation table ${item.airtableTable}`);
    }
    if (item.slotKey && item.slotKey !== "footprint.momentum") {
      throw new Error(`${item.id}: refused non-momentum slot ${item.slotKey}`);
    }
    if (!item.recordId) throw new Error(`${item.id}: missing recordId`);
    if (!byRecord.has(item.recordId)) {
      byRecord.set(item.recordId, {
        recordId: item.recordId,
        table: PRESENTATION_TABLE,
        brand: item.brand,
        brandSlug: item.brandSlug,
        slotKey: item.slotKey || "footprint.momentum",
        remIds: [],
        fieldNames: [],
        momentumActions: [],
        recommendedActions: [],
        issueClassifications: [],
        sourceUrls: [],
        evidenceSummaries: [],
        beforeFieldValues: {},
      });
    }
    const row = byRecord.get(item.recordId);
    row.remIds.push(item.id);
    if (item.fieldName && !row.fieldNames.includes(item.fieldName)) {
      row.fieldNames.push(item.fieldName);
    }
    if (item.momentumAction) row.momentumActions.push(item.momentumAction);
    if (item.recommendedAction) row.recommendedActions.push(item.recommendedAction);
    if (item.issueClassification) row.issueClassifications.push(item.issueClassification);
    if (item.sourceUrl) row.sourceUrls.push(item.sourceUrl);
    if (item.evidenceSummary) row.evidenceSummaries.push(item.evidenceSummary);
    row.beforeFieldValues[item.fieldName || "Body"] = item.currentValue ?? null;
  }

  const patches = [];
  for (const row of byRecord.values()) {
    const primaryAction = row.momentumActions.includes("remove_unsupported")
      ? "remove_unsupported"
      : row.momentumActions.includes("stale_hold")
        ? "stale_hold"
        : row.momentumActions[0] || "remove_unsupported";
    patches.push({
      ...row,
      primaryMomentumAction: primaryAction,
      outcomeClass: classifyOutcome(primaryAction),
      fields: {
        Active: false,
        "External Display Status": "Do Not Display",
      },
      reason:
        primaryAction === "stale_hold"
          ? "Batch A stale_hold: remove stale/property-as-momentum from public Recent Momentum"
          : "Batch A remove_unsupported: remove unsupported Recent Momentum from public",
    });
  }
  return patches.sort((a, b) => a.brandSlug.localeCompare(b.brandSlug) || a.recordId.localeCompare(b.recordId));
}

async function airtableGet(baseId, token, table, recordId) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `GET ${recordId} failed ${res.status}`);
  return json;
}

async function airtablePatch(baseId, token, table, recordId, fields) {
  for (const k of Object.keys(fields)) {
    if (!ALLOWED_WRITE_FIELDS.has(k)) {
      throw new Error(`Refused write to disallowed field: ${k}`);
    }
    if (FORBIDDEN_WRITE_FIELDS.includes(k)) {
      throw new Error(`Refused write to forbidden field: ${k}`);
    }
  }
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH ${recordId} failed ${res.status}`);
  return json;
}

function preflight({ items, patches, universe, flagsOk, apply }) {
  const issues = [];
  if (items.length !== EXPECTED_ITEM_COUNT) {
    issues.push(`item_count_${items.length}_expected_${EXPECTED_ITEM_COUNT}`);
  }
  if (patches.length !== EXPECTED_UNIQUE_RECORDS) {
    issues.push(`unique_records_${patches.length}_expected_${EXPECTED_UNIQUE_RECORDS}`);
  }
  if (universe?.totalCount !== EXPECTED_ACTIVE_COUNT_62) {
    issues.push(`active_universe_${universe?.totalCount}_expected_${EXPECTED_ACTIVE_COUNT_62}`);
  }
  if (apply && !flagsOk) issues.push("missing_required_apply_flags");
  for (const p of patches) {
    if (p.table !== PRESENTATION_TABLE) issues.push(`bad_table:${p.recordId}`);
    if (p.slotKey !== "footprint.momentum") issues.push(`bad_slot:${p.recordId}:${p.slotKey}`);
    for (const f of Object.keys(p.fields)) {
      if (!ALLOWED_WRITE_FIELDS.has(f)) issues.push(`disallowed_field:${f}`);
    }
  }
  return issues;
}

/**
 * Plan + optionally apply Batch A.
 */
export async function runBatchAClaimPatch({
  apply = false,
  argv = [],
  token,
  baseId,
} = {}) {
  const apiKey = token || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const bid = baseId || process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !bid) throw new Error("Set AIRTABLE_API_KEY/AIRTABLE_PAT and AIRTABLE_BASE_ID");

  const flagsOk = APPLY_FLAGS.every((f) => argv.includes(f));
  const recon = readJson(RECON_JSON);
  const { batch, items } = loadBatchAItems(recon);
  const patches = planBatchAPatches(items);
  const universe = await loadActiveUniverse({ includeBrandApi: false });
  const issues = preflight({ items, patches, universe, flagsOk, apply });

  const batchBIds = new Set(
    (recon.proposedRemediationBatches || [])
      .filter((b) => b.batch !== BATCH_A_NAME)
      .flatMap((b) => b.itemIds || [])
  );

  // Live snapshot + validate each target
  const planned = [];
  for (const p of patches) {
    const live = await airtableGet(bid, apiKey, p.table, p.recordId);
    const fields = live.fields || {};
    const slot = nz(fields["Slot Key"]);
    if (slot && slot !== "footprint.momentum") {
      issues.push(`live_slot_mismatch:${p.recordId}:${slot}`);
    }
    const alreadyHidden = isAlreadyHidden(fields);
    planned.push({
      ...p,
      liveSnapshotBefore: snapshotFields(fields),
      alreadyHidden,
      skipApply: alreadyHidden,
      afterFieldsPreview: {
        Active: false,
        "External Display Status": "Do Not Display",
      },
    });
    await sleep(80);
  }

  let status = apply ? BATCH_A_STATUS_BLOCKED : BATCH_A_STATUS_DRY_RUN;
  const applyResults = [];
  let airtableWrites = 0;

  if (issues.length) {
    status = BATCH_A_STATUS_BLOCKED;
  } else if (apply) {
    for (const p of planned) {
      if (p.skipApply) {
        applyResults.push({
          recordId: p.recordId,
          brandSlug: p.brandSlug,
          remIds: p.remIds,
          outcomeClass: p.outcomeClass,
          status: "already_hidden_noop",
          fieldsWritten: {},
        });
        continue;
      }
      try {
        const updated = await airtablePatch(bid, apiKey, p.table, p.recordId, p.fields);
        airtableWrites += 1;
        applyResults.push({
          recordId: p.recordId,
          brandSlug: p.brandSlug,
          remIds: p.remIds,
          outcomeClass: p.outcomeClass,
          status: "patched",
          fieldsWritten: p.fields,
          liveSnapshotAfter: snapshotFields(updated.fields || {}),
        });
      } catch (err) {
        applyResults.push({
          recordId: p.recordId,
          brandSlug: p.brandSlug,
          remIds: p.remIds,
          outcomeClass: p.outcomeClass,
          status: "error",
          error: err?.message || String(err),
        });
      }
      await sleep(WRITE_THROTTLE_MS);
    }
    const errCount = applyResults.filter((r) => r.status === "error").length;
    status = errCount ? BATCH_A_STATUS_PARTIAL : BATCH_A_STATUS_COMPLETE;
  }

  const removed = planned.filter((p) => p.primaryMomentumAction === "remove_unsupported");
  const held = planned.filter((p) => p.primaryMomentumAction === "stale_hold");
  const softened = []; // Batch A blockers: no wording softens — hide only
  const steward = planned.filter((p) => p.primaryMomentumAction === "steward_review");

  const report = {
    version: BATCH_A_VERSION,
    generatedAt: new Date().toISOString(),
    status,
    mode: apply ? (issues.length ? "blocked" : "apply") : "dry-run",
    freezeBaseline: FREEZE_DECISION_62,
    reconSource: RECON_JSON,
    batch: BATCH_A_NAME,
    founderDecision: "Apply Batch A only. Do not apply Batch B–F yet.",
    summary: {
      batchAItemsReviewed: items.length,
      uniqueRecordsTargeted: planned.length,
      itemsRemovedUnsupported: removed.length,
      itemsStaleHeld: held.length,
      itemsSoftened: softened.length,
      itemsStewardReview: steward.length,
      alreadyHiddenNoop: planned.filter((p) => p.alreadyHidden).length,
      wouldPatch: planned.filter((p) => !p.alreadyHidden).length,
      airtableWrites,
      activeUniverse: universe.totalCount,
    },
    scopeGuards: {
      batchAOnly: true,
      batchBFUntouched: true,
      batchBFItemIdsUntouched: [...batchBIds],
      presentationOnly: true,
      recentMomentumOnly: true,
      brandSetupWrites: false,
      hotelPropertyCensusWrites: false,
      brandStatusChanges: false,
      releaseFieldWrites: false,
      companyValidatedWrites: false,
      brandVerifiedWrites: false,
      allowedWriteFields: [...ALLOWED_WRITE_FIELDS],
      fieldsPatchedPerRecord: ["Active", "External Display Status"],
    },
    preflightIssues: issues,
    applyFlagsRequired: APPLY_FLAGS,
    applyFlagsPresent: flagsOk,
    items,
    patches: planned.map((p) => ({
      remIds: p.remIds,
      recordId: p.recordId,
      table: p.table,
      brand: p.brand,
      brandSlug: p.brandSlug,
      slotKey: p.slotKey,
      fieldNamesTargetedByRecon: p.fieldNames,
      primaryMomentumAction: p.primaryMomentumAction,
      outcomeClass: p.outcomeClass,
      reason: p.reason,
      sourceUrls: p.sourceUrls,
      evidenceSummaries: p.evidenceSummaries.map((s) => String(s).slice(0, 400)),
      before: {
        title: p.liveSnapshotBefore?.Title ?? null,
        body: String(p.liveSnapshotBefore?.Body || "").slice(0, 400),
        active: p.liveSnapshotBefore?.Active ?? null,
        externalDisplayStatus: p.liveSnapshotBefore?.["External Display Status"] ?? null,
        caseSummaryOverview: p.liveSnapshotBefore?.["Case Summary Overview"] ?? null,
        caseSummaryBrandRelevance: p.liveSnapshotBefore?.["Case Summary Brand Relevance"] ?? null,
        caseSummaryInterpretation: p.liveSnapshotBefore?.["Case Summary Interpretation"] ?? null,
        reconFieldValues: p.beforeFieldValues,
      },
      after: {
        active: false,
        externalDisplayStatus: "Do Not Display",
        note: "Public Recent Momentum card hidden; Body/Title text retained for rollback (not rewritten).",
      },
      alreadyHidden: p.alreadyHidden,
      fieldsWritten: p.fields,
    })),
    applyResults,
    postApplyGates: null,
    expectedStatus: BATCH_A_STATUS_COMPLETE,
    exactApplyCommand: [
      "node scripts/brand-explorer-62-webhound-claim-patch-batch-a-momentum-blockers.mjs --apply \\",
      ...APPLY_FLAGS.map((f, i) => `  ${f}${i === APPLY_FLAGS.length - 1 ? "" : " \\"}`),
    ].join("\n"),
  };

  return report;
}

export function renderBatchAMarkdown(report, gateResults = null) {
  const s = report.summary || {};
  const gates = gateResults || report.postApplyGates || {};
  const lines = [
    `# Brand Explorer 62 — Webhound Claim Patch Batch A (Recent Momentum blockers)`,
    ``,
    `**Version:** \`${report.version}\`  `,
    `**Status:** \`${report.status}\`  `,
    `**Mode:** ${report.mode}  `,
    `**Generated:** ${report.generatedAt}  `,
    `**Freeze baseline:** \`${report.freezeBaseline}\`  `,
    `**Recon source:** \`${report.reconSource}\``,
    ``,
    `## Founder decision`,
    ``,
    `- Apply **Batch A only** (Recent Momentum blockers).`,
    `- Do **not** apply Batches B–F.`,
    `- Keep patch scope narrow and exact.`,
    ``,
    `## Summary`,
    ``,
    `| Metric | Value |`,
    `| --- | ---: |`,
    `| Batch A items reviewed | ${s.batchAItemsReviewed} |`,
    `| Unique Presentation records | ${s.uniqueRecordsTargeted} |`,
    `| Removed (unsupported) | ${s.itemsRemovedUnsupported} |`,
    `| Stale held from public | ${s.itemsStaleHeld} |`,
    `| Softened wording | ${s.itemsSoftened} |`,
    `| Steward review holds | ${s.itemsStewardReview} |`,
    `| Already hidden (noop) | ${s.alreadyHiddenNoop} |`,
    `| Airtable writes | ${s.airtableWrites} |`,
    `| Active universe | ${s.activeUniverse} |`,
    ``,
    `## Patch method`,
    ``,
    `For each Batch A target Presentation \`footprint.momentum\` row:`,
    ``,
    `- Set \`Active: false\``,
    `- Set \`External Display Status: Do Not Display\``,
    `- Do **not** rewrite Body/Title (retain for rollback)`,
    `- Do **not** invent replacement momentum claims`,
    ``,
    `This matches Wave 13/14 hide pattern and removes the card from owner-facing Recent Momentum.`,
    ``,
    `## Scope confirmation`,
    ``,
    `- Batch B–F untouched: **${report.scopeGuards?.batchBFUntouched ? "yes" : "no"}**`,
    `- Brand Setup writes: **${report.scopeGuards?.brandSetupWrites ? "yes" : "no"}**`,
    `- Hotel Property Census writes: **${report.scopeGuards?.hotelPropertyCensusWrites ? "yes" : "no"}**`,
    `- Brand Status / release / Company Validated / Brand Verified writes: **no**`,
    `- Fields written: \`${(report.scopeGuards?.allowedWriteFields || []).join("`, `")}\``,
    ``,
    `## Exact records patched`,
    ``,
  ];

  for (const p of report.patches || []) {
    lines.push(`### ${p.brand} (\`${p.brandSlug}\`) — \`${p.recordId}\``);
    lines.push(``);
    lines.push(`- Rem IDs: ${(p.remIds || []).join(", ")}`);
    lines.push(`- Momentum action: \`${p.primaryMomentumAction}\` → \`${p.outcomeClass}\``);
    lines.push(`- Recon fields: ${(p.fieldNamesTargetedByRecon || []).join(", ") || "—"}`);
    lines.push(`- Source(s): ${(p.sourceUrls || []).slice(0, 2).join(" · ") || "—"}`);
    lines.push(`- Before Title: ${p.before?.title || "—"}`);
    lines.push(`- Before Active / EDS: \`${p.before?.active}\` / \`${p.before?.externalDisplayStatus || "(empty)"}\``);
    lines.push(`- Before Body (trim): ${String(p.before?.body || "").slice(0, 180).replace(/\n/g, " ")}`);
    lines.push(`- After: Active \`false\` · External Display Status \`Do Not Display\``);
    lines.push(`- Already hidden: ${p.alreadyHidden ? "yes" : "no"}`);
    lines.push(``);
  }

  if (report.preflightIssues?.length) {
    lines.push(`## Preflight issues`);
    lines.push(``);
    for (const i of report.preflightIssues) lines.push(`- ${i}`);
    lines.push(``);
  }

  lines.push(`## Post-apply gates`);
  lines.push(``);
  if (!gates || !Object.keys(gates).length) {
    lines.push(`_Pending — run after apply._`);
  } else {
    lines.push(`| Gate | Result |`);
    lines.push(`| --- | --- |`);
    lines.push(`| Active universe | ${gates.activeUniverse ?? "—"} |`);
    lines.push(`| test:brand-explorer | ${gates.testBrandExplorer ?? "—"} |`);
    lines.push(`| brand-explorer:pvql | ${gates.pvql ?? "—"} |`);
    lines.push(`| brand-explorer:semantic-audit | ${gates.semantic ?? "—"} |`);
    lines.push(`| dealality:batch-learning-audit | ${gates.batchLearning ?? "—"} |`);
  }
  lines.push(``);
  lines.push(`## Apply command`);
  lines.push(``);
  lines.push("```bash");
  lines.push(report.exactApplyCommand);
  lines.push("```");
  lines.push(``);
  lines.push(`## Change impact`);
  lines.push(``);
  lines.push(`- **Classification:** High (Presentation Recent Momentum public visibility)`);
  lines.push(`- **Rollback:** PATCH each recordId to restore \`Active\` + \`External Display Status\` from \`before\` snapshots in this report JSON.`);
  lines.push(`- **Modules/pages:** Brand Explorer Recent Momentum for Batch A brands only.`);
  lines.push(``);
  return lines.join("\n");
}

export function writeBatchAArtifacts(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, REPORT_JSON);
  const mdPath = path.join(REPORTS_DIR, REPORT_MD);
  const docsPath = path.join(DOCS_DIR, DOCS_MD);
  const md = renderBatchAMarkdown(report);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
  fs.writeFileSync(docsPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
  return { jsonPath, mdPath, docsPath };
}
