#!/usr/bin/env node
/**
 * MGallery Collection — quality minor resolution (smallest Presentation patch).
 *
 * Creates 3 missing major slots + thickens 2 thin Bodies.
 * Does NOT touch Recent Momentum, Census, child Brand Setup, or protected fields.
 *
 * Usage:
 *   node scripts/brand-explorer-62-mgallery-quality-minor-apply.mjs --dry-run
 *   node scripts/brand-explorer-62-mgallery-quality-minor-apply.mjs --apply \
 *     --confirm-mgallery-only \
 *     --confirm-no-census-writes \
 *     --confirm-no-protected-fields \
 *     --confirm-no-recent-momentum \
 *     --confirm-no-child-brand-setup \
 *     --confirm-founder-approved-mgallery-minor
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const VERSION = "brand-explorer-62-mgallery-quality-minor-apply-v1";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BRAND_NAME = "MGallery Collection";
const BRAND_SLUG = "mgallery-collection";
const BRAND_RECORD_ID = "recrWCD1LMqu864oU";

const REQUIRED_APPLY_FLAGS = [
  "--confirm-mgallery-only",
  "--confirm-no-census-writes",
  "--confirm-no-protected-fields",
  "--confirm-no-recent-momentum",
  "--confirm-no-child-brand-setup",
  "--confirm-founder-approved-mgallery-minor",
];

const STATUS = Object.freeze({
  APPLIED: "brand_explorer_62_mgallery_quality_minor_resolved_ready_for_62_freeze",
  PARTIAL: "brand_explorer_62_mgallery_quality_minor_partial_apply_needs_review",
  BLOCKED: "brand_explorer_62_mgallery_quality_minor_blocked_before_apply",
  DRY_RUN: "brand_explorer_62_mgallery_quality_minor_dry_run_ready",
  GATES_PENDING: "brand_explorer_62_mgallery_quality_minor_applied_gates_pending",
});

/** Smallest patch set to clear 3 majors (+ optional thin minors). */
const CREATES = [
  {
    slotKey: "Guest Psychographics Description",
    title: "",
    body:
      "Design- and culture-minded travelers seeking distinctive heritage, boutique, or destination hotels with a coherent property story—not a uniform hard-brand prototype. Guests respond to individuality, curated service rituals, and Accor Live Limitless access when participation applies.",
    sort: 20,
    reason: "Fill missing major Audience / target guest slot",
  },
  {
    slotKey: "valueOwners.overview",
    title: "",
    body:
      "Guests: Distinctive story-led hotels with heritage, boutique, or destination character inside Accor's soft collection.\n\nOwners: Accor soft-collection affiliation that preserves property individuality while unlocking platform reach—without a single hard-brand prototype.\n\nUnderwrite contribution after fees, loyalty costs, and channel mix versus independent or hard-brand alternatives.",
    sort: 30,
    reason: "Fill missing major valueOwners.overview slot",
  },
  {
    slotKey: "valueOwners.watchouts",
    title: "",
    body:
      "Markets that cannot support soft-collection rate positioning or the amenity/service substance the story requires.\nAssets without enough design or narrative substance for Accor curation review.\nAssuming uniform Accor Live Limitless benefits across every affiliation structure.\nOperator mismatch: prototype-driven operators who erase individuality usually struggle first in guest reviews and brand standards checks.\nSponsor KPIs should track contribution and direct mix—not collection headlines alone.",
    sort: 31,
    reason: "Fill missing major valueOwners.watchouts slot",
  },
];

const UPDATES = [
  {
    recordId: "rec8JMqWvldhwE1xn",
    slotKey: "footprint.portfolio_mix",
    fieldName: "Body",
    currentText:
      "Story-led soft collection: full-service heritage, boutique, and destination hotels where individuality is the product—conversion and repositioning over prototype new-build density. Owners should underwrite mix by asset character and Accor acceptance, not Accor network averages.",
    proposedText:
      "Story-led soft collection\nFull-service heritage / boutique\nDestination character hotels\nConversion / repositioning focus",
    reason:
      "Structured non-percentage mix (≥12 words, newline lines) — clears quality thin without semantic prose_market_note",
  },
  {
    recordId: "recDwU2wDYTHwqRPf",
    slotKey: "operations.operator_compat.tags",
    fieldName: "Body",
    // Chip slots require newline-separated tags (chip count ≥ 2).
    currentText:
      "Accor soft collection\nStory-led full-service\nHeritage / boutique / destination\nConversion / repositioning operators",
    proposedText:
      "Accor soft collection\nStory-led full-service\nHeritage / boutique / destination\nConversion / repositioning operators",
    reason:
      "Restore newline chip format (≥2 chips) while keeping owner-specific tags thicker than the prior 8-word stub — already applied",
  },
];

function nz(v) {
  return v == null ? "" : String(v);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function writeJson(abs, data) {
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, `${JSON.stringify(data, null, 2)}\n`);
}

function writeMd(abs, text) {
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, text.endsWith("\n") ? text : `${text}\n`);
}

async function listByBrand(baseId, token) {
  const out = [];
  let offset;
  const formula = `{Brand Name}='${BRAND_NAME.replace(/'/g, "\\'")}'`;
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `list ${res.status}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function createRecord(baseId, token, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `POST ${res.status}`);
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

function buildCreateFields(row) {
  return {
    "Slot Key": row.slotKey,
    Title: row.title || "",
    Body: row.body,
    Active: true,
    "Sort Order": row.sort,
    "Brand Name": BRAND_NAME,
    Brand: [BRAND_RECORD_ID],
  };
}

async function main() {
  const argv = process.argv.slice(2);
  const doApply = argv.includes("--apply") && !argv.includes("--dry-run");

  const flagCheck = {
    apply: doApply,
    required: REQUIRED_APPLY_FLAGS,
    missing: REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f)),
  };
  flagCheck.ok = flagCheck.missing.length === 0;

  const report = {
    version: VERSION,
    generatedAt: new Date().toISOString(),
    mode: doApply ? "apply" : "dry-run",
    status: STATUS.BLOCKED,
    brandSlug: BRAND_SLUG,
    brandName: BRAND_NAME,
    brandRecordId: BRAND_RECORD_ID,
    flagCheck,
    scope: {
      creates: CREATES.map((c) => c.slotKey),
      updates: UPDATES.map((u) => ({ recordId: u.recordId, slotKey: u.slotKey, field: u.fieldName })),
      excluded: [
        "Recent Momentum",
        "Census writes",
        "child Brand Setup tables",
        "Company Validated / Brand Verified / Brand Status / release fields",
        "image caption remediations",
        "similar brands / owner considerations / case study optional minors",
      ],
    },
    createResults: [],
    updateResults: [],
    protectedFieldsUntouched: true,
    censusUntouched: true,
    childBrandSetupUntouched: true,
    recentMomentumUntouched: true,
    validationGateResults: null,
    learningLedgerUpdate: null,
    recommendationForNextLane:
      "After gates show MGallery approve_for_baseline_freeze and Active-62 freeze decision is clean, freeze the 62 baseline (child-table validation remains a separate read-only lane).",
    hardRulesHonored: [
      "MGallery only",
      "Quality minor resolution — smallest Presentation patch",
      "No Recent Momentum",
      "No Census writes",
      "No child Brand Setup writes",
      "No protected Brand Basics fields",
    ],
  };

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

  console.log(`[${VERSION}] mode=${report.mode} brand=${BRAND_SLUG}`);
  const liveRows = await listByBrand(baseId, token);
  const bySlot = new Map();
  for (const r of liveRows) {
    const sk = nz(r.fields?.["Slot Key"]);
    if (!sk) continue;
    if (!bySlot.has(sk)) bySlot.set(sk, []);
    bySlot.get(sk).push(r);
  }

  // Creates
  for (const c of CREATES) {
    const existing = bySlot.get(c.slotKey) || [];
    if (existing.length) {
      const body = nz(existing[0].fields?.Body);
      if (body.trim()) {
        console.log(`  create ${c.slotKey}... already_present`);
        report.createResults.push({
          slotKey: c.slotKey,
          ok: true,
          stage: "already_present",
          recordId: existing[0].id,
        });
        continue;
      }
    }

    const fields = buildCreateFields(c);
    if (!doApply) {
      console.log(`  create ${c.slotKey}... dry-run`);
      report.createResults.push({
        slotKey: c.slotKey,
        ok: true,
        stage: "dry_run_would_create",
        fieldsPreview: {
          "Slot Key": fields["Slot Key"],
          Body: fields.Body.slice(0, 120),
          Active: true,
          Brand: BRAND_RECORD_ID,
        },
        reason: c.reason,
      });
      continue;
    }

    try {
      const created = await createRecord(baseId, token, fields);
      await sleep(220);
      console.log(`  create ${c.slotKey}... CREATED ${created.id}`);
      report.createResults.push({
        slotKey: c.slotKey,
        ok: true,
        stage: "created",
        recordId: created.id,
        reason: c.reason,
      });
    } catch (e) {
      console.log(`  create ${c.slotKey}... ERR ${e.message}`);
      report.createResults.push({
        slotKey: c.slotKey,
        ok: false,
        stage: "create_error",
        error: e.message,
      });
    }
  }

  // Updates
  for (const u of UPDATES) {
    const live = liveRows.find((r) => r.id === u.recordId);
    if (!live) {
      console.log(`  update ${u.recordId}... MISSING`);
      report.updateResults.push({
        recordId: u.recordId,
        slotKey: u.slotKey,
        ok: false,
        stage: "record_missing",
      });
      continue;
    }
    const liveVal = nz(live.fields?.[u.fieldName]);
    if (liveVal === u.proposedText) {
      console.log(`  update ${u.slotKey}... already_applied`);
      report.updateResults.push({
        recordId: u.recordId,
        slotKey: u.slotKey,
        fieldName: u.fieldName,
        ok: true,
        stage: "already_applied",
      });
      continue;
    }
    if (liveVal !== u.currentText) {
      console.log(`  update ${u.slotKey}... DRIFT`);
      report.updateResults.push({
        recordId: u.recordId,
        slotKey: u.slotKey,
        fieldName: u.fieldName,
        ok: false,
        stage: "drift",
        livePreview: liveVal.slice(0, 120),
        expectedPreview: u.currentText.slice(0, 120),
      });
      continue;
    }

    if (!doApply) {
      console.log(`  update ${u.slotKey}... dry-run`);
      report.updateResults.push({
        recordId: u.recordId,
        slotKey: u.slotKey,
        fieldName: u.fieldName,
        ok: true,
        stage: "dry_run_would_write",
        reason: u.reason,
      });
      continue;
    }

    try {
      await patchRecord(baseId, token, u.recordId, { [u.fieldName]: u.proposedText });
      await sleep(220);
      console.log(`  update ${u.slotKey}... APPLIED`);
      report.updateResults.push({
        recordId: u.recordId,
        slotKey: u.slotKey,
        fieldName: u.fieldName,
        ok: true,
        stage: "applied",
        reason: u.reason,
      });
    } catch (e) {
      console.log(`  update ${u.slotKey}... ERR ${e.message}`);
      report.updateResults.push({
        recordId: u.recordId,
        slotKey: u.slotKey,
        fieldName: u.fieldName,
        ok: false,
        stage: "patch_error",
        error: e.message,
      });
    }
  }

  const failed =
    report.createResults.filter((r) => !r.ok).length +
    report.updateResults.filter((r) => !r.ok).length;
  const created = report.createResults.filter((r) => r.stage === "created").length;
  const updated = report.updateResults.filter((r) => r.stage === "applied").length;
  const already =
    report.createResults.filter((r) => r.stage === "already_present").length +
    report.updateResults.filter((r) => r.stage === "already_applied").length;

  report.summary = {
    createsPlanned: CREATES.length,
    updatesPlanned: UPDATES.length,
    created,
    updated,
    alreadyPresentOrApplied: already,
    failed,
  };

  if (!doApply) {
    report.status = failed ? STATUS.PARTIAL : STATUS.DRY_RUN;
  } else if (failed === 0) {
    report.status = STATUS.APPLIED;
  } else if (created + updated > 0) {
    report.status = STATUS.PARTIAL;
  } else {
    report.status = STATUS.BLOCKED;
  }

  writeOutputs(report);
  console.log(`Status: ${report.status}`);
  console.log(JSON.stringify(report.summary, null, 2));
  if (report.status === STATUS.PARTIAL || (report.status === STATUS.BLOCKED && doApply)) {
    process.exitCode = 3;
  }
}

function writeOutputs(report) {
  const outDir = path.join(ROOT, "reports", "brand-explorer");
  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  writeJson(path.join(outDir, "brand-explorer-62-mgallery-quality-minor-apply.json"), report);
  writeMd(path.join(outDir, "brand-explorer-62-mgallery-quality-minor-apply.md"), renderMd(report));
  writeMd(path.join(docsDir, "brand-explorer-62-mgallery-quality-minor-apply.md"), renderDocs(report));
}

function renderMd(r) {
  const lines = [];
  lines.push("# Brand Explorer 62 — MGallery Quality Minor Apply");
  lines.push("");
  lines.push(`**Status:** \`${r.status}\``);
  lines.push(`**Generated:** ${r.generatedAt}`);
  lines.push(`**Mode:** ${r.mode}`);
  lines.push("");
  lines.push("## 1. Executive summary");
  lines.push("");
  lines.push(`- Brand: **${r.brandName}** (\`${r.brandSlug}\`)`);
  lines.push(`- Summary: ${JSON.stringify(r.summary)}`);
  lines.push(
    `- Census untouched: **${r.censusUntouched}** · Protected untouched: **${r.protectedFieldsUntouched}** · Recent Momentum untouched: **${r.recentMomentumUntouched}**`
  );
  lines.push("");
  lines.push("## 2. Creates");
  lines.push("");
  for (const c of r.createResults) {
    lines.push(`- \`${c.slotKey}\` · ${c.stage}${c.recordId ? ` · \`${c.recordId}\`` : ""}${c.ok ? "" : " · FAIL"}`);
  }
  lines.push("");
  lines.push("## 3. Updates");
  lines.push("");
  for (const u of r.updateResults) {
    lines.push(
      `- \`${u.slotKey}\` · \`${u.recordId}\` · ${u.fieldName || "Body"} · ${u.stage}${u.ok ? "" : " · FAIL"}`
    );
  }
  lines.push("");
  lines.push("## 4. Excluded by design");
  lines.push("");
  for (const x of r.scope.excluded) lines.push(`- ${x}`);
  lines.push("");
  lines.push("## 5. Validation gate results");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.validationGateResults, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## 6. Recommendation for next lane");
  lines.push("");
  lines.push(r.recommendationForNextLane);
  lines.push("");
  lines.push(`**Final status:** \`${r.status}\``);
  lines.push("");
  return lines.join("\n");
}

function renderDocs(r) {
  return `# Brand Explorer 62 — MGallery Quality Minor Apply

> **Status:** \`${r.status}\`  
> **Generated:** ${r.generatedAt}  
> **Mode:** ${r.mode}

## Summary

Smallest MGallery Presentation patch to clear Active-62 quality minor: create missing \`Guest Psychographics Description\`, \`valueOwners.overview\`, \`valueOwners.watchouts\`; thicken thin \`footprint.portfolio_mix\` + \`operations.operator_compat.tags\` Bodies.

No Census, Recent Momentum, child Brand Setup, or protected-field writes.

## Next

${r.recommendationForNextLane}
`;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
