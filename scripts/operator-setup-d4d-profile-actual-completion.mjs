#!/usr/bin/env node
/**
 * D.4D — Profile Actual Field Completion (Profile ONLY — Platform blocked)
 *
 *   node scripts/operator-setup-d4d-profile-actual-completion.mjs --dry-run
 *   node scripts/operator-setup-d4d-profile-actual-completion.mjs --apply --approve-operator-setup-d4d-profile
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { TEST_FIXTURE_MASTER_IDS } from "../lib/operator-explorer/phase-1-universe.js";
import { PROFILE_RETAIN_REQUIRED, MASTER_RETAIN_REQUIRED, EXEMPLAR_MASTER_IDS } from "../lib/operator-setup/no-optional-fields-policy.js";
import { isBannedGeneric, counterfactualCouldApplyToPeers } from "../lib/operator-setup/field-specific-writer-v2.js";
import {
  D4D_PROFILE_ACTUAL,
  PROFILE_DEPRECATED_HIDE,
  UNRESOLVED_MARKERS,
  classifyCell,
} from "../lib/operator-setup/d4d-profile-actual-research.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/operator-setup/d4d-profile");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");
const PROFILE = "Operator Setup - Profile & Positioning";

const RETAINED_KEYS = PROFILE_RETAIN_REQUIRED.map((f) => f.key);
const NARRATIVE = new Set(["companyDescription", "companyHistory", "differentiators"]);

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-d4d-profile") out.approve = true;
  }
  return out;
}
function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2) + "\n");
}
function writeMd(p, t) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, t.endsWith("\n") ? t : t + "\n");
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function nz(v) {
  return v == null ? "" : String(v).trim();
}
function byOperator(rows) {
  const m = {};
  for (const r of rows) for (const id of r.fields.Operator || []) m[id] = r;
  return m;
}

async function listAll(baseId, token, table) {
  const out = [];
  let offset;
  do {
    const u = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (offset) u.searchParams.set("offset", offset);
    const j = await (await fetch(u, { headers: { Authorization: `Bearer ${token}` } })).json();
    if (j.error) throw new Error(`${table}: ${JSON.stringify(j.error)}`);
    out.push(...(j.records || []));
    offset = j.offset;
    await sleep(35);
  } while (offset);
  return out;
}
async function fetchMeta(baseId, token) {
  const j = await (
    await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${token}` },
    })
  ).json();
  return j.tables || [];
}
async function patchRecord(baseId, token, table, id, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${id}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const j = await res.json();
  if (!res.ok) throw new Error(`PATCH ${table}/${id}: ${JSON.stringify(j)}`);
  return j;
}

function narrativePass(field, text, companyName) {
  if (!text || UNRESOLVED_MARKERS.has(text)) return { ok: false, reason: "unresolved" };
  if (isBannedGeneric(text)) return { ok: false, reason: "generic" };
  if (field === "companyHistory" || field === "companyDescription") {
    const anchor = /\b(19\d{2}|20\d{2}|founded|established|acquired|incorporated|listed|mar del plata|mexico|argentina|spain|toronto|boston|massachusetts|mallorca|family|management|franchise|brand-operator|third-party|integrated|listed|BMV|Southern Cone|Yucatán|Mérida)\b/i.test(
      text
    );
    return { ok: anchor, reason: anchor ? null : "no_factual_anchor" };
  }
  if (field === "differentiators") {
    const specific =
      /\b(franchis(e|ing)|management-contract|management contract|family-owned|asset-light|all-inclusive|resort|luxury|third-party|owner-operator|integrated|Southern Cone|Argentina|Mexico|Spain|listed|Crown Paradise|Grand Brizo|Costa Galana|Sonesta|World of Hyatt|A Sense of Place|no franchis|Franchise Platform|gastronomy|Krystal|BMV)\b/i.test(
        text
      ) || /\b(Meliá|Gran Meliá|Innside|Four Seasons|Rosewood|Mandarin Oriental|Auberge|AADESA|Mar del Plata|Termas de Río Hondo|Bariloche)\b/i.test(text);
    return { ok: specific, reason: specific ? null : "not_company_specific" };
  }
  const cf = counterfactualCouldApplyToPeers(text, companyName);
  return { ok: !cf.fail, reason: cf.reason };
}

function buildProposals(production, profileBy, mastersById) {
  const rows = [];
  const patches = [];
  const researchLog = [];
  const fieldBatch = {};

  for (const fieldDef of PROFILE_RETAIN_REQUIRED) {
    const key = fieldDef.key;
    fieldBatch[key] = [];
  }

  for (const m of production) {
    const pref = profileBy[m.id];
    const pack = D4D_PROFILE_ACTUAL[m.id] || {};
    const isExemplar = EXEMPLAR_MASTER_IDS.includes(m.id);
    const patch = { recordId: pref?.id, masterId: m.id, operator: m.fields.company_name, fields: {} };

    for (const fieldDef of PROFILE_RETAIN_REQUIRED) {
      const key = fieldDef.key;
      const current = pref?.fields?.[key];
      const beforeClass = classifyCell(current);
      let proposed = current;
      let action = "KEEP";
      let researchStatus = "existing sufficient";

      const packKey =
        key === "Soft Brand / Lifestyle Experience"
          ? "softBrand"
          : key === "Brand Families Operated"
            ? "brandFamilies"
            : key === "Service Models Supported"
              ? "serviceModels"
              : key;

      if (pack[packKey] !== undefined && beforeClass !== "ACTUAL") {
        proposed = pack[packKey];
        action = "RESEARCH_FILL";
        researchStatus = (pack.research || []).join("; ");
      } else if (pack[key] !== undefined && beforeClass !== "ACTUAL") {
        proposed = pack[key];
        action = "RESEARCH_FILL";
        researchStatus = (pack.research || []).join("; ");
      } else if (beforeClass === "UNRESOLVED" || beforeClass === "BLANK") {
        // Still unresolved — flag for founder review; do NOT auto-placeholder
        action = "FOUNDER_REVIEW";
        researchStatus = pack.research?.join("; ") || "research attempted — insufficient for actual answer";
      }

      if (NARRATIVE.has(key) && proposed && beforeClass !== "ACTUAL") {
        if (isExemplar && beforeClass === "ACTUAL") {
          proposed = current;
          action = "KEEP_EXEMPLAR";
        } else {
          const pass = narrativePass(key, proposed, m.fields.company_name);
          if (!pass.ok) {
            researchLog.push({
              operator: m.fields.company_name,
              field: key,
              issue: pass.reason,
              proposed: String(proposed).slice(0, 200),
              sources: pack.sources || [],
              searches: pack.research || [],
            });
            // Keep researched draft unless it is still a placeholder/generic failure
            if (pass.reason === "unresolved" || pass.reason === "generic") {
              proposed = current;
              action = "FOUNDER_REVIEW";
            } else {
              action = "RESEARCH_FILL";
            }
          }
        }
      }

      const afterClass = classifyCell(proposed);
      const shouldPatch =
        proposed !== current &&
        afterClass === "ACTUAL" &&
        !isBannedGeneric(String(proposed)) &&
        (action === "RESEARCH_FILL" || (action === "FOUNDER_REVIEW" && beforeClass === "UNRESOLVED"));
      if (shouldPatch) {
        patch.fields[key] = proposed;
      }

      const row = {
        table: "profile",
        field: key,
        masterId: m.id,
        operator: m.fields.company_name,
        before: current ?? null,
        beforeClass,
        proposed: proposed ?? null,
        afterClass,
        action,
        researchStatus,
        sources: pack.sources || [],
      };
      rows.push(row);
      fieldBatch[key].push(row);
    }

    if (Object.keys(patch.fields).length) patches.push(patch);
  }

  return { rows, patches, researchLog, fieldBatch };
}

function verticalQa(fieldBatch) {
  const issues = [];
  for (const [field, rows] of Object.entries(fieldBatch)) {
    const unresolved = rows.filter((r) => r.afterClass === "UNRESOLVED").length;
    const blank = rows.filter((r) => r.afterClass === "BLANK").length;
    const actual = rows.filter((r) => r.afterClass === "ACTUAL").length;
    if (blank > 0) issues.push({ field, type: "blank", count: blank });
    if (NARRATIVE.has(field) && unresolved > 0) {
      issues.push({ field, type: "unresolved_narrative", count: unresolved });
    }
    const texts = rows.filter((r) => r.afterClass === "ACTUAL" && typeof r.proposed === "string").map((r) => r.proposed);
    for (const t of texts) {
      if (isBannedGeneric(t)) issues.push({ field, type: "generic", sample: t.slice(0, 80) });
    }
  }
  return issues;
}

function summarize(rows, productionCount) {
  const retainedCells = productionCount * RETAINED_KEYS.length;
  let actual = 0,
    unresolved = 0,
    blank = 0;
  for (const r of rows) {
    if (r.afterClass === "ACTUAL") actual++;
    else if (r.afterClass === "UNRESOLVED") unresolved++;
    else blank++;
  }
  return {
    retainedCells,
    actual,
    unresolved,
    blank,
    actualResearchCoveragePct: Math.round((actual / retainedCells) * 1000) / 10,
    unresolvedCoveragePct: Math.round((unresolved / retainedCells) * 1000) / 10,
  };
}

function fieldCoverage(rows, field) {
  const f = rows.filter((r) => r.field === field);
  return {
    actual: f.filter((x) => x.afterClass === "ACTUAL").length,
    unresolved: f.filter((x) => x.afterClass === "UNRESOLVED").length,
    blank: f.filter((x) => x.afterClass === "BLANK").length,
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Refuse apply without --approve-operator-setup-d4d-profile");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  mkdirSync(OUT, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

  const meta = await fetchMeta(baseId, token);
  const profileMeta = meta.find((t) => t.name === PROFILE);
  const masters = await listAll(baseId, token, "Operator Setup - Master");
  const profiles = await listAll(baseId, token, PROFILE);

  const production = masters
    .filter((m) => m.fields["Record Purpose"] === "Production" && !TEST_FIXTURE_MASTER_IDS.includes(m.id))
    .sort((a, b) => nz(a.fields.company_name).localeCompare(nz(b.fields.company_name)));

  const profileBy = byOperator(profiles);

  // Live audit (all visible fields left-to-right)
  const liveAudit = JSON.parse(readFileSync(join(OUT, "live-audit.json"), "utf8"));
  const auditLines = [
    `# Operator Profile — Live Field-by-Field Audit (D.4D)`,
    ``,
    `Source: live Airtable \`${PROFILE}\`. Production operators: **${production.length}**.`,
    ``,
    `Classification: **ACTUAL** | **UNRESOLVED** (controlled placeholder — not research-complete) | **BLANK** | **WEAK**`,
    ``,
    `## Summary (all ${profileMeta.fields.length} physical columns)`,
    ``,
    `- Actual: ${liveAudit.summary.actual} / ${liveAudit.summary.totalCells} (${liveAudit.summary.actualCoveragePct}%)`,
    `- Unresolved: ${liveAudit.summary.unresolved}`,
    `- Blank: ${liveAudit.summary.blanks}`,
    `- Weak/generic: ${liveAudit.summary.weak}`,
    ``,
    `## Retained Core Product fields (${RETAINED_KEYS.length})`,
    ``,
    `| Field | Field ID | Actual | Unresolved | Blank |`,
    `| ----- | -------- | ------ | ---------- | ----- |`,
  ];
  for (const f of liveAudit.fields) {
    if (!RETAINED_KEYS.includes(f.fieldName) && f.fieldName !== "Operator") continue;
    auditLines.push(
      `| ${f.fieldName} | \`${f.fieldId}\` | ${f.counts.actual} | ${f.counts.unresolved} | ${f.counts.blank} |`
    );
  }
  auditLines.push(``, `## Deprecated / hide from product view (${PROFILE_DEPRECATED_HIDE.length} fields)`, ``);
  auditLines.push(`These must not appear in the D.4C/D.4D Core Product working grid.`, ``);
  for (const name of PROFILE_DEPRECATED_HIDE) {
    const f = liveAudit.fields.find((x) => x.fieldName === name);
    if (!f) continue;
    auditLines.push(
      `- **${name}** (\`${f.fieldId}\`) — blanks ${f.counts.blank}/36; deprecate/hide`
    );
  }
  auditLines.push(``, `---`, ``);
  for (const f of liveAudit.fields) {
    auditLines.push(`## ${f.fieldName}`, ``, `- **Field ID:** \`${f.fieldId}\``, `- **Type:** ${f.type}`, `- **Actual / Unresolved / Blank:** ${f.counts.actual} / ${f.counts.unresolved} / ${f.counts.blank}`, ``);
    if (f.counts.unresolved || f.counts.blank || f.counts.weak) {
      auditLines.push(`| Operator | Status | Value |`, `| -------- | ------ | ----- |`);
      for (const o of f.operators.filter((x) => x.status !== "ACTUAL")) {
        const val = typeof o.value === "string" ? o.value.replace(/\n/g, " ").slice(0, 120) : JSON.stringify(o.value);
        auditLines.push(`| ${o.operator} | ${o.status} | ${val || "—"} |`);
      }
      auditLines.push(``);
    }
  }
  writeMd(join(REPORTS, "operator-profile-live-field-by-field-audit.md"), auditLines.join("\n"));

  const beforeSummary = summarize(
    RETAINED_KEYS.flatMap((k) =>
      production.map((m) => ({
        afterClass: classifyCell(profileBy[m.id]?.fields?.[k]),
        field: k,
      }))
    ),
    production.length
  );

  const { rows, patches, researchLog, fieldBatch } = buildProposals(production, profileBy);
  const afterSummary = summarize(rows, production.length);
  const qaIssues = verticalQa(fieldBatch);

  const stop = {
    profileRetainedFields: RETAINED_KEYS,
    productionOperators: production.length,
    retainedProfileCells: afterSummary.retainedCells,
    actualValuesBefore: beforeSummary.actual,
    unresolvedBefore: beforeSummary.unresolved,
    blanksBefore: beforeSummary.blank,
    targetedResearchPerformed: Object.keys(D4D_PROFILE_ACTUAL).length,
    officialSourcesAdded: Object.values(D4D_PROFILE_ACTUAL).reduce((n, p) => n + (p.sources?.length || 0), 0),
    actualValuesAfter: afterSummary.actual,
    unresolvedAfter: afterSummary.unresolved,
    blanksAfter: afterSummary.blank,
    genericValues: qaIssues.filter((i) => i.type === "generic").length,
    companyDescriptionActualCoverage: fieldCoverage(rows, "companyDescription"),
    companyHistoryActualCoverage: fieldCoverage(rows, "companyHistory"),
    differentiatorsActualCoverage: fieldCoverage(rows, "differentiators"),
    headquartersCoverage: fieldCoverage(rows, "headquarters"),
    websiteCoverage: fieldCoverage(rows, "website"),
    companySizeCoverage: fieldCoverage(rows, "companySize"),
    parentOwnershipCoverage: production.map((m) => ({
      operator: m.fields.company_name,
      value: m.fields["Operator Parent Company"] || null,
      class: classifyCell(m.fields["Operator Parent Company"]),
    })),
    operatingModelCoverage: production.map((m) => ({
      operator: m.fields.company_name,
      value: m.fields["Operating Model"] || null,
      class: classifyCell(m.fields["Operating Model"]),
    })),
    managementAvailabilityCoverage: production.map((m) => ({
      operator: m.fields.company_name,
      value: m.fields["Management Availability"] || null,
      class: classifyCell(m.fields["Management Availability"]),
    })),
    fieldsRemovedDeprecated: PROFILE_DEPRECATED_HIDE,
    fieldsVisibleButUnpopulatedExpectedZero: PROFILE_DEPRECATED_HIDE.length,
    profileActualResearchCoveragePct: afterSummary.actualResearchCoveragePct,
    profileUnresolvedCoveragePct: afterSummary.unresolvedCoveragePct,
    profileFounderVisualVerdict: afterSummary.unresolved === 0 && afterSummary.blank === 0 ? "PASS" : "NEEDS REVIEW",
    authorizationRequiredBeforePlatform: true,
    fitBlocked: true,
    qaIssues,
    researchLog,
    patchCount: patches.length,
    mode: args.apply ? "apply" : "dry-run",
  };

  writeJson(join(OUT, "proposals.json"), { rows, patches, researchLog, stop, qaIssues });
  writeJson(join(OUT, "d4d-profile-stop-point.json"), stop);

  // Preview (retained fields only)
  const previewLines = [
    `# Operator Profile — Actual Completion Preview (D.4D)`,
    ``,
    `Retained fields only. **Actual research coverage: ${afterSummary.actualResearchCoveragePct}%**. Unresolved: ${afterSummary.unresolved}.`,
    ``,
  ];
  for (const m of production) {
    const pr = profileBy[m.id]?.fields || {};
    previewLines.push(`## ${m.fields.company_name}`, ``, `| Field | Value | Class |`, `| ----- | ----- | ----- |`);
    for (const k of RETAINED_KEYS) {
      const v = rows.find((r) => r.masterId === m.id && r.field === k)?.proposed ?? pr[k];
      const cls = classifyCell(v);
      const cell =
        typeof v === "string"
          ? v.replace(/\n/g, " ").slice(0, 180)
          : Array.isArray(v)
            ? v.join("; ")
            : v ?? "—";
      previewLines.push(`| ${k} | ${cell} | ${cls} |`);
    }
    previewLines.push(``);
  }
  writeMd(join(DOCS, "reviews/operator-profile-actual-completion-preview.md"), previewLines.join("\n"));

  // Unresolved research audit
  const unresolvedRows = rows.filter((r) => r.afterClass === "UNRESOLVED");
  writeMd(
    join(REPORTS, "operator-profile-unresolved-research-audit.md"),
    [
      `# Profile Unresolved Research Audit`,
      ``,
      `Unresolved states are **not** counted as actual research completion.`,
      ``,
      `| Operator | Field | Before | Research attempts |`,
      `| -------- | ----- | ------ | ----------------- |`,
      ...unresolvedRows.map(
        (r) =>
          `| ${r.operator} | ${r.field} | ${JSON.stringify(r.before)?.slice(0, 80)} | ${r.researchStatus} |`
      ),
      ``,
    ].join("\n")
  );

  let writes = 0;
  let failures = 0;
  if (args.apply) {
    const backupDir = join(ROOT, "backups/operator-setup/d4d-profile", ts);
    mkdirSync(backupDir, { recursive: true });
    writeJson(join(backupDir, "profiles-before.json"), profiles);
    writeJson(join(backupDir, "proposals.json"), { rows, patches, stop });

    console.log(`Backup → ${backupDir}`);
    console.log(`Applying ${patches.length} profile patches...`);
    for (const p of patches) {
      try {
        await patchRecord(baseId, token, PROFILE, p.recordId, p.fields);
        writes++;
        await sleep(55);
      } catch (e) {
        failures++;
        console.error(p.operator, e.message || e);
      }
    }
    stop.airtableWrites = writes;
    stop.failures = failures;
    stop.backupDir = backupDir;
  }

  console.log(JSON.stringify({ ...stop, qaPass: qaIssues.length === 0 && afterSummary.blank === 0 }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
