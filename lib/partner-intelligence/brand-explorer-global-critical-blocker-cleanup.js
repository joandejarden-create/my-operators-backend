/**
 * Global Active — Critical semantic blocker extraction + cleanup (Critical-only).
 *
 * Read audit → extract Critical findings → targeted Presentation Title/Body patches.
 * No Brand Status / release / CV / Source / Registry / image writes.
 * Excludes: Four Points Flex, House of Originals, Morgans Originals, Radisson Collection.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { isOwnerFacingPresentationRow } from "./brand-explorer-public-visibility-quality-lock.js";
import { getWave13ActiveIdentityBySlug } from "./brand-explorer-wave13-active-identity-anchors.js";
import {
  runGlobalActiveSemanticAudit,
  writeGlobalActiveSemanticAuditReports,
  EXPECTED_ACTIVE_UNIVERSE_COUNT,
  EXCLUDED_FROM_ACTIVE_SEMANTIC_AUDIT,
} from "./brand-explorer-global-active-semantic-audit.js";

export const GLOBAL_CRITICAL_BLOCKER_CLEANUP_VERSION =
  "global-critical-blocker-cleanup-v2";

export const GLOBAL_CRITICAL_BLOCKER_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-global-critical-blocker-cleanup",
  "--confirm-refreshed-global-audit-used",
  "--confirm-critical-findings-only",
  "--confirm-targeted-visible-copy-only",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-image-writes",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-gate-weakening",
  "--confirm-no-internal-source-language",
  "--confirm-no-placeholder-property-titles",
  "--confirm-no-unsupported-cala-claims",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 320;
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const NEVER_WRITE_BRANDS = new Set(
  EXCLUDED_FROM_ACTIVE_SEMANTIC_AUDIT.map((s) => s.toLowerCase())
);

/** Critical phrase replacements — owner-facing only. */
const CRITICAL_TEXT_REPLACEMENTS = Object.freeze([
  { re: /\bcurrent source pack\b/gi, to: "current brand materials" },
  { re: /\bsource pack\b/gi, to: "brand materials" },
  { re: /\bsource-supported\b/gi, to: "verified" },
  { re: /\bsteward-matched\b/gi, to: "verified" },
  { re: /\bsteward-confirmed\b/gi, to: "verified" },
  { re: /\bsteward notes?\b/gi, to: "review notes" },
  { re: /\bsteward\b/gi, to: "review" },
  { re: /\bvisual diligence\b/gi, to: "visual comparison" },
  { re: /\bunderwriting lane\b/gi, to: "affiliation comparison" },
  { re: /\bbrand-lane evidence\b/gi, to: "brand-specific evidence" },
  { re: /\bassessed as its own brand lane\b/gi, to: "evaluated as its own brand" },
  { re: /\bits own brand lane\b/gi, to: "its own brand" },
  { re: /\bbrand lane\b/gi, to: "brand positioning" },
  { re: /\bUse this labeled example\b/gi, to: "This property example" },
  { re: /\bDirectory card\b/gi, to: "Directory listing" },
  { re: /\bfactory QA\b/gi, to: "quality review" },
  { re: /\bQA gate\b/gi, to: "quality review" },
  { re: /\bQA checklist\b/gi, to: "quality checklist" },
  { re: /\bearly QA\b/gi, to: "early quality checks" },
  { re: /\bgovernance\b/gi, to: "operating discipline" },
  { re: /\bfactory\b/gi, to: "build process" },
  // Strip internal Stage / Brand Basics process clauses before residual Stage token scrub
  {
    re: /\s*No Brand Basics record yet[^.]*\./gi,
    to: "",
  },
  {
    re: /\s*Do not create Brand Basics in Stage\s*\d[^.]*\./gi,
    to: "",
  },
  {
    re: /\s*Do not create Brand Basics in\.?/gi,
    to: "",
  },
  {
    re: /\s*[—-]\s*Stage\s*\d\b[^.]*\./gi,
    to: "",
  },
  {
    re: /\bStage\s*\d\b[^.!?\n]*/gi,
    to: "",
  },
  { re: /\bdo not reuse\b/gi, to: "do not mix" },
  { re: /\bkeep sibling\b/gi, to: "keep adjacent brands" },
  { re: /\bavoid borrowing\b/gi, to: "avoid mixing" },
  { re: /\bSource:\s*/gi, to: "Basis: " },
  { re: /\bSources:\s*/gi, to: "Basis: " },
]);

const ARCHETYPE_TITLE_RE =
  /^(US |North America |Suburban (?!Studios\b)|Airport |Upper-midscale |US longer|employment|kitchenette|competitive sets|secondary)/i;

const GENERIC_PROPERTY_TITLE_RE =
  /^(.+?)\s*[-—]\s*(International Reference|CALA|Property Example|Americas Reference)$/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function checkFlags(required, argv, apply) {
  const missing = required.filter((f) => !argv.includes(f));
  return { apply: apply === true, ok: apply === true && missing.length === 0, missing, required: [...required] };
}

async function airtablePatch(baseId, apiKey, table, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`PATCH ${recordId} → ${res.status}: ${text.slice(0, 300)}`);
  }
  return res.json();
}

export function sanitizeCriticalOwnerFacingText(text) {
  let s = String(text || "");
  for (const rule of CRITICAL_TEXT_REPLACEMENTS) {
    s = s.replace(rule.re, rule.to);
  }
  // Collapse leftover double spaces / punctuation from clause removals
  s = s
    .replace(/[ \t]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/\.\s*\./g, ".")
    .replace(/\s+\./g, ".")
    .trim();
  return s;
}

function brandNameCandidates(brandSlug, brandName) {
  const names = [];
  const push = (v) => {
    const s = nz(v);
    if (s && !names.includes(s)) names.push(s);
  };
  push(brandName);
  const anchor = getWave13ActiveIdentityBySlug(brandSlug);
  if (anchor) {
    push(anchor.name);
    for (const a of anchor.nameAliases || []) push(a);
  }
  return names;
}

async function fetchPresentationRowById(recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey || !recordId) return null;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  if (!res.ok) return null;
  const rec = await res.json();
  const f = rec.fields || {};
  return {
    recordId: rec.id,
    slotKey: nz(f["Slot Key"]),
    title: nz(f.Title),
    body: nz(f.Body),
    brandName: nz(f["Brand Name"]),
    active: f.Active !== false,
    externalDisplayStatus: nz(f["External Display Status"]),
    sortOrder: f["Sort Order"] ?? 0,
  };
}

async function listOwnerRowsForCleanup(brandSlug, brandName, brandRecordId) {
  const seen = new Map();
  for (const name of brandNameCandidates(brandSlug, brandName)) {
    const live = await listPresentationRowsLight(brandRecordId, name);
    for (const r of live.rows || []) {
      if (r?.recordId && !seen.has(r.recordId)) seen.set(r.recordId, r);
    }
    if (seen.size > 0) break;
  }
  return [...seen.values()].filter(isOwnerFacingPresentationRow);
}

function stillHasCriticalPhrase(text) {
  const blob = String(text || "");
  return (
    /\bsource pack\b/i.test(blob) ||
    /\bsource-supported\b/i.test(blob) ||
    /\bsteward(?:-matched|-confirmed)?\b/i.test(blob) ||
    /\bvisual diligence\b/i.test(blob) ||
    /\bunderwriting lane\b/i.test(blob) ||
    /\bbrand-lane evidence\b/i.test(blob) ||
    /\bown brand lane\b/i.test(blob) ||
    /\bfactory\b/i.test(blob) ||
    /\bStage\s*\d/i.test(blob) ||
    /\bgovernance\b/i.test(blob) ||
    /\bUse this labeled example\b/i.test(blob) ||
    /\bSource:\s*/i.test(blob) ||
    /\bSources:\s*/i.test(blob)
  );
}

/**
 * Extract Critical findings from a global audit report into a failure extract.
 */
export function extractCriticalBlockerFailuresFromAudit(auditReport) {
  const failures = [];
  const wave14 = new Set([
    "marriott-hotels",
    "sheraton",
    "westin",
    "residence-inn-by-marriott",
    "springhill-suites-by-marriott",
    "towneplace-suites-by-marriott",
    "aloft-hotels",
    "studiores",
  ]);

  for (const b of auditReport.brandResults || []) {
    if (NEVER_WRITE_BRANDS.has(nz(b.brandSlug).toLowerCase())) continue;
    for (const f of b.findings || []) {
      if (f.severity !== "critical") continue;
      failures.push({
        brand: b.brandName,
        brandSlug: b.brandSlug,
        recordId: b.recordId || null,
        presentationRecordId: f.recordId || null,
        section: f.section,
        field: "Title/Body",
        slotKey: f.slotKey || null,
        currentVisibleCopy: nz(f.currentValue),
        criticalTermOrFailure: f.failureType,
        proposedOwnerFacingFix: f.proposedFix || "Sanitize critical internal/source language",
        sourceSupport: f.needsSteward ? "requires_source_check" : "text_cleanup",
        writeNeeded: true,
        isWave14: wave14.has(nz(b.brandSlug).toLowerCase()),
      });
    }
  }

  return {
    version: GLOBAL_CRITICAL_BLOCKER_CLEANUP_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    writePerformed: false,
    auditGeneratedAt: auditReport.generatedAt || null,
    universe: {
      activeCount: auditReport.activeCount,
      expected: EXPECTED_ACTIVE_UNIVERSE_COUNT,
      reconciled: auditReport.universeReconciled,
    },
    summary: {
      criticalFindingCount: failures.length,
      brandsWithCritical: [...new Set(failures.map((f) => f.brandSlug))].length,
      wave14CriticalCount: failures.filter((f) => f.isWave14).length,
    },
    failures,
  };
}

export function writeCriticalBlockerFailuresReports(extract) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-global-critical-blocker-failures.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-global-critical-blocker-failures.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(extract, null, 2)}\n`, "utf8");

  const lines = [
    "# Global Active — Critical Semantic Blocker Failures",
    "",
    `Generated: ${extract.generatedAt}`,
    `Audit generated: ${extract.auditGeneratedAt}`,
    `Critical findings: **${extract.summary.criticalFindingCount}**`,
    `Brands with Critical: **${extract.summary.brandsWithCritical}**`,
    `Wave 14 Critical (should be 0): **${extract.summary.wave14CriticalCount}**`,
    "",
    "| Brand | Slug | Presentation Record | Section | Failure | Write Needed? | Current Visible Copy |",
    "|-------|------|---------------------|---------|---------|---------------|----------------------|",
  ];
  for (const f of extract.failures) {
    lines.push(
      `| ${f.brand} | \`${f.brandSlug}\` | ${f.presentationRecordId || "—"} | ${String(f.section || "").replace(/\|/g, "/")} | ${f.criticalTermOrFailure} | ${f.writeNeeded} | ${String(f.currentVisibleCopy || "").replace(/\|/g, "/").replace(/\n/g, " ").slice(0, 100)} |`
    );
  }
  lines.push("");
  fs.writeFileSync(mdPath, lines.join("\n"), "utf8");
  return { jsonPath, mdPath };
}

function planPatchesForBrand(brandSlug, brandName, brandRecordId, criticalFailures) {
  return listOwnerRowsForCleanup(brandSlug, brandName, brandRecordId).then(async (ownerRows) => {
    const byId = new Map(ownerRows.map((r) => [r.recordId, r]));
    const patches = [];
    const touched = new Set();

    const candidates = [];
    for (const f of criticalFailures) {
      const pid = f.presentationRecordId;
      if (pid && byId.has(pid)) {
        candidates.push(byId.get(pid));
      } else if (pid && !byId.has(pid)) {
        const fetched = await fetchPresentationRowById(pid);
        if (fetched && isOwnerFacingPresentationRow(fetched)) {
          byId.set(fetched.recordId, fetched);
          candidates.push(fetched);
        }
      }
    }
    // Also scan all owner rows for residual critical phrases (catch unlinked findings)
    for (const r of ownerRows) {
      const text = `${nz(r.title)}\n${nz(r.body)}`;
      if (stillHasCriticalPhrase(text) || ARCHETYPE_TITLE_RE.test(nz(r.title))) {
        candidates.push(r);
      }
    }

    for (const row of candidates) {
      if (!row?.recordId || touched.has(row.recordId)) continue;
      touched.add(row.recordId);

      const titleBefore = nz(row.title);
      const bodyBefore = nz(row.body);
      let titleAfter = sanitizeCriticalOwnerFacingText(titleBefore);
      let bodyAfter = sanitizeCriticalOwnerFacingText(bodyBefore);

      // Hide unverified archetype / placeholder openings cards
      const hideCard =
        row.slotKey === "footprint.openings" &&
        (ARCHETYPE_TITLE_RE.test(titleBefore) ||
          (GENERIC_PROPERTY_TITLE_RE.test(titleBefore) &&
            !/\b(Hotel|Inn|Suites|Resort|Collection|Residence|Aloft|Moxy|Westin|Sheraton|Marriott)\b/i.test(
              titleBefore.split(/[-—]/)[0] || ""
            )));

      const fields = {};
      if (titleAfter !== titleBefore) fields.Title = titleAfter;
      if (bodyAfter !== bodyBefore) fields.Body = bodyAfter;
      if (hideCard) {
        fields.Active = false;
        fields["External Display Status"] = "Do Not Display";
      }

      // Strip trailing inline raw URLs from body except momentum/openings (structured trailing URL allowed)
      if (
        fields.Body != null &&
        !/footprint\.(momentum|openings)/i.test(nz(row.slotKey)) &&
        /https?:\/\//i.test(fields.Body)
      ) {
        fields.Body = fields.Body.replace(/\s*https?:\/\/\S+/gi, "").trim();
      }

      if (!Object.keys(fields).length) continue;
      if (stillHasCriticalPhrase(`${fields.Title || titleAfter}\n${fields.Body || bodyAfter}`) && !hideCard) {
        // Last-resort strip of remaining Stage/factory if replacement left residue
        if (fields.Body) fields.Body = sanitizeCriticalOwnerFacingText(fields.Body);
        if (fields.Title) fields.Title = sanitizeCriticalOwnerFacingText(fields.Title);
      }

      patches.push({
        action: "PATCH",
        table: PRESENTATION_TABLE,
        recordId: row.recordId,
        brandSlug,
        slotKey: row.slotKey,
        fields,
        before: { title: titleBefore.slice(0, 120), body: bodyBefore.slice(0, 160) },
        after: {
          title: (fields.Title || titleAfter).slice(0, 120),
          body: (fields.Body || bodyAfter).slice(0, 160),
          hidden: hideCard,
        },
      });
    }

    return patches;
  });
}

export async function runGlobalCriticalBlockerCleanup({
  dryRun = true,
  argv = [],
  auditReport = null,
} = {}) {
  const apply = argv.includes("--apply") && dryRun === false;
  const flagCheck = checkFlags(GLOBAL_CRITICAL_BLOCKER_CLEANUP_APPLY_FLAGS, argv, apply);

  if (apply && !flagCheck.ok) {
    return {
      version: GLOBAL_CRITICAL_BLOCKER_CLEANUP_VERSION,
      generatedAt: new Date().toISOString(),
      applyPerformed: false,
      pass: false,
      stopRecommended: true,
      readyStatement: "global_critical_blocker_cleanup_blocked_missing_flags",
      missingFlags: flagCheck.missing,
    };
  }

  // Prefer caller-provided fresh audit; otherwise run fresh
  let audit = auditReport;
  if (!audit) {
    audit = await runGlobalActiveSemanticAudit({ dryRun: true, brands: null });
    writeGlobalActiveSemanticAuditReports(audit, { refresh: true });
  }

  const extract = extractCriticalBlockerFailuresFromAudit(audit);
  writeCriticalBlockerFailuresReports(extract);

  if (extract.summary.criticalFindingCount === 0) {
    const report = {
      version: GLOBAL_CRITICAL_BLOCKER_CLEANUP_VERSION,
      generatedAt: new Date().toISOString(),
      dryRun: !apply,
      applyPerformed: false,
      writePerformed: false,
      criticalFindingCount: 0,
      patchCount: 0,
      readyStatement: "global_active_critical_semantic_blockers_clean",
      freezeNote: "Critical blockers = 0. Do not freeze 54 until High-severity review / explicit freeze decision.",
      extractSummary: extract.summary,
    };
    writeGlobalCriticalBlockerCleanupReports(report);
    return report;
  }

  // Group failures by brand
  const byBrand = new Map();
  for (const f of extract.failures) {
    if (!byBrand.has(f.brandSlug)) {
      byBrand.set(f.brandSlug, {
        brandSlug: f.brandSlug,
        brandName: f.brand,
        recordId: f.recordId,
        failures: [],
      });
    }
    byBrand.get(f.brandSlug).failures.push(f);
  }

  const brandPlans = [];
  for (const [slug, meta] of byBrand) {
    if (NEVER_WRITE_BRANDS.has(slug.toLowerCase())) {
      brandPlans.push({ ...meta, patches: [], skipped: "excluded_brand" });
      continue;
    }
    if (!meta.recordId) {
      brandPlans.push({ ...meta, patches: [], skipped: "missing_record_id" });
      continue;
    }
    process.stdout.write(`[critical-cleanup] plan ${slug}...\n`);
    const patches = await planPatchesForBrand(slug, meta.brandName, meta.recordId, meta.failures);
    brandPlans.push({ ...meta, patches, patchCount: patches.length });
    await sleep(250);
  }

  let applyResult = { applied: 0, errors: [] };
  if (apply) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("Missing AIRTABLE_BASE_ID / AIRTABLE_API_KEY");
    for (const plan of brandPlans) {
      for (const p of plan.patches || []) {
        try {
          await airtablePatch(baseId, apiKey, p.table, p.recordId, p.fields);
          applyResult.applied += 1;
        } catch (err) {
          applyResult.errors.push({
            brandSlug: plan.brandSlug,
            recordId: p.recordId,
            slotKey: p.slotKey,
            error: err?.message || String(err),
          });
        }
        await sleep(WRITE_THROTTLE_MS);
      }
    }
  }

  const totalPatches = brandPlans.reduce((n, b) => n + (b.patches?.length || 0), 0);
  const report = {
    version: GLOBAL_CRITICAL_BLOCKER_CLEANUP_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyPerformed: apply === true,
    writePerformed: apply === true && applyResult.applied > 0,
    criticalFindingCount: extract.summary.criticalFindingCount,
    brandsTouched: brandPlans.filter((b) => (b.patches || []).length > 0).map((b) => b.brandSlug),
    patchCount: totalPatches,
    applyResult,
    brandPlans: brandPlans.map((b) => ({
      brandSlug: b.brandSlug,
      brandName: b.brandName,
      recordId: b.recordId,
      failureCount: (b.failures || []).length,
      patchCount: (b.patches || []).length,
      skipped: b.skipped || null,
      patches: b.patches || [],
    })),
    readyStatement: apply
      ? applyResult.errors.length === 0
        ? "global_active_critical_semantic_blockers_clean"
        : "global_critical_blocker_cleanup_applied_with_errors"
      : "global_critical_blocker_cleanup_dry_run_ready",
    freezeNote: "Do not freeze 54 in this task — Critical cleanup only; High/Medium remain for later review.",
  };

  writeGlobalCriticalBlockerCleanupReports(report);
  return report;
}

export function writeGlobalCriticalBlockerCleanupReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-global-critical-blocker-cleanup.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-global-critical-blocker-cleanup.md");
  const byBrandPath = path.join(REPORTS_DIR, "brand-explorer-global-critical-blocker-cleanup-by-brand.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-global-critical-blocker-cleanup.md");

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    "# Global Active — Critical Semantic Blocker Cleanup",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.applyPerformed ? "APPLY" : "dry-run"}**`,
    "",
    `Ready: \`${report.readyStatement}\``,
    `Critical findings in: **${report.criticalFindingCount ?? 0}**`,
    `Patches: **${report.patchCount ?? 0}**`,
    `Applied: **${report.applyResult?.applied ?? 0}**`,
    `Errors: **${report.applyResult?.errors?.length ?? 0}**`,
    "",
    report.freezeNote || "",
    "",
  ];

  const byBrandLines = ["# Global Critical Blocker Cleanup — By Brand", ""];
  for (const b of report.brandPlans || []) {
    lines.push(`## ${b.brandName} (\`${b.brandSlug}\`)`);
    lines.push(`Failures: ${b.failureCount} · Patches: ${b.patchCount}${b.skipped ? ` · skipped=${b.skipped}` : ""}`);
    byBrandLines.push(`## ${b.brandName} (\`${b.brandSlug}\`)`);
    byBrandLines.push(`Patches: ${b.patchCount}`);
    for (const p of (b.patches || []).slice(0, 20)) {
      const line = `- \`${p.slotKey}\` ${p.recordId}: ${(p.before?.title || "").slice(0, 50)} → ${(p.after?.title || "").slice(0, 50)}${p.after?.hidden ? " [HIDE]" : ""}`;
      lines.push(line);
      byBrandLines.push(line);
    }
    lines.push("");
    byBrandLines.push("");
  }

  const md = lines.join("\n");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");
  fs.writeFileSync(byBrandPath, byBrandLines.join("\n"), "utf8");
  return { jsonPath, mdPath, byBrandPath, docsPath };
}
