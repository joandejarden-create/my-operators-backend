/**
 * Wave 14 — Public Active semantic blocker cleanup.
 *
 * Scope: eight active Wave 14 brands only. Targeted Presentation Title/Body/
 * Case Summary patches for rows that fail PVQL forbidden language / semantic
 * steward / Source: / momentum brand-page / Owner Considerations leaks.
 *
 * Does NOT write Brand Status, release fields, CV, Source Library, Registry,
 * images, Four Points Flex, or non-Wave-14 brands.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import { generateWave14PresentationPack } from "./brand-explorer-wave14-tab-factory-build.js";
import { isOwnerFacingPresentationRow } from "./brand-explorer-public-visibility-quality-lock.js";
import {
  WAVE14_VERSION,
  WAVE14_PARTIAL_PROMOTION_SLUGS,
  WAVE14_HELD_PROMOTION_SLUG,
  WAVE14_NEVER_WRITE_FIELDS,
  WAVE14_PROTECTED_BASELINE_COUNT,
} from "./brand-explorer-wave14-factory-plan.js";
import {
  extractWave14PublicActiveSemanticBlockers,
  writeWave14PublicActiveSemanticBlockersReports,
  WAVE14_SEMANTIC_FORBIDDEN_RES,
} from "./brand-explorer-wave14-public-active-semantic-blockers.js";

export const WAVE14_PUBLIC_ACTIVE_SEMANTIC_BLOCKER_CLEANUP_VERSION =
  "wave14-public-active-semantic-blocker-cleanup-v1";

export const WAVE14_PUBLIC_ACTIVE_SEMANTIC_BLOCKER_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-public-active-semantic-blocker-cleanup",
  "--confirm-eight-active-wave14-scope",
  "--confirm-targeted-visible-copy-only",
  "--confirm-pvql-failures-extracted",
  "--confirm-24tab-failures-extracted",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-non-wave14-active-brand-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-image-writes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-internal-source-language",
  "--confirm-no-placeholder-property-titles",
  "--confirm-recent-momentum-semantics-preserved",
  "--confirm-portfolio-mix-structured",
  "--confirm-no-gate-weakening",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 320;
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

/** Slots eligible for targeted Title/Body (and optional Case Summary) patches. */
const TARGETED_SLOT_RE =
  /^(footprint\.(momentum|portfolio_mix|openings|region\.)|overview\.(development_model|why_value|proof\.|featured_application|relative_positioning|typical_use_case)|valueOwners\.watchouts|insight\.similar)/i;

const FORBIDDEN_VISIBLE_RES = Object.freeze([
  ...WAVE14_SEMANTIC_FORBIDDEN_RES.map((r) => r.re),
  /\bvisual diligence\b/i,
  /\bUse this labeled example\b/i,
  /\bDirectory card\b/i,
  /guest brand site supports positioning/i,
  /development materials confirm/i,
  /Brand Site Confirms/i,
]);

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

function rowHasForbidden(r) {
  const text = `${nz(r.title)}\n${nz(r.body)}\n${nz(r.caseSummaryOverview)}\n${nz(r.caseSummaryBrandRelevance)}\n${nz(r.caseSummaryOwnerObjective)}\n${nz(r.caseSummaryInterpretation)}\n${nz(r.caseSummaryTags)}`;
  return FORBIDDEN_VISIBLE_RES.some((re) => re.test(text));
}

function planBrandCleanup(slug) {
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!identity?.recordId) return { brandSlug: slug, blocked: true, blockers: ["unknown_identity"] };

  const newPack = generateWave14PresentationPack(slug);
  const newRows = (newPack.presentation || []).filter((r) => TARGETED_SLOT_RE.test(nz(r.slotKey)));

  const blockers = [];
  for (const r of newRows) {
    const text = `${r.title || ""}\n${r.body || ""}`;
    for (const re of FORBIDDEN_VISIBLE_RES) {
      if (re.test(text)) blockers.push(`${slug}:forbidden_in_new_pack:${re.source}:${r.slotKey}`);
    }
  }

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    blocked: blockers.length > 0,
    blockers,
    newRows,
  };
}

/**
 * Match live dirty rows to regenerated pack rows by slotKey position.
 */
function buildPatchesForBrand(plan, liveRows, failureRecordIds) {
  const patches = [];
  const liveTargeted = (liveRows || []).filter(
    (r) => isOwnerFacingPresentationRow(r) && TARGETED_SLOT_RE.test(nz(r.slotKey))
  );

  const liveBySlot = new Map();
  for (const r of liveTargeted) {
    const key = r.slotKey;
    if (!liveBySlot.has(key)) liveBySlot.set(key, []);
    liveBySlot.get(key).push(r);
  }
  for (const [, arr] of liveBySlot) {
    arr.sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
  }

  const newBySlot = new Map();
  for (const r of plan.newRows) {
    const key = r.slotKey;
    if (!newBySlot.has(key)) newBySlot.set(key, []);
    newBySlot.get(key).push(r);
  }

  const dirtyIds = new Set(failureRecordIds || []);

  for (const [slotKey, newSlotRows] of newBySlot) {
    const liveSlotRows = liveBySlot.get(slotKey) || [];
    for (let i = 0; i < Math.max(newSlotRows.length, liveSlotRows.length); i++) {
      const newRow = newSlotRows[i] || newSlotRows[newSlotRows.length - 1];
      const existing = liveSlotRows[i];
      if (!existing?.recordId || !newRow) continue;

      const dirty = dirtyIds.has(existing.recordId) || rowHasForbidden(existing);
      if (!dirty && nz(existing.body) === nz(newRow.body) && nz(existing.title) === nz(newRow.title)) {
        continue;
      }
      // Always refresh dirty rows; also refresh portfolio_mix / momentum / owner considerations when dirty pack exists
      if (!dirty && !/portfolio_mix|featured_application|development_model|why_value|proof\.|region\.|momentum$/.test(slotKey)) {
        continue;
      }
      if (!dirty && !rowHasForbidden({ title: existing.title, body: existing.body })) {
        // Still patch portfolio_mix for all brands to drop Source: if present
        if (slotKey !== "footprint.portfolio_mix" && !/momentum$/.test(slotKey)) continue;
        if (slotKey === "footprint.portfolio_mix" && !/\bSource:\s*/i.test(nz(existing.body))) continue;
      }

      const fields = {
        Title: newRow.title || "",
        Body: newRow.body || "",
      };
      // Title/Body only — Case Summary field names historically 422 on this table.

      for (const f of WAVE14_NEVER_WRITE_FIELDS) delete fields[f];

      const changed =
        fields.Title !== nz(existing.title) || fields.Body !== nz(existing.body);

      if (!changed) continue;

      patches.push({
        action: "PATCH",
        table: PRESENTATION_TABLE,
        recordId: existing.recordId,
        slotKey,
        brandSlug: plan.brandSlug,
        fields,
        before: {
          title: nz(existing.title).slice(0, 120),
          body: nz(existing.body).slice(0, 160),
        },
        after: {
          title: fields.Title.slice(0, 120),
          body: fields.Body.slice(0, 160),
        },
      });
    }
  }

  // Extra: hide remaining dirty momentum brand-page cards beyond pack length
  const liveMomentum = liveBySlot.get("footprint.momentum") || [];
  const newMomentum = newBySlot.get("footprint.momentum") || [];
  for (let i = newMomentum.length; i < liveMomentum.length; i++) {
    const existing = liveMomentum[i];
    if (!existing?.recordId) continue;
    if (!rowHasForbidden(existing) && !/guest brand site|development materials confirm/i.test(nz(existing.title))) {
      continue;
    }
    // Overwrite with last good new momentum content rather than hide (visibility write allowed only if needed)
    const donor = newMomentum[newMomentum.length - 1];
    if (!donor) continue;
    patches.push({
      action: "PATCH",
      table: PRESENTATION_TABLE,
      recordId: existing.recordId,
      slotKey: "footprint.momentum",
      brandSlug: plan.brandSlug,
      fields: { Title: donor.title || "", Body: donor.body || "" },
      before: { title: nz(existing.title).slice(0, 120), body: nz(existing.body).slice(0, 160) },
      after: { title: (donor.title || "").slice(0, 120), body: (donor.body || "").slice(0, 160) },
      note: "extra_dirty_momentum_overwritten_with_last_valid",
    });
  }

  return patches;
}

export async function runWave14PublicActiveSemanticBlockerCleanup({
  dryRun = true,
  argv = [],
  brands = null,
} = {}) {
  const apply = argv.includes("--apply") && dryRun === false;
  const flagCheck = checkFlags(WAVE14_PUBLIC_ACTIVE_SEMANTIC_BLOCKER_CLEANUP_APPLY_FLAGS, argv, apply);

  if (apply && !flagCheck.ok) {
    return {
      version: WAVE14_PUBLIC_ACTIVE_SEMANTIC_BLOCKER_CLEANUP_VERSION,
      generatedAt: new Date().toISOString(),
      applyPerformed: false,
      pass: false,
      stopRecommended: true,
      readyStatement: "wave14_public_active_semantic_blocker_cleanup_blocked_missing_flags",
      missingFlags: flagCheck.missing,
    };
  }

  const targetSlugs = (brands || WAVE14_PARTIAL_PROMOTION_SLUGS).filter(
    (s) => s !== WAVE14_HELD_PROMOTION_SLUG
  );

  // 1) Extract failures first (required before patch)
  const blockersReport = await extractWave14PublicActiveSemanticBlockers({ brands: targetSlugs });
  writeWave14PublicActiveSemanticBlockersReports(blockersReport);

  const brandPlans = [];
  for (const slug of targetSlugs) {
    brandPlans.push(planBrandCleanup(slug));
  }
  const anyBlocked = brandPlans.some((b) => b.blocked);

  const patchPlans = [];
  for (const plan of brandPlans) {
    if (plan.blocked) {
      patchPlans.push({ ...plan, patches: [], patchCount: 0 });
      continue;
    }
    const live = await listPresentationRowsLight(plan.recordId, plan.brandName);
    const failureRecordIds = (blockersReport.brandResults || [])
      .find((b) => b.brandSlug === plan.brandSlug)
      ?.failures?.map((f) => f.recordId)
      .filter(Boolean);
    const patches = buildPatchesForBrand(plan, live.rows || [], failureRecordIds);
    patchPlans.push({ ...plan, patches, patchCount: patches.length });
    await sleep(200);
  }

  let applyResult = { applied: 0, errors: [] };
  if (apply && !anyBlocked) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("Missing AIRTABLE_BASE_ID / AIRTABLE_API_KEY");

    for (const plan of patchPlans) {
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

  const report = {
    version: WAVE14_PUBLIC_ACTIVE_SEMANTIC_BLOCKER_CLEANUP_VERSION,
    factoryVersion: WAVE14_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyPerformed: apply === true,
    writePerformed: apply === true && applyResult.applied > 0,
    protectedBaselineCount: WAVE14_PROTECTED_BASELINE_COUNT,
    scope: {
      brands: targetSlugs,
      heldUntouched: WAVE14_HELD_PROMOTION_SLUG,
    },
    blockersExtracted: blockersReport.summary,
    anyNewPackBlocked: anyBlocked,
    brandPlans: patchPlans.map((b) => ({
      brandSlug: b.brandSlug,
      brandName: b.brandName,
      recordId: b.recordId,
      blocked: b.blocked,
      blockers: b.blockers,
      patchCount: b.patchCount,
      patches: b.patches,
    })),
    applyResult,
    readyStatement: apply
      ? applyResult.errors.length === 0
        ? "wave14_public_active_semantic_blockers_clean"
        : "wave14_public_active_semantic_blocker_cleanup_applied_with_errors"
      : anyBlocked
        ? "wave14_public_active_semantic_blocker_cleanup_dry_run_blocked"
        : "wave14_public_active_semantic_blocker_cleanup_dry_run_ready",
  };

  writeWave14PublicActiveSemanticBlockerCleanupReports(report);
  return report;
}

export function writeWave14PublicActiveSemanticBlockerCleanupReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave14-public-active-semantic-blocker-cleanup.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave14-public-active-semantic-blocker-cleanup.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-wave14-public-active-semantic-blocker-cleanup.md");

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    "# Wave 14 — Public Active Semantic Blocker Cleanup",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.applyPerformed ? "APPLY" : "dry-run"}**`,
    "",
    `Ready: \`${report.readyStatement}\``,
    `Patches planned: **${report.brandPlans.reduce((n, b) => n + (b.patchCount || 0), 0)}**`,
    `Patches applied: **${report.applyResult?.applied || 0}**`,
    `Apply errors: **${report.applyResult?.errors?.length || 0}**`,
    `Held / untouched: \`${report.scope.heldUntouched}\``,
    `Protected baseline remains: **${report.protectedBaselineCount}**`,
    "",
    "## Per brand",
    "",
  ];
  for (const b of report.brandPlans) {
    lines.push(`### ${b.brandName} (\`${b.brandSlug}\`)`);
    lines.push(`Patches: ${b.patchCount}${b.blocked ? " — BLOCKED" : ""}`);
    for (const p of (b.patches || []).slice(0, 12)) {
      lines.push(`- \`${p.slotKey}\` ${p.recordId}: ${(p.before?.title || "").slice(0, 60)} → ${(p.after?.title || "").slice(0, 60)}`);
    }
    lines.push("");
    const bPath = path.join(
      REPORTS_DIR,
      `brand-explorer-wave14-public-active-semantic-blocker-cleanup-${b.brandSlug}.md`
    );
    fs.writeFileSync(
      bPath,
      [
        `# ${b.brandName} — Public Active Semantic Blocker Cleanup`,
        "",
        `Patches: ${b.patchCount}`,
        "",
        "```json",
        JSON.stringify(b.patches || [], null, 2),
        "```",
        "",
      ].join("\n"),
      "utf8"
    );
  }

  const md = lines.join("\n");
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");
  return { jsonPath, mdPath, docsPath };
}
