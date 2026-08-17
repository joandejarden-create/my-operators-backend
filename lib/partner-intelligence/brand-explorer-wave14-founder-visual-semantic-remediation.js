/**
 * Wave 14 — Founder visual semantic QA remediation.
 *
 * Scope: footprint.momentum, footprint.portfolio_mix, footprint.openings
 *        for eight active Wave 14 brands.
 *
 * Does NOT touch: Brand Status, release fields, CV, Source Library,
 * Registry, Value Scenarios, images (except hiding wrong-property cards),
 * Four Points Flex, or protected non-Wave-14 brands.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import { generateWave14PresentationPack } from "./brand-explorer-wave14-tab-factory-build.js";
import {
  WAVE14_VERSION,
  WAVE14_PARTIAL_PROMOTION_SLUGS,
  WAVE14_HELD_PROMOTION_SLUG,
  WAVE14_NEVER_WRITE_FIELDS,
  WAVE14_FOUNDER_VISUAL_SEMANTIC_REMEDIATION_APPLY_FLAGS,
} from "./brand-explorer-wave14-factory-plan.js";
import { extractWave14FounderVisualSemanticFailures } from "./brand-explorer-wave14-founder-visual-semantic-failures.js";

export const WAVE14_FOUNDER_VISUAL_SEMANTIC_REMEDIATION_VERSION =
  "wave14-founder-visual-semantic-remediation-v1";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 280;
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const TARGETED_SLOTS = new Set([
  "footprint.momentum",
  "footprint.momentum_label",
  "footprint.portfolio_mix",
  "footprint.openings",
]);

const FORBIDDEN_VISIBLE_RES = Object.freeze([
  /\bvisual diligence\b/i,
  /\bsteward-matched\b/i,
  /\bunderwriting lane\b/i,
  /\bbrand-lane evidence\b/i,
  /\bsource-supported\b/i,
  /\bsource pack\b/i,
  /\bfactory\b/i,
  /\bStage\s*\d/i,
  /\bgovernance\b/i,
  /\bUse this labeled example\b/i,
  /\bconfirm the asset under review\b/i,
  /\bEnsure the asset under review\b/i,
  /\bDirectory card\b/i,
  /\bQA\b/,
  /\bsteward to match\b/i,
  /\bpending steward\b/i,
  /\bmarket archetype pending\b/i,
  /Development Page Frames\b/i,
  /\bBrand Site Confirms\s+(Flagship|Full-Service|Premium|All-Suite|Lifestyle|Midscale|Longer)/i,
  /\bBrand Presence Confirms\s+(Premium|Lifestyle|Full-Service)/i,
  /\bBrand Page Confirms\s+(Midscale|Full-Service|Premium)/i,
  /\bGuest Brand Site Confirms\b/i,
  /Positioning Remains Full-Service/i,
  /On Marriott Longer Stays family positioning/i,
  /Development Positioning Remains/i,
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

function planBrandRemediation(slug) {
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!identity?.recordId) return { brandSlug: slug, blocked: true, blockers: ["unknown_identity"] };

  const newPack = generateWave14PresentationPack(slug);
  const newRows = (newPack.presentation || []).filter((r) => TARGETED_SLOTS.has(r.slotKey));

  const blockers = [];
  for (const r of newRows) {
    const text = `${r.title || ""} ${r.body || ""}`;
    for (const re of FORBIDDEN_VISIBLE_RES) {
      if (re.test(text)) blockers.push(`${slug}:forbidden:${re.source}:${r.slotKey}`);
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

export async function runWave14FounderVisualSemanticRemediation({
  dryRun = true,
  argv = [],
  brands = null,
} = {}) {
  const apply = argv.includes("--apply") && dryRun === false;
  const flagCheck = checkFlags(WAVE14_FOUNDER_VISUAL_SEMANTIC_REMEDIATION_APPLY_FLAGS, argv, apply);

  if (apply && !flagCheck.ok) {
    return {
      version: WAVE14_FOUNDER_VISUAL_SEMANTIC_REMEDIATION_VERSION,
      generatedAt: new Date().toISOString(),
      applyPerformed: false,
      pass: false,
      stopRecommended: true,
      readyStatement: "wave14_founder_visual_semantic_remediation_blocked_missing_flags",
      missingFlags: flagCheck.missing,
    };
  }

  const failures = await extractWave14FounderVisualSemanticFailures({
    brands: brands || WAVE14_PARTIAL_PROMOTION_SLUGS,
  });

  const targetSlugs = (brands || WAVE14_PARTIAL_PROMOTION_SLUGS).filter(
    (s) => s !== WAVE14_HELD_PROMOTION_SLUG
  );

  const brandPlans = [];
  for (const slug of targetSlugs) {
    brandPlans.push(planBrandRemediation(slug));
  }

  const anyBlocked = brandPlans.some((b) => b.blocked);

  // For each brand, fetch live rows + match new rows to existing records
  const patchPlans = [];
  for (const plan of brandPlans) {
    if (plan.blocked) continue;
    const live = await listPresentationRowsLight(plan.recordId, plan.brandName);
    const liveRows = (live.rows || []).filter((r) => TARGETED_SLOTS.has(r.slotKey));

    const patches = [];

    // Group live rows by slotKey, sorted by sortOrder
    const liveBySlot = new Map();
    for (const r of liveRows) {
      const key = r.slotKey;
      if (!liveBySlot.has(key)) liveBySlot.set(key, []);
      liveBySlot.get(key).push(r);
    }
    for (const [, arr] of liveBySlot) {
      arr.sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
    }

    // Group new rows by slotKey
    const newBySlot = new Map();
    for (const r of plan.newRows) {
      const key = r.slotKey;
      if (!newBySlot.has(key)) newBySlot.set(key, []);
      newBySlot.get(key).push(r);
    }

    // Match by position within each slotKey group
    for (const [slotKey, newSlotRows] of newBySlot) {
      const liveSlotRows = liveBySlot.get(slotKey) || [];
      for (let i = 0; i < newSlotRows.length; i++) {
        const newRow = newSlotRows[i];
        const existing = liveSlotRows[i];
        if (!existing?.recordId) continue;

        const fields = { Title: newRow.title || "", Body: newRow.body || "" };
        for (const f of WAVE14_NEVER_WRITE_FIELDS) delete fields[f];

        const changed =
          fields.Title !== nz(existing.title) || fields.Body !== nz(existing.body);

        if (changed) {
          patches.push({
            action: "PATCH",
            table: PRESENTATION_TABLE,
            recordId: existing.recordId,
            slotKey: newRow.slotKey,
            fields,
            prior: { title: nz(existing.title).slice(0, 60), body: nz(existing.body).slice(0, 80) },
          });
        }
      }

      // Extra live rows beyond what the new pack provides — check for forbidden language
      // and patch to clean them up (re-use last new row content, or clear forbidden text)
      for (let i = newSlotRows.length; i < liveSlotRows.length; i++) {
        const existing = liveSlotRows[i];
        if (!existing?.recordId) continue;
        const text = `${nz(existing.title)} ${nz(existing.body)}`;
        const hasForbidden = FORBIDDEN_VISIBLE_RES.some((re) => re.test(text));
        const isGenericTitle = /^(Marriott Hotels|Sheraton|Westin|Residence Inn by Marriott|SpringHill Suites by Marriott|TownePlace Suites by Marriott|Aloft Hotels|StudioRes)\s*[-—]\s*(International Reference|CALA|Property Example)$/i.test(nz(existing.title));
        if (hasForbidden || isGenericTitle) {
          // Replace with last available new row content (duplicate the final card)
          const lastNew = newSlotRows[newSlotRows.length - 1];
          const fields = { Title: lastNew?.title || "", Body: lastNew?.body || "" };
          for (const f of WAVE14_NEVER_WRITE_FIELDS) delete fields[f];
          patches.push({
            action: "PATCH",
            table: PRESENTATION_TABLE,
            recordId: existing.recordId,
            slotKey,
            fields,
            prior: { title: nz(existing.title).slice(0, 60), body: nz(existing.body).slice(0, 80) },
          });
        }
      }
    }

    // Slots only in live (no new rows at all) — check for forbidden language
    for (const [slotKey, liveSlotRows] of liveBySlot) {
      if (newBySlot.has(slotKey)) continue;
      for (const existing of liveSlotRows) {
        if (!existing?.recordId) continue;
        const text = `${nz(existing.title)} ${nz(existing.body)}`;
        const hasForbidden = FORBIDDEN_VISIBLE_RES.some((re) => re.test(text));
        if (hasForbidden) {
          // Cannot replace with new content — just sanitize the body
          let cleanBody = nz(existing.body);
          for (const re of FORBIDDEN_VISIBLE_RES) cleanBody = cleanBody.replace(re, "");
          cleanBody = cleanBody.replace(/\s{2,}/g, " ").trim();
          const fields = { Title: nz(existing.title), Body: cleanBody };
          for (const f of WAVE14_NEVER_WRITE_FIELDS) delete fields[f];
          if (fields.Body !== nz(existing.body)) {
            patches.push({
              action: "PATCH",
              table: PRESENTATION_TABLE,
              recordId: existing.recordId,
              slotKey,
              fields,
              prior: { title: nz(existing.title).slice(0, 60), body: nz(existing.body).slice(0, 80) },
            });
          }
        }
      }
    }
    plan.patches = patches;
    patchPlans.push(plan);
  }

  let applied = 0;
  const applyErrors = [];

  if (apply && !anyBlocked) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

    for (const plan of patchPlans) {
      for (const patch of plan.patches) {
        try {
          await airtablePatch(baseId, apiKey, patch.table, patch.recordId, patch.fields);
          applied += 1;
          await sleep(WRITE_THROTTLE_MS);
        } catch (err) {
          applyErrors.push({
            brandSlug: plan.brandSlug,
            slotKey: patch.slotKey,
            error: err?.message || String(err),
          });
        }
      }
    }
  }

  const plannedPatches = patchPlans.reduce((n, b) => n + b.patches.length, 0);
  const pass = !anyBlocked && applyErrors.length === 0;

  const readyStatement = apply
    ? pass
      ? "wave14_founder_visual_semantics_clean_ready_for_value_scenario_recheck_and_54_freeze"
      : "wave14_founder_visual_semantic_remediation_apply_had_errors"
    : anyBlocked
      ? "wave14_founder_visual_semantic_remediation_blocked"
      : "wave14_founder_visual_semantic_remediation_dry_run_ready";

  const report = {
    version: WAVE14_FOUNDER_VISUAL_SEMANTIC_REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    applyPerformed: apply && !anyBlocked,
    applied,
    plannedPatches,
    pass,
    anyBlocked,
    blockers: brandPlans.filter((b) => b.blocked).map((b) => ({ slug: b.brandSlug, blockers: b.blockers })),
    applyErrors,
    brands: patchPlans.map((p) => ({
      slug: p.brandSlug,
      name: p.brandName,
      patchCount: p.patches.length,
      patches: p.patches.map((pa) => ({
        slotKey: pa.slotKey,
        prior: pa.prior,
        newTitle: pa.fields?.Title?.slice(0, 60),
        newBody: pa.fields?.Body?.slice(0, 80),
      })),
    })),
    failureExtraction: failures.summary,
    readyStatement,
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave14-founder-visual-semantic-remediation.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave14-founder-visual-semantic-remediation.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-wave14-founder-visual-semantic-remediation.md");

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    "# Wave 14 — Founder Visual Semantic Remediation",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Dry-run: ${dryRun}`,
    `Apply performed: ${report.applyPerformed}`,
    `Planned patches: ${plannedPatches}`,
    `Applied: ${applied}`,
    `Pass: ${pass}`,
    "",
    `Ready statement: \`${readyStatement}\``,
    "",
    "## Changes by Brand",
    "",
  ];
  for (const b of report.brands) {
    lines.push(`### ${b.name} (\`${b.slug}\`)`);
    lines.push(`Patches: ${b.patchCount}`);
    for (const p of b.patches) {
      lines.push(`- **${p.slotKey}**: "${p.prior?.title}" → "${p.newTitle}"`);
    }
    lines.push("");
  }
  if (report.blockers.length) {
    lines.push("## Blockers");
    for (const bl of report.blockers) lines.push(`- ${bl.slug}: ${bl.blockers.join(", ")}`);
    lines.push("");
  }
  const md = lines.join("\n") + "\n";
  fs.writeFileSync(mdPath, md, "utf8");
  fs.writeFileSync(docsPath, md, "utf8");

  // Per-brand reports
  for (const b of report.brands) {
    const bPath = path.join(REPORTS_DIR, `brand-explorer-wave14-founder-visual-semantic-remediation-${b.slug}.md`);
    const bLines = [
      `# ${b.name} — Founder Visual Semantic Remediation`,
      "",
      `Patches: ${b.patchCount}`,
      "",
    ];
    for (const p of b.patches) {
      bLines.push(`## ${p.slotKey}`);
      bLines.push(`Prior: "${p.prior?.title}" / "${p.prior?.body}"`);
      bLines.push(`New:   "${p.newTitle}" / "${p.newBody}"`);
      bLines.push("");
    }
    fs.writeFileSync(bPath, bLines.join("\n") + "\n", "utf8");
  }

  return report;
}
