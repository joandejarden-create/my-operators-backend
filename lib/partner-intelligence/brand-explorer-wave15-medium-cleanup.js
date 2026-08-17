/**
 * Wave 15 post-release Medium cleanup + Spark featured_application blocker.
 *
 * Scope (only):
 * - home2-suites-by-hilton: footprint.region.cala, overview.scenario.3
 * - homewood-suites-by-hilton: footprint.region.cala, overview.scenario.2–3, valueOwners.scenario.4
 * - spark-by-hilton: overview.scenario.3 title, overview.featured_application
 *
 * Forbidden: VIC, sandbox, Brand Status, release fields, CV, Source Library,
 * Registry, images, Four Points Flex, protected-54 identity changes beyond
 * presentation Title/Body on listed slots.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { toProperCaseScenarioTitle } from "./brand-explorer-scenario-owner-value-bar.js";
import { WAVE15_SLUGS } from "./brand-explorer-wave15-factory-plan.js";

export const WAVE15_MEDIUM_CLEANUP_VERSION = "wave15-medium-cleanup-v1";

export const WAVE15_MEDIUM_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave15-medium-cleanup",
  "--confirm-medium-findings-and-spark-featured-only",
  "--confirm-targeted-presentation-title-body-only",
  "--confirm-no-vic-writes",
  "--confirm-no-sandbox-patch",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-image-writes",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-protected-54-identity-changes",
  "--confirm-target-brands-home2-homewood-spark-only",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 320;
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const ALLOWED_SLUGS = new Set([
  "home2-suites-by-hilton",
  "homewood-suites-by-hilton",
  "spark-by-hilton",
]);

/**
 * Explicit steward patches for the 7 Medium findings + Spark featured_application.
 * Record IDs verified live 2026-08-05.
 */
export const WAVE15_MEDIUM_CLEANUP_PATCHES = Object.freeze([
  {
    brand: "Home2 Suites by Hilton",
    brandSlug: "home2-suites-by-hilton",
    brandRecordId: "reccZ4zV6wMav7a2i",
    presentationRecordId: "recba6qW8X4rw6HbS",
    slotKey: "footprint.region.cala",
    failureType: "cala_label_without_support",
    section: "Geographic Footprint",
    patch: {
      Title: "Caribbean & Latin America",
      Body:
        "CALA inventory for Home2 Suites is not yet confirmed in official Hilton brand materials. Keep CALA cleanly unavailable and use International Reference Home2 hotels for brand-fit diligence until a named CALA property is published on an official Hilton URL.",
    },
  },
  {
    brand: "Home2 Suites by Hilton",
    brandSlug: "home2-suites-by-hilton",
    brandRecordId: "reccZ4zV6wMav7a2i",
    presentationRecordId: "recPdctMJaPMegB3D",
    slotKey: "overview.scenario.3",
    failureType: "weak_owner_value_cues_overview.scenario.3",
    section: "Where This Brand Creates the Most Value",
    patch: {
      Title: "Efficient Dual-Brand Development Pattern",
      Body:
        "Dual-brand development sites—typically Home2 paired with Tru or Hampton—create owner value when shared land, construction, and operating efficiencies lift returns while keeping each brand's guest promise intact. Underwrite the pairing honestly so extended-stay guests still get the Home2 suite experience. Owner value is weaker when the pairing forces compromises Home2 or its sibling brand cannot sustain.",
    },
  },
  {
    brand: "Homewood Suites by Hilton",
    brandSlug: "homewood-suites-by-hilton",
    brandRecordId: "recZjYI4nYflGHFNR",
    presentationRecordId: "reclPov0hJCCSkXiR",
    slotKey: "footprint.region.cala",
    failureType: "cala_label_without_support",
    section: "Geographic Footprint",
    patch: {
      Title: "Caribbean & Latin America",
      Body:
        "CALA inventory for Homewood Suites is not yet confirmed in official Hilton brand materials. Keep CALA cleanly unavailable and use International Reference Homewood hotels for brand-fit diligence until a named CALA property is published on an official Hilton URL.",
    },
  },
  {
    brand: "Homewood Suites by Hilton",
    brandSlug: "homewood-suites-by-hilton",
    brandRecordId: "recZjYI4nYflGHFNR",
    presentationRecordId: "recD0AEtueMsBylLJ",
    slotKey: "overview.scenario.2",
    failureType: "weak_owner_value_cues_overview.scenario.2",
    section: "Where This Brand Creates the Most Value",
    patch: {
      Title: "Kitchen And Breakfast Length-Of-Stay Economics",
      Body:
        "Suite-and-kitchen product plus hot breakfast and evening social create owner value when length-of-stay economics—not short-stay select-service rate math—drive the underwrite. Owners should capitalize residential suite mix, F&B rhythms, and housekeeping cadence for multi-night guests. Affiliation helps when the asset can deliver a residential stay guests will book for weeks, not a transient all-suite night.",
    },
  },
  {
    brand: "Homewood Suites by Hilton",
    brandSlug: "homewood-suites-by-hilton",
    brandRecordId: "recZjYI4nYflGHFNR",
    presentationRecordId: "recno7dZ51VjDy7uK",
    slotKey: "overview.scenario.3",
    failureType: "weak_owner_value_cues_overview.scenario.3",
    section: "Where This Brand Creates the Most Value",
    patch: {
      Title: "Upscale Extended-Stay Coverage Depth",
      Body:
        "Suburban and urban extended-stay assets fit Homewood when Hilton distribution supports recurring corporate, medical, and relocation demand at upscale suite quality. Underwrite staffing, kitchen product, and social programming for residential stays rather than SpringHill short-stay or Hilton Garden Inn focused-service logic. Owner value is weaker when the thesis drifts into Home2 midscale kitchens or Hampton focused-service breakfast prototypes.",
    },
  },
  {
    brand: "Homewood Suites by Hilton",
    brandSlug: "homewood-suites-by-hilton",
    brandRecordId: "recZjYI4nYflGHFNR",
    presentationRecordId: "recyClZQAawcxTbX1",
    slotKey: "valueOwners.scenario.4",
    failureType: "sentence_case_title_valueOwners.scenario.4",
    section: "Value Creation Scenarios",
    patch: {
      Title: toProperCaseScenarioTitle("Honors Reach For Longer Stays"),
    },
  },
  {
    brand: "Spark by Hilton",
    brandSlug: "spark-by-hilton",
    brandRecordId: "recfv66er4Ch2vJDO",
    presentationRecordId: "recJ4BSgBfyPNg8at",
    slotKey: "overview.scenario.3",
    failureType: "sentence_case_title_overview.scenario.3",
    section: "Where This Brand Creates the Most Value",
    patch: {
      Title: toProperCaseScenarioTitle("Efficient Reflag For Aging Independents"),
    },
  },
  {
    brand: "Spark by Hilton",
    brandSlug: "spark-by-hilton",
    brandRecordId: "recfv66er4Ch2vJDO",
    presentationRecordId: "recZE1DeNVJlZPyhn",
    slotKey: "overview.featured_application",
    failureType: "placeholder_featured_application",
    section: "Owner / property fit",
    patch: {
      Title: "Conversion-only premium economy reflag",
      Body:
        "Spark by Hilton fits aging independent or under-branded hotels when a light conversion PIP, Hilton Honors distribution, and premium-economy product can lift demand without Tru new-build cost or Hampton focused-service capital. Owner value holds when sponsors underwrite Spark honestly as conversion-only value-tier affiliation—not as a Hampton substitute or design-diluted Tru. Confirm the asset's bones, PIP envelope, and competitive set before treating Spark as the affiliation path.",
    },
  },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function parseWave15MediumCleanupFlags(argv = []) {
  const apply = argv.includes("--apply");
  const missing = WAVE15_MEDIUM_CLEANUP_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply,
    ok: apply === true && missing.length === 0,
    missing,
    required: [...WAVE15_MEDIUM_CLEANUP_APPLY_FLAGS],
  };
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
  };
}

export async function planWave15MediumCleanupPatches() {
  const planned = [];
  for (const d of WAVE15_MEDIUM_CLEANUP_PATCHES) {
    if (!ALLOWED_SLUGS.has(d.brandSlug)) {
      planned.push({ ...d, skipped: "slug_not_allowed" });
      continue;
    }
    if (!WAVE15_SLUGS.includes(d.brandSlug)) {
      planned.push({ ...d, skipped: "not_wave15_slug" });
      continue;
    }
    const row = await fetchPresentationRowById(d.presentationRecordId);
    if (!row) {
      planned.push({
        brandSlug: d.brandSlug,
        recordId: d.presentationRecordId,
        slotKey: d.slotKey,
        skipped: "presentation_row_not_found",
      });
      continue;
    }
    if (row.slotKey && row.slotKey !== d.slotKey) {
      planned.push({
        brandSlug: d.brandSlug,
        recordId: d.presentationRecordId,
        slotKey: d.slotKey,
        liveSlotKey: row.slotKey,
        skipped: "slot_key_mismatch",
      });
      continue;
    }
    const fields = {};
    if (d.patch.Title != null && nz(d.patch.Title) !== nz(row.title)) fields.Title = d.patch.Title;
    if (d.patch.Body != null && nz(d.patch.Body) !== nz(row.body)) fields.Body = d.patch.Body;
    if (!Object.keys(fields).length) {
      planned.push({
        brandSlug: d.brandSlug,
        recordId: d.presentationRecordId,
        slotKey: d.slotKey,
        failureType: d.failureType,
        skipped: "already_matches_target",
        before: { title: row.title, body: row.body.slice(0, 160) },
      });
      continue;
    }
    planned.push({
      action: "PATCH",
      table: PRESENTATION_TABLE,
      brand: d.brand,
      brandSlug: d.brandSlug,
      brandRecordId: d.brandRecordId,
      recordId: d.presentationRecordId,
      slotKey: d.slotKey,
      failureType: d.failureType,
      section: d.section,
      fieldMapping: Object.fromEntries(
        Object.keys(fields).map((k) => [k === "Title" ? "title" : "body", k])
      ),
      before: { title: row.title, body: row.body.slice(0, 220) },
      sanitizedPayloadPreview: fields,
      needsWrite: true,
    });
  }
  return planned;
}

export async function runWave15MediumCleanup({ dryRun = true, argv = [] } = {}) {
  const flagCheck = parseWave15MediumCleanupFlags(argv);
  const planned = await planWave15MediumCleanupPatches();
  const toWrite = planned.filter((p) => p.needsWrite === true);
  const skipped = planned.filter((p) => p.skipped);

  const report = {
    version: WAVE15_MEDIUM_CLEANUP_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: dryRun === true,
    apply: flagCheck.apply,
    writePerformed: false,
    airtableWrites: 0,
    vicTouched: false,
    sandboxPatch: false,
    brandStatusChanges: 0,
    releaseFieldChanges: 0,
    imageWrites: 0,
    flagCheck,
    allowedSlugs: [...ALLOWED_SLUGS],
    plannedCount: planned.length,
    patchCount: toWrite.length,
    skippedCount: skipped.length,
    planned,
    applyResults: [],
    readyStatement: dryRun
      ? "wave15_medium_cleanup_dry_run_ready_for_apply"
      : "wave15_medium_cleanup_blocked_missing_flags",
  };

  if (dryRun || !flagCheck.apply) {
    writeWave15MediumCleanupReports(report);
    return report;
  }

  if (!flagCheck.ok) {
    report.readyStatement = "wave15_medium_cleanup_blocked_missing_flags";
    report.flagCheck = flagCheck;
    writeWave15MediumCleanupReports(report);
    return report;
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  for (const p of toWrite) {
    try {
      await airtablePatch(baseId, apiKey, PRESENTATION_TABLE, p.recordId, p.sanitizedPayloadPreview);
      report.applyResults.push({
        brandSlug: p.brandSlug,
        recordId: p.recordId,
        slotKey: p.slotKey,
        applied: true,
        fieldsPatched: Object.keys(p.sanitizedPayloadPreview),
      });
      report.airtableWrites += 1;
      await sleep(WRITE_THROTTLE_MS);
    } catch (err) {
      report.applyResults.push({
        brandSlug: p.brandSlug,
        recordId: p.recordId,
        slotKey: p.slotKey,
        applied: false,
        error: err.message,
      });
    }
  }

  report.writePerformed = report.airtableWrites > 0;
  report.readyStatement =
    report.applyResults.every((r) => r.applied) && report.applyResults.length === toWrite.length
      ? "wave15_medium_cleanup_applied_ready_for_62_validation"
      : "wave15_medium_cleanup_apply_incomplete";
  writeWave15MediumCleanupReports(report);
  return report;
}

export function writeWave15MediumCleanupReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave15-medium-cleanup.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave15-medium-cleanup.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-wave15-medium-cleanup.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    "# Brand Explorer — Wave 15 Medium Cleanup",
    "",
    `Version: \`${report.version}\``,
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.dryRun ? "dry-run" : "apply"}** · writes=${report.airtableWrites}`,
    `Ready: \`${report.readyStatement}\``,
    "",
    "## Scope",
    "",
    "- 7 Medium semantic findings (Home2 / Homewood / Spark)",
    "- Spark `overview.featured_application` placeholder blocker",
    "- Presentation Title/Body only",
    "- No VIC / sandbox / Brand Status / release / image writes",
    "",
    "## Patches",
    "",
    "| Brand | Slug | Slot | Failure | Record ID | Action |",
    "|-------|------|------|---------|-----------|--------|",
  ];
  for (const p of report.planned || []) {
    const action = p.skipped || (p.needsWrite ? "PATCH" : "—");
    lines.push(
      `| ${p.brand || "—"} | \`${p.brandSlug}\` | \`${p.slotKey}\` | \`${p.failureType || "—"}\` | \`${p.recordId}\` | ${action} |`
    );
  }
  lines.push("");
  if (report.applyResults?.length) {
    lines.push("## Apply results");
    lines.push("");
    for (const r of report.applyResults) {
      lines.push(
        `- \`${r.brandSlug}\` \`${r.slotKey}\` → ${r.applied ? "applied" : `FAILED: ${r.error}`}`
      );
    }
    lines.push("");
  }
  const body = `${lines.join("\n")}\n`;
  fs.writeFileSync(mdPath, body, "utf8");
  fs.writeFileSync(docsPath, body, "utf8");
  return { jsonPath, mdPath, docsPath };
}
