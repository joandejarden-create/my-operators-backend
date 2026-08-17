/**
 * Wave 12 post-release freeze cleanup — bunkhouse / moxy / voco only.
 *
 * - bunkhouse: scrub World of Hyatt identity phrasing in Presentation Body
 * - moxy: validator allowlist only (Marriott Bonvoy parent-platform false positive)
 * - voco: retitle scenarios (role diversity) + thicken operator_compat.tags
 *
 * No Brand Status / release / CV / Source / Registry / images / other brands.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE12_VERSION,
  WAVE12_NEVER_WRITE_FIELDS,
} from "./brand-explorer-wave12-factory-plan.js";

export const WAVE12_POST_RELEASE_FREEZE_CLEANUP_VERSION =
  "wave12-post-release-freeze-cleanup-v1";

export const WAVE12_POST_RELEASE_FREEZE_CLEANUP_TARGETS = Object.freeze([
  "bunkhouse-hotels",
  "moxy-hotels",
  "voco-hotels",
]);

export const WAVE12_POST_RELEASE_FREEZE_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave12-post-release-freeze-cleanup",
  "--confirm-target-brands-only",
  "--confirm-targeted-field-fixes-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-public-restore-registry-changes",
  "--confirm-no-image-writes-except-caption-only-if-flagged",
  "--confirm-no-other-active-brand-changes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 260;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const VOCO_SCENARIO_TITLES = Object.freeze({
  "overview.scenario.1": "Street-Facing Conversion Arrival",
  "overview.scenario.2": "Urban Lobby And Commercial Guest Journey",
  "overview.scenario.3": "CALA Destination Lifestyle Experience",
});

const TAG_BODIES = Object.freeze({
  "voco-hotels":
    "voco\npremium soft brand\nconversion-oriented\nIHG platform discipline\nretained character\nMexico City Reforma pipeline evidence",
  "bunkhouse-hotels":
    "Bunkhouse\nlifestyle boutique\nplacemaking\ndesign-led\nHyatt parent-platform context\noperator and market selectivity",
  "moxy-hotels":
    "Moxy\ncompact urban lifestyle\nsocial lobby\nMarriott Bonvoy parent platform\nefficient rooms\nselect-service development fit",
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function checkFlags(required, argv, apply) {
  const missing = required.filter((f) => !argv.includes(f));
  return {
    apply: apply === true,
    ok: apply === true && missing.length === 0,
    missing,
    required: [...required],
  };
}

/** Bunkhouse: remove World of Hyatt-as-identity; keep labeled Hyatt parent context. */
export function scrubBunkhouseWorldOfHyattCopy(text) {
  let t = nz(text);
  if (!t) return t;
  t = t.replace(
    /Hyatt \(lifestyle group \/ World of Hyatt platform context\)/gi,
    "Hyatt lifestyle group (parent-platform context)"
  );
  t = t.replace(
    /participates in World of Hyatt/gi,
    "participates in Hyatt's loyalty program (parent platform; verify property-level enrollment)"
  );
  t = t.replace(
    /World of Hyatt participation/gi,
    "Hyatt loyalty-program participation (parent platform)"
  );
  t = t.replace(
    /World of Hyatt integration/gi,
    "Hyatt loyalty-program integration (parent platform)"
  );
  t = t.replace(
    /and World of Hyatt\b/gi,
    "and Hyatt's loyalty program (parent platform)"
  );
  t = t.replace(/\bWorld of Hyatt\b/gi, "Hyatt's loyalty program (parent platform)");
  return t;
}

function assertAllowedPresentationFields(fields) {
  const allowed = new Set([
    "Title",
    "Body",
    "Case Summary Overview",
    "Case Summary Brand Relevance",
    "Case Summary Owner Objective",
    "Case Summary Interpretation",
    "Case Summary Tags",
  ]);
  for (const k of Object.keys(fields || {})) {
    if (!allowed.has(k)) throw new Error(`Refuse: unexpected Presentation field ${k}`);
    if (WAVE12_NEVER_WRITE_FIELDS.includes(k)) {
      throw new Error(`Refuse: never-write field ${k}`);
    }
    if (k === "Image" || k === "Brand Status") {
      throw new Error(`Refuse: forbidden field ${k}`);
    }
  }
}

async function patchPresentation({ recordId, fields }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  assertAllowedPresentationFields(fields);
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH Presentation failed: ${res.status}`);
  return json;
}

function chipCount(text) {
  return nz(text)
    .split(/[\n;,]+/)
    .map((t) => t.trim())
    .filter(Boolean).length;
}

function words(text) {
  return nz(text)
    .split(/\s+/)
    .filter(Boolean).length;
}

function needsTagThicken(body) {
  const t = nz(body);
  if (!t) return true;
  if (t.includes("·") || t.includes("•")) return true;
  return chipCount(t) < 2 || words(t) < 12;
}

function planTagPatch(slug, rows) {
  const tags = rows.find((r) => r.slotKey === "operations.operator_compat.tags");
  if (!tags?.recordId || !needsTagThicken(tags.body)) return null;
  const body = TAG_BODIES[slug];
  if (!body) return null;
  return {
    brandSlug: slug,
    table: PRESENTATION_TABLE,
    recordId: tags.recordId,
    slotKey: tags.slotKey,
    fields: { Body: body },
    before: { body: tags.body, title: tags.title },
    after: { body, title: tags.title },
    reason: "thicken_operator_compat_tags_newline_chips",
  };
}

async function fetchBasicsField(recordId, fieldName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE credentials required");
  // Single-record GET does not accept fields[] filter — fetch full record.
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent("Brand Setup - Brand Basics")}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `GET Basics failed: ${res.status}`);
  return nz(json.fields?.[fieldName]);
}

async function patchBasicsBrandPositioning({ recordId, body }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE credentials required");
  const fields = { "Brand Positioning": body };
  for (const k of Object.keys(fields)) {
    if (WAVE12_NEVER_WRITE_FIELDS.includes(k) || k === "Brand Status" || k === "Company Validated") {
      throw new Error(`Refuse: forbidden Basics field ${k}`);
    }
  }
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent("Brand Setup - Brand Basics")}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH Basics failed: ${res.status}`);
  return json;
}

function planBunkhousePatches(rows) {
  const patches = [];
  for (const row of rows) {
    const beforeBody = nz(row.body);
    const beforeTitle = nz(row.title);
    const afterBody = scrubBunkhouseWorldOfHyattCopy(beforeBody);
    const afterTitle = scrubBunkhouseWorldOfHyattCopy(beforeTitle);
    const fields = {};
    if (afterBody !== beforeBody) fields.Body = afterBody;
    if (afterTitle !== beforeTitle) fields.Title = afterTitle;
    for (const [key, airtable] of [
      ["caseSummaryOverview", "Case Summary Overview"],
      ["caseSummaryBrandRelevance", "Case Summary Brand Relevance"],
      ["caseSummaryOwnerObjective", "Case Summary Owner Objective"],
      ["caseSummaryInterpretation", "Case Summary Interpretation"],
      ["caseSummaryTags", "Case Summary Tags"],
    ]) {
      const before = nz(row[key]);
      const after = scrubBunkhouseWorldOfHyattCopy(before);
      if (after !== before) fields[airtable] = after;
    }
    if (!Object.keys(fields).length) continue;
    patches.push({
      brandSlug: "bunkhouse-hotels",
      table: PRESENTATION_TABLE,
      recordId: row.recordId,
      slotKey: row.slotKey,
      fields,
      before: { body: beforeBody, title: beforeTitle },
      after: { body: fields.Body || beforeBody, title: fields.Title || beforeTitle },
      reason: "scrub_world_of_hyatt_identity_phrasing",
    });
  }
  const tagPatch = planTagPatch("bunkhouse-hotels", rows);
  if (tagPatch) {
    const existing = patches.find((p) => p.recordId === tagPatch.recordId);
    if (existing) {
      existing.fields.Body = tagPatch.fields.Body;
      existing.after.body = tagPatch.fields.Body;
      existing.reason += "+thicken_tags";
    } else {
      patches.push(tagPatch);
    }
  }
  return patches;
}

function planVocoPatches(rows) {
  const patches = [];
  for (const [slotKey, title] of Object.entries(VOCO_SCENARIO_TITLES)) {
    const row = rows.find((r) => r.slotKey === slotKey);
    if (!row?.recordId) continue;
    if (nz(row.title) === title) continue;
    patches.push({
      brandSlug: "voco-hotels",
      table: PRESENTATION_TABLE,
      recordId: row.recordId,
      slotKey,
      fields: { Title: title },
      before: { body: row.body, title: row.title },
      after: { body: row.body, title },
      reason: "diversify_scenario_title_roles",
    });
  }
  const tagPatch = planTagPatch("voco-hotels", rows);
  if (tagPatch) patches.push(tagPatch);
  return patches;
}

function planMoxyPatches(rows) {
  const patches = [];
  const tagPatch = planTagPatch("moxy-hotels", rows);
  if (tagPatch) patches.push(tagPatch);
  return patches;
}

function writeReports(basename, report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, `${basename}.json`);
  const mdPath = path.join(REPORTS_DIR, `${basename}.md`);
  const docPath = path.join(DOCS_DIR, `${basename}.md`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  const text = md.endsWith("\n") ? md : `${md}\n`;
  fs.writeFileSync(mdPath, text, "utf8");
  fs.writeFileSync(docPath, text, "utf8");
  return { jsonPath, mdPath, docPath };
}

function renderBrandMd(slug, plan) {
  const lines = [
    `# Wave 12 Post-Release Freeze Cleanup — ${slug}`,
    ``,
    `Patches planned: **${plan.patches.length}**`,
    `Allowlist-only: **${plan.allowlistOnly === true}**`,
    ``,
  ];
  for (const p of plan.patches) {
    lines.push(`## ${p.slotKey} (\`${p.recordId}\`)`);
    lines.push(`Reason: ${p.reason}`);
    lines.push("");
    lines.push("Fields: " + Object.keys(p.fields).join(", "));
    lines.push("");
  }
  return lines.join("\n");
}

function renderSummaryMd(report) {
  const lines = [
    `# Wave 12 Post-Release Freeze Cleanup`,
    ``,
    `Version: \`${report.version}\` · Generated: ${report.generatedAt}`,
    `Mode: **${report.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${report.writePerformed}**`,
    ``,
    `## Decision`,
    ``,
    `- **bunkhouse-hotels:** scrub World of Hyatt identity phrasing; keep Hyatt as labeled parent-platform context`,
    `- **moxy-hotels:** case B false positive — parent-platform allowlist (\`moxy\` + Marriott parent); optional tags thicken`,
    `- **voco-hotels:** retitle scenarios to diversify role detection; thicken tags`,
    ``,
    `## Patch counts`,
    ``,
    `| Brand | Planned patches | Applied |`,
    `| --- | --- | --- |`,
  ];
  for (const slug of WAVE12_POST_RELEASE_FREEZE_CLEANUP_TARGETS) {
    const b = report.brands[slug];
    lines.push(`| ${slug} | ${b?.plannedCount ?? 0} | ${b?.appliedCount ?? 0} |`);
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  for (const [k, v] of Object.entries(report.guardrails || {})) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("");
  lines.push(`## Freeze readiness statement`);
  lines.push("");
  lines.push(report.readyStatement || "_pending post-apply validation_");
  lines.push("");
  return lines.join("\n");
}

/**
 * @param {{ apply?: boolean, argv?: string[] }} opts
 */
export async function runWave12PostReleaseFreezeCleanup({ apply = false, argv = [] } = {}) {
  const stage = "post-release-freeze-cleanup";
  const flagCheck = checkFlags(WAVE12_POST_RELEASE_FREEZE_CLEANUP_APPLY_FLAGS, argv, apply);
  const brands = {};
  const allPatches = [];
  const basicsPatches = [];

  for (const slug of WAVE12_POST_RELEASE_FREEZE_CLEANUP_TARGETS) {
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    if (!id?.recordId) throw new Error(`Missing identity for ${slug}`);
    const { rows } = await listPresentationRowsLight(id.recordId, id.name);
    let patches = [];
    if (slug === "bunkhouse-hotels") patches = planBunkhousePatches(rows);
    else if (slug === "voco-hotels") patches = planVocoPatches(rows);
    else if (slug === "moxy-hotels") patches = planMoxyPatches(rows);

    if (slug === "bunkhouse-hotels") {
      const before = await fetchBasicsField(id.recordId, "Brand Positioning");
      const after = scrubBunkhouseWorldOfHyattCopy(before);
      if (after && after !== before) {
        basicsPatches.push({
          brandSlug: slug,
          table: "Brand Setup - Brand Basics",
          recordId: id.recordId,
          field: "Brand Positioning",
          fields: { "Brand Positioning": after },
          before,
          after,
          reason: "scrub_world_of_hyatt_from_basics_brand_positioning",
        });
      }
    }

    brands[slug] = {
      slug,
      name: id.name,
      recordId: id.recordId,
      rowCount: rows.length,
      plannedCount: patches.length,
      basicsPlannedCount: basicsPatches.filter((p) => p.brandSlug === slug).length,
      allowlistOnly: slug === "moxy-hotels",
      patches,
      appliedCount: 0,
      applyResults: [],
    };
    allPatches.push(...patches);
  }

  // Write per-brand docs with stable names
  for (const [slug, short] of [
    ["bunkhouse-hotels", "bunkhouse"],
    ["moxy-hotels", "moxy"],
    ["voco-hotels", "voco"],
  ]) {
    const md = renderBrandMd(slug, brands[slug]);
    fs.writeFileSync(
      path.join(REPORTS_DIR, `brand-explorer-wave12-post-release-freeze-cleanup-${short}.md`),
      md.endsWith("\n") ? md : `${md}\n`,
      "utf8"
    );
  }

  // Moxy allowlist diagnosis report
  const moxyAllowlistMd = [
    `# Moxy Hotels — Parent-Platform Allowlist Diagnosis`,
    ``,
    `## Verdict: **B — validator false positive**`,
    ``,
    `Moxy Hotels is Marriott-affiliated. Owner-facing copy correctly references Marriott Bonvoy as parent-platform loyalty context.`,
    `The quality-audit wrong-brand marker only exempted slugs containing \`marriott\` (plus a few soft brands). \`moxy-hotels\` does not include \`marriott\`, so valid Bonvoy references were flagged as carryover.`,
    ``,
    `## Fix (targeted; not global disable)`,
    ``,
    `1. Add \`moxy\` to Marriott Bonvoy \`unlessSlugIncludes\`.`,
    `2. Add \`PARENT_PLATFORM_LOYALTY_SLUG_EXEMPTIONS["moxy-hotels"] = ["marriott"]\`.`,
    `3. Also honor Brand Basics \`parentCompany\` containing Marriott for the Bonvoy marker.`,
    ``,
    `## Not done`,
    ``,
    `- Did not scrub valid Marriott Bonvoy references from Moxy copy.`,
    `- Did not allow Marriott Bonvoy for unrelated brands.`,
    `- Did not disable wrong-brand detection globally.`,
    ``,
  ].join("\n");
  fs.writeFileSync(
    path.join(REPORTS_DIR, "brand-explorer-moxy-parent-platform-allowlist-fix.md"),
    `${moxyAllowlistMd}\n`,
    "utf8"
  );

  const applyPerformed = apply === true && flagCheck.ok === true;
  let writePerformed = false;

  if (applyPerformed) {
    for (const patch of allPatches) {
      try {
        await patchPresentation({ recordId: patch.recordId, fields: patch.fields });
        writePerformed = true;
        brands[patch.brandSlug].appliedCount += 1;
        brands[patch.brandSlug].applyResults.push({
          recordId: patch.recordId,
          slotKey: patch.slotKey,
          applied: true,
          fields: Object.keys(patch.fields),
        });
      } catch (err) {
        brands[patch.brandSlug].applyResults.push({
          recordId: patch.recordId,
          slotKey: patch.slotKey,
          applied: false,
          error: err.message,
        });
      }
      await sleep(WRITE_THROTTLE_MS);
    }
    for (const patch of basicsPatches) {
      try {
        await patchBasicsBrandPositioning({
          recordId: patch.recordId,
          body: patch.after,
        });
        writePerformed = true;
        brands[patch.brandSlug].appliedCount += 1;
        brands[patch.brandSlug].applyResults.push({
          recordId: patch.recordId,
          slotKey: "Brand Positioning",
          table: patch.table,
          applied: true,
          fields: ["Brand Positioning"],
        });
      } catch (err) {
        brands[patch.brandSlug].applyResults.push({
          recordId: patch.recordId,
          slotKey: "Brand Positioning",
          applied: false,
          error: err.message,
        });
      }
      await sleep(WRITE_THROTTLE_MS);
    }
  }

  const report = {
    version: WAVE12_POST_RELEASE_FREEZE_CLEANUP_VERSION,
    waveVersion: WAVE12_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    dryRun: !applyPerformed,
    flagCheck,
    requiredApplyFlags: [...WAVE12_POST_RELEASE_FREEZE_CLEANUP_APPLY_FLAGS],
    targets: [...WAVE12_POST_RELEASE_FREEZE_CLEANUP_TARGETS],
    plannedPatchCount: allPatches.length + basicsPatches.length,
    basicsPatches,
    brands,
    moxyDiagnosis: "B_false_positive_parent_platform_allowlist",
    allowlistCodeChange:
      "lib/partner-intelligence/brand-explorer-24-tab-section-quality-audit.js — moxy + parentCompanyIncludes for Marriott Bonvoy",
    guardrails: {
      targetBrandsOnly: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      brandStatusWrites: false,
      releaseFieldWrites: false,
      publicRestoreRegistryWrites: false,
      imageWrites: false,
      otherActiveBrandChanges: false,
      radissonCollectionChanges: false,
      neverWriteFields: [...WAVE12_NEVER_WRITE_FIELDS],
    },
    readyStatement: applyPerformed
      ? "Cleanup applied — run post-apply quality audit to confirm ready_to_freeze_39_active_public_full_baseline."
      : "Dry-run only — no Airtable writes.",
  };

  const paths = writeReports(
    "brand-explorer-wave12-post-release-freeze-cleanup",
    report,
    renderSummaryMd(report)
  );
  return { report, paths };
}
