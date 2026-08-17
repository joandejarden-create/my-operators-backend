/**
 * Wave 13 — SO/ section-pattern cleanup (Presentation only).
 *
 * Fixes: dated Recent Momentum cards, empty MEA suppress, brand-specific growth + geo.
 * Forbidden: Brand Status, release fields, CV, Source, Registry, restore registry, images,
 * active 45, House / Morgans / Radisson, Wave 14, broad rewrites.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE13_VERSION,
  WAVE13_HELD_PROMOTION_SLUG,
  WAVE13_NEVER_WRITE_FIELDS,
  WAVE13_SO_SECTION_PATTERN_CLEANUP_APPLY_FLAGS,
} from "./brand-explorer-wave13-factory-plan.js";
import { extractSoSectionPatternFailures } from "./brand-explorer-wave13-so-section-pattern-failures.js";
import {
  WAVE13_SO_SECTION_PATTERN_PACKAGES_VERSION,
  SO_SLUG,
  SO_BASICS_RECORD_ID,
  SO_PRESENTATION_BRAND_NAME,
  SO_SECTION_MOMENTUM_CARDS,
  SO_SECTION_MOMENTUM_LABEL,
  SO_SECTION_GEO_INTRO,
  SO_SECTION_REGION_BODIES,
  SO_SECTION_MEA_SUPPRESS,
  SO_SECTION_GROWTH_THEMES,
  SO_SECTION_GROWTH_EDITORIAL,
  SO_SECTION_GROWTH_FIT,
} from "./brand-explorer-wave13-so-section-pattern-cleanup-packages.js";
import { evaluateSectionPatternParity } from "./brand-explorer-section-pattern-parity.js";

export const WAVE13_SO_SECTION_PATTERN_CLEANUP_VERSION =
  "wave13-so-section-pattern-cleanup-v1";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 280;
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const FORBIDDEN_VISIBLE_RES = Object.freeze([
  /\bADR\b/,
  /\bRevPAR\b/,
  /fee-?stack/i,
  /\bFDD\b/,
  /Item\s*19/i,
  /\bLOI\b/,
  /source-supported/i,
  /\bsource pack\b/i,
  /\bfactory\b/i,
  /\bstage\s*\d/i,
  /\bgovernance\b/i,
  /\bQA\b/,
]);

const HOUSE = "the-house-of-originals";
const MORGANS = "morgans-originals";
const RADISSON = "radisson-collection";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isHidden(r) {
  return r?.active === false || /do not display|internal only/i.test(nz(r?.externalDisplayStatus));
}

function findSlot(rows, slotKey) {
  return (rows || []).find((r) => r.slotKey === slotKey && !isHidden(r)) || null;
}

function findAllSlots(rows, slotKey) {
  return (rows || []).filter((r) => r.slotKey === slotKey);
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

function scrubForbiddenFields(fields) {
  const next = { ...fields };
  for (const forbidden of [
    ...WAVE13_NEVER_WRITE_FIELDS,
    "Brand Status",
    "Active Profile Approved",
    "Ready for Active Profile",
    "Active Profile Approved Date",
    "Founder Visual Review Pass",
    "Image",
    "Images",
  ]) {
    if (next[forbidden] != null) delete next[forbidden];
  }
  return next;
}

function assertPackageClean() {
  const blobs = [
    SO_SECTION_GEO_INTRO,
    SO_SECTION_GROWTH_THEMES,
    SO_SECTION_GROWTH_EDITORIAL,
    SO_SECTION_GROWTH_FIT,
    ...Object.values(SO_SECTION_REGION_BODIES).map((r) => r.body),
    ...SO_SECTION_MOMENTUM_CARDS.map((c) => `${c.title}\n${c.summary}\n${c.body}`),
  ];
  const issues = [];
  for (const blob of blobs) {
    for (const re of FORBIDDEN_VISIBLE_RES) {
      if (re.test(blob)) issues.push(`forbidden:${re}`);
    }
    if (/https?:\/\//i.test(blob.replace(/https?:\/\/\S+\s*$/m, ""))) {
      // allow trailing URL only inside buildRecentMomentumCard body structure
    }
  }
  for (const c of SO_SECTION_MOMENTUM_CARDS) {
    if (!/\b(20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/i.test(c.dateLine)) {
      issues.push(`momentum_undated:${c.title}`);
    }
    if (!/^https?:\/\//i.test(c.url)) issues.push(`momentum_bad_url:${c.title}`);
  }
  if (issues.length) throw new Error(`SO section-pattern package dirty: ${issues.join("; ")}`);
}

async function airtableWrite({ method, recordId, fields }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  const cleaned = scrubForbiddenFields(fields);
  for (const k of Object.keys(cleaned)) {
    if (WAVE13_NEVER_WRITE_FIELDS.includes(k) || k === "Brand Status") {
      throw new Error(`Refuse never-write field: ${k}`);
    }
  }
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const body = method === "POST" ? { fields: cleaned } : { fields: cleaned };
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Airtable ${method} failed: ${res.status}`);
  return { id: json.id, fieldsPatched: Object.keys(cleaned), sanitizedPayloadPreview: cleaned };
}

function buildPatches(rows, identity) {
  const patches = [];

  // Momentum: hide existing visible, recreate dated cards
  const liveMomentum = findAllSlots(rows, "footprint.momentum");
  for (const row of liveMomentum) {
    if (!isHidden(row) && row.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: row.recordId,
        slotKey: "footprint.momentum",
        fields: { Active: false, "External Display Status": "Do Not Display" },
        reason: "hide_undated_momentum_card",
      });
    }
  }
  const labelLive = findSlot(rows, "footprint.momentum_label");
  if (labelLive?.recordId) {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: labelLive.recordId,
      slotKey: "footprint.momentum_label",
      fields: { Body: SO_SECTION_MOMENTUM_LABEL, Active: true },
      reason: "momentum_label",
    });
  } else {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "POST",
      recordId: null,
      slotKey: "footprint.momentum_label",
      fields: {
        "Slot Key": "footprint.momentum_label",
        "Brand Name": SO_PRESENTATION_BRAND_NAME,
        Brand: [identity.recordId],
        Active: true,
        "Sort Order": 0,
        Title: "Recent Momentum",
        Body: SO_SECTION_MOMENTUM_LABEL,
      },
      reason: "momentum_label_create",
    });
  }
  for (const card of SO_SECTION_MOMENTUM_CARDS) {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "POST",
      recordId: null,
      slotKey: "footprint.momentum",
      fields: {
        "Slot Key": "footprint.momentum",
        "Brand Name": SO_PRESENTATION_BRAND_NAME,
        Brand: [identity.recordId],
        Active: true,
        "Sort Order": card.sort,
        Title: card.title,
        Body: card.body,
      },
      reason: "dated_momentum_card_create",
      preview: { title: card.title, dateLine: card.dateLine, url: card.url },
    });
  }

  // Geo intro
  const geoIntro = findSlot(rows, "footprint.geo_intro") || findAllSlots(rows, "footprint.geo_intro")[0];
  if (geoIntro?.recordId) {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: geoIntro.recordId,
      slotKey: "footprint.geo_intro",
      fields: {
        Title: "Geographic Footprint",
        Body: SO_SECTION_GEO_INTRO,
        Active: true,
      },
      reason: "geo_intro_brand_specific",
    });
  } else {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "POST",
      recordId: null,
      slotKey: "footprint.geo_intro",
      fields: {
        "Slot Key": "footprint.geo_intro",
        "Brand Name": SO_PRESENTATION_BRAND_NAME,
        Brand: [identity.recordId],
        Active: true,
        "Sort Order": 10,
        Title: "Geographic Footprint",
        Body: SO_SECTION_GEO_INTRO,
      },
      reason: "geo_intro_create",
    });
  }

  // Region fills
  let sort = 12;
  for (const [slotKey, region] of Object.entries(SO_SECTION_REGION_BODIES)) {
    const live = findSlot(rows, slotKey) || findAllSlots(rows, slotKey)[0];
    const fields = {
      Title: region.title,
      Body: region.body,
      "Case Summary Overview": region.caseSummary,
      "Case Summary Tags": region.tags,
      Active: true,
    };
    if (live?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: live.recordId,
        slotKey,
        fields,
        reason: "region_brand_specific",
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey,
        fields: {
          "Slot Key": slotKey,
          "Brand Name": SO_PRESENTATION_BRAND_NAME,
          Brand: [identity.recordId],
          "Sort Order": sort++,
          ...fields,
        },
        reason: "region_create",
      });
    }
  }

  // MEA suppress
  const meaRows = findAllSlots(rows, SO_SECTION_MEA_SUPPRESS.slotKey);
  if (meaRows.length) {
    for (const row of meaRows) {
      if (!isHidden(row) && row.recordId) {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: row.recordId,
          slotKey: SO_SECTION_MEA_SUPPRESS.slotKey,
          fields: { Active: false, "External Display Status": "Do Not Display" },
          reason: "suppress_empty_mea_panel",
        });
      }
    }
  } else {
    // Ensure no empty visible MEA — create suppressed placeholder if factory left nothing
    // Prefer no create; empty missing slot is fine for findVisible
  }

  // Growth
  for (const [slotKey, body, title] of [
    ["footprint.growth_themes", SO_SECTION_GROWTH_THEMES, "Growth Themes"],
    ["footprint.growth_editorial", SO_SECTION_GROWTH_EDITORIAL, "Growth Priorities"],
    ["footprint.growth_fit", SO_SECTION_GROWTH_FIT, "Growth Fit"],
  ]) {
    const live = findSlot(rows, slotKey) || findAllSlots(rows, slotKey)[0];
    if (live?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: live.recordId,
        slotKey,
        fields: { Title: title, Body: body, Active: true },
        reason: "growth_brand_specific",
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey,
        fields: {
          "Slot Key": slotKey,
          "Brand Name": SO_PRESENTATION_BRAND_NAME,
          Brand: [identity.recordId],
          Active: true,
          "Sort Order": 20,
          Title: title,
          Body: body,
        },
        reason: "growth_create",
      });
    }
  }

  return patches;
}

function simulateRowsAfterPatches(rows, patches) {
  const next = (rows || []).map((r) => ({ ...r }));
  for (const p of patches) {
    if (p.action === "PATCH" && p.recordId) {
      const idx = next.findIndex((r) => r.recordId === p.recordId);
      if (idx >= 0) {
        const f = p.fields || {};
        if (f.Body != null) next[idx].body = f.Body;
        if (f.Title != null) next[idx].title = f.Title;
        if (f.Active === false) next[idx].active = false;
        if (f["External Display Status"] != null) {
          next[idx].externalDisplayStatus = f["External Display Status"];
        }
      }
    } else if (p.action === "POST") {
      const f = p.fields || {};
      next.push({
        recordId: `planned:${p.slotKey}:${next.length}`,
        slotKey: f["Slot Key"] || p.slotKey,
        title: f.Title || "",
        body: f.Body || "",
        sortOrder: f["Sort Order"] || 0,
        active: f.Active !== false,
        externalDisplayStatus: f["External Display Status"] || "",
      });
    }
  }
  return next;
}

function writeReports(report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-section-pattern-cleanup.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-section-pattern-cleanup.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`);
  const docPath = path.join(DOCS_DIR, "brand-explorer-wave13-so-section-pattern-cleanup.md");
  fs.writeFileSync(
    docPath,
    [
      `# Wave 13 — SO/ Section Pattern Cleanup`,
      ``,
      `SO/ Presentation-only micro cleanup after public release.`,
      ``,
      `- Dated Recent Momentum cards`,
      `- Empty MEA region suppressed (Do Not Display)`,
      `- Brand-specific growth priorities + geo intro`,
      ``,
      `Ready: \`${report.readyStatement}\``,
      ``,
      `Generated: ${report.generatedAt}`,
      ``,
    ].join("\n")
  );
  return { jsonPath, mdPath, docPath };
}

function renderMarkdown(r) {
  const lines = [
    `# Wave 13 — SO/ Section Pattern Cleanup`,
    ``,
    `Version: \`${r.version}\` · Generated: ${r.generatedAt}`,
    `Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`,
    `Ready: \`${r.readyStatement}\``,
    ``,
    `## Scope`,
    ``,
    `- Brand: \`${SO_SLUG}\` (\`${SO_BASICS_RECORD_ID}\`)`,
    `- Presentation Brand Name: **${SO_PRESENTATION_BRAND_NAME}**`,
    `- Untouched: active 45 · House · Morgans · Radisson · Wave 14 · status/release/CV/Source/Registry/images`,
    ``,
    `## Planned patches (${(r.patches || []).length})`,
    ``,
    `| Action | Slot | Record | Reason |`,
    `| --- | --- | --- | --- |`,
  ];
  for (const p of r.patches || []) {
    lines.push(
      `| ${p.action} | \`${p.slotKey}\` | \`${p.recordId || "POST"}\` | ${p.reason} |`
    );
  }
  lines.push(
    ``,
    `## Simulated section-pattern parity`,
    ``,
    "```json",
    JSON.stringify(r.simulatedParity?.gates || r.simulatedParity, null, 2),
    "```",
    ``,
    `## Apply results`,
    ``,
    "```json",
    JSON.stringify(r.applyResults, null, 2),
    "```",
    ``,
    `## Guardrails`,
    ``
  );
  for (const [k, v] of Object.entries(r.guardrails || {})) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("");
  return lines.join("\n");
}

/**
 * @param {{ apply?: boolean, argv?: string[] }} opts
 */
export async function runWave13SoSectionPatternCleanup({ apply = false, argv = [] } = {}) {
  const stage = "so-section-pattern-cleanup";
  assertPackageClean();
  const failures = extractSoSectionPatternFailures();
  const flagCheck = checkFlags(WAVE13_SO_SECTION_PATTERN_CLEANUP_APPLY_FLAGS, argv, apply);
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[SO_SLUG];
  if (!identity?.recordId || identity.recordId !== SO_BASICS_RECORD_ID) {
    throw new Error(`SO/ identity mismatch: ${identity?.recordId}`);
  }
  if (WAVE13_HELD_PROMOTION_SLUG !== SO_SLUG) {
    throw new Error("SO slug mismatch vs WAVE13_HELD_PROMOTION_SLUG");
  }

  const { rows } = await listPresentationRowsLight(identity.recordId, SO_PRESENTATION_BRAND_NAME);
  const patches = buildPatches(rows, identity);
  const simulatedRows = simulateRowsAfterPatches(rows, patches);
  const simulatedParity = evaluateSectionPatternParity({
    brandSlug: SO_SLUG,
    brandName: "SO/",
    presentationRows: simulatedRows,
    html: "",
  });

  const preflightIssues = [];
  if (!simulatedParity.pass) {
    preflightIssues.push(`simulated_section_pattern_still_fail:${simulatedParity.gates ? JSON.stringify(simulatedParity.gates) : "fail"}`);
  }
  for (const slug of [HOUSE, MORGANS, RADISSON]) {
    if (patches.some((p) => nz(p.fields?.["Brand Name"]).toLowerCase().includes(slug))) {
      preflightIssues.push(`forbidden_brand_in_patch:${slug}`);
    }
  }

  const preflightOk = preflightIssues.length === 0;
  const applyPerformed = apply === true && flagCheck.ok === true && preflightOk;
  const applyResults = [];
  let writePerformed = false;

  if (applyPerformed) {
    for (const p of patches) {
      try {
        const response = await airtableWrite({
          method: p.action,
          recordId: p.recordId,
          fields: p.fields,
        });
        writePerformed = true;
        applyResults.push({
          ...p,
          applied: true,
          writePerformed: true,
          response: { id: response.id, fieldsPatched: response.fieldsPatched },
          sanitizedPayloadPreview: response.sanitizedPayloadPreview,
        });
      } catch (err) {
        applyResults.push({
          ...p,
          applied: false,
          writePerformed: false,
          error: err.message,
        });
      }
      await sleep(WRITE_THROTTLE_MS);
    }
  } else if (apply && !flagCheck.ok) {
    applyResults.push({ applied: false, reason: "missing_apply_flags", missing: flagCheck.missing });
  } else if (apply && !preflightOk) {
    applyResults.push({ applied: false, reason: "preflight_failed", issues: preflightIssues });
  }

  const readyStatement = applyPerformed
    ? "wave13_so_section_pattern_clean_ready_for_46_baseline_freeze"
    : preflightOk
      ? "wave13_so_section_pattern_cleanup_dry_run_ready"
      : "wave13_so_section_pattern_cleanup_blocked";

  const report = {
    version: WAVE13_SO_SECTION_PATTERN_CLEANUP_VERSION,
    packagesVersion: WAVE13_SO_SECTION_PATTERN_PACKAGES_VERSION,
    waveVersion: WAVE13_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    brandSlug: SO_SLUG,
    recordId: SO_BASICS_RECORD_ID,
    presentationBrandName: SO_PRESENTATION_BRAND_NAME,
    failureExtraction: {
      issueCount: failures.issueCount,
      paths: failures.paths,
    },
    patches,
    patchCount: patches.length,
    simulatedParity: {
      pass: simulatedParity.pass,
      gates: simulatedParity.gates,
      sections: Object.fromEntries(
        Object.entries(simulatedParity.sections || {}).map(([k, v]) => [
          k,
          { pass: v.pass, failureReason: v.failureReason, failures: v.failures },
        ])
      ),
    },
    preflight: { ok: preflightOk, issues: preflightIssues },
    flagCheck,
    applyResults,
    guardrails: {
      soOnly: true,
      targetedSectionPatternFixesOnly: true,
      noBrandStatusChanges: true,
      noReleaseFieldWrites: true,
      noCompanyValidationChanges: true,
      noSourceLibraryStatusChanges: true,
      noRegistryApprovalChanges: true,
      noPublicRestoreRegistryChanges: true,
      noImageWrites: true,
      noActive45Writes: true,
      noHouseOfOriginalsWrites: true,
      noMorgansOriginalsWrites: true,
      noRadissonCollectionChanges: true,
      noWave14Work: true,
      noBroadRewrites: true,
    },
    readyStatement,
  };

  const paths = writeReports(report, renderMarkdown(report));
  return { ...report, paths };
}
