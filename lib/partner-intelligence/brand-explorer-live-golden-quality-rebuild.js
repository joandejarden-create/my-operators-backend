/**
 * Live Golden-Quality Content Rebuild
 * (Hotel Indigo, MGallery Collection, SLH)
 *
 * In-place content remediation while brands remain active_profile_ready.
 * Does not change release fields, Company Validated, Source Library, Registry,
 * or protected golden brands.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import { ORIGINAL_GOLDEN_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";
import { LIVE_GOLDEN_REBUILD_CONTENT } from "./brand-explorer-live-golden-quality-rebuild-content.js";
import {
  evaluateGoldenContentQuality,
} from "./brand-explorer-golden-content-quality.js";

export const LIVE_GOLDEN_REBUILD_VERSION = "live-golden-quality-rebuild";

export const TARGET_BRANDS = Object.freeze([
  "hotel-indigo",
  "mgallery-collection",
  "small-luxury-hotels-of-the-world",
]);

export const PROTECTED_BRANDS = Object.freeze([
  ...ORIGINAL_GOLDEN_RELEASE_SLUGS,
  "tribute-portfolio",
]);

export const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
export const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-live-golden-quality-rebuild",
  "--confirm-keep-active-profile-ready",
  "--confirm-no-company-validation-changes",
  "--confirm-no-active-release-field-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-protected-brands-unchanged",
  "--confirm-no-unsupported-metrics",
  "--confirm-no-empty-bullets",
  "--confirm-no-blank-cards",
  "--confirm-no-stub-chips",
  "--confirm-no-duplicate-scenario-images",
  "--confirm-brand-specific-copy",
  "--confirm-benchmark-quality-met",
]);

export const REPORT_JSON = "brand-explorer-live-golden-quality-rebuild.json";
export const REPORT_MD = "brand-explorer-live-golden-quality-rebuild.md";

const BRAND_REPORT_MD = Object.freeze({
  "hotel-indigo": "brand-explorer-live-golden-quality-rebuild-hotel-indigo.md",
  "mgallery-collection": "brand-explorer-live-golden-quality-rebuild-mgallery.md",
  "small-luxury-hotels-of-the-world": "brand-explorer-live-golden-quality-rebuild-slh.md",
});

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(s) {
  return nz(s).split(/\s+/).filter(Boolean).length;
}

/** v40C residual scrub treats blank-line paragraphs as dirty owner copy. */
function normalizeOwnerBody(s) {
  return nz(s).replace(/\n{2,}/g, "\n");
}

function isHidden(row) {
  const st = nz(row.externalDisplayStatus || row["External Display Status"]);
  return /do not display|internal only/i.test(st) || row.active === false || row.visible === false;
}

async function fetchBrandApi(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`Brand API failed for ${slug}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

function caseSummaryFields(cs) {
  if (!cs) return {};
  return {
    "Case Summary Overview": nz(cs.overview),
    "Case Summary Brand Relevance": nz(cs.brandRelevance),
    "Case Summary Owner Objective": nz(cs.ownerObjective),
    "Case Summary Interpretation": nz(cs.interpretation),
    "Case Summary Tags": nz(cs.tags),
  };
}

function findOpeningRow(rows, propertyName) {
  const needle = nz(propertyName).toLowerCase();
  return (rows || []).find(
    (r) =>
      nz(r.slotKey) === "footprint.openings" &&
      !isHidden(r) &&
      nz(r.title).toLowerCase().includes(needle)
  );
}

function findSlotRow(rows, slotKey) {
  return (rows || []).find((r) => nz(r.slotKey) === slotKey && !isHidden(r)) || null;
}

function stableImageKey(url) {
  const u = nz(url);
  if (!u) return "";
  try {
    const parsed = new URL(u);
    return `${parsed.origin}${parsed.pathname}`.toLowerCase();
  } catch {
    return u.split("?")[0].toLowerCase();
  }
}

function scenarioImagesAreDistinct(presentationRows) {
  const keys = [1, 2, 3]
    .map((i) => stableImageKey(findSlotRow(presentationRows, `overview.scenario.${i}`)?.imageUrl))
    .filter(Boolean);
  return keys.length >= 3 && new Set(keys).size === keys.length;
}

function pickDistinctScenarioImages(presentationRows) {
  const openings = (presentationRows || [])
    .filter((r) => nz(r.slotKey) === "footprint.openings" && !isHidden(r) && nz(r.imageUrl))
    .sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0));
  const gallery = (presentationRows || [])
    .filter((r) => /^materials\.gallery\.\d+$/.test(nz(r.slotKey)) && !isHidden(r) && nz(r.imageUrl))
    .sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0));
  const pool = [...openings, ...gallery];
  const urls = [];
  const seen = new Set();
  for (const row of pool) {
    const u = nz(row.imageUrl);
    const key = stableImageKey(u);
    if (!u || !key || seen.has(key)) continue;
    seen.add(key);
    urls.push({ url: u, fromRecordId: row.recordId, slotKey: row.slotKey });
    if (urls.length >= 3) break;
  }
  return urls;
}

function auditBrandAgainstBenchmark(brandSlug, brandApi, presentationRows, quality) {
  const defects = [];
  const below = [];
  const pack = LIVE_GOLDEN_REBUILD_CONTENT[brandSlug];

  if (!pack) defects.push({ type: "missing_content_pack", severity: "high" });

  for (const slot of [
    "overview.why_value",
    "overview.proof.1",
    "overview.featured_application",
    "overview.differentiators.identity",
    "footprint.geo_intro",
    "footprint.growth_editorial",
  ]) {
    if (!findSlotRow(presentationRows, slot)) {
      defects.push({ type: "missing_slot", slot, severity: "high" });
      below.push(slot);
    }
  }

  for (const i of [1, 2, 3]) {
    const row = findSlotRow(presentationRows, `overview.scenario.${i}`);
    if (!row) {
      defects.push({ type: "missing_scenario", slot: `overview.scenario.${i}`, severity: "high" });
      below.push(`overview.scenario.${i}`);
    } else if (words(row.body) < 45) {
      defects.push({
        type: "thin_scenario",
        slot: `overview.scenario.${i}`,
        words: words(row.body),
        severity: "high",
      });
      below.push(`overview.scenario.${i}`);
    }
  }

  const openings = (presentationRows || []).filter(
    (r) => nz(r.slotKey) === "footprint.openings" && !isHidden(r)
  );
  for (const o of openings) {
    if (words(o.body) < 30) {
      defects.push({
        type: "thin_opening",
        recordId: o.recordId,
        title: o.title,
        words: words(o.body),
        severity: "medium",
      });
    }
    if (
      brandSlug === "small-luxury-hotels-of-the-world" &&
      /cala property example/i.test(nz(o.title)) &&
      /(san r[eé]gis|quinta da comporta)/i.test(nz(o.title))
    ) {
      defects.push({
        type: "mislabelled_geography",
        recordId: o.recordId,
        title: o.title,
        severity: "high",
      });
    }
  }

  const scenImgs = [1, 2, 3]
    .map((i) => nz(findSlotRow(presentationRows, `overview.scenario.${i}`)?.imageUrl))
    .filter(Boolean);
  if (scenImgs.length >= 2 && new Set(scenImgs).size < scenImgs.length) {
    defects.push({ type: "duplicate_scenario_images", severity: "high", urls: scenImgs });
  }

  if (quality && !quality.pass) {
    for (const f of quality.failures || []) {
      defects.push({ type: "quality_gate", detail: f, severity: "high" });
    }
  }

  const genericPos = /Leisure Discerning travelers|Luxury\s*\/\s*Discerning,\s*Experience-Oriented/i.test(
    nz(brandApi.brandPositioning) + " " + nz(brandApi.guestPsychographics)
  );
  if (genericPos) {
    defects.push({ type: "generic_positioning", severity: "high" });
    below.push("Brand Positioning / Audience");
  }

  return {
    defectCount: defects.length,
    defects,
    sectionsBelowBenchmark: [...new Set(below)],
  };
}

export function parseLiveGoldenApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

export async function planLiveGoldenRebuildBrand(brandSlug) {
  if (!TARGET_BRANDS.includes(brandSlug)) {
    throw new Error(`Target brands only: ${TARGET_BRANDS.join(", ")}`);
  }
  if (PROTECTED_BRANDS.includes(brandSlug)) {
    throw new Error(`Refuse protected brand ${brandSlug}`);
  }

  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig) throw new Error(`No brand config for ${brandSlug}`);
  const pack = LIVE_GOLDEN_REBUILD_CONTENT[brandSlug];
  if (!pack) throw new Error(`No content pack for ${brandSlug}`);

  const ctx = await loadBrandFactoryContext(brandSlug);
  const brandApi = await fetchBrandApi(brandSlug);
  const rows = ctx.presentationRows || [];
  const html = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: false,
  });
  const quality = evaluateGoldenContentQuality(brandApi, rows, html, { brandSlug });
  const audit = auditBrandAgainstBenchmark(brandSlug, brandApi, rows, quality);

  const patches = [];
  const blockers = [];

  if (brandApi.shouldRenderFullProfile !== true || brandApi.brandExplorerDisplayState !== "active_profile_ready") {
    blockers.push("must_remain_active_profile_ready");
  }

  // Brand Basics (positioning / audience / promise)
  const basicsFields = {};
  const basicsBefore = {
    "Brand Positioning": brandApi.brandPositioning,
    "Target Guest Segments": brandApi.targetGuestSegments,
    "Guest Psychographics Description": brandApi.guestPsychographics,
    "Brand Customer Promise": brandApi.brandCustomerPromise,
  };
  for (const [field, value] of Object.entries(pack.basics || {})) {
    if (FORBIDDEN_WRITE_FIELDS.has(field)) blockers.push(`forbidden_basics:${field}`);
    const current =
      field === "Brand Positioning"
        ? nz(brandApi.brandPositioning)
        : field === "Target Guest Segments"
          ? brandApi.targetGuestSegments
          : field === "Guest Psychographics Description"
            ? nz(brandApi.guestPsychographics)
            : field === "Brand Customer Promise"
              ? nz(brandApi.brandCustomerPromise)
              : "";
    const same =
      Array.isArray(value) && Array.isArray(current)
        ? JSON.stringify([...value].sort()) === JSON.stringify([...current].map(String).sort())
        : Array.isArray(value)
          ? JSON.stringify(value) === JSON.stringify([].concat(current || []).map(String))
          : nz(current) === nz(value);
    if (!same) {
      basicsFields[field] = value;
    }
  }
  if (Object.keys(basicsFields).length) {
    const forbidden = scanForbiddenLanguage(
      Object.values(basicsFields)
        .flat()
        .map(String)
        .join("\n")
    );
    if (forbidden.length) {
      blockers.push(`basics_forbidden:${forbidden.map((h) => h.id).join(",")}`);
    } else {
      patches.push({
        table: BRAND_BASICS_TABLE,
        action: "PATCH",
        recordId: brandConfig.recordId,
        kind: "basics",
        fields: basicsFields,
        before: basicsBefore,
        after: basicsFields,
      });
    }
  }

  const scenarioImages = pickDistinctScenarioImages(rows);

  // Presentation upserts from pack
  for (const row of pack.presentation || []) {
    const existing = findSlotRow(rows, row.slotKey);
    const fields = {
      Title: nz(row.title),
      Body: normalizeOwnerBody(row.body),
      ...caseSummaryFields(row.caseSummary),
    };
    const forbidden = scanForbiddenLanguage(`${fields.Title}\n${fields.Body}`);
    if (forbidden.length) {
      blockers.push(`copy_forbidden:${row.slotKey}:${forbidden.map((h) => h.id).join(",")}`);
      continue;
    }

    // Assign distinct scenario images from openings/gallery inventory only when needed
    const scenMatch = /^overview\.scenario\.(\d+)$/.exec(row.slotKey);
    if (scenMatch) {
      const idx = Number(scenMatch[1]) - 1;
      const pick = scenarioImages[idx];
      const existingKey = stableImageKey(existing?.imageUrl);
      const pickKey = stableImageKey(pick?.url);
      const needDistinctAssignment = !scenarioImagesAreDistinct(rows);
      if (pick?.url && needDistinctAssignment && existingKey !== pickKey) {
        fields.Image = [{ url: pick.url }];
      }
    }

    if (existing?.recordId) {
      const sameBody =
        nz(existing.body) === normalizeOwnerBody(row.body) && nz(existing.title) === nz(row.title);
      const sameCase =
        !row.caseSummary ||
        (nz(existing.caseSummaryOverview) === nz(row.caseSummary.overview) &&
          nz(existing.caseSummaryBrandRelevance) === nz(row.caseSummary.brandRelevance));
      const needImage = Boolean(fields.Image);
      if (sameBody && sameCase && !needImage) continue;
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: existing.recordId,
        slotKey: row.slotKey,
        kind: "presentation_update",
        fields,
        before: { title: existing.title, body: existing.body, imageUrl: existing.imageUrl },
        after: { title: row.title, body: row.body, imageUrl: fields.Image?.[0]?.url || existing.imageUrl },
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey: row.slotKey,
        kind: "presentation_create",
        fields: {
          "Slot Key": row.slotKey,
          "Brand Name": brandConfig.name,
          Brand: [brandConfig.recordId],
          Active: true,
          "Sort Order": row.sortOrder ?? 0,
          ...fields,
        },
        before: null,
        after: { title: row.title, body: row.body },
      });
    }
  }

  // Opening deepenings
  for (const opening of pack.openings || []) {
    const existing = findOpeningRow(rows, opening.propertyName);
    if (!existing?.recordId) {
      blockers.push(`opening_not_found:${opening.propertyName}`);
      continue;
    }
    const fields = {
      Title: nz(opening.title),
      Body: normalizeOwnerBody(opening.body),
      ...caseSummaryFields(opening.caseSummary),
    };
    const forbidden = scanForbiddenLanguage(`${fields.Title}\n${fields.Body}`);
    if (forbidden.length) {
      blockers.push(`opening_forbidden:${opening.propertyName}:${forbidden.map((h) => h.id).join(",")}`);
      continue;
    }
    if (
      nz(existing.title) === nz(opening.title) &&
      nz(existing.body) === normalizeOwnerBody(opening.body) &&
      nz(existing.caseSummaryOverview) === nz(opening.caseSummary?.overview)
    ) {
      continue;
    }
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: existing.recordId,
      slotKey: "footprint.openings",
      kind: "opening_update",
      propertyName: opening.propertyName,
      fields,
      before: {
        title: existing.title,
        body: existing.body,
        caseSummaryOverview: existing.caseSummaryOverview,
      },
      after: {
        title: opening.title,
        body: opening.body,
        caseSummaryOverview: opening.caseSummary?.overview,
      },
    });
  }

  // Image distinctiveness gate on planned scenario patches
  const plannedScenarioUrls = patches
    .filter((p) => /^overview\.scenario\.\d+$/.test(nz(p.slotKey)) && p.fields?.Image?.[0]?.url)
    .map((p) => stableImageKey(p.fields.Image[0].url));
  if (plannedScenarioUrls.length >= 2 && new Set(plannedScenarioUrls).size < plannedScenarioUrls.length) {
    blockers.push("planned_duplicate_scenario_images");
  }
  if (!scenarioImagesAreDistinct(rows) && scenarioImages.length < 3) {
    blockers.push(`insufficient_distinct_images:${scenarioImages.length}`);
  }

  return {
    brandSlug,
    brandName: brandConfig.name,
    recordId: brandConfig.recordId,
    liveState: {
      displayState: brandApi.brandExplorerDisplayState,
      shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
    },
    audit,
    qualityBefore: quality,
    scenarioImagePlan: scenarioImages,
    patches,
    blockers,
    blocked: blockers.length > 0,
    projection: {
      remainsActiveProfileReady: true,
      patchCount: patches.length,
      creates: patches.filter((p) => p.action === "POST").length,
      updates: patches.filter((p) => p.action === "PATCH").length,
    },
    guardrails: {
      companyValidatedChanges: false,
      activeReleaseFieldChanges: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      protectedBrandChanges: false,
      unlock: false,
    },
  };
}

async function airtableWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `${method} ${table} failed: ${res.status}`);
  return json;
}

export async function applyLiveGoldenRebuild({ brandResults, apply = false, argv = [] } = {}) {
  const flagCheck = parseLiveGoldenApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of brandResults) {
    if (brand.blocked) {
      resultsByBrand[brand.brandSlug] = { applied: false, reason: "blocked", blockers: brand.blockers };
      continue;
    }
    if (PROTECTED_BRANDS.includes(brand.brandSlug)) {
      throw new Error(`Refuse protected brand write ${brand.brandSlug}`);
    }
    const created = [];
    const updated = [];
    const errors = [];
    for (const patch of brand.patches) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_WRITE_FIELDS.has(key)) {
          throw new Error(`Forbidden field write: ${key}`);
        }
      }
      try {
        if (patch.action === "POST") {
          const json = await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            fields: patch.fields,
            method: "POST",
          });
          created.push({ recordId: json.id, slotKey: patch.slotKey, kind: patch.kind });
        } else {
          await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            recordId: patch.recordId,
            fields: patch.fields,
            method: "PATCH",
          });
          updated.push({
            recordId: patch.recordId,
            slotKey: patch.slotKey,
            kind: patch.kind,
            table: patch.table,
          });
        }
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        errors.push({ slotKey: patch.slotKey, message: err.message });
      }
    }
    resultsByBrand[brand.brandSlug] = {
      applied: errors.length === 0,
      created,
      updated,
      errors,
    };
  }
  return { applied: true, resultsByBrand, flagCheck };
}

export async function runLiveGoldenQualityRebuild({
  brands = TARGET_BRANDS,
  dryRun = true,
  argv = [],
} = {}) {
  for (const b of brands) {
    if (PROTECTED_BRANDS.includes(b)) throw new Error(`Refuse protected brand ${b}`);
    if (!TARGET_BRANDS.includes(b)) throw new Error(`Targets only: ${TARGET_BRANDS.join(", ")}`);
  }

  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await planLiveGoldenRebuildBrand(brandSlug));
  }

  const applyResult = dryRun
    ? { applied: false, reason: "dry_run_only" }
    : await applyLiveGoldenRebuild({ brandResults, apply: true, argv });

  // Post-apply quality snapshot (best-effort; dry-run uses projected)
  const post = [];
  if (!dryRun && applyResult.applied) {
    for (const brandSlug of brands) {
      const ctx = await loadBrandFactoryContext(brandSlug);
      const brandApi = await fetchBrandApi(brandSlug);
      const html = renderBrandExplorerHtmlForTest(brandApi, {
        allPanels: true,
        internalPreview: false,
      });
      const ql = evaluateBrandExternalQualityLock(brandApi, html, { brandSlug });
      const quality = evaluateGoldenContentQuality(brandApi, ctx.presentationRows || [], html, {
        brandSlug,
      });
      post.push({
        brandSlug,
        displayState: brandApi.brandExplorerDisplayState,
        full: brandApi.shouldRenderFullProfile === true,
        externalLockPass: ql.externalQualityLockPass === true,
        qualityPass: quality.pass,
        qualityFailures: quality.failures,
      });
    }
  }

  return {
    version: LIVE_GOLDEN_REBUILD_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    brands,
    brandResults,
    applyResult,
    postApply: post,
    summary: {
      totalPatches: brandResults.reduce((n, b) => n + b.patches.length, 0),
      blockedBrands: brandResults.filter((b) => b.blocked).length,
      totalDefects: brandResults.reduce((n, b) => n + (b.audit?.defectCount || 0), 0),
      writes: dryRun ? false : applyResult.applied === true,
    },
    guardrails: {
      activeProfileReadyPreserved: true,
      companyValidatedChanges: false,
      activeReleaseFieldChanges: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      protectedBrandChanges: false,
    },
    benchmarks: ["tribute-portfolio", "kimpton", "radisson-individuals-by-choice", "design-hotels"],
  };
}

function brandMd(b) {
  const lines = [
    `# Live Golden-Quality Rebuild — ${b.brandName}`,
    "",
    `Slug: \`${b.brandSlug}\``,
    "",
    "## Live state",
    "",
    `- Display: **${b.liveState.displayState}**`,
    `- Full profile: **${b.liveState.shouldRenderFullProfile}**`,
    "",
    "## Defects vs benchmark",
    "",
    `- Count: **${b.audit.defectCount}**`,
    `- Sections below benchmark: ${b.audit.sectionsBelowBenchmark.join(", ") || "—"}`,
    "",
  ];
  for (const d of b.audit.defects.slice(0, 40)) {
    lines.push(`- ${d.type}${d.slot ? ` · ${d.slot}` : ""}${d.detail ? ` · ${d.detail}` : ""}`);
  }
  lines.push("", "## Projection", "");
  lines.push(`- Patches: **${b.projection.patchCount}** (create ${b.projection.creates} / update ${b.projection.updates})`);
  lines.push(`- Remains active_profile_ready: **${b.projection.remainsActiveProfileReady}**`);
  lines.push(`- Blocked: **${b.blocked}**`);
  if (b.blockers.length) {
    lines.push("", "### Blockers", "");
    for (const x of b.blockers) lines.push(`- ${x}`);
  }
  lines.push("", "## Scenario image plan", "");
  for (const [i, img] of (b.scenarioImagePlan || []).entries()) {
    lines.push(`- scenario.${i + 1} ← ${img.slotKey} (${img.fromRecordId})`);
  }
  lines.push("", "## Patches", "");
  for (const p of b.patches) {
    lines.push(
      `- **${p.action}** · ${p.kind} · ${p.slotKey || p.table} · ${p.recordId || "CREATE"}`
    );
    if (p.after?.title) lines.push(`  - title: ${String(p.after.title).slice(0, 100)}`);
    if (p.after?.body) lines.push(`  - body: ${String(p.after.body).slice(0, 160)}…`);
  }
  lines.push("", "## Remaining founder judgment", "");
  lines.push("- Spot-check scenario imagery distinctiveness in the live atelier.");
  lines.push("- Confirm Brand Snapshot metric cards that come from Brand Footprint (suppress elsewhere if blank).");
  lines.push("- Taste-pass Owner Considerations (standards.*) already present from v42A-R2.");
  lines.push("");
  return lines.join("\n");
}

export function writeLiveGoldenRebuildReports(report) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    `# Live Golden-Quality Content Rebuild`,
    "",
    `Generated: ${report.generatedAt}`,
    `dryRun=${report.dryRun}`,
    "",
    "## Summary",
    "",
    `- Total defects found: **${report.summary.totalDefects}**`,
    `- Total patches: **${report.summary.totalPatches}**`,
    `- Blocked brands: **${report.summary.blockedBrands}**`,
    `- Presentation/Basics writes: **${report.summary.writes}**`,
    `- Benchmarks: ${report.benchmarks.join(", ")}`,
    "",
    "## What this remediates",
    "",
    "- Brand-specific Positioning / Audience / Promise (Basics + Guest Psychographics)",
    "- Deep scenario cards with distinct opening/gallery images",
    "- Why Value Is Strongest (5 complete bullets)",
    "- Proof points, featured application, differentiators, best-at, footprint/growth",
    "- Deeper property examples; SLH non-CALA geography labels corrected",
    "- No Company Validated / release-field / Source / Registry / protected-brand writes",
    "",
    "## Guardrails",
    "",
    `- Keep active_profile_ready: **${report.guardrails.activeProfileReadyPreserved}**`,
    `- Company Validated changes: **${report.guardrails.companyValidatedChanges}**`,
    `- Active-release field changes: **${report.guardrails.activeReleaseFieldChanges}**`,
    `- Protected brand changes: **${report.guardrails.protectedBrandChanges}**`,
    "",
  ];
  for (const b of report.brandResults) {
    md.push(`### ${b.brandSlug}`);
    md.push(`- Live state: **${b.liveState.displayState}** (full=${b.liveState.shouldRenderFullProfile})`);
    md.push(`- Defects: ${b.audit.defectCount}`);
    md.push(`- Patches: ${b.patches.length}`);
    md.push(`- Below benchmark: ${b.audit.sectionsBelowBenchmark.join(", ") || "—"}`);
    md.push(`- Scenario images planned: ${(b.scenarioImagePlan || []).length}`);
    if (b.qualityBefore && !b.qualityBefore.pass) {
      md.push(`- Quality failures (before): ${b.qualityBefore.failures.join("; ")}`);
    }
    md.push("");
  }
  if (report.postApply?.length) {
    md.push("## Post-apply quality", "");
    for (const p of report.postApply) {
      md.push(
        `- **${p.brandSlug}**: state=${p.displayState} externalLock=${p.externalLockPass} goldenQuality=${p.qualityPass}${
          p.qualityFailures?.length ? ` (${p.qualityFailures.join("; ")})` : ""
        }`
      );
    }
    md.push("");
  }
  md.push("## Remaining founder judgment", "");
  md.push("- Visual taste-pass of scenario imagery and property cards in the live atelier.");
  md.push("- Brand Snapshot metric empties that come from Brand Footprint (not invented here).");
  md.push("- Confirm Owner Considerations (standards.*) still read as owner-useful after content depth upgrade.");
  md.push("");
  fs.writeFileSync(mdPath, md.join("\n"));

  const brandPaths = {};
  for (const b of report.brandResults) {
    const name = BRAND_REPORT_MD[b.brandSlug];
    if (!name) continue;
    const p = path.join(reportsDir, name);
    fs.writeFileSync(p, brandMd(b));
    brandPaths[b.brandSlug] = p;
  }
  return { jsonPath, mdPath, brandPaths };
}
