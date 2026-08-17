/**
 * Final 39-brand freeze blocker cleanup:
 * Lane A — image uniqueness remediation (4 brands)
 * Lane B — ADR / forbidden owner-facing language scrub (4 brands)
 *
 * Never writes CV / Source Library / Registry / Brand Status / release / restore registry.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildImageIdentity,
  evaluateImageUniqueness,
  pickDistinctImageAssets,
  GALLERY_DISTINCT_MIN,
  SCENARIO_DISTINCT_MIN,
  PROPERTY_DISTINCT_MIN,
} from "./brand-explorer-image-uniqueness.js";
import {
  evaluateBrandImageRoleMatch,
  GALLERY_ROLE_CAPTIONS,
  DEFAULT_GALLERY_ROLE_SEQUENCE,
  detectVisualCategory,
  IMAGE_ROLES,
} from "./brand-explorer-image-role-match.js";
import { toAirtableFetchableImageUrl } from "./brand-explorer-lane2-image-materialization.js";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import { resolveActiveUniverseRecordId } from "./brand-explorer-active-universe.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import { buildWave12ImageAssetPackForBrand } from "./brand-explorer-wave12-image-materialization.js";
import { isLogoImageUrl, isGenericBrandOrLifestyleImageUrl } from "./brand-explorer-footprint-opening-image-governance.js";

export const FINAL_39_FREEZE_BLOCKER_CLEANUP_VERSION = "brand-explorer-39-final-freeze-blocker-cleanup-v2";

export const IMAGE_TARGETS = Object.freeze([
  "voco-hotels",
  "avid-hotels",
  "holiday-inn-express",
  "vignette-collection",
]);

/** Original Lane B targets from quality-audit freeze blockers. */
export const ADR_TARGETS_PRIMARY = Object.freeze([
  "small-luxury-hotels-of-the-world",
  "suburban-studios",
  "trademark-collection-by-wyndham",
  "woodspring-suites",
]);

/**
 * Residual Active/Live brands that still fail PVQL / display on the same
 * forbidden owner-facing language class (ADR token or fee-stack phrasing).
 * Included so 39/39 can become freeze-ready without a separate status task.
 */
export const ADR_TARGETS_RESIDUAL = Object.freeze([
  "ascend",
  "comfort-inn-suites",
  "country-inn-suites",
  "everhome-suites",
  "hotel-indigo",
  "kimpton",
  "mgallery-collection",
  "preferred-hotels-and-resorts",
  "quality-inn",
  "radisson",
  "radisson-blu",
  "radisson-individuals-by-choice",
  "bw-premier-collection",
]);

export const ADR_TARGETS = Object.freeze([...ADR_TARGETS_PRIMARY, ...ADR_TARGETS_RESIDUAL]);

export const APPLY_FLAGS = Object.freeze([
  "--approve-39-final-freeze-blocker-cleanup",
  "--confirm-target-brands-only",
  "--confirm-image-writes-only-for-uniqueness-targets",
  "--confirm-copy-writes-only-for-adr-targets",
  "--confirm-scene7-filename-aware-distinct-images",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-public-restore-registry-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-raw-urls",
  "--confirm-no-forbidden-owner-facing-language",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const NEVER_WRITE = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library status",
  "Registry approval/status",
  "Brand Status",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
]);

const BRAND_NAMES = Object.freeze({
  "voco-hotels": "Voco Hotels",
  "avid-hotels": "avid hotels",
  "holiday-inn-express": "Holiday Inn Express",
  "vignette-collection": "Vignette Collection",
  "small-luxury-hotels-of-the-world": "Small Luxury Hotels of the World",
  "suburban-studios": "Suburban Studios",
  "trademark-collection-by-wyndham": "Trademark Collection by Wyndham",
  "woodspring-suites": "WoodSpring Suites",
  ascend: "Ascend Hotel Collection",
  "comfort-inn-suites": "Comfort Inn & Suites",
  "country-inn-suites": "Country Inn & Suites by Choice",
  "everhome-suites": "Everhome Suites",
  "hotel-indigo": "Hotel Indigo",
  kimpton: "Kimpton Hotels",
  "mgallery-collection": "MGallery Collection",
  "preferred-hotels-and-resorts": "Preferred Hotels & Resorts",
  "quality-inn": "Quality Inn",
  radisson: "Radisson by Choice",
  "radisson-blu": "Radisson Blu by Choice",
  "radisson-individuals-by-choice": "Radisson Individuals by Choice",
  "bw-premier-collection": "BW Premier Collection",
});

const SIBLING_RE = Object.freeze({
  "voco-hotels": /hotel-indigo|kimpton|vignette|holiday-inn(?!-express)|avid|even-/i,
  "avid-hotels": /holiday-inn(?!-express)|even-|voco|hotel-indigo/i,
  "holiday-inn-express": /holiday-inn(?!-express)|avid|even-|voco|club-vacations/i,
  "vignette-collection": /hotel-indigo|kimpton|voco|holiday-inn|intercontinental|crowne|even-/i,
});

const ADR_REPLACEMENTS = Object.freeze([
  {
    re: /\bbefore modeling peer luxury ADR\b/gi,
    replace: "before underwriting peer luxury demand for the asset",
  },
  {
    re: /\bwhen place authenticity drives ADR and affiliation monetizes that demand\b/gi,
    replace: "when place authenticity drives guest demand and affiliation monetizes that demand",
  },
  {
    re: /\boutrun pure transient economy ADR\b/gi,
    replace: "outrun pure transient economy rate assumptions",
  },
  {
    re: /\bbefore modeling peer ADR\b/gi,
    replace: "before underwriting peer demand comps",
  },
  {
    re: /\bbefore modeling ADR from newer peers\b/gi,
    replace: "before underwriting demand comps from newer peers",
  },
  {
    re: /\bagainst realistic ADR\b/gi,
    replace: "against realistic rate support",
  },
  {
    re: /\bagainst realistic corridor ADR\b/gi,
    replace: "against realistic corridor rate support",
  },
  {
    re: /\bcorridor['’]s ADR support\b/gi,
    replace: "corridor's rate support",
  },
  {
    re: /\bmodeled on transient ADR that the product will not earn\b/gi,
    replace: "modeled on transient rates the product will not earn",
  },
  {
    re: /\bcan justify upper-upscale ADR\b/gi,
    replace: "can justify upper-upscale rate support",
  },
  {
    re: /\bcan carry premium ADR\b/gi,
    replace: "can carry premium rate support",
  },
  {
    re: /\bimports hard-brand ADR comps\b/gi,
    replace: "imports hard-brand comps",
  },
  {
    re: /\bimports primary-market ADR comps\b/gi,
    replace: "imports primary-market comps",
  },
  {
    re: /\btransient peak ADR the product will not earn\b/gi,
    replace: "transient peak rates the product will not earn",
  },
  {
    re: /\bConfirm fee stack and operator depth\b/gi,
    replace: "Confirm affiliation economics and operator depth",
  },
  { re: /\bfee stack\b/gi, replace: "affiliation economics" },
  { re: /\bparticipation cost categories\b/gi, replace: "affiliation economics" },
  // Fallback: strip remaining ADR tokens without spelling the metric acronym back in.
  { re: /\bADR\b/g, replace: "rate support" },
  { re: /\badr\b/g, replace: "rate support" },
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const FIXTURES = path.join(ROOT, "fixtures");
const REPORTS = path.join(ROOT, "reports");
const DOCS = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function checkFlags(argv = [], apply = false) {
  const missing = APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: !!apply,
    ok: !apply || missing.length === 0,
    missing,
    required: [...APPLY_FLAGS],
  };
}

function resolveIdentity(slug) {
  const factory = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  /** Active-universe ids from quiet PVQL when resolveActiveUniverseRecordId is incomplete. */
  const ACTIVE_ID_FALLBACK = Object.freeze({
    ascend: "reclkgOzvAcBheUSo",
    "comfort-inn-suites": "recOzH5iAE1xEjyD0",
    "country-inn-suites": "recaayt9u7YYg8h7Y",
    "everhome-suites": "recqkkrsevi4r9ibj",
    "hotel-indigo": "recegXrqaPiSLGCIe",
    kimpton: "recCKuXCmGvxHPfb3",
    "mgallery-collection": "recrWCD1LMqu864oU",
    "preferred-hotels-and-resorts": "recwl5JOYxlChuCAr",
    "quality-inn": "recd8o4k1JddhkRWW",
    radisson: "recywbx1YQSTCPqW1",
    "radisson-blu": "recWPEvxBQxVVzSq3",
    "radisson-individuals-by-choice": "recRyvM8OmLlDj9G7",
    "bw-premier-collection": "recwXZ5gVZ8ZH8ekA",
  });
  const recordId =
    factory?.recordId || resolveActiveUniverseRecordId(slug) || ACTIVE_ID_FALLBACK[slug] || null;
  const name = factory?.name || BRAND_NAMES[slug] || slug;
  if (!recordId) throw new Error(`Missing recordId for ${slug}`);
  return { slug, recordId, name };
}

function loadPool(slug) {
  const candidates = [
    path.join(FIXTURES, `wave12-${slug}-gallery-pool.json`),
    path.join(FIXTURES, `lane2-${slug}-gallery-pool.json`),
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      const raw = JSON.parse(fs.readFileSync(p, "utf8"));
      return Array.isArray(raw) ? raw : raw.assets || raw.pool || [];
    }
  }
  return [];
}

const BRAND_URL_HINT = Object.freeze({
  "voco-hotels": /voco/i,
  "avid-hotels": /avid/i,
  "holiday-inn-express": /holiday-inn-express|hiex/i,
  "vignette-collection": /vignette/i,
});

function rejectedPoolAsset(url, slug) {
  const u = nz(url);
  if (!u) return "empty_url";
  if (isLogoImageUrl(u) || /fmt=png-alpha|Stays\?/i.test(u)) return "logo_or_placeholder";
  if (isGenericBrandOrLifestyleImageUrl(u)) return "generic_brand_image";
  if (/1600x560|app-mktg|qr1|ribbon|download|logo|wordmark|icon|offer-tile|golf-team|afternoon-tea/i.test(u)) {
    return "marketing_or_non_property_asset";
  }
  const sib = SIBLING_RE[slug];
  if (sib && sib.test(u)) return "sibling_or_wrong_brand_url";
  const hint = BRAND_URL_HINT[slug];
  if (hint && !hint.test(u)) return "missing_brand_url_hint";
  return null;
}

function identityForRow(row) {
  return buildImageIdentity(row.imageUrl, {
    slotKey: row.slotKey,
    title: row.title,
    filename: row.imageFilename || row.filename,
    propertyName: row.propertyName,
  });
}

function galleryRoleCaption(index, asset) {
  const role =
    nz(asset.assignedRole) ||
    detectVisualCategory({
      imageUrl: asset.imageUrl,
      title: asset.caption || asset.propertyName || "",
    }).category;
  const caption =
    (role && GALLERY_ROLE_CAPTIONS[role]) ||
    GALLERY_ROLE_CAPTIONS[DEFAULT_GALLERY_ROLE_SEQUENCE[index]] ||
    "Property photography";
  return asset.propertyName ? `${caption} — ${asset.propertyName}` : caption;
}

function planImageLane(slug, rows) {
  const uniqueness = evaluateImageUniqueness({ presentationRows: rows, brandSlug: slug });
  const patches = [];
  const usedGroupIds = [];
  for (const row of rows) {
    if (!row.imageUrl) continue;
    if (
      !String(row.slotKey || "").startsWith("materials.gallery.") &&
      !String(row.slotKey || "").startsWith("overview.scenario.") &&
      row.slotKey !== "footprint.openings"
    ) {
      continue;
    }
    usedGroupIds.push(identityForRow(row).duplicateGroupId);
  }

  // Keep first occurrence in each duplicate group within a section; replace subsequent slots.
  // Uniqueness is section-scoped (gallery / scenario / openings) — do not cross-collapse.
  const replaceSlots = [];
  for (const section of ["gallery", "scenario", "property_example"]) {
    const seen = new Set();
    const list =
      section === "gallery"
        ? uniqueness.gallery || []
        : section === "scenario"
          ? uniqueness.scenarios || []
          : uniqueness.properties || [];
    for (const img of list) {
      const key = img.duplicateGroupId;
      if (!key || !img.imageUrl) continue;
      if (!seen.has(key)) {
        seen.add(key);
        continue;
      }
      replaceSlots.push({
        section,
        slotKey: img.slotKey,
        recordId: img.recordId,
        title: img.title,
        duplicateGroupId: key,
        currentImageUrl: img.imageUrl,
      });
    }
  }

  // Also ensure we can fill to mins if distinct counts are short (missing slots).
  const needGallery = Math.max(0, GALLERY_DISTINCT_MIN - (uniqueness.galleryDistinctCount || 0));
  const needScenario = Math.max(0, SCENARIO_DISTINCT_MIN - (uniqueness.scenarioDistinctCount || 0));
  const needProperty = Math.max(0, PROPERTY_DISTINCT_MIN - (uniqueness.propertyExampleDistinctCount || 0));

  let pool = loadPool(slug)
    .map((a) => ({ ...a, imageUrl: nz(a.imageUrl) }))
    .filter((a) => a.imageUrl && !rejectedPoolAsset(a.imageUrl, slug));

  // Wave12 brands: prefer pack candidates already Scene7-aware distinct.
  if (IMAGE_TARGETS.includes(slug) && slug !== "vignette-collection") {
    try {
      const pack = buildWave12ImageAssetPackForBrand(slug);
      const cand = [
        ...(pack.visualAssetPack?.galleryCandidates || []),
        ...(pack.visualAssetPack?.scenarioCandidates || []),
        ...(pack.visualAssetPack?.propertyExampleCandidates || []),
      ];
      if (cand.length) pool = [...cand, ...pool];
    } catch {
      /* keep fixture pool */
    }
  }

  const exclude = [...new Set(usedGroupIds.filter(Boolean))];

  for (const target of replaceSlots) {
    // Free the duplicate's group from "used" only for the slot being replaced — still exclude keepers.
    const excludeForPick = exclude.filter((g) => g !== target.duplicateGroupId);
    // Actually keepers still use that group — must exclude it so we don't re-pick same asset.
    const picked = pickDistinctImageAssets(pool, 1, { excludeGroupIds: exclude });
    if (!picked.length) {
      patches.push({
        brandSlug: slug,
        lane: "image_uniqueness",
        slotKey: target.slotKey,
        recordId: target.recordId,
        blocked: true,
        reason: "no_distinct_replacement_in_pool",
        before: { imageUrl: target.currentImageUrl, title: target.title },
      });
      continue;
    }
    const asset = picked[0];
    exclude.push(asset._imageIdentity.duplicateGroupId);
    const fields = {
      Image: [{ url: toAirtableFetchableImageUrl(asset.imageUrl) }],
    };
    if (String(target.slotKey || "").startsWith("materials.gallery.")) {
      const idx = Math.max(0, Number(String(target.slotKey).match(/(\d+)$/)?.[1] || 1) - 1);
      fields.Title = galleryRoleCaption(idx, asset);
    }
    patches.push({
      brandSlug: slug,
      lane: "image_uniqueness",
      table: PRESENTATION_TABLE,
      slotKey: target.slotKey,
      recordId: target.recordId,
      fields,
      before: { imageUrl: target.currentImageUrl, title: target.title },
      after: { imageUrl: asset.imageUrl, title: fields.Title || target.title },
      reason: "replace_near_duplicate_with_scene7_aware_distinct_asset",
      duplicateGroupId: target.duplicateGroupId,
      replacementGroupId: asset._imageIdentity.duplicateGroupId,
    });
  }

  return {
    slug,
    uniquenessBefore: {
      pass: uniqueness.pass === true,
      galleryDistinctCount: uniqueness.galleryDistinctCount,
      scenarioDistinctCount: uniqueness.scenarioDistinctCount,
      propertyExampleDistinctCount: uniqueness.propertyExampleDistinctCount,
      duplicateGroups: uniqueness.duplicateGroups || [],
    },
    replaceSlotCount: replaceSlots.length,
    needGallery,
    needScenario,
    needProperty,
    patches: patches.filter((p) => !p.blocked),
    blockedPatches: patches.filter((p) => p.blocked),
  };
}

function scrubAdrText(text) {
  let out = nz(text);
  if (!out) return out;
  for (const rule of ADR_REPLACEMENTS) {
    out = out.replace(rule.re, rule.replace);
  }
  return out.replace(/[ \t]{2,}/g, " ").replace(/\s+\./g, ".").trim();
}

function planAdrLane(slug, rows) {
  const patches = [];
  for (const row of rows) {
    if (row.active === false) continue;
    if (/do not display|internal only/i.test(nz(row.externalDisplayStatus))) continue;
    const fields = {};
    const before = {};
    const after = {};
    for (const [key, airtableField] of [
      ["title", "Title"],
      ["body", "Body"],
      ["caseSummaryOverview", "Case Summary Overview"],
      ["caseSummaryBrandRelevance", "Case Summary Brand Relevance"],
      ["caseSummaryOwnerObjective", "Case Summary Owner Objective"],
      ["caseSummaryInterpretation", "Case Summary Interpretation"],
      ["caseSummaryTags", "Case Summary Tags"],
    ]) {
      const raw = nz(row[key]);
      if (!raw) continue;
      if (!/\badr\b/i.test(raw) && !/\bfee stack\b/i.test(raw)) continue;
      const cleaned = scrubAdrText(raw);
      if (cleaned === raw) continue;
      const forbidden = scanForbiddenLanguage(cleaned).filter((h) => h.id === "adr");
      const stillFee = /\bfee stack\b/i.test(cleaned);
      if (forbidden.length || stillFee) {
        // last-resort strip
        const harder = cleaned
          .replace(/\badr\b/gi, "rate support")
          .replace(/\bfee stack\b/gi, "affiliation economics")
          .replace(/\bparticipation cost categories\b/gi, "affiliation economics");
        fields[airtableField] = harder;
        before[airtableField] = raw;
        after[airtableField] = harder;
      } else {
        fields[airtableField] = cleaned;
        before[airtableField] = raw;
        after[airtableField] = cleaned;
      }
    }
    if (!Object.keys(fields).length) continue;
    const joined = Object.values(fields).join("\n");
    const still = scanForbiddenLanguage(joined).filter((h) =>
      /adr|revpar|fdd|item_?19|loi|fee_?stack|raw_url/i.test(h.id || h.label || "")
    );
    if (/\bfee stack\b/i.test(joined)) {
      still.push({ id: "fee_stack", label: "fee stack" });
    }
    patches.push({
      brandSlug: slug,
      lane: "adr_forbidden_language",
      table: PRESENTATION_TABLE,
      slotKey: row.slotKey,
      recordId: row.recordId,
      fields,
      before,
      after,
      reason: "scrub_forbidden_adr_owner_facing_copy",
      remainingForbidden: still.map((h) => h.id || h.label),
      blocked: still.length > 0,
    });
  }
  return {
    slug,
    patches: patches.filter((p) => !p.blocked),
    blockedPatches: patches.filter((p) => p.blocked),
  };
}

async function patchPresentation({ recordId, fields }) {
  for (const k of Object.keys(fields)) {
    if (NEVER_WRITE.includes(k)) throw new Error(`Refuse forbidden field ${k}`);
  }
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE credentials required");
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  let lastErr = null;
  for (let attempt = 1; attempt <= 8; attempt++) {
    const res = await fetch(url, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields }),
    });
    const json = await res.json().catch(() => ({}));
    if (res.ok) return json;
    lastErr = new Error(json.error?.message || `PATCH failed ${res.status}`);
    if (!(res.status === 429 || res.status >= 500) || attempt === 8) break;
    await sleep(Math.min(30_000, 800 * 2 ** (attempt - 1)));
  }
  throw lastErr;
}

function writeReports(report) {
  fs.mkdirSync(REPORTS, { recursive: true });
  fs.mkdirSync(DOCS, { recursive: true });
  const base = "brand-explorer-39-final-freeze-blocker-cleanup";
  fs.writeFileSync(path.join(REPORTS, `${base}.json`), `${JSON.stringify(report, null, 2)}\n`);
  const md = renderSummaryMd(report);
  fs.writeFileSync(path.join(REPORTS, `${base}.md`), md);
  fs.writeFileSync(path.join(DOCS, `${base}.md`), md);
  fs.writeFileSync(
    path.join(REPORTS, "brand-explorer-39-image-uniqueness-remediation.md"),
    renderLaneMd(report, "image_uniqueness")
  );
  fs.writeFileSync(
    path.join(REPORTS, "brand-explorer-39-adr-forbidden-language-scrub.md"),
    renderLaneMd(report, "adr_forbidden_language")
  );
}

function renderLaneMd(report, lane) {
  const lines = [
    `# Brand Explorer 39 — ${lane}`,
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.applyPerformed ? "APPLY" : "dry-run"}**`,
    "",
  ];
  for (const [slug, row] of Object.entries(report.brands || {})) {
    if (row.lane !== lane && !(row.lanes || []).includes(lane)) continue;
    lines.push(`## ${slug}`);
    lines.push("");
    const patches = (row.patches || []).filter((p) => p.lane === lane);
    lines.push(`Patches: **${patches.length}**`);
    for (const p of patches) {
      lines.push(`- \`${p.slotKey}\` (\`${p.recordId}\`) — ${p.reason}`);
    }
    lines.push("");
  }
  return `${lines.join("\n")}\n`;
}

function renderSummaryMd(report) {
  const lines = [
    `# Brand Explorer 39 — Final Freeze Blocker Cleanup`,
    "",
    `Version: \`${report.version}\` · Generated: ${report.generatedAt}`,
    `Mode: **${report.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${!!report.writePerformed}**`,
    "",
    `## Targets`,
    "",
    `- Image uniqueness: ${IMAGE_TARGETS.join(", ")}`,
    `- ADR / forbidden-language scrub (primary): ${ADR_TARGETS_PRIMARY.join(", ")}`,
    `- ADR / forbidden-language scrub (residual PVQL/display): ${ADR_TARGETS_RESIDUAL.join(", ")}`,
    "",
    `## Patch counts`,
    "",
    `| Brand | Lane | Planned | Applied | Blocked |`,
    `| --- | --- | ---: | ---: | ---: |`,
  ];
  for (const slug of [...IMAGE_TARGETS, ...ADR_TARGETS]) {
    const b = report.brands[slug];
    if (!b) continue;
    lines.push(
      `| ${slug} | ${b.lane} | ${b.plannedCount || 0} | ${b.appliedCount || 0} | ${b.blockedCount || 0} |`
    );
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  for (const [k, v] of Object.entries(report.guardrails || {})) {
    lines.push(`- ${k}: ${v}`);
  }
  lines.push("");
  lines.push("## Freeze readiness statement");
  lines.push("");
  lines.push(report.readyStatement || "_pending post-apply validation_");
  lines.push("");
  return `${lines.join("\n")}\n`;
}

/**
 * @param {{ apply?: boolean, argv?: string[] }} opts
 */
export async function run39FinalFreezeBlockerCleanup({ apply = false, argv = [] } = {}) {
  const flagCheck = checkFlags(argv, apply);
  if (apply && !flagCheck.ok) {
    throw new Error(`Missing apply flags: ${flagCheck.missing.join(", ")}`);
  }

  const brands = {};
  const allPatches = [];

  for (const slug of IMAGE_TARGETS) {
    const id = resolveIdentity(slug);
    const { rows } = await listPresentationRowsLight(id.recordId, id.name);
    const plan = planImageLane(slug, rows);
    brands[slug] = {
      ...id,
      lane: "image_uniqueness",
      lanes: ["image_uniqueness"],
      plannedCount: plan.patches.length,
      blockedCount: plan.blockedPatches.length,
      appliedCount: 0,
      uniquenessBefore: plan.uniquenessBefore,
      patches: plan.patches,
      blockedPatches: plan.blockedPatches,
    };
    allPatches.push(...plan.patches);
  }

  for (const slug of ADR_TARGETS) {
    const id = resolveIdentity(slug);
    const { rows } = await listPresentationRowsLight(id.recordId, id.name);
    const plan = planAdrLane(slug, rows);
    brands[slug] = {
      ...id,
      lane: "adr_forbidden_language",
      lanes: ["adr_forbidden_language"],
      plannedCount: plan.patches.length,
      blockedCount: plan.blockedPatches.length,
      appliedCount: 0,
      patches: plan.patches,
      blockedPatches: plan.blockedPatches,
    };
    allPatches.push(...plan.patches);
  }

  let writePerformed = false;
  if (apply) {
    for (const p of allPatches) {
      if (!p.recordId || !p.fields) continue;
      if (p.lane === "image_uniqueness" && !IMAGE_TARGETS.includes(p.brandSlug)) {
        throw new Error(`Refuse image write for non-image target ${p.brandSlug}`);
      }
      if (p.lane === "adr_forbidden_language" && !ADR_TARGETS.includes(p.brandSlug)) {
        throw new Error(`Refuse copy write for non-ADR target ${p.brandSlug}`);
      }
      if (p.lane === "image_uniqueness" && !p.fields.Image) {
        throw new Error(`Image lane patch missing Image field for ${p.slotKey}`);
      }
      if (p.lane === "adr_forbidden_language" && p.fields.Image) {
        throw new Error(`ADR lane must not write Image for ${p.slotKey}`);
      }
      await patchPresentation({ recordId: p.recordId, fields: p.fields });
      writePerformed = true;
      brands[p.brandSlug].appliedCount = (brands[p.brandSlug].appliedCount || 0) + 1;
      await sleep(250);
    }
  }

  const report = {
    version: FINAL_39_FREEZE_BLOCKER_CLEANUP_VERSION,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed: !!apply,
    writePerformed,
    dryRun: !apply,
    flagCheck,
    imageTargets: [...IMAGE_TARGETS],
    adrTargets: [...ADR_TARGETS],
    plannedPatchCount: allPatches.length,
    brands,
    guardrails: {
      targetBrandsOnly: true,
      imageWritesOnlyForUniquenessTargets: true,
      copyWritesOnlyForAdrTargets: true,
      scene7FilenameAwareDistinctImages: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      brandStatusWrites: false,
      releaseFieldWrites: false,
      publicRestoreRegistryWrites: false,
      neverWriteFields: NEVER_WRITE.join(", "),
    },
    readyStatement: apply
      ? "cleanup_applied_pending_quiet_sequential_validation"
      : "dry_run_only_no_writes",
  };

  writeReports(report);
  return report;
}
