/**
 * Wave 12 Stage 6 — post-image content + Recent Momentum evidence cleanup.
 *
 * Targeted Presentation / limited Basics patches only. No images (except caption
 * if flagged), Brand Status, release, CV, Source Library, Registry, or protected 27.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import {
  isFlexibilitySlotKey,
  sanitizeFlexibilityPresentationBody,
} from "../brand-explorer-flexibility-levels.mjs";
import { TAB_FACTORY_PROTECTED_BRANDS } from "./brand-explorer-tab-contracts.js";
import { BUILT_BLOCKED_PROTECTED_PUBLIC_FULL } from "./brand-explorer-built-blocked-content.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE12_VERSION,
  WAVE12_SLUGS,
  WAVE12_FORBIDDEN_WRITE_FIELDS,
} from "./brand-explorer-wave12-factory-plan.js";
import {
  generateWave12TabFactoryPack,
  scrubWave12StubChips,
} from "./brand-explorer-wave12-tab-factory-build-generator.js";
import { EXPECTED_ACTIVE_COUNT_27 } from "./brand-explorer-27-active-public-full-baseline.js";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";

export const WAVE12_POST_IMAGE_CONTENT_CLEANUP_VERSION =
  "wave12-post-image-content-cleanup-v1";

export const WAVE12_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave12-post-image-content-cleanup",
  "--confirm-target-brands-only",
  "--confirm-targeted-field-fixes-only",
  "--confirm-recent-momentum-and-openings-quality",
  "--confirm-cala-first-priority",
  "--confirm-international-reference-labels-where-needed",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-protected-27-brand-changes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-image-writes-except-caption-only-if-flagged",
  "--confirm-no-broad-rewrites",
  "--confirm-no-raw-urls",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS_TABLE = "Brand Setup - Brand Basics";

const ALLOWED_BASICS_FIELDS = new Set([
  "Brand Positioning",
  "Guest Psychographics Description",
  "Brand Value Proposition",
  "Key Brand Differentiators",
]);

const FORBIDDEN_WRITE_FIELDS = new Set([
  ...WAVE12_FORBIDDEN_WRITE_FIELDS,
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Brand Status",
  "Image",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
]);

const URL_ALLOWED_SLOTS = new Set(["footprint.momentum", "footprint.openings"]);

/** Slots commonly thin / pattern-failing after Stage 5 (from Wave 12 audit). */
const PRIORITY_SLOTS = new Set([
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
  "overview.proof.1",
  "overview.proof.2",
  "overview.proof.3",
  "overview.proof.4",
  "overview.differentiators.identity",
  "overview.differentiators.commercial",
  "overview.relative_positioning",
  "overview.bestAt.1",
  "overview.portfolio_context",
  "valueOwners.lifecycle.1",
  "valueOwners.lifecycle.2",
  "valueOwners.lifecycle.3",
  "valueOwners.lifecycle.4",
  "valueOwners.lifecycle.5",
  "valueOwners.lifecycle.6",
  "economics.opening.step.1",
  "economics.opening.step.2",
  "economics.opening.step.3",
  "economics.opening.step.4",
  "economics.opening.step.5",
  "operations.operator_compat.summary",
  "operations.operator_compat.fit",
  "operations.standards_philosophy",
  "footprint.growth_fit",
  "footprint.portfolio_mix",
  "footprint.momentum",
  "footprint.momentum_label",
  "footprint.openings",
  "Brand Positioning",
]);

const STUB_RE =
  /\b(conversion-friendly\.?|neighborhood focus|boutique design)\b/i;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function hasStub(text) {
  return STUB_RE.test(nz(text));
}

function scrubAll(text) {
  return scrubWave12StubChips(text);
}

export function parseWave12PostImageContentCleanupFlags(argv = []) {
  const missing = WAVE12_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS.filter(
    (f) => !argv.includes(f)
  );
  return {
    ok: missing.length === 0,
    missing,
    required: [...WAVE12_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS],
  };
}

function resolveWave12Identity(slug) {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!id?.recordId || !id?.name) {
    throw new Error(`Missing factory-preview identity for ${slug}`);
  }
  return { slug, recordId: id.recordId, name: id.name };
}

async function fetchBasicsFields(recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey || !recordId) return {};
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Basics get failed ${res.status}`);
  const f = json.fields || {};
  return {
    "Brand Positioning": nz(f["Brand Positioning"]),
    "Guest Psychographics Description": nz(f["Guest Psychographics Description"]),
    "Brand Value Proposition": nz(f["Brand Value Proposition"]),
    "Key Brand Differentiators": nz(f["Key Brand Differentiators"]),
  };
}

async function airtableWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const maxAttempts = 8;
  let lastErr = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const res = await fetch(url, {
      method,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields }),
    });
    const json = await res.json().catch(() => ({}));
    if (res.ok) return json;
    const msg = json.error?.message || `${method} ${table} failed: ${res.status}`;
    lastErr = new Error(msg);
    const retryable = res.status === 429 || res.status >= 500;
    if (!retryable || attempt === maxAttempts) break;
    await sleep(Math.min(30_000, 800 * 2 ** (attempt - 1)));
  }
  throw lastErr || new Error(`${method} ${table} failed`);
}

function liveRowsForSlot(rows, slotKey) {
  return (rows || [])
    .filter(
      (r) =>
        nz(r.slotKey) === slotKey &&
        r.active !== false &&
        !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
    )
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
}

function validateOwnerCopy(body, slotKey) {
  let cleaned = scrubAll(body);
  if (isFlexibilitySlotKey(slotKey)) {
    cleaned = sanitizeFlexibilityPresentationBody(cleaned);
  }
  const allowUrls = URL_ALLOWED_SLOTS.has(slotKey);
  const forbidden = scanForbiddenLanguage(cleaned).filter((hit) => {
    if (allowUrls && hit.id === "raw_url") return false;
    return true;
  });
  if (forbidden.length) {
    return {
      ok: false,
      cleaned,
      reason: `forbidden:${forbidden.map((f) => f.id || f.label).join(",")}`,
    };
  }
  if (!allowUrls && /https?:\/\//i.test(cleaned)) {
    return { ok: false, cleaned, reason: "raw_url_not_allowed_slot" };
  }
  return { ok: true, cleaned, reason: null };
}

function shouldPatchContent(slotKey, live, generated) {
  const liveBody = nz(live?.body);
  const liveTitle = nz(live?.title);
  const genBody = nz(generated?.body);
  const genTitle = nz(generated?.title);
  if (!genBody && !genTitle && slotKey !== "footprint.momentum_label") return false;

  const stub = hasStub(liveBody) || hasStub(liveTitle) || hasStub(genBody) || hasStub(genTitle);
  const priority = PRIORITY_SLOTS.has(slotKey);
  const momentumFamily =
    slotKey === "footprint.momentum" ||
    slotKey === "footprint.momentum_label" ||
    slotKey === "footprint.openings";
  const changed = liveBody !== genBody || (genTitle && liveTitle !== genTitle);
  const caseChanged =
    (nz(generated?.caseSummaryTags) &&
      nz(live?.caseSummaryTags) !== nz(generated?.caseSummaryTags)) ||
    (nz(generated?.caseSummaryBrandRelevance) &&
      nz(live?.caseSummaryBrandRelevance) !== nz(generated?.caseSummaryBrandRelevance)) ||
    (nz(generated?.caseSummaryOverview) &&
      nz(live?.caseSummaryOverview) !== nz(generated?.caseSummaryOverview));

  // Openings: always refresh Case Summary geography fields when they conflict with tags.
  if (slotKey === "footprint.openings") {
    const liveRelevance = nz(live?.caseSummaryBrandRelevance);
    const tags = nz(live?.caseSummaryTags) || nz(generated?.caseSummaryTags);
    if (
      /\bCALA\b/i.test(tags) &&
      /international reference/i.test(liveRelevance) &&
      !/\bCALA\b/i.test(liveRelevance)
    ) {
      return true;
    }
    if (
      /international reference/i.test(tags) &&
      /\bCALA\b/i.test(liveRelevance) &&
      !/international reference/i.test(liveRelevance)
    ) {
      return true;
    }
  }

  if (!(stub || priority || momentumFamily)) return false;
  return changed || stub || caseChanged || (momentumFamily && changed);
}

/**
 * Plan targeted post-image content patches for one Wave 12 brand.
 */
export async function planWave12PostImageContentCleanupForBrand(slug) {
  const identity = resolveWave12Identity(slug);
  const blockers = [];

  if (
    BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(slug) ||
    TAB_FACTORY_PROTECTED_BRANDS.includes(slug)
  ) {
    return {
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: ["protected_brand_refuse"],
      patches: [],
      basicsPatches: [],
    };
  }
  if (!WAVE12_SLUGS.includes(slug)) {
    return {
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: ["not_wave12_target"],
      patches: [],
      basicsPatches: [],
    };
  }

  const pack = generateWave12TabFactoryPack(slug, {
    recordId: identity.recordId,
    brandName: identity.name,
  });
  const fetch = await listPresentationRowsLight(identity.recordId, identity.name);
  const rows = fetch.rows || [];
  const basicsBefore = await fetchBasicsFields(identity.recordId);

  const patches = [];
  const usedRecordIds = new Set();

  // Group generated multi-card slots (momentum / openings) in order.
  const genBySlot = new Map();
  for (const row of pack.presentation || []) {
    const slotKey = nz(row.slotKey);
    if (!genBySlot.has(slotKey)) genBySlot.set(slotKey, []);
    genBySlot.get(slotKey).push(row);
  }

  for (const [slotKey, genRows] of genBySlot.entries()) {
    const liveList = liveRowsForSlot(rows, slotKey);
    const max = Math.max(genRows.length, liveList.length);
    for (let i = 0; i < max; i++) {
      const generated = genRows[i];
      const live = liveList[i] || null;
      if (!generated) continue;
      if (!shouldPatchContent(slotKey, live, generated)) {
        if (live?.recordId) usedRecordIds.add(live.recordId);
        continue;
      }

      const titleVal = validateOwnerCopy(nz(generated.title), slotKey);
      const bodyVal = validateOwnerCopy(nz(generated.body), slotKey);
      if (!titleVal.ok || !bodyVal.ok) {
        blockers.push(
          `validation:${slotKey}:${titleVal.reason || bodyVal.reason || "fail"}`
        );
        continue;
      }

      const caseFields = {};
      for (const [api, airtable] of [
        ["caseSummaryOverview", "Case Summary Overview"],
        ["caseSummaryBrandRelevance", "Case Summary Brand Relevance"],
        ["caseSummaryOwnerObjective", "Case Summary Owner Objective"],
        ["caseSummaryInterpretation", "Case Summary Interpretation"],
        ["caseSummaryTags", "Case Summary Tags"],
      ]) {
        if (nz(generated[api])) caseFields[airtable] = scrubAll(nz(generated[api]));
      }

      const fields = {
        Body: bodyVal.cleaned,
        ...(titleVal.cleaned ? { Title: titleVal.cleaned } : {}),
        ...caseFields,
      };
      if (generated.sortOrder != null) {
        fields["Sort Order"] = generated.sortOrder;
      }

      // Never touch Image in this stage.
      for (const k of Object.keys(fields)) {
        if (FORBIDDEN_WRITE_FIELDS.has(k) || k === "Image") {
          delete fields[k];
        }
      }

      if (live?.recordId && !usedRecordIds.has(live.recordId)) {
        usedRecordIds.add(live.recordId);
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: live.recordId,
          slotKey,
          fields,
          before: {
            body: live.body,
            title: live.title,
            sortOrder: live.sortOrder,
            caseSummaryTags: live.caseSummaryTags,
          },
          after: {
            body: fields.Body,
            title: fields.Title || "",
            sortOrder: fields["Sort Order"],
          },
          reason: hasStub(live.body) || hasStub(live.title)
            ? "stub_chip_scrub_or_thicken"
            : PRIORITY_SLOTS.has(slotKey)
              ? "priority_slot_thicken_or_pattern"
              : "momentum_openings_quality",
        });
      } else if (!live) {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "POST",
          recordId: null,
          slotKey,
          fields: {
            "Slot Key": slotKey,
            "Brand Name": identity.name,
            Brand: [identity.recordId],
            Active: true,
            "Sort Order": generated.sortOrder ?? 0,
            Title: fields.Title || "",
            Body: fields.Body,
            ...caseFields,
          },
          before: null,
          after: { body: fields.Body, title: fields.Title || "" },
          reason: "missing_priority_slot_create",
        });
      }
    }
  }

  // Scrub any remaining live rows that still carry stub chips (even outside priority).
  for (const live of rows) {
    if (!live?.recordId || usedRecordIds.has(live.recordId)) continue;
    const slotKey = nz(live.slotKey);
    if (!hasStub(live.body) && !hasStub(live.title) && !hasStub(live.caseSummaryTags)) {
      continue;
    }
    const bodyVal = validateOwnerCopy(scrubAll(live.body), slotKey);
    const titleVal = validateOwnerCopy(scrubAll(live.title), slotKey);
    if (!bodyVal.ok || !titleVal.ok) {
      blockers.push(`stub_scrub_validation:${slotKey}:${bodyVal.reason || titleVal.reason}`);
      continue;
    }
    usedRecordIds.add(live.recordId);
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: live.recordId,
      slotKey,
      fields: {
        Body: bodyVal.cleaned,
        ...(titleVal.cleaned ? { Title: titleVal.cleaned } : {}),
        ...(hasStub(live.caseSummaryTags)
          ? { "Case Summary Tags": scrubAll(live.caseSummaryTags) }
          : {}),
      },
      before: { body: live.body, title: live.title },
      after: { body: bodyVal.cleaned, title: titleVal.cleaned },
      reason: "residual_stub_chip_scrub",
    });
  }

  const basicsPatches = [];
  const basicsFields = pack.basicsFields || {};
  const nextBasics = {};
  for (const field of ALLOWED_BASICS_FIELDS) {
    const before = nz(basicsBefore[field]);
    let after = nz(basicsFields[field]);
    if (!after && hasStub(before)) after = scrubAll(before);
    else if (after) after = scrubAll(after);
    if (!after) continue;
    if (before === after && !hasStub(before)) continue;
    if (hasStub(before) || PRIORITY_SLOTS.has(field) || before !== after) {
      const val = validateOwnerCopy(after, field);
      if (!val.ok) {
        blockers.push(`basics_validation:${field}:${val.reason}`);
        continue;
      }
      nextBasics[field] = val.cleaned;
    }
  }
  // Always scrub stub-bearing basics even if pack omitted the field.
  for (const field of ALLOWED_BASICS_FIELDS) {
    if (nextBasics[field]) continue;
    const before = nz(basicsBefore[field]);
    if (!hasStub(before)) continue;
    const val = validateOwnerCopy(scrubAll(before), field);
    if (!val.ok) {
      blockers.push(`basics_stub_validation:${field}:${val.reason}`);
      continue;
    }
    nextBasics[field] = val.cleaned;
  }
  if (Object.keys(nextBasics).length) {
    basicsPatches.push({
      table: BASICS_TABLE,
      action: "PATCH",
      recordId: identity.recordId,
      fields: nextBasics,
      before: Object.fromEntries(Object.keys(nextBasics).map((k) => [k, basicsBefore[k] ?? null])),
      after: nextBasics,
      reason: "basics_stub_or_positioning_cleanup",
    });
  }

  const hardBlock = blockers.some(
    (b) => b === "protected_brand_refuse" || b === "not_wave12_target"
  );

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    reportSlug: slug,
    blocked: hardBlock,
    blockers,
    patches,
    basicsPatches,
    existingPresentationCount: rows.length,
    plannedPresentationWrites: patches.length,
    plannedBasicsWrites: basicsPatches.length,
    brandStatusUntouched: true,
    releaseFieldsWritten: false,
    imagesWritten: false,
    companyValidatedUntouched: true,
  };
}

export async function applyWave12PostImageContentCleanupPlans({
  brandResults = [],
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseWave12PostImageContentCleanupFlags(argv);
  if (!apply) {
    return {
      applied: false,
      reason: "dry_run",
      flagCheck,
      resultsByBrand: {},
    };
  }
  if (!flagCheck.ok) {
    return {
      applied: false,
      reason: "missing_apply_flags",
      flagCheck,
      resultsByBrand: {},
    };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of brandResults) {
    if (brand.blocked && (brand.blockers || []).includes("protected_brand_refuse")) {
      resultsByBrand[brand.brandSlug] = {
        applied: false,
        reason: "blocked",
        blockers: brand.blockers,
      };
      continue;
    }
    if (!WAVE12_SLUGS.includes(brand.brandSlug)) {
      throw new Error(`Refusing non-target brand write: ${brand.brandSlug}`);
    }

    const created = [];
    const updated = [];
    const basicsUpdated = [];
    const errors = [];

    for (const patch of [...(brand.basicsPatches || []), ...(brand.patches || [])]) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_WRITE_FIELDS.has(key) || key === "Image") {
          throw new Error(`Forbidden field write: ${key}`);
        }
        if (patch.table === BASICS_TABLE && !ALLOWED_BASICS_FIELDS.has(key)) {
          throw new Error(`Forbidden Brand Basics field: ${key}`);
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
          created.push(json.id);
        } else {
          await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            recordId: patch.recordId,
            fields: patch.fields,
            method: "PATCH",
          });
          if (patch.table === BASICS_TABLE) basicsUpdated.push(patch.recordId);
          else updated.push(patch.recordId);
        }
        await sleep(280);
      } catch (err) {
        errors.push({
          table: patch.table,
          slotKey: patch.slotKey || Object.keys(patch.fields || {})[0],
          error: err?.message || String(err),
        });
      }
    }

    resultsByBrand[brand.brandSlug] = {
      applied: errors.length === 0,
      created,
      updated,
      basicsUpdated,
      errors,
      releaseFieldsWritten: false,
      brandStatusUntouched: true,
      companyValidatedUntouched: true,
      imagesWritten: false,
    };
  }

  return {
    applied: Object.values(resultsByBrand).some((r) => r.applied),
    reason: "wave12_post_image_content_cleanup_applied",
    flagCheck,
    resultsByBrand,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    brandStatusUntouched: true,
    releaseFieldsWritten: false,
    imagesWritten: false,
    protected27Untouched: true,
  };
}

function brandMd(brand, applyResult = null) {
  const appliedBrand = applyResult?.resultsByBrand?.[brand.brandSlug];
  return [
    `# Wave 12 Post-Image Content Cleanup — ${brand.brandName || brand.brandSlug}`,
    ``,
    `- Slug: \`${brand.brandSlug}\``,
    `- Record: \`${brand.recordId || "—"}\``,
    `- Presentation patches: **${brand.patches?.length ?? 0}**`,
    `- Basics patches: **${brand.basicsPatches?.length ?? 0}**`,
    `- Blocked: **${brand.blocked === true}**`,
    `- Applied: **${appliedBrand?.applied === true}**`,
    `- Brand Status untouched: **true**`,
    `- Images written: **false**`,
    `- Company Validated untouched: **true**`,
    ``,
    `## Blockers`,
    ``,
    ...(brand.blockers?.length ? brand.blockers.map((b) => `- ${b}`) : ["- (none)"]),
    ``,
    `## Presentation patches`,
    ``,
    ...((brand.patches || []).map(
      (p) =>
        `- \`${p.action}\` \`${p.slotKey}\`${p.recordId ? ` (${p.recordId})` : ""} · ${p.reason || ""}`
    ) || ["- (none)"]),
    ``,
    `## Basics patches`,
    ``,
    ...((brand.basicsPatches || []).map(
      (p) => `- \`${p.action}\` fields: ${Object.keys(p.fields || {}).join(", ")}`
    ) || ["- (none)"]),
    ``,
  ].join("\n");
}

export function writeWave12PostImageContentCleanupReports(plan, applyResult = null) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  for (const brand of plan.brandResults || []) {
    const mdPath = path.join(
      REPORTS_DIR,
      `brand-explorer-wave12-post-image-content-cleanup-${brand.reportSlug || brand.brandSlug}.md`
    );
    fs.writeFileSync(mdPath, `${brandMd(brand, applyResult)}\n`, "utf8");
  }

  const report = {
    ...plan,
    dryRun: !applyResult?.applied,
    airtableWrites: applyResult?.applied === true,
    applyResult,
  };
  const jsonPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave12-post-image-content-cleanup.json"
  );
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const mdPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave12-post-image-content-cleanup.md"
  );
  const md = [
    `# Brand Explorer Wave 12 — Post-Image Content Cleanup`,
    ``,
    `Generated: ${plan.generatedAt}`,
    `Dry-run: **${!applyResult?.applied}** · Applied: **${applyResult?.applied === true}**`,
    `Presentation writes planned: **${plan.summary?.plannedPresentationWrites ?? 0}**`,
    `Basics writes planned: **${plan.summary?.plannedBasicsWrites ?? 0}**`,
    `Blocked: **${(plan.summary?.blockedSlugs || []).join(", ") || "none"}**`,
    ``,
    `## Guardrails`,
    ``,
    `- Target Wave 12 brands only`,
    `- No Brand Status / release / CV / Source Library / Registry writes`,
    `- No protected 27 / Radisson Collection`,
    `- No image writes (caption-only only if flagged; none in this pass)`,
    `- Targeted thicken + stub scrub + Recent Momentum / Openings quality`,
    ``,
    `## Brands`,
    ``,
    `| Slug | Name | Pres | Basics | Blocked |`,
    `| --- | --- | ---: | ---: | --- |`,
    ...(plan.brandResults || []).map(
      (b) =>
        `| \`${b.brandSlug}\` | ${b.brandName} | ${b.patches?.length ?? 0} | ${b.basicsPatches?.length ?? 0} | ${b.blocked === true} |`
    ),
    ``,
    `## Apply flags`,
    ``,
    ...WAVE12_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS.map((f) => `- \`${f}\``),
    ``,
  ].join("\n");
  fs.writeFileSync(mdPath, md, "utf8");

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave12-post-image-content-cleanup.md");
  fs.writeFileSync(
    docPath,
    [
      `# Wave 12 Post-Image Content Cleanup`,
      ``,
      `Stage 6 of the Wave 12 factory applies targeted non-image cleanup after Stage 5 image materialization.`,
      ``,
      `## Scope`,
      ``,
      `- Thin owner-facing scenarios / proofs / lifecycle / opening steps`,
      `- Stub chips (\`conversion-friendly\`, \`neighborhood focus\`)`,
      `- Recent Momentum dated cards, CALA-first Sort Order, International Reference labels`,
      `- Openings region labels where needed`,
      ``,
      `## Forbidden`,
      ``,
      `- Brand Status, release fields, Company Validated, Source Library, Registry`,
      `- Protected 27 brands, Radisson Collection, non-target brands`,
      `- Image materialization / broad rewrites`,
      ``,
      `## Commands`,
      ``,
      "```bash",
      "npm run brand-explorer-wave12-factory -- --stage post-image-content-cleanup --dry-run",
      "npm run brand-explorer-wave12-factory -- --stage post-image-content-cleanup --apply \\",
      ...WAVE12_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS.map(
        (f, i, arr) => `  ${f}${i < arr.length - 1 ? " \\" : ""}`
      ),
      "```",
      ``,
      `Protected baseline remains ${EXPECTED_ACTIVE_COUNT_27}.`,
      ``,
    ].join("\n"),
    "utf8"
  );

  return { jsonPath, mdPath, docPath };
}

export async function runWave12PostImageContentCleanup({ dryRun = true, argv = [] } = {}) {
  const flagCheck = parseWave12PostImageContentCleanupFlags(argv);
  const apply = argv.includes("--apply") && !dryRun;
  const brandResults = [];

  for (const slug of WAVE12_SLUGS) {
    const plan = await planWave12PostImageContentCleanupForBrand(slug);
    brandResults.push(plan);
    await sleep(350);
  }

  const applyResult = await applyWave12PostImageContentCleanupPlans({
    brandResults,
    apply,
    argv,
  });

  const summary = {
    version: WAVE12_POST_IMAGE_CONTENT_CLEANUP_VERSION,
    wave12Version: WAVE12_VERSION,
    stage: "post-image-content-cleanup",
    generatedAt: new Date().toISOString(),
    dryRun: !applyResult?.applied,
    applyRequested: argv.includes("--apply"),
    flagCheck,
    protectedBaselineCount: EXPECTED_ACTIVE_COUNT_27,
    plannedPresentationWrites: brandResults.reduce(
      (n, b) => n + (b.patches?.length || 0),
      0
    ),
    plannedBasicsWrites: brandResults.reduce(
      (n, b) => n + (b.basicsPatches?.length || 0),
      0
    ),
    blockedSlugs: brandResults.filter((b) => b.blocked).map((b) => b.brandSlug),
  };

  const plan = {
    ...summary,
    brandResults,
    summary,
  };
  writeWave12PostImageContentCleanupReports(plan, applyResult);
  return { ...plan, applyResult };
}
