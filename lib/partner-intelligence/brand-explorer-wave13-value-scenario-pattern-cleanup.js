/**
 * Wave 13 — Value scenario / visual pattern parity remediation.
 *
 * Fixes:
 * - Value Creation Scenarios (valueOwners.scenario.1–4) — create/patch Title+Body
 * - Where This Brand Creates the Most Value (overview.scenario.1–3) — Title+Body+Image
 *
 * Target: six public Wave 13 brands + SO/ (held Under Review; no status/release writes).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { toAirtableFetchableImageUrl } from "./brand-explorer-lane2-image-materialization.js";
import {
  buildImageIdentity,
  evaluateImageUniqueness,
} from "./brand-explorer-image-uniqueness.js";
import { detectVisualCategory } from "./brand-explorer-image-role-match.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  toProperCaseScenarioTitle,
  SCENARIO_SLOTS,
  SCENARIO_MIN_BODY_WORDS,
} from "./brand-explorer-scenario-owner-value-bar.js";
import {
  VALUE_CREATION_SCENARIO_SLOTS,
  words,
} from "./brand-explorer-value-creation-scenarios-bar.js";
import {
  WAVE13_VERSION,
  WAVE13_PARTIAL_PROMOTION_SLUGS,
  WAVE13_HELD_PROMOTION_SLUG,
  WAVE13_NEVER_WRITE_FIELDS,
  WAVE13_VALUE_SCENARIO_PATTERN_CLEANUP_APPLY_FLAGS,
} from "./brand-explorer-wave13-factory-plan.js";
import {
  WAVE13_VALUE_SCENARIO_PACKAGES_VERSION,
  WAVE13_VALUE_SCENARIO_TARGET_SLUGS,
  getWave13ValueScenarioPackage,
} from "./brand-explorer-wave13-value-scenario-pattern-packages.js";

export const WAVE13_VALUE_SCENARIO_PATTERN_CLEANUP_VERSION =
  "wave13-value-scenario-pattern-cleanup-v1";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const WRITE_THROTTLE_MS = 280;
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");
const FIXTURES = path.join(ROOT, "fixtures");

const FORBIDDEN_VISIBLE_RES = Object.freeze([
  /\bADR\b/,
  /\bRevPAR\b/,
  /fee-?stack/i,
  /\bFDD\b/,
  /Item\s*19/i,
  /\bLOI\b/,
  /https?:\/\//i,
  /owner-fit diligence/i,
  /confirm owner/i,
  /confirm operator/i,
  /source-supported/i,
  /\bfactory\b/i,
  /\bstage\s*\d/i,
  /accor\s*\/\s*ennismore/i,
  /^property fit$/i,
  /^support across lifecycle$/i,
]);

const BAD_OVERVIEW_TITLES = Object.freeze([
  /^property fit$/i,
  /^support across lifecycle$/i,
  /^where this brand creates the most value$/i,
]);

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

function resolveIdentity(slug) {
  return FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug] || null;
}

function findSlot(rows, slotKey) {
  const matches = (rows || [])
    .filter(
      (r) =>
        nz(r.slotKey) === slotKey &&
        r.active !== false &&
        !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
    )
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
  return matches[0] || null;
}

function assertPackageClean(pkg, slug) {
  const issues = [];
  for (const c of [...pkg.valueOwnersScenarios, ...pkg.overviewScenarios]) {
    const blob = `${c.title}\n${c.body}`;
    for (const re of FORBIDDEN_VISIBLE_RES) {
      if (re.test(blob) || re.test(c.title)) {
        issues.push(`${slug}:${c.title}:${re.source}`);
      }
    }
  }
  return issues;
}

function loadGalleryPool(slug) {
  const p = path.join(FIXTURES, `wave13-${slug}-gallery-pool.json`);
  if (!fs.existsSync(p)) return [];
  const raw = JSON.parse(fs.readFileSync(p, "utf8"));
  const list = Array.isArray(raw)
    ? raw
    : raw.assets || raw.images || raw.accepted || raw.pool || [];
  return Array.isArray(list) ? list : [];
}

function pickScenarioImages(slug, overviewCards, liveScenarioRows) {
  const pool = loadGalleryPool(slug);
  const usedGroups = new Set();
  const picks = [];
  const flagged = [];

  for (let i = 0; i < overviewCards.length; i++) {
    const card = overviewCards[i];
    const live = liveScenarioRows[i];
    const liveUrl = nz(live?.imageUrl);
    const liveId = liveUrl
      ? buildImageIdentity(liveUrl, { title: live?.title || "" }).duplicateGroupId
      : null;
    const liveCat = liveUrl
      ? detectVisualCategory({
          imageUrl: liveUrl,
          title: nz(live?.title),
          filename: nz(live?.imageFilename),
        }).category
      : null;

    const wantRole = card.imageRole;
    const liveOk =
      liveUrl &&
      liveId &&
      !usedGroups.has(liveId) &&
      liveCat === wantRole;

    if (liveOk) {
      usedGroups.add(liveId);
      picks.push({
        imageUrl: liveUrl,
        caption: card.imageCaption || live?.title || wantRole,
        role: wantRole,
        reusedLive: true,
        needsWrite: false,
      });
      continue;
    }

    const candidates = pool.filter((a) => {
      const url = nz(a.imageUrl);
      if (!url) return false;
      const id = buildImageIdentity(url, {
        propertyName: a.propertyName,
        title: a.title || "",
      }).duplicateGroupId;
      if (usedGroups.has(id)) return false;
      const cat =
        a.role ||
        detectVisualCategory({
          imageUrl: url,
          title: a.propertyName || a.title || "",
          filename: a.filename,
        }).category;
      return cat === wantRole;
    });

    let chosen = candidates[0];
    if (!chosen) {
      chosen = pool.find((a) => {
        const url = nz(a.imageUrl);
        if (!url) return false;
        const id = buildImageIdentity(url, {
          propertyName: a.propertyName,
        }).duplicateGroupId;
        return !usedGroups.has(id);
      });
    }

    if (!chosen?.imageUrl) {
      flagged.push(`missing_image_for_scenario_${i + 1}_${wantRole}`);
      picks.push({
        imageUrl: liveUrl || null,
        caption: card.imageCaption,
        role: wantRole,
        reusedLive: Boolean(liveUrl),
        needsWrite: false,
      });
      if (liveId) usedGroups.add(liveId);
      continue;
    }

    const nextUrl = chosen.imageUrl;
    const nextId = buildImageIdentity(nextUrl, {
      propertyName: chosen.propertyName,
    }).duplicateGroupId;
    usedGroups.add(nextId);
    const needsWrite = !liveUrl || liveId !== nextId;
    if (needsWrite) flagged.push(`image_role_or_dupe_fix_scenario_${i + 1}`);
    picks.push({
      imageUrl: nextUrl,
      caption: card.imageCaption || chosen.propertyName || wantRole,
      role: wantRole,
      propertyName: chosen.propertyName || null,
      reusedLive: false,
      needsWrite,
    });
  }

  return { picks, flagged };
}

export async function planWave13ValueScenarioPatternCleanupForBrand(slug) {
  const identity = resolveIdentity(slug);
  const pkg = getWave13ValueScenarioPackage(slug);
  if (!identity?.recordId) {
    return { brandSlug: slug, blocked: true, blockers: ["unknown_identity"], patches: [] };
  }
  if (!pkg) {
    return {
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: ["no_package"],
      patches: [],
    };
  }
  if (!WAVE13_VALUE_SCENARIO_TARGET_SLUGS.includes(slug)) {
    return {
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: ["not_wave13_value_scenario_target"],
      patches: [],
    };
  }

  const packageIssues = assertPackageClean(pkg, slug);
  if (packageIssues.length) {
    return {
      brandSlug: slug,
      brandName: pkg.brandName,
      recordId: identity.recordId,
      blocked: true,
      blockers: packageIssues,
      patches: [],
    };
  }

  const fetch = await listPresentationRowsLight(identity.recordId, identity.name);
  const rows = fetch.rows || [];
  const liveOverview = SCENARIO_SLOTS.map((sk) => findSlot(rows, sk));
  const liveValueOwners = VALUE_CREATION_SCENARIO_SLOTS.map((sk) => findSlot(rows, sk));

  const before = {
    overview: liveOverview.map((r) => ({
      title: nz(r?.title),
      body: nz(r?.body).slice(0, 220),
      words: words(r?.body || ""),
      imageUrl: r?.imageUrl || null,
    })),
    valueOwners: liveValueOwners.map((r) => ({
      title: nz(r?.title),
      body: nz(r?.body).slice(0, 180),
      words: words(r?.body || ""),
      present: Boolean(r?.recordId),
    })),
  };

  const { picks: imagePicks, flagged: imageFlags } = pickScenarioImages(
    slug,
    pkg.overviewScenarios,
    liveOverview
  );

  const patches = [];
  const reasons = [];

  for (let i = 0; i < VALUE_CREATION_SCENARIO_SLOTS.length; i++) {
    const slotKey = VALUE_CREATION_SCENARIO_SLOTS[i];
    const live = liveValueOwners[i];
    const next = pkg.valueOwnersScenarios[i];
    const nextTitle = toProperCaseScenarioTitle(next.title);
    const nextBody = nz(next.body);
    const titleNeeds = !live?.recordId || nz(live.title) !== nextTitle;
    const bodyNeeds = !live?.recordId || nz(live.body) !== nextBody;
    if (!titleNeeds && !bodyNeeds) continue;

    if (live?.recordId) {
      const fields = {};
      if (titleNeeds) fields.Title = nextTitle;
      if (bodyNeeds) fields.Body = nextBody;
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: live.recordId,
        slotKey,
        fields,
        before: { title: nz(live.title), body: nz(live.body).slice(0, 160) },
        after: { title: nextTitle, body: nextBody.slice(0, 160) },
        reason: "value_creation_scenario_parity",
      });
    } else {
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
          "Sort Order": i,
          Title: nextTitle,
          Body: nextBody,
        },
        before: null,
        after: { title: nextTitle, body: nextBody.slice(0, 160) },
        reason: "value_creation_scenario_create",
      });
    }
    reasons.push(slotKey);
  }

  for (let i = 0; i < SCENARIO_SLOTS.length; i++) {
    const slotKey = SCENARIO_SLOTS[i];
    const live = liveOverview[i];
    const next = pkg.overviewScenarios[i];
    const img = imagePicks[i];
    const nextTitle = toProperCaseScenarioTitle(next.title);
    const nextBody = nz(next.body);
    const liveTitle = nz(live?.title);
    const liveBody = nz(live?.body);
    const titleNeeds =
      !live?.recordId ||
      liveTitle !== nextTitle ||
      BAD_OVERVIEW_TITLES.some((re) => re.test(liveTitle));
    const bodyNeeds =
      !live?.recordId ||
      liveBody !== nextBody ||
      words(liveBody) < SCENARIO_MIN_BODY_WORDS ||
      FORBIDDEN_VISIBLE_RES.some((re) => re.test(liveBody));

    const fields = {};
    const patchReasons = [];
    if (titleNeeds) {
      fields.Title = nextTitle;
      patchReasons.push("retitle_owner_value_topic");
    }
    if (bodyNeeds) {
      fields.Body = nextBody;
      patchReasons.push("owner_value_body_rewrite");
    }
    if (img?.needsWrite && img.imageUrl && live?.recordId) {
      const fetchable = toAirtableFetchableImageUrl(img.imageUrl);
      if (fetchable) {
        fields.Image = [{ url: fetchable }];
        patchReasons.push("scenario_image_role_match");
        if (img.caption) fields.Title = fields.Title || nextTitle;
      }
    }

    for (const forbidden of WAVE13_NEVER_WRITE_FIELDS) {
      if (fields[forbidden] != null) delete fields[forbidden];
    }

    if (!Object.keys(fields).length && live?.recordId) continue;

    if (live?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: live.recordId,
        slotKey,
        fields: {
          ...(fields.Title ? { Title: fields.Title } : {}),
          ...(fields.Body ? { Body: fields.Body } : {}),
          ...(fields.Image ? { Image: fields.Image } : {}),
        },
        before: {
          title: liveTitle,
          body: liveBody.slice(0, 160),
          imageUrl: live.imageUrl || null,
        },
        after: {
          title: fields.Title || liveTitle,
          body: (fields.Body || liveBody).slice(0, 160),
          imageUrl: fields.Image?.[0]?.url || live.imageUrl || null,
          imageRole: img?.role || null,
        },
        reason: patchReasons.join("+") || "overview_scenario_parity",
      });
    } else {
      const fetchable = img?.imageUrl ? toAirtableFetchableImageUrl(img.imageUrl) : null;
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
          "Sort Order": 30 + i,
          Title: nextTitle,
          Body: nextBody,
          ...(fetchable ? { Image: [{ url: fetchable }] } : {}),
        },
        before: null,
        after: { title: nextTitle, body: nextBody.slice(0, 160), imageUrl: fetchable },
        reason: "overview_scenario_create",
      });
    }
    reasons.push(slotKey);
  }

  const plannedScenarioImages = SCENARIO_SLOTS.map((slotKey, i) => {
    const patch = patches.find((p) => p.slotKey === slotKey);
    const live = liveOverview[i];
    return {
      slotKey,
      imageUrl: patch?.fields?.Image?.[0]?.url || imagePicks[i]?.imageUrl || live?.imageUrl || "",
      title: patch?.fields?.Title || pkg.overviewScenarios[i].title,
    };
  });
  const uniqueness = evaluateImageUniqueness({
    brandSlug: slug,
    presentationRows: [
      ...plannedScenarioImages,
      ...[1, 2, 3, 4, 5, 6].map((n) => ({
        slotKey: `materials.gallery.${n}`,
        imageUrl: `https://cdn.example.com/wave13-value-scenario/${slug}-g${n}.jpg`,
        title: `Gallery ${n}`,
      })),
      ...[1, 2, 3].map((n) => ({
        slotKey: "footprint.openings",
        imageUrl: `https://cdn.example.com/wave13-value-scenario/${slug}-o${n}.jpg`,
        title: `Open ${n}`,
        recordId: `open-${n}`,
      })),
    ],
  });

  return {
    brandSlug: slug,
    brandName: pkg.brandName,
    recordId: identity.recordId,
    heldUnderReviewOnly: slug === WAVE13_HELD_PROMOTION_SLUG,
    blocked: false,
    blockers: [],
    before,
    after: {
      overview: pkg.overviewScenarios.map((c, i) => ({
        title: c.title,
        body: c.body,
        words: words(c.body),
        imageRole: c.imageRole,
        imageUrl: imagePicks[i]?.imageUrl || null,
      })),
      valueOwners: pkg.valueOwnersScenarios.map((c) => ({
        title: c.title,
        body: c.body,
        words: words(c.body),
      })),
    },
    patches,
    imageFlags,
    diagnostics: {
      plannedScenarioDistinct:
        uniqueness.scenarioDistinctCount ??
        new Set(
          plannedScenarioImages
            .filter((r) => r.imageUrl)
            .map((r) => buildImageIdentity(r.imageUrl).duplicateGroupId)
        ).size,
      uniquenessScenarioPass: (uniqueness.scenarioDistinctCount || 0) >= 3,
      slotsTouched: reasons,
    },
    plannedWrites: patches.length,
    brandStatusUntouched: true,
    releaseFieldsUntouched: true,
    companyValidatedUntouched: true,
  };
}

async function airtablePatch(baseId, apiKey, table, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`Airtable PATCH ${recordId}: ${res.status} ${JSON.stringify(json)}`);
  }
  return json;
}

async function airtableCreate(baseId, apiKey, table, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`Airtable POST: ${res.status} ${JSON.stringify(json)}`);
  }
  return json;
}

function renderBrandMd(plan) {
  const lines = [
    `# Wave 13 Value Scenario Cleanup — ${plan.brandName}`,
    "",
    `Slug: \`${plan.brandSlug}\``,
    plan.heldUnderReviewOnly ? "- **SO/ held** — Under Review; no status/release writes" : "",
    "",
    "## Before",
    "",
    "### Where This Brand Creates the Most Value",
    "",
  ];
  for (const [i, c] of (plan.before?.overview || []).entries()) {
    lines.push(`**${i + 1}. ${c.title || "(blank)"}** (${c.words} words)`);
    lines.push("");
    lines.push(c.body || "_(empty)_");
    lines.push("");
  }
  lines.push("### Value Creation Scenarios", "");
  for (const [i, c] of (plan.before?.valueOwners || []).entries()) {
    lines.push(
      `- ${i + 1}. ${c.present ? `**${c.title || "(untitled)"}** (${c.words}w)` : "_missing_"}`
    );
  }
  lines.push("", "## After", "", "### Where This Brand Creates the Most Value", "");
  for (const [i, c] of (plan.after?.overview || []).entries()) {
    lines.push(`**${i + 1}. ${c.title}** (${c.words} words · ${c.imageRole})`);
    lines.push("");
    lines.push(c.body);
    lines.push("");
  }
  lines.push("### Value Creation Scenarios", "");
  for (const [i, c] of (plan.after?.valueOwners || []).entries()) {
    lines.push(`**${i + 1}. ${c.title}** (${c.words} words)`);
    lines.push("");
    lines.push(c.body);
    lines.push("");
  }
  lines.push("## Patches", "");
  for (const p of plan.patches || []) {
    lines.push(`- \`${p.action}\` \`${p.slotKey}\` — ${p.reason}`);
  }
  return lines.filter((l, idx, arr) => !(l === "" && arr[idx - 1] === "")).join("\n");
}

function renderSummaryMd(report) {
  return [
    `# Wave 13 — Value Scenario Pattern Cleanup`,
    "",
    `Version: \`${report.version}\` · Packages: \`${report.packagesVersion}\``,
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.applyPerformed ? "APPLY" : "DRY-RUN"}**`,
    "",
    `Ready: \`${report.readyStatement}\``,
    "",
    "## Scope",
    "",
    `- Public six: ${WAVE13_PARTIAL_PROMOTION_SLUGS.join(", ")}`,
    `- Held (patched, not released): ${WAVE13_HELD_PROMOTION_SLUG}`,
    "",
    "## Summary",
    "",
    `- Brands: ${report.summary.brands}`,
    `- Planned patches: ${report.summary.plannedPatches}`,
    `- Creates: ${report.summary.creates}`,
    `- Patches: ${report.summary.patches}`,
    `- Image writes: ${report.summary.imageWrites}`,
    "",
    "## Brands",
    "",
    ...report.brandResults.map(
      (b) =>
        `- **${b.brandName}** (\`${b.brandSlug}\`): ${
          b.blocked
            ? `BLOCKED (${(b.blockers || []).join(", ")})`
            : `${b.plannedWrites} writes · scenarioDistinct=${b.diagnostics?.plannedScenarioDistinct}`
        }`
    ),
    "",
    "## Guardrails",
    "",
    `- Brand Status writes: **false**`,
    `- Release field writes: **false**`,
    `- SO/ remains Under Review: **true**`,
    `- Protected 39 / House / Morgans / Radisson: untouched`,
    "",
  ].join("\n");
}

export async function runWave13ValueScenarioPatternCleanup({
  dryRun = true,
  argv = [],
  brands = null,
} = {}) {
  const apply = argv.includes("--apply") && dryRun === false;
  const flagCheck = checkFlags(
    WAVE13_VALUE_SCENARIO_PATTERN_CLEANUP_APPLY_FLAGS,
    argv,
    apply
  );

  const targetSlugs = (brands?.length ? brands : [...WAVE13_VALUE_SCENARIO_TARGET_SLUGS]).filter(
    (s) => WAVE13_VALUE_SCENARIO_TARGET_SLUGS.includes(s)
  );

  if (apply && !flagCheck.ok) {
    throw new Error(
      `Missing required apply flags: ${flagCheck.missing.join(", ") || "(none)"}`
    );
  }

  const brandResults = [];
  for (const slug of targetSlugs) {
    brandResults.push(await planWave13ValueScenarioPatternCleanupForBrand(slug));
  }

  const applyResults = {};
  let writePerformed = false;
  if (apply && flagCheck.ok) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

    for (const plan of brandResults) {
      if (plan.blocked) {
        applyResults[plan.brandSlug] = {
          applied: false,
          reason: "blocked",
          blockers: plan.blockers,
        };
        continue;
      }
      const wrote = [];
      const errors = [];
      for (const patch of plan.patches || []) {
        try {
          if (patch.action === "PATCH" && patch.recordId) {
            await airtablePatch(baseId, apiKey, patch.table, patch.recordId, patch.fields);
            wrote.push({ slotKey: patch.slotKey, action: "PATCH", recordId: patch.recordId });
            writePerformed = true;
          } else if (patch.action === "POST") {
            const created = await airtableCreate(baseId, apiKey, patch.table, patch.fields);
            wrote.push({
              slotKey: patch.slotKey,
              action: "POST",
              recordId: created.id || null,
            });
            writePerformed = true;
          }
          await sleep(WRITE_THROTTLE_MS);
        } catch (err) {
          errors.push({ slotKey: patch.slotKey, message: err.message });
        }
      }
      applyResults[plan.brandSlug] = {
        applied: errors.length === 0,
        wrote,
        errors,
      };
    }
  }

  const readyStatement = apply
    ? "wave13_value_scenario_pattern_clean_visual_review_ready"
    : "wave13_value_scenario_pattern_cleanup_dry_run_ready";

  const report = {
    version: WAVE13_VALUE_SCENARIO_PATTERN_CLEANUP_VERSION,
    waveVersion: WAVE13_VERSION,
    packagesVersion: WAVE13_VALUE_SCENARIO_PACKAGES_VERSION,
    stage: "value-scenario-pattern-cleanup",
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    apply,
    applyPerformed: apply === true,
    writePerformed,
    flagCheck,
    requiredApplyFlags: [...WAVE13_VALUE_SCENARIO_PATTERN_CLEANUP_APPLY_FLAGS],
    targetSlugs,
    publicSix: [...WAVE13_PARTIAL_PROMOTION_SLUGS],
    held: WAVE13_HELD_PROMOTION_SLUG,
    brandResults,
    applyResults,
    summary: {
      brands: brandResults.length,
      blocked: brandResults.filter((b) => b.blocked).length,
      plannedPatches: brandResults.reduce((n, b) => n + (b.patches?.length || 0), 0),
      creates: brandResults.reduce(
        (n, b) => n + (b.patches || []).filter((p) => p.action === "POST").length,
        0
      ),
      patches: brandResults.reduce(
        (n, b) => n + (b.patches || []).filter((p) => p.action === "PATCH").length,
        0
      ),
      imageWrites: brandResults.reduce(
        (n, b) =>
          n + (b.patches || []).filter((p) => p.fields?.Image || p.reason?.includes("image")).length,
        0
      ),
    },
    guardrails: {
      brandStatusWrites: false,
      releaseFieldWrites: false,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryApprovalWrites: false,
      soHeldUnderReview: true,
      protected39Untouched: true,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
      targetedValueScenarioFixesOnly: true,
    },
    readyStatement,
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-value-scenario-pattern-cleanup.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-value-scenario-pattern-cleanup.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(mdPath, renderSummaryMd(report));

  const perBrandPaths = [];
  for (const plan of brandResults) {
    if (plan.blocked) continue;
    const p = path.join(
      REPORTS_DIR,
      `brand-explorer-wave13-value-scenario-pattern-cleanup-${plan.brandSlug}.md`
    );
    fs.writeFileSync(p, `${renderBrandMd(plan)}\n`);
    perBrandPaths.push(p);
  }

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave13-value-scenario-pattern-cleanup.md");
  fs.writeFileSync(
    docPath,
    [
      `# Brand Explorer — Wave 13 Value Scenario Pattern Cleanup`,
      "",
      `> Ready: \`${readyStatement}\``,
      "",
      "Targeted remediation for Value Creation Scenarios (`valueOwners.scenario.1–4`) and Where This Brand Creates the Most Value (`overview.scenario.1–3`), including scenario image role-matching when flagged.",
      "",
      "## Scope",
      "",
      "- Six public Wave 13 brands + SO/ (held Under Review)",
      "- No Brand Status / release / CV / Source Library / Registry writes",
      "",
      "## Commands",
      "",
      "```bash",
      "npm run brand-explorer-wave13-factory -- --stage value-scenario-pattern-cleanup --dry-run",
      "npm run brand-explorer-wave13-factory -- --stage value-scenario-pattern-cleanup --apply \\",
      "  --approve-wave13-value-scenario-pattern-cleanup \\",
      "  --confirm-wave13-target-scope-only \\",
      "  --confirm-so-held-status-unchanged \\",
      "  ...",
      "```",
      "",
      `Last generated: ${report.generatedAt}`,
      "",
    ].join("\n")
  );

  // Visual QA packet (before/after)
  const visualQaPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave13-value-scenario-visual-qa.md"
  );
  const visualLines = [
    `# Wave 13 — Value Scenario Visual QA Packet`,
    "",
    `Generated: ${report.generatedAt}`,
    "",
  ];
  for (const plan of brandResults) {
    if (plan.blocked) continue;
    visualLines.push(`## ${plan.brandName}`, "");
    visualLines.push("### Before — Where This Brand Creates the Most Value", "");
    for (const [i, c] of (plan.before?.overview || []).entries()) {
      visualLines.push(`${i + 1}. **${c.title || "(blank)"}**`);
      visualLines.push("");
      visualLines.push(`> ${c.body || "_(empty)_"}`);
      visualLines.push("");
    }
    visualLines.push("### After — Where This Brand Creates the Most Value", "");
    for (const [i, c] of (plan.after?.overview || []).entries()) {
      visualLines.push(`${i + 1}. **${c.title}** (${c.imageRole})`);
      visualLines.push("");
      visualLines.push(`> ${c.body}`);
      visualLines.push("");
      if (c.imageUrl) visualLines.push(`Image: ${c.imageUrl}`);
      visualLines.push("");
    }
    visualLines.push("### After — Value Creation Scenarios", "");
    for (const [i, c] of (plan.after?.valueOwners || []).entries()) {
      visualLines.push(`${i + 1}. **${c.title}** — ${c.body}`);
      visualLines.push("");
    }
    visualLines.push(
      "### Founder visual notes",
      "",
      "- Titles are owner-value topics (not section labels).",
      "- Bodies are concise and brand-specific.",
      "- Scenario images selected for role match + distinctness when pool allows.",
      plan.heldUnderReviewOnly
        ? "- SO/ remains Under Review / not public-full."
        : "- Public brand; no status/release changes in this stage.",
      "",
      "### Remaining cautions",
      "",
      ...(plan.imageFlags || []).map((f) => `- ${f}`),
      (plan.imageFlags || []).length ? "" : "- None flagged beyond normal steward review.",
      ""
    );
  }
  fs.writeFileSync(visualQaPath, `${visualLines.join("\n")}\n`);

  return {
    ...report,
    paths: { jsonPath, mdPath, docPath, visualQaPath, perBrandPaths },
  };
}
