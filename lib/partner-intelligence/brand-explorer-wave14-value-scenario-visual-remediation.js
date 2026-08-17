/**
 * Wave 14 — Value scenario visual QA remediation (eight active brands).
 *
 * Scope: overview.scenario.1–3 Title / Body / Image only.
 * Four Points Flex held. No Brand Status / release / CV / Source / Registry /
 * Recent Momentum / Geo / gallery outside scenario cards.
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
  words,
} from "./brand-explorer-scenario-owner-value-bar.js";
import {
  WAVE14_VERSION,
  WAVE14_PARTIAL_PROMOTION_SLUGS,
  WAVE14_HELD_PROMOTION_SLUG,
  WAVE14_NEVER_WRITE_FIELDS,
  WAVE14_PROTECTED_BASELINE_COUNT,
  WAVE14_VALUE_SCENARIO_VISUAL_REMEDIATION_APPLY_FLAGS,
} from "./brand-explorer-wave14-factory-plan.js";
import {
  WAVE14_VALUE_SCENARIO_VISUAL_PACKAGES_VERSION,
  WAVE14_VALUE_SCENARIO_TARGET_SLUGS,
  getWave14ValueScenarioVisualPackage,
} from "./brand-explorer-wave14-value-scenario-visual-packages.js";
import { extractWave14ValueScenarioVisualFailures } from "./brand-explorer-wave14-value-scenario-visual-failures.js";

export const WAVE14_VALUE_SCENARIO_VISUAL_REMEDIATION_VERSION =
  "wave14-value-scenario-visual-remediation-v1";

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
  /\bBonvoy Reach\b/i,
  /Operating Efficiency After Affiliation/i,
  /Operating Model Discipline After Affiliation/i,
  /sequence (design|systems|standards|PIP|training)/i,
  /\bdo not reuse\b/i,
  /\bavoid borrowing\b/i,
  /\bkeep responsibilities clear\b/i,
  /deliverable after affiliation/i,
  /\bsource pack\b/i,
  /\bfactory\b/i,
  /\bstage\s*\d/i,
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

function isHidden(row) {
  return (
    row?.active === false ||
    /do not display|internal only/i.test(nz(row?.externalDisplayStatus))
  );
}

function findSlot(rows, slotKey) {
  const matches = (rows || [])
    .filter((r) => nz(r.slotKey) === slotKey && !isHidden(r))
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
  return matches[0] || null;
}

function assertPackageClean(pkg, slug) {
  const issues = [];
  for (const c of pkg.overviewScenarios || []) {
    for (const re of FORBIDDEN_VISIBLE_RES) {
      if (re.test(`${c.title}\n${c.body}`)) {
        issues.push(`${slug}:forbidden:${re.source}`);
      }
    }
    if (words(c.body) < SCENARIO_MIN_BODY_WORDS) {
      issues.push(`${slug}:thin_body:${c.title}:${words(c.body)}`);
    }
    if (/\bBonvoy Reach\b/i.test(c.title)) {
      issues.push(`${slug}:generic_bonvoy_title:${c.title}`);
    }
  }
  const roles = (pkg.overviewScenarios || []).map((c) => c.imageRole);
  if (new Set(roles).size < 2) {
    issues.push(`${slug}:insufficient_image_role_diversity`);
  }
  return issues;
}

function loadGalleryPool(slug) {
  const p = path.join(FIXTURES, `wave14-${slug}-gallery-pool.json`);
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
  const usedRoles = new Set();
  const picks = [];
  const flagged = [];

  for (let i = 0; i < overviewCards.length; i++) {
    const card = overviewCards[i];
    const live = liveScenarioRows[i];
    const liveUrl = nz(live?.imageUrl);
    const liveId = liveUrl
      ? buildImageIdentity(liveUrl, {
          title: live?.title || "",
          filename: live?.imageFilename || live?.filename || "",
        }).duplicateGroupId
      : null;
    const liveCat = liveUrl
      ? detectVisualCategory({
          imageUrl: liveUrl,
          title: nz(live?.title),
          filename: nz(live?.imageFilename || live?.filename),
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
      usedRoles.add(wantRole);
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
        filename: a.filename || "",
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
          title: a.title || "",
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
        needsWrite: Boolean(liveUrl && liveId && !usedGroups.has(liveId)),
      });
      if (liveId) usedGroups.add(liveId);
      continue;
    }

    const nextUrl = chosen.imageUrl;
    const nextId = buildImageIdentity(nextUrl, {
      propertyName: chosen.propertyName,
      filename: chosen.filename || "",
    }).duplicateGroupId;
    usedGroups.add(nextId);
    usedRoles.add(wantRole);
    const needsWrite = !liveUrl || liveId !== nextId || liveCat !== wantRole;
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

  if (usedGroups.size < 3) {
    flagged.push(`scenario_image_group_count_${usedGroups.size}`);
  }

  return { picks, flagged, usedGroups: [...usedGroups], usedRoles: [...usedRoles] };
}

export async function planWave14ValueScenarioVisualRemediationForBrand(slug) {
  const identity = resolveIdentity(slug);
  const pkg = getWave14ValueScenarioVisualPackage(slug);
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
  if (!WAVE14_VALUE_SCENARIO_TARGET_SLUGS.includes(slug)) {
    return {
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: ["not_wave14_value_scenario_target"],
      patches: [],
    };
  }
  if (slug === WAVE14_HELD_PROMOTION_SLUG) {
    return {
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: ["four_points_flex_held_no_writes"],
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

  const before = {
    overview: liveOverview.map((r) => ({
      title: nz(r?.title),
      body: nz(r?.body).slice(0, 240),
      words: words(r?.body || ""),
      imageUrl: r?.imageUrl || null,
      imageFilename: r?.imageFilename || null,
    })),
  };

  const { picks: imagePicks, flagged: imageFlags } = pickScenarioImages(
    slug,
    pkg.overviewScenarios,
    liveOverview
  );

  const patches = [];
  for (let i = 0; i < SCENARIO_SLOTS.length; i++) {
    const slotKey = SCENARIO_SLOTS[i];
    const live = liveOverview[i];
    const next = pkg.overviewScenarios[i];
    const img = imagePicks[i];
    const nextTitle = toProperCaseScenarioTitle(next.title);
    const nextBody = nz(next.body);
    const liveTitle = nz(live?.title);
    const liveBody = nz(live?.body);

    const titleNeeds = !live?.recordId || liveTitle !== nextTitle;
    const bodyNeeds =
      !live?.recordId ||
      liveBody !== nextBody ||
      words(liveBody) < SCENARIO_MIN_BODY_WORDS ||
      FORBIDDEN_VISIBLE_RES.some((re) => re.test(liveBody)) ||
      /\bBonvoy Reach\b/i.test(liveTitle) ||
      /Network Reach With|Operating Efficiency After|Operating Model Discipline After/i.test(
        liveTitle
      );

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
      }
    }

    for (const forbidden of WAVE14_NEVER_WRITE_FIELDS) {
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
  }

  // Distinctness must use source CDN URLs from picks — not wsrv.nl proxy write URLs
  // (basename of https://wsrv.nl/ collapses every card to file:wsrv.nl).
  const plannedScenarioImages = SCENARIO_SLOTS.map((slotKey, i) => {
    const live = liveOverview[i];
    const sourceUrl = imagePicks[i]?.imageUrl || live?.imageUrl || "";
    let sourceFilename = "";
    try {
      sourceFilename = decodeURIComponent(new URL(sourceUrl).pathname.split("/").pop() || "")
        .split("?")[0];
    } catch {
      sourceFilename = nz(live?.imageFilename);
    }
    return {
      slotKey,
      imageUrl: sourceUrl,
      title: pkg.overviewScenarios[i].title,
      imageFilename: sourceFilename,
    };
  });

  const uniqueness = evaluateImageUniqueness({
    brandSlug: slug,
    presentationRows: [
      ...plannedScenarioImages,
      ...[1, 2, 3, 4, 5, 6].map((n) => ({
        slotKey: `materials.gallery.${n}`,
        imageUrl: `https://cdn.example.com/wave14-value-scenario/${slug}-g${n}.jpg`,
        title: `Gallery ${n}`,
      })),
      ...[1, 2, 3].map((n) => ({
        slotKey: "footprint.openings",
        imageUrl: `https://cdn.example.com/wave14-value-scenario/${slug}-o${n}.jpg`,
        title: `Open ${n}`,
        recordId: `open-${n}`,
      })),
    ],
  });

  const distinctIds = new Set(
    plannedScenarioImages
      .filter((r) => r.imageUrl)
      .map((r) =>
        buildImageIdentity(r.imageUrl, { filename: r.imageFilename || "" }).duplicateGroupId
      )
  );

  const blockers = [];
  if (distinctIds.size < 3) {
    blockers.push(`planned_scenario_images_not_distinct:${distinctIds.size}`);
  }
  if (imageFlags.some((f) => f.startsWith("missing_image"))) {
    blockers.push(...imageFlags.filter((f) => f.startsWith("missing_image")));
  }

  return {
    brandSlug: slug,
    brandName: pkg.brandName,
    recordId: identity.recordId,
    blocked: blockers.length > 0,
    blockers,
    before,
    after: {
      overview: pkg.overviewScenarios.map((c, i) => ({
        title: c.title,
        body: c.body,
        words: words(c.body),
        imageRole: c.imageRole,
        imageUrl: imagePicks[i]?.imageUrl || null,
      })),
    },
    patches,
    imageFlags,
    diagnostics: {
      plannedScenarioDistinct: distinctIds.size,
      uniquenessScenarioPass: (uniqueness.scenarioDistinctCount || 0) >= 3,
      imageRoles: imagePicks.map((p) => p.role),
    },
    plannedWrites: patches.length,
    brandStatusUntouched: true,
    releaseFieldsUntouched: true,
    companyValidatedUntouched: true,
    fourPointsFlexUntouched: true,
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
    `# Wave 14 Value Scenario Visual Remediation — ${plan.brandName}`,
    "",
    `Slug: \`${plan.brandSlug}\``,
    "",
    "## Before",
    "",
  ];
  for (const [i, c] of (plan.before?.overview || []).entries()) {
    lines.push(`**${i + 1}. ${c.title || "(blank)"}** (${c.words} words)`);
    lines.push("");
    lines.push(c.body || "_(empty)_");
    lines.push("");
  }
  lines.push("## After", "");
  for (const [i, c] of (plan.after?.overview || []).entries()) {
    lines.push(`**${i + 1}. ${c.title}** (${c.words} words · ${c.imageRole})`);
    lines.push("");
    lines.push(c.body);
    lines.push("");
    lines.push(`Image: \`${c.imageUrl || ""}\``);
    lines.push("");
  }
  lines.push("## Patches", "");
  for (const p of plan.patches || []) {
    lines.push(`- \`${p.action}\` \`${p.slotKey}\` — ${p.reason}`);
  }
  if (plan.blocked) {
    lines.push("", `**BLOCKED:** ${(plan.blockers || []).join(", ")}`);
  }
  return `${lines.join("\n")}\n`;
}

function renderSummaryMd(report) {
  return [
    `# Wave 14 — Value Scenario Visual Remediation`,
    "",
    `Version: \`${report.version}\` · Packages: \`${report.packagesVersion}\``,
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.applyPerformed ? "APPLY" : "DRY-RUN"}**`,
    "",
    `Ready: \`${report.readyStatement}\``,
    "",
    "## Scope",
    "",
    `- Eight active: ${WAVE14_PARTIAL_PROMOTION_SLUGS.join(", ")}`,
    `- Held (untouched): ${WAVE14_HELD_PROMOTION_SLUG}`,
    `- Protected baseline count remains: ${WAVE14_PROTECTED_BASELINE_COUNT}`,
    "",
    "## Summary",
    "",
    `- Brands: ${report.summary.brands}`,
    `- Planned patches: ${report.summary.plannedPatches}`,
    `- Creates: ${report.summary.creates}`,
    `- Patches: ${report.summary.patches}`,
    `- Image writes: ${report.summary.imageWrites}`,
    `- Blocked brands: ${report.summary.blocked}`,
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
    `- Four Points Flex untouched: **true**`,
    `- CV / Source / Registry: **untouched**`,
    `- Recent Momentum / Geo: **untouched**`,
    "",
  ].join("\n");
}

export async function runWave14ValueScenarioVisualRemediation({
  dryRun = true,
  argv = [],
  brands = null,
} = {}) {
  const apply = argv.includes("--apply") && dryRun === false;
  const flagCheck = checkFlags(
    WAVE14_VALUE_SCENARIO_VISUAL_REMEDIATION_APPLY_FLAGS,
    argv,
    apply
  );

  if (apply && !flagCheck.ok) {
    return {
      version: WAVE14_VALUE_SCENARIO_VISUAL_REMEDIATION_VERSION,
      packagesVersion: WAVE14_VALUE_SCENARIO_VISUAL_PACKAGES_VERSION,
      generatedAt: new Date().toISOString(),
      applyPerformed: false,
      pass: false,
      stopRecommended: true,
      readyStatement: "wave14_value_scenario_visual_remediation_blocked_missing_flags",
      missingFlags: flagCheck.missing,
      summary: { blocked: true },
    };
  }

  // Failure extraction first (read-only artifact)
  const failures = await extractWave14ValueScenarioVisualFailures({
    brands: brands || WAVE14_VALUE_SCENARIO_TARGET_SLUGS,
  });

  const targetSlugs = (brands || WAVE14_VALUE_SCENARIO_TARGET_SLUGS).filter(
    (s) => s !== WAVE14_HELD_PROMOTION_SLUG
  );

  const brandResults = [];
  for (const slug of targetSlugs) {
    brandResults.push(await planWave14ValueScenarioVisualRemediationForBrand(slug));
  }

  const anyBlocked = brandResults.some((b) => b.blocked);
  let applied = 0;
  const applyErrors = [];

  if (apply && !anyBlocked) {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

    for (const plan of brandResults) {
      for (const patch of plan.patches || []) {
        try {
          if (patch.action === "PATCH" && patch.recordId) {
            await airtablePatch(baseId, apiKey, patch.table, patch.recordId, patch.fields);
          } else if (patch.action === "POST") {
            await airtableCreate(baseId, apiKey, patch.table, patch.fields);
          }
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

  const plannedPatches = brandResults.reduce((n, b) => n + (b.patches?.length || 0), 0);
  const imageWrites = brandResults.reduce(
    (n, b) => n + (b.patches || []).filter((p) => p.fields?.Image).length,
    0
  );
  const creates = brandResults.reduce(
    (n, b) => n + (b.patches || []).filter((p) => p.action === "POST").length,
    0
  );
  const patchesOnly = brandResults.reduce(
    (n, b) => n + (b.patches || []).filter((p) => p.action === "PATCH").length,
    0
  );

  const readyStatement =
    apply && !anyBlocked && applyErrors.length === 0
      ? "wave14_value_scenario_visual_quality_clean_ready_for_54_freeze"
      : anyBlocked
        ? "wave14_value_scenario_visual_remediation_blocked"
        : applyErrors.length
          ? "wave14_value_scenario_visual_remediation_apply_errors"
          : "wave14_value_scenario_visual_remediation_dry_run_ready";

  const report = {
    version: WAVE14_VALUE_SCENARIO_VISUAL_REMEDIATION_VERSION,
    factoryVersion: WAVE14_VERSION,
    packagesVersion: WAVE14_VALUE_SCENARIO_VISUAL_PACKAGES_VERSION,
    generatedAt: new Date().toISOString(),
    applyPerformed: apply === true && applyErrors.length === 0 && !anyBlocked,
    dryRun: !apply,
    pass: !anyBlocked && applyErrors.length === 0,
    stopRecommended: anyBlocked || applyErrors.length > 0,
    readyStatement,
    failuresArtifact: failures.paths || null,
    failuresSummary: failures.summary || null,
    flagCheck,
    summary: {
      brands: brandResults.length,
      plannedPatches,
      creates,
      patches: patchesOnly,
      imageWrites,
      blocked: brandResults.filter((b) => b.blocked).length,
      appliedWrites: applied,
      applyErrors: applyErrors.length,
    },
    brandResults: brandResults.map((b) => ({
      brandSlug: b.brandSlug,
      brandName: b.brandName,
      recordId: b.recordId,
      blocked: b.blocked,
      blockers: b.blockers,
      plannedWrites: b.plannedWrites,
      diagnostics: b.diagnostics,
      imageFlags: b.imageFlags,
      beforeTitles: (b.before?.overview || []).map((c) => c.title),
      afterTitles: (b.after?.overview || []).map((c) => c.title),
    })),
    applyErrors,
    guardrails: {
      brandStatusWrites: false,
      releaseFieldWrites: false,
      fourPointsFlexWrites: false,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryWrites: false,
      recentMomentumWrites: false,
      geoFootprintWrites: false,
      protectedBaselineCount: WAVE14_PROTECTED_BASELINE_COUNT,
    },
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave14-value-scenario-visual-remediation.json"
  );
  const mdPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave14-value-scenario-visual-remediation.md"
  );
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, renderSummaryMd(report), "utf8");

  for (const plan of brandResults) {
    const brandMd = path.join(
      REPORTS_DIR,
      `brand-explorer-wave14-value-scenario-visual-remediation-${plan.brandSlug}.md`
    );
    fs.writeFileSync(brandMd, renderBrandMd(plan), "utf8");
  }

  const docsPath = path.join(
    DOCS_DIR,
    "brand-explorer-wave14-value-scenario-visual-remediation.md"
  );
  fs.writeFileSync(
    docsPath,
    [
      `# Wave 14 Value Scenario Visual Remediation`,
      "",
      `Ready: \`${readyStatement}\``,
      "",
      "Founder visual QA remediation for **Where This Brand Creates the Most Value**",
      "on the eight active Wave 14 brands. Four Points Flex remains held.",
      "",
      "## Scope",
      "",
      "- Writes: `overview.scenario.1–3` Title / Body / Image only",
      "- No Brand Status / release / CV / Source / Registry / Momentum / Geo writes",
      "",
      "## Image uniqueness",
      "",
      "- `image-uniqueness-v3` collapses Marriott DAM size variants (`mhr_1189921-1024x576` vs `-scaled`)",
      "- Scenario picks require three distinct duplicate groups + role diversity from Wave 14 gallery pools",
      "",
      `See reports: \`${path.basename(jsonPath)}\``,
      "",
    ].join("\n"),
    "utf8"
  );

  return {
    ...report,
    paths: {
      jsonPath,
      mdPath,
      docsPath,
      failures: failures.paths,
      brandReports: brandResults.map(
        (b) =>
          path.join(
            REPORTS_DIR,
            `brand-explorer-wave14-value-scenario-visual-remediation-${b.brandSlug}.md`
          )
      ),
    },
  };
}
