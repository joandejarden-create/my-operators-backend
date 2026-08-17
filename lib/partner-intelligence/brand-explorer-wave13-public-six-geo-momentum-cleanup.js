/**
 * Wave 13 Public Six — Geographic Footprint + Recent Momentum cleanup.
 * Six Active public brands only. SO/ untouched. No status/release/image writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE13_VERSION,
  WAVE13_PARTIAL_PROMOTION_SLUGS,
  WAVE13_HELD_PROMOTION_SLUG,
  WAVE13_NEVER_WRITE_FIELDS,
  WAVE13_PUBLIC_SIX_GEO_MOMENTUM_CLEANUP_APPLY_FLAGS,
} from "./brand-explorer-wave13-factory-plan.js";
import {
  WAVE13_PUBLIC_SIX_GEO_MOMENTUM_PACKAGES_VERSION,
  WAVE13_PUBLIC_SIX_GEO_MOMENTUM_SLUGS,
  getWave13PublicSixGeoMomentumPackage,
} from "./brand-explorer-wave13-public-six-geo-momentum-packages.js";
import { RECENT_MOMENTUM_DEFAULT_LABEL } from "./brand-explorer-recent-momentum-contract.js";

export const WAVE13_PUBLIC_SIX_GEO_MOMENTUM_CLEANUP_VERSION =
  "wave13-public-six-geo-momentum-cleanup-v1";

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
  /\bCompany Validated\b/i,
  /\billustrative activity\b/i,
  /\bdirectional themes?\b/i,
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(text) {
  return nz(text)
    .split(/\s+/)
    .filter(Boolean).length;
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

function findAllSlots(rows, slotKey) {
  return (rows || [])
    .filter((r) => nz(r.slotKey) === slotKey && !isHidden(r))
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
}

function assertPackageClean(pkg, slug) {
  const issues = [];
  const blobs = [
    pkg.geoIntro,
    ...pkg.regions.map((r) => `${r.title}\n${r.body}\n${r.caseSummary}\n${r.tags}`),
    ...pkg.momentumCards.map((c) => `${c.title}\n${c.dateLine}\n${c.summary}`),
    pkg.momentumLabel,
  ];
  for (const blob of blobs) {
    for (const re of FORBIDDEN_VISIBLE_RES) {
      if (re.test(blob || "")) issues.push(`${slug}:${re.source}`);
    }
  }
  for (const c of pkg.momentumCards) {
    if (!/^https?:\/\//i.test(c.url || "")) {
      issues.push(`${slug}:momentum_missing_url:${c.title}`);
    }
    if (words(c.summary) < 35) {
      issues.push(`${slug}:momentum_summary_thin:${c.title}:${words(c.summary)}`);
    }
  }
  if (pkg.regions.length < 3) issues.push(`${slug}:regions_below_3`);
  for (const r of pkg.regions) {
    if (words(r.body) < 12) issues.push(`${slug}:region_thin:${r.slotKey}`);
  }
  if (words(pkg.geoIntro) < 30) issues.push(`${slug}:geo_intro_thin`);
  return [...new Set(issues)];
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
  ]) {
    if (next[forbidden] != null) delete next[forbidden];
  }
  return next;
}

async function airtablePatch(baseId, apiKey, table, recordId, fields) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields: scrubForbiddenFields(fields) }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH failed ${res.status}`);
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
    body: JSON.stringify({ fields: scrubForbiddenFields(fields) }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `POST failed ${res.status}`);
  return json;
}

export async function planWave13PublicSixGeoMomentumCleanupForBrand(slug) {
  if (!WAVE13_PUBLIC_SIX_GEO_MOMENTUM_SLUGS.includes(slug)) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["out_of_public_six_scope"],
      patches: [],
    };
  }
  if (slug === WAVE13_HELD_PROMOTION_SLUG) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["so_held_and_untouched"],
      patches: [],
    };
  }

  const identity = resolveIdentity(slug);
  const pkg = getWave13PublicSixGeoMomentumPackage(slug);
  if (!identity?.recordId || !pkg) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["missing_identity_or_package"],
      patches: [],
    };
  }

  const packageIssues = assertPackageClean(pkg, slug);
  if (packageIssues.length) {
    return {
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: packageIssues,
      patches: [],
    };
  }

  const { rows, skipped } = await listPresentationRowsLight(identity.recordId, identity.name);
  if (skipped) {
    return {
      brandSlug: slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: [skipped],
      patches: [],
    };
  }

  const patches = [];
  const before = {
    geoIntro: null,
    regions: [],
    momentum: [],
  };

  const geoIntroLive =
    findSlot(rows, "footprint.geo_intro") || findSlot(rows, "footprint.geo.summary");
  before.geoIntro = {
    slotKey: geoIntroLive?.slotKey || "footprint.geo_intro",
    recordId: geoIntroLive?.recordId || null,
    words: words(geoIntroLive?.body),
    body: nz(geoIntroLive?.body).slice(0, 180),
  };

  if (geoIntroLive?.recordId) {
    if (nz(geoIntroLive.body) !== pkg.geoIntro || words(geoIntroLive.body) < 30) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: geoIntroLive.recordId,
        slotKey: geoIntroLive.slotKey || "footprint.geo_intro",
        fields: { Body: pkg.geoIntro },
        reason: "geo_intro_owner_facing",
      });
    }
  } else {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "POST",
      recordId: null,
      slotKey: "footprint.geo_intro",
      fields: {
        "Slot Key": "footprint.geo_intro",
        "Brand Name": identity.name,
        Brand: [identity.recordId],
        Active: true,
        "Sort Order": 10,
        Title: "Geographic Footprint",
        Body: pkg.geoIntro,
      },
      reason: "geo_intro_create",
    });
  }

  for (const region of pkg.regions) {
    const live = findSlot(rows, region.slotKey);
    before.regions.push({
      slotKey: region.slotKey,
      recordId: live?.recordId || null,
      title: nz(live?.title),
      words: words(live?.body),
      empty: !live || words(live?.body) < 12,
    });
    const fields = {
      Title: region.title,
      Body: region.body,
      "Case Summary Overview": region.caseSummary,
      "Case Summary Tags": region.tags,
    };
    if (live?.recordId) {
      const needs =
        nz(live.title) !== region.title ||
        nz(live.body) !== region.body ||
        words(live.body) < 12 ||
        nz(live.caseSummaryTags) !== region.tags ||
        nz(live.caseSummaryOverview) !== region.caseSummary;
      if (needs) {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: live.recordId,
          slotKey: region.slotKey,
          fields,
          reason: "region_card_source_supported",
        });
      }
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey: region.slotKey,
        fields: {
          "Slot Key": region.slotKey,
          "Brand Name": identity.name,
          Brand: [identity.recordId],
          Active: true,
          "Sort Order": region.sort || 12,
          ...fields,
        },
        reason: "region_card_create",
      });
    }
  }

  const liveMomentum = findAllSlots(rows, "footprint.momentum");
  before.momentum = liveMomentum.map((r) => ({
    recordId: r.recordId,
    title: nz(r.title),
    body: nz(r.body).slice(0, 160),
    words: words(r.body),
  }));

  for (const row of liveMomentum) {
    // Active:false alone does not reliably persist on legacy rows (stays undefined → still visible).
    // External Display Status "Do Not Display" is the durable hide signal used by brand-library.
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: row.recordId,
      slotKey: "footprint.momentum",
      fields: {
        Active: false,
        "External Display Status": "Do Not Display",
      },
      reason: "hide_unstructured_momentum_card",
    });
  }

  const labelLive = findSlot(rows, "footprint.momentum_label");
  const labelBody = pkg.momentumLabel || RECENT_MOMENTUM_DEFAULT_LABEL;
  if (labelLive?.recordId) {
    if (nz(labelLive.body) !== labelBody) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: labelLive.recordId,
        slotKey: "footprint.momentum_label",
        fields: { Body: labelBody, Active: true },
        reason: "momentum_label_contract",
      });
    }
  } else {
    const anyLabel = (rows || []).find((r) => r.slotKey === "footprint.momentum_label");
    if (anyLabel?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: anyLabel.recordId,
        slotKey: "footprint.momentum_label",
        fields: { Body: labelBody, Active: true },
        reason: "reactivate_momentum_label",
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey: "footprint.momentum_label",
        fields: {
          "Slot Key": "footprint.momentum_label",
          "Brand Name": identity.name,
          Brand: [identity.recordId],
          Active: true,
          "Sort Order": 1,
          Title: "",
          Body: labelBody,
        },
        reason: "create_momentum_label",
      });
    }
  }

  for (const card of pkg.momentumCards) {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "POST",
      recordId: null,
      slotKey: "footprint.momentum",
      fields: {
        "Slot Key": "footprint.momentum",
        "Brand Name": identity.name,
        Brand: [identity.recordId],
        Active: true,
        "Sort Order": card.sort || 1,
        Title: card.title,
        Body: card.body,
        "Case Summary Tags": card.regionLabel || "",
        "Case Summary Overview": `${card.dateLine} · ${card.regionLabel || ""}`.trim(),
      },
      reason: "create_structured_momentum_card",
      after: {
        title: card.title,
        dateLine: card.dateLine,
        regionLabel: card.regionLabel,
        url: card.url,
      },
    });
  }

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    blocked: false,
    blockers: [],
    before,
    after: {
      geoIntroWords: words(pkg.geoIntro),
      regions: pkg.regions.map((r) => ({
        slotKey: r.slotKey,
        title: r.title,
        words: words(r.body),
        tags: r.tags,
      })),
      momentum: pkg.momentumCards.map((c) => ({
        title: c.title,
        dateLine: c.dateLine,
        regionLabel: c.regionLabel,
        summaryWords: words(c.summary),
        url: c.url,
      })),
    },
    patches,
    plannedWrites: patches.length,
    diagnostics: {
      regionCount: pkg.regions.length,
      momentumCount: pkg.momentumCards.length,
      hideMomentum: liveMomentum.length,
    },
  };
}

function renderBrandMd(plan) {
  const lines = [
    `# Wave 13 Public Six Geo + Momentum Cleanup — ${plan.brandName}`,
    "",
    `Slug: \`${plan.brandSlug}\` · Record: \`${plan.recordId}\``,
    "",
    "## Before",
    "",
    `- Geo intro words: ${plan.before?.geoIntro?.words ?? 0}`,
    `- Regions: ${(plan.before?.regions || [])
      .map((r) => `${r.slotKey}${r.empty ? " (empty)" : ` (${r.words}w)`}`)
      .join(", ")}`,
    `- Momentum cards: ${(plan.before?.momentum || []).length}`,
    "",
    "## After",
    "",
    `- Geo intro words: ${plan.after?.geoIntroWords}`,
    "",
  ];
  for (const r of plan.after?.regions || []) {
    lines.push(`- **${r.title}** (\`${r.slotKey}\`, ${r.words}w) — ${r.tags}`);
  }
  lines.push("", "### Recent Momentum", "");
  for (const c of plan.after?.momentum || []) {
    lines.push(`- **${c.title}** · ${c.dateLine} · ${c.regionLabel}`);
  }
  lines.push("", "## Patches", "");
  for (const p of plan.patches || []) {
    lines.push(`- \`${p.action}\` \`${p.slotKey}\` — ${p.reason}`);
  }
  return lines.join("\n");
}

function renderSummaryMd(report) {
  return [
    `# Wave 13 — Public Six Geographic Footprint + Recent Momentum Cleanup`,
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
    `- Held / untouched: ${WAVE13_HELD_PROMOTION_SLUG}`,
    "",
    "## Summary",
    "",
    `- Brands: ${report.summary.brands}`,
    `- Planned patches: ${report.summary.plannedPatches}`,
    `- Creates: ${report.summary.creates}`,
    `- Patches: ${report.summary.patches}`,
    `- Image writes: **0**`,
    `- Brand Status / release / CV / Source Library / Registry writes: **false**`,
    "",
    "## Brands",
    "",
    ...report.brandResults.map(
      (b) =>
        `- **${b.brandName}** (\`${b.brandSlug}\`): ${
          b.blocked
            ? `BLOCKED (${(b.blockers || []).join(", ")})`
            : `${b.plannedWrites} writes · regions=${b.diagnostics?.regionCount} · momentum=${b.diagnostics?.momentumCount}`
        }`
    ),
    "",
  ].join("\n");
}

export async function runWave13PublicSixGeoMomentumCleanup({
  dryRun = true,
  argv = [],
  brands = null,
} = {}) {
  const apply = argv.includes("--apply") && dryRun === false;
  const flagCheck = checkFlags(
    WAVE13_PUBLIC_SIX_GEO_MOMENTUM_CLEANUP_APPLY_FLAGS,
    argv,
    apply
  );

  const targetSlugs = (brands?.length ? brands : [...WAVE13_PUBLIC_SIX_GEO_MOMENTUM_SLUGS]).filter(
    (s) => WAVE13_PUBLIC_SIX_GEO_MOMENTUM_SLUGS.includes(s)
  );

  if (apply && !flagCheck.ok) {
    throw new Error(
      `Missing required apply flags: ${flagCheck.missing.join(", ") || "(none)"}`
    );
  }

  const brandResults = [];
  for (const slug of targetSlugs) {
    brandResults.push(await planWave13PublicSixGeoMomentumCleanupForBrand(slug));
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
    ? "wave13_public_six_geo_momentum_clean_ready_for_45_or_so_decision"
    : "wave13_public_six_geo_momentum_cleanup_dry_run_ready";

  const report = {
    version: WAVE13_PUBLIC_SIX_GEO_MOMENTUM_CLEANUP_VERSION,
    waveVersion: WAVE13_VERSION,
    packagesVersion: WAVE13_PUBLIC_SIX_GEO_MOMENTUM_PACKAGES_VERSION,
    stage: "public-six-geo-momentum-cleanup",
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    apply,
    applyPerformed: apply === true,
    writePerformed,
    flagCheck,
    requiredApplyFlags: [...WAVE13_PUBLIC_SIX_GEO_MOMENTUM_CLEANUP_APPLY_FLAGS],
    targetSlugs,
    publicSix: [...WAVE13_PARTIAL_PROMOTION_SLUGS],
    heldUntouched: WAVE13_HELD_PROMOTION_SLUG,
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
      imageWrites: 0,
    },
    readyStatement,
    guardrails: {
      soUntouched: true,
      noBrandStatus: true,
      noReleaseFields: true,
      noImages: true,
      noProtected39: true,
      noHouseMorgansRadisson: true,
    },
  };

  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave13-public-six-geo-momentum-cleanup.json"
  );
  const mdPath = path.join(
    REPORTS_DIR,
    "brand-explorer-wave13-public-six-geo-momentum-cleanup.md"
  );
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, `${renderSummaryMd(report)}\n`, "utf8");

  for (const plan of brandResults) {
    if (plan.blocked && !plan.brandName) continue;
    const brandMd = path.join(
      REPORTS_DIR,
      `brand-explorer-wave13-public-six-geo-momentum-cleanup-${plan.brandSlug}.md`
    );
    fs.writeFileSync(brandMd, `${renderBrandMd(plan)}\n`, "utf8");
  }

  const docPath = path.join(
    DOCS_DIR,
    "brand-explorer-wave13-public-six-geo-momentum-cleanup.md"
  );
  fs.writeFileSync(
    docPath,
    [
      `# Brand Explorer — Wave 13 Public Six Geo + Momentum Cleanup`,
      "",
      "Patches Geographic Footprint (≥3 region cards) and Recent Momentum (structured date + URL cards) for the six Active Wave 13 public brands.",
      "",
      "## Scope",
      "",
      `- In: ${WAVE13_PUBLIC_SIX_GEO_MOMENTUM_SLUGS.join(", ")}`,
      `- Out: SO/, House of Originals, Morgans Originals, Radisson Collection, protected 39 content`,
      "",
      "## Forbidden writes",
      "",
      "Brand Status, release fields, images, CV / Source Library / Registry, broad rewrites.",
      "",
      "## Commands",
      "",
      "```bash",
      "npm run brand-explorer-wave13-factory -- --stage public-six-geo-momentum-cleanup --dry-run",
      "npm run brand-explorer-wave13-factory -- --stage public-six-geo-momentum-cleanup --apply \\",
      "  --approve-wave13-public-six-geo-momentum-cleanup \\",
      "  --confirm-six-public-brand-scope \\",
      "  ...",
      "```",
      "",
      `Ready when green: \`${readyStatement}\``,
      "",
      `Last generated: ${report.generatedAt}`,
      "",
    ].join("\n"),
    "utf8"
  );

  return {
    ...report,
    paths: { jsonPath, mdPath, docPath },
    pass: brandResults.every((b) => !b.blocked),
    stopRecommended: brandResults.some((b) => b.blocked),
    airtableWrites: writePerformed,
  };
}
