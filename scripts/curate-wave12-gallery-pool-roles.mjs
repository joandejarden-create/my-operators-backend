#!/usr/bin/env node
/**
 * Curate Wave 12 gallery-pool fixture `role` fields.
 *
 * Strategy:
 * 1. detectVisualCategory (Accor/filename cues)
 * 2. Expanded Marriott / Hilton / EVEN / Bunkhouse filename heuristics
 * 3. Optional Scene7 stem override map (fixtures/wave12-gallery-role-overrides.json)
 * 4. Remaining unknowns stay unassigned (reported) — do not invent Exterior/Lobby/F&B
 *
 *   node scripts/curate-wave12-gallery-pool-roles.mjs --dry-run
 *   node scripts/curate-wave12-gallery-pool-roles.mjs --write
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WAVE12_SLUGS } from "../lib/partner-intelligence/brand-explorer-wave12-factory-plan.js";
import {
  IMAGE_ROLES,
  DEFAULT_GALLERY_ROLE_SEQUENCE,
  detectVisualCategory,
} from "../lib/partner-intelligence/brand-explorer-image-role-match.js";
import { buildImageIdentity } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURES = path.join(ROOT, "fixtures");
const REPORTS = path.join(ROOT, "reports");
const OVERRIDE_PATH = path.join(FIXTURES, "wave12-gallery-role-overrides.json");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function scene7Stem(url) {
  const id = buildImageIdentity(url).sourceImageId || "";
  return id.replace(/^(scene7|file):/i, "");
}

/**
 * Filename / URL cues beyond detectVisualCategory — Marriott DAM + Hilton stories + EVEN.
 */
export function inferRoleFromPathHints(imageUrl = "", caption = "") {
  const blob = `${imageUrl} ${caption}`.toLowerCase();
  // Avoid brand-name traps like "holiday-inn-express-and-suites-…"
  const blobNoBrandSuites = blob.replace(/and[\s_-]?suites/gi, " ");

  if (
    /guest[\s_-]?room|guestrooms|bedroom|junior[\s_-]?suite|king[\s_-]?premium|in[\s_-]?room|bathroom|gdlac-guestroom|guestroom-|one[\s_-]?queen[\s_-]?bed|standard[\s_-]?king|suite[\s_-]?parlor|suite-000/i.test(
      blobNoBrandSuites
    ) ||
    /(?:^|[^a-z])suite(?:[^a-z]|$)/i.test(blobNoBrandSuites)
  ) {
    return IMAGE_ROLES.guest_room_suite;
  }
  if (
    /restaurant|dining|bar[\s_-]|_bar|breakfast|fb-|f&b|culinary|lobby-bar|bistro|cocktails|shelbys|communal[\s_-]?table|pool[\s_-]?bar|benno[\s_-]?dinner|happy[\s_-]?hour|grapefruit[\s_-]?club/i.test(
      blob
    )
  ) {
    return IMAGE_ROLES.food_beverage_experience;
  }
  if (
    /lobby|reception|lounge|atrium|desk-|front[\s_-]?desk|check[\s_-]?in|media[\s_-]?pods|the[\s_-]?commons|frontdesk/i.test(
      blob
    )
  ) {
    return IMAGE_ROLES.public_space_lobby;
  }
  if (
    /exterior|entrance|facade|façade|arrival|street|signage|hero[\s_-]?daytime|hero[\s_-]?night|building[\s_-]?exterior|airport[\s_-]?shuttle|drone|motor[\s_-]?lobby/i.test(
      blob
    )
  ) {
    return IMAGE_ROLES.exterior_arrival;
  }
  if (/pool|spa|wellness|fitness|gym|athletic|swimming|plunge[\s_-]?pool|rooftop(?!.*bar)/i.test(blob)) {
    if (/pool|spa|wellness|fitness|gym|athletic|swimming|plunge/i.test(blob)) {
      return IMAGE_ROLES.wellness_pool_spa;
    }
    return IMAGE_ROLES.property_setting;
  }
  if (
    /patio|garden|terrace|destination|skyline|harbor|harbour|mountain[\s_-]?view|city[\s_-]?view|neighborhood|lifestyle\d|generalproperty|landscape|airport[\s_-]?hotel/i.test(
      blob
    )
  ) {
    return IMAGE_ROLES.property_setting;
  }
  if (/meeting|ballroom|conference|boardroom|event[\s_-]?space|folklore[\s_-]?boardroom/i.test(blob)) {
    return IMAGE_ROLES.meeting_event;
  }
  if (
    /design[\s_-]?detail|details?-?\d|staircase|corridor|hallway|materiality|bikes|velvet[\s_-]?room|mural|neon|vase|bathroom|accessible[\s_-]?bathroom/i.test(
      blob
    )
  ) {
    return IMAGE_ROLES.design_detail;
  }
  if (/room[\s_-]?ocean|ocean[\s_-]?king|poolside[\s_-]?king|power[\s_-]?down|get[\s_-]?ready|blaak[\s_-]?king|blaak[\s_-]?couch/i.test(blob)) {
    return IMAGE_ROLES.guest_room_suite;
  }
  return null;
}

function loadOverrides() {
  if (!fs.existsSync(OVERRIDE_PATH)) return {};
  try {
    const raw = JSON.parse(fs.readFileSync(OVERRIDE_PATH, "utf8")) || {};
    delete raw._meta;
    return raw;
  } catch {
    return {};
  }
}

function resolveRole(row, overridesBySlug, slug) {
  const url = nz(row.imageUrl);
  const caption = nz(row.caption);
  const stem = scene7Stem(url);

  const slugOverrides = overridesBySlug[slug] || {};
  if (stem && slugOverrides[stem]) return { role: slugOverrides[stem], source: "override_stem" };
  if (url && slugOverrides[url]) return { role: slugOverrides[url], source: "override_url" };

  const detected = detectVisualCategory({
    imageUrl: url,
    sourcePageUrl: nz(row.sourcePageUrl),
    title: caption,
    altText: caption,
  });
  if (detected.category && detected.category !== IMAGE_ROLES.unknown) {
    return { role: detected.category, source: `detect:${detected.confidence}` };
  }

  const hint = inferRoleFromPathHints(url, caption);
  if (hint) return { role: hint, source: "path_hint" };

  if (nz(row.role) && Object.values(IMAGE_ROLES).includes(nz(row.role))) {
    return { role: nz(row.role), source: "existing" };
  }

  return { role: null, source: "unknown" };
}

function analyzeBrand(slug, overridesBySlug) {
  const fixturePath = path.join(FIXTURES, `wave12-${slug}-gallery-pool.json`);
  if (!fs.existsSync(fixturePath)) {
    return { slug, missing: true, rows: [], nextRows: [], coverage: null };
  }
  const rows = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const nextRows = [];
  const byRole = {};
  const sources = {};
  const unknownStems = new Set();

  for (const row of rows) {
    const { role, source } = resolveRole(row, overridesBySlug, slug);
    sources[source] = (sources[source] || 0) + 1;
    if (role) byRole[role] = (byRole[role] || 0) + 1;
    else unknownStems.add(scene7Stem(row.imageUrl) || row.imageUrl);

    nextRows.push({
      ...row,
      ...(role ? { role } : (() => {
        const { role: _drop, ...rest } = row;
        return rest;
      })()),
    });
    // Ensure role key is set/cleared cleanly
    if (role) nextRows[nextRows.length - 1].role = role;
    else delete nextRows[nextRows.length - 1].role;
  }

  const distinctAssigned = new Set(
    nextRows.filter((r) => r.role).map((r) => buildImageIdentity(r.imageUrl).duplicateGroupId)
  );
  const sequenceCoverage = DEFAULT_GALLERY_ROLE_SEQUENCE.map((role) => ({
    role,
    count: byRole[role] || 0,
    ok: (byRole[role] || 0) >= 1,
  }));

  return {
    slug,
    missing: false,
    fixturePath,
    rows,
    nextRows,
    byRole,
    sources,
    unknownStemCount: unknownStems.size,
    unknownStems: [...unknownStems].slice(0, 24),
    sequenceCoverage,
    canFillGallerySequence: sequenceCoverage.every((c) => c.ok),
    assignedDistinct: distinctAssigned.size,
    changed: JSON.stringify(rows) !== JSON.stringify(nextRows),
  };
}

function main() {
  const argv = process.argv.slice(2);
  const write = argv.includes("--write");
  const dryRun = !write;
  const brandIdx = argv.indexOf("--brands");
  const brands =
    brandIdx >= 0 && argv[brandIdx + 1]
      ? argv[brandIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
      : [...WAVE12_SLUGS];

  const overrides = loadOverrides();
  const results = brands.map((slug) => analyzeBrand(slug, overrides));

  if (write) {
    for (const r of results) {
      if (r.missing || !r.changed) continue;
      fs.writeFileSync(r.fixturePath, `${JSON.stringify(r.nextRows, null, 2)}\n`, "utf8");
    }
  }

  const report = {
    version: "wave12-gallery-pool-role-curation-v1",
    dryRun,
    write,
    overridePath: OVERRIDE_PATH,
    overrideExists: fs.existsSync(OVERRIDE_PATH),
    brands: results.map((r) => ({
      slug: r.slug,
      missing: !!r.missing,
      rowCount: r.nextRows?.length || 0,
      changed: !!r.changed,
      byRole: r.byRole || {},
      sources: r.sources || {},
      canFillGallerySequence: !!r.canFillGallerySequence,
      sequenceCoverage: r.sequenceCoverage || [],
      unknownStemCount: r.unknownStemCount || 0,
      unknownStemsSample: r.unknownStems || [],
    })),
    summary: {
      brands: results.length,
      canFillAllSixRoles: results.filter((r) => r.canFillGallerySequence).map((r) => r.slug),
      cannotFillAllSixRoles: results.filter((r) => !r.missing && !r.canFillGallerySequence).map((r) => r.slug),
      totalUnknownStems: results.reduce((n, r) => n + (r.unknownStemCount || 0), 0),
    },
  };

  fs.mkdirSync(REPORTS, { recursive: true });
  const jsonPath = path.join(REPORTS, "brand-explorer-wave12-gallery-pool-role-curation.json");
  const mdPath = path.join(REPORTS, "brand-explorer-wave12-gallery-pool-role-curation.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const lines = [
    `# Wave 12 Gallery Pool Role Curation`,
    ``,
    `- Dry-run: ${dryRun}`,
    `- Write fixtures: ${write}`,
    `- Brands that can fill all 6 gallery roles: ${report.summary.canFillAllSixRoles.join(", ") || "none"}`,
    `- Brands still short on roles: ${report.summary.cannotFillAllSixRoles.join(", ") || "none"}`,
    `- Unknown stems remaining: ${report.summary.totalUnknownStems}`,
    ``,
  ];
  for (const b of report.brands) {
    lines.push(`## ${b.slug}`);
    lines.push(`- Rows: ${b.rowCount} · changed: ${b.changed} · canFill6: ${b.canFillGallerySequence}`);
    lines.push(`- Roles: ${JSON.stringify(b.byRole)}`);
    lines.push(`- Sources: ${JSON.stringify(b.sources)}`);
    if (b.unknownStemCount) {
      lines.push(`- Unknown stems (${b.unknownStemCount}): ${b.unknownStemsSample.slice(0, 8).join(", ")}`);
    }
    lines.push("");
  }
  fs.writeFileSync(mdPath, `${lines.join("\n")}\n`, "utf8");

  console.log(`Wrote ${mdPath}`);
  console.log(
    `canFill6=${report.summary.canFillAllSixRoles.length}/${results.length} unknownStems=${report.summary.totalUnknownStems} write=${write}`
  );
  if (report.summary.cannotFillAllSixRoles.length) {
    console.log(`Still short: ${report.summary.cannotFillAllSixRoles.join(", ")}`);
  }
}

main();
