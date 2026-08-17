/**
 * Wave 12 Stage 3 — Official Source Packs (read-only).
 * Writes report markdown/json + docs only. No Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  WAVE12_VERSION,
  WAVE12_SLUGS,
  WAVE12_BRAND_PLAN,
  WAVE12_PROTECTED_BASELINE_COUNT,
} from "./brand-explorer-wave12-factory-plan.js";
import {
  WAVE12_SOURCE_PACKS_VERSION,
  WAVE12_SOURCE_PACKS_BY_SLUG,
  TGS_AVOID_NOTE,
  getWave12SourcePack,
} from "./brand-explorer-wave12-source-packs-content.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function validatePack(pack) {
  const failures = [];
  if (!pack) {
    failures.push("missing_pack");
    return failures;
  }
  if (!pack.officialBrandPage?.url) failures.push("missing_official_brand_page");
  if (!/^https?:\/\//i.test(nz(pack.officialBrandPage?.url))) {
    failures.push("official_brand_page_not_http");
  }
  if (!(pack.propertyExamples || []).length) failures.push("missing_property_examples");
  for (const p of pack.propertyExamples || []) {
    if (!nz(p.propertyName) || !nz(p.matchKey)) failures.push(`property_missing_name:${p.url || "?"}`);
    if (p.propertyName !== p.matchKey) {
      // soft: matchKey should equal propertyName for name-based matching
      failures.push(`property_match_key_mismatch:${p.propertyName}`);
    }
    if (!["CALA", "International Reference"].includes(p.geographyLabel)) {
      failures.push(`property_geography_label:${p.propertyName}`);
    }
  }
  if (!(pack.recentMomentumCandidates || []).length) {
    failures.push("missing_recent_momentum_candidates");
  }
  for (const m of pack.recentMomentumCandidates || []) {
    if (!nz(m.dateLine)) failures.push(`momentum_missing_date:${m.title || "?"}`);
    if (!/^https?:\/\//i.test(nz(m.announcementUrl))) {
      failures.push(`momentum_missing_url:${m.title || "?"}`);
    }
    if (!nz(m.linkLabel)) failures.push(`momentum_missing_link_label:${m.title || "?"}`);
  }
  const tgs = pack.targetGuestSegmentsRecommendation?.recommended || [];
  if (!tgs.length) failures.push("missing_tgs_recommendation");
  const joined = tgs.join(", ");
  if (/Luxury\s*\/\s*Discerning/i.test(joined) && /\bLeisure\b/i.test(joined)) {
    failures.push("tgs_luxury_discerning_leisure_adjacency");
  }
  if (pack.writeAirtable === true) failures.push("write_airtable_must_be_false");
  if (pack.writeTargetGuestSegments === true) failures.push("tgs_write_must_be_false");
  return failures;
}

function renderPackMarkdown(pack, validationFailures) {
  const lines = [
    `# Wave 12 Source Pack — ${pack.name}`,
    "",
    `Slug: \`${pack.slug}\` · Record: \`${pack.recordId}\` · Parent: **${pack.parentPlatform}** (${pack.family})`,
    `Version: \`${WAVE12_SOURCE_PACKS_VERSION}\` · Airtable writes: **false**`,
    `CALA availability: **${pack.calaAvailability}**`,
    "",
    "## Brand lens",
    "",
    pack.lens,
    "",
    "## Official brand page",
    "",
    `- [${pack.officialBrandPage.label}](${pack.officialBrandPage.url}) — trust: ${pack.officialBrandPage.trust}`,
    "",
  ];

  if (pack.developmentPage) {
    lines.push(
      "## Development page",
      "",
      `- [${pack.developmentPage.label}](${pack.developmentPage.url}) — trust: ${pack.developmentPage.trust}`,
      pack.developmentPage.note ? `  - Note: ${pack.developmentPage.note}` : "",
      ""
    );
  } else {
    lines.push("## Development page", "", "_Not located as a dedicated brand development URL; use brand page + parent development hubs carefully labeled._", "");
  }

  lines.push("## Parent / platform context (labeled)", "");
  for (const p of pack.parentPlatformContext || []) {
    lines.push(`- [${p.label}](${p.url}) — ${p.note || "Parent/platform context only."}`);
  }
  lines.push("");

  lines.push(
    "## Property / opening examples",
    "",
    "URLs are matched by **property name** (`matchKey`), never by array index.",
    "",
    "| Property name (match key) | Geography | Market | Official URL |",
    "| --- | --- | --- | --- |"
  );
  for (const p of pack.propertyExamples || []) {
    lines.push(
      `| ${p.propertyName} | ${p.geographyLabel} | ${p.market} | ${p.url} |`
    );
  }
  lines.push("");

  lines.push(
    "## Recent Momentum candidates",
    "",
    "Each candidate includes **date + structured link label + announcement URL** (URL stays in source pack / trailing link field — not raw in public body prose).",
    "",
    "| Date | Title | Geography | Link label | Announcement URL |",
    "| --- | --- | --- | --- | --- |"
  );
  for (const m of pack.recentMomentumCandidates || []) {
    lines.push(
      `| ${m.dateLine} | ${m.title} | ${m.geographyLabel} | ${m.linkLabel} | ${m.announcementUrl} |`
    );
  }
  lines.push("");
  for (const m of pack.recentMomentumCandidates || []) {
    lines.push(`### ${m.title}`, "", m.summary, "");
  }

  lines.push("## Image source hints", "");
  for (const img of pack.imageSourceHints || []) {
    lines.push(
      `- [${img.label}](${img.url})${img.note ? ` — ${img.note}` : ""}`
    );
  }
  lines.push("");

  const tgs = pack.targetGuestSegmentsRecommendation || {};
  lines.push(
    "## Target Guest Segments recommendation (do not write yet)",
    "",
    `- Recommended: **${(tgs.recommended || []).join(", ")}**`,
    `- Avoid: ${(tgs.avoid || []).join("; ") || TGS_AVOID_NOTE}`,
    `- Rationale: ${tgs.rationale || ""}`,
    "",
    `> ${TGS_AVOID_NOTE}`,
    "",
    "## Distinguish from",
    "",
    ...(pack.distinguishFrom || []).map((s) => `- \`${s}\``),
    "",
    "## Steward notes",
    "",
    ...(pack.notes || []).map((n) => `- ${n}`),
    "",
    "## Validation",
    "",
    validationFailures.length
      ? validationFailures.map((f) => `- FAIL: \`${f}\``).join("\n")
      : "- PASS — pack structure meets Stage 3 acceptance checks",
    "",
    "## Protections",
    "",
    "- No Airtable writes",
    "- No Brand Status / release / CV / Source Library / Registry writes",
    "- No Target Guest Segments writes in this stage",
    "- No protected 27 Presentation/Basics writes",
    ""
  );
  return `${lines.filter((l) => l !== undefined).join("\n").replace(/\n{3,}/g, "\n\n")}\n`;
}

function packFileSlug(slug) {
  return `brand-explorer-wave12-source-pack-${slug}.md`;
}

export async function runWave12SourcePacks({ dryRun = true } = {}) {
  ensureDir(REPORTS_DIR);
  ensureDir(DOCS_DIR);

  const brands = [];
  const packPaths = [];
  let passCount = 0;

  for (const slug of WAVE12_SLUGS) {
    const plan = WAVE12_BRAND_PLAN[slug];
    const pack = getWave12SourcePack(slug);
    const failures = validatePack(pack);
    const ok = failures.length === 0;
    if (ok) passCount += 1;

    const mdName = packFileSlug(slug);
    const mdPath = path.join(REPORTS_DIR, mdName);
    if (pack) {
      fs.writeFileSync(mdPath, renderPackMarkdown(pack, failures), "utf8");
      packPaths.push(mdPath);
    }

    const calaCount = (pack?.propertyExamples || []).filter((p) => p.geographyLabel === "CALA").length;
    const intlCount = (pack?.propertyExamples || []).filter(
      (p) => p.geographyLabel === "International Reference"
    ).length;

    brands.push({
      slug,
      name: pack?.name || plan?.name || slug,
      recordId: pack?.recordId || null,
      parentPlatform: pack?.parentPlatform || plan?.parentPlatform || null,
      calaAvailability: pack?.calaAvailability || "unknown",
      hasOfficialBrandPage: Boolean(pack?.officialBrandPage?.url),
      hasDevelopmentPage: Boolean(pack?.developmentPage?.url),
      parentContextCount: (pack?.parentPlatformContext || []).length,
      propertyExampleCount: (pack?.propertyExamples || []).length,
      calaPropertyCount: calaCount,
      internationalReferencePropertyCount: intlCount,
      momentumCandidateCount: (pack?.recentMomentumCandidates || []).length,
      targetGuestSegmentsRecommended: pack?.targetGuestSegmentsRecommendation?.recommended || [],
      packPath: `reports/${mdName}`,
      validationPass: ok,
      validationFailures: failures,
      writeAirtable: false,
      writeTargetGuestSegments: false,
    });
  }

  const summary = {
    version: WAVE12_SOURCE_PACKS_VERSION,
    factoryVersion: WAVE12_VERSION,
    stage: "source-packs",
    generatedAt: new Date().toISOString(),
    dryRun: dryRun !== false,
    airtableWrites: false,
    brandStatusWrites: false,
    releaseFieldWrites: false,
    targetGuestSegmentsWrites: false,
    protected27Writes: false,
    protectedBaselineCount: WAVE12_PROTECTED_BASELINE_COUNT,
    expectedPacks: WAVE12_SLUGS.length,
    packsCreated: packPaths.length,
    packsPassingValidation: passCount,
    allPacksCreated: packPaths.length === WAVE12_SLUGS.length,
    allPacksValid: passCount === WAVE12_SLUGS.length,
    brandsWithOfficialBrandPage: brands.filter((b) => b.hasOfficialBrandPage).length,
    brandsWithCalaExamples: brands.filter((b) => b.calaPropertyCount > 0).length,
    brandsIntlReferenceOnly: brands.filter(
      (b) => b.calaPropertyCount === 0 && b.internationalReferencePropertyCount > 0
    ).length,
    nextStage: "tab-factory-build",
    wave12ResumeNote:
      "Stage 3 source packs complete (read-only). Do not start content generation until packs are reviewed; then proceed to Stage 4 tab-factory-build.",
  };

  const report = {
    ...summary,
    tgsAvoidRule: TGS_AVOID_NOTE,
    brands,
    packFiles: packPaths.map((p) => path.relative(ROOT, p).replace(/\\/g, "/")),
  };

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave12-source-pack-summary.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave12-source-pack-summary.md");
  const docPath = path.join(DOCS_DIR, "brand-explorer-wave12-source-packs.md");

  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const md = [
    "# Wave 12 — Official Source Pack Summary",
    "",
    `Version: \`${WAVE12_SOURCE_PACKS_VERSION}\` · Generated: ${summary.generatedAt}`,
    `Dry-run: **${summary.dryRun}** · Airtable writes: **false**`,
    "",
    "## Acceptance",
    "",
    `| Check | Result |`,
    `| --- | --- |`,
    `| Packs created | ${summary.packsCreated}/${summary.expectedPacks} |`,
    `| Packs valid | ${summary.packsPassingValidation}/${summary.expectedPacks} |`,
    `| Official brand pages | ${summary.brandsWithOfficialBrandPage}/12 |`,
    `| Brands with CALA examples | ${summary.brandsWithCalaExamples} |`,
    `| Brands International Reference only | ${summary.brandsIntlReferenceOnly} |`,
    `| Target Guest Segments written | **false** (recommendations only) |`,
    `| Protected 27 changes | **false** |`,
    "",
    summary.allPacksCreated && summary.allPacksValid
      ? "**Stage 3 PASS** — proceed to review, then Stage 4 tab-factory-build."
      : "**Stage 3 incomplete** — fix validation failures before content generation.",
    "",
    "## Per-brand packs",
    "",
    "| Brand | CALA avail | Props (CALA/Intl) | Momentum | TGS recommendation | Pack | Valid |",
    "| --- | --- | --- | ---: | --- | --- | --- |",
  ];
  for (const b of brands) {
    md.push(
      `| ${b.name} | ${b.calaAvailability} | ${b.calaPropertyCount}/${b.internationalReferencePropertyCount} | ${b.momentumCandidateCount} | ${(b.targetGuestSegmentsRecommended || []).join(", ")} | [\`${path.basename(b.packPath)}\`](${b.packPath}) | ${b.validationPass ? "PASS" : "FAIL"} |`
    );
  }
  md.push(
    "",
    "## Target Guest Segments rule",
    "",
    TGS_AVOID_NOTE,
    "",
    "Recommendations are recorded for a later approved Brand Basics patch stage — **not written now**.",
    "",
    "## Protections",
    "",
    "- No Airtable writes",
    "- No Brand Status / release field writes",
    "- No Company Validated / Source Library / Registry writes",
    "- No protected 27 Presentation or Basics changes",
    "- No Wave 12 content generation in this stage",
    "",
    "## Next",
    "",
    summary.wave12ResumeNote,
    ""
  );
  fs.writeFileSync(mdPath, `${md.join("\n")}\n`, "utf8");

  const doc = [
    "# Brand Explorer — Wave 12 Official Source Packs",
    "",
    `Version: \`${WAVE12_SOURCE_PACKS_VERSION}\``,
    "",
    "## Purpose",
    "",
    "Stage 3 of the Wave 12 factory builds **official source packs** for 12 Under Review brands before any tab-factory content generation.",
    "",
    "## Source hierarchy",
    "",
    "1. Brand-specific official brand page",
    "2. Brand-specific development page where available",
    "3. Official property pages (match by **property name**, not array index)",
    "4. Official parent-company pages — **parent/platform context only**",
    "5. Credible announcement / opening / development sources",
    "6. Image sources tied to official brand or property pages",
    "",
    "## Geography labels",
    "",
    "- **CALA** — Caribbean & Latin America examples preferred when available",
    "- **International Reference** — non-CALA examples explicitly labeled",
    "",
    "## Target Guest Segments",
    "",
    TGS_AVOID_NOTE,
    "",
    "Stage 3 records recommendations only. Do not patch Brand Basics until an approved later stage.",
    "",
    "## Command",
    "",
    "```bash",
    "npm run brand-explorer-wave12-factory -- --stage source-packs --dry-run",
    "```",
    "",
    "## Outputs",
    "",
    "- `reports/brand-explorer-wave12-source-pack-<slug>.md` (12 files)",
    "- `reports/brand-explorer-wave12-source-pack-summary.{json,md}`",
    "- This doc",
    "",
    "## Next stage",
    "",
    "Stage 4 — `tab-factory-build` (only after source pack review).",
    "",
  ];
  fs.writeFileSync(docPath, `${doc.join("\n")}\n`, "utf8");

  return {
    version: WAVE12_VERSION,
    stage: "source-packs",
    generatedAt: summary.generatedAt,
    dryRun: summary.dryRun,
    pass: summary.allPacksCreated && summary.allPacksValid,
    stopRecommended: !(summary.allPacksCreated && summary.allPacksValid),
    airtableWrites: false,
    summary: {
      packsCreated: summary.packsCreated,
      packsPassingValidation: summary.packsPassingValidation,
      allPacksValid: summary.allPacksValid,
      brandsWithCalaExamples: summary.brandsWithCalaExamples,
      nextStage: summary.nextStage,
    },
    paths: {
      jsonPath,
      mdPath,
      docPath,
      packPaths,
    },
    report,
  };
}
