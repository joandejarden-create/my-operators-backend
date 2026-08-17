/**
 * Wave 16A Stage 1 — Source foundation + differentiation architecture.
 * READ ONLY: local reports/docs/fixtures only. Zero Airtable writes.
 *
 * Usage:
 *   node scripts/brand-explorer-wave16a-stage1-source-foundation.mjs --dry-run
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  WAVE16A_VERSION,
  WAVE16A_PROTECTED_BASELINE_COUNT,
  WAVE16A_PROTECTED_BASELINE_ID,
  WAVE16A_SLUGS,
  WAVE16A_IDENTITIES,
  WAVE16A_FLEX_HOLD,
  WAVE16A_OUTSIDE_COHORT,
  WAVE16A_STATUS_FROM,
  WAVE16A_PARENT_PLATFORM,
} from "./brand-explorer-wave16a-factory-plan.js";
import {
  WAVE16A_STAGE1_VERSION,
  WAVE16A_PACKS_BY_SLUG,
  WAVE16A_RECOMMENDED_BUILD_ORDER,
  getWave16aStage1Pack,
} from "./brand-explorer-wave16a-stage1-source-content.js";

export const READY_FULL = "wave16a_stage1_source_foundation_ready_for_controlled_build";
export const READY_PARTIAL = "wave16a_stage1_partial_ready_subset_identified";
export const READY_BLOCKED = "wave16a_stage1_blocked";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS = path.join(ROOT, "reports");
const DOCS = path.join(ROOT, "docs", "data-intelligence");
const PACK_DIR = path.join(REPORTS, "brand-explorer-wave16a-stage1-brand-packs");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function mockRes() {
  return {
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
}

function writeJson(abs, data) {
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, `${JSON.stringify(data, null, 2)}\n`);
}

function writeMd(abs, text) {
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, text.endsWith("\n") ? text : `${text}\n`);
}

async function fetchBrand(recordId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`Brand fetch failed for ${recordId}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

async function revalidateIdentities(universe) {
  const rows = [];
  for (const slug of WAVE16A_SLUGS) {
    const expected = WAVE16A_IDENTITIES[slug];
    const live = await fetchBrand(expected.recordId);
    const status = nz(live.brandStatus || live.status);
    const name = nz(live.name || live.brandName);
    const parent = nz(live.parentCompany);
    const issues = [];
    if (isBrandStatusActive(status)) {
      issues.push("STATUS_CHANGED_SINCE_READINESS_AUDIT");
    } else if (status !== WAVE16A_STATUS_FROM) {
      issues.push(`unexpected_status:${status || "(empty)"}`);
    }
    if (name && name !== expected.exactBrandBasicsName) {
      issues.push(`name_mismatch:live=${name};expected=${expected.exactBrandBasicsName}`);
    }
    if (universe.slugSet.has(slug)) {
      issues.push("unexpectedly_in_active_universe");
    }
    if (WAVE16A_OUTSIDE_COHORT.includes(slug)) {
      issues.push("slug_listed_outside_cohort");
    }
    rows.push({
      slug,
      suppliedName: expected.suppliedName,
      exactBrandBasicsName: expected.exactBrandBasicsName,
      liveName: name,
      recordId: expected.recordId,
      parentCompany: parent || WAVE16A_PARENT_PLATFORM,
      brandStatus: status,
      readinessScore: expected.readinessScore,
      inActiveUniverse: universe.slugSet.has(slug),
      ok: issues.length === 0,
      issues,
      classification: issues.includes("STATUS_CHANGED_SINCE_READINESS_AUDIT")
        ? "STATUS_CHANGED_SINCE_READINESS_AUDIT"
        : issues.length
          ? "IDENTITY_ISSUE"
          : "RESOLVED_UNDER_REVIEW",
    });
  }
  return rows;
}

async function confirmFlex(universe) {
  const live = await fetchBrand(WAVE16A_FLEX_HOLD.recordId);
  const status = nz(live.brandStatus || live.status);
  return {
    ...WAVE16A_FLEX_HOLD,
    brandStatus: status,
    inActiveUniverse: universe.slugSet.has(WAVE16A_FLEX_HOLD.slug),
    protectedHoldOk:
      status === WAVE16A_STATUS_FROM &&
      !isBrandStatusActive(status) &&
      !universe.slugSet.has(WAVE16A_FLEX_HOLD.slug),
    scoredForWave16A: false,
    promotionRecommended: false,
  };
}

function buildDifferentiationMatrix(packs) {
  return {
    version: `${WAVE16A_STAGE1_VERSION}-differentiation`,
    generatedAt: new Date().toISOString(),
    brands: WAVE16A_SLUGS.map((slug) => {
      const p = packs[slug];
      return {
        slug,
        name: p.name,
        ...p.differentiation,
      };
    }),
  };
}

function buildPropertyCandidates(packs) {
  return {
    version: `${WAVE16A_STAGE1_VERSION}-properties`,
    generatedAt: new Date().toISOString(),
    note: "Stage 1 candidates only — no image materialization, no Presentation writes.",
    brands: WAVE16A_SLUGS.map((slug) => {
      const p = packs[slug];
      return {
        slug,
        name: p.name,
        propertyCandidates: p.propertyCandidates,
        strongExampleCount: (p.propertyCandidates || []).filter((x) =>
          (x.roles || []).includes("strongExample")
        ).length,
        galleryPoolCount: (p.propertyCandidates || []).length,
      };
    }),
  };
}

function buildMomentumCandidates(packs) {
  return {
    version: `${WAVE16A_STAGE1_VERSION}-momentum`,
    generatedAt: new Date().toISOString(),
    note: "READ ONLY candidates — do NOT write Recent Momentum Airtable fields.",
    brands: WAVE16A_SLUGS.map((slug) => {
      const p = packs[slug];
      return {
        slug,
        name: p.name,
        momentumCandidates: p.momentumCandidates || [],
        recommendedCount: (p.momentumCandidates || []).filter((m) => m.recommendedForLater).length,
      };
    }),
  };
}

function buildFieldSupportMatrix(packs) {
  return {
    version: `${WAVE16A_STAGE1_VERSION}-field-support`,
    generatedAt: new Date().toISOString(),
    brands: WAVE16A_SLUGS.map((slug) => {
      const p = packs[slug];
      return { slug, name: p.name, fieldSupport: p.fieldSupport };
    }),
  };
}

function mdDifferentiation(matrix) {
  const lines = [
    `# Wave 16A Stage 1 — Sibling Differentiation Matrix`,
    ``,
    `> READ ONLY · no Airtable writes · no owner-facing copy`,
    ``,
  ];
  for (const b of matrix.brands) {
    lines.push(`## ${b.name} (\`${b.slug}\`)`);
    lines.push(``);
    lines.push(`- **Core identity:** ${b.coreIdentity}`);
    lines.push(`- **Owner proposition:** ${b.ownerProposition}`);
    lines.push(`- **Typical asset fit:** ${b.typicalAssetFit}`);
    lines.push(`- **Service model:** ${b.serviceModel}`);
    lines.push(`- **Experience differentiator:** ${b.experienceDifferentiator}`);
    lines.push(`- **Development differentiator:** ${b.developmentDifferentiator}`);
    lines.push(`- **Conversion / new-build:** ${b.conversionNewBuildOrientation}`);
    lines.push(`- **Contamination siblings:** ${(b.contaminationSiblings || []).join(", ")}`);
    lines.push(`- **Must NOT migrate language from:** ${(b.mustNotMigrateLanguageFrom || []).join("; ")}`);
    lines.push(``);
    lines.push(`| Sibling | Must remain distinct because | Forbidden substitutions |`);
    lines.push(`| --- | --- | --- |`);
    for (const r of b.siblingRules || []) {
      lines.push(
        `| ${r.sibling} | ${r.mustRemainDistinctBecause} | ${(r.forbiddenSubstitutions || []).join("; ")} |`
      );
    }
    lines.push(``);
  }
  return lines.join("\n");
}

function mdBuildSequencing({ order, packs, identities }) {
  const lines = [
    `# Wave 16A Stage 1 — Recommended Internal Build Sequencing`,
    ``,
    `> Risk-adjusted order (not readiness-score order). Test writer on LOW-risk brands first.`,
    ``,
    `| Order | Brand | Slug | Build risk | Readiness | Stage1 ready |`,
    `| ---: | --- | --- | --- | ---: | --- |`,
  ];
  order.forEach((slug, i) => {
    const p = packs[slug];
    const id = identities.find((x) => x.slug === slug);
    lines.push(
      `| ${i + 1} | ${p.name} | \`${slug}\` | **${p.buildRisk}** | ${id?.readinessScore ?? "—"} | ${p.stage1Ready ? "yes" : "no"} |`
    );
  });
  lines.push(``);
  lines.push(`## Risk drivers`);
  lines.push(``);
  for (const slug of order) {
    const p = packs[slug];
    lines.push(`- **${p.name} (${p.buildRisk}):** ${(p.buildRiskDrivers || []).join("; ")}`);
  }
  lines.push(``);
  lines.push(`## Outside cohort (untouched)`);
  lines.push(``);
  lines.push(`- Four Points Flex by Sheraton — PROTECTED_HOLD`);
  lines.push(`- Marriott Conference Center — TAXONOMY_REVIEW`);
  lines.push(`- Wave 16B brands — untouched`);
  lines.push(`- House / Morgans / Radisson Collection — excluded`);
  lines.push(``);
  return lines.join("\n");
}

function mdFoundation(report) {
  const lines = [
    `# Brand Explorer Wave 16A — Stage 1 Source Foundation`,
    ``,
    `> **Ready:** \`${report.readyStatement}\``,
    `> **Generated:** ${report.generatedAt}`,
    `> **Mode:** READ ONLY · Airtable writes: **false**`,
    ``,
    `## Preflight`,
    ``,
    `| Check | Result |`,
    `| --- | --- |`,
    `| Active universe | **${report.preflight.activeUniverseCount}** (expected ${WAVE16A_PROTECTED_BASELINE_COUNT}) |`,
    `| Protected baseline | \`${WAVE16A_PROTECTED_BASELINE_ID}\` |`,
    `| Identities revalidated | ${report.preflight.identitiesOk ? "11/11 PASS" : "ISSUES"} |`,
    `| All Under Review | ${report.preflight.allUnderReview ? "yes" : "NO"} |`,
    `| Four Points Flex hold | ${report.flex.protectedHoldOk ? "PASS" : "FAIL"} |`,
    `| Airtable writes | **0** |`,
    ``,
    `## Identity`,
    ``,
    `| Brand | Exact Basics | Slug | Record ID | Status | OK |`,
    `| --- | --- | --- | --- | --- | --- |`,
  ];
  for (const r of report.identities) {
    lines.push(
      `| ${r.suppliedName} | ${r.exactBrandBasicsName} | \`${r.slug}\` | \`${r.recordId}\` | ${r.brandStatus} | ${r.ok ? "yes" : r.issues.join("; ")} |`
    );
  }
  lines.push(``);
  lines.push(`## Stage 1 readiness by brand`);
  lines.push(``);
  lines.push(`| Brand | Build risk | Stage1 ready | Props | Momentum recs | Blockers |`);
  lines.push(`| --- | --- | --- | ---: | ---: | --- |`);
  for (const slug of WAVE16A_RECOMMENDED_BUILD_ORDER) {
    const p = report.packs[slug];
    lines.push(
      `| ${p.name} | **${p.buildRisk}** | ${p.stage1Ready ? "yes" : "no"} | ${(p.propertyCandidates || []).length} | ${(p.momentumCandidates || []).filter((m) => m.recommendedForLater).length} | ${(p.stage1Blockers || []).join("; ") || "—"} |`
    );
  }
  lines.push(``);
  lines.push(`## Recommended build order`);
  lines.push(``);
  report.recommendedBuildOrder.forEach((slug, i) => {
    lines.push(`${i + 1}. **${report.packs[slug].name}** (\`${slug}\`) — ${report.packs[slug].buildRisk}`);
  });
  lines.push(``);
  lines.push(`## Temporarily removed from build eligibility`);
  lines.push(``);
  const removed = report.temporarilyRemoved || [];
  if (!removed.length) lines.push(`- _(none)_`);
  else for (const r of removed) lines.push(`- **${r.slug}** — ${r.reason}`);
  lines.push(``);
  lines.push(`## Four Points Flex`);
  lines.push(``);
  lines.push("```json");
  lines.push(JSON.stringify(report.flex, null, 2));
  lines.push("```");
  lines.push(``);
  lines.push(`## Wave 16B / outside cohort`);
  lines.push(``);
  lines.push(`Untouched: ${(report.outsideCohortUntouched || []).join(", ")}`);
  lines.push(``);
  lines.push(`## Next stage`);
  lines.push(``);
  lines.push(report.nextStage || "Controlled factory-preview / tab-factory build for LOW-risk brands first.");
  lines.push(``);
  lines.push(`## Artifacts`);
  lines.push(``);
  for (const [k, v] of Object.entries(report.artifacts || {})) {
    lines.push(`- ${k}: \`${v}\``);
  }
  lines.push(``);
  return lines.join("\n");
}

function perBrandMd(pack, identity) {
  return [
    `# Wave 16A Stage 1 — ${pack.name}`,
    ``,
    `> READ ONLY source package · slug \`${pack.slug}\` · record \`${identity.recordId}\``,
    ``,
    `## Identity`,
    ``,
    `- Supplied: ${identity.suppliedName}`,
    `- Exact Basics: ${identity.exactBrandBasicsName}`,
    `- Status expected: Under Review`,
    `- Parent: ${WAVE16A_PARENT_PLATFORM}`,
    `- Build risk: **${pack.buildRisk}**`,
    `- Stage1 ready: ${pack.stage1Ready}`,
    ``,
    `## Positioning inputs`,
    ``,
    "```json",
    JSON.stringify(pack.positioning, null, 2),
    "```",
    ``,
    `## Official references (${(pack.officialReferences || []).length})`,
    ``,
    ...(pack.officialReferences || []).map(
      (r) =>
        `- **${r.type}** — ${r.title} — \`${r.url}\` — ${r.brandSpecificVsParent} — conf ${r.confidence}`
    ),
    ``,
    `## Development references (${(pack.developmentReferences || []).length})`,
    ``,
    ...(pack.developmentReferences || []).map(
      (r) => `- **${r.type}** — ${r.title} — \`${r.url}\` — conf ${r.confidence}`
    ),
    ``,
    `## Property candidates (${(pack.propertyCandidates || []).length})`,
    ``,
    ...(pack.propertyCandidates || []).map(
      (p) =>
        `- **${p.name}** — ${p.city}, ${p.country} (${p.region}) — roles: ${(p.roles || []).join(", ")} — images: ${p.imageAvailability}`
    ),
    ``,
    `## Momentum candidates (DO NOT WRITE)`,
    ``,
    ...(pack.momentumCandidates || []).length
      ? (pack.momentumCandidates || []).map(
          (m) =>
            `- ${m.date || "date TBD"} — ${m.event} — ${m.geography} — recommended=${m.recommendedForLater}`
        )
      : ["- _(none recommended — cleanly unavailable or needs research)_"],
    ``,
    `## Owner-decision questions`,
    ``,
    ...(pack.ownerDecisionQuestions || []).map((q, i) => `${i + 1}. ${q}`),
    ``,
    `## Field support`,
    ``,
    "```json",
    JSON.stringify(pack.fieldSupport, null, 2),
    "```",
    ``,
    `## Differentiation summary`,
    ``,
    `- Core: ${pack.differentiation?.coreIdentity}`,
    `- Contamination siblings: ${(pack.differentiation?.contaminationSiblings || []).join(", ")}`,
    ``,
    `## Notes`,
    ``,
    ...(pack.notes || []).map((n) => `- ${n}`),
    ``,
  ].join("\n");
}

export async function runWave16aStage1SourceFoundation({ argv = [] } = {}) {
  const generatedAt = new Date().toISOString();
  console.log(`[${WAVE16A_STAGE1_VERSION}] Stage 1 source foundation (read-only)`);

  const universe = await loadActiveUniverse({ includeDetails: false });
  if (universe.totalCount !== WAVE16A_PROTECTED_BASELINE_COUNT) {
    const blocked = {
      version: WAVE16A_STAGE1_VERSION,
      waveVersion: WAVE16A_VERSION,
      generatedAt,
      readyStatement: READY_BLOCKED,
      airtableWrites: false,
      writePerformed: false,
      preflight: {
        activeUniverseCount: universe.totalCount,
        expected: WAVE16A_PROTECTED_BASELINE_COUNT,
        ok: false,
        stopReason: "active_universe_mismatch",
      },
    };
    writeJson(path.join(REPORTS, "brand-explorer-wave16a-stage1-source-foundation.json"), blocked);
    writeMd(
      path.join(REPORTS, "brand-explorer-wave16a-stage1-source-foundation.md"),
      `# Wave 16A Stage 1 BLOCKED\n\nActive universe ${universe.totalCount} ≠ ${WAVE16A_PROTECTED_BASELINE_COUNT}.\n`
    );
    return blocked;
  }

  const identities = await revalidateIdentities(universe);
  const flex = await confirmFlex(universe);
  const identitiesOk = identities.every((r) => r.ok);
  const allUnderReview = identities.every((r) => r.brandStatus === WAVE16A_STATUS_FROM);

  const packs = {};
  const missingPacks = [];
  for (const slug of WAVE16A_SLUGS) {
    try {
      packs[slug] = getWave16aStage1Pack(slug);
    } catch (err) {
      missingPacks.push({ slug, error: err.message });
    }
  }

  const temporarilyRemoved = identities
    .filter((r) => !r.ok)
    .map((r) => ({ slug: r.slug, reason: r.issues.join("; ") }));

  const stage1ReadyBrands = WAVE16A_SLUGS.filter(
    (slug) => packs[slug]?.stage1Ready === true && !temporarilyRemoved.some((t) => t.slug === slug)
  );

  const recommendedBuildOrder = (
    WAVE16A_RECOMMENDED_BUILD_ORDER.length
      ? WAVE16A_RECOMMENDED_BUILD_ORDER
      : WAVE16A_SLUGS
  ).filter((slug) => stage1ReadyBrands.includes(slug) || packs[slug]);

  let readyStatement = READY_FULL;
  if (!identitiesOk || !flex.protectedHoldOk || missingPacks.length) {
    readyStatement = READY_BLOCKED;
  } else if (stage1ReadyBrands.length < WAVE16A_SLUGS.length) {
    readyStatement = READY_PARTIAL;
  }

  const differentiation = buildDifferentiationMatrix(packs);
  const properties = buildPropertyCandidates(packs);
  const momentum = buildMomentumCandidates(packs);
  const fieldSupport = buildFieldSupportMatrix(packs);

  const artifacts = {
    foundationJson: "reports/brand-explorer-wave16a-stage1-source-foundation.json",
    foundationMd: "reports/brand-explorer-wave16a-stage1-source-foundation.md",
    differentiationJson: "reports/brand-explorer-wave16a-stage1-differentiation-matrix.json",
    differentiationMd: "reports/brand-explorer-wave16a-stage1-differentiation-matrix.md",
    propertiesJson: "reports/brand-explorer-wave16a-stage1-property-candidates.json",
    momentumJson: "reports/brand-explorer-wave16a-stage1-momentum-candidates.json",
    fieldSupportJson: "reports/brand-explorer-wave16a-stage1-field-support-matrix.json",
    sequencingMd: "reports/brand-explorer-wave16a-stage1-build-sequencing.md",
    docs: "docs/data-intelligence/brand-explorer-wave16a-stage1-source-foundation.md",
    perBrandDir: "reports/brand-explorer-wave16a-stage1-brand-packs/",
  };

  const report = {
    version: WAVE16A_STAGE1_VERSION,
    waveVersion: WAVE16A_VERSION,
    generatedAt,
    readyStatement,
    airtableWrites: false,
    writePerformed: false,
    argvNote: argv.includes("--dry-run") ? "dry-run" : "local-artifact-generation",
    preflight: {
      activeUniverseCount: universe.totalCount,
      expected: WAVE16A_PROTECTED_BASELINE_COUNT,
      ok: universe.totalCount === WAVE16A_PROTECTED_BASELINE_COUNT && identitiesOk && flex.protectedHoldOk,
      identitiesOk,
      allUnderReview,
      protectedBaseline: WAVE16A_PROTECTED_BASELINE_ID,
      missingPacks,
    },
    identities,
    flex,
    outsideCohortUntouched: [...WAVE16A_OUTSIDE_COHORT],
    packs,
    recommendedBuildOrder,
    buildRiskByBrand: Object.fromEntries(
      WAVE16A_SLUGS.map((slug) => [slug, packs[slug]?.buildRisk || null])
    ),
    stage1ReadyByBrand: Object.fromEntries(
      WAVE16A_SLUGS.map((slug) => [slug, packs[slug]?.stage1Ready === true])
    ),
    temporarilyRemoved,
    nextStage:
      readyStatement === READY_FULL
        ? "Next: controlled factory-preview / tab-factory build starting with LOW-risk brands (Fairfield → Four Points → Delta). Do not promote or release."
        : readyStatement === READY_PARTIAL
          ? "Next: proceed only for stage1Ready subset; research blockers for remaining brands."
          : "Resolve preflight / identity / Flex hold issues before any build.",
    artifacts,
    guardrails: {
      noBrandStatusWrites: true,
      noReleaseWrites: true,
      noProtectedFieldWrites: true,
      noPresentationWrites: true,
      noRecentMomentumWrites: true,
      noImageWrites: true,
      noActive62Writes: true,
      flexUntouched: true,
      wave16bUntouched: true,
      conferenceCenterOutside: true,
    },
  };

  writeJson(path.join(REPORTS, "brand-explorer-wave16a-stage1-source-foundation.json"), report);
  writeMd(path.join(REPORTS, "brand-explorer-wave16a-stage1-source-foundation.md"), mdFoundation(report));
  writeMd(path.join(DOCS, "brand-explorer-wave16a-stage1-source-foundation.md"), mdFoundation(report));
  writeJson(path.join(REPORTS, "brand-explorer-wave16a-stage1-differentiation-matrix.json"), differentiation);
  writeMd(path.join(REPORTS, "brand-explorer-wave16a-stage1-differentiation-matrix.md"), mdDifferentiation(differentiation));
  writeJson(path.join(REPORTS, "brand-explorer-wave16a-stage1-property-candidates.json"), properties);
  writeJson(path.join(REPORTS, "brand-explorer-wave16a-stage1-momentum-candidates.json"), momentum);
  writeJson(path.join(REPORTS, "brand-explorer-wave16a-stage1-field-support-matrix.json"), fieldSupport);
  writeMd(
    path.join(REPORTS, "brand-explorer-wave16a-stage1-build-sequencing.md"),
    mdBuildSequencing({ order: recommendedBuildOrder, packs, identities })
  );

  fs.mkdirSync(PACK_DIR, { recursive: true });
  for (const slug of WAVE16A_SLUGS) {
    if (!packs[slug]) continue;
    const id = WAVE16A_IDENTITIES[slug];
    writeJson(path.join(PACK_DIR, `${slug}.json`), { identity: id, pack: packs[slug] });
    writeMd(path.join(PACK_DIR, `${slug}.md`), perBrandMd(packs[slug], id));
  }

  console.log(`Ready: ${readyStatement}`);
  console.log(
    JSON.stringify(
      {
        active: universe.totalCount,
        identitiesOk,
        flexOk: flex.protectedHoldOk,
        stage1Ready: stage1ReadyBrands.length,
        order: recommendedBuildOrder,
        writes: false,
      },
      null,
      2
    )
  );

  return report;
}
