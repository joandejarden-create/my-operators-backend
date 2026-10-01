/**
 * Export Wave 16A LOW-risk brand content into a single apply-ready presentation fixture.
 * Combines Stage 2A tab-factory text, Stage 2B image materialization, and section-pattern momentum.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateWave16aStage2aPresentationPack } from "./brand-explorer-wave16a-stage2a-controlled-tab-build.js";
import { buildWave16aStage2bImageAssetPackForBrand } from "./brand-explorer-wave16a-stage2b-image-materialization.js";
import { WAVE16A_IDENTITIES } from "./brand-explorer-wave16a-factory-plan.js";
import {
  buildSectionPatternParityPresentationRows,
  getSectionPatternParityContent,
} from "./brand-explorer-section-pattern-parity-content.js";
import { buildFairfieldOpeningsFixtureRows } from "./brand-explorer-fairfield-openings-fixture-rows.js";
import { remediateFairfieldFixtureRows } from "./brand-explorer-fairfield-content-remediation.js";
import {
  resolveOfficialBrandWebsiteFromSourcePack,
  FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
} from "./brand-explorer-brand-website-preference.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const EXPORT_WAVE16A_FULL_FIXTURE_VERSION = "export-wave16a-full-fixture-v1";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeSlotKey(slotKey) {
  const sk = nz(slotKey);
  if (/^insight\.similar\.\d+$/i.test(sk)) return "insight.similar";
  return sk;
}

const MULTI_ROW_SLOT_KEYS = new Set([
  "footprint.openings",
  "footprint.momentum",
  "standards.requirement",
  "insight.similar",
  "commercial.theme",
  "economics.risk",
  "economics.fee",
]);

function toFixtureRow(row) {
  const out = {
    slotKey: normalizeSlotKey(row.slotKey),
    title: nz(row.title),
    body: nz(row.body),
    sort: typeof row.sortOrder === "number" ? row.sortOrder : Number(row.sort) || 0,
  };
  if (row.imageUrl) out.imageUrl = nz(row.imageUrl);
  for (const k of [
    "caseSummaryOverview",
    "caseSummaryBrandRelevance",
    "caseSummaryOwnerObjective",
    "caseSummaryInterpretation",
    "caseSummaryTags",
    "summaryUrl",
  ]) {
    if (row[k]) out[k] = nz(row[k]);
  }
  return out;
}

function mergeImagePatch(rowMap, multiRows, patch) {
  const slotKey = normalizeSlotKey(patch.slotKey);
  const fields = patch.fields || {};
  const imageUrl =
    patch.imageUrl ||
    (Array.isArray(fields.Image) && fields.Image[0]?.url ? fields.Image[0].url : "");
  const title = nz(fields.Title);
  const body = nz(fields.Body);
  const extras = {
    caseSummaryOverview: fields["Case Summary Overview"],
    caseSummaryBrandRelevance: fields["Case Summary Brand Relevance"],
    caseSummaryOwnerObjective: fields["Case Summary Owner Objective"],
    caseSummaryInterpretation: fields["Case Summary Interpretation"],
    caseSummaryTags: fields["Case Summary Tags"],
  };

  if (MULTI_ROW_SLOT_KEYS.has(slotKey)) {
    multiRows.push(
      toFixtureRow({
        slotKey,
        title,
        body,
        sort: multiRows.filter((r) => r.slotKey === slotKey).length + 1,
        imageUrl,
        ...extras,
      })
    );
    return;
  }

  const existing = rowMap.get(slotKey);
  if (existing) {
    if (title) existing.title = title;
    if (body) existing.body = body;
    if (imageUrl) existing.imageUrl = imageUrl;
    for (const [k, v] of Object.entries(extras)) {
      if (v) existing[k] = nz(v);
    }
    return;
  }

  rowMap.set(
    slotKey,
    toFixtureRow({
      slotKey,
      title,
      body,
      sortOrder: 900,
      imageUrl,
      ...extras,
    })
  );
}

function buildImagePatchesFromAssetPack(slug, assetPack) {
  const visual = assetPack?.visualAssetPack || {};
  const assets = [
    ...(visual.galleryCandidates || []).map((a) => ({ ...a, kind: "gallery" })),
    ...(visual.scenarioCandidates || []).map((a) => ({ ...a, kind: "scenario" })),
    ...(visual.propertyExampleCandidates || []).map((a) => ({ ...a, kind: "property" })),
  ];

  const patches = [];
  for (const asset of assets) {
    const slotKey = asset.slotKey || asset.planSlotKey;
    if (!slotKey) continue;
    patches.push({
      slotKey,
      imageUrl: asset.imageUrl,
      fields: {
        Title: asset.title || asset.caption || "",
        Body: asset.body || asset.roleLabel || asset.caption || "",
        Image: asset.imageUrl ? [{ url: asset.imageUrl }] : undefined,
        "Case Summary Overview": asset.caseSummaryOverview,
        "Case Summary Brand Relevance": asset.caseSummaryBrandRelevance,
        "Case Summary Owner Objective": asset.caseSummaryOwnerObjective,
        "Case Summary Interpretation": asset.caseSummaryInterpretation,
        "Case Summary Tags": asset.caseSummaryTags,
      },
    });
  }
  return patches;
}

function loadCachedStage2bPatches(slug) {
  const cached = path.join(
    ROOT,
    "reports",
    `brand-explorer-wave16a-stage2b-image-materialization-${slug}.json`
  );
  if (!fs.existsSync(cached)) return null;
  try {
    const json = JSON.parse(fs.readFileSync(cached, "utf8"));
    return json.presentationPatches || null;
  } catch {
    return null;
  }
}

/**
 * @param {string} slug Wave 16A slug (e.g. fairfield-by-marriott)
 * @param {{ includeMomentum?: boolean, preferCachedStage2b?: boolean }} opts
 */
export function exportWave16aFullFixture(slug, opts = {}) {
  const identity = WAVE16A_IDENTITIES[slug];
  if (!identity?.recordId) {
    throw new Error(`Unknown Wave 16A slug: ${slug}`);
  }

  const stage2a = generateWave16aStage2aPresentationPack(slug, {
    airtableName: identity.exactBrandBasicsName || identity.suppliedName,
    recordId: identity.recordId,
  });

  const rowMap = new Map();
  const multiRows = [];

  for (const pRow of stage2a.presentation || []) {
    const slotKey = normalizeSlotKey(pRow.slotKey);
    if (/^footprint\.momentum/i.test(slotKey)) continue;
    if (slotKey === "footprint.openings") continue;
    const fixtureRow = toFixtureRow(pRow);
    if (MULTI_ROW_SLOT_KEYS.has(slotKey)) {
      multiRows.push(fixtureRow);
    } else {
      rowMap.set(slotKey, fixtureRow);
    }
  }

  const assetPack = buildWave16aStage2bImageAssetPackForBrand(slug);
  let imagePatches =
    opts.preferCachedStage2b !== false ? loadCachedStage2bPatches(slug) : null;
  if (!imagePatches?.length) {
    imagePatches = buildImagePatchesFromAssetPack(slug, assetPack);
  }

  for (const patch of imagePatches || []) {
    if (normalizeSlotKey(patch.slotKey) === "footprint.openings") continue;
    mergeImagePatch(rowMap, multiRows, patch);
  }

  if (slug === "fairfield-by-marriott") {
    for (const row of buildFairfieldOpeningsFixtureRows()) {
      multiRows.push(toFixtureRow(row));
    }
  }

  if (opts.includeMomentum !== false) {
    const momentumPack = getSectionPatternParityContent(slug);
    if (momentumPack) {
      const momentumRows = buildSectionPatternParityPresentationRows({
        ...momentumPack,
        geoIntro: null,
        regions: [],
        growthThemes: null,
        growthEditorial: null,
        portfolioContext: null,
      });
      for (const r of momentumRows) {
        const slotKey = normalizeSlotKey(r.slotKey);
        if (slotKey === "footprint.openings") continue;
        if (/^footprint\.momentum/i.test(slotKey)) {
          multiRows.push(toFixtureRow(r));
        } else if (!rowMap.has(slotKey)) {
          rowMap.set(slotKey, toFixtureRow(r));
        }
      }
    }
  }

  let rows = [...rowMap.values(), ...multiRows].sort((a, b) => {
    const sk = a.slotKey.localeCompare(b.slotKey);
    if (sk !== 0) return sk;
    return (a.sort || 0) - (b.sort || 0);
  });

  if (slug === "fairfield-by-marriott") {
    ({ rows } = remediateFairfieldFixtureRows(rows));
  }

  const brandWebsite =
    slug === "fairfield-by-marriott"
      ? FAIRFIELD_OFFICIAL_BRAND_WEBSITE
      : resolveOfficialBrandWebsiteFromSourcePack(slug) || undefined;

  return {
    version: EXPORT_WAVE16A_FULL_FIXTURE_VERSION,
    slug,
    brandName: identity.exactBrandBasicsName || identity.suppliedName,
    recordId: identity.recordId,
    brandWebsite: brandWebsite || null,
    rowCount: rows.length,
    stage2bAssetPackPass: assetPack?.pass === true,
    stage2bBlockers: assetPack?.blockers || [],
    rows,
    fixture: {
      targetBrandBasicsName: identity.exactBrandBasicsName || identity.suppliedName,
      brandNameFallback: identity.exactBrandBasicsName || identity.suppliedName,
      brandRecordId: identity.recordId,
      ...(brandWebsite ? { brandWebsite } : {}),
      instructions: `Wave 16A full presentation for ${identity.exactBrandBasicsName}. Apply: node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-record-id ${identity.recordId} --fixture fixtures/brand-explorer-presentation-${slug}-full.json --only-missing`,
      rows,
    },
  };
}

export function writeWave16aFullFixture(slug, opts = {}) {
  const exported = exportWave16aFullFixture(slug, opts);
  const outPath =
    opts.outPath ||
    path.join(ROOT, "fixtures", `brand-explorer-presentation-${slug}-full.json`);
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(exported.fixture, null, 2), "utf8");
  return { ...exported, outPath };
}
