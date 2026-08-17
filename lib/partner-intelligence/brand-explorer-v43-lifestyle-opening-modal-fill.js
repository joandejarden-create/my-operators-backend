/**
 * Pre-v43 lifestyle opening modal fill (Indigo / MGallery / SLH).
 *
 * v47 created footprint.openings with images but null Case Summary fields.
 * External-owner readiness requires those modal fields before v43 unlock.
 *
 * Writes Case Summary* only on the three CALA openings per brand.
 * Does not unlock, touch Company Validated, Source Library, Registry, or images.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { evaluateExternalOwnerReadinessRule } from "./brand-explorer-external-owner-readiness-rules.js";
import { buildCalaPropertyOpeningTitle } from "./brand-explorer-cala-property-example-rules.js";
import {
  GRADUATED_LIFESTYLE_COHORT_SLUGS,
  ORIGINAL_GOLDEN_RELEASE_SLUGS,
} from "./brand-explorer-os-state-machine.js";
import {
  HOTEL_INDIGO_PROPERTY_CATALOG,
  MGALLERY_PROPERTY_CATALOG,
  SLH_PROPERTY_CATALOG,
  selectPropertyExampleCatalog,
} from "./brand-explorer-lifestyle-affiliation-property-catalog.js";

export const VERSION = "v43-pre-opening-modal";
export const TARGET_BRANDS = Object.freeze([...GRADUATED_LIFESTYLE_COHORT_SLUGS]);
export const PROTECTED_RELEASED = Object.freeze([...ORIGINAL_GOLDEN_RELEASE_SLUGS]);
export const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const CATALOGS = Object.freeze({
  "hotel-indigo": HOTEL_INDIGO_PROPERTY_CATALOG,
  "mgallery-collection": MGALLERY_PROPERTY_CATALOG,
  "small-luxury-hotels-of-the-world": SLH_PROPERTY_CATALOG,
});

const BRAND_LENS = Object.freeze({
  "hotel-indigo": {
    brandLabel: "Hotel Indigo",
    relevanceCue: "neighborhood lifestyle fit within the IHG Hotel Indigo system—not InterContinental",
    objectiveCue:
      "Illustrates neighborhood storytelling, design narrative, and operating implications for Hotel Indigo conversions or new builds.",
    takeawayCue:
      "Use as a property-example reference only; confirm Hotel Indigo / IHG design standards, systems cutover, and agreement terms before capital commitments.",
  },
  "mgallery-collection": {
    brandLabel: "MGallery Collection",
    relevanceCue: "Accor MGallery soft-collection fit and local character—not a generic Accor prototype",
    objectiveCue:
      "Illustrates collection conversion / repositioning suitability, brand standards, and operating implications for owners.",
    takeawayCue:
      "Use as a collection-fit reference only; confirm Accor / MGallery review, standards, and commercial terms before capital commitments.",
  },
  "small-luxury-hotels-of-the-world": {
    brandLabel: "Small Luxury Hotels of the World",
    relevanceCue: "independent luxury consortium / affiliation fit—not a franchise chain prototype",
    objectiveCue:
      "Illustrates membership quality expectations, independent ownership character, and distribution/recognition diligence.",
    takeawayCue:
      "Use as a membership-fit reference only; confirm current SLH criteria, inspections, and property-level agreement terms before commitments.",
  },
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");
export const REPORT_JSON = "brand-explorer-v43-lifestyle-opening-modal-fill.json";
export const REPORT_MD = "brand-explorer-v43-lifestyle-opening-modal-fill.md";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function isPlaceholder(v) {
  const t = nz(v);
  return !t || t === "—" || t === "-";
}

export function buildLifestyleOpeningModalFields(brandSlug, catalog) {
  const lens = BRAND_LENS[brandSlug];
  if (!lens) throw new Error(`No modal lens for ${brandSlug}`);
  const loc = [catalog.marketCity, catalog.stateRegion].filter(Boolean).join(", ");
  const overview = `${catalog.propertyName} in ${loc} — ${lens.brandLabel} CALA property example illustrating ${lens.relevanceCue}.`;
  const relevance = catalog.ownerRelevance || catalog.teaser || overview;
  const tags = catalog.chips || "CALA";
  return {
    "Case Summary Overview": overview,
    "Case Summary Brand Relevance": relevance,
    "Case Summary Owner Objective": lens.objectiveCue,
    "Case Summary Interpretation": lens.takeawayCue,
    "Case Summary Tags": tags,
  };
}

function matchCatalog(brandSlug, openingRow) {
  const catalog = selectPropertyExampleCatalog(CATALOGS[brandSlug], 3);
  const title = nz(openingRow.title).toLowerCase();
  return (
    catalog.find((c) => title.includes(nz(c.propertyName).toLowerCase())) ||
    catalog.find((c) => title.includes(nz(c.marketCity).toLowerCase())) ||
    null
  );
}

export async function planBrandOpeningModalFill(brandSlug) {
  if (!TARGET_BRANDS.includes(brandSlug)) {
    throw new Error(`Targets only: ${TARGET_BRANDS.join(", ")}`);
  }
  if (PROTECTED_RELEASED.includes(brandSlug)) {
    throw new Error(`Refuse protected released brand ${brandSlug}`);
  }

  const ctx = await loadBrandFactoryContext(brandSlug);
  const openings = (ctx.presentationRows || []).filter(
    (r) => nz(r.slotKey) === "footprint.openings" && r.active !== false && r.visible !== false
  );
  const readinessBefore = evaluateExternalOwnerReadinessRule(ctx.presentationRows || []);
  const patches = [];

  for (const row of openings) {
    const catalog = matchCatalog(brandSlug, row);
    if (!catalog) continue;
    const fields = buildLifestyleOpeningModalFields(brandSlug, catalog);
    const needs =
      isPlaceholder(row.caseSummaryOverview) ||
      isPlaceholder(row.caseSummaryBrandRelevance) ||
      isPlaceholder(row.caseSummaryOwnerObjective) ||
      isPlaceholder(row.caseSummaryInterpretation);
    if (!needs) continue;
    patches.push({
      action: "PATCH",
      recordId: row.recordId,
      slotKey: "footprint.openings",
      title: row.title || buildCalaPropertyOpeningTitle(catalog),
      propertyName: catalog.propertyName,
      fields,
      current: {
        caseSummaryOverview: row.caseSummaryOverview || "",
        caseSummaryBrandRelevance: row.caseSummaryBrandRelevance || "",
        caseSummaryOwnerObjective: row.caseSummaryOwnerObjective || "",
        caseSummaryInterpretation: row.caseSummaryInterpretation || "",
      },
    });
  }

  // Project readiness after patch
  const projectedRows = (ctx.presentationRows || []).map((r) => {
    const p = patches.find((x) => x.recordId === r.recordId);
    if (!p) return r;
    return {
      ...r,
      caseSummaryOverview: p.fields["Case Summary Overview"],
      caseSummaryBrandRelevance: p.fields["Case Summary Brand Relevance"],
      caseSummaryOwnerObjective: p.fields["Case Summary Owner Objective"],
      caseSummaryInterpretation: p.fields["Case Summary Interpretation"],
      caseSummaryTags: p.fields["Case Summary Tags"],
    };
  });
  const readinessAfter = evaluateExternalOwnerReadinessRule(projectedRows);

  return {
    brandSlug,
    openingsCount: openings.length,
    patches,
    readinessBefore: { pass: readinessBefore.pass, blockers: readinessBefore.blockers },
    readinessAfter: { pass: readinessAfter.pass, blockers: readinessAfter.blockers },
  };
}

async function patchPresentation(recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Airtable PATCH ${recordId} failed: ${res.status} ${text.slice(0, 400)}`);
  }
  return res.json();
}

export async function applyOpeningModalFill(brandResults, { dryRun = true } = {}) {
  if (dryRun) return { applied: false, reason: "dry_run_only", resultsByBrand: {} };
  const resultsByBrand = {};
  for (const brand of brandResults) {
    const updated = [];
    const errors = [];
    for (const patch of brand.patches) {
      try {
        await patchPresentation(patch.recordId, patch.fields);
        updated.push({ recordId: patch.recordId, title: patch.title });
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        errors.push({ recordId: patch.recordId, message: err.message });
      }
    }
    resultsByBrand[brand.brandSlug] = { updated, errors };
  }
  return { applied: true, resultsByBrand };
}

export async function runLifestyleOpeningModalFill({
  brands = TARGET_BRANDS,
  dryRun = true,
} = {}) {
  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await planBrandOpeningModalFill(brandSlug));
  }
  const applyResult = await applyOpeningModalFill(brandResults, { dryRun });
  return {
    version: VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    brands,
    brandResults,
    applyResult,
    summary: {
      totalPatches: brandResults.reduce((n, b) => n + b.patches.length, 0),
      projectedExternalOwnerPass: brandResults.filter((b) => b.readinessAfter.pass).length,
      presentationWrites: dryRun ? false : applyResult.applied === true,
    },
    guardrails: {
      activeRelease: false,
      companyValidatedChanges: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      releasedBrandChanges: false,
    },
  };
}

export function writeOpeningModalReports(report) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const lines = [
    `# Lifestyle Opening Modal Fill (pre-v43)`,
    "",
    `Generated: ${report.generatedAt}`,
    `dryRun=${report.dryRun}`,
    "",
    `Total patches: **${report.summary.totalPatches}**`,
    `Projected external-owner pass: **${report.summary.projectedExternalOwnerPass}/${report.brands.length}**`,
    `Presentation writes: **${report.summary.presentationWrites}**`,
    "",
  ];
  for (const b of report.brandResults) {
    lines.push(`## ${b.brandSlug}`);
    lines.push(`- Openings: ${b.openingsCount}`);
    lines.push(`- Patches: ${b.patches.length}`);
    lines.push(`- Readiness before: ${b.readinessBefore.pass} (${b.readinessBefore.blockers.join(", ") || "none"})`);
    lines.push(`- Readiness after: ${b.readinessAfter.pass} (${b.readinessAfter.blockers.join(", ") || "none"})`);
    for (const p of b.patches) {
      lines.push(`  - ${p.recordId} · ${p.propertyName}`);
    }
    lines.push("");
  }
  fs.writeFileSync(mdPath, lines.join("\n"));
  return { jsonPath, mdPath };
}
