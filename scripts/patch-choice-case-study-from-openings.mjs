/**
 * Replace generic CHI materials.caseStudy copy with hotel-specific opening copy.
 * Updates rows in place (Title/Body + Case Summary fields only).
 *
 * Skips brands that already have curated case studies:
 * - Radisson by Choice
 * - Radisson Blu by Choice
 *
 * Usage:
 *   node scripts/patch-choice-case-study-from-openings.mjs --dry-run
 *   node scripts/patch-choice-case-study-from-openings.mjs
 */
import "../load-env.js";
import Airtable from "airtable";
import { resolveProfileForAirtableName } from "./lib/choice-chi-brand-resolve.mjs";
import { buildCalaOpeningsForProfile } from "./lib/choice-cala-openings-from-census.mjs";

const BASICS = "Brand Setup - Brand Basics";
const PRESENTATION = "Brand Setup - Brand Explorer Presentation";
const SKIP_BRANDS = new Set(["Radisson by Choice", "Radisson Blu by Choice"]);

function normalizeHotelStyleTitle(raw) {
  let s = String(raw || "").trim();
  if (!s) return s;
  s = s.replace(/\s*\((?:CALA|.*?comp).*?\)\s*$/i, "");
  s = s.replace(/\s+—\s+/g, " ");
  s = s.replace(/\s{2,}/g, " ").trim();
  return s;
}

function parseArgs(argv) {
  return { dryRun: argv.includes("--dry-run") };
}

async function listChiBrands(base) {
  const rows = await base(BASICS).select({ maxRecords: 500 }).all();
  return rows
    .filter((r) => String(r.get("Parent Company") || "").includes("Choice Hotels International"))
    .map((r) => String(r.get("Brand Name") || "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

async function selectRowsForBrand(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  const pushAll = (rows) => {
    for (const r of rows) {
      if (!seen.has(r.id)) {
        seen.add(r.id);
        merged.push(r);
      }
    }
  };
  try {
    pushAll(
      await base(PRESENTATION)
        .select({
          filterByFormula: `OR({Brand Name} = "${esc}", {Brand} = "${esc}")`,
          maxRecords: 500,
        })
        .all()
    );
  } catch {
    // Fallback if Brand Name field is absent.
    pushAll(
      await base(PRESENTATION)
        .select({
          filterByFormula: `{Brand} = "${esc}"`,
          maxRecords: 500,
        })
        .all()
    );
  }
  return merged;
}

function firstOpening(records) {
  const rows = records
    .filter((r) => String(r.get("Slot Key") || "").trim() === "footprint.openings")
    .map((r) => ({
      id: r.id,
      sort: Number(r.get("Sort Order") || 0),
      title: String(r.get("Title") || "").trim(),
      body: String(r.get("Body") || "").trim(),
      caseSummaryOverview: String(r.get("Case Summary Overview") || "").trim(),
      caseSummaryOwnerObjective: String(r.get("Case Summary Owner Objective") || "").trim(),
      caseSummaryBrandRelevance: String(r.get("Case Summary Brand Relevance") || "").trim(),
      caseSummaryInterpretation: String(r.get("Case Summary Interpretation") || "").trim(),
      caseSummaryTags: String(r.get("Case Summary Tags") || "").trim(),
    }))
    .filter((r) => r.title && r.body)
    .sort((a, b) => a.sort - b.sort);
  return rows[0] || null;
}

function firstOpeningFromCatalog(airtableBrandName) {
  const profile = resolveProfileForAirtableName(airtableBrandName);
  const openings = buildCalaOpeningsForProfile(profile.name);
  const o = openings[0];
  if (!o?.title || !o?.body) return null;
  return {
    title: o.title,
    body: o.body,
    caseSummaryOverview: o.caseSummaryOverview || "",
    caseSummaryOwnerObjective: o.caseSummaryOwnerObjective || "",
    caseSummaryBrandRelevance: o.caseSummaryBrandRelevance || "",
    caseSummaryInterpretation: o.caseSummaryInterpretation || "",
    caseSummaryTags: o.caseSummaryTags || "",
  };
}

function caseStudyRows(records) {
  return records.filter((r) => String(r.get("Slot Key") || "").trim() === "materials.caseStudy");
}

async function main() {
  const { dryRun } = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  const base = new Airtable({ apiKey: key }).base(baseId);

  const brands = (await listChiBrands(base)).filter((b) => !SKIP_BRANDS.has(b));
  console.log(`${dryRun ? "[dry-run] " : ""}Patching materials.caseStudy from footprint.openings for ${brands.length} brand(s)…`);

  let updates = 0;
  for (const brand of brands) {
    const records = await selectRowsForBrand(base, brand);
    const opening = firstOpening(records) || firstOpeningFromCatalog(brand);
    const cases = caseStudyRows(records);
    if (!opening) {
      console.log(`- ${brand}: skip (no opening source row/catalog)`);
      continue;
    }
    if (!cases.length) {
      console.log(`- ${brand}: skip (no materials.caseStudy row)`);
      continue;
    }

    for (const row of cases) {
      const beforeTitle = String(row.get("Title") || "").trim();
      const beforeBody = String(row.get("Body") || "").trim();
      const changed = beforeTitle !== opening.title || beforeBody !== opening.body;
      if (!changed) {
        console.log(`- ${brand}: already specific`);
        continue;
      }
      const fields = {
        Title: normalizeHotelStyleTitle(opening.title),
        Body: opening.body,
        "Case Summary Overview": opening.caseSummaryOverview,
        "Case Summary Owner Objective": opening.caseSummaryOwnerObjective,
        "Case Summary Brand Relevance": opening.caseSummaryBrandRelevance,
        "Case Summary Interpretation": opening.caseSummaryInterpretation,
        "Case Summary Tags": opening.caseSummaryTags,
      };
      console.log(`- ${brand}: "${beforeTitle}" -> "${opening.title}"`);
      if (!dryRun) await base(PRESENTATION).update(row.id, fields);
      updates += 1;
    }
  }

  console.log(`${dryRun ? "Would update" : "Updated"} ${updates} materials.caseStudy row(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

