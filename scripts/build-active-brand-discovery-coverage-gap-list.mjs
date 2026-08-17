/**
 * Read-only Active/Live Brand Setup coverage gap list for Webhound learning.
 * No Airtable writes.
 */
import fs from "node:fs";
import { buildActiveBrandSetupControlList } from "../lib/research-engine-v2/census-autopilot-active-brand-scope.js";
import { resolveExtractorFamily } from "../lib/research-engine-v2/census-family-extractor-registry.js";

const PARENT_BY_SLUG = {
  "aloft-hotels": "Marriott",
  "autograph-collection": "Marriott",
  "ac-hotels-by-marriott": "Marriott",
  "city-express-by-marriott": "Marriott",
  "courtyard-by-marriott": "Marriott",
  "marriott-hotels": "Marriott",
  "moxy-hotels": "Marriott",
  "residence-inn-by-marriott": "Marriott",
  sheraton: "Marriott",
  "springhill-suites-by-marriott": "Marriott",
  studiores: "Marriott",
  "towneplace-suites-by-marriott": "Marriott",
  "tribute-portfolio": "Marriott",
  westin: "Marriott",
  "canopy-by-hilton": "Hilton",
  "curio-collection": "Hilton",
  "doubletree-by-hilton": "Hilton",
  "hampton-by-hilton": "Hilton",
  "hilton-garden-inn": "Hilton",
  "hilton-hotels-and-resorts": "Hilton",
  "home2-suites-by-hilton": "Hilton",
  "homewood-suites-by-hilton": "Hilton",
  "motto-by-hilton": "Hilton",
  "spark-by-hilton": "Hilton",
  "tempo-by-hilton": "Hilton",
  "tru-by-hilton": "Hilton",
  "tapestry-collection-by-hilton": "Hilton",
  ascend: "Choice",
  "comfort-inn-suites": "Choice",
  "country-inn-suites": "Choice",
  "quality-inn": "Choice",
  radisson: "Choice",
  "radisson-blu": "Choice",
  "radisson-red": "Choice",
  "radisson-individuals-by-choice": "Choice",
  "suburban-studios": "Choice",
  "avid-hotels": "IHG",
  "even-hotels": "IHG",
  "holiday-inn-express": "IHG",
  "hotel-indigo": "IHG",
  kimpton: "IHG",
  "voco-hotels": "IHG",
  "handwritten-collection": "IHG",
  "vignette-collection": "IHG",
  "design-hotels": "Accor",
  "fairmont-hotels-and-resorts": "Accor",
  ibis: "Accor",
  mercure: "Accor",
  novotel: "Accor",
  pullman: "Accor",
  "mgallery-collection": "Accor",
  "mama-shelter": "Accor",
  "so-hotels-and-resorts": "Accor",
  "dazzler-by-wyndham": "Wyndham",
  "trademark-collection-by-wyndham": "Wyndham",
  "woodspring-suites": "Wyndham",
  "everhome-suites": "Wyndham",
  "bw-premier-collection": "BWH Hotels",
  "bw-signature-collection": "BWH Hotels",
  "preferred-hotels-and-resorts": "Preferred Hotels & Resorts",
  "small-luxury-hotels-of-the-world": "SLH",
  "bunkhouse-hotels": "Bunkhouse",
};

const COVERED = new Set(["Marriott", "Hilton", "Choice", "IHG"]);
const MODULES = {
  Marriott: "lib/research-engine-v2/census-autopilot-marriott-discovery-adapter.js",
  Hilton: "lib/research-engine-v2/census-autopilot-hilton-cala-discovery-adapter.js",
  Choice: "lib/research-engine-v2/census-autopilot-choice-cala-discovery-adapter.js",
  IHG: "lib/research-engine-v2/census-autopilot-ihg-cala-discovery-adapter.js",
  Accor: "lib/accor-brand-directory-extract.js; lib/accor-continent-directory-extract.js",
  Wyndham: "lib/wyndham-brand-directory-extract.js",
  "BWH Hotels": "lib/bwh-brand-directory-extract.js",
  Hyatt: "lib/hyatt-brand-directory-extract.js",
};

const list = buildActiveBrandSetupControlList({ region: "CALA", includeHeldProbe: true });
const brands = list.brands.map((b) => {
  const inferred = PARENT_BY_SLUG[b.brand_slug] || b.parent_company || null;
  const fam = resolveExtractorFamily(inferred).family;
  const covered = COVERED.has(inferred) || COVERED.has(fam);
  let discovery_adapter_status;
  let webhound_needed;
  let notes = "";
  if (covered) {
    discovery_adapter_status = "supported";
    webhound_needed = false;
    notes = "Excluded from Webhound — multi-parent CALA Autopilot adapters wired";
  } else if (inferred === "Accor" || inferred === "Wyndham" || inferred === "BWH Hotels") {
    discovery_adapter_status = "partial";
    webhound_needed = true;
    notes = "Directory extractors exist but not wired into Autopilot source_discovery";
  } else if (inferred === "Preferred Hotels & Resorts" || inferred === "SLH") {
    discovery_adapter_status = "missing";
    webhound_needed = true;
    notes = "Soft-brand / collection — may need distinct discovery handling";
  } else if (inferred === "Bunkhouse") {
    discovery_adapter_status = "missing";
    webhound_needed = true;
    notes = "Small portfolio; lower CALA inventory priority";
  } else {
    discovery_adapter_status = "missing";
    webhound_needed = true;
  }
  return {
    brand_name: b.brand_name,
    brand_slug: b.brand_slug,
    parent_company: inferred,
    parent_company_brand_setup_raw: b.parent_company,
    brand_family: fam !== "generic" ? fam : inferred || "unknown",
    discovery_adapter_status,
    region_relevance: "CALA",
    source_family: inferred || fam || "unknown",
    existing_code_module: MODULES[inferred] || MODULES[fam] || null,
    webhound_needed,
    notes,
  };
});

const byParent = {};
for (const r of brands) {
  const p = r.parent_company || "UNKNOWN";
  if (!byParent[p]) {
    byParent[p] = {
      brands: [],
      status: r.discovery_adapter_status,
      webhound_needed: r.webhound_needed,
      module: r.existing_code_module,
    };
  }
  byParent[p].brands.push(r.brand_slug);
}

const gapParents = Object.entries(byParent)
  .filter(([, v]) => v.webhound_needed)
  .map(([parent, v]) => ({
    parent_company: parent,
    brand_count: v.brands.length,
    brands: v.brands,
    discovery_adapter_status: v.status,
    existing_code_module: v.module,
    webhound_needed: true,
    representative_brands: v.brands.slice(0, 3),
    priority_countries: ["Mexico", "Dominican Republic", "Costa Rica", "Colombia", "Panama"],
  }));

const out = {
  run_type: "active_brand_discovery_coverage_gap_list",
  generated_at: new Date().toISOString(),
  production_writes: false,
  airtable_writes: false,
  brand_setup_writes: false,
  brand_explorer_writes: false,
  active_brands_total: brands.length,
  parent_companies: Object.keys(byParent).sort(),
  excluded_from_webhound: ["Marriott", "IHG", "Hilton", "Choice"],
  exclusion_reason: "Multi-parent CALA discovery adapters already wired (supported)",
  brand_setup_parent_field_gap:
    "Many Active/Live Brand Setup rows lack Parent Company; Autopilot Active Setup discovery may skip IHG/Accor/etc. unless parent is inferred from slug.",
  by_parent: byParent,
  brands,
  webhound_gap_parents: gapParents,
  webhound_case_budget: {
    max_cases: 40,
    planned_cases_estimate: gapParents.reduce((n, g) => n + Math.min(3, g.brand_count) * 3, 0),
    note: "Prefer parent/source-family patterns × priority countries; not property population",
  },
};

fs.mkdirSync("reports/research-engine-v2", { recursive: true });
fs.writeFileSync(
  "reports/research-engine-v2/active-brand-discovery-coverage-gap-list.json",
  JSON.stringify(out, null, 2)
);

const md = [];
md.push("# Active Brand Discovery Coverage Gap List");
md.push("");
md.push(`Generated: ${out.generated_at}`);
md.push("");
md.push("**Production writes:** false · Brand Setup/Explorer untouched · Hotel Property Census not written");
md.push("");
md.push("## Summary");
md.push("");
md.push(`- Active/Live brands in scope: **${out.active_brands_total}**`);
md.push(`- Excluded from Webhound (already supported): ${out.excluded_from_webhound.join(", ")}`);
md.push(
  `- Gap parents for Webhound: **${gapParents.length}** (${gapParents.map((g) => g.parent_company).join(", ")})`
);
md.push("");
md.push("## Parent matrix");
md.push("");
md.push("| Parent | Brands | Adapter status | Module | Webhound? |");
md.push("| --- | ---: | --- | --- | --- |");
for (const [p, v] of Object.entries(byParent).sort((a, b) => a[0].localeCompare(b[0]))) {
  md.push(
    `| ${p} | ${v.brands.length} | ${v.status} | ${v.module || "—"} | ${v.webhound_needed ? "yes" : "no"} |`
  );
}
md.push("");
md.push("## Webhound gap parents");
md.push("");
for (const g of gapParents) {
  md.push(`### ${g.parent_company}`);
  md.push(`- Brands (${g.brand_count}): ${g.brands.join(", ")}`);
  md.push(`- Representative sample: ${g.representative_brands.join(", ")}`);
  md.push(`- Status: ${g.discovery_adapter_status}`);
  md.push(`- Module: ${g.existing_code_module || "none"}`);
  md.push("");
}
md.push("## Brand rows (compact)");
md.push("");
md.push("| Brand | Slug | Parent (inferred) | Status | WH |");
md.push("| --- | --- | --- | --- | --- |");
for (const b of brands.sort((a, c) => a.brand_slug.localeCompare(c.brand_slug))) {
  md.push(
    `| ${b.brand_name} | ${b.brand_slug} | ${b.parent_company} | ${b.discovery_adapter_status} | ${b.webhound_needed ? "Y" : "N"} |`
  );
}
fs.writeFileSync("reports/research-engine-v2/active-brand-discovery-coverage-gap-list.md", md.join("\n"));
console.log(
  JSON.stringify(
    {
      ok: true,
      brands: brands.length,
      gap_parents: gapParents.map((g) => g.parent_company),
    },
    null,
    2
  )
);
