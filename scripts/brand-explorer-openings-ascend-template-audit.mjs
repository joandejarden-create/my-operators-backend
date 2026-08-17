/**
 * One-shot audit: footprint.openings vs Ascend-style property-example card template.
 * Dry-run only — no writes.
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { loadActiveUniverse } from "../lib/partner-intelligence/brand-explorer-active-universe.js";
import {
  SECTION_PATTERN_TRUE_INCOMPLETE,
  resolveSectionPatternBrandIdentity,
} from "../lib/partner-intelligence/brand-explorer-section-pattern-parity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");

const GENERIC_TEASER_RE =
  /property example for owners comparing affiliation fit,\s*design narrative/i;
const PROPERTY_EXAMPLE_TITLE_RE = /—\s*Property Example\s*$/i;
const CALA_PROPERTY_EXAMPLE_TITLE_RE = /—\s*(?:CALA|U\.S\.|International Reference)\s+Property Example\s*$/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function isHttp(u) {
  return /^https?:\/\//i.test(nz(u));
}

/** Mirror atelier parseFootprintOpeningParas (card body only). */
function parseOpeningsBody(bodyRaw) {
  let paras = nz(bodyRaw)
    .split(/\n\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
  let url = "";
  if (paras.length && isHttp(paras[paras.length - 1])) {
    url = paras[paras.length - 1];
    paras = paras.slice(0, -1);
  }
  const out = {
    paraCount: paras.length,
    chips: "",
    loc: "",
    asset: "",
    scenario: "",
    teaser: "",
    url,
    shape: "unknown",
  };
  if (paras.length >= 6) {
    out.shape = "six_block_case_study";
    [out.chips, out.loc, out.asset, out.teaser] = [paras[0], paras[1], paras[2], paras[3]];
  } else if (paras.length === 5) {
    out.shape = "five_block_ascend";
    [out.chips, out.loc, out.asset, out.scenario, out.teaser] = paras;
  } else if (paras.length === 4) {
    out.shape = "four_block_legacy";
    [out.chips, out.loc, out.asset, out.teaser] = paras;
  } else if (paras.length >= 1) {
    out.shape = "thin_or_blob";
    out.teaser = paras.join(" ");
  } else {
    out.shape = "empty";
  }
  return out;
}

function scoreCard(row, brandName) {
  const title = nz(row.title);
  const parsed = parseOpeningsBody(row.body);
  const failures = [];
  if (!title) failures.push("missing_title");
  if (PROPERTY_EXAMPLE_TITLE_RE.test(title)) failures.push("title_ends_with_property_example");
  if (CALA_PROPERTY_EXAMPLE_TITLE_RE.test(title)) failures.push("title_generic_property_example_suffix");
  // Ascend gold: "Name Brand — City" (em dash + city/market), not "— Property Example"
  if (!/—/.test(title) && !/ - /.test(title)) failures.push("title_missing_emdash_city_or_market");
  if (/Property Example/i.test(title)) failures.push("title_contains_property_example");

  if (parsed.paraCount < 4) failures.push(`body_para_count_below_4:${parsed.paraCount}`);
  if (!nz(parsed.chips) || !parsed.chips.includes(",")) failures.push("missing_comma_chips");
  if (!nz(parsed.loc)) failures.push("missing_location_line");
  if (!nz(parsed.asset)) failures.push("missing_meta_asset_line");
  if (!nz(parsed.teaser) || parsed.teaser.length < 40) failures.push("missing_or_thin_teaser");
  if (GENERIC_TEASER_RE.test(parsed.teaser) || GENERIC_TEASER_RE.test(nz(row.body))) {
    failures.push("generic_affiliation_fit_boilerplate");
  }
  if (parsed.shape === "thin_or_blob" || parsed.shape === "empty") {
    failures.push(`wrong_body_shape:${parsed.shape}`);
  }
  // Ascend cards typically have scenario accent (5-block). 4-block is acceptable legacy.
  const preferred = parsed.shape === "five_block_ascend" || parsed.shape === "four_block_legacy";
  if (!preferred && parsed.shape === "six_block_case_study") {
    // still ok for modal-rich rows
  }

  const hasImage = isHttp(row.imageUrl);
  if (!hasImage) failures.push("missing_image");

  // Soft: title should include brand or city signal
  const brandHint = nz(brandName).split(/\s+/).slice(0, 2).join(" ");
  if (brandHint && !new RegExp(brandHint.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(title)) {
    // not hard fail — Ascend includes brand in title but some gold use short titles
  }

  return {
    recordId: row.recordId || null,
    title,
    sort: row.sort,
    hasImage,
    parsed,
    failures,
    pass: failures.filter((f) => !f.startsWith("missing_image")).length === 0,
    // image missing is medium — still structure fail if other issues
    structurePass:
      failures.filter((f) => f !== "missing_image").length === 0,
  };
}

async function fetchBrand(recordId) {
  const res = {
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
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (!res.payload?.brand) throw new Error(`fetch failed ${recordId}`);
  return res.payload.brand;
}

function visibleOpenings(blocks) {
  return (blocks || []).filter(
    (b) =>
      b.slotKey === "footprint.openings" &&
      b.active !== false &&
      !/do not display|internal only/i.test(nz(b.externalDisplayStatus))
  );
}

async function main() {
  const universe = await loadActiveUniverse();
  const brandEntries = [];
  const seen = new Set();
  for (const b of universe.brands || []) {
    if (!b?.slug || seen.has(b.slug)) continue;
    seen.add(b.slug);
    brandEntries.push({
      slug: b.slug,
      recordId: b.recordId,
      cohort: "active_universe",
    });
  }
  for (const slug of SECTION_PATTERN_TRUE_INCOMPLETE) {
    if (seen.has(slug)) continue;
    const id = resolveSectionPatternBrandIdentity(slug);
    brandEntries.push({
      slug,
      recordId: id.recordId,
      cohort: "true_incomplete",
    });
    seen.add(slug);
  }
  brandEntries.sort((a, b) => a.slug.localeCompare(b.slug));

  const report = {
    version: "openings-ascend-template-audit-v2",
    generatedAt: new Date().toISOString(),
    goldReference: "ascend (Ascend Hotel Collection)",
    cohortNote:
      "Canonical Active/Live universe (Brand Status Active/Live) + true-incomplete overlay",
    expectedStructure: {
      title: "Property Name [Brand] — City/Market (not — Property Example)",
      body:
        "chips CSV\\n\\nlocation\\n\\nmeta/asset\\n\\nscenario accent (preferred)\\n\\nteaser\\n\\noptional https URL",
      ui: "image overlay title + loc; meta country/line; lime scenario; teaser; blue tag chips; View Property",
      forbidden: ["— Property Example titles", "affiliation fit / design narrative boilerplate"],
    },
    brands: [],
    summary: {},
  };

  for (const entry of brandEntries) {
    const id = entry.recordId
      ? { recordId: entry.recordId, name: entry.slug }
      : resolveSectionPatternBrandIdentity(entry.slug);
    // Prefer known map identity for display name when available
    const resolved = resolveSectionPatternBrandIdentity(entry.slug);
    const recordId = entry.recordId || resolved.recordId;
    const brand = await fetchBrand(recordId);
    const rows = visibleOpenings(brand.brandExplorer?.blocks || []).sort(
      (a, b) => Number(a.sort || 0) - Number(b.sort || 0)
    );
    const cards = rows.map((r) => scoreCard(r, brand.name));
    const structurePassCount = cards.filter((c) => c.structurePass).length;
    const brandPass =
      cards.length >= 2 && structurePassCount === cards.length;
    const failureTallies = {};
    for (const c of cards) {
      for (const f of c.failures) {
        const key = f.split(":")[0];
        failureTallies[key] = (failureTallies[key] || 0) + 1;
      }
    }
    if (cards.length < 2) {
      failureTallies.insufficient_openings = 1;
    }
    report.brands.push({
      brandSlug: entry.slug,
      brandName: brand.name,
      cohort: entry.cohort,
      openingCount: cards.length,
      structurePassCount,
      brandPass,
      failureTallies,
      cards: cards.map((c) => ({
        title: c.title,
        shape: c.parsed.shape,
        paraCount: c.parsed.paraCount,
        hasImage: c.hasImage,
        structurePass: c.structurePass,
        failures: c.failures,
        chipsPreview: nz(c.parsed.chips).slice(0, 80),
        teaserPreview: nz(c.parsed.teaser).slice(0, 100),
      })),
    });
    console.log(
      `${brandPass ? "PASS" : "FAIL"} ${entry.slug.padEnd(34)} openings=${cards.length} structureOk=${structurePassCount}/${cards.length} ${Object.keys(failureTallies).join(",") || "-"}`
    );
  }

  const active = report.brands.filter((b) => b.cohort === "active_universe");
  const incomplete = report.brands.filter((b) => b.cohort === "true_incomplete");
  report.summary = {
    brandsAudited: report.brands.length,
    activeUniversePass: active.filter((b) => b.brandPass).length,
    activeUniverseFail: active.filter((b) => !b.brandPass).length,
    incompletePass: incomplete.filter((b) => b.brandPass).length,
    incompleteFail: incomplete.filter((b) => !b.brandPass).length,
    failingActiveUniverse: active.filter((b) => !b.brandPass).map((b) => b.brandSlug),
    failingIncomplete: incomplete.filter((b) => !b.brandPass).map((b) => b.brandSlug),
  };

  const outJson = path.join(ROOT, "reports", "brand-explorer-openings-ascend-template-audit.json");
  const outMd = path.join(ROOT, "reports", "brand-explorer-openings-ascend-template-audit.md");
  fs.writeFileSync(outJson, JSON.stringify(report, null, 2));
  const md = [
    `# Openings / Examples / Properties — Ascend template audit`,
    ``,
    `Generated: ${report.generatedAt}`,
    ``,
    `**Gold:** Ascend Hotel Collection card structure (title with city, chips, location, meta, scenario, property-specific teaser — not generic “Property Example” AI boilerplate).`,
    ``,
    `## Summary`,
    ``,
    `- Active/Live universe: **${report.summary.activeUniversePass}/${active.length}** pass`,
    `- True incomplete: **${report.summary.incompletePass}/${incomplete.length}** pass`,
    `- Failing active universe: ${report.summary.failingActiveUniverse.join(", ") || "(none)"}`,
    `- Failing incomplete: ${report.summary.failingIncomplete.join(", ") || "(none)"}`,
    ``,
    `| Brand | Cohort | Openings | Structure OK | Pass | Top failures |`,
    `| --- | --- | ---: | ---: | --- | --- |`,
    ...report.brands.map((b) => {
      const top = Object.entries(b.failureTallies)
        .sort((a, c) => c[1] - a[1])
        .slice(0, 4)
        .map(([k, n]) => `${k}×${n}`)
        .join(", ");
      return `| ${b.brandSlug} | ${b.cohort} | ${b.openingCount} | ${b.structurePassCount} | ${b.brandPass ? "PASS" : "FAIL"} | ${top || "—"} |`;
    }),
    ``,
  ];
  fs.writeFileSync(outMd, md.join("\n"));
  console.log(`\nWrote ${outJson}`);
  console.log(`Wrote ${outMd}`);
  console.log(JSON.stringify(report.summary, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
