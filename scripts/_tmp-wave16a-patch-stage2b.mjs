import fs from "node:fs";

const p = "lib/partner-intelligence/brand-explorer-wave16a-stage2b-image-materialization.js";
let s = fs.readFileSync(p, "utf8");

const rejectFn = String.raw`export function isWave16aStage2bRejectedImageUrl(url, { brandSlug = "", propertyName = "", sourcePageUrl = "" } = {}) {
  const u = nz(url);
  const lower = u.toLowerCase();
  const ctx = `${lower} ${nz(propertyName).toLowerCase()} ${nz(sourcePageUrl).toLowerCase()}`;
  if (!lower) return { rejected: true, reason: "missing_url" };
  if (isLogoImageUrl(lower)) return { rejected: true, reason: "logo" };
  if (/gettyimages|istock|family-at-the-beach|snorkeling|maldives|stays\b|chiclet|learning-hub|portal|fpx-image\.jpg|photo-coming/i.test(lower)) {
    return { rejected: true, reason: "stock_or_generic_filler" };
  }
  if (FLEX_CONTAMINATION_RE.test(ctx)) {
    return { rejected: true, reason: "four_points_flex_contamination" };
  }
  const sibling = SIBLING_RE_BY_SLUG[brandSlug];
  const hint = BRAND_HINT_RE_BY_SLUG[brandSlug];
  if (sibling && sibling.test(ctx) && !(hint && hint.test(ctx))) {
    return { rejected: true, reason: "sibling_or_wrong_brand" };
  }
  const isBrandHostImage =
    /(?:fairfield|four-points|delta-hotels|marriott-hotels|sheraton|westin|springhillsuites|towneplacesuites|aloft-hotels)\.marriott\.com/i.test(
      lower
    ) || /hotel-development\.marriott\.com\/resourcefiles\//i.test(lower);
  if (!isOfficialLifestylePropertyImageUrl(u) && !isBrandHostImage) {
    return { rejected: true, reason: "not_official_property_cdn" };
  }
  if (isGenericBrandOrLifestyleImageUrl(u)) {
    return { rejected: true, reason: "generic_brand_lifestyle" };
  }
  return { rejected: false, reason: null };
}`;

// Fix: String.raw + template literals conflict. Build without String.raw.
const rejectFn2 = [
  'export function isWave16aStage2bRejectedImageUrl(url, { brandSlug = "", propertyName = "", sourcePageUrl = "" } = {}) {',
  "  const u = nz(url);",
  "  const lower = u.toLowerCase();",
  "  const ctx = `${lower} ${nz(propertyName).toLowerCase()} ${nz(sourcePageUrl).toLowerCase()}`;",
  '  if (!lower) return { rejected: true, reason: "missing_url" };',
  '  if (isLogoImageUrl(lower)) return { rejected: true, reason: "logo" };',
  '  if (/gettyimages|istock|family-at-the-beach|snorkeling|maldives|stays\\b|chiclet|learning-hub|portal|fpx-image\\.jpg|photo-coming/i.test(lower)) {',
  '    return { rejected: true, reason: "stock_or_generic_filler" };',
  "  }",
  "  if (FLEX_CONTAMINATION_RE.test(ctx)) {",
  '    return { rejected: true, reason: "four_points_flex_contamination" };',
  "  }",
  "  const sibling = SIBLING_RE_BY_SLUG[brandSlug];",
  "  const hint = BRAND_HINT_RE_BY_SLUG[brandSlug];",
  "  if (sibling && sibling.test(ctx) && !(hint && hint.test(ctx))) {",
  '    return { rejected: true, reason: "sibling_or_wrong_brand" };',
  "  }",
  "  const isBrandHostImage =",
  "    /(?:fairfield|four-points|delta-hotels|marriott-hotels|sheraton|westin|springhillsuites|towneplacesuites|aloft-hotels)\\.marriott\\.com/i.test(",
  "      lower",
  "    ) || /hotel-development\\.marriott\\.com\\/resourcefiles\\//i.test(lower);",
  "  if (!isOfficialLifestylePropertyImageUrl(u) && !isBrandHostImage) {",
  '    return { rejected: true, reason: "not_official_property_cdn" };',
  "  }",
  "  if (isGenericBrandOrLifestyleImageUrl(u)) {",
  '    return { rejected: true, reason: "generic_brand_lifestyle" };',
  "  }",
  "  return { rejected: false, reason: null };",
  "}",
].join("\n");

const start = s.indexOf("export function isWave16aStage2bRejectedImageUrl");
const end = s.indexOf("function normalizeWave16aStage2bPool");
if (start < 0 || end < 0) throw new Error(`reject fn bounds missing ${start} ${end}`);
s = s.slice(0, start) + rejectFn2 + "\n\n" + s.slice(end);

s = s.replace(
  "const gate = isWave16aStage2bRejectedImageUrl(imageUrl, { brandSlug });",
  `const gate = isWave16aStage2bRejectedImageUrl(imageUrl, {
      brandSlug,
      propertyName: nz(row.propertyName),
      sourcePageUrl: nz(row.sourcePageUrl),
    });`
);

const catalogFn = [
  "function propertyCatalogForSlug(slug) {",
  "  const pool = loadWave16aStage2bGalleryPool(slug);",
  "  const byKey = new Map();",
  "  for (const row of pool) {",
  '    if (nz(row.label) === "brand_site") continue;',
  "    if (!nz(row.propertyName) || !nz(row.sourcePageUrl)) continue;",
  "    if (!/\\/hotels\\/[a-z0-9-]+\\/overview/i.test(row.sourcePageUrl)) continue;",
  "    const propertyKey =",
  "      nz(row.propertyKey) ||",
  "      String(row.propertyName)",
  "        .toLowerCase()",
  '        .replace(/[^a-z0-9]+/g, "-")',
  '        .replace(/^-|-$/g, "");',
  "    if (!propertyKey || byKey.has(propertyKey)) continue;",
  "    if (FLEX_CONTAMINATION_RE.test(`${row.propertyName} ${row.sourcePageUrl} ${row.imageUrl || \"\"}`)) continue;",
  "    byKey.set(propertyKey, {",
  "      propertyKey,",
  "      propertyName: row.propertyName,",
  '      marketCity: row.marketCity || "",',
  '      geographyLabel: row.geographyLabel || "International Reference",',
  "      sourcePageUrl: row.sourcePageUrl,",
  '      teaser: row.caption || "",',
  "    });",
  "  }",
  "  return [...byKey.values()];",
  "}",
].join("\n");

const cStart = s.indexOf("function propertyCatalogForSlug");
const cEnd = s.indexOf("function assignPropertyExampleAssets");
if (cStart < 0 || cEnd < 0) throw new Error(`catalog bounds missing ${cStart} ${cEnd}`);
s = s.slice(0, cStart) + catalogFn + "\n\n" + s.slice(cEnd);

s = s.split("not_wave14_target").join("not_wave16a_stage2b_target");
s = s.split("wave14Version").join("wave16aVersion");
s = s.split("brand-explorer-wave14-factory").join("brand-explorer-wave16a-stage2b-images");
s = s.split("fixtures/wave14-{slug}-gallery-pool.json").join("fixtures/wave16a-{slug}-gallery-pool.json");
s = s.split("scripts/harvest-wave14-image-pools.mjs").join("scripts/_tmp-wave16a-build-gallery-pools.mjs");
s = s.split("reports/brand-explorer-wave14-active-baseline-watch-note.md").join(
  "reports/brand-explorer-wave16a-stage2b-image-materialization.md"
);

fs.writeFileSync(p, s);
console.log("patched ok", s.length);
