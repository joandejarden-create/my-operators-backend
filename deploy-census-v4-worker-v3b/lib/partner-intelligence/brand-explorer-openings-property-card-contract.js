/**
 * Brand Explorer — Openings / Examples / Properties permanent card contract.
 *
 * Gold reference: Ascend Hotel Collection property-example cards.
 *
 * Title: `{Property Name} {Brand} — {City}`
 *   NOT `{Name} — Property Example` / `— CALA Property Example`
 *
 * Body (prefer blank-line paragraphs; single newlines also accepted by UI):
 *   1) comma-separated chips
 *   2) location line (city, country / district)
 *   3) meta / asset line (country or conversion · keys · amenities)
 *   4) optional scenario accent (rendered uppercase / lime in UI)
 *   5) property-specific opening teaser
 *   6) optional trailing https URL
 *
 * Case Summary columns power the View Property modal; Case Summary Tags
 * backstop blue chips when Body chips are thin.
 */

export const OPENINGS_PROPERTY_CARD_CONTRACT_VERSION = "openings-property-card-contract-v1";
export const OPENINGS_SLOT = "footprint.openings";

export const OPENINGS_FORBIDDEN_TITLE_PATTERNS = Object.freeze([
  /—\s*Property Example\s*$/i,
  /—\s*CALA Property Example\s*$/i,
  /—\s*U\.S\. Property Example\s*$/i,
  /—\s*International Reference Example\s*$/i,
]);

export const OPENINGS_FORBIDDEN_TEASER_PATTERNS = Object.freeze([
  /\bproperty example for owners comparing affiliation fit/i,
  /\baffiliation fit,\s*design narrative,\s*guest-experience intensity/i,
  /\bdirectional property example for collection fit\b/i,
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

/**
 * Ascend-style title: Property (+ brand if not already in name) — City.
 */
export function buildOpeningsPropertyCardTitle({
  propertyName,
  brandName = "",
  marketCity = "",
} = {}) {
  const name = nz(propertyName);
  const brand = nz(brandName);
  const city = nz(marketCity);
  if (!name) throw new Error("Openings card title requires propertyName");
  let head = name;
  // Match core brand even when Airtable name is "… by Choice".
  const brandCore = brand
    .replace(/\s+by\s+Choice\s*$/i, "")
    .replace(/\s*\(Choice\)\s*$/i, "")
    .trim();
  const brandAlreadyInName =
    brand && new RegExp(brand.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(name);
  const coreAlreadyInName =
    brandCore && new RegExp(brandCore.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(name);
  // "Comfort Inn" already implies "Comfort Inn & Suites" — do not double-append brand.
  const brandHead = (brandCore || brand).split(/\s+/).slice(0, 2).join(" ");
  const headAlreadyInName =
    brandHead &&
    brandHead.length >= 4 &&
    new RegExp(brandHead.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(name);
  if (brand && !brandAlreadyInName && !coreAlreadyInName && !headAlreadyInName) {
    const brandForTitle = brandCore || brand;
    head = `${name}, ${brandForTitle}`.replace(/,\s*,/g, ",");
    // Prefer Ascend spacing without forcing comma: "Amberes 64 Ascend Hotel Collection"
    if (!/,/.test(name)) head = `${name} ${brandForTitle}`;
  }
  if (city) return `${head} — ${city}`;
  return head;
}

/**
 * Canonical Body for one openings card (blank-line separated for reliable parse).
 */
export function buildOpeningsPropertyCardBody({
  chips,
  locationLine,
  metaLine,
  scenarioLine = "",
  teaser,
  sourceUrl = "",
} = {}) {
  const chipStr = Array.isArray(chips) ? chips.filter(Boolean).join(", ") : nz(chips);
  const loc = nz(locationLine);
  const meta = nz(metaLine);
  const scenario = nz(scenarioLine);
  const tease = nz(teaser);
  const url = nz(sourceUrl);
  if (!chipStr || !loc || !meta || !tease) {
    throw new Error(
      "Openings card Body requires chips, locationLine, metaLine, and property-specific teaser"
    );
  }
  if (OPENINGS_FORBIDDEN_TEASER_PATTERNS.some((re) => re.test(tease))) {
    throw new Error("Openings teaser looks like generic affiliation-fit boilerplate");
  }
  const blocks = scenario
    ? [chipStr, loc, meta, scenario, tease]
    : [chipStr, loc, meta, tease];
  if (url) {
    if (!/^https?:\/\//i.test(url)) throw new Error(`Openings sourceUrl must be https: ${url}`);
    blocks.push(url);
  }
  return blocks.join("\n\n");
}

/**
 * Template helper for catalogs / Lane 2 / CALA builders.
 */
export function buildOpeningsPropertyCard({
  propertyName,
  brandName,
  marketCity,
  country = "",
  chips,
  locationLine,
  metaLine,
  scenarioLine,
  teaser,
  sourceUrl,
  caseSummaryOverview,
  caseSummaryTags,
} = {}) {
  const title = buildOpeningsPropertyCardTitle({ propertyName, brandName, marketCity });
  const chipList = Array.isArray(chips)
    ? chips
    : nz(chips)
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
  const loc =
    nz(locationLine) ||
    [nz(marketCity), nz(country)].filter(Boolean).join(", ") ||
    nz(marketCity);
  const meta = nz(metaLine) || nz(country) || nz(marketCity) || chipList[0] || "Property";
  const body = buildOpeningsPropertyCardBody({
    chips: chipList.length ? chipList : [nz(country) || "Market", nz(marketCity) || "City"].filter(Boolean),
    locationLine: loc,
    metaLine: meta,
    scenarioLine:
      nz(scenarioLine) ||
      (chipList.length > 1 ? chipList.join(" / ").toUpperCase() : ""),
    teaser,
    sourceUrl,
  });
  const tags =
    nz(caseSummaryTags) ||
    chipList.join(", ") ||
    [nz(country), nz(marketCity)].filter(Boolean).join(", ");
  return {
    slotKey: OPENINGS_SLOT,
    title,
    body,
    caseSummaryOverview: nz(caseSummaryOverview) || nz(teaser),
    caseSummaryTags: tags,
  };
}

export function openingsTitleLooksLikeLegacyPropertyExample(title) {
  return OPENINGS_FORBIDDEN_TITLE_PATTERNS.some((re) => re.test(nz(title)));
}

export function openingsTeaserLooksGeneric(text) {
  return OPENINGS_FORBIDDEN_TEASER_PATTERNS.some((re) => re.test(nz(text)));
}

/**
 * Split openings Body into structural lines — Ascend uses single OR double newlines.
 */
export function splitOpeningsBodyUnits(bodyRaw) {
  const raw = nz(bodyRaw);
  if (!raw) return [];
  const blank = raw
    .split(/\n\n+/)
    .map((s) => s.trim())
    .filter(Boolean);
  if (blank.length >= 4) return blank;
  const single = raw
    .split(/\n/)
    .map((s) => s.trim())
    .filter(Boolean);
  if (single.length >= 4) return single;
  return blank.length ? blank : single;
}

export function openingsPropertyCardContractChecklist() {
  return {
    version: OPENINGS_PROPERTY_CARD_CONTRACT_VERSION,
    slot: OPENINGS_SLOT,
    goldReference: "Ascend Hotel Collection",
    titleFormat: "{Property} {Brand} — {City}",
    bodyFormat: "chips\\n\\nlocation\\n\\nmeta\\n\\n[scenario]\\n\\nteaser\\n\\n[https://url]",
    forbiddenTitles: ["— Property Example", "— CALA Property Example"],
    forbiddenTeasers: ["affiliation fit, design narrative… boilerplate"],
    futureBrandRequirement:
      "Use buildOpeningsPropertyCard / buildOpeningsPropertyCardTitle from this contract in Tab Factory, Lane 2, and CALA builders — never untitled Property Example stubs.",
  };
}
