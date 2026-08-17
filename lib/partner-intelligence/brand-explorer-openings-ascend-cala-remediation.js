/**
 * Active-brand Openings remediation — Ascend card template + CALA-first selection.
 *
 * Prefer CALA property cards when available; fall back to U.S./global only when
 * CALA inventory is insufficient (same geographic fallback as CALA property rules).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildOpeningsPropertyCard,
  buildOpeningsPropertyCardTitle,
  splitOpeningsBodyUnits,
  openingsTitleLooksLikeLegacyPropertyExample,
  openingsTeaserLooksGeneric,
} from "./brand-explorer-openings-property-card-contract.js";
import {
  selectPropertyExamplesWithGeographicFallback,
  isCalaPropertyCatalogEntry,
} from "./brand-explorer-cala-property-example-rules.js";
import {
  DESIGN_HOTELS_PROPERTY_CATALOG,
  HOTEL_INDIGO_PROPERTY_CATALOG,
  MGALLERY_PROPERTY_CATALOG,
  SLH_PROPERTY_CATALOG,
} from "./brand-explorer-lifestyle-affiliation-property-catalog.js";
import { TRIBUTE_PROPERTY_CATALOG } from "./brand-explorer-lifestyle-affiliation-brand-config.js";
import { SUBURBAN_PROPERTY_CATALOG } from "./brand-explorer-active-profile-brand-config.js";
import { WOODSPRING_PROPERTY_CATALOG } from "./brand-explorer-woodspring-real-property-examples-writer.js";
import {
  LANE2_PROPERTY_CATALOG_BY_SLUG,
} from "./brand-explorer-lane2-property-catalog.js";
import { BUILT_BLOCKED_TRUE_INCOMPLETE } from "./brand-explorer-built-blocked-content.js";
import { SECTION_PATTERN_AUDIT_DEFAULT_BRANDS } from "./brand-explorer-section-pattern-parity.js";
import { resolveSectionPatternBrandIdentity } from "./brand-explorer-section-pattern-parity.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const OPENINGS_ASCEND_CALA_REMEDIATION_VERSION = "openings-ascend-cala-remediation-v2";
export const OPENINGS_ACTIVE_BRANDS = Object.freeze([...SECTION_PATTERN_AUDIT_DEFAULT_BRANDS]);
export const OPENINGS_INCOMPLETE_BRANDS = Object.freeze([...BUILT_BLOCKED_TRUE_INCOMPLETE]);
/** Active + true-incomplete — full Explorer openings remediation cohort. */
export const OPENINGS_ALL_BRANDS = Object.freeze([
  ...OPENINGS_ACTIVE_BRANDS,
  ...OPENINGS_INCOMPLETE_BRANDS,
]);

export const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
export const OPENINGS_SLOT = "footprint.openings";

const MIN_CARDS = 3;

const CHOICE_PROFILE_BY_SLUG = Object.freeze({
  ascend: "Ascend Hotel Collection",
  "comfort-inn-suites": "Comfort Inn & Suites",
  "country-inn-suites": "Country Inn & Suites by Radisson (Choice)",
  "quality-inn": "Quality Inn",
  radisson: "Radisson (Choice)",
  "radisson-blu": "Radisson Blu (Choice)",
  // Double-space profile key matches CURATED_BY_PROFILE / BRAND_URL_SLUGS.
  "radisson-red": "Radisson RED  (Choice)",
  "radisson-individuals-by-choice": "Radisson Individual (Choice)",
  "suburban-studios": "Suburban Studios",
  "woodspring-suites": "WoodSpring Suites",
  "everhome-suites": "Everhome Suites",
});

const FIXTURE_BY_SLUG = Object.freeze({
  kimpton: "fixtures/brand-explorer-presentation-kimpton-footprint-openings.json",
  "curio-collection": "fixtures/brand-explorer-presentation-curio-full.json",
  radisson: "fixtures/brand-explorer-presentation-radisson-footprint-openings.json",
  "radisson-blu": "fixtures/brand-explorer-presentation-radisson-blu-footprint-openings.json",
  "radisson-red": "fixtures/brand-explorer-presentation-radisson-red-choice-full.json",
  "radisson-individuals-by-choice":
    "fixtures/brand-explorer-presentation-radisson-individuals-footprint-openings.json",
  "everhome-suites": "fixtures/brand-explorer-presentation-everhome-footprint-openings.json",
  "country-inn-suites": "fixtures/brand-explorer-presentation-country-inn-footprint-openings.json",
});

const CATALOG_BY_SLUG = Object.freeze({
  "design-hotels": DESIGN_HOTELS_PROPERTY_CATALOG,
  "hotel-indigo": HOTEL_INDIGO_PROPERTY_CATALOG,
  "mgallery-collection": MGALLERY_PROPERTY_CATALOG,
  "small-luxury-hotels-of-the-world": SLH_PROPERTY_CATALOG,
  "tribute-portfolio": TRIBUTE_PROPERTY_CATALOG,
  "suburban-studios": SUBURBAN_PROPERTY_CATALOG,
  "woodspring-suites": WOODSPRING_PROPERTY_CATALOG,
  ...LANE2_PROPERTY_CATALOG_BY_SLUG,
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function readFixtureRows(relPath) {
  const full = path.join(ROOT, relPath);
  if (!fs.existsSync(full)) return [];
  const data = JSON.parse(fs.readFileSync(full, "utf8"));
  const rows = data.rows || data.presentation || [];
  return rows.filter((r) => r.slotKey === OPENINGS_SLOT);
}

function cardLooksCala(card) {
  // Strip U.S. "Panama City Beach" / "Panama City, Florida" so they do not
  // false-positive as Panama (CALA).
  const hay = [
    card.title,
    card.body,
    card.caseSummaryTags,
    card.chips,
    card.geographyLabel,
    card.meta,
  ]
    .filter(Boolean)
    .join(" ")
    .replace(/panama\s+city\s+beach/gi, " ")
    .replace(/panama\s+city\s*,?\s*florida/gi, " ")
    .replace(/panama\s+city\s+beach,\s*florida/gi, " ");
  if (/\bcala\b/i.test(hay)) return true;
  return /\b(mexico|colombia|panama|peru|chile|argentina|brazil|barbados|dominican|jamaica|costa rica|ecuador|guatemala|honduras|nicaragua|salvador|uruguay|paraguay|bolivia|suriname|trinidad|aruba|bahamas|belize|cuba|puerto rico|caribbean)\b/i.test(
    hay
  );
}

function normalizeFixtureCard(row, brandName) {
  const units = splitOpeningsBodyUnits(row.body);
  let chips = units[0] || nz(row.caseSummaryTags).replace(/,/g, ", ");
  let loc = units[1] || "";
  let meta = units[2] || "";
  let scenario = units.length >= 5 ? units[3] : "";
  let teaser = units.length >= 5 ? units[4] : units[3] || nz(row.caseSummaryOverview);
  const urlMatch = nz(row.body).match(/https?:\/\/\S+/i);
  const sourceUrl = urlMatch ? urlMatch[0] : "";

  // Infer city from title or location
  const title = nz(row.title);
  let marketCity = "";
  const em = title.match(/—\s*(.+)$/);
  if (em) marketCity = em[1].trim();
  else if (loc) marketCity = loc.split(",")[0].trim();

  const propertyName = title
    .replace(/\s*—\s*(?:CALA |U\.S\. )?Property Example\s*$/i, "")
    .replace(/\s*—\s*.+$/, "")
    .trim() || title;

  if (!chips || !loc || !meta || !teaser) {
    // Keep original body if already multi-line enough; still fix title
    const ascendTitle = buildOpeningsPropertyCardTitle({
      propertyName: title.includes("—") ? propertyName : title,
      brandName,
      marketCity,
    });
    return {
      title: openingsTitleLooksLikeLegacyPropertyExample(title)
        ? ascendTitle
        : /—/.test(title)
          ? title
          : ascendTitle,
      body: nz(row.body),
      caseSummaryOverview: nz(row.caseSummaryOverview),
      caseSummaryOwnerObjective: nz(row.caseSummaryOwnerObjective),
      caseSummaryBrandRelevance: nz(row.caseSummaryBrandRelevance),
      caseSummaryInterpretation: nz(row.caseSummaryInterpretation),
      caseSummaryTags: nz(row.caseSummaryTags) || chips,
      isCala: cardLooksCala(row),
      source: "fixture",
    };
  }

  const built = buildOpeningsPropertyCard({
    propertyName,
    brandName,
    marketCity,
    chips,
    locationLine: loc,
    metaLine: meta,
    scenarioLine: scenario,
    teaser,
    sourceUrl,
    caseSummaryOverview: nz(row.caseSummaryOverview) || teaser,
    caseSummaryTags: nz(row.caseSummaryTags) || chips,
  });
  return {
    ...built,
    caseSummaryOwnerObjective: nz(row.caseSummaryOwnerObjective),
    caseSummaryBrandRelevance: nz(row.caseSummaryBrandRelevance),
    caseSummaryInterpretation: nz(row.caseSummaryInterpretation),
    isCala: cardLooksCala({ ...built, caseSummaryTags: built.caseSummaryTags }),
    source: "fixture",
  };
}

function catalogToCard(entry, brandName) {
  const geo = nz(entry.geographyLabel);
  const geoParts = geo
    .split("/")
    .map((s) => s.trim())
    .filter(Boolean);
  const chips =
    nz(entry.chips) ||
    [geoParts[0] || "Market", entry.marketCity, geoParts[1] || geoParts[geoParts.length - 1] || "Urban"]
      .filter(Boolean)
      .join(", ");
  const locationLine =
    nz(entry.locationLine) ||
    [entry.marketCity, entry.stateRegion || geoParts.find((p) => !/^(cala|us|u\.s\.|global|europe)$/i.test(p))]
      .filter(Boolean)
      .join(", ") ||
    nz(entry.marketCity) ||
    "Market location";
  const metaLine =
    (nz(entry.meta) || "").replace(/Property Example · /i, "") ||
    geo ||
    nz(entry.stateRegion) ||
    nz(entry.marketCity) ||
    brandName;
  const scenarioLine =
    (nz(entry.scenario) || "").replace(/PROPERTY EXAMPLE\s*\/\s*/i, "").trim() ||
    chips
      .split(",")
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean)
      .slice(0, 4)
      .join(" / ");
  const teaser =
    nz(entry.teaser) ||
    nz(entry.ownerRelevance) ||
    `${nz(entry.propertyName)}${entry.marketCity ? ` in ${entry.marketCity}` : ""} is a ${brandName} property reference for owners underwriting design narrative, capital intensity, and systems participation for the specific asset.`;

  const built = buildOpeningsPropertyCard({
    propertyName: entry.propertyName,
    brandName,
    marketCity: entry.marketCity,
    country: entry.stateRegion || "",
    chips,
    locationLine,
    metaLine,
    scenarioLine,
    teaser,
    sourceUrl: entry.sourcePageUrl || "",
    caseSummaryOverview: teaser,
    caseSummaryTags: chips,
  });
  return {
    ...built,
    isCala: isCalaPropertyCatalogEntry(entry) || cardLooksCala(built),
    source: "catalog",
  };
}

function selectCalaFirst(cards, minimum = MIN_CARDS) {
  const cala = cards.filter((c) => c.isCala);
  const other = cards.filter((c) => !c.isCala);
  if (cala.length >= minimum) {
    return {
      selected: cala.slice(0, Math.max(minimum, Math.min(cala.length, 6))),
      tierUsed: "cala",
      calaCount: cala.length,
    };
  }
  const merged = [...cala, ...other];
  return {
    selected: merged.slice(0, Math.max(minimum, Math.min(merged.length, 6))),
    tierUsed: cala.length ? "cala_partial_fallback" : "non_cala_fallback",
    calaCount: cala.length,
  };
}

/**
 * Build preferred Ascend-shaped openings pack for one active brand.
 */
export function buildOpeningsAscendCalaPack(slug) {
  const identity = resolveSectionPatternBrandIdentity(slug);
  const brandName = identity.name || slug;
  /** @type {object[]} */
  let candidates = [];

  // 1) Lifestyle catalogs
  const catalog = CATALOG_BY_SLUG[slug];
  if (catalog?.length) {
    const picked = selectPropertyExamplesWithGeographicFallback(catalog, { minimum: MIN_CARDS });
    candidates = picked.selected.map((e) => catalogToCard(e, brandName));
  }

  // 2) Dedicated fixtures
  const fixtureRel = FIXTURE_BY_SLUG[slug];
  if (fixtureRel) {
    const rows = readFixtureRows(fixtureRel);
    for (const row of rows) {
      candidates.push(normalizeFixtureCard(row, brandName));
    }
  }

  // 3) Choice CALA curated/census (dynamic import path avoided — sync require via read of build fn)
  // Loaded lazily in async plan via buildCalaOpeningsForProfile when available.

  // Dedupe by title
  const seen = new Set();
  const deduped = [];
  for (const c of candidates) {
    const key = nz(c.title).toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    deduped.push(c);
  }

  const selection = selectCalaFirst(deduped, MIN_CARDS);
  return {
    brandSlug: slug,
    brandName,
    recordId: identity.recordId,
    ...selection,
    cards: selection.selected.map((c, i) => ({ ...c, sort: i + 1 })),
    candidateCount: deduped.length,
  };
}

/**
 * Enrich Choice packs using census/curated module (async-safe sync import).
 */
/**
 * Reject cross-brand CALA comps (e.g. Quality Inn used as Suburban context).
 */
function isSameBrandOpeningCard(card, brandName) {
  const brand = nz(brandName).toLowerCase();
  if (!brand) return true;
  const hay = [card.title, card.body, card.caseSummaryBrandRelevance, card.caseSummaryOverview]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  if (/\bcomp only\b|\bnot [a-z].{0,40}-flagged\b|\bportfolio reference—not\b/i.test(hay)) {
    return false;
  }
  // Brand token must appear in title or body for curated Choice cards.
  const brandToken = brand.replace(/\s+by choice$/i, "").trim();
  const compact = brandToken.replace(/&/g, "and");
  if (hay.includes(brandToken) || hay.includes(compact)) return true;
  // Short flags: RED, Blu
  if (/\bred\b/.test(brand) && /\bradisson\s+red\b/.test(hay)) return true;
  return false;
}

export async function enrichChoicePackIfNeeded(pack) {
  const slug = pack.brandSlug;
  const profile = CHOICE_PROFILE_BY_SLUG[slug];
  if (!profile) return pack;
  if (pack.calaCount >= MIN_CARDS && pack.cards.length >= MIN_CARDS) return pack;

  try {
    const { buildCalaOpeningsForProfile } = await import(
      "../../scripts/lib/choice-cala-openings-from-census.mjs"
    );
    const curated = buildCalaOpeningsForProfile(profile) || [];
    const fromCurated = curated
      .map((row) =>
        normalizeFixtureCard(
          {
            title: row.title,
            body: row.body,
            caseSummaryOverview: row.caseSummaryOverview,
            caseSummaryOwnerObjective: row.caseSummaryOwnerObjective,
            caseSummaryBrandRelevance: row.caseSummaryBrandRelevance,
            caseSummaryInterpretation: row.caseSummaryInterpretation,
            caseSummaryTags: row.caseSummaryTags,
          },
          pack.brandName
        )
      )
      .filter((c) => isSameBrandOpeningCard(c, pack.brandName));
    const merged = [...fromCurated, ...pack.cards];
    const seen = new Set();
    const deduped = [];
    for (const c of merged) {
      const key = nz(c.title).toLowerCase();
      if (!key || seen.has(key)) continue;
      seen.add(key);
      deduped.push(c);
    }
    const selection = selectCalaFirst(deduped, MIN_CARDS);
    return {
      ...pack,
      ...selection,
      cards: selection.selected.map((c, i) => ({ ...c, sort: i + 1 })),
      candidateCount: deduped.length,
      choiceCurated: fromCurated.length,
    };
  } catch (err) {
    return { ...pack, choiceEnrichError: String(err?.message || err) };
  }
}

/**
 * Fold live openings into the pack so CALA live rows (e.g. Concón) are kept
 * when catalog/fixture packs are short; then re-rank CALA-first.
 */
export function mergeLiveOpeningsIntoPack(pack, liveRows = []) {
  const brandName = pack.brandName;
  const fromLive = (liveRows || [])
    .map((row) =>
      normalizeFixtureCard(
        {
          title: row.title,
          body: row.body,
          caseSummaryOverview: row.caseSummaryOverview,
          caseSummaryOwnerObjective: row.caseSummaryOwnerObjective,
          caseSummaryBrandRelevance: row.caseSummaryBrandRelevance,
          caseSummaryInterpretation: row.caseSummaryInterpretation,
          caseSummaryTags: row.caseSummaryTags,
        },
        brandName
      )
    )
    .filter((c) => {
      const t = nz(c.title);
      if (!t) return false;
      if (openingsTitleLooksLikeLegacyPropertyExample(t)) return false;
      if (openingsTeaserLooksGeneric(c.body || c.caseSummaryOverview || "")) return false;
      // Skip directional / photography placeholders.
      if (/directional|employment corridor example|studio kitchenette example|weekly-stay employment/i.test(t)) {
        return false;
      }
      if (/corridor extended-stay example|conversion suite example|purpose-built suite example/i.test(t)) {
        return false;
      }
      if (/affiliation fit|collection fit/i.test(c.body || "")) return false;
      return true;
    });

  const merged = [...(pack.cards || []), ...fromLive];
  const seen = new Set();
  const deduped = [];
  for (const c of merged) {
    const key = nz(c.title)
      .toLowerCase()
      .replace(/\s+/g, " ")
      .replace(/,—|,/g, " ")
      .trim();
    // Soft-dedupe Humano Lima vs Humano, Lima, a Tribute…
    const soft = key
      .replace(/\ba\s+tribute\s+portfolio\s+hotel\b/g, "")
      .replace(/\btribute\s+portfolio\b/g, "")
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    if (!soft || seen.has(soft)) continue;
    seen.add(soft);
    deduped.push(c);
  }
  const selection = selectCalaFirst(deduped, Math.max(MIN_CARDS, Math.min(liveRows.length || MIN_CARDS, 6)));
  return {
    ...pack,
    ...selection,
    cards: selection.selected.map((c, i) => ({ ...c, sort: i + 1 })),
    candidateCount: deduped.length,
    liveMerged: fromLive.length,
  };
}

export function visibleOpenings(blocks) {
  return (blocks || [])
    .filter(
      (b) =>
        b.slotKey === OPENINGS_SLOT &&
        b.active !== false &&
        !/do not display|internal only/i.test(nz(b.externalDisplayStatus))
    )
    .sort((a, b) => Number(a.sort || 0) - Number(b.sort || 0));
}

/**
 * Plan PATCH fields for live openings rows (match by sort index).
 * Does not create new rows (preserves images on existing records).
 */
export function planOpeningsAscendCalaPatches(liveRows, pack) {
  const patches = [];
  const cards = pack.cards || [];
  const n = Math.min(liveRows.length, cards.length);
  for (let i = 0; i < n; i++) {
    const live = liveRows[i];
    const card = cards[i];
    if (!live?.recordId) continue;
    const fields = {};
    if (nz(live.title) !== nz(card.title)) fields.Title = card.title;
    if (nz(live.body) !== nz(card.body)) fields.Body = card.body;
    if (card.caseSummaryOverview && nz(live.caseSummaryOverview) !== nz(card.caseSummaryOverview)) {
      fields["Case Summary Overview"] = card.caseSummaryOverview;
    }
    if (card.caseSummaryTags && nz(live.caseSummaryTags) !== nz(card.caseSummaryTags)) {
      fields["Case Summary Tags"] = card.caseSummaryTags;
    }
    if (card.caseSummaryOwnerObjective) {
      fields["Case Summary Owner Objective"] = card.caseSummaryOwnerObjective;
    }
    if (card.caseSummaryBrandRelevance) {
      fields["Case Summary Brand Relevance"] = card.caseSummaryBrandRelevance;
    }
    if (card.caseSummaryInterpretation) {
      fields["Case Summary Interpretation"] = card.caseSummaryInterpretation;
    }
    if (Number(live.sort || 0) !== Number(card.sort || 0)) {
      fields["Sort Order"] = card.sort;
    }
    if (!Object.keys(fields).length) continue;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: live.recordId,
      slotKey: OPENINGS_SLOT,
      brandSlug: pack.brandSlug,
      priorTitle: live.title,
      title: card.title,
      isCala: card.isCala === true,
      fields,
      fieldMapping: {
        Title: "Ascend-style property title",
        Body: "chips / location / meta / scenario / teaser [/ url]",
        "Case Summary Overview": "View Property modal overview",
        "Case Summary Tags": "modal + chip backstop",
      },
      sanitizedPayloadPreview: {
        title: card.title,
        bodyPreview: nz(card.body).slice(0, 160),
        isCala: card.isCala,
      },
    });
  }

  // Hide leftover live openings beyond the Ascend/CALA pack so UI/audit
  // do not keep pre-template or duplicate Hilton-style cards.
  for (let i = cards.length; i < liveRows.length; i++) {
    const live = liveRows[i];
    if (!live?.recordId) continue;
    if (/do not display|internal only/i.test(nz(live.externalDisplayStatus))) continue;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: live.recordId,
      slotKey: OPENINGS_SLOT,
      brandSlug: pack.brandSlug,
      priorTitle: live.title,
      title: live.title,
      quarantine: true,
      fields: {
        "External Display Status": "Do Not Display",
      },
      fieldMapping: {
        "External Display Status": "Hide surplus openings beyond CALA-first pack",
      },
      sanitizedPayloadPreview: {
        title: live.title,
        externalDisplayStatus: "Do Not Display",
        reason: "unmatched_live_beyond_pack",
      },
    });
  }

  return {
    brandSlug: pack.brandSlug,
    brandName: pack.brandName,
    tierUsed: pack.tierUsed,
    calaCount: pack.calaCount,
    liveCount: liveRows.length,
    packCount: cards.length,
    patched: patches.length,
    unmatchedLive: Math.max(0, liveRows.length - cards.length),
    unmatchedPack: Math.max(0, cards.length - liveRows.length),
    patches,
    validation: {
      pass: cards.length >= MIN_CARDS && patches.length >= 0,
      failedChecks: cards.length < MIN_CARDS ? [`pack_below_min:${cards.length}`] : [],
    },
  };
}
