/**
 * Brand Explorer — Section Pattern Parity content packs.
 *
 * Registry of remediation packs that replace wrong-pattern Recent Momentum
 * (untitled blobs / source-note framing) and thin Geographic Footprint regions
 * with benchmark-shaped cards (Tribute / Kimpton / Design Hotels pattern).
 *
 * Permanent Recent Momentum template: buildRecentMomentumCard + contract module.
 * Future brands: copy brand-explorer-section-pattern-parity-content-_TEMPLATE.js
 * and register here in CONTENT_BY_SLUG.
 *
 * @see brand-explorer-section-pattern-parity.js
 * @see brand-explorer-recent-momentum-contract.js
 */
import { buildMomentumBody } from "./brand-explorer-momentum-link-label.js";
import {
  buildRecentMomentumCard,
  withRecentMomentumSortOrder,
  RECENT_MOMENTUM_DEFAULT_LABEL,
} from "./brand-explorer-recent-momentum-contract.js";
import { momentumCard, regionCard } from "./brand-explorer-section-pattern-parity.js";
import { MGALLERY_SECTION_PATTERN_PARITY_CONTENT } from "./brand-explorer-section-pattern-parity-content-mgallery.js";
import { HOTEL_INDIGO_SECTION_PATTERN_PARITY_CONTENT } from "./brand-explorer-section-pattern-parity-content-hotel-indigo.js";
import { SLH_SECTION_PATTERN_PARITY_CONTENT } from "./brand-explorer-section-pattern-parity-content-slh.js";
import { COMFORT_SECTION_PATTERN_PARITY_CONTENT } from "./brand-explorer-section-pattern-parity-content-comfort.js";
import { COUNTRY_SECTION_PATTERN_PARITY_CONTENT } from "./brand-explorer-section-pattern-parity-content-country.js";
import { SUBURBAN_SECTION_PATTERN_PARITY_CONTENT } from "./brand-explorer-section-pattern-parity-content-suburban.js";
import { WOODSPRING_SECTION_PATTERN_PARITY_CONTENT } from "./brand-explorer-section-pattern-parity-content-woodspring.js";
import { QUALITY_INN_SECTION_PATTERN_PARITY_CONTENT } from "./brand-explorer-section-pattern-parity-content-quality-inn.js";
import {
  RADISSON_SECTION_PATTERN_PARITY_CONTENT,
  RADISSON_BLU_SECTION_PATTERN_PARITY_CONTENT,
  RADISSON_RED_SECTION_PATTERN_PARITY_CONTENT,
} from "./brand-explorer-section-pattern-parity-content-radisson-family.js";
import {
  ASCEND_SECTION_PATTERN_PARITY_CONTENT,
  CURIO_SECTION_PATTERN_PARITY_CONTENT,
  DESIGN_HOTELS_SECTION_PATTERN_PARITY_CONTENT,
  EVERHOME_SECTION_PATTERN_PARITY_CONTENT,
  KIMPTON_SECTION_PATTERN_PARITY_CONTENT,
  RADISSON_INDIVIDUALS_SECTION_PATTERN_PARITY_CONTENT,
  TRIBUTE_SECTION_PATTERN_PARITY_CONTENT,
} from "./brand-explorer-section-pattern-parity-content-benchmark-momentum.js";

export const SECTION_PATTERN_PARITY_CONTENT_VERSION = "section-pattern-parity-content-v1";

export const CONTENT_BY_SLUG = Object.freeze({
  "mgallery-collection": MGALLERY_SECTION_PATTERN_PARITY_CONTENT,
  "hotel-indigo": HOTEL_INDIGO_SECTION_PATTERN_PARITY_CONTENT,
  "small-luxury-hotels-of-the-world": SLH_SECTION_PATTERN_PARITY_CONTENT,
  "comfort-inn-suites": COMFORT_SECTION_PATTERN_PARITY_CONTENT,
  "country-inn-suites": COUNTRY_SECTION_PATTERN_PARITY_CONTENT,
  "suburban-studios": SUBURBAN_SECTION_PATTERN_PARITY_CONTENT,
  "woodspring-suites": WOODSPRING_SECTION_PATTERN_PARITY_CONTENT,
  "quality-inn": QUALITY_INN_SECTION_PATTERN_PARITY_CONTENT,
  radisson: RADISSON_SECTION_PATTERN_PARITY_CONTENT,
  "radisson-blu": RADISSON_BLU_SECTION_PATTERN_PARITY_CONTENT,
  "radisson-red": RADISSON_RED_SECTION_PATTERN_PARITY_CONTENT,
  ascend: ASCEND_SECTION_PATTERN_PARITY_CONTENT,
  "curio-collection": CURIO_SECTION_PATTERN_PARITY_CONTENT,
  "design-hotels": DESIGN_HOTELS_SECTION_PATTERN_PARITY_CONTENT,
  "everhome-suites": EVERHOME_SECTION_PATTERN_PARITY_CONTENT,
  kimpton: KIMPTON_SECTION_PATTERN_PARITY_CONTENT,
  "radisson-individuals-by-choice": RADISSON_INDIVIDUALS_SECTION_PATTERN_PARITY_CONTENT,
  "tribute-portfolio": TRIBUTE_SECTION_PATTERN_PARITY_CONTENT,
});

export function getSectionPatternParityContent(slug) {
  const key = String(slug || "")
    .trim()
    .toLowerCase();
  return CONTENT_BY_SLUG[key] || null;
}

export function listSectionPatternParityContentBrands() {
  return Object.keys(CONTENT_BY_SLUG);
}

/**
 * Normalize a pack's momentumCards to the permanent Recent Momentum template.
 * Body = date + summary + trailing https announcement URL (newest-first Sort Order).
 */
export function normalizeMomentumCards(pack) {
  const cards = pack?.momentumCards || [];
  return withRecentMomentumSortOrder(
    cards.map((c, i) => {
      const title = String(c.title || "").trim();
      const dateLine = String(c.dateLine || "").trim();
      const summary = String(c.summary || "").trim();
      const url = String(c.url || "").trim();
      const sort = Number.isFinite(c.sort) ? c.sort : i + 1;
      return buildRecentMomentumCard({ title, dateLine, summary, url, sort });
    })
  );
}

/**
 * Build presentation-row shaped patches from a content pack.
 * Does not write Company Validated / release fields.
 */
export function buildSectionPatternParityPresentationRows(pack) {
  if (!pack?.brandSlug) return [];
  const rows = [];
  const cards = normalizeMomentumCards(pack);

  if (pack.momentumLabel) {
    rows.push({
      slotKey: "footprint.momentum_label",
      title: "",
      body: pack.momentumLabel,
      sort: 0,
    });
  }

  if (pack.replaceMomentum !== false || cards.length) {
    for (const c of cards) {
      rows.push(
        momentumCard({
          title: c.title,
          dateLine: c.dateLine,
          summary: c.summary,
          url: c.url,
          sort: c.sort,
        })
      );
    }
  }

  if (pack.geoIntro) {
    rows.push({
      slotKey: "footprint.geo_intro",
      title: "Geographic footprint",
      body: pack.geoIntro,
      sort: 10,
    });
  }

  for (const r of pack.regions || []) {
    rows.push(regionCard(r.slotKey, r.title || "", r.body || "", r.sort ?? 11));
  }

  if (pack.growthThemes) {
    rows.push({
      slotKey: "footprint.growth_themes",
      title: "Growth themes",
      body: pack.growthThemes,
      sort: 20,
    });
  }

  if (pack.growthEditorial) {
    rows.push({
      slotKey: "footprint.growth_editorial",
      title: "Growth editorial",
      body: pack.growthEditorial,
      sort: 21,
    });
  }

  if (pack.portfolioContext?.body) {
    rows.push({
      slotKey: "overview.portfolio_context",
      title: pack.portfolioContext.title || "Portfolio context",
      body: pack.portfolioContext.body,
      sort: 90,
    });
  }

  return rows;
}

export {
  MGALLERY_SECTION_PATTERN_PARITY_CONTENT,
  HOTEL_INDIGO_SECTION_PATTERN_PARITY_CONTENT,
  SLH_SECTION_PATTERN_PARITY_CONTENT,
  COMFORT_SECTION_PATTERN_PARITY_CONTENT,
  COUNTRY_SECTION_PATTERN_PARITY_CONTENT,
  SUBURBAN_SECTION_PATTERN_PARITY_CONTENT,
  WOODSPRING_SECTION_PATTERN_PARITY_CONTENT,
  QUALITY_INN_SECTION_PATTERN_PARITY_CONTENT,
  RADISSON_SECTION_PATTERN_PARITY_CONTENT,
  RADISSON_BLU_SECTION_PATTERN_PARITY_CONTENT,
  RADISSON_RED_SECTION_PATTERN_PARITY_CONTENT,
};
