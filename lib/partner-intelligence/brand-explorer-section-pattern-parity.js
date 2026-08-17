/**
 * Brand Explorer — Section Pattern Parity
 *
 * A section does not pass simply because it has non-empty text.
 * It must match the established Brand Explorer product pattern used by
 * benchmark profiles (Tribute, Kimpton, Radisson Individuals, Design Hotels).
 *
 * Permanent Tab Factory gate: section_pattern_parity.
 */
import { buildMomentumBody, parseMomentumPresentationBody } from "./brand-explorer-momentum-link-label.js";
import {
  RECENT_MOMENTUM_MIN_CARDS,
  RECENT_MOMENTUM_MIN_LINKED_URLS,
  RECENT_MOMENTUM_MIN_STRUCTURED_DATES,
  isStructuredMomentumDateLine,
  looksLikeDiligenceFillerMomentum,
} from "./brand-explorer-recent-momentum-contract.js";
import {
  BUILT_BLOCKED_IDENTITIES,
  BUILT_BLOCKED_PROTECTED_PUBLIC_FULL,
  BUILT_BLOCKED_TARGETS,
  BUILT_BLOCKED_TRUE_INCOMPLETE,
} from "./brand-explorer-built-blocked-content.js";
import { STABILIZATION_BRAND_IDENTITY } from "./brand-explorer-public-profile-stabilization.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { resolveActiveUniverseRecordId } from "./brand-explorer-active-universe.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";

export const SECTION_PATTERN_PARITY_VERSION = "section-pattern-parity-v1";

export const SECTION_PATTERN_STATUSES = Object.freeze([
  "pass",
  "wrong_pattern",
  "too_generic",
  "source_note_style",
  "missing_region_breakdown",
  "missing_owner_interpretation",
  "unsupported_geography",
  "should_suppress",
  "needs_patch",
]);

export const SECTION_PATTERN_BENCHMARK_REFS = Object.freeze({
  recent_momentum: "Tribute / Kimpton / Design Hotels named openings with date + geography + owner relevance",
  geographic_footprint: "Tribute / Kimpton / Radisson Blu regional cards + brand-specific geo_intro",
  portfolio_context: "Benchmark overview.portfolio_context ladder + owner-facing positioning",
  growth_priorities: "Benchmark footprint.growth_themes + growth_editorial/fit chips and narrative",
});

/** Public-full + fullyReady cohort for this gate (excludes true-incomplete). */
export const SECTION_PATTERN_AUDIT_DEFAULT_BRANDS = Object.freeze([
  ...BUILT_BLOCKED_PROTECTED_PUBLIC_FULL,
  ...BUILT_BLOCKED_TARGETS,
]);

export const SECTION_PATTERN_TRUE_INCOMPLETE = BUILT_BLOCKED_TRUE_INCOMPLETE;

const EXTRA_IDENTITIES = Object.freeze({
  "hotel-indigo": { recordId: "recegXrqaPiSLGCIe", name: "Hotel Indigo" },
  "mgallery-collection": { recordId: "recrWCD1LMqu864oU", name: "MGallery Collection" },
  "small-luxury-hotels-of-the-world": {
    recordId: "recjjSnY2opb8P4DG",
    name: "Small Luxury Hotels of the World",
  },
  "radisson-individuals-by-choice": {
    recordId: "recRyvM8OmLlDj9G7",
    name: "Radisson Individuals by Choice",
  },
});

export function resolveSectionPatternBrandIdentity(slug) {
  const key = String(slug || "").toLowerCase();
  const active = getActiveProfileBrandConfig(key);
  if (active?.recordId) {
    return { slug: key, recordId: active.recordId, name: active.name || key };
  }
  const built = BUILT_BLOCKED_IDENTITIES[key];
  if (built?.recordId) {
    return { slug: key, recordId: built.recordId, name: built.name || key };
  }
  const stab = STABILIZATION_BRAND_IDENTITY[key];
  if (stab?.recordId) {
    return { slug: key, recordId: stab.recordId, name: stab.name || key };
  }
  const extra = EXTRA_IDENTITIES[key];
  if (extra?.recordId && !/Placeholder/i.test(extra.recordId)) {
    return { slug: key, recordId: extra.recordId, name: extra.name || key };
  }
  // Wave 12 / factory-preview cohort (Under Review; not Active/Live)
  const factory = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[key];
  if (factory?.recordId) {
    return { slug: key, recordId: factory.recordId, name: factory.name || key };
  }
  // Active/Live anchors + full-build identities (BW Premier/Signature, Preferred, etc.)
  const rid = resolveActiveUniverseRecordId(key);
  if (rid) return { slug: key, recordId: rid, name: key };
  return { slug: key, recordId: null, name: key };
}

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(s) {
  return nz(s).split(/\s+/).filter(Boolean).length;
}

function visibleRows(rows, slotKey) {
  return (rows || [])
    .filter(
      (r) =>
        nz(r.slotKey) === slotKey &&
        r.active !== false &&
        r.visible !== false &&
        !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
    )
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
}

function findVisible(rows, slotKey) {
  return visibleRows(rows, slotKey)[0] || null;
}

function parseMomentum(row) {
  const parsed = parseMomentumPresentationBody(row?.body, row?.title);
  return {
    title: nz(parsed.headline || row?.title),
    dateLine: nz(parsed.dateLine),
    summary: nz(parsed.description),
    url: nz(parsed.sourceUrl),
    body: nz(row?.body),
    recordId: row?.recordId || null,
  };
}

const SOURCE_NOTE_RES = [
  /\billustrative activity\b/i,
  /\bdirectional themes?\b/i,
  /\buse as directional(?:\s+context)?\b/i,
  /\bpublic (?:accor|ihg|choice|marriott).{0,60}materials\b/i,
  /\bsigned pipeline totals\b/i,
  /\binternal methodology\b/i,
  /\bsource note\b/i,
  /\bpresentation or verified disclosures when available\b/i,
];

const PARENT_GENERIC_RES = [
  /\baccor collection positioning\b/i,
  /\bpublic ihg lifestyle context\b/i,
  /\bslh consortium visibility\b/i,
  /\bbrand press resources updated\b/i,
  /\bdevelopment page signals\b/i,
  /\breinforces extended-stay portfolio strategy\b/i,
];

const OWNER_SIGNAL_RES = [
  /\bowner/i,
  /\baffiliation/i,
  /\bconversion/i,
  /\bunderwrit/i,
  /\bdiligence/i,
  /\bfranchise/i,
  /\bmembership/i,
  /\bcompare/i,
  /\bevaluat/i,
  /\bfit\b/i,
];

const GEO_SIGNAL_RES = [
  /\b[A-Z][a-záéíóúñ]+(?:\s+[A-Z][a-záéíóúñ]+)?\b/,
  /\b(mexico|peru|colombia|caribbean|cayman|cayman|lima|cayman|europe|asia|americas|cala|mea|apac|portugal|france|italy)\b/i,
];

function looksSourceNote(text) {
  return SOURCE_NOTE_RES.some((re) => re.test(nz(text)));
}

function looksParentGeneric(text) {
  return PARENT_GENERIC_RES.some((re) => re.test(nz(text)));
}

function hasOwnerInterpretation(text) {
  return OWNER_SIGNAL_RES.some((re) => re.test(nz(text))) && words(text) >= 12;
}

function hasGeoOrPropertySignal(text) {
  return GEO_SIGNAL_RES.some((re) => re.test(nz(text)));
}

function brandToken(brandName, brandSlug) {
  const name = nz(brandName);
  if (name) return name.split(/\s+/)[0];
  return nz(brandSlug).split("-")[0];
}

function brandTokens(brandName, brandSlug) {
  const stop = new Set([
    "the",
    "of",
    "by",
    "and",
    "a",
    "an",
    "hotel",
    "hotels",
    "collection",
    "suites",
    "inn",
    "world",
    "small",
    "luxury",
  ]);
  const fromName = nz(brandName)
    .split(/[^A-Za-z0-9]+/)
    .map((t) => t.trim())
    .filter((t) => t.length >= 4 && !stop.has(t.toLowerCase()));
  const fromSlug = nz(brandSlug)
    .split("-")
    .filter((t) => t.length >= 4 && !stop.has(t.toLowerCase()));
  const tokens = [...new Set([...fromName, ...fromSlug])];
  // Prefer distinctive brand tokens
  if (/mgallery/i.test(`${brandName} ${brandSlug}`)) tokens.unshift("MGallery");
  if (/indigo/i.test(`${brandName} ${brandSlug}`)) tokens.unshift("Indigo");
  if (/kimpton/i.test(`${brandName} ${brandSlug}`)) tokens.unshift("Kimpton");
  if (/tribute/i.test(`${brandName} ${brandSlug}`)) tokens.unshift("Tribute");
  if (/slh|small-luxury/i.test(`${brandName} ${brandSlug}`)) tokens.unshift("SLH");
  // Basics primary name is often "SO/" (too short for token filters); slug carries resorts.
  if (/so-hotels|SO\//i.test(`${brandName} ${brandSlug}`)) {
    tokens.unshift("SO/");
    tokens.unshift("resorts");
  }
  return tokens.length ? tokens : [brandToken(brandName, brandSlug)].filter(Boolean);
}

function brandSpecific(text, brandName, brandSlug) {
  const tokens = brandTokens(brandName, brandSlug);
  if (!tokens.length) return true;
  const corpus = nz(text);
  return tokens.some((token) => {
    if (!token || token.length < 3) return false;
    return new RegExp(token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(corpus);
  });
}

/**
 * Evaluate Recent Momentum against benchmark product pattern.
 */
export function evaluateRecentMomentumPattern({
  brandSlug,
  brandName,
  presentationRows = [],
  html = "",
} = {}) {
  const labelRow = findVisible(presentationRows, "footprint.momentum_label");
  const momentumRows = visibleRows(presentationRows, "footprint.momentum").map(parseMomentum);
  const label = nz(labelRow?.body || labelRow?.title);
  const momentumCorpus = [label, ...momentumRows.map((m) => `${m.title}\n${m.body}`)].join("\n");
  const momentumHtml = (() => {
    const m = nz(html).match(
      /<h2[^>]*>\s*Recent Momentum\s*<\/h2>[\s\S]*?(?=<h2[^>]*>|<section class="oe-section">|$)/i
    );
    return m ? m[0] : "";
  })();
  const failures = [];
  let status = "pass";
  let currentPattern = "empty";
  const expectedPattern = SECTION_PATTERN_BENCHMARK_REFS.recent_momentum;

  if (!momentumRows.length) {
    currentPattern = "missing_momentum_cards";
    status = "should_suppress";
    failures.push("no_momentum_cards");
    return sectionResult({
      section: "recent_momentum",
      status,
      currentPattern,
      expectedPattern,
      failureReason: "No Recent Momentum cards. Use a clean limited-activity state or suppress; do not show filler.",
      proposedPatch: "Add 2–4 named activity cards (opening/conversion/membership) or suppress section.",
      failures,
      metrics: { cardCount: 0, labeled: Boolean(label) },
    });
  }

  const titled = momentumRows.filter((m) => words(m.title) >= 3);
  const dated = momentumRows.filter((m) =>
    /\b(20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/i.test(
      `${m.dateLine || ""} ${m.body || ""} ${m.summary || ""}`
    )
  );
  const withGeo = momentumRows.filter((m) => hasGeoOrPropertySignal(`${m.title} ${m.summary}`));
  const withOwner = momentumRows.filter((m) =>
    hasOwnerInterpretation(`${m.title} ${m.summary || m.body}`)
  );
  const withUrl = momentumRows.filter((m) => /^https?:\/\//i.test(m.url));

  if (momentumRows.length === 1 && !titled.length) {
    currentPattern = "single_untitled_blob";
  } else if (momentumRows.length === 1) {
    currentPattern = "single_card";
  } else if (titled.length >= 2 && dated.length >= 2) {
    currentPattern = "benchmark_card_list";
  } else {
    currentPattern = "partial_cards";
  }

  const illustrativeHint = /\billustrative activity\b/i.test(momentumHtml);
  const sourceNoteCopy = looksSourceNote(label) || looksSourceNote(momentumCorpus);
  if (illustrativeHint || sourceNoteCopy) {
    status = "source_note_style";
    failures.push("source_note_or_illustrative_framing");
  }
  if (looksParentGeneric(momentumCorpus)) {
    if (status === "pass") status = "too_generic";
    failures.push("parent_company_generic_framing");
  }
  if (momentumRows.length < RECENT_MOMENTUM_MIN_CARDS) {
    if (status === "pass") status = "needs_patch";
    failures.push(`card_count_below_min:${momentumRows.length}`);
  }
  if (titled.length < RECENT_MOMENTUM_MIN_CARDS) {
    if (status === "pass") status = "wrong_pattern";
    failures.push(`titled_cards_below_min:${titled.length}`);
  }
  if (dated.length < 2) {
    if (status === "pass") status = "needs_patch";
    failures.push(`dated_cards_below_min:${dated.length}`);
  }
  if (withGeo.length < 2) {
    if (status === "pass") status = "needs_patch";
    failures.push(`geo_or_property_signal_below_min:${withGeo.length}`);
  }
  // Strong named opening cards can pass without explicit "owner" wording;
  // require owner interpretation when the pattern is weak/generic.
  const strongNamedOpenings = titled.length >= 2 && dated.length >= 2 && withGeo.length >= 2;
  if (withOwner.length < 1 && !strongNamedOpenings) {
    if (status === "pass") status = "missing_owner_interpretation";
    failures.push("missing_owner_facing_interpretation");
  }
  if (!brandSpecific(momentumCorpus, brandName, brandSlug)) {
    if (status === "pass") status = "too_generic";
    failures.push("copy_not_brand_specific");
  }
  if (momentumRows.length === 1 && words(momentumRows[0].body) > 20 && !titled.length) {
    status = "wrong_pattern";
    failures.push("untitled_directional_blob");
  }
  if (withUrl.length < RECENT_MOMENTUM_MIN_LINKED_URLS) {
    if (status === "pass") status = "needs_patch";
    failures.push(`linked_announcement_url_below_min:${withUrl.length}`);
  }
  // Collapsed scrubber bodies mash date into the summary and drop blank-line structure.
  const structuredDate = momentumRows.filter((m) => isStructuredMomentumDateLine(m.dateLine));
  if (structuredDate.length < RECENT_MOMENTUM_MIN_STRUCTURED_DATES) {
    if (status === "pass") status = "wrong_pattern";
    failures.push(`structured_date_line_below_min:${structuredDate.length}`);
  }
  if (looksLikeDiligenceFillerMomentum(momentumCorpus)) {
    if (status === "pass") status = "wrong_pattern";
    failures.push("diligence_filler_not_openings_press");
  }

  const failureReason = failures.length ? failures.join("; ") : null;

  return sectionResult({
    section: "recent_momentum",
    status: failures.length ? status : "pass",
    currentPattern,
    expectedPattern,
    failureReason,
    proposedPatch: failures.length
      ? "Replace with 2–4 named momentum cards (activity type, geography/property, timeframe, owner relevance, linked source when available). Remove Illustrative activity / directional themes framing."
      : null,
    failures,
    metrics: {
      cardCount: momentumRows.length,
      titledCount: titled.length,
      datedCount: dated.length,
      geoSignalCount: withGeo.length,
      ownerSignalCount: withOwner.length,
      urlCount: withUrl.length,
      labeled: Boolean(label),
      labelPreview: label.slice(0, 120),
    },
  });
}

const REGION_SLOTS = Object.freeze([
  "footprint.region.am",
  "footprint.region.cala",
  "footprint.region.eu",
  "footprint.region.mea",
  "footprint.region.apac",
]);

/**
 * Evaluate Geographic Footprint against benchmark regional pattern.
 */
export function evaluateGeographicFootprintPattern({
  brandSlug,
  brandName,
  presentationRows = [],
} = {}) {
  const geo =
    findVisible(presentationRows, "footprint.geo_intro") ||
    findVisible(presentationRows, "footprint.geo.summary");
  const regions = REGION_SLOTS.map((slot) => {
    const row = findVisible(presentationRows, slot);
    return {
      slot,
      title: nz(row?.title),
      body: nz(row?.body),
      words: words(row?.body),
      empty: !row || words(row?.body) < 12,
      recordId: row?.recordId || null,
    };
  });
  const filledRegions = regions.filter((r) => !r.empty);
  const failures = [];
  let status = "pass";
  const expectedPattern = SECTION_PATTERN_BENCHMARK_REFS.geographic_footprint;
  let currentPattern = filledRegions.length >= 3 ? "regional_breakdown" : "thin_or_missing_regions";

  if (!geo || words(geo.body) < 30) {
    status = "needs_patch";
    failures.push(`thin_or_missing_geo_intro:${geo ? words(geo.body) : 0}`);
  }
  if (filledRegions.length < 3) {
    status = "missing_region_breakdown";
    failures.push(`filled_regions_below_min:${filledRegions.length}`);
    currentPattern = "missing_region_breakdown";
  }
  if (regions.some((r) => r.title && r.empty)) {
    if (status === "pass") status = "needs_patch";
    failures.push("empty_region_cards_present");
  }
  const geoCorpus = [nz(geo?.body), ...filledRegions.map((r) => r.body)].join("\n");
  const strongRegional =
    filledRegions.length >= 3 && (geo ? words(geo.body) >= 30 : false) && brandSpecific(geoCorpus, brandName, brandSlug);
  if (!hasOwnerInterpretation(geoCorpus) && !strongRegional) {
    if (status === "pass") status = "missing_owner_interpretation";
    failures.push("missing_owner_facing_footprint_interpretation");
  }
  if (!brandSpecific(geoCorpus, brandName, brandSlug)) {
    if (status === "pass") status = "too_generic";
    failures.push("footprint_not_brand_specific");
  }
  if (looksSourceNote(geoCorpus)) {
    if (status === "pass") status = "source_note_style";
    failures.push("footprint_source_note_style");
  }

  // Soft-brand / affiliation: do not invent CALA when only European examples exist for SLH mislabels
  if (brandSlug === "small-luxury-hotels-of-the-world") {
    for (const r of filledRegions) {
      if (/cala/i.test(r.slot) && /(portugal|france|milan|rome|copenhagen)/i.test(r.body) && !/(caribbean|latin|mexico|colombia|peru|brazil)/i.test(r.body)) {
        status = "unsupported_geography";
        failures.push(`mislabeled_cala_geography:${r.slot}`);
      }
    }
  }

  return sectionResult({
    section: "geographic_footprint",
    status: failures.length ? status : "pass",
    currentPattern,
    expectedPattern,
    failureReason: failures.length ? failures.join("; ") : null,
    proposedPatch: failures.length
      ? "Provide brand-specific geo_intro plus ≥3 filled regional cards (Americas/CALA/Europe/MEA/APAC as relevant) with presence signal and owner interpretation. No empty region panels."
      : null,
    failures,
    metrics: {
      geoWords: geo ? words(geo.body) : 0,
      filledRegionCount: filledRegions.length,
      emptyRegionCount: regions.filter((r) => r.empty).length,
      regions: regions.map((r) => ({ slot: r.slot, empty: r.empty, words: r.words, title: r.title })),
    },
  });
}

export function evaluatePortfolioContextPattern({ brandSlug, brandName, presentationRows = [] } = {}) {
  const row = findVisible(presentationRows, "overview.portfolio_context");
  const failures = [];
  let status = "pass";
  if (!row || words(row.body) < 25) {
    status = "needs_patch";
    failures.push(`thin_or_missing_portfolio_context:${row ? words(row.body) : 0}`);
  } else if (!brandSpecific(row.body, brandName, brandSlug)) {
    status = "too_generic";
    failures.push("portfolio_context_not_brand_specific");
  }
  // Owner interpretation is preferred but not required when body is brand-specific and substantial.
  return sectionResult({
    section: "portfolio_context",
    status,
    currentPattern: row ? "present" : "missing",
    expectedPattern: SECTION_PATTERN_BENCHMARK_REFS.portfolio_context,
    failureReason: failures.length ? failures.join("; ") : null,
    proposedPatch: failures.length
      ? "Add owner-facing portfolio ladder context specific to this brand (not parent-umbrella filler)."
      : null,
    failures,
    metrics: { words: row ? words(row.body) : 0, title: nz(row?.title) },
  });
}

export function evaluateGrowthPrioritiesPattern({ brandSlug, brandName, presentationRows = [] } = {}) {
  const themes = findVisible(presentationRows, "footprint.growth_themes");
  const editorial =
    findVisible(presentationRows, "footprint.growth_editorial") ||
    findVisible(presentationRows, "footprint.growth_fit");
  const themeCount = themes
    ? nz(themes.body)
        .split(/[\n;]+/)
        .map((t) => t.trim())
        .filter(Boolean).length
    : 0;
  const failures = [];
  let status = "pass";
  if (!editorial || words(editorial.body) < 30) {
    status = "needs_patch";
    failures.push(`weak_growth_narrative:${editorial ? words(editorial.body) : 0}`);
  }
  if (themeCount < 2) {
    if (status === "pass") status = "needs_patch";
    failures.push(`growth_theme_chips_below_min:${themeCount}`);
  }
  const corpus = `${nz(themes?.body)}\n${nz(editorial?.body)}`;
  if (editorial && !brandSpecific(corpus, brandName, brandSlug)) {
    if (status === "pass") status = "too_generic";
    failures.push("growth_priorities_not_brand_specific");
  }
  return sectionResult({
    section: "growth_priorities",
    status,
    currentPattern: editorial && themeCount >= 2 ? "themes_plus_narrative" : "incomplete",
    expectedPattern: SECTION_PATTERN_BENCHMARK_REFS.growth_priorities,
    failureReason: failures.length ? failures.join("; ") : null,
    proposedPatch: failures.length
      ? "Add ≥2 growth theme chips plus owner-facing growth editorial/fit narrative aligned to brand model."
      : null,
    failures,
    metrics: { themeCount, editorialWords: editorial ? words(editorial.body) : 0 },
  });
}

function sectionResult({
  section,
  status,
  currentPattern,
  expectedPattern,
  failureReason,
  proposedPatch,
  failures,
  metrics,
}) {
  return {
    section,
    status,
    pass: status === "pass",
    currentPattern,
    expectedPattern,
    failureReason,
    proposedPatch,
    benchmarkReference: expectedPattern,
    failures,
    metrics,
  };
}

/**
 * Full section pattern parity evaluation for one brand payload.
 */
export function evaluateSectionPatternParity({
  brandSlug,
  brandName,
  presentationRows = [],
  html = "",
} = {}) {
  const recent_momentum = evaluateRecentMomentumPattern({
    brandSlug,
    brandName,
    presentationRows,
    html,
  });
  const geographic_footprint = evaluateGeographicFootprintPattern({
    brandSlug,
    brandName,
    presentationRows,
  });
  const portfolio_context = evaluatePortfolioContextPattern({
    brandSlug,
    brandName,
    presentationRows,
  });
  const growth_priorities = evaluateGrowthPrioritiesPattern({
    brandSlug,
    brandName,
    presentationRows,
  });

  const sections = {
    recent_momentum,
    geographic_footprint,
    portfolio_context,
    growth_priorities,
  };

  const recent_momentum_pattern_pass = recent_momentum.pass === true;
  const geographic_footprint_pattern_pass = geographic_footprint.pass === true;
  const portfolio_context_pattern_pass = portfolio_context.pass === true;
  const growth_priorities_pattern_pass = growth_priorities.pass === true;

  const pass =
    recent_momentum_pattern_pass &&
    geographic_footprint_pattern_pass &&
    portfolio_context_pattern_pass &&
    growth_priorities_pattern_pass;

  return {
    version: SECTION_PATTERN_PARITY_VERSION,
    brandSlug,
    brandName: brandName || brandSlug,
    pass,
    gates: {
      recent_momentum_pattern_pass,
      geographic_footprint_pattern_pass,
      portfolio_context_pattern_pass,
      growth_priorities_pattern_pass,
      section_pattern_parity: pass,
    },
    sections,
    findings: Object.values(sections).filter((s) => !s.pass),
  };
}

/** Helper for remediation content authors. */
export function momentumCard({ title, dateLine, summary, url, sort = 0 }) {
  return {
    slotKey: "footprint.momentum",
    title,
    body: buildMomentumBody({ dateLine, summary, sourceUrl: url }),
    sort,
  };
}

export function regionCard(slotKey, title, body, sort = 0) {
  return { slotKey, title, body, sort };
}
