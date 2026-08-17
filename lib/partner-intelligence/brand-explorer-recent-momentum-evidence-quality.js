/**
 * Brand Explorer — Recent Momentum / Openings Evidence Quality gate.
 *
 * Permanent factory gate: fails when momentum/openings evidence is thin,
 * mislabeled, wrong-brand, missing date/link, or shows raw URLs in public HTML.
 */
import {
  parseMomentumPresentationBody,
  buildMomentumBody,
} from "./brand-explorer-momentum-link-label.js";
import {
  isStructuredMomentumDateLine,
  looksLikeDiligenceFillerMomentum,
  RECENT_MOMENTUM_MIN_CARDS,
} from "./brand-explorer-recent-momentum-contract.js";

export const MOMENTUM_EVIDENCE_QUALITY_VERSION = "recent-momentum-evidence-quality-v1";

export const MOMENTUM_EVIDENCE_TARGET_SLUGS = Object.freeze([
  "dazzler-by-wyndham",
  "trademark-collection-by-wyndham",
  "tapestry-collection-by-hilton",
]);

export const EVIDENCE_TYPES = Object.freeze([
  "opening",
  "signing",
  "conversion",
  "development_announcement",
  "portfolio_expansion",
  "brand_milestone",
  "official_brand_property_proof",
]);

export const REGION_LABELS = Object.freeze(["CALA", "International Reference"]);

const CALA_RE =
  /\b(cala|latam|latin america|caribbean|mexico|argentina|brazil|colombia|chile|peru|ecuador|panama|costa rica|dominican|puerto rico|uruguay|paraguay|guatemala|honduras|nicaragua|el salvador|jamaica|bahamas|barbados|trinidad|cuba|belize|guyana|suriname|venezuela)\b/i;

const INT_REF_RE = /\binternational reference\b/i;

const GENERIC_RE =
  /\b(directional themes?|illustrative activity|owner diligence|confirm current activity|not a property-level|pipeline disclosure)\b/i;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(text) {
  return nz(text)
    .split(/\s+/)
    .filter(Boolean).length;
}

function isHidden(row) {
  return (
    row?.active === false ||
    /do not display|internal only/i.test(nz(row?.externalDisplayStatus))
  );
}

function visibleRows(rows, slotKey) {
  return (rows || [])
    .filter((r) => nz(r.slotKey) === slotKey && !isHidden(r))
    .sort((a, b) => Number(a.sortOrder ?? 0) - Number(b.sortOrder ?? 0));
}

export function classifyRegionFromText(text) {
  const t = nz(text);
  if (INT_REF_RE.test(t)) return "International Reference";
  if (CALA_RE.test(t)) return "CALA";
  return null;
}

export function brandTokenMatchers(brandSlug, brandName = "") {
  const slug = nz(brandSlug).toLowerCase();
  const name = nz(brandName).toLowerCase();
  // Require brand token; sibling-brand contrast in copy is allowed when the home brand is present.
  if (slug.includes("dazzler")) {
    return { must: [/dazzler/i], forbid: [] };
  }
  if (slug.includes("trademark")) {
    return { must: [/trademark/i], forbid: [] };
  }
  if (slug.includes("tapestry")) {
    return { must: [/tapestry/i], forbid: [] };
  }
  if (slug.includes("voco")) {
    return { must: [/voco/i], forbid: [] };
  }
  if (slug.includes("even")) {
    return { must: [/even/i], forbid: [] };
  }
  if (slug.includes("avid")) {
    return { must: [/avid/i], forbid: [] };
  }
  if (slug.includes("moxy")) {
    return { must: [/moxy/i], forbid: [] };
  }
  if (slug.includes("motto")) {
    return { must: [/motto/i], forbid: [] };
  }
  if (slug.includes("tempo")) {
    return { must: [/tempo/i], forbid: [] };
  }
  if (slug.includes("canopy")) {
    return { must: [/canopy/i], forbid: [] };
  }
  if (slug.includes("bunkhouse")) {
    return { must: [/bunkhouse/i], forbid: [] };
  }
  if (slug.includes("city-express")) {
    return { must: [/city express/i], forbid: [] };
  }
  if (slug.includes("holiday-inn-express")) {
    return { must: [/holiday inn express/i], forbid: [] };
  }
  if (slug.includes("courtyard")) {
    return { must: [/courtyard/i], forbid: [] };
  }
  if (slug.includes("ac-hotels")) {
    return { must: [/\bac\b/i, /ac hotel/i], forbid: [] };
  }
  if (slug.includes("so-hotels") || /so\//i.test(name)) {
    return { must: [/SO\//i, /so\/\s*hotels/i], forbid: [] };
  }
  return {
    must: name ? [new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i")] : [],
    forbid: [],
  };
}

function corpusHasWrongBrand(corpus, brandSlug, brandName) {
  const { must, forbid } = brandTokenMatchers(brandSlug, brandName);
  if (must.length && !must.some((re) => re.test(corpus))) return "missing_brand_token";
  for (const re of forbid) {
    if (re.test(corpus)) return `wrong_brand_token:${re.source}`;
  }
  return null;
}

function rawUrlVisibleInHtml(html, sectionHint = "Recent Momentum") {
  const blob = nz(html);
  if (!blob) return { count: 0, samples: [] };
  const sectionRe = new RegExp(
    `<h2[^>]*>\\s*${sectionHint}\\s*<\\/h2>[\\s\\S]*?(?=<h2[^>]*>|<section class="oe-section">|$)`,
    "i"
  );
  const m = blob.match(sectionRe);
  const slice = m ? m[0] : "";
  if (!slice) return { count: 0, samples: [] };
  // Hyperlinks are allowed; fail only when a raw URL remains as visible text.
  const stripped = slice
    .replace(/<a\b[^>]*>[\s\S]*?<\/a>/gi, " ")
    .replace(/\b(?:href|src)\s*=\s*["'][^"']*["']/gi, "")
    .replace(/<[^>]+>/g, " ");
  const hits = stripped.match(/https?:\/\/[^\s<]+/gi) || [];
  return { count: hits.length, samples: hits.slice(0, 5) };
}

export function hasCalaInventory(brandSlug, propertyCatalog = []) {
  return (propertyCatalog || []).some((p) =>
    CALA_RE.test(`${p.geographyLabel || ""} ${p.marketCity || ""} ${p.stateRegion || ""} ${p.chips || ""}`)
  );
}

/**
 * Evaluate one brand's momentum + openings evidence quality.
 */
export function evaluateRecentMomentumEvidenceQuality({
  brandSlug,
  brandName = "",
  presentationRows = [],
  html = "",
  propertyCatalog = [],
  calaAvailableOverride = null,
} = {}) {
  const failures = [];
  const momentumCards = [];
  const openingsCards = [];

  const momentumRows = visibleRows(presentationRows, "footprint.momentum");
  const openingsRows = visibleRows(presentationRows, "footprint.openings");
  const calaAvailable =
    calaAvailableOverride != null ? calaAvailableOverride : hasCalaInventory(brandSlug, propertyCatalog);

  if (momentumRows.length < RECENT_MOMENTUM_MIN_CARDS) {
    failures.push({
      id: "momentum_card_count_below_min",
      severity: "fail",
      detail: `cards=${momentumRows.length} min=${RECENT_MOMENTUM_MIN_CARDS}`,
    });
  }

  let calaMomentumCount = 0;
  let intlLabeledCount = 0;
  let firstRegion = null;

  for (const row of momentumRows) {
    const parsed = parseMomentumPresentationBody(row.body, row.title);
    const corpus = [row.title, parsed.description, parsed.dateLine, row.caseSummaryTags].join("\n");
    const region =
      classifyRegionFromText(corpus) ||
      classifyRegionFromText(row.caseSummaryTags || "") ||
      classifyRegionFromText(row.title || "");
    const issues = [];

    if (!nz(row.title) || words(row.title) < 3) issues.push("title_too_thin");
    if (!nz(parsed.dateLine) || !isStructuredMomentumDateLine(parsed.dateLine)) {
      // Allow Directory / Collection as structured evidence labels used for property proof
      if (!/^(Directory|Collection|Editorial|Affiliation|Pipeline)$/i.test(nz(parsed.dateLine))) {
        issues.push("missing_or_invalid_date");
      }
    }
    if (!/^https?:\/\//i.test(nz(parsed.sourceUrl))) issues.push("missing_source_url");
    if (words(parsed.description) < 35) issues.push(`body_too_thin:${words(parsed.description)}`);
    if (GENERIC_RE.test(corpus) || looksLikeDiligenceFillerMomentum(corpus)) {
      issues.push("generic_or_diligence_filler");
    }
    const wrong = corpusHasWrongBrand(corpus, brandSlug, brandName);
    if (wrong) issues.push(wrong);
    if (!region) issues.push("missing_region_label");
    if (region === "CALA") calaMomentumCount += 1;
    if (region === "International Reference") intlLabeledCount += 1;
    if (!firstRegion && region) firstRegion = region;

    // Fake announcement years on property-only pages without press framing
    if (
      /^\d{4}$/.test(nz(parsed.dateLine)) &&
      /\/overview\/?$/i.test(nz(parsed.sourceUrl)) &&
      !/\b(opening|opens|opened|conversion|signing|milestone|announcement|joins|joined|inaugur)\b/i.test(
        `${row.title} ${parsed.description}`
      )
    ) {
      issues.push("invented_year_on_property_listing");
    }

    const card = {
      brandSlug,
      section: "Recent Momentum",
      recordId: row.recordId || null,
      title: row.title,
      date: parsed.dateLine,
      link: parsed.sourceUrl,
      region: region || "UNLABELED",
      evidenceType: inferEvidenceType(row.title, parsed.description, parsed.sourceUrl),
      wordCount: words(parsed.description),
      issues,
      status: issues.length ? "fail" : "pass",
    };
    momentumCards.push(card);
    for (const issue of issues) {
      failures.push({
        id: `momentum:${issue}`,
        severity: "fail",
        recordId: row.recordId,
        title: row.title,
        detail: issue,
      });
    }
  }

  if (calaAvailable && calaMomentumCount === 0 && momentumRows.length) {
    failures.push({
      id: "cala_available_but_unused",
      severity: "fail",
      detail: "CALA inventory exists but no CALA-labeled momentum cards",
    });
  }
  if (calaAvailable && firstRegion && firstRegion !== "CALA" && calaMomentumCount > 0) {
    failures.push({
      id: "cala_not_prioritized_first",
      severity: "fail",
      detail: `firstRegion=${firstRegion}`,
    });
  }
  if (!calaAvailable && momentumRows.length && intlLabeledCount < momentumRows.length) {
    failures.push({
      id: "non_cala_missing_international_reference_label",
      severity: "fail",
      detail: `intlLabeled=${intlLabeledCount}/${momentumRows.length}`,
    });
  }

  const rawMomentum = rawUrlVisibleInHtml(html, "Recent Momentum");
  if (rawMomentum.count > 0) {
    failures.push({
      id: "raw_url_in_public_momentum_html",
      severity: "fail",
      detail: rawMomentum.samples.join(", "),
    });
  }

  const rawOpenings = rawUrlVisibleInHtml(html, "Openings\\s*/\\s*Examples\\s*/\\s*Properties");
  if (rawOpenings.count > 0) {
    failures.push({
      id: "raw_url_in_public_openings_html",
      severity: "fail",
      detail: rawOpenings.samples.join(", "),
    });
  }

  for (const row of openingsRows) {
    const corpus = [
      row.title,
      row.body,
      row.caseSummaryOverview,
      row.caseSummaryTags,
      row.caseSummaryBrandRelevance,
    ].join("\n");
    // Prefer explicit tags/title geography labels over incidental corpus mentions
    // (e.g. Stage 5 photo captions saying "International Reference photography").
    const region =
      classifyRegionFromText(row.caseSummaryTags || "") ||
      classifyRegionFromText(row.title || "") ||
      classifyRegionFromText(corpus) ||
      classifyRegionFromText(row.caseSummaryOverview || "");
    const issues = [];
    const wrong = corpusHasWrongBrand(corpus, brandSlug, brandName);
    if (wrong) issues.push(wrong);
    if (!region) issues.push("missing_region_label");
    if (!calaAvailable && region && region !== "International Reference" && !CALA_RE.test(corpus)) {
      // US-only without International Reference label
      if (!INT_REF_RE.test(corpus) && !CALA_RE.test(corpus)) {
        issues.push("non_cala_missing_international_reference_label");
      }
    }
    if (calaAvailable && !CALA_RE.test(corpus) && !INT_REF_RE.test(corpus)) {
      // openings outside CALA while CALA exists — allowed if labeled International Reference
      issues.push("non_cala_opening_unlabeled_while_cala_available");
    }

    const sourceLink = (nz(row.body).match(/https?:\/\/\S+/i) || [""])[0];
    const titleHead = nz(row.title).toLowerCase().split(/\s*[—–]\s*/)[0];
    const titleTokens = titleHead
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter(Boolean);
    const GEO_OR_GENERIC = new Set([
      "collection",
      "hotel",
      "hotels",
      "resort",
      "by",
      "wyndham",
      "hilton",
      "dazzler",
      "trademark",
      "tapestry",
      "buenos",
      "aires",
      "argentina",
      "florida",
      "beach",
      "miami",
      "urban",
      "views",
      "the",
      "and",
    ]);
    const distinctive = titleTokens.filter((w) => w.length > 3 && !GEO_OR_GENERIC.has(w));
    if (sourceLink && distinctive.length) {
      const urlBlob = sourceLink.toLowerCase().replace(/[^a-z0-9]+/g, " ");
      const hit = distinctive.some((tok) => urlBlob.includes(tok));
      if (!hit) issues.push("openings_source_url_mismatch_property");
    }

    const card = {
      brandSlug,
      section: "Openings / Examples / Properties",
      recordId: row.recordId || null,
      title: row.title,
      region: region || "UNLABELED",
      brandCorrect: !wrong,
      calaAvailable,
      currentLabel: row.caseSummaryTags || region || "",
      issues,
      status: issues.length ? "fail" : "pass",
      sourceLink,
    };
    openingsCards.push(card);
    for (const issue of issues) {
      failures.push({
        id: `openings:${issue}`,
        severity: "fail",
        recordId: row.recordId,
        title: row.title,
        detail: issue,
      });
    }
  }

  if (calaAvailable) {
    const calaOpenings = openingsCards.filter((c) => c.region === "CALA").length;
    if (openingsCards.length && calaOpenings === 0) {
      failures.push({
        id: "openings_cala_available_but_unused",
        severity: "fail",
        detail: "CALA inventory exists but openings lack CALA-labeled examples",
      });
    }
  }

  const uniqueFails = [];
  const seen = new Set();
  for (const f of failures) {
    const key = `${f.id}|${f.recordId || ""}|${f.detail || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    uniqueFails.push(f);
  }

  return {
    version: MOMENTUM_EVIDENCE_QUALITY_VERSION,
    brandSlug,
    brandName,
    pass: uniqueFails.length === 0,
    calaAvailable,
    momentumCards,
    openingsCards,
    failures: uniqueFails,
    summary: {
      momentumCards: momentumCards.length,
      openingsCards: openingsCards.length,
      failCount: uniqueFails.length,
      calaMomentumCount,
      intlLabeledCount,
    },
  };
}

function inferEvidenceType(title, body, url) {
  const t = `${title} ${body}`.toLowerCase();
  if (/milestone|surpass|100 hotels|portfolio/.test(t)) return "brand_milestone";
  if (/join|joined|conversion|rebrand|relaunches|relaunch/.test(t)) return "conversion";
  if (/opening|opened|inaugur|debut/.test(t)) return "opening";
  if (/signing|signed|franchise agreement/.test(t)) return "signing";
  if (/\/overview\/?$/i.test(nz(url)) || /property proof|official property|directory/.test(t)) {
    return "official_brand_property_proof";
  }
  if (/development|pipeline|expand/.test(t)) return "development_announcement";
  return "official_brand_property_proof";
}

export { buildMomentumBody, parseMomentumPresentationBody };
